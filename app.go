package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"time"
)

//go:embed web/index.html
var indexHTML []byte

type Status struct {
	Phase        string           `json:"phase"`
	Message      string           `json:"message"`
	Room         Room             `json:"room"`
	Endpoint     string           `json:"endpoint,omitempty"`
	Attempt      int              `json:"attempt"`
	LoginOK      bool             `json:"loginOK"`
	GroupSent    bool             `json:"groupSent"`
	Heartbeats   int64            `json:"heartbeats"`
	Packets      int64            `json:"packets"`
	Types        map[string]int64 `json:"types"`
	Events       map[string]int64 `json:"events"`
	UpdatedAt    string           `json:"updatedAt"`
	LastReceived string           `json:"lastReceived,omitempty"`
}

// SSE 广播只送页面读的九项；剩下四项是诊断计数器，曾占 SSE 字节的 38%。
// /api/status 仍全量。白名单是显式的：新增 Status 字段默认不进广播。
type wireStatus Status

func (s wireStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Phase        string `json:"phase"`
		Message      string `json:"message"`
		Room         Room   `json:"room"`
		Endpoint     string `json:"endpoint,omitempty"`
		Attempt      int    `json:"attempt"`
		LoginOK      bool   `json:"loginOK"`
		GroupSent    bool   `json:"groupSent"`
		Heartbeats   int64  `json:"heartbeats"`
		LastReceived string `json:"lastReceived,omitempty"`
	}{s.Phase, s.Message, s.Room, s.Endpoint, s.Attempt, s.LoginOK, s.GroupSent, s.Heartbeats, s.LastReceived})
}

// 订阅首帧、20 秒保活、hub 广播三处共用，漏掉一处就会发出全量。
func statusEvent(status Status) streamEvent {
	return streamEvent{Name: "status", Data: wireStatus(status)}
}

type streamEvent struct {
	Name string
	Data any
}

// 开播瞬间几十个贵族同时进房是常态，令牌桶留这点突发，之后降到每 2 秒一条。
const (
	enterBurst    = 5.0
	enterInterval = 2 * time.Second
)

// 只节流计数器刷新；相位变化走 Update() 无条件立即发，不会晚一拍。
// 代价是「连接详情」里的心跳与最近接收最多旧 5 秒。
const statusInterval = 5 * time.Second

// 按 uid 记最近一次付费送礼，给随后到达的孵化产物找出处。满了整表清空：
// 最坏只是几条产物少一行注解，它们本来就不计价。
const maxGiftTriggers = 512

// 队列必须明显大于回放条数：回放逐条写出，慢设备写完之前新消息还在进队，
// 一溢出就掉线重连、再回放一次，会自激。
const (
	historyLimit    = 300
	subscriberQueue = 512
)

type giftTrigger struct {
	gift string
	at   time.Time
}

type eventHub struct {
	mu          sync.Mutex
	status      Status
	history     []Event
	gifts       giftCatalog
	giftRoomID  string
	triggers    map[string]giftTrigger
	subscribers map[chan streamEvent]struct{}
	lastPublish time.Time
	enterTokens float64
	enterAt     time.Time
	closed      bool
}

func newHub() *eventHub {
	return &eventHub{
		status:      Status{Phase: "idle", Message: "输入房间，开始接收文字消息", Types: map[string]int64{}, Events: map[string]int64{}},
		triggers:    make(map[string]giftTrigger),
		subscribers: make(map[chan streamEvent]struct{}),
	}
}

func copyCounts(source map[string]int64) map[string]int64 {
	target := make(map[string]int64, len(source))
	for key, count := range source {
		target[key] = count
	}
	return target
}

func (h *eventHub) snapshotLocked() Status {
	status := h.status
	status.Types, status.Events = copyCounts(status.Types), copyCounts(status.Events)
	return status
}

func (h *eventHub) Snapshot() Status {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snapshotLocked()
}

func (h *eventHub) broadcastLocked(event streamEvent) {
	for channel := range h.subscribers {
		select {
		case channel <- event:
		default:
			// 踢掉写不动的订阅；EventSource 会重连并拿到新快照。
			delete(h.subscribers, channel)
			close(channel)
			// 排空，让 stream 尽快释放这个观看者。
			for range channel {
			}
		}
	}
}

func (h *eventHub) publishStatusLocked() {
	h.lastPublish = time.Now()
	h.status.UpdatedAt = h.lastPublish.Format(time.RFC3339Nano)
	h.broadcastLocked(statusEvent(h.snapshotLocked()))
}

func (h *eventHub) Update(change func(*Status)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	change(&h.status)
	h.publishStatusLocked()
}

func (h *eventHub) Reset(input string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.status = Status{Phase: "resolving", Message: "正在查询真实房间号", Room: Room{Input: input}, Types: map[string]int64{}, Events: map[string]int64{}}
	h.history = nil
	h.enterTokens, h.enterAt = 0, time.Time{}
	clear(h.triggers)
	// 保留上一份礼物目录；Packet 只认 RID 对得上的那份。
	h.broadcastLocked(streamEvent{Name: "reset", Data: struct{}{}})
	h.publishStatusLocked()
}

// 首次调用给满桶，刚进房时最近几位贵宾照样能看到。
func (h *eventHub) allowEnterLocked(now time.Time) bool {
	if h.enterAt.IsZero() {
		h.enterTokens = enterBurst
	} else if elapsed := now.Sub(h.enterAt); elapsed > 0 {
		h.enterTokens = math.Min(enterBurst, h.enterTokens+elapsed.Seconds()/enterInterval.Seconds())
	}
	h.enterAt = now
	if h.enterTokens < 1 {
		return false
	}
	h.enterTokens--
	return true
}

// 付费礼物进表，孵化产物出表，都按 uid 配对；窗口内没出处就不加注解。
func (h *eventHub) traceGiftTriggerLocked(event *Event, now time.Time) {
	if event.UserID == "" {
		return
	}
	if triggerGift(*event) {
		if len(h.triggers) >= maxGiftTriggers {
			clear(h.triggers)
		}
		h.triggers[event.UserID] = giftTrigger{gift: event.Gift, at: now}
		return
	}
	if !derivedGift(*event) {
		return
	}
	if trigger, exists := h.triggers[event.UserID]; exists && now.Sub(trigger.at) <= derivedGiftWindow {
		event.TriggeredBy = trigger.gift
	}
}

// 目录会分几次到达（重试补齐），同房间的叠加而不覆盖。
func (h *eventHub) mergeGifts(roomID string, catalog giftCatalog) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.giftRoomID == roomID {
		catalog = h.gifts.supplement(catalog)
	}
	h.gifts, h.giftRoomID = catalog, roomID
}

func (h *eventHub) Packet(fields map[string]string, roomID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.status.Packets++
	h.status.Types[fields["type"]]++
	h.status.LastReceived = time.Now().Format(time.RFC3339Nano)
	catalog := h.gifts
	if h.giftRoomID != roomID {
		catalog = nil
	}
	if event, ok := normalizeEvent(fields, roomID, catalog); ok && (event.Kind != "enter" || h.allowEnterLocked(time.Now())) {
		h.traceGiftTriggerLocked(&event, time.Now())
		h.status.Events[event.Kind]++
		event.Index = h.status.Events[event.Kind]
		if len(h.history) == historyLimit {
			copy(h.history, h.history[1:])
			h.history = h.history[:historyLimit-1]
		}
		h.history = append(h.history, event)
		h.broadcastLocked(streamEvent{Name: "message", Data: event})
	}
	if time.Since(h.lastPublish) >= statusInterval {
		h.publishStatusLocked()
	}
}

func (h *eventHub) Subscribe() (Status, []Event, chan streamEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	channel := make(chan streamEvent, subscriberQueue)
	if h.closed {
		close(channel)
	} else {
		h.subscribers[channel] = struct{}{}
	}
	return h.snapshotLocked(), append([]Event(nil), h.history...), channel
}

func (h *eventHub) IsSubscribed(channel chan streamEvent) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, exists := h.subscribers[channel]
	return exists
}

func (h *eventHub) Unsubscribe(channel chan streamEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.subscribers[channel]; exists {
		delete(h.subscribers, channel)
		close(channel)
	}
}

func (h *eventHub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for channel := range h.subscribers {
		delete(h.subscribers, channel)
		close(channel)
	}
}

type application struct {
	access     accessInfo
	auth       *passwordAuth
	settings   *settingsStore
	scoreboard *scoreboardStore
	icons      *iconStore
	rooms      *roomManager
}

func newApplication(resolver roomResolver, config clientConfig, limits roomManagerConfig, password string) *application {
	return &application{
		rooms:      newRoomManager(resolver, config, limits),
		auth:       newPasswordAuth(password),
		settings:   &settingsStore{value: serverSettings{DefaultRoom: defaultRoom}},
		scoreboard: newScoreboardStore(),
		icons:      newIconStore(),
	}
}

func (a *application) Close() {
	a.icons.Close()
	a.rooms.Close()
}

func writeJSON(w http.ResponseWriter, code int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(data)
}

func (a *application) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Write(indexHTML)
	})
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		rid := r.URL.Query().Get("rid")
		if !roomIDPattern.MatchString(rid) || rid == "0" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "请提供真实房间号 rid"})
			return
		}
		status, exists := a.rooms.Status(rid)
		if !exists {
			writeJSON(w, http.StatusNotFound, map[string]string{"code": "room_inactive", "error": "房间当前未被监控"})
			return
		}
		writeJSON(w, http.StatusOK, status)
	})
	mux.HandleFunc("GET /api/access", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, a.access) })
	a.settings.registerRoutes(mux)
	a.scoreboard.registerRoutes(mux)
	a.icons.registerRoutes(mux)
	mux.HandleFunc("POST /api/resolve", func(w http.ResponseWriter, r *http.Request) {
		var request struct{ Room string }
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2048))
		if err := decoder.Decode(&request); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "请输入房间号或房间链接"})
			return
		}
		input, err := parseRoomInput(request.Room)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		ctx, cancel := a.roomRequestContext(r)
		defer cancel()
		room, err := a.rooms.Resolve(ctx, input)
		if a.sessionExpired(r.Context().Value(authSessionKey{}).(*authSession)) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "请先登录"})
			return
		}
		if r.Context().Err() != nil {
			return
		}
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "房间信息获取失败"})
			return
		}
		writeJSON(w, http.StatusOK, room)
	})
	mux.HandleFunc("GET /events", a.stream)
	return a.auth.protect(mux)
}

func (a *application) sessionExpired(session *authSession) bool {
	select {
	case <-session.done:
		return true
	default:
		return !a.auth.now().Before(session.expires)
	}
}

// 会话到期时解析也要停。
func (a *application) roomRequestContext(r *http.Request) (context.Context, func()) {
	session := r.Context().Value(authSessionKey{}).(*authSession)
	ctx, cancel := context.WithTimeout(r.Context(), session.expires.Sub(a.auth.now()))
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-session.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, func() { cancel(); <-watchDone }
}

func (a *application) stream(w http.ResponseWriter, r *http.Request) {
	input, err := parseRoomInput(r.URL.Query().Get("room"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if _, ok := w.(http.Flusher); !ok {
		http.Error(w, "浏览器连接不支持实时更新", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	if r.Method == http.MethodHead {
		return
	}
	session := r.Context().Value(authSessionKey{}).(*authSession)
	ctx, cancel := a.roomRequestContext(r)
	defer cancel()
	controller := http.NewResponseController(w)
	send := func(name string, value any) bool {
		data, err := json.Marshal(value)
		if err != nil {
			return false
		}
		controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, data); err != nil {
			return false
		}
		return controller.Flush() == nil
	}
	endSession := func() { send("auth-expired", struct{}{}) }
	subscription, err := a.rooms.Subscribe(ctx, input)
	if err != nil {
		if a.sessionExpired(session) {
			endSession()
			return
		}
		if r.Context().Err() != nil {
			return
		}
		var rejected *subscriptionError
		if !errors.As(err, &rejected) {
			rejected = &subscriptionError{Code: "resolve_failed", Message: "房间信息获取失败"}
		}
		if a.rooms.ctx.Err() != nil {
			rejected = &subscriptionError{Code: "server_closing", Message: "服务正在关闭", roomID: rejected.roomID}
		}
		envelope := roomEnvelope{Data: rejected}
		if rejected.roomID != "" {
			envelope.RoomID = &rejected.roomID
		}
		send("subscription-error", envelope)
		return
	}
	defer subscription.Close()
	write := func(event streamEvent) bool {
		if a.sessionExpired(session) {
			endSession()
			return false
		}
		if ctx.Err() != nil || a.rooms.ctx.Err() != nil {
			return false
		}
		return send(event.Name, subscription.worker.envelope(event.Data))
	}
	if !write(streamEvent{Name: "reset", Data: struct{}{}}) || !write(statusEvent(subscription.status)) {
		return
	}
	for _, event := range subscription.history {
		if !subscription.worker.hub.IsSubscribed(subscription.events) || !write(streamEvent{Name: "message", Data: event}) {
			return
		}
	}
	keepalive := time.NewTicker(20 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-ctx.Done():
			if a.sessionExpired(session) {
				endSession()
			}
			return
		case <-a.rooms.ctx.Done():
			return
		case event, open := <-subscription.events:
			if !open || !write(event) {
				return
			}
		case <-keepalive.C:
			if !write(statusEvent(subscription.worker.hub.Snapshot())) {
				return
			}
		}
	}
}
