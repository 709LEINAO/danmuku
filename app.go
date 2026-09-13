package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
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

// 订阅首帧、20 秒保活、hub 广播三处共用 frame()，漏掉一处就会发出全量。

// 开播瞬间几十个贵族同时进房是常态，令牌桶留这点突发，之后降到每 2 秒一条。
const (
	enterBurst    = 5.0
	enterInterval = 2 * time.Second
)

// 钻粉开通按活动扎堆来：主播喊一次卡能连着砸进几十条。桶留 3 个，正好一次
// 填满置顶区那三行，之后每 5 秒放一条。
const (
	diamondBurst    = 3.0
	diamondInterval = 5 * time.Second
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

// 一次写出攒多少字节。成本大头是 write+flush 这对系统调用，不是序列化：一条
// chatmsg 三四百字节，逐帧写等于每条消息给每个观看者各来一次。攒到这个上限就发，
// 再大只会让慢连接的首字节更晚。
const maxStreamBatch = 64 << 10

type giftTrigger struct {
	gift string
	at   time.Time
}

// 首次调用给满桶：刚进房时最近几位贵宾、最近几张钻粉卡照样能看到。
type tokenBucket struct {
	tokens float64
	at     time.Time
}

func (b *tokenBucket) allow(now time.Time, burst float64, interval time.Duration) bool {
	if b.at.IsZero() {
		b.tokens = burst
	} else if elapsed := now.Sub(b.at); elapsed > 0 {
		b.tokens = math.Min(burst, b.tokens+elapsed.Seconds()/interval.Seconds())
	}
	b.at = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// SSE 帧在 hub 里编码一次再扇出：同一房间所有订阅者收到的字节完全相同，因为信封的
// roomId/generation 是 worker 级的、创建后不变。原先每个订阅者的 stream goroutine 各自
// Marshal 一份，CPU 随观看者数线性增长——实测 1000 条/秒下 20 人占 50 % CPU、送达 100 %，
// 100 人占 122 % 且送达跌到 56 %、被踢 621 次。
type eventHub struct {
	mu sync.Mutex
	// 构造后不变，frame() 因此无需持锁。
	roomID     string
	generation string
	resetFrame []byte
	status     Status
	history    [][]byte
	// history[i] 的序号恒为 historyBase+i，所以只存一个基准就够，不必给每帧配结构体。
	// nextSeq 是下一条消息要用的号，编码失败时不消耗。
	historyBase uint64
	nextSeq     uint64
	gifts       giftCatalog
	giftRoomID  string
	triggers    map[string]giftTrigger
	subscribers map[chan []byte]struct{}
	lastPublish time.Time
	// 收包时刻只记不格式化：每包 Format 一次 RFC3339Nano 是笔白账，这个值要么
	// 5 秒一次随状态帧出门，要么走 /api/status，其余时候没人看。
	lastReceived time.Time
	enter        tokenBucket
	diamond      tokenBucket
	closed       bool
}

func newHub(roomID, generation string) *eventHub {
	hub := &eventHub{
		roomID: roomID, generation: generation,
		status:      Status{Phase: "idle", Message: "输入房间，开始接收文字消息", Types: map[string]int64{}, Events: map[string]int64{}},
		triggers:    make(map[string]giftTrigger),
		subscribers: make(map[chan []byte]struct{}),
	}
	// 内容恒定，编一次供全部订阅者复用。
	hub.resetFrame = hub.frame("reset", "", struct{}{})
	return hub
}

// 整条 SSE 帧，含信封。编码失败返回 nil，调用方跳过这一帧而不是断开订阅。
// id 非空时写进 id: 行——浏览器会记住它，断线重连时用 Last-Event-ID 头带回来。
// 只有消息帧带 id：状态帧与 reset 帧不该把续传点往前推。
func (h *eventHub) frame(name, id string, data any) []byte {
	payload, err := json.Marshal(roomEnvelope{RoomID: &h.roomID, Generation: &h.generation, Data: data})
	if err != nil {
		log.Printf("RID=%s 无法编码 %s 帧：%v", h.roomID, name, err)
		return nil
	}
	frame := make([]byte, 0, len("event: \nid: \ndata: \n\n")+len(name)+len(id)+len(payload))
	frame = append(frame, "event: "...)
	frame = append(frame, name...)
	if id != "" {
		frame = append(frame, "\nid: "...)
		frame = append(frame, id...)
	}
	frame = append(frame, "\ndata: "...)
	frame = append(frame, payload...)
	return append(frame, '\n', '\n')
}

// 续传点带上 generation：worker 被回收重建后序号从头开始，光凭序号会把新旧两段混起来。
func (h *eventHub) eventID(seq uint64) string {
	return h.generation + ":" + strconv.FormatUint(seq, 36)
}

// Last-Event-ID 对应的回放起点（history 下标）。generation 对不上、落后得比历史窗口还多、
// 或者压根解析不出来，都退回全量回放——那正是加这个机制之前的行为。
func (h *eventHub) replayFromLocked(lastEventID string) int {
	generation, number, ok := strings.Cut(lastEventID, ":")
	if !ok || generation != h.generation {
		return 0
	}
	seen, err := strconv.ParseUint(number, 36, 64)
	if err != nil || seen < h.historyBase {
		return 0
	}
	offset := seen - h.historyBase + 1
	if offset > uint64(len(h.history)) {
		// 客户端报的比我们手上的还新，不重发。
		return len(h.history)
	}
	return int(offset)
}

func (h *eventHub) ResetFrame() []byte { return h.resetFrame }

// 读 Status 之前把推迟的时间戳补上，这是唯一会看它的地方。
func (h *eventHub) syncReceivedLocked() {
	if !h.lastReceived.IsZero() {
		h.status.LastReceived = h.lastReceived.Format(time.RFC3339Nano)
	}
}

// wireStatus 不输出 Types/Events，所以这里不必先 snapshot 拷贝那两张表。
func (h *eventHub) statusFrameLocked() []byte {
	h.syncReceivedLocked()
	return h.frame("status", "", wireStatus(h.status))
}

func (h *eventHub) StatusFrame() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.statusFrameLocked()
}

func copyCounts(source map[string]int64) map[string]int64 {
	target := make(map[string]int64, len(source))
	for key, count := range source {
		target[key] = count
	}
	return target
}

func (h *eventHub) snapshotLocked() Status {
	h.syncReceivedLocked()
	status := h.status
	status.Types, status.Events = copyCounts(status.Types), copyCounts(status.Events)
	return status
}

func (h *eventHub) Snapshot() Status {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snapshotLocked()
}

func (h *eventHub) broadcastLocked(frame []byte) {
	if frame == nil {
		return
	}
	for channel := range h.subscribers {
		select {
		case channel <- frame:
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
	h.broadcastLocked(h.statusFrameLocked())
}

func (h *eventHub) Update(change func(*Status)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	change(&h.status)
	h.publishStatusLocked()
}

// 只有会扎堆刷屏的两类过桶，其余一律放行。
func (h *eventHub) allowEventLocked(kind string, now time.Time) bool {
	switch kind {
	case "enter":
		return h.enter.allow(now, enterBurst, enterInterval)
	case "diamond":
		return h.diamond.allow(now, diamondBurst, diamondInterval)
	}
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
	now := time.Now()
	h.status.Packets++
	h.status.Types[fields["type"]]++
	h.lastReceived = now
	catalog := h.gifts
	if h.giftRoomID != roomID {
		catalog = nil
	}
	if event, ok := normalizeEvent(fields, roomID, catalog); ok && h.allowEventLocked(event.Kind, now) {
		h.traceGiftTriggerLocked(&event, now)
		h.status.Events[event.Kind]++
		event.Index = h.status.Events[event.Kind]
		// 历史存编好的帧而不是 Event：回放不必逐条重编，一条 chatmsg 也从 ~1 KB 的
		// 全量 Fields 降到投影后的帧长。
		if frame := h.frame("message", h.eventID(h.nextSeq), wireEvent(event)); frame != nil {
			h.nextSeq++
			if len(h.history) == historyLimit {
				copy(h.history, h.history[1:])
				h.history = h.history[:historyLimit-1]
				h.historyBase++
			}
			h.history = append(h.history, frame)
			h.broadcastLocked(frame)
		}
	}
	if now.Sub(h.lastPublish) >= statusInterval {
		h.publishStatusLocked()
	}
}

// lastEventID 来自浏览器重连时自动带的 Last-Event-ID 头；为空就是全新连接，全量回放。
func (h *eventHub) Subscribe(lastEventID string) ([]byte, [][]byte, chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	channel := make(chan []byte, subscriberQueue)
	if h.closed {
		close(channel)
	} else {
		h.subscribers[channel] = struct{}{}
	}
	// 帧只读，与仍在历史里的那份共享底层数组。
	pending := h.history[h.replayFromLocked(lastEventID):]
	return h.statusFrameLocked(), append([][]byte(nil), pending...), channel
}

func (h *eventHub) IsSubscribed(channel chan []byte) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, exists := h.subscribers[channel]
	return exists
}

func (h *eventHub) Unsubscribe(channel chan []byte) {
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

// ctx.Err() 要锁 cancelCtx 的互斥量，而 a.rooms.ctx 是全进程共享的那一个；帧路径上
// 每次都查会把所有 stream goroutine 挤到同一把锁上。Done() 首次之后是原子读。
func cancelled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func (a *application) sessionExpired(session *authSession) bool {
	if cancelled(session.ctx) {
		return true
	}
	return !a.auth.now().Before(session.deadline())
}

// 会话到期时解析也要停。AfterFunc 不起 goroutine，会话撤销直接连到这次请求的取消上。
func (a *application) roomRequestContext(r *http.Request) (context.Context, func()) {
	session := r.Context().Value(authSessionKey{}).(*authSession)
	ctx, cancel := context.WithTimeout(r.Context(), session.deadline().Sub(a.auth.now()))
	stop := context.AfterFunc(session.ctx, cancel)
	return ctx, func() { stop(); cancel() }
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
	sendFrame := func(frame []byte) bool {
		if frame == nil {
			return true
		}
		controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := w.Write(frame); err != nil {
			return false
		}
		return controller.Flush() == nil
	}
	// 只有这两类帧不经 hub：一个没有房间信封，一个的信封来自失败的订阅。
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
	// 浏览器重连会带上它，服务端据此只补客户端真正漏掉的那几条，而不是又推 300 条。
	subscription, err := a.rooms.Subscribe(ctx, input, r.Header.Get("Last-Event-ID"))
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
	write := func(frame []byte) bool {
		if a.sessionExpired(session) {
			endSession()
			return false
		}
		if cancelled(ctx) || cancelled(a.rooms.ctx) {
			return false
		}
		return sendFrame(frame)
	}
	if !write(subscription.worker.hub.ResetFrame()) || !write(subscription.statusFrame) {
		return
	}
	// 回放也成批写：300 条逐条 write+flush 是重连风暴里最贵的一段，而慢设备正是在
	// 这段里被新进的消息挤爆队列、掉线、再回放一次的。
	//
	// 按需长：安静房间里每条消息都走 coalesce 的零拷贝分支，缓冲一直是 nil；只有真
	// 积压过的连接才会撑到上限。预先给每条连接留 64 KiB 的话，100 个观看者光这里就是
	// 6 MiB，而绝大多数时候一个字节都用不上。
	var batch []byte
	for index := 0; index < len(subscription.history); {
		if !subscription.worker.hub.IsSubscribed(subscription.events) {
			return
		}
		batch = batch[:0]
		for index < len(subscription.history) && len(batch) < maxStreamBatch {
			batch = append(batch, subscription.history[index]...)
			index++
		}
		if !write(batch) {
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
		case frame, open := <-subscription.events:
			if !open {
				return
			}
			var payload []byte
			var ended bool
			payload, batch, ended = coalesce(frame, subscription.events, batch)
			if !write(payload) || ended {
				return
			}
		case <-keepalive.C:
			if !write(subscription.worker.hub.StatusFrame()) {
				return
			}
		}
	}
}

// 把订阅通道里已经排着的帧一并取走。只取已经到手的，不等待，所以一毫秒延迟都不加：
// 低速时一条也取不到，返回的还是 hub 那份共享字节、零拷贝，与逐帧写完全一样；高速时
// 取到的正好是上一次 write+flush 期间堆起来的量，于是系统调用从每帧一次降到每批一次。
// 返回 reuse 供下一轮复用容量；ended 表示订阅在排空途中被关掉了。
func coalesce(first []byte, events <-chan []byte, buffer []byte) (payload, reuse []byte, ended bool) {
	payload, buffer = first, buffer[:0]
	for len(payload) < maxStreamBatch {
		select {
		case next, open := <-events:
			if !open {
				return payload, buffer, true
			}
			if len(buffer) == 0 {
				buffer = append(buffer, first...)
			}
			buffer = append(buffer, next...)
			payload = buffer
		default:
			return payload, buffer, false
		}
	}
	return payload, buffer, false
}
