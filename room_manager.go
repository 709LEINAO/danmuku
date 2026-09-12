package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type roomManagerConfig struct {
	MaxRooms   int
	MaxViewers int
	IdleGrace  time.Duration
	MaxDials   int
}

func defaultRoomManagerConfig() roomManagerConfig {
	return roomManagerConfig{MaxRooms: 12, MaxViewers: 20, IdleGrace: 60 * time.Second, MaxDials: 2}
}

type roomEnvelope struct {
	RoomID     *string `json:"roomId"`
	Generation *string `json:"generation"`
	Data       any     `json:"data"`
}

type subscriptionError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	roomID  string
}

func (e *subscriptionError) Error() string { return e.Message }

type roomWorker struct {
	room       Room
	generation string
	hub        *eventHub
	cancel     context.CancelFunc
	done       chan struct{}
	// 以下生命周期字段由 manager 锁保护。
	viewers int
	idle    *time.Timer
	closing bool
}

func (w *roomWorker) envelope(data any) roomEnvelope {
	return roomEnvelope{RoomID: &w.room.ID, Generation: &w.generation, Data: data}
}

type roomSubscription struct {
	manager *roomManager
	worker  *roomWorker
	status  Status
	history []Event
	events  chan streamEvent
	once    sync.Once
}

func (s *roomSubscription) Close() {
	s.once.Do(func() {
		s.worker.hub.Unsubscribe(s.events)
		s.manager.release(s.worker)
	})
}

// 手机息屏会掐断 SSE，回前台重连一次就要重抓一张 2 MiB 的 SSR 页；而 worker 已在跑时，
// 新解析出的 Room 除了查表之外整个被丢掉，这趟回源是纯浪费。只缓存成功的解析。
const (
	roomResolveTTL   = 5 * time.Minute
	maxResolvedRooms = 64
)

type resolvedRoom struct {
	room Room
	at   time.Time
}

type roomManager struct {
	mu          sync.Mutex
	config      roomManagerConfig
	resolver    roomResolver
	client      danmakuClient
	catalogHTTP *http.Client
	props       *propCatalogStore
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	workers     map[string]*roomWorker
	resolved    map[string]resolvedRoom
	now         func() time.Time
	viewers     int // 含仍在解析、或等待房间腾位的请求。
	closed      bool
	processID   string
	nextID      uint64
	tasks       sync.WaitGroup // worker、在途请求、空闲回收回调。
}

func newRoomManager(resolver roomResolver, config clientConfig, limits roomManagerConfig) *roomManager {
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	config.DialSlots = make(chan struct{}, limits.MaxDials)
	return &roomManager{
		config: limits, resolver: resolver, client: danmakuClient{config: config},
		// 道具表 1.4 MB，5 秒等于要求 280 KB/s，弱网必挂。三个 rid 接口另有 5 秒时限
		// （见 fetchGiftRows），放宽这里不连累它们。
		catalogHTTP: &http.Client{Timeout: 30 * time.Second},
		props:       newPropCatalogStore(),
		ctx:         ctx, cancel: cancel, done: make(chan struct{}), workers: make(map[string]*roomWorker),
		resolved: make(map[string]resolvedRoom), now: time.Now,
		processID: base64.RawURLEncoding.EncodeToString(random[:]),
	}
}

// 命中缓存就不出门。并发的同一房间仍各自回源一次，不值得为此再叠一层单飞。
func (m *roomManager) resolveRoom(ctx context.Context, input string) (Room, error) {
	key, err := parseRoomInput(input)
	if err != nil {
		return Room{}, err
	}
	m.mu.Lock()
	entry, cached := m.resolved[key]
	fresh := cached && m.now().Sub(entry.at) < roomResolveTTL
	m.mu.Unlock()
	if fresh {
		return entry.room, nil
	}
	room, err := m.resolver.Resolve(ctx, key)
	if err != nil {
		return room, err
	}
	m.mu.Lock()
	// 满了整表清空：这张表只省一趟网络，丢了最坏也就重新解析一遍。
	if _, exists := m.resolved[key]; !exists && len(m.resolved) >= maxResolvedRooms {
		clear(m.resolved)
	}
	m.resolved[key] = resolvedRoom{room: room, at: m.now()}
	m.mu.Unlock()
	return room, nil
}

func (m *roomManager) requestContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(m.ctx, cancel)
	return ctx, func() { stop(); cancel() }
}

// 脱离调用方 worker 的取消，但仍随 manager 一起结束。
func (m *roomManager) propLeadContext(ctx context.Context) (context.Context, func()) {
	return m.requestContext(context.WithoutCancel(ctx))
}

func (m *roomManager) Resolve(ctx context.Context, input string) (Room, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return Room{}, &subscriptionError{Code: "server_closing", Message: "服务正在关闭"}
	}
	m.tasks.Add(1)
	m.mu.Unlock()
	defer m.tasks.Done()
	ctx, cancel := m.requestContext(ctx)
	defer cancel()
	return m.resolveRoom(ctx, input)
}

func (m *roomManager) Subscribe(ctx context.Context, input string) (*roomSubscription, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, &subscriptionError{Code: "server_closing", Message: "服务正在关闭"}
	}
	if m.viewers >= m.config.MaxViewers {
		m.mu.Unlock()
		return nil, &subscriptionError{Code: "viewer_limit", Message: "观看连接已达上限，请稍后重试"}
	}
	m.viewers++
	m.tasks.Add(1)
	m.mu.Unlock()
	acquired := false
	defer func() {
		if !acquired {
			m.release(nil)
		}
		m.tasks.Done()
	}()
	ctx, cancel := m.requestContext(ctx)
	defer cancel()
	room, err := m.resolveRoom(ctx, input)
	if err != nil {
		return nil, &subscriptionError{Code: "resolve_failed", Message: "房间信息获取失败"}
	}
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, &subscriptionError{Code: "server_closing", Message: "服务正在关闭", roomID: room.ID}
		}
		if err := ctx.Err(); err != nil {
			m.mu.Unlock()
			return nil, err
		}
		worker := m.workers[room.ID]
		var retiring *roomWorker
		if worker != nil && worker.closing {
			retiring = worker
		} else if worker == nil && len(m.workers) >= m.config.MaxRooms {
			for _, candidate := range m.workers {
				if candidate.viewers == 0 {
					retiring = candidate
					break
				}
			}
			if retiring == nil {
				m.mu.Unlock()
				return nil, &subscriptionError{Code: "room_limit", Message: "正在观看的房间已达上限，请稍后重试", roomID: room.ID}
			}
		}
		if retiring != nil {
			retiring.closing = true
			m.stopIdleLocked(retiring)
			m.mu.Unlock()
			// 上游与目录都退出之前，槽位和 RID 一直占着。
			retiring.cancel()
			select {
			case <-retiring.done:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if worker == nil {
			workerCtx, workerCancel := context.WithCancel(m.ctx)
			m.nextID++
			worker = &roomWorker{
				room: room, generation: m.processID + "-" + strconv.FormatUint(m.nextID, 36),
				hub: newHub(), cancel: workerCancel, done: make(chan struct{}),
			}
			worker.hub.status.Room = room
			worker.hub.status.Phase, worker.hub.status.Message = "connecting", "正在连接弹幕服务"
			m.workers[room.ID] = worker
			m.tasks.Add(1)
			go m.runWorker(workerCtx, worker)
		}
		m.stopIdleLocked(worker)
		worker.viewers++
		m.mu.Unlock()
		status, history, events := worker.hub.Subscribe()
		acquired = true
		return &roomSubscription{manager: m, worker: worker, status: status, history: history, events: events}, nil
	}
}

func (m *roomManager) stopIdleLocked(worker *roomWorker) {
	if worker.idle != nil {
		if worker.idle.Stop() {
			m.tasks.Done()
		}
		worker.idle = nil
	}
}

func (m *roomManager) release(worker *roomWorker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return // Close 已释放全部观看者并取消全部 worker。
	}
	m.viewers--
	if worker == nil {
		return
	}
	worker.viewers--
	if worker.viewers != 0 || worker.closing {
		return
	}
	m.tasks.Add(1)
	var timer *time.Timer
	timer = time.AfterFunc(m.config.IdleGrace, func() {
		defer m.tasks.Done()
		m.mu.Lock()
		if m.closed || worker.idle != timer || worker.viewers != 0 || worker.closing {
			m.mu.Unlock()
			return
		}
		worker.idle = nil
		worker.closing = true
		m.mu.Unlock()
		worker.cancel()
	})
	worker.idle = timer
}

func (m *roomManager) Status(rid string) (roomEnvelope, bool) {
	m.mu.Lock()
	worker := m.workers[rid]
	active := !m.closed && worker != nil && !worker.closing
	m.mu.Unlock()
	if !active {
		return roomEnvelope{}, false
	}
	return worker.envelope(worker.hub.Snapshot()), true
}

// 目录有四个来源，任一失败都按退避重试到全部齐；已到手的部分立即生效。
// 不重试的话，开房那一刻的一次超时会让整个 worker 存活期都没有礼物价。
const (
	giftCatalogRetryMin = 5 * time.Second
	giftCatalogRetryMax = 10 * time.Minute
)

func (m *roomManager) loadGiftCatalog(ctx context.Context, worker *roomWorker, done chan<- struct{}) {
	defer close(done)
	delay := giftCatalogRetryMin
	for attempt := 1; ; attempt++ {
		// 道具表全站共享，领头者的回源不能跟着某一个房间走，否则这个房间被回收会
		// 连累正等同一张表的其他房间。
		catalog, err := fetchGiftCatalog(ctx, worker.room.ID, m.catalogHTTP, m.props, m.propLeadContext)
		if ctx.Err() != nil {
			return
		}
		if len(catalog) > 0 {
			worker.hub.mergeGifts(worker.room.ID, catalog)
		}
		if err == nil {
			return
		}
		log.Printf("礼物参考信息暂不完整 RID=%s（第 %d 次，%s 后重试）：%v", worker.room.ID, attempt, delay, err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(delay*2, giftCatalogRetryMax)
	}
}

func (m *roomManager) runWorker(ctx context.Context, worker *roomWorker) {
	defer m.tasks.Done()
	catalogDone := make(chan struct{})
	go m.loadGiftCatalog(ctx, worker, catalogDone)
	m.client.Run(ctx, worker.room.ID, func(state clientState) {
		worker.hub.Update(func(status *Status) {
			status.Phase, status.Message, status.Endpoint, status.Attempt = state.Phase, state.Message, state.Endpoint, state.Attempt
			status.LoginOK, status.GroupSent = state.LoginOK, state.GroupSent
		})
		log.Printf("[%s] RID=%s %s (%s)", state.Phase, worker.room.ID, state.Message, state.Endpoint)
	}, func(fields map[string]string) {
		worker.hub.Packet(fields, worker.room.ID)
	}, func() {
		worker.hub.Update(func(status *Status) { status.Heartbeats++ })
	})
	worker.cancel()
	<-catalogDone
	worker.hub.Close()
	m.mu.Lock()
	m.stopIdleLocked(worker)
	delete(m.workers, worker.room.ID)
	m.mu.Unlock()
	close(worker.done)
}

func (m *roomManager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		<-m.done
		return
	}
	m.closed = true
	m.viewers = 0
	workers := make([]*roomWorker, 0, len(m.workers))
	for _, worker := range m.workers {
		worker.viewers = 0
		worker.closing = true
		m.stopIdleLocked(worker)
		workers = append(workers, worker)
	}
	m.mu.Unlock()
	m.cancel()
	for _, worker := range workers {
		worker.hub.Close()
	}
	m.tasks.Wait()
	close(m.done)
}
