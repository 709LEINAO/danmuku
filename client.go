package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

type clientConfig struct {
	Endpoints    []string
	Heartbeat    time.Duration
	ReconnectMin time.Duration
	ReconnectMax time.Duration
	LoginTimeout time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// 入组后活过这么久才算一次成功连接；更短的会话按失败计入退避。
	StableSession time.Duration
	Dialer        *websocket.Dialer
	DialSlots     chan struct{}
}

func defaultClientConfig() clientConfig {
	return clientConfig{
		// 8501–8506 都能用（8507 不通）。单个端口会间歇性在 TLS 阶段静默拒握手，铺开才好换。
		Endpoints: []string{
			"wss://danmuproxy.douyu.com:8506/", "wss://danmuproxy.douyu.com:8503/",
			"wss://danmuproxy.douyu.com:8501/", "wss://danmuproxy.douyu.com:8502/",
			"wss://danmuproxy.douyu.com:8504/", "wss://danmuproxy.douyu.com:8505/",
		},
		Heartbeat: 45 * time.Second, ReconnectMin: 400 * time.Millisecond, ReconnectMax: 15 * time.Second,
		LoginTimeout: 15 * time.Second, ReadTimeout: 100 * time.Second, WriteTimeout: 8 * time.Second,
		StableSession: 30 * time.Second,
		Dialer: &websocket.Dialer{
			Proxy: http.ProxyFromEnvironment, HandshakeTimeout: 8 * time.Second,
			// 上游 TLS 1.2 需要 RSA AES-GCM；证书与主机名校验、TLS 1.3 均照常。
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, CurvePreferences: []tls.CurveID{tls.X25519, tls.CurveP256, tls.CurveP384}, CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256, tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
				tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
			}},
		},
	}
}

type clientState struct {
	Phase, Message, Endpoint string
	Attempt                  int
	LoginOK, GroupSent       bool
}

type danmakuClient struct{ config clientConfig }

func (c danmakuClient) Run(ctx context.Context, roomID string, onState func(clientState), onPacket func(map[string]string), onHeartbeat func()) {
	delay := c.config.ReconnectMin
	failedAttempts := 0
	for attempt := 1; ctx.Err() == nil; attempt++ {
		endpoint := c.config.Endpoints[(attempt-1)%len(c.config.Endpoints)]
		state := clientState{Phase: "connecting", Message: "正在连接弹幕服务", Endpoint: endpoint, Attempt: attempt}
		onState(state)
		started := time.Now()
		connected, err := c.session(ctx, roomID, state, onState, onPacket, onHeartbeat)
		if ctx.Err() != nil {
			return
		}
		// 入组后随即被掐也算失败，否则上游「登录即踢」时会以 ReconnectMin 无休止重连。
		if connected && time.Since(started) >= c.config.StableSession {
			delay = c.config.ReconnectMin
			failedAttempts = 0
		} else {
			failedAttempts++
		}
		state.Phase = "reconnecting"
		state.Message = fmt.Sprintf("连接中断，%s 后重试：%v", delay, err)
		onState(state)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		// 先短间隔轮一遍端点池；全败才说明是真连不上，这时才退避。
		if failedAttempts >= len(c.config.Endpoints) {
			delay *= 2
			if delay > c.config.ReconnectMax {
				delay = c.config.ReconnectMax
			}
		}
	}
}

func (c danmakuClient) session(ctx context.Context, roomID string, state clientState, onState func(clientState), onPacket func(map[string]string), onHeartbeat func()) (bool, error) {
	header := http.Header{"Origin": []string{"https://www.douyu.com"}, "User-Agent": []string{"Mozilla/5.0"}}
	if c.config.DialSlots != nil {
		select {
		case c.config.DialSlots <- struct{}{}:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	var stopHandshakeClose func() bool
	defer func() {
		if stopHandshakeClose != nil {
			stopHandshakeClose()
		}
	}()
	baseDialer := c.config.Dialer
	if baseDialer == nil {
		baseDialer = websocket.DefaultDialer
	}
	dialer := *baseDialer
	dialContext := dialer.NetDialContext
	if dialContext == nil {
		if dialer.NetDial != nil {
			dial := dialer.NetDial
			dialContext = func(_ context.Context, network, address string) (net.Conn, error) {
				return dial(network, address)
			}
		} else {
			dialContext = (&net.Dialer{}).DialContext
		}
	}
	closeOnCancel := func(dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
		return func(dialCtx context.Context, network, address string) (net.Conn, error) {
			conn, err := dial(dialCtx, network, address)
			if err == nil {
				// 在 Gorilla 开始 CONNECT / TLS / Upgrade 之前接管取消：GotConn 太晚。
				stopHandshakeClose = context.AfterFunc(ctx, func() { conn.Close() })
			}
			return conn, err
		}
	}
	dialer.NetDialContext = closeOnCancel(dialContext)
	if dialer.NetDialTLSContext != nil {
		dialer.NetDialTLSContext = closeOnCancel(dialer.NetDialTLSContext)
	}
	conn, resp, err := dialer.DialContext(ctx, state.Endpoint, header)
	if c.config.DialSlots != nil {
		<-c.config.DialSlots
	}
	if err != nil {
		if resp != nil {
			resp.Body.Close()
		}
		return false, err
	}
	conn.SetReadLimit(4 * maxPacketSize)
	sessionCtx, cancel := context.WithCancel(ctx)
	stopClose := context.AfterFunc(sessionCtx, func() { conn.Close() })
	if stopHandshakeClose != nil {
		// 先装上会话期的关闭钩子，再放掉握手期那个。
		stopHandshakeClose()
		stopHandshakeClose = nil
	}
	packets := make(chan map[string]string, 64)
	var readError error
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		// 关队列即发布错误，保证已收到的包先被消费完。
		defer close(packets)
		var decoder packetDecoder
		for {
			conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
			_, data, readErr := conn.ReadMessage()
			if readErr != nil {
				readError = readErr
				return
			}
			messages, decodeErr := decoder.Feed(data)
			for _, message := range messages {
				select {
				case packets <- message:
				case <-sessionCtx.Done():
					return
				}
			}
			if decodeErr != nil {
				readError = decodeErr
				return
			}
		}
	}()
	defer func() {
		cancel()
		conn.Close()
		stopClose()
		<-readerDone
	}()
	write := func(body string) error {
		conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
		return conn.WriteMessage(websocket.BinaryMessage, encodePacket(body))
	}
	if err = write(encodeSTT("type", "loginreq", "roomid", roomID)); err != nil {
		return false, err
	}
	state.Phase, state.Message = "logging_in", "已连接，正在匿名登录弹幕服务"
	onState(state)
	loginTimer := time.NewTimer(c.config.LoginTimeout)
	defer loginTimer.Stop()
	loginTimeout := loginTimer.C
	heartbeat := time.NewTicker(c.config.Heartbeat)
	defer heartbeat.Stop()
	heartbeatTick := heartbeat.C
	var writeError error
	joined := false
	beat := func() {
		if err := write(encodeSTT("type", "mrkl")); err != nil {
			// 先让读侧收尾并交出队列，再报这次写失败。
			writeError = err
			heartbeat.Stop()
			heartbeatTick = nil
			conn.Close()
			return
		}
		onHeartbeat()
	}
	for {
		select {
		case <-ctx.Done():
			return joined, ctx.Err()
		case <-loginTimeout:
			return false, errors.New("等待匿名登录响应超时")
		case <-heartbeatTick:
			if joined {
				beat()
			}
		case packet, ok := <-packets:
			if !ok {
				if err := ctx.Err(); err != nil {
					return joined, err
				}
				if writeError != nil {
					return joined, writeError
				}
				return joined, readError
			}
			onPacket(packet)
			switch packet["type"] {
			case "loginres":
				if joined {
					continue
				}
				if err := write(encodeSTT("type", "joingroup", "rid", roomID, "gid", "-9999")); err != nil {
					return false, err
				}
				joined = true
				loginTimer.Stop()
				loginTimeout = nil
				state.Phase, state.Message = "connected", "匿名登录成功，已请求接收本房间弹幕"
				state.LoginOK, state.GroupSent = true, true
				onState(state)
				beat()
			case "error":
				return joined, fmt.Errorf("弹幕服务返回错误：%s", first(packet["msg"], packet["code"], packet["id"], "未知错误"))
			}
		}
	}
}
