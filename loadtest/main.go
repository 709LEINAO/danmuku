// Command loadtest 用一个假的斗鱼弹幕上游给真实的 douyu-danmaku 灌消息，量它在本机的承压能力：
// 拉起一份服务进程（DANMAKU_UPSTREAM 指向假上游），开 N 个 SSE 观看者，按梯度提高每秒消息数，
// 记录送达率、被踢重连次数、端到端延迟分位与服务进程的 CPU／内存。
//
//	./danmaku-loadtest -binary ./douyu-danmaku-linux-amd64 -room 231059 -viewers 20 -rates 100,300,1000,3000 -seconds 20
//
// 房间号仍由服务进程向斗鱼解析（需要外网），只有弹幕连接被换成假上游；线上那份服务不受影响。
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type options struct {
	binary     string
	room       string
	viewers    int
	rates      []float64
	seconds    int
	settle     int
	giftEvery  int
	enterEvery int
	out        string
	serverLog  string
}

func parseOptions() (options, error) {
	var opts options
	var rates string
	flag.StringVar(&opts.binary, "binary", "", "douyu-danmaku 可执行文件路径（必填）")
	flag.StringVar(&opts.room, "room", "231059", "订阅的房间号，由服务进程向斗鱼解析")
	flag.IntVar(&opts.viewers, "viewers", 20, "同时打开的 SSE 观看者数")
	flag.StringVar(&rates, "rates", "100,300,1000,3000", "各阶段每秒消息数，逗号分隔")
	flag.IntVar(&opts.seconds, "seconds", 20, "每阶段持续秒数")
	flag.IntVar(&opts.settle, "settle", 3, "阶段结束后停发、等在途消息送完的秒数")
	flag.IntVar(&opts.giftEvery, "gift-every", 20, "每多少条消息夹一条礼物（0 关闭）")
	flag.IntVar(&opts.enterEvery, "enter-every", 50, "每多少条消息夹一条贵族进场（0 关闭）")
	flag.StringVar(&opts.out, "out", "loadtest-result.json", "结果 JSON 输出路径")
	flag.StringVar(&opts.serverLog, "server-log", "loadtest-server.log", "被测服务进程的日志输出路径")
	flag.Parse()
	if opts.binary == "" {
		return opts, errors.New("-binary 必填")
	}
	if opts.viewers < 1 || opts.seconds < 1 || opts.settle < 0 {
		return opts, errors.New("-viewers、-seconds 须为正数，-settle 不能为负")
	}
	for _, item := range strings.Split(rates, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		rate, err := strconv.ParseFloat(item, 64)
		if err != nil || rate <= 0 {
			return opts, fmt.Errorf("-rates 含无效值 %q", item)
		}
		opts.rates = append(opts.rates, rate)
	}
	if len(opts.rates) == 0 {
		return opts, errors.New("-rates 至少一个阶段")
	}
	return opts, nil
}

// --- 斗鱼 STT 协议（与服务端 codec.go 同形，工具不能导入 main 包，只能照抄这几十行） ---

var sttEscape = strings.NewReplacer("@", "@A", "/", "@S")
var sttUnescape = strings.NewReplacer("@S", "/", "@A", "@")

func encodeSTT(fields ...string) string {
	var b strings.Builder
	for i := 0; i+1 < len(fields); i += 2 {
		b.WriteString(sttEscape.Replace(fields[i]))
		b.WriteString("@=")
		b.WriteString(sttEscape.Replace(fields[i+1]))
		b.WriteByte('/')
	}
	return b.String()
}

func decodeSTT(body string) map[string]string {
	fields := make(map[string]string)
	for _, field := range strings.Split(body, "/") {
		if key, value, ok := strings.Cut(field, "@="); ok {
			fields[sttUnescape.Replace(key)] = sttUnescape.Replace(value)
		}
	}
	return fields
}

// 服务端→客户端的包类型是 690。
func framePacket(buf *bytes.Buffer, body string) {
	var header [12]byte
	length := uint32(len(body) + 9)
	binary.LittleEndian.PutUint32(header[0:4], length)
	binary.LittleEndian.PutUint32(header[4:8], length)
	binary.LittleEndian.PutUint16(header[8:10], 690)
	buf.Write(header[:])
	buf.WriteString(body)
	buf.WriteByte(0)
}

// 服务进程每条 WS 消息只装一个包（loginreq / joingroup / mrkl）。
func parseClientPacket(data []byte) map[string]string {
	if len(data) < 13 {
		return nil
	}
	return decodeSTT(string(data[12 : len(data)-1]))
}

// --- 假上游 ---

type upstream struct {
	rate       atomic.Int64 // 每秒包数
	seq        atomic.Int64
	sent       atomic.Int64
	sessions   atomic.Int64 // 服务进程连过来的次数；>1 说明它的 WS 会话断过
	giftEvery  int
	enterEvery int
	joined     chan struct{}
	joinOnce   sync.Once
}

var upgrader = websocket.Upgrader{
	CheckOrigin:     func(*http.Request) bool { return true },
	ReadBufferSize:  64 << 10,
	WriteBufferSize: 1 << 20,
}

type wsWriter struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

func (w *wsWriter) write(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return w.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (u *upstream) serve(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	u.sessions.Add(1)
	writer := &wsWriter{conn: conn}
	done := make(chan struct{})
	defer close(done)
	room := ""
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		packet := parseClientPacket(data)
		switch packet["type"] {
		case "loginreq":
			room = packet["roomid"]
			var buf bytes.Buffer
			framePacket(&buf, encodeSTT("type", "loginres", "userid", "0", "roomgroup", "0", "pg", "1", "sessionid", "0",
				"username", "", "nickname", "", "live_stat", "1", "is_illegal", "0", "ill_ct", "", "ill_ts", "0", "now", "0",
				"ps", "0", "es", "0", "it", "0", "its", "0", "npv", "0", "best_dlev", "0", "cur_lev", "0", "nrc", "0", "ih", "0"))
			if err := writer.write(buf.Bytes()); err != nil {
				return
			}
		case "joingroup":
			u.joinOnce.Do(func() { close(u.joined) })
			go u.flood(writer, room, done)
		}
	}
}

var giftSamples = [...][2]string{{"20004", "火箭"}, {"20002", "办卡"}, {"23625", "至尊飞机"}}

// 字段数与长度照着线上 chatmsg（约 40 个字段、600 字节）配，服务端解码、建档、投影的工作量才对得上。
func (u *upstream) packet(buf *bytes.Buffer, room string, seq int64, stamp int64) {
	user := seq % 5000
	uid := strconv.FormatInt(100000000+user, 10)
	nick := "压测观众" + strconv.FormatInt(user, 10)
	level := strconv.FormatInt(user%120+1, 10)
	switch {
	case u.giftEvery > 0 && seq%int64(u.giftEvery) == 0:
		gift := giftSamples[int(seq/int64(u.giftEvery))%len(giftSamples)]
		framePacket(buf, encodeSTT("type", "dgb", "rid", room, "gfid", gift[0], "gfn", gift[1], "gs", "1", "uid", uid, "nn", nick,
			"level", level, "dw", "0", "gfcnt", "1", "hits", strconv.FormatInt(seq%50+1, 10), "bnn", "压测牌", "bl", "12", "brid", room,
			"ct", "1", "pid", "0", "cid", "", "eid", "0", "ifs", "0", "gt", "", "ce", "", "rpid", "0", "slt", "0", "elt", "0", "nl", "0"))
	case u.enterEvery > 0 && seq%int64(u.enterEvery) == 0:
		framePacket(buf, encodeSTT("type", "uenter", "rid", room, "uid", uid, "nn", nick, "level", level,
			"ic", "avatar_v3/202301/abcdef0123456789", "rni", "0", "el", "", "sahf", "0", "nl", strconv.FormatInt(user%9+1, 10),
			"ceid", "0", "crw", "0", "ol", "0", "wgei", "0"))
	default:
		framePacket(buf, encodeSTT("type", "chatmsg", "rid", room, "ct", "1", "uid", uid, "nn", nick,
			"txt", strconv.FormatInt(seq, 10)+"|"+strconv.FormatInt(stamp, 10),
			"cid", "0123456789abcdef0123456789abcdef", "ic", "avatar_v3/202301/abcdef0123456789", "level", level, "sahf", "0",
			"cst", strconv.FormatInt(stamp/1e6, 10), "bnn", "压测牌", "bl", strconv.FormatInt(user%30+1, 10), "brid", room,
			"hc", "fedcba9876543210fedcba9876543210", "ol", "0", "rev", "0", "hl", "0", "ifs", "0", "el", "", "lk", "", "dms", "5",
			"pdg", "25", "pdk", "21", "ext", "", "col", strconv.FormatInt(user%7, 10), "nl", "0", "pg", "1", "rg", "1", "urlev", "1"))
	}
}

// 每 10 ms 一拍，按当前速率补足欠发的包数，同一拍的包合成一帧（服务端解码器按长度拆）。
func (u *upstream) flood(writer *wsWriter, room string, done <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	owed, last := 0.0, time.Now()
	var buf bytes.Buffer
	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			owed += float64(u.rate.Load()) * now.Sub(last).Seconds()
			last = now
			n := int(owed)
			if n <= 0 {
				continue
			}
			// 单帧上限：服务端读限 4 MiB，600 字节一包，1000 包不到 1 MiB。
			if n > 1000 {
				n = 1000
			}
			owed -= float64(n)
			buf.Reset()
			stamp := time.Now().UnixNano()
			for i := 0; i < n; i++ {
				u.packet(&buf, room, u.seq.Add(1), stamp)
			}
			if err := writer.write(buf.Bytes()); err != nil {
				return
			}
			u.sent.Add(int64(n))
		}
	}
}

// --- 观看者 ---

// 0.1 ms 一格，最后一格收 ≥ 20 s 的；分位取格子上界，偏保守。
const (
	histBuckets = 200001
	histBucket  = 100 * time.Microsecond
)

type stage struct {
	chat, gift, enter, other atomic.Int64
	dupes, gaps, resets      atomic.Int64
	reconnects, bytes        atomic.Int64
	maxLatency               atomic.Int64
	hist                     [histBuckets]atomic.Int64
}

func (s *stage) observe(latency time.Duration) {
	bucket := int(latency / histBucket)
	if bucket < 0 {
		bucket = 0
	}
	if bucket >= histBuckets {
		bucket = histBuckets - 1
	}
	s.hist[bucket].Add(1)
	for {
		current := s.maxLatency.Load()
		if int64(latency) <= current || s.maxLatency.CompareAndSwap(current, int64(latency)) {
			return
		}
	}
}

func (s *stage) percentile(p float64) time.Duration {
	var total int64
	for i := range s.hist {
		total += s.hist[i].Load()
	}
	if total == 0 {
		return 0
	}
	target, cum := int64(math.Ceil(p*float64(total))), int64(0)
	for i := range s.hist {
		if cum += s.hist[i].Load(); cum >= target {
			return time.Duration(i+1) * histBucket
		}
	}
	return time.Duration(histBuckets) * histBucket
}

type viewer struct {
	id      int
	last    map[string]int64
	current *atomic.Pointer[stage]
	notice  func(string)
}

func jsonField(data []byte, key string) []byte {
	marker := []byte(`"` + key + `":`)
	start := bytes.Index(data, marker)
	if start < 0 {
		return nil
	}
	rest := data[start+len(marker):]
	if len(rest) > 0 && rest[0] == '"' {
		end := bytes.IndexByte(rest[1:], '"')
		if end < 0 {
			return nil
		}
		return rest[1 : 1+end]
	}
	end := bytes.IndexAny(rest, ",}")
	if end < 0 {
		return nil
	}
	return rest[:end]
}

func (v *viewer) handle(event string, data []byte, now time.Time) {
	st := v.current.Load()
	st.bytes.Add(int64(len(data)))
	switch event {
	case "message":
	case "reset":
		st.resets.Add(1)
		return
	case "subscription-error", "auth-expired":
		v.notice(fmt.Sprintf("观看者 %d 收到 %s：%s", v.id, event, data))
		return
	default:
		return
	}
	kind := string(jsonField(data, "kind"))
	index, _ := strconv.ParseInt(string(jsonField(data, "index")), 10, 64)
	if last := v.last[kind]; index <= last {
		st.dupes.Add(1)
		return
	} else {
		if last > 0 && index > last+1 {
			st.gaps.Add(index - last - 1)
		}
		v.last[kind] = index
	}
	switch kind {
	case "chat":
		st.chat.Add(1)
		if _, stamp, ok := bytes.Cut(jsonField(data, "text"), []byte("|")); ok {
			if sent, err := strconv.ParseInt(string(stamp), 10, 64); err == nil {
				st.observe(now.Sub(time.Unix(0, sent)))
			}
		}
	case "gift":
		st.gift.Add(1)
	case "enter":
		st.enter.Add(1)
	default:
		st.other.Add(1)
	}
}

func (v *viewer) stream(ctx context.Context, client *http.Client, base, cookie, room string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/events?room="+url.QueryEscape(room), nil)
	if err != nil {
		return err
	}
	request.Header.Set("Cookie", cookie)
	request.Header.Set("Accept", "text/event-stream")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return fmt.Errorf("HTTP %d：%s", response.StatusCode, bytes.TrimSpace(body))
	}
	reader := bufio.NewReaderSize(response.Body, 256<<10)
	event, data := "", make([]byte, 0, 4096)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		line = bytes.TrimRight(line, "\r\n")
		switch {
		case len(line) == 0:
			if len(data) > 0 {
				v.handle(event, data, time.Now())
			}
			event, data = "", data[:0]
		case bytes.HasPrefix(line, []byte("event:")):
			event = string(bytes.TrimSpace(line[6:]))
		case bytes.HasPrefix(line, []byte("data:")):
			data = append(data, bytes.TrimSpace(line[5:])...)
		}
	}
}

// 掉线就像浏览器 EventSource 那样隔 1 秒重连；服务端会回放 300 条历史，计入 dupes。
func (v *viewer) run(ctx context.Context, client *http.Client, base, cookie, room string) {
	for ctx.Err() == nil {
		err := v.stream(ctx, client, base, cookie, room)
		if ctx.Err() != nil {
			return
		}
		v.current.Load().reconnects.Add(1)
		v.notice(fmt.Sprintf("观看者 %d 断开：%v", v.id, err))
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// --- 被测服务进程 ---

type serverProbe struct {
	at       time.Time
	packets  int64
	events   map[string]int64
	cpu      float64 // 进程累计 CPU 秒
	rssKB    int64
	threads  int
	procOK   bool
	statusOK bool
}

func probeServer(client *http.Client, base, cookie, room string, pid int) serverProbe {
	probe := serverProbe{at: time.Now(), events: map[string]int64{}}
	probe.cpu, probe.rssKB, probe.threads, probe.procOK = sampleProcess(pid)
	request, _ := http.NewRequest(http.MethodGet, base+"/api/status?rid="+url.QueryEscape(room), nil)
	request.Header.Set("Cookie", cookie)
	response, err := client.Do(request)
	if err != nil {
		return probe
	}
	defer response.Body.Close()
	var envelope struct {
		Data struct {
			Packets int64            `json:"packets"`
			Events  map[string]int64 `json:"events"`
		} `json:"data"`
	}
	if response.StatusCode == http.StatusOK && json.NewDecoder(response.Body).Decode(&envelope) == nil {
		probe.packets, probe.events, probe.statusOK = envelope.Data.Packets, envelope.Data.Events, true
		if probe.events == nil {
			probe.events = map[string]int64{}
		}
	}
	return probe
}

func freePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func waitReady(client *http.Client, base string, cmd *exec.Cmd, exited <-chan error) error {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-exited:
			return fmt.Errorf("服务进程提前退出：%v", err)
		default:
		}
		response, err := client.Get(base + "/login")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return errors.New("服务进程 20 秒内没有开始监听")
}

func login(client *http.Client, base, password string) (string, error) {
	form := url.Values{"password": {password}, "next": {"/"}}
	request, err := http.NewRequest(http.MethodPost, base+"/login", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Origin", base)
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	for _, cookie := range response.Cookies() {
		if cookie.Name == "danmaku_session" && response.StatusCode == http.StatusSeeOther {
			return cookie.Name + "=" + cookie.Value, nil
		}
	}
	return "", fmt.Errorf("登录失败：HTTP %d", response.StatusCode)
}

// --- 结果 ---

type stageResult struct {
	TargetRate     float64 `json:"targetRate"`
	SentRate       float64 `json:"upstreamSentRate"`
	PacketRate     float64 `json:"serverPacketRate"`
	ChatEvents     int64   `json:"serverChatEvents"`
	GiftEvents     int64   `json:"serverGiftEvents"`
	EnterEvents    int64   `json:"serverEnterEvents"`
	DeliveredChat  int64   `json:"deliveredChatTotal"`
	DeliveredGift  int64   `json:"deliveredGiftTotal"`
	DeliveredEnter int64   `json:"deliveredEnterTotal"`
	Delivery       float64 `json:"chatDeliveryRatio"`
	Dupes          int64   `json:"duplicates"`
	Gaps           int64   `json:"gaps"`
	Reconnects     int64   `json:"viewerReconnects"`
	Resets         int64   `json:"resets"`
	BytesPerViewer float64 `json:"bytesPerViewerPerSecond"`
	P50Ms          float64 `json:"p50Ms"`
	P95Ms          float64 `json:"p95Ms"`
	P99Ms          float64 `json:"p99Ms"`
	MaxMs          float64 `json:"maxMs"`
	CPUPercent     float64 `json:"serverCpuPercent"`
	RSSMiB         float64 `json:"serverRssMiB"`
	Threads        int     `json:"serverThreads"`
	ToolCPUPercent float64 `json:"toolCpuPercent"`
	UpstreamDrops  int64   `json:"upstreamSessionChanges"`
}

type report struct {
	StartedAt string        `json:"startedAt"`
	Host      hostInfo      `json:"host"`
	Binary    string        `json:"binary"`
	Room      string        `json:"room"`
	Viewers   int           `json:"viewers"`
	Seconds   int           `json:"stageSeconds"`
	Settle    int           `json:"settleSeconds"`
	Stages    []stageResult `json:"stages"`
	Notices   []string      `json:"notices"`
	Verdict   string        `json:"verdict"`
}

func ms(d time.Duration) float64 { return math.Round(float64(d)/float64(time.Millisecond)*10) / 10 }

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "压测失败：", err)
		os.Exit(1)
	}
}

func run() error {
	opts, err := parseOptions()
	if err != nil {
		return err
	}
	binary, err := filepath.Abs(opts.binary)
	if err != nil {
		return err
	}
	if _, err := os.Stat(binary); err != nil {
		return err
	}
	var noticesMu sync.Mutex
	var notices []string
	notice := func(text string) {
		noticesMu.Lock()
		defer noticesMu.Unlock()
		if len(notices) < 200 {
			notices = append(notices, time.Now().Format("15:04:05.000")+" "+text)
		}
		if len(notices) <= 20 {
			fmt.Fprintln(os.Stderr, "  ·", text)
		}
	}

	// 假上游
	up := &upstream{giftEvery: opts.giftEvery, enterEvery: opts.enterEvery, joined: make(chan struct{})}
	upListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	upServer := &http.Server{Handler: http.HandlerFunc(up.serve)}
	go upServer.Serve(upListener)
	defer upServer.Close()
	upstreamURL := "ws://" + upListener.Addr().String() + "/"

	// 被测服务进程
	port, err := freePort()
	if err != nil {
		return err
	}
	var secret [16]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return err
	}
	password := hex.EncodeToString(secret[:])
	base := "http://127.0.0.1:" + strconv.Itoa(port)
	logFile, err := os.Create(opts.serverLog)
	if err != nil {
		return err
	}
	defer logFile.Close()
	cmd := exec.Command(binary, "-addr", "127.0.0.1:"+strconv.Itoa(port), "-max-viewers", strconv.Itoa(opts.viewers+8), "-room-idle-grace", "1m")
	cmd.Dir = filepath.Dir(binary)
	cmd.Env = append(os.Environ(), "DANMAKU_PASSWORD="+password, "DANMAKU_UPSTREAM="+upstreamURL)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("启动服务进程：%w", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	defer func() {
		terminate(cmd)
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			cmd.Process.Kill()
		}
	}()

	transport := &http.Transport{MaxIdleConnsPerHost: 16, ResponseHeaderTimeout: 15 * time.Second, DisableCompression: true}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if err := waitReady(client, base, cmd, exited); err != nil {
		return err
	}
	cookie, err := login(client, base, password)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "服务进程 PID %d 已就绪（%s），假上游 %s\n", cmd.Process.Pid, base, upstreamURL)

	// 观看者
	current := &atomic.Pointer[stage]{}
	current.Store(new(stage))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < opts.viewers; i++ {
		v := &viewer{id: i + 1, last: map[string]int64{}, current: current, notice: notice}
		wg.Add(1)
		go func() {
			defer wg.Done()
			v.run(ctx, client, base, cookie, opts.room)
		}()
	}
	defer wg.Wait()
	defer cancel()
	fmt.Fprintf(os.Stderr, "等待服务进程解析房间 %s 并连上假上游…\n", opts.room)
	select {
	case <-up.joined:
	case err := <-exited:
		return fmt.Errorf("服务进程退出：%v（见 %s）", err, opts.serverLog)
	case <-time.After(90 * time.Second):
		return fmt.Errorf("90 秒内没有连上假上游，多半是房间解析失败（见 %s）", opts.serverLog)
	}
	time.Sleep(2 * time.Second)

	rep := report{StartedAt: time.Now().Format(time.RFC3339), Host: describeHost(), Binary: binary, Room: opts.room, Viewers: opts.viewers, Seconds: opts.seconds, Settle: opts.settle}
	fmt.Fprintf(os.Stderr, "主机：%s\n", rep.Host.Summary())
	fmt.Printf("%8s %8s %8s %8s %7s %5s %7s %7s %7s %7s %6s %7s %4s\n", "目标/s", "实发/s", "入包/s", "chat/s", "送达率", "被踢", "p50ms", "p95ms", "p99ms", "maxms", "CPU%", "RSSMiB", "线程")
	for _, rate := range opts.rates {
		st := new(stage)
		current.Store(st)
		sentBefore, sessionsBefore := up.sent.Load(), up.sessions.Load()
		toolBefore := selfCPU()
		before := probeServer(client, base, cookie, opts.room, cmd.Process.Pid)
		up.rate.Store(int64(rate))
		time.Sleep(time.Duration(opts.seconds) * time.Second)
		up.rate.Store(0)
		loaded := probeServer(client, base, cookie, opts.room, cmd.Process.Pid)
		toolAfter := selfCPU()
		time.Sleep(time.Duration(opts.settle) * time.Second)
		after := probeServer(client, base, cookie, opts.room, cmd.Process.Pid)
		seconds := loaded.at.Sub(before.at).Seconds()
		result := stageResult{
			TargetRate:     rate,
			SentRate:       float64(up.sent.Load()-sentBefore) / seconds,
			PacketRate:     float64(loaded.packets-before.packets) / seconds,
			ChatEvents:     after.events["chat"] - before.events["chat"],
			GiftEvents:     after.events["gift"] - before.events["gift"],
			EnterEvents:    after.events["enter"] - before.events["enter"],
			DeliveredChat:  st.chat.Load(),
			DeliveredGift:  st.gift.Load(),
			DeliveredEnter: st.enter.Load(),
			Dupes:          st.dupes.Load(),
			Gaps:           st.gaps.Load(),
			Reconnects:     st.reconnects.Load(),
			Resets:         st.resets.Load(),
			BytesPerViewer: float64(st.bytes.Load()) / float64(opts.viewers) / seconds,
			P50Ms:          ms(st.percentile(0.50)),
			P95Ms:          ms(st.percentile(0.95)),
			P99Ms:          ms(st.percentile(0.99)),
			MaxMs:          ms(time.Duration(st.maxLatency.Load())),
			Threads:        loaded.threads,
			UpstreamDrops:  up.sessions.Load() - sessionsBefore,
		}
		if result.ChatEvents > 0 {
			result.Delivery = float64(result.DeliveredChat) / float64(result.ChatEvents*int64(opts.viewers))
		}
		cpu, rss, threads := "n/a", "n/a", "n/a"
		if before.procOK && loaded.procOK {
			result.CPUPercent = math.Round((loaded.cpu-before.cpu)/seconds*1000) / 10
			result.RSSMiB = math.Round(float64(loaded.rssKB)/1024*10) / 10
			cpu, rss, threads = strconv.FormatFloat(result.CPUPercent, 'f', 1, 64), strconv.FormatFloat(result.RSSMiB, 'f', 1, 64), strconv.Itoa(result.Threads)
		}
		if toolBefore >= 0 && toolAfter >= 0 {
			result.ToolCPUPercent = math.Round((toolAfter-toolBefore)/seconds*1000) / 10
		}
		rep.Stages = append(rep.Stages, result)
		fmt.Printf("%8.0f %8.0f %8.0f %8.0f %6.2f%% %5d %7.1f %7.1f %7.1f %7.1f %6s %7s %4s\n",
			rate, result.SentRate, result.PacketRate, float64(result.ChatEvents)/seconds, result.Delivery*100, result.Reconnects,
			result.P50Ms, result.P95Ms, result.P99Ms, result.MaxMs, cpu, rss, threads)
		if result.UpstreamDrops > 0 {
			notice(fmt.Sprintf("阶段 %.0f/s：服务进程的上游 WS 会话重连了 %d 次", rate, result.UpstreamDrops))
		}
	}
	rep.Verdict = verdict(rep.Stages)
	noticesMu.Lock()
	rep.Notices = append([]string(nil), notices...)
	noticesMu.Unlock()
	fmt.Println(rep.Verdict)
	data, _ := json.MarshalIndent(rep, "", "  ")
	if err := os.WriteFile(opts.out, data, 0o644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "结果已写入 %s，服务进程日志在 %s\n", opts.out, opts.serverLog)
	return nil
}

// 「稳」的门槛：没人被踢、chat 全部送达（≥ 99.9%）、p99 一秒以内。
func verdict(stages []stageResult) string {
	best := -1.0
	for _, stage := range stages {
		if stage.Reconnects == 0 && stage.Gaps == 0 && stage.Delivery >= 0.999 && stage.P99Ms <= 1000 && stage.TargetRate > best {
			best = stage.TargetRate
		}
	}
	if best < 0 {
		return "结论：没有一个阶段达到「无人被踢、送达 ≥ 99.9%、p99 ≤ 1 s」，从最低档就已过载。"
	}
	return fmt.Sprintf("结论：稳定承载到 %.0f 条/秒（无人被踢、送达 ≥ 99.9%%、p99 ≤ 1 s）。", best)
}
