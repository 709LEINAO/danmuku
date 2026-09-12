package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func chatFields(room, text string) map[string]string {
	return map[string]string{"type": "chatmsg", "rid": room, "uid": "42", "nn": "阿宝", "txt": text}
}

func frameBody(t *testing.T, frame []byte, name string) []byte {
	t.Helper()
	prefix := []byte("event: " + name + "\ndata: ")
	if !bytes.HasPrefix(frame, prefix) {
		t.Fatalf("帧前缀不是 %q：%q", name, frame)
	}
	if !bytes.HasSuffix(frame, []byte("\n\n")) {
		t.Fatalf("帧未以空行结束：%q", frame)
	}
	return bytes.TrimSuffix(bytes.TrimPrefix(frame, prefix), []byte("\n\n"))
}

func TestFrameEnvelope(t *testing.T) {
	hub := newHub("231059", "abc-1")
	body := frameBody(t, hub.frame("message", Event{Kind: "chat", Text: "喂"}), "message")
	var envelope struct {
		RoomID     string          `json:"roomId"`
		Generation string          `json:"generation"`
		Data       json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("信封无法解析：%v", err)
	}
	if envelope.RoomID != "231059" || envelope.Generation != "abc-1" {
		t.Fatalf("信封 = %q / %q", envelope.RoomID, envelope.Generation)
	}
}

// 帧体里出现裸换行会把一条 SSE 记录切成两条，正文换行必须留在 JSON 转义里。
func TestFrameKeepsNewlinesEscaped(t *testing.T) {
	hub := newHub("1", "g")
	hub.Packet(chatFields("1", "第一行\n第二行"), "1")
	if len(hub.history) != 1 {
		t.Fatalf("历史 = %d 条，期望 1", len(hub.history))
	}
	// event 行 1 个 + data 行 1 个 + 结尾空行 1 个。
	if count := bytes.Count(hub.history[0], []byte("\n")); count != 3 {
		t.Fatalf("帧含 %d 个换行，期望 3：%q", count, hub.history[0])
	}
}

// 这次改动的核心：一条消息全房间只编码一次，订阅者与历史共享同一份字节。
func TestBroadcastEncodesOnce(t *testing.T) {
	hub := newHub("1", "g")
	_, _, first := hub.Subscribe()
	_, _, second := hub.Subscribe()
	hub.Packet(chatFields("1", "喂"), "1")
	a, b := <-first, <-second
	if len(a) == 0 || &a[0] != &b[0] {
		t.Fatal("两个订阅者拿到的不是同一份编码")
	}
	if len(hub.history) != 1 || &hub.history[0][0] != &a[0] {
		t.Fatal("历史里的帧与广播出去的不是同一份编码")
	}
}

func TestSubscribeReplaysHistory(t *testing.T) {
	hub := newHub("1", "g")
	hub.Packet(chatFields("1", "一"), "1")
	hub.Packet(chatFields("1", "二"), "1")
	statusFrame, history, _ := hub.Subscribe()
	frameBody(t, statusFrame, "status")
	if len(history) != 2 {
		t.Fatalf("回放 = %d 条，期望 2", len(history))
	}
	for index, want := range []string{`"一"`, `"二"`} {
		if !bytes.Contains(history[index], []byte(want)) {
			t.Errorf("回放第 %d 条不含 %s：%q", index, want, history[index])
		}
	}
}

// status 帧的白名单是显式的：往 Status 上加字段不该悄悄挤进这条广播。
func TestStatusFrameOmitsDiagnostics(t *testing.T) {
	hub := newHub("1", "g")
	hub.Packet(chatFields("1", "喂"), "1")
	var envelope struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(frameBody(t, hub.StatusFrame(), "status"), &envelope); err != nil {
		t.Fatalf("status 信封无法解析：%v", err)
	}
	for _, key := range []string{"types", "events", "packets", "updatedAt"} {
		if _, present := envelope.Data[key]; present {
			t.Errorf("status 帧不该含诊断字段 %q", key)
		}
	}
	for _, key := range []string{"phase", "message", "room", "heartbeats"} {
		if _, present := envelope.Data[key]; !present {
			t.Errorf("status 帧缺少页面要读的 %q", key)
		}
	}
}

// /api/status 仍是全量出口，不跟着 SSE 一起瘦身。
func TestSnapshotKeepsDiagnostics(t *testing.T) {
	hub := newHub("1", "g")
	hub.Packet(chatFields("1", "喂"), "1")
	status := hub.Snapshot()
	if status.Packets != 1 || status.Types["chatmsg"] != 1 || status.Events["chat"] != 1 {
		t.Fatalf("诊断计数 = %+v", status)
	}
}

func diamondFields(room string, extra map[string]string) map[string]string {
	fields := map[string]string{"type": "dfobc", "rid": room, "uid": "42", "nick": "阿宝", "mn": "1"}
	for key, value := range extra {
		fields[key] = value
	}
	return fields
}

// 四种广播只差动作词，字段一致；昵称在 nick 而不是 nn。
func TestDiamondFanKinds(t *testing.T) {
	for _, item := range []struct {
		packet  string
		renew   bool
		viaGift bool
	}{
		{"dfobc", false, false},
		{"dfrbc", true, false},
		{"odfpbc", false, true},
		{"rdfpbc", true, true},
	} {
		event, ok := normalizeEvent(diamondFields("1", map[string]string{"type": item.packet, "mn": "3"}), "1", nil)
		if !ok {
			t.Fatalf("%s 被丢弃", item.packet)
		}
		if event.Kind != "diamond" || event.User != "阿宝" {
			t.Errorf("%s → kind=%q user=%q", item.packet, event.Kind, event.User)
		}
		fan := event.DiamondFan
		if fan == nil {
			t.Fatalf("%s 没带 diamondFan", item.packet)
		}
		if fan.Months != 3 || fan.Renew != item.renew || fan.ViaGift != item.viaGift {
			t.Errorf("%s → %+v", item.packet, *fan)
		}
	}
}

// rrid 是钻粉牌归属的主播房间，跟广播落在哪个房间是两回事：在本房间买别家主播
// 的钻粉，这边也收得到，但不该显示。rrid 缺席按本房间算。
func TestDiamondFanKeepsOnlyThisRoom(t *testing.T) {
	if _, ok := normalizeEvent(diamondFields("1", map[string]string{"rrid": "999", "rnick": "别家主播"}), "1", nil); ok {
		t.Error("开给别家主播的钻粉没有被丢弃")
	}
	for _, rrid := range []string{"", "1"} {
		event, ok := normalizeEvent(diamondFields("1", map[string]string{"rrid": rrid, "cdays": "7"}), "1", nil)
		if !ok {
			t.Fatalf("rrid=%q 的本房间开通被丢弃", rrid)
		}
		if event.DiamondFan.BonusDays != 7 {
			t.Errorf("rrid=%q → %+v", rrid, *event.DiamondFan)
		}
	}
}

// 月数缺失照样播报：「谁开通了钻粉」本身就是要看的那句。
func TestDiamondFanKeepsEventWithoutMonths(t *testing.T) {
	event, ok := normalizeEvent(diamondFields("1", map[string]string{"mn": ""}), "1", nil)
	if !ok || event.DiamondFan == nil || event.DiamondFan.Months != 0 {
		t.Fatalf("缺月数时 ok=%v event=%+v", ok, event.DiamondFan)
	}
}

// 桶满时正好放行三条，与页面同时并存三行对得上；之后按间隔滴。
func TestDiamondBurstMatchesPinRows(t *testing.T) {
	hub := newHub("1", "g")
	now := time.Now()
	passed := 0
	for range 10 {
		if hub.allowEventLocked("diamond", now) {
			passed++
		}
	}
	if passed != int(diamondBurst) {
		t.Fatalf("满桶放行 %d 条，期望 %v", passed, diamondBurst)
	}
	if hub.allowEventLocked("diamond", now.Add(diamondInterval)) != true {
		t.Fatal("过了一个间隔仍不放行")
	}
	// 两只桶互不影响。
	if !hub.allowEventLocked("enter", now) {
		t.Fatal("钻粉刷爆桶后连带挡住了进场")
	}
	// 其余类型不过桶。
	for range 100 {
		if !hub.allowEventLocked("chat", now) {
			t.Fatal("聊天被限流了")
		}
	}
}

func TestDiamondPacketReachesSubscriber(t *testing.T) {
	hub := newHub("1", "g")
	_, _, events := hub.Subscribe()
	hub.Packet(diamondFields("1", map[string]string{"mn": "12"}), "1")
	frame := <-events
	body := frameBody(t, frame, "message")
	for _, want := range []string{`"kind":"diamond"`, `"months":12`, `"user":"阿宝"`} {
		if !bytes.Contains(body, []byte(want)) {
			t.Errorf("帧缺少 %s：%s", want, body)
		}
	}
}

// 道具表把免费道具和真付费礼物混在一起，只有名单里那几种撤掉 Prop。
func TestPaidPropsAreNotMarkedProp(t *testing.T) {
	payload := `DYConfigCallback({"error":0,"data":{
		"192":{"name":"赞","pc":10,"devote":1},
		"1757":{"name":"办卡","pc":600,"devote":60},
		"21668":{"name":"钻粉卡","pc":21800,"devote":2180},
		"24108":{"name":"钻粉飞机","pc":10000,"devote":1000},
		"99001":{"name":"钻粉卡","pc":50,"devote":5}
	}})`
	catalog, err := parsePropCatalog(strings.NewReader(payload))
	if err != nil {
		t.Fatalf("解析道具表失败：%v", err)
	}
	for id, wantProp := range map[string]bool{
		"192":   true,  // 免费道具，维持不计营收
		"1757":  false, // ¥6 的办卡，真付费
		"21668": false, // ¥218 的钻粉卡，正是这次要救回来的
		"24108": false,
		"99001": true, // 同名但低于 ¥1 下限，按赠品挡回去
	} {
		if got := catalog[id].Prop; got != wantProp {
			t.Errorf("%s（%s）Prop=%v，期望 %v", id, catalog[id].Name, got, wantProp)
		}
	}
	// 撤掉 Prop 之后才谈得上计价。
	if reference := catalog.reference("21668", "1"); reference == nil || reference.Prop || reference.NominalAmount != "218.00" {
		t.Fatalf("钻粉卡参考价 = %+v", reference)
	}
}

func benchEvent() Event {
	return Event{
		Kind: "chat", Index: 1, Source: "chatmsg", At: "2026-09-12T19:00:00.123456+08:00",
		RoomID: "231059", UserID: "12345678", User: "某位观众",
		Text:     "这条弹幕大概就是现场常见的长度，再带上几个字凑够",
		UserMeta: &UserMetadata{FanBadge: &FanBadge{Name: "阿宝", Level: 21, RoomID: "231059"}},
		Fields: map[string]string{
			"type": "chatmsg", "rid": "231059", "uid": "12345678", "nn": "某位观众",
			"txt": "这条弹幕大概就是现场常见的长度，再带上几个字凑够", "col": "2", "ct": "0",
			"level": "35", "bnn": "阿宝", "bl": "21", "brid": "231059", "nail": "3721_231059",
		},
	}
}

// 一条消息扇出给 100 个观看者的编码成本。Shared 是现在的做法，PerSubscriber 是
// 改动前的做法（每个 stream goroutine 各自 Marshal 一遍），留作护栏：
// 实测 3.6 µs/2.8 KB vs 334 µs/242 KB，差 92 倍。1000 条/秒下后者光序列化就要
// 334 ms/秒 CPU 加 242 MB/秒 分配，正是 100 观看者压测送达率跌到 56 % 的原因。
const benchViewers = 100

func BenchmarkFanoutShared(b *testing.B) {
	hub := newHub("231059", "g")
	event := benchEvent()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		frame := hub.frame("message", event)
		for range benchViewers {
			_ = frame
		}
	}
}

func BenchmarkFanoutPerSubscriber(b *testing.B) {
	roomID, generation := "231059", "g"
	event := benchEvent()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for range benchViewers {
			json.Marshal(roomEnvelope{RoomID: &roomID, Generation: &generation, Data: event})
		}
	}
}

func TestSlowSubscriberDropped(t *testing.T) {
	hub := newHub("1", "g")
	_, _, slow := hub.Subscribe()
	for range subscriberQueue + 1 {
		hub.Packet(chatFields("1", "刷"), "1")
	}
	if hub.IsSubscribed(slow) {
		t.Fatal("队列灌满后订阅仍留在表里")
	}
	if _, open := <-slow; open {
		t.Fatal("被踢的订阅通道应已关闭并排空")
	}
}
