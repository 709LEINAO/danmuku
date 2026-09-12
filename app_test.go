package main

import (
	"bytes"
	"encoding/json"
	"testing"
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
