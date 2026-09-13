package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Event struct {
	Kind          string            `json:"kind"`
	Index         int64             `json:"index"`
	Source        string            `json:"source"`
	At            string            `json:"at"`
	RoomID        string            `json:"roomId"`
	UserID        string            `json:"userId,omitempty"`
	User          string            `json:"user"`
	Text          string            `json:"text,omitempty"`
	GiftID        string            `json:"giftId,omitempty"`
	Gift          string            `json:"gift,omitempty"`
	Count         string            `json:"count,omitempty"`
	Combo         string            `json:"combo,omitempty"`
	NominalAmount string            `json:"nominalAmount,omitempty"`
	TriggeredBy   string            `json:"triggeredBy,omitempty"`
	UserMeta      *UserMetadata     `json:"userMeta,omitempty"`
	GiftReference *GiftReference    `json:"giftReference,omitempty"`
	DiamondFan    *DiamondFan       `json:"diamondFan,omitempty"`
	Fields        map[string]string `json:"fields,omitempty"`
	// 只服务端内部用（语音弹幕的原始字段表），页面一个都不读，从不进 SSE。
	VoiceFields map[string]string `json:"-"`
}

// chatmsg 有四十来个字段，页面只读 col。服务端内部仍持全量（ct、level、bnn、nl 都要用），
// 只在写进 SSE 的那一刻投影：939 → 311 字节，重连回放 300 条时这笔差价要乘 300。
var wireFields = [...]string{"col"}

func projectFields(fields map[string]string) map[string]string {
	var projected map[string]string
	for _, key := range wireFields {
		if value, present := fields[key]; present {
			if projected == nil {
				projected = make(map[string]string, len(wireFields))
			}
			projected[key] = value
		}
	}
	// 全不命中回 nil，配合 omitempty 连空对象都不占位。
	return projected
}

// 投影在写进 SSE 的那一刻做。这件事原先挂在 Event.MarshalJSON 上，但 encoding/json
// 拿到 Marshaler 返回的字节后还要再 compact 校验一遍，等于每帧编码两趟：实测同一条
// 弹幕 3.4 µs/2.8 KB/14 allocs，换成普通结构体一趟编码只要 1.8 µs/1.0 KB/5 allocs。
// 值接收者返回副本，调用方手里的 Event 仍持全量 Fields。
func wireEvent(e Event) Event {
	e.Fields = projectFields(e.Fields)
	return e
}

type UserMetadata struct {
	Level         *int64     `json:"level,omitempty"`
	FanBadge      *FanBadge  `json:"fanBadge,omitempty"`
	RoomAdmin     bool       `json:"roomAdmin,omitempty"`
	PlatformAdmin bool       `json:"platformAdmin,omitempty"`
	Noble         *NobleRole `json:"noble,omitempty"`
}

type FanBadge struct {
	Name   string `json:"name,omitempty"`
	Level  int64  `json:"level,omitempty"`
	RoomID string `json:"roomId,omitempty"`
	// 个人牌前缀挂件 ID，按人发，盖过房间那张限时的。
	Attr int64 `json:"attr,omitempty"`
}

type NobleRole struct {
	Level int64  `json:"level"`
	Name  string `json:"name,omitempty"`
}

var nobleNames = map[int64]string{
	1: "骑士", 2: "子爵", 3: "伯爵", 4: "公爵", 5: "国王", 6: "皇帝",
	7: "游侠", 8: "超级皇帝", 9: "幻神",
}

// nail 形如「挂件ID_房间号」以 / 相连，只取房间号对得上这块粉丝牌的那条
// （同斗鱼前端 filterValidAttrId）；同房间多条取第一条。
func fanBadgeAttr(nail, room string) int64 {
	if nail == "" || room == "" {
		return 0
	}
	for _, pair := range strings.Split(nail, "/") {
		id, rid, ok := strings.Cut(pair, "_")
		if !ok || rid != room {
			continue
		}
		if attr, err := strconv.ParseInt(id, 10, 64); err == nil && attr > 0 {
			return attr
		}
	}
	return 0
}

func userMetadata(fields, fallback map[string]string) *UserMetadata {
	value := func(key string) string {
		if text, present := fields[key]; present {
			return text
		}
		return fallback[key]
	}
	meta := &UserMetadata{RoomAdmin: value("rg") == "4", PlatformAdmin: value("pg") == "5"}
	if level, err := strconv.ParseInt(value("level"), 10, 64); err == nil && level >= 0 {
		meta.Level = &level
	}
	badge := &FanBadge{Name: value("bnn")}
	if level, err := strconv.ParseInt(value("bl"), 10, 64); err == nil && level > 0 {
		badge.Level = level
	}
	if room, err := strconv.ParseUint(value("brid"), 10, 64); err == nil && room > 0 {
		badge.RoomID = value("brid")
		badge.Attr = fanBadgeAttr(value("nail"), badge.RoomID)
	}
	if badge.Name != "" || badge.Level != 0 || badge.RoomID != "" {
		meta.FanBadge = badge
	}
	if level, err := strconv.ParseInt(value("nl"), 10, 64); err == nil && level > 0 {
		meta.Noble = &NobleRole{Level: level, Name: nobleNames[level]}
	}
	if meta.Level == nil && meta.FanBadge == nil && !meta.RoomAdmin && !meta.PlatformAdmin && meta.Noble == nil {
		return nil
	}
	return meta
}

// uenter 实测 4 条/秒，全放行几十秒就冲光 300 条历史。只留伯爵及以上，
// 并排除体验档的游侠(7)——这三档刷得太勤，占位配不上信息量。
const minNoticeNoble = 3

func noticeNoble(level int64) bool {
	return level >= minNoticeNoble && level != 7
}

func guestEnter(meta *UserMetadata) bool {
	return meta != nil && meta.Noble != nil && noticeNoble(meta.Noble.Level)
}

// 钻石粉丝开通/续费。Months 为 0 表示广播没带月数，斗鱼自己也只显示一个「-」。
type DiamondFan struct {
	Months    int64 `json:"months,omitempty"`
	BonusDays int64 `json:"bonusDays,omitempty"`
	Renew     bool  `json:"renew,omitempty"`
	ViaGift   bool  `json:"viaGift,omitempty"`
}

// 斗鱼把这件事拆成四种广播：直接买(dfobc)、直接续(dfrbc)、送礼触发开通(odfpbc)、
// 送礼触发续费(rdfpbc)。字段一模一样，只差动作词，所以合成一张表。
var diamondFanKinds = map[string]DiamondFan{
	"dfobc":  {},
	"dfrbc":  {Renew: true},
	"odfpbc": {ViaGift: true},
	"rdfpbc": {Renew: true, ViaGift: true},
}

// 昵称在 nick 而不是别处的 nn，别照着 chatmsg 抄。月数缺失不丢事件：
// 「谁开通了钻粉」本身就是要看的那句，月数只是附注。
func diamondFan(fields map[string]string, roomID string) *DiamondFan {
	fan, known := diamondFanKinds[fields["type"]]
	if !known {
		return nil
	}
	// rrid 是钻粉牌归属的主播房间，跟这条广播落在哪个房间(rid)是两回事：在 A 房间
	// 买 B 主播的钻粉，A 也收得到。只留开给本房间的，别家那张牌这边不关心。
	// rrid 缺席按本房间算，免得字段偶尔不下发就整条丢掉。
	if anchor := fields["rrid"]; anchor != "" && anchor != roomID {
		return nil
	}
	if months, err := strconv.ParseInt(fields["mn"], 10, 64); err == nil && months > 0 {
		fan.Months = months
	}
	if days, err := strconv.ParseInt(fields["cdays"], 10, 64); err == nil && days > 0 {
		fan.BonusDays = days
	}
	return &fan
}

// 幻兽蛋一类礼物随后会再下发一条孵化产物，gfid 恒为 0、ct=99，钱已全额记在触发它的
// 那条上。只回溯出处、不计价，免得同一笔钱记两遍。
const (
	derivedGiftFlag   = "99"
	derivedGiftWindow = 3 * time.Second
)

func derivedGift(event Event) bool {
	return event.Kind == "gift" && event.GiftReference == nil && event.Fields["ct"] == derivedGiftFlag
}

// 只有真实付费的礼物能当触发源；免费道具本就不计营收。
func triggerGift(event Event) bool {
	return event.Kind == "gift" && event.GiftReference != nil && !event.GiftReference.Prop
}

func first(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func giftName(name, id string, catalog giftCatalog) string {
	gift, _ := catalog.lookup(id)
	return first(name, gift.Name, "礼物 #"+id)
}

func normalizeEvent(fields map[string]string, roomID string, catalog giftCatalog) (Event, bool) {
	event := Event{Source: fields["type"], At: time.Now().Format(time.RFC3339Nano), RoomID: roomID, Fields: fields}
	// 广播的 rid 不是目的地，只有 drid 是。
	if fields["type"] == "spbc" {
		if fields["drid"] != roomID {
			return Event{}, false
		}
		event.Kind, event.UserID = "broadcast", fields["sid"]
		event.User = first(fields["sn"], "匿名用户")
		event.GiftID, event.Gift = fields["gfid"], giftName(fields["gn"], fields["gfid"], catalog)
		event.Count = first(fields["gc"], "1")
		return event, true
	}
	if fields["rid"] != "" && fields["rid"] != roomID {
		return Event{}, false
	}
	event.UserID, event.User = fields["uid"], first(fields["nn"], "匿名用户")
	event.UserMeta = userMetadata(fields, nil)
	switch fields["type"] {
	case "chatmsg":
		event.Kind, event.Text = "chat", fields["txt"]
	case "dgb":
		event.Kind, event.GiftID = "gift", fields["gfid"]
		event.Gift = giftName(fields["gfn"], event.GiftID, catalog)
		event.Count, event.Combo = first(fields["gfcnt"], "1"), fields["hits"]
		event.GiftReference = catalog.reference(event.GiftID, event.Count)
	case "uenter", "tuenter":
		if !guestEnter(event.UserMeta) {
			return Event{}, false
		}
		event.Kind = "enter"
	case "dfobc", "dfrbc", "odfpbc", "rdfpbc":
		fan := diamondFan(fields, roomID)
		if fan == nil {
			return Event{}, false
		}
		event.Kind, event.DiamondFan = "diamond", fan
		event.User = first(fields["nick"], fields["nn"], "匿名用户")
	case "comm_chatmsg":
		if fields["btype"] != "voiceDanmu" {
			return Event{}, false
		}
		voice := decodeSTT(fields["chatmsg"])
		if voice["rid"] != "" && voice["rid"] != roomID {
			return Event{}, false
		}
		event.Kind, event.Text, event.VoiceFields = "voice", voice["txt"], voice
		event.UserID, event.User = first(voice["uid"], fields["uid"]), first(voice["nn"], fields["nn"], "匿名用户")
		event.UserMeta = userMetadata(voice, fields)
		if amount, err := strconv.ParseInt(fields["cprice"], 10, 64); err == nil && amount >= 0 {
			event.NominalAmount = fmt.Sprintf("%d.%02d", amount/100, amount%100)
		}
	default:
		return Event{}, false
	}
	return event, true
}
