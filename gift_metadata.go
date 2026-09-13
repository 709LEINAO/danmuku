package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// UnitPrice 按 Currency 计价：YUCHI 是分，YUWAN 是鱼丸。NominalAmount 只是参考，
// 不代表真实付款。Prop 标记背包道具——斗鱼给免费道具也标单价但送出不扣钱，
// 所以前端不把它并进营收，改用 Devote（斗鱼自己衡量这类道具的口径）。
type GiftReference struct {
	UnitPrice     int64  `json:"unitPrice"`
	Count         int64  `json:"count"`
	Currency      string `json:"currency"`
	Source        string `json:"source"`
	NominalAmount string `json:"nominalAmount,omitempty"`
	Prop          bool   `json:"prop,omitempty"`
	Devote        int64  `json:"devote,omitempty"`
}

type giftMetadata struct {
	Name      string
	UnitPrice *int64
	Currency  string
	Source    string
	Prop      bool
	Devote    int64
}

type giftCatalog map[string]giftMetadata

func knownGift(name string, price int64) giftMetadata {
	return giftMetadata{Name: name, UnitPrice: &price, Currency: "YUCHI", Source: "verified-fallback"}
}

// 已核实的小份 YUCHI 兜底；未知 ID 与币种一律不折算成人民币。
var fallbackGifts = giftCatalog{
	"20002": knownGift("办卡", 600),
	"20003": knownGift("飞机", 10000),
	"20004": knownGift("火箭", 50000),
	"20005": knownGift("超级火箭", 200000),
	"23625": knownGift("至尊飞机", 10000),
	"23624": knownGift("至尊火箭", 50000),
	"23623": knownGift("至尊超火", 200000),
	"23622": knownGift("至尊飞船", 500000),
}

func (catalog giftCatalog) lookup(id string) (giftMetadata, bool) {
	if gift, exists := catalog[id]; exists {
		return gift, true
	}
	gift, exists := fallbackGifts[id]
	return gift, exists
}

func (catalog giftCatalog) reference(id, quantity string) *GiftReference {
	gift, exists := catalog.lookup(id)
	count, err := strconv.ParseInt(quantity, 10, 64)
	if !exists || gift.UnitPrice == nil || *gift.UnitPrice < 0 || gift.Currency == "" || err != nil || count <= 0 {
		return nil
	}
	reference := &GiftReference{UnitPrice: *gift.UnitPrice, Count: count, Currency: gift.Currency, Source: gift.Source, Prop: gift.Prop}
	if gift.Devote > 0 && gift.Devote <= math.MaxInt64/count {
		reference.Devote = gift.Devote * count
	}
	if gift.Currency == "YUCHI" && reference.UnitPrice <= math.MaxInt64/count {
		amount := reference.UnitPrice * count
		reference.NominalAmount = fmt.Sprintf("%d.%02d", amount/100, amount%100)
	}
	return reference
}

// 用 RawMessage 才能区分「字段缺席」与「显式 null / 0」。只收普通礼物命名空间，
// 皮肤／特效／道具 ID 各归各。
type giftCatalogRow struct {
	ID        int64           `json:"id"`
	Name      json.RawMessage `json:"name"`
	PriceInfo json.RawMessage `json:"priceInfo"`
	Coverage  int             `json:"coverage"`
}

func parseGiftRows(body io.Reader) ([]giftCatalogRow, error) {
	var response struct {
		Error int `json:"error"`
		Data  *struct {
			GiftList []giftCatalogRow `json:"giftList"`
		} `json:"data"`
	}
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode gift catalog: %w", err)
	}
	if response.Error != 0 || response.Data == nil || response.Data.GiftList == nil {
		return nil, fmt.Errorf("gift catalog unavailable (error %d)", response.Error)
	}
	return response.Data.GiftList, nil
}

func catalogFromGiftRows(rows []giftCatalogRow, source string) (giftCatalog, error) {
	catalog := make(giftCatalog, len(rows))
	for _, entry := range rows {
		if entry.ID <= 0 {
			continue
		}
		gift := giftMetadata{Source: source}
		if entry.Name != nil {
			if err := json.Unmarshal(entry.Name, &gift.Name); err != nil {
				return nil, fmt.Errorf("gift %d name: %w", entry.ID, err)
			}
		}
		if entry.PriceInfo != nil {
			var price struct {
				Price     *int64 `json:"price"`
				PriceType string `json:"priceType"`
			}
			if err := json.Unmarshal(entry.PriceInfo, &price); err != nil {
				return nil, fmt.Errorf("gift %d price: %w", entry.ID, err)
			}
			gift.Currency = price.PriceType
			if price.Price != nil && *price.Price >= 0 {
				gift.UnitPrice = price.Price
			}
		}
		catalog[strconv.FormatInt(entry.ID, 10)] = gift
	}
	return catalog, nil
}

func parseGiftCatalog(body io.Reader) (giftCatalog, error) {
	rows, err := parseGiftRows(body)
	if err != nil {
		return nil, err
	}
	return catalogFromGiftRows(rows, "douyu-room-catalog")
}

func mergeV5GiftRows(base, details []giftCatalogRow) []giftCatalogRow {
	merged := append([]giftCatalogRow(nil), base...)
	positions := make(map[int64]int, len(base))
	for index, entry := range base {
		positions[entry.ID] = index
	}
	for _, overlay := range details {
		if index, exists := positions[overlay.ID]; exists {
			// 照搬官方 handleGiftList 的同 ID 浅覆盖：priceInfo 在场就整块替换。
			if overlay.Name != nil {
				merged[index].Name = overlay.Name
			}
			if overlay.PriceInfo != nil {
				merged[index].PriceInfo = overlay.PriceInfo
			}
		} else if overlay.Coverage != 1 {
			merged = append(merged, overlay)
		}
	}
	// giftIds 的顺序与 skinData 的合并不影响这张按 ID 查的表。
	return merged
}

// 补充而不丢弃其他 ID，也不用缺字段的记录盖掉有效的；价与币种始终同源。
func (catalog giftCatalog) supplement(additions giftCatalog) giftCatalog {
	merged := make(giftCatalog, len(catalog)+len(additions))
	for id, gift := range catalog {
		merged[id] = gift
	}
	for id, gift := range additions {
		previous, exists := merged[id]
		if !exists {
			merged[id] = gift
			continue
		}
		if gift.Name != "" {
			previous.Name = gift.Name
		}
		if gift.UnitPrice != nil && gift.Currency != "" {
			previous.UnitPrice, previous.Currency, previous.Source = gift.UnitPrice, gift.Currency, gift.Source
			previous.Prop, previous.Devote = gift.Prop, gift.Devote
		}
		merged[id] = previous
	}
	return merged
}

// 房间目录查不到的那一半全在这份全站道具配置里：1585 条，与房间目录交集为零。
// 单价字段 pc 与房间目录的 price 同单位同值，全库 ry=0（没有鱼丸档）。
const propConfigEndpoint = "https://webconf.douyucdn.cn/resource/common/prop_gift_list/prop_gift_config.json"

// 这张表混着两类东西：背包免费道具（荧光棒、赞……标了单价但送出不扣钱）和
// 真金白银买来、只是走背包发放的礼物（钻粉卡 ¥218、钻粉飞机 ¥100、办卡 ¥6）。
// 38 个字段里没有一个能区分：type 全是 2，ry 与 active_type 全 0，exp 与 devote
// 恒等于 pc/10，免费的「赞」和付费的「钻粉卡」逐字段同形。价格也划不开线——
// 免费的「稳」有 ¥50 的版本，付费的「办卡」只要 ¥6。
//
// 所以只按名字放行已核实付费的那几种，其余一律沿用 Prop（不计营收）。方向是
// 保守的：漏记一种付费礼物只是少算，错放一种免费道具会把营收合计和置顶门槛
// 一起撑起来。发现漏了哪种，往这张表加一行就行。
var paidPropNames = map[string]struct{}{
	"钻粉卡":   {},
	"钻粉飞机":  {},
	"钻粉摩天轮": {},
	"办卡":    {},
}

// 同名以后要是出个白送的赠品版，用这条下限挡住。已核实的付费档最低是 ¥6 的
// 办卡，免费道具都在 ¥1 以内。
const minPaidPropPrice = 100

// pc 与 devote 不保证是整数（实测有 665.9）。用 json.Number 收下再自己取整，
// 免得一行小数废掉整张表。
type propConfigRow struct {
	Name   string      `json:"name"`
	Price  json.Number `json:"pc"`
	Devote json.Number `json:"devote"`
}

// 向下取整：贡献值宁可少算。
func propNumber(value json.Number) (int64, bool) {
	if value == "" {
		return 0, false
	}
	if number, err := value.Int64(); err == nil {
		return number, true
	}
	number, err := value.Float64()
	if err != nil || math.IsNaN(number) || number < 0 || number > math.MaxInt64 {
		return 0, false
	}
	return int64(math.Floor(number)), true
}

// 这个地址回的是 JSONP：DYConfigCallback({...});
func stripJSONP(payload []byte) ([]byte, error) {
	start, end := bytes.IndexByte(payload, '('), bytes.LastIndexByte(payload, ')')
	if start < 0 || end <= start {
		return nil, errors.New("prop config: not a JSONP payload")
	}
	return payload[start+1 : end], nil
}

func parsePropCatalog(body io.Reader) (giftCatalog, error) {
	payload, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("read prop config: %w", err)
	}
	inner, err := stripJSONP(payload)
	if err != nil {
		return nil, err
	}
	// 逐行解码：第三方静态表，一行长歪只丢那一行。
	var response struct {
		Error int                        `json:"error"`
		Data  map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(inner, &response); err != nil {
		return nil, fmt.Errorf("decode prop config: %w", err)
	}
	if response.Error != 0 || response.Data == nil {
		return nil, fmt.Errorf("prop config unavailable (error %d)", response.Error)
	}
	catalog := make(giftCatalog, len(response.Data))
	for id, raw := range response.Data {
		if number, err := strconv.ParseInt(id, 10, 64); err != nil || number <= 0 {
			continue
		}
		var entry propConfigRow
		if err := json.Unmarshal(raw, &entry); err != nil {
			continue
		}
		gift := giftMetadata{Name: entry.Name, Currency: "YUCHI", Source: "douyu-prop-config", Prop: true}
		if devote, ok := propNumber(entry.Devote); ok {
			gift.Devote = devote
		}
		if price, ok := propNumber(entry.Price); ok && price >= 0 {
			gift.UnitPrice = &price
			if _, paid := paidPropNames[entry.Name]; paid && price >= minPaidPropPrice {
				gift.Prop = false
			}
		}
		catalog[id] = gift
	}
	if len(catalog) == 0 {
		return nil, errors.New("prop config: no usable rows")
	}
	return catalog, nil
}

// 全站静态表与房间号无关，每个 worker 各下一遍就是十几 MB 重复流量加 12 份内存映射。
// 改成进程内共享（缓存 + 单飞，形状同 scoreboardStore）。挂在 roomManager 上而非包级全局。
const (
	propCatalogTTL = 12 * time.Hour
	// 过期后回源失败就继续用旧表，隔这么久再试；表是静态的，旧一点无妨。
	propCatalogRetry = time.Minute
)

type propCatalogStore struct {
	mu      sync.Mutex
	cached  giftCatalog
	fetched time.Time
	retryAt time.Time
	single  flight[giftCatalog]
	ttl     time.Duration
	retry   time.Duration
	now     func() time.Time
}

func newPropCatalogStore() *propCatalogStore {
	return &propCatalogStore{ttl: propCatalogTTL, retry: propCatalogRetry, now: time.Now}
}

// client 按次传入：catalogHTTP 允许在 newApplication 之后被替换，提前捕获会让替换失效。
// lead 把回源与调用方 worker 的取消解绑，传 nil 则直接用调用方 ctx。
func (s *propCatalogStore) catalog(ctx context.Context, client *http.Client, lead func(context.Context) (context.Context, func())) (giftCatalog, error) {
	s.mu.Lock()
	if now := s.now(); s.cached != nil && (now.Sub(s.fetched) < s.ttl || now.Before(s.retryAt)) {
		cached := s.cached
		s.mu.Unlock()
		return cached, nil
	}
	s.mu.Unlock()
	return s.single.do(ctx, func() (giftCatalog, error) {
		fetchCtx, release := ctx, func() {}
		if lead != nil {
			fetchCtx, release = lead(ctx)
		}
		catalog, err := fetchPropCatalog(fetchCtx, client)
		release()

		// 在这里落缓存，跟随者是在这之后才被放行的。失败时有旧表就沿用旧表、稍后再试，
		// 改写的是所有人（含跟随者）拿到的那一份；没有旧表才把错误交给调用方重试。
		s.mu.Lock()
		defer s.mu.Unlock()
		if err == nil {
			s.cached, s.fetched = catalog, s.now()
			return catalog, nil
		}
		if s.cached != nil {
			s.retryAt = s.now().Add(s.retry)
			return s.cached, nil
		}
		return catalog, err
	})
}

func fetchPropCatalog(ctx context.Context, client *http.Client) (giftCatalog, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, propConfigEndpoint, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("prop config HTTP status %d", response.StatusCode)
	}
	// 实测 1.4 MB，留一倍余量。
	return parsePropCatalog(io.LimitReader(response.Body, 4<<20))
}

// 共享 client 为道具表放宽到了 30 秒，这三个小响应不该跟着拖长。
const giftRowsTimeout = 5 * time.Second

func fetchGiftRows(ctx context.Context, endpoint string, client *http.Client) ([]giftCatalogRow, error) {
	ctx, cancel := context.WithTimeout(ctx, giftRowsTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gift catalog HTTP status %d", response.StatusCode)
	}
	return parseGiftRows(io.LimitReader(response.Body, 2<<20))
}

// publish 在房间目录到手、还没等到全站道具表时先发一版。开房那几秒到的礼物原先一律
// 拿不到参考价——而且不会回头重算，帧早就编码发出去了，那几笔钱就永久漏在合计之外。
//
// 先发的是房间目录而不是随便哪个源：它本来就是优先级最高的那层，所以早到的礼物拿到的
// 就是最终价，不会出现「先便宜后变贵」的跳动。道具表随后补上只在全站表里的那些（钻粉卡
// 一类），而它是进程内共享缓存，只有进程起来后开的第一个房间才真的要等它下载。
func fetchGiftCatalog(ctx context.Context, roomID string, client *http.Client, props *propCatalogStore, lead func(context.Context) (context.Context, func()), publish func(giftCatalog)) (giftCatalog, error) {
	query := url.Values{"rid": {roomID}}.Encode()
	endpoints := []string{
		"https://gift.douyucdn.cn/api/gift/v2/web/list?" + query,
		"https://gift.douyucdn.cn/api/gift/v5/web/base/list?" + query,
		"https://www.douyu.com/japi/reward/giftv2/list/details/web/v5?" + query + "&userLevel=0",
	}
	type result struct {
		index int
		rows  []giftCatalogRow
		err   error
	}
	results := make(chan result, len(endpoints))
	for index, endpoint := range endpoints {
		go func() {
			rows, err := fetchGiftRows(ctx, endpoint, client)
			results <- result{index: index, rows: rows, err: err}
		}()
	}
	propResults := make(chan struct {
		catalog giftCatalog
		err     error
	}, 1)
	go func() {
		catalog, err := props.catalog(ctx, client, lead)
		propResults <- struct {
			catalog giftCatalog
			err     error
		}{catalog, err}
	}()
	var rows [3][]giftCatalogRow
	var failures []error
	for range endpoints {
		response := <-results
		rows[response.index] = response.rows
		if response.err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", endpoints[response.index], response.err))
		}
	}
	legacy, legacyErr := catalogFromGiftRows(rows[0], "douyu-room-catalog")
	v5, v5Err := catalogFromGiftRows(mergeV5GiftRows(rows[1], rows[2]), "douyu-room-catalog-v5")
	rooms := legacy.supplement(v5)
	// 三个 rid 接口都是小响应（各 5 秒时限），道具表是 1.4 MB（30 秒）。房间目录一齐就先发，
	// 不陪着最慢的那个等。
	if len(rooms) > 0 && publish != nil {
		publish(rooms)
	}
	prop := <-propResults
	if prop.err != nil {
		failures = append(failures, fmt.Errorf("%s: %w", propConfigEndpoint, prop.err))
	}
	// 道具配置垫底，房间目录永远优先。
	return prop.catalog.supplement(rooms), errors.Join(append(failures, legacyErr, v5Err)...)
}
