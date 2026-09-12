package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 等级、贵族、粉丝牌三种徽章图由 Go 端代取并缓存，浏览器只连本机——直接写斗鱼地址
// 等于让每个观看者去连外网 CDN，弱网下常拉不出来。
//
// 代理只认键不认地址：路径里只能出现主题名和一个整数，真实 URL 由服务端查配置得出、
// 再过主机白名单，因此不可能被当成开放代理。
const (
	nobleConfigEndpoint = "https://wconf.douyucdn.cn/resource/noble/global/web.json"
	medalConfigEndpoint = "https://wconf.douyucdn.cn/resource/common/fans_medal_web_v5.json"
	// 牌前缀的另一个来源：上面那份按房间发限时活动，这份按人发（弹幕里的 nail），个人的优先。
	propertyConfigEndpoint = "https://webconf.douyucdn.cn/resource/common/property_info_22.json"
	// ?v=1.2 是斗鱼自带的版本号，保留是为了和页面共用缓存键。
	levelIconTemplate = "https://shark2.douyucdn.cn/front-publish/static-file-master/userLevelIconV6/web-%s/newm3_lv%d.png?v=1.2"
)

// 等级上限是试出来的：lv151 仍是 200，lv152 起一律 404。贵族与粉丝牌的边界来自配置表。
// 这几个只用来挡住路径上的越界数字，真实地址仍靠查表。
const (
	maxUserLevel  = 151
	maxNobleLevel = 9
	maxMedalLevel = 60
)

// 最后一道闸：即使斗鱼配置被换掉，也只会去这三个域名取图。
var iconHosts = map[string]bool{
	"shark2.douyucdn.cn": true,
	"sta-op.douyucdn.cn": true,
	"res.douyucdn.cn":    true,
}

// 全部图加起来约 3.5 MB，24 MiB 留了两个数量级余量。
const maxIconCacheBytes = 24 << 20

// 这些静态图按年计不变，12 小时一轮只为接住节日皮肤和限时牌前缀。
const iconRefreshInterval = 12 * time.Hour

type iconCatalog struct {
	noble map[int64]string  // 贵族等级 → 小图
	medal map[int64]string  // 粉丝牌等级 → 窄版底牌
	room  map[string]string // 房间号 → 限时牌前缀
	attr  map[int64]string  // 个人挂件 ID → 牌前缀，盖过房间那张
}

type cachedAsset struct {
	body         []byte
	contentType  string
	etag         string // 回给浏览器的强校验值，由内容算出
	upstreamETag string
	lastModified string
	usedAt       time.Time
}

// 开播瞬间同一张图会被几十条弹幕同时要到，单飞保证只出门一次。
type iconFetch struct {
	done  chan struct{}
	asset *cachedAsset
	err   error
}

type iconEndpoints struct {
	Noble    string
	Medal    string
	Property string
	Level    string // fmt 模板：主题名 + 等级
}

func defaultIconEndpoints() iconEndpoints {
	return iconEndpoints{
		Noble:    nobleConfigEndpoint,
		Medal:    medalConfigEndpoint,
		Property: propertyConfigEndpoint,
		Level:    levelIconTemplate,
	}
}

type iconStore struct {
	client    *http.Client
	endpoints iconEndpoints
	now       func() time.Time
	interval  time.Duration

	mu       sync.Mutex
	catalog  *iconCatalog
	assets   map[string]*cachedAsset
	bytes    int
	inflight map[string]*iconFetch

	// Start/Close 同一把锁：两个 sync.Once 各管一半时，先 Close 再 Start 会重复关 done。
	lifecycle sync.Mutex
	started   bool
	stopped   bool
	cancel    context.CancelFunc
	done      chan struct{}

	// store 的存活期：回源脱离发起它的浏览器请求，但不脱离进程，否则 Close() 要干等
	// 一轮下载超时。建在构造函数里，不调 Start 也能收干净。
	life    context.Context
	endLife context.CancelFunc
}

func newIconStore() *iconStore {
	life, endLife := context.WithCancel(context.Background())
	return &iconStore{
		client:    &http.Client{Timeout: 20 * time.Second},
		endpoints: defaultIconEndpoints(),
		now:       time.Now,
		interval:  iconRefreshInterval,
		assets:    make(map[string]*cachedAsset),
		inflight:  make(map[string]*iconFetch),
		done:      make(chan struct{}),
		life:      life,
		endLife:   endLife,
	}
}

// 后台刷新由 main 显式打开；不调 Start 就一次网络都不发。Close 之后再 Start 不起作用。
func (s *iconStore) Start() {
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	if s.started || s.stopped {
		return
	}
	s.started = true
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	go s.refreshLoop(ctx)
}

// 可重复、可并发调用，都等到刷新循环真正退出才返回。
func (s *iconStore) Close() {
	s.lifecycle.Lock()
	if !s.stopped {
		s.stopped = true
		s.endLife()
		if s.started {
			s.cancel()
		} else {
			close(s.done)
		}
	}
	s.lifecycle.Unlock()
	<-s.done
}

func (s *iconStore) refreshLoop(ctx context.Context) {
	defer close(s.done)
	s.refresh(ctx)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refresh(ctx)
		}
	}
}

// 换配置表，再逐张 revalidate。任何一步失败都保留旧数据：宁可图旧，不能让徽章空掉。
func (s *iconStore) refresh(ctx context.Context) {
	if catalog, err := s.fetchCatalog(ctx); err == nil {
		s.mu.Lock()
		s.catalog = catalog
		s.mu.Unlock()
	}
	s.mu.Lock()
	keys := make([]string, 0, len(s.assets))
	for key := range s.assets {
		keys = append(keys, key)
	}
	s.mu.Unlock()
	sort.Strings(keys)
	for _, key := range keys {
		if ctx.Err() != nil {
			return
		}
		s.revalidate(ctx, key)
	}
}

func (s *iconStore) Ready() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.catalog != nil
}

// --- 配置解析 ---

type nobleConfigLevel struct {
	Name   string `json:"name"`
	Symbol string `json:"web_symbol_pic1"`
}

func parseNobleConfig(body io.Reader) (map[int64]string, error) {
	var response struct {
		Error int `json:"error"`
		Data  *struct {
			Host   string                      `json:"host"`
			Levels map[string]nobleConfigLevel `json:"all_level_list"`
		} `json:"data"`
	}
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode noble config: %w", err)
	}
	if response.Error != 0 || response.Data == nil || len(response.Data.Levels) == 0 {
		return nil, fmt.Errorf("noble config unavailable (error %d)", response.Error)
	}
	icons := make(map[int64]string, len(response.Data.Levels))
	for key, entry := range response.Data.Levels {
		level, err := strconv.ParseInt(key, 10, 64)
		if err != nil || level < 1 || level > maxNobleLevel || entry.Symbol == "" {
			continue
		}
		// host 是目录前缀，web_symbol_pic1 是相对路径，同斗鱼自己的拼法。
		if target, ok := allowedIconURL(response.Data.Host + entry.Symbol); ok {
			icons[level] = target
		}
	}
	if len(icons) == 0 {
		return nil, errors.New("noble config: no usable levels")
	}
	return icons, nil
}

// 粉丝牌配置是限时活动表加两张映射表，单条长歪只丢那一条。
type medalConfigEntry struct {
	Resource struct {
		WebHdPic string `json:"webHdPic"`
	} `json:"resource"`
	Rooms     map[string]json.RawMessage `json:"room_ids_v2"`
	StartTime string                     `json:"start_time"`
	EndTime   string                     `json:"end_time"`
}

func parseMedalConfig(body io.Reader, now time.Time) (map[int64]string, map[string]string, error) {
	payload, err := io.ReadAll(body)
	if err != nil {
		return nil, nil, fmt.Errorf("read medal config: %w", err)
	}
	// 线上现在回普通 JSON，但这类配置历史上也出现过 JSONP 形态，两种都收。
	inner := unwrapJSONP(payload)
	var response struct {
		Error int `json:"error"`
		Data  *struct {
			CommBg map[string]string `json:"commBg"`
			BgPics map[string]string `json:"bgPics"`
			// 数组或按 id 建索引的对象都可能，晚一步再逐条解。
			Medals json.RawMessage `json:"fansMedals"`
		} `json:"data"`
	}
	if err := json.Unmarshal(inner, &response); err != nil {
		return nil, nil, fmt.Errorf("decode medal config: %w", err)
	}
	if response.Error != 0 || response.Data == nil {
		return nil, nil, fmt.Errorf("medal config unavailable (error %d)", response.Error)
	}
	// bg_ 是 66px 窄版，lbg_ 是 84px 宽版（多出的 18px 留给牌尾挂件）。不画挂件，用窄版。
	plates := make(map[int64]string, maxMedalLevel+1)
	for level := int64(0); level <= maxMedalLevel; level++ {
		name := response.Data.CommBg["bg_"+strconv.FormatInt(level, 10)]
		if name == "" {
			continue
		}
		if target, ok := allowedIconURL(response.Data.BgPics[name]); ok {
			plates[level] = target
		}
	}
	if len(plates) == 0 {
		return nil, nil, errors.New("medal config: no usable plates")
	}
	rooms := parseMedalRooms(response.Data.Medals, now)
	return plates, rooms, nil
}

// 只有真被回调名裹住才剥壳，免得把字符串里的括号当成壳。
func unwrapJSONP(payload []byte) []byte {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] == '{' || trimmed[0] == '[' {
		return payload
	}
	inner, err := stripJSONP(trimmed)
	if err != nil {
		return payload
	}
	return inner
}

// 数组与对象两种形态都拆成逐条原始 JSON，一条长歪不拖垮整份配置。
func medalEntries(raw json.RawMessage) []json.RawMessage {
	var list []json.RawMessage
	if json.Unmarshal(raw, &list) == nil {
		return list
	}
	var indexed map[string]json.RawMessage
	if json.Unmarshal(raw, &indexed) != nil {
		return nil
	}
	list = make([]json.RawMessage, 0, len(indexed))
	for _, entry := range indexed {
		list = append(list, entry)
	}
	return list
}

// room_ids_v2 写成 {"0":1} 的是按挂件分发的个人样式，不是房间默认样式，照搬会给所有人
// 挂上同一张图。只收明确列出房间号的那些。
func parseMedalRooms(raw json.RawMessage, now time.Time) map[string]string {
	type candidate struct {
		start int64
		url   string
	}
	best := make(map[string]candidate)
	stamp := now.Unix()
	for _, raw := range medalEntries(raw) {
		var entry medalConfigEntry
		if err := json.Unmarshal(raw, &entry); err != nil || len(entry.Rooms) == 0 {
			continue
		}
		if _, wildcard := entry.Rooms["0"]; wildcard {
			continue
		}
		start, startErr := strconv.ParseInt(entry.StartTime, 10, 64)
		end, endErr := strconv.ParseInt(entry.EndTime, 10, 64)
		if startErr != nil || endErr != nil || stamp < start || stamp > end {
			continue
		}
		target, ok := allowedIconURL(entry.Resource.WebHdPic)
		if !ok {
			continue
		}
		for room := range entry.Rooms {
			if number, err := strconv.ParseUint(room, 10, 64); err != nil || number == 0 {
				continue
			}
			// 同房间挂着两期活动时取开始得晚的那期。
			if previous, exists := best[room]; !exists || start > previous.start {
				best[room] = candidate{start: start, url: target}
			}
		}
	}
	rooms := make(map[string]string, len(best))
	for room, pick := range best {
		rooms[room] = pick.url
	}
	return rooms
}

// 个人挂件表：键是弹幕 nail 里的挂件 ID，web_pic 是牌前缀图；外面裹着 JSONP。
func parsePropertyConfig(body io.Reader) (map[int64]string, error) {
	payload, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("read property config: %w", err)
	}
	var response struct {
		Data *struct {
			Properties map[string]json.RawMessage `json:"property_info"`
		} `json:"data"`
	}
	if err := json.Unmarshal(unwrapJSONP(payload), &response); err != nil {
		return nil, fmt.Errorf("decode property config: %w", err)
	}
	if response.Data == nil || len(response.Data.Properties) == 0 {
		return nil, errors.New("property config unavailable")
	}
	icons := make(map[int64]string, len(response.Data.Properties))
	for key, raw := range response.Data.Properties {
		id, err := strconv.ParseInt(key, 10, 64)
		if err != nil || id <= 0 {
			continue
		}
		// 逐条解：一条长歪不该带走其余的。
		var entry struct {
			WebPic string `json:"web_pic"`
		}
		if json.Unmarshal(raw, &entry) != nil {
			continue
		}
		if target, ok := allowedIconURL(entry.WebPic); ok {
			icons[id] = target
		}
	}
	if len(icons) == 0 {
		return nil, errors.New("property config: no usable icons")
	}
	return icons, nil
}

func allowedIconURL(raw string) (string, bool) {
	if raw == "" {
		return "", false
	}
	// 配置里偶尔是协议相对地址，补成 https 再判主机。
	if strings.HasPrefix(raw, "//") {
		raw = "https:" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || !iconHosts[parsed.Host] {
		return "", false
	}
	return parsed.String(), true
}

func (s *iconStore) fetchJSON(ctx context.Context, endpoint string, limit int64) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	response, err := s.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s HTTP status %d", endpoint, response.StatusCode)
	}
	return io.ReadAll(io.LimitReader(response.Body, limit))
}

type iconLevelResult struct {
	icons map[int64]string
	err   error
}

func (s *iconStore) fetchCatalog(ctx context.Context) (*iconCatalog, error) {
	nobles := make(chan iconLevelResult, 1)
	go func() {
		// 贵族配置实测 30 KB。
		body, err := s.fetchJSON(ctx, s.endpoints.Noble, 1<<20)
		if err != nil {
			nobles <- iconLevelResult{err: err}
			return
		}
		icons, err := parseNobleConfig(strings.NewReader(string(body)))
		nobles <- iconLevelResult{icons: icons, err: err}
	}()
	attrs := make(chan iconLevelResult, 1)
	go func() {
		// 个人挂件表实测 92 KB。
		body, err := s.fetchJSON(ctx, s.endpoints.Property, 4<<20)
		if err != nil {
			attrs <- iconLevelResult{err: err}
			return
		}
		icons, err := parsePropertyConfig(strings.NewReader(string(body)))
		attrs <- iconLevelResult{icons: icons, err: err}
	}()
	// 粉丝牌配置实测 106 KB，留一倍余量。
	medalBody, medalErr := s.fetchJSON(ctx, s.endpoints.Medal, 4<<20)
	var plates map[int64]string
	var rooms map[string]string
	if medalErr == nil {
		plates, rooms, medalErr = parseMedalConfig(strings.NewReader(string(medalBody)), s.now())
	}
	noble, attr := <-nobles, <-attrs
	if noble.err != nil && medalErr != nil && attr.err != nil {
		return nil, errors.Join(noble.err, medalErr, attr.err)
	}
	catalog := &iconCatalog{noble: noble.icons, medal: plates, room: rooms, attr: attr.icons}
	// 只成功一部分也先用起来，缺的那几块沿用上一份。
	s.mu.Lock()
	if previous := s.catalog; previous != nil {
		if catalog.noble == nil {
			catalog.noble = previous.noble
		}
		if catalog.medal == nil {
			catalog.medal, catalog.room = previous.medal, previous.room
		}
		if catalog.attr == nil {
			catalog.attr = previous.attr
		}
	}
	s.mu.Unlock()
	return catalog, nil
}

// --- 键 → 地址 ---

// 路径每一段都在这里翻译成固定来源的地址，翻不出来就是 404。
func (s *iconStore) resolve(kind, key string) (string, bool) {
	s.mu.Lock()
	catalog := s.catalog
	s.mu.Unlock()
	switch kind {
	case "level":
		theme, level, ok := strings.Cut(key, "/")
		if !ok || (theme != "light" && theme != "dark") {
			return "", false
		}
		number, err := strconv.Atoi(level)
		if err != nil || number < 1 || number > maxUserLevel {
			return "", false
		}
		return fmt.Sprintf(s.endpoints.Level, theme, number), true
	case "noble":
		if catalog == nil {
			return "", false
		}
		number, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return "", false
		}
		target, exists := catalog.noble[number]
		return target, exists
	case "medal":
		if catalog == nil {
			return "", false
		}
		number, err := strconv.ParseInt(key, 10, 64)
		if err != nil || number < 0 {
			return "", false
		}
		// 顶格之后不再有新底牌，沿用最高那张。
		if number > maxMedalLevel {
			number = maxMedalLevel
		}
		target, exists := catalog.medal[number]
		return target, exists
	case "room":
		if catalog == nil {
			return "", false
		}
		if number, err := strconv.ParseUint(key, 10, 64); err != nil || number == 0 {
			return "", false
		}
		target, exists := catalog.room[key]
		return target, exists
	case "attr":
		if catalog == nil {
			return "", false
		}
		number, err := strconv.ParseInt(key, 10, 64)
		if err != nil || number <= 0 {
			return "", false
		}
		target, exists := catalog.attr[number]
		return target, exists
	}
	return "", false
}

// --- 资源缓存 ---

func (s *iconStore) cached(key string) *cachedAsset {
	s.mu.Lock()
	defer s.mu.Unlock()
	asset := s.assets[key]
	if asset != nil {
		asset.usedAt = s.now()
	}
	return asset
}

func (s *iconStore) store(key string, asset *cachedAsset) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storeLocked(key, asset)
}

func (s *iconStore) storeLocked(key string, asset *cachedAsset) {
	if previous, exists := s.assets[key]; exists {
		s.bytes -= len(previous.body)
	}
	asset.usedAt = s.now()
	s.assets[key] = asset
	s.bytes += len(asset.body)
	for s.bytes > maxIconCacheBytes && len(s.assets) > 1 {
		oldest, oldestAt := "", time.Time{}
		for candidate, entry := range s.assets {
			if candidate == key {
				continue
			}
			if oldest == "" || entry.usedAt.Before(oldestAt) {
				oldest, oldestAt = candidate, entry.usedAt
			}
		}
		if oldest == "" {
			return
		}
		s.bytes -= len(s.assets[oldest].body)
		delete(s.assets, oldest)
	}
}

func newCachedAsset(response *http.Response, target string, body []byte) *cachedAsset {
	contentType := response.Header.Get("Content-Type")
	if contentType == "" || strings.HasPrefix(contentType, "text/") {
		// CDN 偶尔回 text/plain，按扩展名纠回图片，否则浏览器不认。
		contentType = iconContentType(target)
	}
	digest := sha256.Sum256(body)
	return &cachedAsset{
		body:         body,
		contentType:  contentType,
		etag:         `"` + base64.RawURLEncoding.EncodeToString(digest[:12]) + `"`,
		upstreamETag: response.Header.Get("ETag"),
		lastModified: response.Header.Get("Last-Modified"),
	}
}

func iconContentType(target string) string {
	// 等级图带着 ?v=1.2，先切查询串再看扩展名。
	path, _, _ := strings.Cut(target, "?")
	switch {
	case strings.HasSuffix(path, ".webp"):
		return "image/webp"
	case strings.HasSuffix(path, ".gif"):
		return "image/gif"
	case strings.HasSuffix(path, ".jpg"), strings.HasSuffix(path, ".jpeg"):
		return "image/jpeg"
	default:
		return "image/png"
	}
}

// 最大的是 200~360 KB 的粉丝牌动图，4 MiB 是两个数量级的余量。
const maxIconBytes = 4 << 20

// 单飞领头者的时限，与 client 超时同值。
const iconLeadTimeout = 20 * time.Second

func (s *iconStore) download(ctx context.Context, target string, previous *cachedAsset) (*cachedAsset, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	if previous != nil {
		if previous.upstreamETag != "" {
			request.Header.Set("If-None-Match", previous.upstreamETag)
		}
		if previous.lastModified != "" {
			request.Header.Set("If-Modified-Since", previous.lastModified)
		}
	}
	response, err := s.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotModified && previous != nil {
		return previous, nil
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("icon HTTP status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxIconBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) == 0 || len(body) > maxIconBytes {
		return nil, fmt.Errorf("icon size %d out of range", len(body))
	}
	return newCachedAsset(response, target, body), nil
}

// 领头的回源不跟着发起它的浏览器走，但仍随 store 一起结束。
// 同 roomManager.requestContext。
func (s *iconStore) leadContext(ctx context.Context) (context.Context, func()) {
	lead, cancel := context.WithTimeout(context.WithoutCancel(ctx), iconLeadTimeout)
	stop := context.AfterFunc(s.life, cancel)
	return lead, func() { stop(); cancel() }
}

// 同键只回源一次，其余请求在 done 上等。
func (s *iconStore) fetch(ctx context.Context, key, target string, previous *cachedAsset) (*cachedAsset, error) {
	s.mu.Lock()
	if pending, exists := s.inflight[key]; exists {
		s.mu.Unlock()
		select {
		case <-pending.done:
			return pending.asset, pending.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	pending := &iconFetch{done: make(chan struct{})}
	s.inflight[key] = pending
	s.mu.Unlock()

	lead, release := s.leadContext(ctx)
	asset, err := s.download(lead, target, previous)
	release()
	s.mu.Lock()
	if err == nil {
		s.storeLocked(key, asset)
	}
	delete(s.inflight, key)
	s.mu.Unlock()
	pending.asset, pending.err = asset, err
	close(pending.done)
	return asset, err
}

func (s *iconStore) revalidate(ctx context.Context, key string) {
	kind, rest, ok := strings.Cut(key, ":")
	if !ok {
		return
	}
	target, ok := s.resolve(kind, rest)
	if !ok {
		return
	}
	// 取不到就保留旧的，刷新失败不该让好用的图消失。
	s.fetch(ctx, key, target, s.cached(key))
}

// --- HTTP ---

// 浏览器缓存一天。换图靠 Go 端 12 小时一轮的 revalidate 改掉 ETag。
const iconBrowserMaxAge = 86400

func (s *iconStore) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/icons", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		catalog := s.catalog
		s.mu.Unlock()
		manifest := map[string]any{"ready": catalog != nil, "levelMax": maxUserLevel, "medalMax": maxMedalLevel}
		if catalog != nil {
			levels := make([]int64, 0, len(catalog.noble))
			for level := range catalog.noble {
				levels = append(levels, level)
			}
			sort.Slice(levels, func(a, b int) bool { return levels[a] < levels[b] })
			manifest["noble"] = levels
		}
		writeJSON(w, http.StatusOK, manifest)
	})
	mux.HandleFunc("GET /assets/level/{theme}/{level}", func(w http.ResponseWriter, r *http.Request) {
		s.serve(w, r, "level", r.PathValue("theme")+"/"+r.PathValue("level"))
	})
	mux.HandleFunc("GET /assets/noble/{level}", func(w http.ResponseWriter, r *http.Request) {
		s.serve(w, r, "noble", r.PathValue("level"))
	})
	mux.HandleFunc("GET /assets/medal/{level}", func(w http.ResponseWriter, r *http.Request) {
		s.serve(w, r, "medal", r.PathValue("level"))
	})
	mux.HandleFunc("GET /assets/medal/room/{room}", func(w http.ResponseWriter, r *http.Request) {
		s.serve(w, r, "room", r.PathValue("room"))
	})
	// 个人挂件优先，查不到退回房间的限时牌前缀（同斗鱼的优先级）；两者各用各的缓存键。
	mux.HandleFunc("GET /assets/medal/room/{room}/{attr}", func(w http.ResponseWriter, r *http.Request) {
		if attr := r.PathValue("attr"); attr != "" && attr != "0" {
			if _, ok := s.resolve("attr", attr); ok {
				s.serve(w, r, "attr", attr)
				return
			}
		}
		s.serve(w, r, "room", r.PathValue("room"))
	})
}

// 404 也让浏览器记十分钟：多数房间没有专属牌前缀，每换一个房间重问一次是浪费。
func iconNotFound(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "public, max-age=600")
	http.Error(w, "icon not found", http.StatusNotFound)
}

func (s *iconStore) serve(w http.ResponseWriter, r *http.Request, kind, key string) {
	target, ok := s.resolve(kind, key)
	if !ok {
		iconNotFound(w)
		return
	}
	cacheKey := kind + ":" + key
	asset := s.cached(cacheKey)
	if asset == nil {
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		fetched, err := s.fetch(ctx, cacheKey, target, nil)
		if err != nil || fetched == nil {
			// 别让浏览器把上游的一次抽风记一天。
			w.Header().Set("Cache-Control", "no-store")
			http.Error(w, "icon unavailable", http.StatusBadGateway)
			return
		}
		asset = fetched
	}
	w.Header().Set("Content-Type", asset.contentType)
	w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", iconBrowserMaxAge))
	w.Header().Set("ETag", asset.etag)
	if match := r.Header.Get("If-None-Match"); match != "" && strings.Contains(match, asset.etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(asset.body)))
	w.Write(asset.body)
}
