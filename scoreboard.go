package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// 团播查分站的公开名册，一次请求带回全部主播，追踪多少人都只发一个。
// 对方没有 CORS 头，前端不能直连，一律经此代理。
const (
	scoreboardAnchors = "https://tuanbo.littletuan.com/api/viewer/anchors?spaceKey=fps&projectKey=s1"
	scoreboardReferer = "https://tuanbo.littletuan.com/"
	scoreboardAgent   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
	scoreboardTTL     = 45 * time.Second
	scoreboardMaxBody = 4 << 20
	// 单飞领头者的时限，与 client 超时同值。
	scoreboardLead = 12 * time.Second
)

// 上游一行 30 多个字段，只挑前端要的转出去：34 KB 压到 4 KB。
type upstreamAnchor struct {
	DouyuUID  string   `json:"douyu_uid"`
	UID       string   `json:"uid"`
	Name      string   `json:"name"`
	Nick      []string `json:"nick"`
	RID       string   `json:"rid"`
	Live      int      `json:"live_status"`
	Balance   int64    `json:"coinBalance"`
	TodayFlow int64    `json:"todayFlow"`
	AllowGift int      `json:"allow_gift"`
	AllowBomb int      `json:"allow_bomb"`
}

type upstreamAnchors struct {
	Success   bool             `json:"success"`
	Anchors   []upstreamAnchor `json:"anchors"`
	CoinName  string           `json:"coinName"`
	FlowLabel string           `json:"todayFlowLabel"`
	UpdatedAt string           `json:"updatedAt"`
}

type scoreboardAnchor struct {
	UID       string   `json:"uid"`
	Name      string   `json:"name"`
	Nick      []string `json:"nick,omitempty"`
	RID       string   `json:"rid,omitempty"`
	Live      bool     `json:"live"`
	Balance   int64    `json:"balance"`
	TodayFlow int64    `json:"todayFlow"`
	Giftable  bool     `json:"giftable"`
	Bombable  bool     `json:"bombable"`
}

type scoreboardSnapshot struct {
	Anchors   []scoreboardAnchor `json:"anchors"`
	CoinName  string             `json:"coinName"`
	FlowLabel string             `json:"flowLabel"`
	UpdatedAt string             `json:"updatedAt"`
}

type scoreboardCall struct {
	done chan struct{}
	snap scoreboardSnapshot
	err  error
}

type scoreboardStore struct {
	mu      sync.Mutex
	client  *http.Client
	cached  scoreboardSnapshot
	fetched time.Time
	call    *scoreboardCall
	now     func() time.Time
}

func newScoreboardStore() *scoreboardStore {
	return &scoreboardStore{client: &http.Client{Timeout: 12 * time.Second}, now: time.Now}
}

// 缓存期内直接回旧数据；过期时并发请求只真正发出一次。
func (s *scoreboardStore) snapshot(ctx context.Context) (scoreboardSnapshot, time.Time, error) {
	s.mu.Lock()
	if !s.fetched.IsZero() && s.now().Sub(s.fetched) < scoreboardTTL {
		snap, at := s.cached, s.fetched
		s.mu.Unlock()
		return snap, at, nil
	}
	if call := s.call; call != nil {
		s.mu.Unlock()
		select {
		case <-call.done:
			s.mu.Lock()
			at := s.fetched
			s.mu.Unlock()
			return call.snap, at, call.err
		case <-ctx.Done():
			return scoreboardSnapshot{}, time.Time{}, ctx.Err()
		}
	}
	call := &scoreboardCall{done: make(chan struct{})}
	s.call = call
	s.mu.Unlock()

	// 回源与领头者解绑：他关掉标签页不该让同在等的其他页面一起拿到 context canceled。
	lead, cancel := context.WithTimeout(context.WithoutCancel(ctx), scoreboardLead)
	call.snap, call.err = s.fetch(lead)
	cancel()

	// 先落缓存再放行跟随者，否则他们会读到上一轮的 fetched。
	s.mu.Lock()
	s.call = nil
	if call.err == nil {
		s.cached, s.fetched = call.snap, s.now()
	}
	at := s.fetched
	s.mu.Unlock()
	close(call.done)
	return call.snap, at, call.err
}

func (s *scoreboardStore) fetch(ctx context.Context) (scoreboardSnapshot, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, scoreboardAnchors, nil)
	if err != nil {
		return scoreboardSnapshot{}, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Referer", scoreboardReferer)
	request.Header.Set("User-Agent", scoreboardAgent)
	response, err := s.client.Do(request)
	if err != nil {
		return scoreboardSnapshot{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return scoreboardSnapshot{}, fmt.Errorf("查分站返回 %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, scoreboardMaxBody))
	if err != nil {
		return scoreboardSnapshot{}, err
	}
	var payload upstreamAnchors
	if err := json.Unmarshal(body, &payload); err != nil {
		return scoreboardSnapshot{}, fmt.Errorf("查分站返回的数据无法解析：%w", err)
	}
	if !payload.Success {
		return scoreboardSnapshot{}, fmt.Errorf("查分站拒绝了这次请求")
	}
	snap := scoreboardSnapshot{
		Anchors:   make([]scoreboardAnchor, 0, len(payload.Anchors)),
		CoinName:  payload.CoinName,
		FlowLabel: payload.FlowLabel,
		UpdatedAt: payload.UpdatedAt,
	}
	for _, row := range payload.Anchors {
		uid := row.DouyuUID
		if uid == "" {
			uid = row.UID
		}
		if uid == "" {
			continue
		}
		snap.Anchors = append(snap.Anchors, scoreboardAnchor{
			UID:       uid,
			Name:      row.Name,
			Nick:      row.Nick,
			RID:       row.RID,
			Live:      row.Live == 1,
			Balance:   row.Balance,
			TodayFlow: row.TodayFlow,
			Giftable:  row.AllowGift == 1,
			Bombable:  row.AllowBomb == 1,
		})
	}
	return snap, nil
}

func (s *scoreboardStore) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/scoreboard/anchors", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		snap, at, err := s.snapshot(ctx)
		if r.Context().Err() != nil {
			return
		}
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "查分站暂时读不到，请稍后再试"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"anchors":   snap.Anchors,
			"coinName":  snap.CoinName,
			"flowLabel": snap.FlowLabel,
			"updatedAt": snap.UpdatedAt,
			"cachedAt":  at.UTC(),
		})
	})
}
