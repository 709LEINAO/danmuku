package main

import (
	"context"
	"sync"
)

// 同一个键同一时刻只出门一次，其余调用方在 done 上等；等待期间自己的 ctx 结束就先走，
// 不拖累领头者，也不影响其他跟随者。
//
// 落缓存要放在 fetch 里面：fetch 返回之后、跟随者被放行之前缓存已经是新的，所以没人
// 会读到上一轮的时间戳。道具表「回源失败就沿用旧表」也靠这一点——fetch 的返回值就是
// 所有人拿到的那一份，领头者改写它，跟随者跟着拿到旧表而不是错误。
//
// 这里只收拢领头/跟随这段骨架。TTL、退避、陈旧兜底三处各不相同，留在各自的 store 里；
// 硬塞进来只会在这里长出一排开关。
type flightGroup[K comparable, V any] struct {
	mu       sync.Mutex
	inflight map[K]*flightCall[V]
}

type flightCall[V any] struct {
	done  chan struct{}
	value V
	err   error
}

func (g *flightGroup[K, V]) do(ctx context.Context, key K, fetch func() (V, error)) (V, error) {
	g.mu.Lock()
	if call := g.inflight[key]; call != nil {
		g.mu.Unlock()
		select {
		case <-call.done:
			return call.value, call.err
		case <-ctx.Done():
			var zero V
			return zero, ctx.Err()
		}
	}
	call := &flightCall[V]{done: make(chan struct{})}
	if g.inflight == nil {
		g.inflight = make(map[K]*flightCall[V])
	}
	g.inflight[key] = call
	g.mu.Unlock()

	call.value, call.err = fetch()

	g.mu.Lock()
	delete(g.inflight, key)
	g.mu.Unlock()
	close(call.done)
	return call.value, call.err
}

// 全站只有一份要取的场合（道具表、查分站名册）：键退化成空结构体。
type flight[V any] struct{ group flightGroup[struct{}, V] }

func (f *flight[V]) do(ctx context.Context, fetch func() (V, error)) (V, error) {
	return f.group.do(ctx, struct{}{}, fetch)
}
