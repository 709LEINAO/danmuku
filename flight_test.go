package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 一群人同时要同一样东西，只该出门一次，且每个人拿到的都是那一次的结果。
func TestFlightSharesOneCall(t *testing.T) {
	var group flight[int]
	var calls atomic.Int64
	release := make(chan struct{})
	const followers = 32
	var wait sync.WaitGroup
	results := make([]int, followers)
	errs := make([]error, followers)
	started := make(chan struct{}, followers)
	for i := range followers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			started <- struct{}{}
			results[i], errs[i] = group.do(context.Background(), func() (int, error) {
				calls.Add(1)
				<-release
				return 7, nil
			})
		}()
	}
	for range followers {
		<-started
	}
	// 给跟随者时间挂到 done 上，再放领头者走。
	time.Sleep(50 * time.Millisecond)
	close(release)
	wait.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("回源 %d 次，期望 1 次", got)
	}
	for i := range followers {
		if results[i] != 7 || errs[i] != nil {
			t.Fatalf("第 %d 个拿到 %d / %v", i, results[i], errs[i])
		}
	}
}

// 跟随者自己的 ctx 断了就先走，不能把领头者一起拖停，也不能让其他人拿不到结果。
func TestFlightFollowerLeavesOnContext(t *testing.T) {
	var group flight[int]
	release := make(chan struct{})
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		group.do(context.Background(), func() (int, error) {
			<-release
			return 1, nil
		})
	}()
	// 等领头者占住位置。
	for {
		group.group.mu.Lock()
		busy := len(group.group.inflight) > 0
		group.group.mu.Unlock()
		if busy {
			break
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := group.do(ctx, func() (int, error) { t.Error("跟随者不该自己回源"); return 0, nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("跟随者 err = %v，期望 context.Canceled", err)
	}
	select {
	case <-leaderDone:
		t.Fatal("跟随者离开把领头者也带走了")
	default:
	}
	close(release)
	<-leaderDone
}

// 上一趟结束之后，下一趟要真的重新出门（否则缓存永远不会刷新）。
func TestFlightRefetchesAfterDone(t *testing.T) {
	var group flight[int]
	var calls atomic.Int64
	for range 3 {
		if _, err := group.do(context.Background(), func() (int, error) {
			return int(calls.Add(1)), nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("回源 %d 次，期望 3 次", got)
	}
}

// 不同键互不排队：一张图卡住不该挡住别的图。
func TestFlightGroupKeysAreIndependent(t *testing.T) {
	var group flightGroup[string, string]
	blocked := make(chan struct{})
	go group.do(context.Background(), "a", func() (string, error) { <-blocked; return "a", nil })
	done := make(chan string, 1)
	go func() {
		value, _ := group.do(context.Background(), "b", func() (string, error) { return "b", nil })
		done <- value
	}()
	select {
	case value := <-done:
		if value != "b" {
			t.Fatalf("键 b 拿到 %q", value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("键 b 被键 a 挡住了")
	}
	close(blocked)
}

// 领头者报错时，错误要原样交给每个跟随者。
func TestFlightSharesError(t *testing.T) {
	var group flight[int]
	want := errors.New("上游挂了")
	release := make(chan struct{})
	var wait sync.WaitGroup
	errs := make([]error, 8)
	for i := range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, errs[i] = group.do(context.Background(), func() (int, error) {
				<-release
				return 0, want
			})
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wait.Wait()
	for i, err := range errs {
		if !errors.Is(err, want) {
			t.Fatalf("第 %d 个 err = %v", i, err)
		}
	}
}
