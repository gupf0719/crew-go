//go:build integration

// Package aguiload 提供持久化取消补偿的集成压测负载。
package aguiload

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

const Sessions = 20000
const Pending = 8

// Key 生成与 Handler 同形的存储键。
func Key(thread string) string {
	return fmt.Sprintf("agui-thread:%x", sha256.Sum256([]byte("6:tenant"+thread)))
}

type countedStore struct {
	agui.SessionStore
	pages atomic.Int64
}

func (s *countedStore) ListCancellations(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	s.pages.Add(1)
	return s.SessionStore.ListCancellations(ctx, cursor, limit)
}

// Run 测首次清理延迟与一个空闲轮询间隔内的后台负载。调用方使用隔离的真实后端。
func Run(t *testing.T, store agui.SessionStore, replicas int, counters func() string) {
	t.Helper()
	g, err := compose_v2.NewBuilder().WithName("load").AddNode("unused", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil }).AddEdge(compose_v2.StartNode, "unused").AddEdge("unused", compose_v2.EndNode).Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx := compose_v2.WithCheckpointerCtx(t.Context(), store)
	keys := make(map[string]string, Pending)
	for i := 0; i < Pending; i++ {
		thread := fmt.Sprintf("pending-%d", i)
		key := Key(thread)
		sid := "load-" + thread
		func() {
			cx, a, err := compose_v2.BeginTaskActivity(ctx, key, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Release()
			cx, b, err := compose_v2.BeginTaskActivity(cx, sid, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Release()
			if err := store.SaveNS(cx, sid, "", &compose_v2.Checkpoint{Status: compose_v2.CPStatusPending}); err != nil {
				t.Fatal(err)
			}
			r := &agui.SessionRecord{TaskID: sid, Closing: true, Cancellation: &agui.SessionCancellation{Owner: "tenant", ThreadID: thread, InterruptIDs: []string{"approval"}}}
			if _, err := store.SaveSession(cx, key, r, ""); err != nil {
				t.Fatal(err)
			}
		}()
		keys[key] = sid
	}
	wrapped := &countedStore{SessionStore: store}
	// 预热发现路径，单独报告一次性迁移与周期扫描。
	warmStart := time.Now()
	var warmPages int
	for cursor := ""; ; {
		_, next, err := store.ListCancellations(ctx, cursor, 64)
		if err != nil {
			t.Fatal(err)
		}
		warmPages++
		if next == "" {
			break
		}
		cursor = next
	}
	t.Logf("warm_pages=%d warm_elapsed=%s before %s", warmPages, time.Since(warmStart), counters())
	start := time.Now()
	servers := make([]*agui.Server, 0, replicas)
	for i := 0; i < replicas; i++ {
		h := agui.Handler(func(h hook.Hooks) agent_v2.Agent {
			return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("load"), agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return nil }))
		}, agui.WithSessionStore(wrapped), agui.WithOwnerResolver(func(context.Context) string { return "tenant" }))
		servers = append(servers, h)
		defer h.Close()
	}
	var first, last time.Duration
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	for len(keys) > 0 {
		select {
		case <-deadline.C:
			t.Fatalf("timed out with %d pending cancellations", len(keys))
		case <-ticker.C:
			for key, sid := range keys {
				r, err := store.LoadSession(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
				if r != nil {
					continue
				}
				ok, err := store.ExistsNS(ctx, sid, "")
				if err != nil || ok {
					t.Fatalf("checkpoint still exists: %v %v", ok, err)
				}
				last = time.Since(start)
				if first == 0 {
					first = last
				}
				delete(keys, key)
			}
		}
	}
	time.Sleep(time.Until(start.Add(6200 * time.Millisecond)))
	for _, h := range servers {
		_ = h.Close()
	}
	if os.Getenv("CREW_AGUI_CHECK_LOAD") == "1" && wrapped.pages.Load() > 3 {
		t.Fatalf("duplicate or full discovery: %d pages in the observation window", wrapped.pages.Load())
	}
	t.Logf("replicas=%d sessions=%d pending=%d list_calls=%d first=%s last=%s after %s", replicas, Sessions, Pending, wrapped.pages.Load(), first, last, counters())
}
