package redis_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"crew-go/redis"
	"crew-go/store"
)

func TestListNamespacesExcludesDeletedAndExpiredBeforePagination(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := t.Context()
	st := redis.NewStore(client)
	root := store.NS("projects")
	deleted, expired := store.NS("projects", "a"), store.NS("projects", "b", "child")
	live, tail := store.NS("projects", "c"), store.NS("projects", "d", "child")
	for _, ns := range []store.Namespace{deleted, live, tail} {
		if err := st.Put(ctx, ns, "key", "value"); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Put(ctx, expired, "key", "short", store.WithTTL(300*time.Microsecond)); err != nil {
		t.Fatal(err)
	}
	if item, err := st.Get(ctx, expired, "key"); err != nil || item == nil {
		t.Fatalf("推进时钟前读不到短 TTL 条目: %v, %v", item, err)
	}
	if err := st.Delete(ctx, deleted, "key"); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(time.Millisecond)
	if item, err := st.Get(ctx, expired, "key"); err != nil || item != nil {
		t.Errorf("小于 1 毫秒的正 TTL 应到期: %v, %v", item, err)
	}
	for offset, want := range []store.Namespace{live, tail[:2]} {
		names, err := st.ListNamespaces(ctx, store.WithListPrefix(root), store.WithListMaxDepth(2),
			store.WithListLimit(1), store.WithListOffset(offset))
		if err != nil || len(names) != 1 || !names[0].Equal(want) {
			t.Errorf("offset %d 的 namespace 应为 %v，实际 %v (%v)", offset, want, names, err)
		}
	}
	for _, ns := range []store.Namespace{deleted, expired} {
		indexed, err := client.SIsMember(ctx, "crew:store:nsidx", ns.String()).Result()
		if err != nil || indexed {
			t.Errorf("空 namespace %v 仍占索引: %v, %v", ns, indexed, err)
		}
	}
}

func TestListNamespacesBackfillsLegacyMembersWithoutMovingItems(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := t.Context()
	const prefix = "old[projects]"
	legacy, alias := store.NS("projects", "active"), store.NS("projects", "active:legacy")
	const record = `{"value":"saved","created_at":"2026-01-01T08:00:00Z","updated_at":"2026-01-01T08:00:00Z"}`
	if err := client.Set(ctx, prefix+":store:item:projects.active:legacy:key:", record, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.SAdd(ctx, prefix+":store:nsidx", "projects.a-empty", legacy.String(), alias.String()).Err(); err != nil {
		t.Fatal(err)
	}
	st := redis.NewStore(client, redis.WithStorePrefix(prefix))
	names, err := st.ListNamespaces(ctx)
	if err != nil || len(names) != 2 || !names[0].Equal(legacy) || !names[1].Equal(alias) {
		t.Fatalf("应从旧格式恢复已知的候选且回收空名称: %v, %v", names, err)
	}
	if item, err := st.Get(ctx, legacy, "legacy:key:"); err != nil || item == nil || item.Value != "saved" {
		t.Fatalf("回填改动了旧条目: %v, %v", item, err)
	}
	if err := st.Put(ctx, alias, "key:", "updated"); err != nil {
		t.Fatal(err)
	}
	names, err = st.ListNamespaces(ctx)
	if err != nil || len(names) != 1 || !names[0].Equal(alias) {
		t.Fatalf("重写后只应保留准确归属: %v, %v", names, err)
	}
}

func TestStorePutReclaimsExpiredNamespaceMembersWithoutListing(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := t.Context()
	st := redis.NewStore(client)
	now := time.Now().Truncate(time.Second)
	mr.SetTime(now)
	ns, transient := store.NS("hot", "mixed"), store.NS("hot", "transient")
	permanentKey := strings.Repeat("a:", 1500) + "key"
	if err := st.Put(ctx, ns, permanentKey, "permanent"); err != nil {
		t.Fatal(err)
	}
	for round := range 4 {
		for i := range 24 {
			if err := st.Put(ctx, ns, fmt.Sprintf("temp-%d-%d", round, i), "expires", store.WithTTL(time.Second)); err != nil {
				t.Fatal(err)
			}
		}
		now = now.Add(2 * time.Second)
		mr.SetTime(now)
		mr.FastForward(2 * time.Second)
	}
	if err := st.Put(ctx, transient, "one", "expires", store.WithTTL(time.Second)); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(2 * time.Second)
	if err := st.Put(ctx, ns, "latest", "live"); err != nil {
		t.Fatal(err)
	}
	index := func(ns store.Namespace) string {
		return "crew:store:nsitems:" + hex.EncodeToString([]byte(ns.String()))
	}
	if size, err := client.ZCard(ctx, index(ns)).Result(); err != nil || size < 2 || size > 10 {
		t.Fatalf("无枚举时写入应持续回收过期成员，索引中还有 %d 项，err=%v", size, err)
	}
	if exists, err := client.Exists(ctx, index(transient)).Result(); err != nil || exists != 0 {
		t.Fatalf("只含 TTL 项的索引应自身到期: %d, %v", exists, err)
	}
	if item, err := st.Get(ctx, ns, permanentKey); err != nil || item == nil || item.Value != "permanent" {
		t.Fatalf("含多个冒号的永久条目应保留: %v, %v", item, err)
	}
}

type namespaceIndexGate struct {
	command          string
	waiting, release chan struct{}
	once             sync.Once
}

func (*namespaceIndexGate) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (*namespaceIndexGate) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

func (g *namespaceIndexGate) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, command goredis.Cmder) error {
		if command.Name() == g.command {
			g.once.Do(func() {
				close(g.waiting)
				select {
				case <-g.release:
				case <-ctx.Done():
				}
			})
		}
		return next(ctx, command)
	}
}

func TestListNamespacesUsesLiveOwnershipDuringConcurrentPut(t *testing.T) {
	for _, phase := range []string{"prune", "backfill"} {
		t.Run(phase, func(t *testing.T) {
			mr := miniredis.RunT(t)
			client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
			reader := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { _ = client.Close(); _ = reader.Close() })
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			former := store.NS("projects", "team")
			current := former
			command := "evalsha"
			st := redis.NewStore(client)
			if err := st.Put(ctx, former, "old", "old", store.WithTTL(time.Second)); err != nil {
				t.Fatal(err)
			}
			if phase == "backfill" {
				current = store.NS("projects", "team:next")
				command = "scan"
			} else if names, err := st.ListNamespaces(ctx); err != nil || len(names) != 1 {
				t.Fatalf("索引预热失败: %v, %v", names, err)
			}
			mr.FastForward(2 * time.Second)
			gate := &namespaceIndexGate{command: command, waiting: make(chan struct{}), release: make(chan struct{})}
			reader.AddHook(gate)
			type outcome struct {
				names []store.Namespace
				err   error
			}
			done := make(chan outcome, 1)
			go func() {
				names, err := redis.NewStore(reader).ListNamespaces(ctx)
				done <- outcome{names, err}
			}()
			select {
			case <-gate.waiting:
			case result := <-done:
				t.Fatalf("未进入索引维护: %+v", result)
			case <-ctx.Done():
				t.Fatal("未到达索引维护屏障")
			}
			if err := st.Put(ctx, current, "new", "live"); err != nil {
				t.Fatal(err)
			}
			close(gate.release)
			result := <-done
			if result.err != nil || len(result.names) != 1 || !result.names[0].Equal(current) {
				t.Fatalf("并发写入应留下实际归属 %v: %+v", current, result)
			}
			if item, err := st.Get(ctx, current, "new"); err != nil || item == nil || item.Value != "live" {
				t.Fatalf("并发写入结果丢失: %v, %v", item, err)
			}
		})
	}
}
