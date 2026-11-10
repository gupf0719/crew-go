package agui

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// ttlFixture 造一个带 TTL 的 handler + 一个 checkpointer，返回「某个 sid 还有没有存档」的探针。
func ttlFixture(t *testing.T, ttl time.Duration) (*handler, compose_v2.Checkpointer, func(string) bool) {
	t.Helper()
	cp := compose_v2.NewInMemoryCheckpointer()
	h := &handler{
		cp:          cp,
		maxSessions: DefaultMaxSessions,
		sessionTTL:  ttl,
		sessions:    map[sessionKey]*sessionEntry{},
	}
	hasCP := func(sid string) bool {
		pend, err := cp.ListPending(context.Background(), sid)
		if err != nil {
			t.Fatalf("ListPending: %v", err)
		}
		return len(pend) > 0
	}
	return h, cp, hasCP
}

// seedPending 往表里塞一条「挂起且已闲置 idle」的会话，并给它落一份 cp。
func seedPending(t *testing.T, h *handler, cp compose_v2.Checkpointer, key sessionKey, idle time.Duration) string {
	t.Helper()
	sess := agent_v2.NewRunSession()
	if err := cp.SaveNS(context.Background(), sess.RunID, "", &compose_v2.Checkpoint{
		GraphID:           "g",
		Status:            compose_v2.CPStatusPending,
		PendingInterrupts: []*compose_v2.AskQuery{{InterruptID: "q1", Question: "等回答"}},
	}); err != nil {
		t.Fatalf("SaveNS: %v", err)
	}
	h.sessions[key] = &sessionEntry{sess: sess, touchedAt: time.Now().Add(-idle).UnixNano()}
	return sess.RunID
}

// TestSessionTTL_ReclaimsAbandonedAndDropsCP 钉住**正向**的那一半：过期的挂起会话真的被
// 回收了，**而且它的 cp 真的被清了**。
//
// ⚠️ 只断言「表里没了」不够——那在「回收了但忘了清 cp」这个退化下照样为真，而漏掉的正是
// 这个特性存在的理由（cp 里装的是整条消息历史，它才是占地方的那个）。
func TestSessionTTL_ReclaimsAbandonedAndDropsCP(t *testing.T) {
	h, cp, hasCP := ttlFixture(t, time.Hour)
	gone := sessionKey{owner: "o", threadID: "abandoned"}
	sid := seedPending(t, h, cp, gone, 2*time.Hour)
	if !hasCP(sid) {
		t.Fatalf("fixture 没搭对：种下的会话该有 cp")
	}

	// 另一个人发请求 → 顺手扫
	_, evicted, ok := h.acquireForRun(sessionKey{owner: "other", threadID: "t2"})
	if !ok {
		t.Fatalf("acquireForRun 应成功")
	}
	// caller 负责清 cp——这里手动走一遍它那段
	h.cleanupEvicted(t.Context(), evicted)
	if _, still := h.sessions[gone]; still {
		t.Errorf("闲置 2 小时（TTL=1 小时）的会话没被回收")
	}
	if hasCP(sid) {
		t.Errorf("会话回收了但 cp 还在——那正是这个特性要清的东西")
	}
}

func TestServeRun_RetainsPendingTaskWhenCheckpointCleanupIsBusy(t *testing.T) {
	h, cp, hasCP := ttlFixture(t, 0)
	key := sessionKey{owner: "o", threadID: "busy-task"}
	sid := seedPending(t, h, cp, key, 0)
	previous := h.sessions[key]
	touchedAt := previous.touchedAt
	previous.open = map[string]bool{"q1": true}
	_, activity, err := compose_v2.BeginTaskActivity(t.Context(), sid, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer activity.Release()

	w := httptest.NewRecorder()
	sse, ok := NewSSEWriter(w)
	if !ok {
		t.Fatal("测试需要支持 SSE 的 ResponseWriter")
	}
	h.serveRun(t.Context(), sse, &RunAgentInput{ThreadID: key.threadID,
		Messages: []AGUIMessage{{Role: "user", Content: "开新任务"}}}, key)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"code":"busy"`) {
		t.Fatalf("应拒绝覆盖正在执行的任务，实际 HTTP %d %s", w.Code, w.Body.String())
	}
	if !hasCP(sid) || h.sessions[key] != previous || previous.busy || !previous.open["q1"] || previous.touchedAt != touchedAt {
		t.Fatal("应保留旧任务的存档、挂起问题、续跑入口和闲置时间")
	}
	activity.Release()
	if err := h.dropCP(t.Context(), sid); err != nil || hasCP(sid) {
		t.Fatal("任务执行结束后应能清理存档")
	}
}

func TestServeRun_DoesNotRestoreEntryDeletedBeforeTaskConflict(t *testing.T) {
	h, cp, hasCP := ttlFixture(t, 0)
	key := sessionKey{owner: "o", threadID: "late-conflict"}
	sid := seedPending(t, h, cp, key, 0)
	h.build = func(hooks hook.Hooks) agent_v2.Agent {
		if hasCP(sid) {
			t.Fatal("旧存档应在构造新 Agent 前清理")
		}
		_, activity, err := compose_v2.BeginTaskActivity(t.Context(), sid, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(activity.Release)
		return agent_v2.NewBasicAgent(agent_v2.WithName("busy-replace"), agent_v2.WithModel(&capModel{}), agent_v2.WithHooks(hooks))
	}
	w := httptest.NewRecorder()
	sse, ok := NewSSEWriter(w)
	if !ok {
		t.Fatal("测试需要支持 SSE 的 ResponseWriter")
	}
	ctx := compose_v2.WithCheckpointerCtx(t.Context(), cp)
	h.serveRun(ctx, sse, &RunAgentInput{ThreadID: key.threadID,
		Messages: []AGUIMessage{{Role: "user", Content: "开新任务"}}}, key)
	if !strings.Contains(w.Body.String(), `"code":"busy"`) || h.sessions[key] != nil {
		t.Fatalf("删档后的任务冲突应报 busy，旧入口应移除，实际 %s", w.Body.String())
	}
}

// TestSessionTTL_BusyEntryNeverReclaimed 钉住在途的动不得。
//
// 在途 entry 正被一个请求驱动着，删了它之后那个请求的 storeInterrupt 会写到一个已经不在
// 表里的 entry 上（对 nil 是 no-op 不 panic，但**挂起会静默丢失**）。
func TestSessionTTL_BusyEntryNeverReclaimed(t *testing.T) {
	h, cp, _ := ttlFixture(t, time.Hour)
	busyKey := sessionKey{owner: "o", threadID: "inflight"}
	seedPending(t, h, cp, busyKey, 2*time.Hour)
	h.sessions[busyKey].busy = true

	_, _, _ = h.acquireForRun(sessionKey{owner: "other", threadID: "t2"})
	if _, still := h.sessions[busyKey]; !still {
		t.Errorf("在途会话被回收了——它正被一个请求驱动着，删了之后那个请求的挂起会静默丢失")
	}
}

func TestSessionReclaim_RetriesAfterActiveTask(t *testing.T) {
	for _, tc := range []struct {
		name string
		ttl  time.Duration
		max  int
	}{
		{name: "TTL", ttl: time.Hour, max: DefaultMaxSessions},
		{name: "LRU", max: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, cp, hasCP := ttlFixture(t, tc.ttl)
			h.maxSessions = tc.max
			h.build = func(hook.Hooks) agent_v2.Agent { return nil }
			old := sessionKey{owner: "o", threadID: "old"}
			sid := seedPending(t, h, cp, old, 2*time.Hour)
			_, activity, err := compose_v2.BeginTaskActivity(t.Context(), sid, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer activity.Release()

			trigger := func() {
				t.Helper()
				key := sessionKey{owner: "o", threadID: "new"}
				sse, ok := NewSSEWriter(httptest.NewRecorder())
				if !ok {
					t.Fatal("测试需要支持 SSE 的 ResponseWriter")
				}
				h.serveRun(t.Context(), sse, &RunAgentInput{ThreadID: key.threadID,
					Messages: []AGUIMessage{{Role: "user", Content: "开新任务"}}}, key)
			}

			trigger()
			if h.sessions[old] == nil || !hasCP(sid) {
				t.Fatal("回收应保留仍在执行的任务的会话和存档")
			}
			activity.Release()
			h.lastSweep = 0
			trigger()
			if h.sessions[old] != nil || hasCP(sid) {
				t.Fatal("任务执行结束后应能回收会话和存档")
			}
		})
	}
}

// TestSessionTTL_ZeroMeansNever 钉住默认（0）不按时间回收。
func TestSessionTTL_ZeroMeansNever(t *testing.T) {
	h, cp, _ := ttlFixture(t, 0)
	old := sessionKey{owner: "o", threadID: "ancient"}
	seedPending(t, h, cp, old, 300*24*time.Hour)

	_, _, _ = h.acquireForRun(sessionKey{owner: "other", threadID: "t2"})
	if _, still := h.sessions[old]; !still {
		t.Errorf("TTL=0 是「不按时间回收」，闲置 300 天的也不该动")
	}
}

// TestSessionTTL_BusyEarlyReturnStillReportsExpired 钉住**我实现时自己踩的那个坑**：
// busy 早退路径上把扫出来的 sid 丢在地上。
//
// sweep 已经把 entry 从表里删了，RunID 掉在地上就是一堆没人清的 cp 孤儿——而且这条路径
// 上调用方拿到 ok=false 直接返回，**没有第二次机会**。
func TestSessionTTL_BusyEarlyReturnStillReportsExpired(t *testing.T) {
	h, cp, _ := ttlFixture(t, time.Hour)
	expired := sessionKey{owner: "o", threadID: "abandoned"}
	sid := seedPending(t, h, cp, expired, 2*time.Hour)

	// 目标键自己在途 ⟹ acquireForRun 走 busy 早退
	target := sessionKey{owner: "o", threadID: "inflight"}
	h.sessions[target] = &sessionEntry{busy: true, touchedAt: time.Now().UnixNano()}

	_, evicted, ok := h.acquireForRun(target)
	h.cleanupEvicted(t.Context(), evicted)
	if ok {
		t.Fatalf("fixture 没搭对：目标键在途，应走 busy 早退")
	}
	if _, still := h.sessions[expired]; still {
		t.Fatalf("fixture 没搭对：过期那条该已被 sweep 删掉")
	}
	found := false
	for _, task := range evicted {
		if task.sid == sid {
			found = true
		}
	}
	if !found {
		t.Errorf("busy 早退路径把扫出来的 sid 丢了：evicted=%v，缺 %q——"+
			"entry 已从表里删掉，这个 cp 再也没人清", evicted, sid)
	}
}

// TestSessionTTL_SweepIsThrottled 钉住限频：扫一次是 O(n)，每个请求都扫会让一万条的表
// 变成每请求一万次比较。
func TestSessionTTL_SweepIsThrottled(t *testing.T) {
	h, cp, _ := ttlFixture(t, time.Hour)
	seedPending(t, h, cp, sessionKey{owner: "o", threadID: "a"}, 2*time.Hour)

	// 第一次请求触发扫描
	_, evicted, _ := h.acquireForRun(sessionKey{owner: "x", threadID: "1"})
	h.cleanupEvicted(t.Context(), evicted)
	first := h.lastSweep
	if first == 0 {
		t.Fatalf("fixture 没搭对：第一次请求该扫一遍")
	}
	// 紧接着再来一个 → 在 TTL/4 内，不该重扫
	seedPending(t, h, cp, sessionKey{owner: "o", threadID: "b"}, 2*time.Hour)
	_, evicted, _ = h.acquireForRun(sessionKey{owner: "x", threadID: "2"})
	h.cleanupEvicted(t.Context(), evicted)
	if h.lastSweep != first {
		t.Errorf("TTL/4 内重复扫了：每请求 O(n) 在一万条的表上就是每请求一万次比较")
	}
	if _, still := h.sessions[sessionKey{owner: "o", threadID: "b"}]; !still {
		t.Errorf("限频期内不该回收——回收最晚迟到 TTL/4 是这个设计接受的代价")
	}
}

// TestSessionTTL_ExpiredIsUnusableBeforeSweep 钉住「过期即不可用」跟「什么时候真删」是
// 两段独立的语义。
//
// 不拆开的话，回收延迟就是语义延迟——惰性扫描只在有请求时跑，而低流量部署下「下次扫描」
// 可能是几天以后，那期间一条早该过期的会话仍然续得起来。这里把限频卡死（刚扫过），让
// sweep 一定不跑，然后续发那条过期会话。
func TestSessionTTL_ExpiredIsUnusableBeforeSweep(t *testing.T) {
	h, cp, _ := ttlFixture(t, time.Hour)
	key := sessionKey{owner: "o", threadID: "stale"}
	seedPending(t, h, cp, key, 2*time.Hour)
	h.lastSweep = time.Now().UnixNano() // 刚扫过 ⟹ 限频内，sweep 一定不跑

	sess, _, _, ok, busy := h.acquireForResume(key)
	if ok || sess != nil {
		t.Errorf("过期会话仍然续得起来——语义上界被回收节奏绑架了")
	}
	if busy {
		t.Errorf("不该报 busy，它只是过期了")
	}
	if _, still := h.sessions[key]; !still {
		t.Errorf("fixture 没搭对：限频内 sweep 不该跑，entry 该还在表里——" +
			"这条要验的正是「还没删但已经不可用」")
	}
}

// TestMaxSessions_LimitIsSoftAcrossTenants 钉住**上限是软的**这个事实本身。
//
// `WithSessionTTL` 的 godoc 一度写「n 上限 DefaultMaxSessions」，据此算出「万级比较」的
// 性能忠告。实测差 20 倍——`maxSessions=5` 下 100 个租户各挂一条，表里就是 100 条。原因
// 写在 `WithMaxSessions` 自己的 godoc 里（「到达者不赢平局」⟹ 新 owner 淘汰不着别人、
// 自己唯一那条又在途 ⟹ 谁也不淘汰），两句话在同一个文件里自相矛盾了。
//
// 这条把那个数量级钉死，省得下一个人再照着一个想当然的界写忠告。
func TestMaxSessions_LimitIsSoftAcrossTenants(t *testing.T) {
	h := &handler{maxSessions: 5, sessions: map[sessionKey]*sessionEntry{}}
	const tenants = 100
	for i := 0; i < tenants; i++ {
		key := sessionKey{owner: fmt.Sprintf("tenant-%03d", i), threadID: "t"}
		if _, _, ok := h.acquireForRun(key); !ok {
			t.Fatalf("acquireForRun(%v) 失败", key)
		}
		// 挂起后释放 busy —— 模拟「用户关了页面，会话留在表里」
		h.sessions[key].sess = agent_v2.NewRunSession()
		h.releaseBusy(key)
	}
	if got := len(h.sessions); got < tenants {
		t.Errorf("maxSessions=5、%d 个租户各挂 1 条，表里只剩 %d 条——"+
			"如果真被压到 5 条附近，说明淘汰跨租户生效了，那是另一个更严重的问题（跨租户 DoS）",
			tenants, got)
	}
	t.Logf("maxSessions=5，%d 个租户各挂 1 条 → 表里实际 %d 条（上限是软的，"+
		"增量挂在**身份数**上不挂在请求数上）", tenants, len(h.sessions))
}

func TestSessionTTL_InFlightReclaimDoesNotCountForLRU(t *testing.T) {
	h, cp, hasCP := ttlFixture(t, time.Hour)
	h.maxSessions = 3
	expired := sessionKey{owner: "victim", threadID: "expired"}
	live := sessionKey{owner: "victim", threadID: "live"}
	busy := sessionKey{owner: "busy", threadID: "busy"}
	newcomer := sessionKey{owner: "newcomer", threadID: "newcomer"}
	seedPending(t, h, cp, expired, 2*time.Hour)
	liveSID := seedPending(t, h, cp, live, 0)
	h.sessions[busy] = &sessionEntry{busy: true, touchedAt: time.Now().UnixNano()}
	_, first, ok := h.acquireForRun(busy)
	if ok || len(first) != 1 || first[0].key != expired {
		t.Fatalf("fixture: first request must claim expired cleanup only: ok=%v evictions=%+v", ok, first)
	}
	t.Cleanup(func() { h.cleanupEvicted(t.Context(), first) })
	_, second, ok := h.acquireForRun(newcomer)
	if !ok {
		t.Fatal("fixture: new request must start")
	}
	h.cleanupEvicted(t.Context(), second)
	if h.sessions[live] == nil || !hasCP(liveSID) {
		t.Fatalf("pending eviction was double charged; live tenant checkpoint destroyed: evictions=%+v", second)
	}
}

type remoteBusyCheckpointer struct {
	compose_v2.Checkpointer
	t          *testing.T
	h          *handler
	remoteSID  string
	remoteBusy bool
	token      string
}

func (c *remoteBusyCheckpointer) CheckpointTaskLeases() compose_v2.TaskLeaseStore { return c }
func (c *remoteBusyCheckpointer) TryAcquireTaskLease(_ context.Context, sid, token string, _ time.Duration) (bool, error) {
	c.t.Helper()
	if !c.h.mu.TryLock() {
		c.t.Fatal("lease backend called under session lock")
	}
	c.h.mu.Unlock()
	if sid == c.remoteSID && c.remoteBusy {
		return false, nil
	}
	c.token = token
	return true, nil
}
func (c *remoteBusyCheckpointer) RenewTaskLease(_ context.Context, _, _ string, _ time.Duration) (bool, error) {
	return true, nil
}
func (c *remoteBusyCheckpointer) ReleaseTaskLease(_ context.Context, _, token string) error {
	c.token = ""
	return nil
}
func (c *remoteBusyCheckpointer) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	c.t.Helper()
	if c.token == "" || c.token != compose_v2.CheckpointTaskLeaseToken(ctx, c, sid) {
		c.t.Fatal("DeleteSubtree missing lease token")
	}
	return c.Checkpointer.DeleteSubtree(ctx, sid, ns)
}

func TestSessionTTL_RemoteBusyKeepsEntryUntilReclaim(t *testing.T) {
	h, cp, hasCP := ttlFixture(t, time.Hour)
	old := sessionKey{owner: "o", threadID: "old"}
	sid := seedPending(t, h, cp, old, 2*time.Hour)
	h.cp = &remoteBusyCheckpointer{Checkpointer: cp, t: t, h: h, remoteSID: sid, remoteBusy: true}
	oldEntry := h.sessions[old]
	h.build = func(hook.Hooks) agent_v2.Agent { return nil }
	run := func(key sessionKey) int {
		w := httptest.NewRecorder()
		sse, _ := NewSSEWriter(w)
		h.serveRun(t.Context(), sse, &RunAgentInput{ThreadID: key.threadID, Messages: []AGUIMessage{{Role: "user", Content: "new"}}}, key)
		return w.Code
	}
	if code := run(old); code != http.StatusConflict || h.sessions[old] != oldEntry || !hasCP(sid) {
		t.Fatalf("same-thread remote conflict should keep old entry, got code=%d", code)
	}
	h.lastSweep = 0
	run(sessionKey{owner: "o", threadID: "new"})
	if h.sessions[old] != oldEntry || oldEntry.busy || !hasCP(sid) {
		t.Fatal("remote busy TTL sweep should leave old entry idle for retry")
	}
	h.cp.(*remoteBusyCheckpointer).remoteBusy = false
	h.lastSweep = 0
	run(sessionKey{owner: "o", threadID: "new"})
	if h.sessions[old] != nil || hasCP(sid) {
		t.Fatal("TTL retry did not reclaim checkpoint")
	}
}

func TestSessionCapacity_SkipsRemoteBusyOldest(t *testing.T) {
	h, cp, hasCP := ttlFixture(t, 0)
	h.maxSessions = 2
	busy := sessionKey{owner: "one-owner", threadID: "oldest"}
	idle := sessionKey{owner: "one-owner", threadID: "idle"}
	new1 := sessionKey{owner: "one-owner", threadID: "new1"}
	busySID := seedPending(t, h, cp, busy, 2*time.Hour)
	idleSID := seedPending(t, h, cp, idle, time.Hour)
	h.cp = &remoteBusyCheckpointer{Checkpointer: cp, t: t, h: h, remoteSID: busySID, remoteBusy: true}
	_, evicted, ok := h.acquireForRun(new1)
	if !ok {
		t.Fatal("fixture must reserve new run")
	}
	h.cleanupEvicted(t.Context(), evicted)
	if h.sessions[busy] == nil || !hasCP(busySID) {
		t.Fatal("busy remote task was evicted")
	}
	if h.sessions[idle] != nil || hasCP(idleSID) {
		t.Fatal("remote-busy oldest blocked eviction of an eligible idle task")
	}
}

func TestServeRun_NestedBusyDropsOwnCheckpoint(t *testing.T) {
	h, cp, _ := ttlFixture(t, 0)
	child := agent_v2.NewRunSession()
	_, activity, err := compose_v2.BeginTaskActivity(t.Context(), "", []string{child.RunID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(activity.Release)
	var rootSID string
	h.build = func(hooks hook.Hooks) agent_v2.Agent {
		b := compose_v2.NewBuilder().WithName("parent")
		b.AddNode("child", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			rootSID = agent_v2.GetSession(ctx).RunID
			sub := agent_v2.NewBasicAgent(agent_v2.WithName("child"), agent_v2.WithModel(&capModel{}))
			_, err := sub.Run(agent_v2.DetachTask(ctx), child, schema.UserMessage("go"))
			return nil, err
		})
		b.AddEdge(compose_v2.StartNode, "child")
		b.AddEdge("child", compose_v2.EndNode)
		graph, err := b.Compile()
		if err != nil {
			t.Fatal(err)
		}
		return agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName("parent"), agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }), agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return schema.AssistantMessage("done") }))
	}
	w := httptest.NewRecorder()
	sse, _ := NewSSEWriter(w)
	key := sessionKey{owner: "o", threadID: "busy-child"}
	ctx := compose_v2.WithCheckpointerCtx(t.Context(), cp)
	h.serveRun(ctx, sse, &RunAgentInput{ThreadID: key.threadID, Messages: []AGUIMessage{{Role: "user", Content: "new"}}}, key)
	if rootSID == "" || !strings.Contains(w.Body.String(), `"code":"busy"`) {
		t.Fatalf("fixture expected nested busy SSE: %s", w.Body.String())
	}
	all, err := cp.LoadAllInSession(t.Context(), rootSID)
	if err != nil || len(all) > 0 {
		t.Fatalf("nested busy left own checkpoint with no entry: len=%d entry=%v err=%v", len(all), h.sessions[key], err)
	}
}
