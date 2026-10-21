package agui

import (
	"context"
	"fmt"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
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
	if _, still := h.sessions[gone]; still {
		t.Errorf("闲置 2 小时（TTL=1 小时）的会话没被回收")
	}
	// caller 负责清 cp——这里手动走一遍它那段
	for _, s := range evicted {
		h.dropCP(context.Background(), s)
	}
	if hasCP(sid) {
		t.Errorf("会话回收了但 cp 还在——那正是这个特性要清的东西")
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
	if ok {
		t.Fatalf("fixture 没搭对：目标键在途，应走 busy 早退")
	}
	if _, still := h.sessions[expired]; still {
		t.Fatalf("fixture 没搭对：过期那条该已被 sweep 删掉")
	}
	found := false
	for _, s := range evicted {
		if s == sid {
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
	_, _, _ = h.acquireForRun(sessionKey{owner: "x", threadID: "1"})
	first := h.lastSweep
	if first == 0 {
		t.Fatalf("fixture 没搭对：第一次请求该扫一遍")
	}
	// 紧接着再来一个 → 在 TTL/4 内，不该重扫
	seedPending(t, h, cp, sessionKey{owner: "o", threadID: "b"}, 2*time.Hour)
	_, _, _ = h.acquireForRun(sessionKey{owner: "x", threadID: "2"})
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

	sess, _, ok, busy := h.acquireForResume(key)
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
