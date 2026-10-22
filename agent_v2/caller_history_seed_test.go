package agent_v2_test

// 调用方前文写进历史层这一步的不变量（只有拥有这场对话的写入器写，见 caller_history_owner_test.go）：
//   - 写进去的前文按 ID 去重、排在最前：几个写入器同时判出「这场对话是空的」各写一份、或者一份落在别的记录之后，读出来
//     都只有一份、在最前面；
//   - 前文没写成（写失败）：这次运行以客户端为准，先跑的、后跑的成员都看到前文，各一份；
//   - 转交的共享视图（Rewrite / Raw）下前文只出现一次。

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// loadBarrierStore：前 n 次 Load 读完之后在屏障处会合（2 秒兜底放行），让并发的几次「判断这场对话是不是空的」都发生在
// 任何一次写之前。
type loadBarrierStore struct {
	*agent_v2.InMemoryHistoryStore
	mu    sync.Mutex
	loads int
	n     int
	wg    sync.WaitGroup
}

func newLoadBarrierStore(n int) *loadBarrierStore {
	s := &loadBarrierStore{InMemoryHistoryStore: agent_v2.NewInMemoryHistoryStore(), n: n}
	s.wg.Add(n)
	return s
}

func (s *loadBarrierStore) Load(ctx context.Context, key agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	msgs, err := s.InMemoryHistoryStore.Load(ctx, key, q)
	s.mu.Lock()
	s.loads++
	k := s.loads
	s.mu.Unlock()
	if k <= s.n {
		s.wg.Done()
		done := make(chan struct{})
		go func() { s.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
	return msgs, err
}

// callerRecords 数历史层里这场对话带前文标注的记录，按 ID 计份数。
func callerRecords(t *testing.T, st agent_v2.HistoryStore, conv string) (total int, ids map[string]int) {
	t.Helper()
	recs, err := agent_v2.LoadAllHistory(context.Background(), st, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: conv})
	if err != nil {
		t.Fatal(err)
	}
	ids = map[string]int{}
	for _, r := range recs {
		if v, _ := r.Annotations[agent_v2.CallerHistoryAnnotation].(bool); v {
			total++
			ids[r.ID]++
		}
	}
	return total, ids
}

// 同一场对话两个顶层 Run 并发、都判断出「这场对话是空的」、各写了一份前文：下一轮读出来只有一份、在最前面。
func TestCallerHistory_ConcurrentSeeds_ReadOnceAtFront(t *testing.T) {
	st := newLoadBarrierStore(2)
	ag := newCallerAgent("assistant", &callerSeenModel{reply: "答"}, agent_v2.WithHistory(st, "tenant-a"))
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := agent_v2.WithCallerHistoryCtx(threadCtx("conv-seed-race"), earlierTurns())
			if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(fmt.Sprintf("q%d", i))); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	total, ids := callerRecords(t, st, "conv-seed-race")
	if total != 4 || len(ids) != 2 {
		t.Fatalf("fixture 没搭对：应各写一份、同一组 ID（4 条记录、2 个 ID），实际 %d 条、%d 个 ID", total, len(ids))
	}

	m := &callerSeenModel{reply: "答"}
	next := newCallerAgent("assistant", m, agent_v2.WithHistory(st, "tenant-a"))
	if _, err := next.Run(threadCtx("conv-seed-race"), agent_v2.NewRunSession(), schema.UserMessage("下一问")); err != nil {
		t.Fatal(err)
	}
	got := m.call(t, 0)
	if len(got) < 2 || !slices.Equal(got[:2], wantEarlier) || countPrefixLines(got, "我叫小明") != 1 {
		t.Errorf("下一轮看到 %q，前文应只有一份、在最前面", got)
	}
}

// holdFirstLoadStore：第一次 Load 读完后停住，等 release 关闭才返回（让另一个 Run 在这期间整段跑完）。
type holdFirstLoadStore struct {
	*agent_v2.InMemoryHistoryStore
	n       atomic.Int32
	reached chan struct{}
	release chan struct{}
}

func (s *holdFirstLoadStore) Load(ctx context.Context, key agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	msgs, err := s.InMemoryHistoryStore.Load(ctx, key, q)
	if s.n.Add(1) == 1 {
		close(s.reached)
		<-s.release
	}
	return msgs, err
}

// B 先读到空、A 整段跑完、B 才把前文写进去——前文落在 A 的记录之后。B 交来的前文比 A 的多一轮（两个标签页、各带
// 各的前文）：下一轮相同的那几条只有一份，B 多出来的那轮也排在最前面（都是服务端第一次见到这场对话之前的轮次），
// 不会出现在 A 的记录之后、对话的中间。
func TestCallerHistory_SeedLandsAfterRecords_HoistedToFront(t *testing.T) {
	st := &holdFirstLoadStore{InMemoryHistoryStore: agent_v2.NewInMemoryHistoryStore(),
		reached: make(chan struct{}), release: make(chan struct{})}
	ag := newCallerAgent("assistant", &callerSeenModel{reply: "答"}, agent_v2.WithHistory(st, "tenant-a"))
	done := make(chan error, 1)
	go func() {
		priorB := append(earlierTurns(), schema.UserMessage("我住在北京"), schema.AssistantMessage("好的"))
		ctx := agent_v2.WithCallerHistoryCtx(threadCtx("conv-seed-late"), priorB)
		_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("qB"))
		done <- err
	}()
	<-st.reached
	ctxA := agent_v2.WithCallerHistoryCtx(threadCtx("conv-seed-late"), earlierTurns())
	if _, err := ag.Run(ctxA, agent_v2.NewRunSession(), schema.UserMessage("qA")); err != nil {
		t.Fatal(err)
	}
	close(st.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	m := &callerSeenModel{reply: "答"}
	next := newCallerAgent("assistant", m, agent_v2.WithHistory(st, "tenant-a"))
	if _, err := next.Run(threadCtx("conv-seed-late"), agent_v2.NewRunSession(), schema.UserMessage("下一问")); err != nil {
		t.Fatal(err)
	}
	got := m.call(t, 0)
	want := append(slices.Clone(wantEarlier), "~user:我住在北京", "~assistant:好的")
	if len(got) < 4 || !slices.Equal(got[:4], want) || countPrefixLines(got, "我叫小明") != 1 {
		t.Errorf("下一轮看到 %q，应以 %q 开头、相同的前文只有一份", got, want)
	}
}

func countPrefixLines(lines []string, sub string) int {
	n := 0
	for _, l := range lines {
		if strings.Contains(l, sub) {
			n++
		}
	}
	return n
}

// failFirstAppendStore：第一次 Append 失败（前文那一批），之后照常。
type failFirstAppendStore struct {
	*agent_v2.InMemoryHistoryStore
	n atomic.Int32
}

func (s *failFirstAppendStore) Append(ctx context.Context, key agent_v2.HistoryKey, msgs []*schema.Message) error {
	if s.n.Add(1) == 1 {
		return errors.New("transient write failure")
	}
	return s.InMemoryHistoryStore.Append(ctx, key, msgs)
}

// 前文没写成（那一次写失败）、后面的写成了：这次运行以客户端为准，先跑的 A、后跑的 B 都看到前文、各一份（B 读历史时
// 历史层已有 A 的记录、却没有前文——以服务端为准的话 B 就失忆了）。
func TestCallerHistory_SeedWriteFails_LaterMemberStillSees(t *testing.T) {
	st := &failFirstAppendStore{InMemoryHistoryStore: agent_v2.NewInMemoryHistoryStore()}
	ma, mb := &callerSeenModel{reply: "A"}, &callerSeenModel{reply: "B"}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
		AddStep("a", newCallerAgent("member_a", ma, agent_v2.WithHistory(st, "tenant-a"))).
		AddStep("b", newCallerAgent("member_b", mb, agent_v2.WithHistory(st, "tenant-a")))
	ctx := agent_v2.WithCallerHistoryCtx(threadCtx("conv-seed-fail"), earlierTurns())
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("第一问")); err != nil {
		t.Fatal(err)
	}
	if total, _ := callerRecords(t, st, "conv-seed-fail"); total != 0 {
		t.Fatalf("fixture 没搭对：前文那一批应没写成，历史层里有 %d 条", total)
	}
	for name, m := range map[string]*callerSeenModel{"A": ma, "B": mb} {
		if got := m.call(t, 0); len(got) < 2 || !slices.Equal(got[:2], wantEarlier) || countPrefixLines(got, "我叫小明") != 1 {
			t.Errorf("成员 %s 看到 %q，应以前文开头、只有一份", name, got)
		}
	}
}

// 转交的共享视图（Rewrite / Raw）下，历史层里的前文只出现一次：前置的那份之外，按归属投影时不再出现一遍（它没有
// 归属，投影会把它当成「别人说的」再给一份）。
func TestCallerHistory_HandoffSharedViews_PrefixOnce(t *testing.T) {
	for _, mode := range []struct {
		name string
		m    agent_v2.HandoffHistoryMode
	}{{"rewrite", agent_v2.HandoffHistoryRewrite}, {"raw", agent_v2.HandoffHistoryRaw}} {
		t.Run(mode.name, func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			var billingPrompts [][]*schema.Message
			front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithModel(&dupProbeModel{reply: "TRANSFER"}),
				agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3), agent_v2.WithHistory(hs, "tenant-a"))
			billing := agent_v2.NewBasicAgent(agent_v2.WithName("billing"),
				agent_v2.WithModel(&dupProbeModel{reply: "已受理", captured: &billingPrompts}),
				agent_v2.WithMaxRounds(3), agent_v2.WithHistory(hs, "tenant-a"))
			ho := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("ho"), agent_v2.WithHandoffInitial("front"),
				agent_v2.WithHandoffHistoryMode(mode.m)).AddAgent("front", front).AddAgent("billing", billing)
			turns := [][]*schema.Message{earlierTurns(),
				append(earlierTurns(), schema.UserMessage("我要退款"), schema.AssistantMessage("已受理"))}
			for i, q := range []string{"我要退款", "进度如何"} {
				ctx := agent_v2.WithCallerHistoryCtx(threadCtx("conv-handoff-"+mode.name), turns[i])
				if _, err := ho.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(q)); err != nil {
					t.Fatalf("turn %d: %v", i+1, err)
				}
				if len(billingPrompts) <= i {
					t.Fatalf("turn %d billing 没被调用", i+1)
				}
				n := 0
				for _, m := range billingPrompts[i] {
					if m != nil && strings.Contains(m.Content, "我叫小明") {
						n++
					}
				}
				if n != 1 {
					t.Errorf("turn %d billing 看到前文「我叫小明」%d 次，want 1", i+1, n)
				}
			}
		})
	}
}
