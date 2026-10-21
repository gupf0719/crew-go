package agent_v2_test

import (
	"context"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// 共享历史视图下，同一条原始输入被并发扇出的多个分支同时拿到时，只该记一份。
//
// 原来的判据是**读一遍历史看在不在**。顺序多跳成立（前一跳写完了后一跳才读），并发扇出
// 不成立：三个分支同时起跑，读历史时谁都还没写完，三个都判「没记过」于是都记。实测连跑
// 八次分别记了 1/2/2/3/1/2/2/3 份——**每次都不一样**，而且下一个 Run 每个分支的 prior 里
// 那句话就出现两三遍，模型看到用户把同一个问题问了好几次。
//
// 修法是把判据从读侧挪到写入器侧的一次原子认领：写入器是单 worker，它的锁是这条流上唯一
// 天然串行的点。
//
// **测试必须逼出真并发，而且 barrier 要卡在对的地方**：把它卡在 model 上是不够的——记
// input 发生在 preamble，等分支跑到 model 时判断早就做完了；更麻烦的是 `loadHistory` 会
// 等写入器排空，所以只要分支之间稍有先后，后来者读 prior 时前一个的写已经落盘，读侧判据
// 反而变得可靠，**坏的实现也能过**（实测：barrier 卡 model 时 revert 后连跑五次全绿）。
//
// 屏障必须卡在**读历史那一刻**：三个分支的 Load 都到齐了才一起返回，此时谁的写都还没
// 发生，读侧判据必然三个都判「没记过」。这也正是真实慢后端（Redis/PG 往返几毫秒）下的
// 常态——差别只是那里靠网络延迟拉开窗口，这里靠屏障把窗口钉死。
//
// **sanity-revert**：把 basic_agent.go 的 `claimSharedRootInput(...)` 换回
// `!priorHasRootInputNonce(priorForDedup, rootInputNonce(input))`，本测试 FAIL 并报
// 同一条输入记了多份。
func TestHistory_ConcurrentFanout_SharedInputRecordedOnce(t *testing.T) {
	const branches = 3
	hs := &gatedHistoryStore{
		HistoryStore: agent_v2.NewInMemoryHistoryStore(),
		gate:         newFanoutBarrier(branches),
	}

	mk := func(name, reply string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(name),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage(reply)})),
			agent_v2.WithHistory(hs, "acme"), agent_v2.WithMaxRounds(2))
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("br1", mk("a1", "答案一")).
		AddBranch("br2", mk("a2", "答案二")).
		AddBranch("br3", mk("a3", "答案三"))
	// 共享视图（转交模式）才有「同一条输入交给多个执行体」这回事；SelfOnly 下每个分支
	// 记自己名下的一份是**有意的**——那样每条分支流各自完整（有问有答）。
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("ho"), agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", par)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-cc")
	if _, err := ho.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("原始问题")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "acme", ConvID: "conv-cc"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	byNonce := map[string]int{}
	for _, m := range msgs {
		if m.Role == schema.RoleUser && m.ID != "" {
			byNonce[m.ID]++
		}
	}
	if len(byNonce) != 1 {
		t.Fatalf("fixture 没造出预期状态：应当只有一条带 nonce 的原始输入，实际 %d 种 %v",
			len(byNonce), byNonce)
	}
	for n, cnt := range byNonce {
		if cnt != 1 {
			t.Errorf("同一条原始输入（nonce=%s）在历史里记了 %d 份，want 1——"+
				"下一个 Run 每个分支的 prior 里那句话会出现 %d 遍，"+
				"模型看到用户把同一个问题问了好几次", n[:8], cnt, cnt)
		}
	}

	// 反面：那一份必须真的在（去重不能变成谁都不记）。
	inputs := 0
	for _, m := range msgs {
		if m.Role == schema.RoleUser && m.Content == "原始问题" {
			inputs++
		}
	}
	if inputs != 1 {
		t.Errorf("历史里「原始问题」出现 %d 次，want 1——去重过头会让 input 彻底丢失，"+
			"下一轮模型看到一串没有前因的回答", inputs)
	}
}

// gatedHistoryStore 在**读**历史时撞屏障：三个分支的 Load 都到齐了才一起返回。
// 写照常透传（屏障只钉读窗口，不影响写入器的单 worker 语义）。
type gatedHistoryStore struct {
	agent_v2.HistoryStore
	gate *fanoutBarrier
}

func (s *gatedHistoryStore) Load(ctx context.Context, key agent_v2.HistoryKey) ([]*schema.Message, error) {
	s.gate.wait()
	return s.HistoryStore.Load(ctx, key)
}

// fanoutBarrier 卡住前 n 个到达者，等人齐了一起放行。
type fanoutBarrier struct {
	mu      sync.Mutex
	arrived int
	n       int
	ready   chan struct{}
}

func newFanoutBarrier(n int) *fanoutBarrier {
	return &fanoutBarrier{n: n, ready: make(chan struct{})}
}

// wait 记一次到达；人齐了关闸放行所有人。超出 n 的调用（后续轮次）直接通过。
func (b *fanoutBarrier) wait() {
	b.mu.Lock()
	b.arrived++
	full := b.arrived == b.n
	b.mu.Unlock()
	if full {
		close(b.ready)
		return
	}
	<-b.ready
}
