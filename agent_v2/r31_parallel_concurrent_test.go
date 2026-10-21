package agent_v2_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// barrierStubAgent 模拟 sub-agent.Run：写 Output+Exit 后用 barrier 强制等其他
// branch 也写完才 return events。让 sub-agent.Run() 返回的 events slice 必然
// 含 sibling branch 的事件——精准复现 ParallelAgent 共享 parentSession 串台。
type barrierStubAgent struct {
	name    string
	exitMsg string
	wgWrote *sync.WaitGroup // 写完 exit 后 Done
	wgGo    *sync.WaitGroup // 等所有 branch 都写完
}

func (b *barrierStubAgent) Run(ctx context.Context, sess *agent_v2.RunSession, input *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	startOffset := len(sess.Events())
	sess.AddEvent(agent_v2.NewOutputEvent(b.name, schema.UserMessage("step")))
	sess.AddEvent(agent_v2.NewExitEvent(b.name, schema.AssistantMessage(b.exitMsg)))
	// 我写完了
	b.wgWrote.Done()
	// 等其他 branch 也写完——保证 sub-agent.Run() 返回 events 时 sibling Exit
	// 已 append 到共享 session.events
	b.wgGo.Wait()
	// 取 startOffset 之后的快照（模拟 collectEvents）——sibling 写的 Exit 必在里面
	all := sess.Events()
	if startOffset >= len(all) {
		return nil, nil
	}
	out := make([]*agent_v2.AgentEvent, len(all)-startOffset)
	copy(out, all[startOffset:])
	return out, nil
}

func (b *barrierStubAgent) Resume(ctx context.Context, sid string, opts ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (b *barrierStubAgent) Name() string        { return b.name }
func (b *barrierStubAgent) Description() string { return "" }

// TestR31_ParallelAgent_FanoutFinalMessage_NoSiblingCrosstalk 复现 R31 视角 D
// HIGH-1：ParallelAgent 多分支并发 → sub-agent.Run() 返回的 events 含 sibling
// Exit → 直接 FinalMessage 反向扫会拿到 sibling 的最终消息（错置归属）。
//
// fix 前：branchFn 用 FinalMessage(events) → A/B 都拿到最后写入的 Exit（同一个）
// fix 后：branchFn 按 sub-agent.Name() 过滤 → A 拿 A 的 Exit、B 拿 B 的 Exit
//
// 实现：用 barrier 强制两 branch 都写完 Exit 才 return events——让 sub-agent.Run()
// 返回的 slice 必然包含 sibling 的 Exit。然后业务自定义 merger 捕获每分支收到的
// content 验证归属正确。
func TestR31_ParallelAgent_FanoutFinalMessage_NoSiblingCrosstalk(t *testing.T) {
	var wgWrote sync.WaitGroup
	wgWrote.Add(2)
	var wgGo sync.WaitGroup
	wgGo.Add(1) // 主测等所有 branch 写完才 release

	// 用 goroutine 等 wgWrote.Wait() 然后 wgGo.Done()——让两 branch 都被卡到都写完
	done := make(chan struct{})
	go func() {
		wgWrote.Wait()
		wgGo.Done()
		close(done)
	}()

	branchA := &barrierStubAgent{
		name:    "agent_A",
		exitMsg: "A-FINAL-VALUE",
		wgWrote: &wgWrote,
		wgGo:    &wgGo,
	}
	branchB := &barrierStubAgent{
		name:    "agent_B",
		exitMsg: "B-FINAL-VALUE",
		wgWrote: &wgWrote,
		wgGo:    &wgGo,
	}

	// 自定义 merger 捕获各 branch 实际收到的 content
	captured := map[string]string{}
	var mu sync.Mutex
	merger := func(_ context.Context, results []agent_v2.ParallelBranchResult) (*schema.Message, error) {
		mu.Lock()
		defer mu.Unlock()
		for _, r := range results {
			if r.Output != nil {
				captured[r.Name] = r.Output.Content
			}
		}
		return schema.AssistantMessage("merged"), nil
	}

	par := agent_v2.NewParallelAgent(
		agent_v2.WithParallelName("par"),
		agent_v2.WithParallelMerger(merger),
	).AddBranch("A", branchA).AddBranch("B", branchB)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := par.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 防御：确保 barrier 已 release（否则 deadlock 会被 ctx timeout 触发）
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("barrier 未 release，sub-agent.Run() 未都 return")
	}

	mu.Lock()
	defer mu.Unlock()
	// 正确归属：A 分支收到 A-FINAL-VALUE，B 分支收到 B-FINAL-VALUE
	if got := captured["A"]; got != "A-FINAL-VALUE" {
		t.Errorf("branch A 串台：merger 收到 %q (expected A-FINAL-VALUE)", got)
	}
	if got := captured["B"]; got != "B-FINAL-VALUE" {
		t.Errorf("branch B 串台：merger 收到 %q (expected B-FINAL-VALUE)", got)
	}
}
