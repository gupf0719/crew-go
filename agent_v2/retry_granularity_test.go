package agent_v2_test

import (
	"context"
	"crew-go/tool"
	"errors"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

type retryCountAgent struct {
	name  string
	calls *int
	fail  bool
}

func (c *retryCountAgent) Run(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	*c.calls++
	if c.fail {
		return nil, errors.New("boom")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewOutputEvent(c.name, schema.AssistantMessage("ok-"+c.name))}, nil
}
func (c *retryCountAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (c *retryCountAgent) Name() string        { return c.name }
func (c *retryCountAgent) Description() string { return c.name }

// TestRetry_Granularity 钉住重试的粒度：失败的部分拿满尝试次数，**已成功的部分不重放**。
//
// # 这条修了两次才对，历史值得记全
//
// 缺陷：重试再调一次 `member.Run`，顶层入口 push 的「新开」信号沿 ctx 被成员内部每层
// 继承 ⟹ 每次尝试都跳过 auto-load 从头再跑（实测成员内部已成功那步跑了 4 次，连同副作用）。
//
// **第一次修（只清信号）引发跨任务泄漏，回退**：上个被放弃任务的挂起档被本任务的第 2 次
// 尝试捡走——「新开」信号一位承载两问（「别捡上个任务的残留」/「这一次从头开始」），
// 清掉就两个一起丢。
//
// **第二次修（现行）**：清信号 + auto-load 闸加**身份维**（`Checkpoint.BelongsToTask`，
// 顶层任务代号沿 ctx 传、同任务全树一枚）——跨任务残留被身份挡住，清信号只放行本任务
// 自己的进度。所以本测试与 `TestRetry_CrossTaskIsolationHolds` **必须同时绿**，
// 只绿一条都是回归。
func TestRetry_Granularity(t *testing.T) {
	t.Run("同层兄弟不重放", func(t *testing.T) {
		var a, b int
		seq := agent_v2.NewSequentialAgent(
			agent_v2.WithSequentialName("seq"), agent_v2.WithSequentialMaxRetries(3),
		).AddStep("A", &retryCountAgent{name: "A", calls: &a}).
			AddStep("B", &retryCountAgent{name: "B", calls: &b, fail: true})
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
		_, _ = seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
		if a != 1 {
			t.Errorf("已成功的上游步骤跑了 %d 次，期望 1", a)
		}
		if b != 4 {
			t.Errorf("失败的那步跑了 %d 次，期望 4（1 次 + 3 次重试）——重试本身没生效？", b)
		}
	})

	t.Run("成员内部已成功那步不重放", func(t *testing.T) {
		var x, y int
		inner := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("inner")).
			AddStep("X", &retryCountAgent{name: "X", calls: &x}).
			AddStep("Y", &retryCountAgent{name: "Y", calls: &y, fail: true})
		outer := agent_v2.NewSequentialAgent(
			agent_v2.WithSequentialName("outer"), agent_v2.WithSequentialMaxRetries(3),
		).AddStep("inner", inner)
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
		_, _ = outer.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
		// 钉修复（2026-08-28 第二次落地）：重试第 2 次尝试起清「新开」信号 + auto-load
		// 闸加**身份维**（Checkpoint.BelongsToTask）。第一次只清信号、没有身份维，
		// 引发跨任务泄漏回退过——所以这条与 TestRetry_CrossTaskIsolationHolds **必须
		// 同时绿**：只绿这条 = 又走回那条错路。
		if x != 1 {
			t.Errorf("成员内部已成功那步跑了 %d 次，期望 1——重试把成员上一次已经做完的"+
				"工作连同副作用一起重放了", x)
		}
	})
}

// TestRetry_CrossTaskIsolationHolds 钉住**配了重试也不能让上个任务的残留被捡起来**。
//
// ⚠️ 这条是补上的哨兵缺口。`cross_task_isolation_test.go` 的两条用例**都没有配重试**，
// 于是一个「第 2 次尝试起清掉新开信号」的改动能全绿通过整套测试，而它实际打破了
// 那条不变量。触发条件是三条同时成立：
//
//  1. 同 sid 跨任务复用（`NewRunSession` godoc 邀请的「与业务持久化主键对齐」用法）
//  2. 任务 A 停在中断且**不 Resume**（被放弃）⟹ 留下 Pending 子树
//  3. 任务 B 是全新 Run + **配了重试**，且第 1 次尝试失败在**比残留 ns 更浅**的位置
//     （所以没覆盖掉 A 的档）
//
// 第 3 条不刁钻：重试存在的理由正是「成员会瞬时失败」，而成员在浅层先挂掉是最常见的形态。
//
// **承重断言是「B 的工具跑了没」而不是「B 有没有中断」**——中断可能来自别的原因，
// 而 B 用自己的工具执行了 A 的待办才是泄漏的实质。B 的 model 被配置成永不产生 tool_call，
// 所以 hold 只要跑过一次，那个 tool_call 就只能来自 A 的存档。
func TestRetry_CrossTaskIsolationHolds(t *testing.T) {
	var holdRuns atomic.Int32
	mkHold := func() tool.Tool {
		return tool.NewFuncTool("hold", "要人确认", map[string]any{"type": "object"},
			func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
				holdRuns.Add(1)
				if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认?"}); err != nil {
					return nil, err
				}
				return tool.Success("确认了"), nil
			})
	}
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	// 任务 A：深处（member/I2）中断，放弃不 Resume
	tkA := agent_v2_newToolkit()
	tkA.Register(mkHold())
	i2A := agent_v2.NewBasicAgent(agent_v2.WithName("I2"), agent_v2.WithToolkit(tkA),
		agent_v2.WithMaxRounds(4), agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("hold"), schema.AssistantMessage("A done")})))
	var cA atomic.Int32
	cA.Store(1) // A 的 I1 直接成功
	innerA := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("inner")).
		AddStep("I1", &failOnceMember{name: "I1", calls: &cA}).AddStep("I2", i2A)
	outerA := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("outer")).
		AddStep("member", innerA)
	if _, err := outerA.Run(ctx, sess, schema.UserMessage("任务A")); err == nil {
		t.Fatal("fixture 没搭对：任务 A 该停在中断上（不中断就没有残留可泄漏）")
	}
	if pend, _ := cp.ListPending(ctx, sess.RunID); len(pend) == 0 {
		t.Fatal("fixture 没搭对：任务 A 没留下挂起档，这条用例恒真")
	}

	// 任务 B：同 sid、全新 Run、配重试；I1 第 1 次失败（浅层）
	holdRuns.Store(0)
	tkB := agent_v2_newToolkit()
	tkB.Register(mkHold())
	i2B := agent_v2.NewBasicAgent(agent_v2.WithName("I2"), agent_v2.WithToolkit(tkB),
		agent_v2.WithMaxRounds(4), agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantMessage("B 直接完成")}))) // 永不产生 tool_call
	var cB atomic.Int32
	innerB := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("inner")).
		AddStep("I1", &failOnceMember{name: "I1", calls: &cB}).AddStep("I2", i2B)
	outerB := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("outer"),
		agent_v2.WithSequentialMaxRetries(3)).AddStep("member", innerB)
	_, errB := outerB.Run(ctx, sess, schema.UserMessage("任务B"))

	if cB.Load() < 2 {
		t.Fatalf("fixture 没搭对：B 的 I1 只跑了 %d 次，重试没生效 ⟹ 走不到第 2 次尝试", cB.Load())
	}
	if n := holdRuns.Load(); n > 0 {
		t.Errorf("任务 B 执行了任务 A 的待办：hold 跑了 %d 次，而 B 的 model 永不产生 tool_call。\n"+
			"errB=%v\n上一个被放弃任务的挂起档被 B 捡起来了——跨任务隔离破了。", n, errB)
	}
}

type failOnceMember struct {
	name  string
	calls *atomic.Int32
}

func (f *failOnceMember) Run(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	if f.calls.Add(1) == 1 {
		return nil, errors.New("瞬时失败")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewOutputEvent(f.name, schema.AssistantMessage("ok"))}, nil
}
func (f *failOnceMember) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (f *failOnceMember) Name() string        { return f.name }
func (f *failOnceMember) Description() string { return f.name }
