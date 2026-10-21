package agent_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// TestCompositeRejectsPerCallOverride 验证 composite 编排对 per-call 覆盖 fail-fast 返
// ErrCompositeOverride（MED-1）。per-call 覆盖（WithRunModel / WithRunSystemPrompt /
// WithRunMaxRounds）只在 BasicAgent 生效——框架级 resolveModel / resolveSystemPrompt /
// resolveMaxRounds 自动读这三个 ctx 值，而 composite 从不把它们透传给 inner step，传了是
// 静默 no-op（footgun），故 Run / Resume 入口直接挡掉。
//
// sanity-revert：去掉 composite 的 rejectCompositeOverride guard 后，带覆盖的 Run 会照常
// 成功（err=nil，覆盖被静默丢弃），下面 errors.Is 断言从 pass 变 fail，精确指向 "guard 缺失"。
// baseline（不传覆盖正常跑）保证 old/new 行为分叉、非 tautological。
func TestCompositeRejectsPerCallOverride(t *testing.T) {
	mkBasic := func(name string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(name),
			agent_v2.WithModel(&perCallModel{respFn: constReply("done")}),
		)
	}

	// 三种 per-call 覆盖都应被拒（覆盖 runOverrides 的 model / systemPrompt / maxRounds 三字段）
	overrides := []struct {
		name string
		opt  agent_v2.RunOption
	}{
		{"WithRunModel", agent_v2.WithRunModel(&perCallModel{respFn: constReply("x")})},
		{"WithRunSystemPrompt", agent_v2.WithRunSystemPrompt("p")},
		{"WithRunMaxRounds", agent_v2.WithRunMaxRounds(3)},
		{"WithRunModelOptions", agent_v2.WithRunModelOptions(model.WithExtraBody(map[string]any{"k": "v"}))},
	}

	// 两种代表性 composite（覆盖 AddStep / AddBranch 不同 builder 范式；grep 已验 8 编排
	// guard 一致，此处验行为机制）
	composites := []struct {
		name string
		mk   func() agent_v2.Agent
	}{
		{"Sequential", func() agent_v2.Agent {
			return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("s", mkBasic("step"))
		}},
		{"Parallel", func() agent_v2.Agent {
			return agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).AddBranch("b", mkBasic("branch"))
		}},
	}

	for _, c := range composites {
		for _, ov := range overrides {
			ag := c.mk()
			_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"), ov.opt)
			if !errors.Is(err, agent_v2.ErrCompositeOverride) {
				t.Fatalf("%s.Run(%s) should fail-fast with ErrCompositeOverride, got: %v", c.name, ov.name, err)
			}
		}
	}

	// baseline：不传覆盖时 composite 正常跑完（行为分叉锚点——revert guard 后这条仍 pass，
	// 但上面带覆盖的从 fail 变 pass，差异精确指向 guard）
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("s", mkBasic("step"))
	if _, err := seq.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("composite Run without override should succeed, got: %v", err)
	}

	// Resume 路径同样 fail-fast。需先制造真实 pending cp（commonResumePrep 的 no-cp 检查
	// 在 guard 之前，nonexistent sid 会先撞 ErrNoPendingCheckpoint）：用会中断的 step Run
	// 出 pending cp，再带覆盖 Resume → cp 加载成功后 guard 触发 → ErrCompositeOverride。
	askTool := tool.NewFuncTool("ask", "ask user", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "ask_id"); ok {
				return tool.Success("user: " + ans), nil
			}
			return nil, &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "ask_id", Question: "?"}}
		})
	tk := tool.NewToolkit()
	tk.Register(askTool)
	askModel := &perCallModel{respFn: func(int) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc", Function: schema.FunctionCall{Name: "ask", Arguments: "{}"}},
		})
	}}
	step := agent_v2.NewBasicAgent(
		agent_v2.WithName("step"),
		agent_v2.WithModel(askModel),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5),
	)
	seqR := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seqR")).AddStep("s", step)
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	_, runErr := seqR.Run(ctx, sess, schema.UserMessage("hi"))
	if _, isInt := agent_v2.IsInterrupt(runErr); !isInt {
		t.Fatalf("expected interrupt to create pending cp, got: %v", runErr)
	}
	_, err := seqR.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{"ask_id": schema.UserMessage("ok")}),
		agent_v2.WithResumeModel(&perCallModel{respFn: constReply("x")}),
		agent_v2.WithResumeSession(sess),
	)
	if !errors.Is(err, agent_v2.ErrCompositeOverride) {
		t.Fatalf("Sequential.Resume with WithResumeModel should fail-fast with ErrCompositeOverride, got: %v", err)
	}
}
