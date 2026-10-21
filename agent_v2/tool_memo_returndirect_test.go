package agent_v2_test

import (
	"context"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestToolMemo_ReturnDirectAnnotationSurvivesResume memo 命中时直接返存下来的 ToolMsg——
// 它上面挂的 annotation 必须跨 cp 活着。
//
// `ReturnDirectAnnotation` 是最要紧的一个：丢了 ⟹ handoff 的 transfer_to_agent 在 Resume
// 后不再短路 ReAct 循环，控制权转移静默失效（模型会接着自己往下聊）。memo 存整条 Message
// 而不只是 content 正是为此（见 toolMemoEntry godoc）。
//
// **必须跟无中断基线对照**：return-direct 短路时 final 本身就是空串（既有行为），不比对
// 基线会把它误判成「memo 路径丢了产出」。
func TestToolMemo_ReturnDirectAnnotationSurvivesResume(t *testing.T) {
	var calls atomic.Int32
	rdTool := tool.NewFuncTool("go_direct", "return-direct", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			calls.Add(1)
			r := tool.Success("直接返回")
			r.Metadata = map[string]any{tool.ReturnDirectMetaKey: true}
			return r, nil
		})
	askTool := tool.NewFuncTool("hold", "中断", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if _, ok := compose_v2.GetResumeAnswerText(ctx, "aq"); ok {
				return tool.Success("ok"), nil
			}
			return nil, &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "aq", Question: "?"}}
		})
	tk := tool.NewToolkit()
	tk.Register(rdTool)
	tk.Register(askTool)

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("rdAg"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "t_rd", Function: schema.FunctionCall{Name: "go_direct", Arguments: `{}`}},
				{ID: "t_hold", Function: schema.FunctionCall{Name: "hold", Arguments: `{}`}},
			}),
			schema.AssistantMessage("不该走到这"),
		})),
		agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(5))

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("应中断: %v", err)
	}
	t.Logf("首跑 go_direct 调用 %d 次", calls.Load())

	ev, rerr := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("y")}))
	t.Logf("Resume err=%v  go_direct 累计调用 %d 次（应仍 1）", rerr, calls.Load())
	fin := agent_v2.FinalMessage(ev)
	if fin != nil {
		t.Logf("final = %q", fin.Content)
	} else {
		t.Logf("final = nil")
	}
	if calls.Load() != 1 {
		t.Errorf("go_direct 累计应恰 1 次，实际 %d ——memo 未命中", calls.Load())
	}
	memoFinal := ""
	if fin != nil {
		memoFinal = fin.Content
	}
	if memoFinal == "不该走到这" {
		t.Errorf("return-direct 在 Resume 后失效了：final=%q\n"+
			"  = memo 命中路径把 ReturnDirectAnnotation 丢了（basicReturnDirectCh 没被置上、\n"+
			"  ReAct 没短路），handoff 的控制权转移会静默失效", memoFinal)
	}

	// ── 无中断基线：同形状但 hold 工具直接成功 ──
	var calls2 atomic.Int32
	rd2 := tool.NewFuncTool("go_direct", "return-direct", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			calls2.Add(1)
			r := tool.Success("直接返回")
			r.Metadata = map[string]any{tool.ReturnDirectMetaKey: true}
			return r, nil
		})
	ok2 := tool.NewFuncTool("hold", "不中断", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success("ok"), nil
		})
	tk2 := tool.NewToolkit()
	tk2.Register(rd2)
	tk2.Register(ok2)
	agB := agent_v2.NewBasicAgent(
		agent_v2.WithName("rdAgB"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "t_rd", Function: schema.FunctionCall{Name: "go_direct", Arguments: `{}`}},
				{ID: "t_hold", Function: schema.FunctionCall{Name: "hold", Arguments: `{}`}},
			}),
			schema.AssistantMessage("不该走到这"),
		})),
		agent_v2.WithToolkit(tk2), agent_v2.WithMaxRounds(5))
	evB, errB := agB.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go"))
	finB := agent_v2.FinalMessage(evB)
	if finB != nil {
		t.Logf("【基线】无中断 err=%v final=%q", errB, finB.Content)
	} else {
		t.Logf("【基线】无中断 err=%v final=nil", errB)
	}
	baseFinal := ""
	if finB != nil {
		baseFinal = finB.Content
	}
	if memoFinal != baseFinal {
		t.Errorf("memo 路径与无中断基线的 final 不一致: memo=%q 基线=%q\n"+
			"  两者必须一样——不一样说明 memo 存回来的 ToolMsg 跟首跑产出有差异", memoFinal, baseFinal)
	}
}
