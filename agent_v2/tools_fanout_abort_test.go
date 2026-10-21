package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestToolsFanout_AbortDominatesSiblingInterrupt 钉住多 tool_call 扇出里「终局失败严格
// 支配中断」。
//
// 场景门槛很低：一个普通 BasicAgent + 两个工具。一个工具返终局失败（`AgentAsTool` 包的
// 子 agent 违反协议就是这个形状），兄弟工具同时抛中断。
//
// 报中断的代价实测过、也写在 exitErr 的 godoc 里：引擎落 Pending 档 ⟹ **人被问了、人答
// 了** ⟹ Resume 时 abort 赢 ⟹ 出口 purge 把刚给的答案连同兄弟已完成的工作一起清掉。
// 一次白问 + 一次销毁，换来的信息量为零。
//
// ⚠️ 这条不能靠「compose_v2 那边已经有哨兵」省掉：**共用谓词不产生保证**。toolsFn 这侧
// 曾经是逐字手写的第三份仲裁，行为上跟公共判据完全冗余，却独独漏了这一档——而全套测试
// 当时是绿的。
func TestToolsFanout_AbortDominatesSiblingInterrupt(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("boom", "会终局失败的", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return nil, fmt.Errorf("子 agent 违反协议: %w", compose_v2.ErrRunAborted)
		}))
	tk.Register(tool.NewFuncTool("gate", "会问人的", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准转账 5000 吗"})
			return nil, err
		}))

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("bot"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithModel(newScopeStub(schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc1", Type: "function", Function: schema.FunctionCall{Name: "boom", Arguments: "{}"}},
			{ID: "tc2", Type: "function", Function: schema.FunctionCall{Name: "gate", Arguments: "{}"}},
		}), schema.AssistantMessage("好了"))))

	_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("干活"))
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Errorf("兄弟分支已经终局失败了，还报中断：%v\n"+
			"  → 调用方会去落 Pending 档问人，人答完 Resume 时 abort 才赢，"+
			"答案连同兄弟工作一起被 purge", err)
	}
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("应返终局失败，实际 %v", err)
	}
}
