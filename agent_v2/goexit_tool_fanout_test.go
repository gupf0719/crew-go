package agent_v2_test

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestToolFanout_GoexitIsNotSuccess 守「tool goroutine 异常终止不能被静默跳过」。
//
// # 它防的是什么
//
// 一轮 N>=2 个 tool_call 时各 tool 在自己的 goroutine 里跑，结果写进**预分配**的
// outcomes 切片。`runtime.Goexit` 会跑 defer 但不是 panic——runOne 的 recover（H-02）
// 接不住，`outcomes[i]` 留在零值 ⟹ 收集循环 `err==nil && toolMsg==nil` **静默跳过**
// ⟹ 那个 tc **没有配对的 tool_result**：实测修复前 Run 返 `err=<nil>`（完全成功），
// 而下一轮发给模型的消息流里 `tc_bad` 悬空——对真 provider 是 400/422。
//
// # 修法与档位
//
// goroutine 里一道 `finished` 旗（普通语句置位，defer 里置位没用——Goexit 也跑 defer），
// 未置位则 `outcomes[i]` 写成带 tc 身份的错。**归节点失败档，跟 tool panic 同一路**
// （firstNonIntErr ⟹ tools 节点失败 ⟹ Resume 重试整轮，memo 保已成功的不重放）——
// 框架判不出 Goexit 的触发条件是瞬时还是确定性，不升级成终局失败。
//
// sanity-revert：把 goroutine 里 `!finished` 那支去掉 → 承重 1 红
// （err=<nil>，静默成功）。
func TestToolFanout_GoexitIsNotSuccess(t *testing.T) {
	goodTool := tool.NewFuncTool(
		"good", "returns fine",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success("ok"), nil
		},
	)
	badTool := tool.NewFuncTool(
		"bad", "goexits",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			runtime.Goexit() // zap.WithFatalHook 形态：logger.Fatal 结束当前 goroutine
			return tool.Success("unreachable"), nil
		},
	)
	tk := tool.NewToolkit()
	tk.Register(goodTool)
	tk.Register(badTool)

	// 一轮两个 tool_call ⟹ 走 N>=2 goroutine fanout 路径
	resp1 := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc_good", Function: schema.FunctionCall{Name: "good", Arguments: `{}`}},
		{ID: "tc_bad", Function: schema.FunctionCall{Name: "bad", Arguments: `{}`}},
	})
	m := newStubModel([]*schema.Message{resp1, schema.AssistantMessage("done")})

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("goexit_fanout_agent"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
	)
	sess := &agent_v2.RunSession{RunID: "goexit_tool_sid"}
	_, err := ag.Run(context.Background(), sess, schema.UserMessage("run both tools"))
	t.Logf("Run err=%v", err)

	// **承重 1**：不能静默成功。修复前 err=<nil> 而 tc_bad 没有配对的 tool_result。
	if err == nil {
		t.Fatal("tool goroutine 异常终止（runtime.Goexit）被静默跳过：Run 返 nil\n" +
			"  下一轮消息流里 tc_bad 悬空（无配对 tool_result），对真 provider 是 400/422")
	}

	// **承重 2**：错误带 tool 身份（名字 + tc.ID），定位得了
	if !strings.Contains(err.Error(), `"bad"`) || !strings.Contains(err.Error(), "tc_bad") {
		t.Errorf("错误正文该点名出事的 tool（名字与 id），实际：%v", err)
	}
}
