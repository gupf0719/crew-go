package agent_v2_test

// 结构化输出里「算不算一次失败的尝试」与「预算耗尽时报出来的原因」是同一个判据：final_output 这次
// 调用在 OnToolEnd 之后是不是失败的——参数不是合法 JSON、schema 校验失败、校验通过却被钩子否决都算，
// 钩子改成成功的不算。曾经只认 schema 失败与钩子否决，参数非法哪处都不认（均实测，见各条）。

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// runStructured 跑一个配了 person schema 的 BasicAgent，预算耗尽时取出 StructuredOutputError。
func runStructured(t *testing.T, m *structModel, retries int, opts ...agent_v2.Option) ([]*agent_v2.AgentEvent, *agent_v2.StructuredOutputError, error) {
	t.Helper()
	all := append(append([]agent_v2.Option{agent_v2.WithName("extract"), agent_v2.WithModel(m)}, opts...),
		agent_v2.WithOutputSchema("person", personSchema(), agent_v2.WithMaxValidationRetries(retries)),
		agent_v2.WithMaxRounds(10))
	evs, err := agent_v2.NewBasicAgent(all...).Run(
		compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer()),
		agent_v2.NewRunSession(), schema.UserMessage("x"))
	var soe *agent_v2.StructuredOutputError
	if errors.As(err, &soe) {
		return evs, soe, err
	}
	return evs, nil, err
}

// TestStructuredOutput_InvalidArgsCountsAsFailure：final_output 的参数不是合法 JSON（模型输出被截断的常见
// 形态），两次都这样、预算 1——预算按这两次耗尽，报出来的原因却曾经是空的。
func TestStructuredOutput_InvalidArgsCountsAsFailure(t *testing.T) {
	m := &structModel{responses: []*schema.Message{finalOutputCall(`{"name":"A"`), finalOutputCall(`{"name":"B"`)}}
	_, soe, err := runStructured(t, m, 1)
	if soe == nil {
		t.Fatalf("预算应耗尽、返回 StructuredOutputError，实际 %v", err)
	}
	if soe.Attempts != 2 || !strings.Contains(soe.LastError, "invalid_arguments") {
		t.Errorf("应报 2 次尝试、原因是参数非法：Attempts=%d LastError=%q", soe.Attempts, soe.LastError)
	}
}

// TestStructuredOutput_LastErrorIsLatestFailure：先 schema 失败、后参数非法——报的应是最后一次（参数非法），
// 曾经报的是更早那次 schema 失败。
func TestStructuredOutput_LastErrorIsLatestFailure(t *testing.T) {
	m := &structModel{responses: []*schema.Message{finalOutputCall(`{"name":"A"}`), finalOutputCall(`{"name":`)}}
	_, soe, err := runStructured(t, m, 1)
	if soe == nil {
		t.Fatalf("预算应耗尽、返回 StructuredOutputError，实际 %v", err)
	}
	if !strings.Contains(soe.LastError, "invalid_arguments") {
		t.Errorf("应报最后一次失败（参数非法），实际 %q", soe.LastError)
	}
}

// TestStructuredOutput_InvalidArgsBlocksBusinessShortCircuit：同一批 [业务 exit 工具, 参数非法的 final_output]——
// 参数非法也是一次失败的尝试，这一轮要回炉、业务工具不短路。曾经业务工具短路收尾、报「模型未产出结构化
// 输出」，而且这个错不可重试。对照组把 final_output 换成 schema 校验失败，两组应一样。
func TestStructuredOutput_InvalidArgsBlocksBusinessShortCircuit(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("do_exit", "业务 exit", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: "业务产出",
				Metadata: map[string]any{tool.ReturnDirectMetaKey: true}}, nil
		}))
	for _, c := range []struct{ name, args string }{
		{"参数非法", `{"name":"张三",`},
		{"schema 失败（对照）", `{"name":"张三"}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			m := &structModel{responses: []*schema.Message{
				schema.AssistantToolCallMessage([]schema.ToolCall{
					{ID: "tc_biz", Function: schema.FunctionCall{Name: "do_exit", Arguments: `{}`}},
					{ID: "tc_fo", Function: schema.FunctionCall{Name: agent_v2.FinalOutputToolName, Arguments: c.args}},
				}),
				finalOutputCall(`{"name":"张三","age":31}`),
			}}
			evs, _, err := runStructured(t, m, 2, agent_v2.WithToolkit(tk))
			if err != nil {
				t.Fatalf("应回炉、第二轮给出合法 JSON 后收尾，实际 %v", err)
			}
			if m.idx != 2 {
				t.Errorf("模型应被调 2 次（回炉一次），实际 %d 次", m.idx)
			}
			if fm := agent_v2.FinalMessage(evs); fm == nil || !strings.Contains(fm.Content, `"age":31`) {
				t.Errorf("最终答复应是第二轮的合法 JSON，实际 %v", fm)
			}
		})
	}
}

// acceptAnyToolResult 把失败的工具结果改成成功（内容换成一段 JSON）。
type acceptAnyToolResult struct{ hook.NoOpHooks }

const acceptedByHook = `{"name":"hook","age":1}`

func (acceptAnyToolResult) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out != nil && out.Result != nil && !out.Result.Success {
		out.Result = &schema.ToolCallResult{Success: true, Output: acceptedByHook}
	}
	return ctx, nil
}

// TestStructuredOutput_HookAcceptedStillFailedAttempt：OnToolEnd 把 schema 失败的 final_output 改成成功——答复
// 有效与否归框架，那次没经过校验、不成为答复，照样算一次失败的尝试：回炉、计入预算，报出来的原因是钩子
// 之前那次失败。曾经报的是钩子给的那段成功 JSON；后来又有一版把它当成「不算失败」，预算耗尽时原因为空、
// 同批的业务 exit 工具照样短路、报「模型未产出结构化输出」（均实测）。
func TestStructuredOutput_HookAcceptedStillFailedAttempt(t *testing.T) {
	t.Run("预算耗尽时报钩子之前的失败", func(t *testing.T) {
		m := &structModel{responses: []*schema.Message{finalOutputCall(`{"name":"A"}`), finalOutputCall(`{"name":"B"}`)}}
		_, soe, err := runStructured(t, m, 1, agent_v2.WithHooks(acceptAnyToolResult{}))
		if soe == nil {
			t.Fatalf("钩子改成的成功不是答复，预算应照样耗尽，实际 %v", err)
		}
		if soe.Attempts != 2 || !strings.Contains(soe.LastError, agent_v2.StructuredOutputInvalidCode) {
			t.Errorf("应报 2 次尝试、原因是钩子之前的 schema 失败：Attempts=%d LastError=%q", soe.Attempts, soe.LastError)
		}
	})
	t.Run("同批的业务 exit 工具让路回炉", func(t *testing.T) {
		tk := tool.NewToolkit()
		tk.Register(tool.NewFuncTool("do_exit", "业务 exit", map[string]any{"type": "object"},
			func(context.Context, map[string]any) (*tool.Result, error) {
				return &tool.Result{Success: true, Output: "业务产出",
					Metadata: map[string]any{tool.ReturnDirectMetaKey: true}}, nil
			}))
		m := &structModel{responses: []*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "tc_biz", Function: schema.FunctionCall{Name: "do_exit", Arguments: `{}`}},
				{ID: "tc_fo", Function: schema.FunctionCall{Name: agent_v2.FinalOutputToolName, Arguments: `{"name":"张三"}`}},
			}),
			finalOutputCall(`{"name":"张三","age":31}`),
		}}
		evs, _, err := runStructured(t, m, 2, agent_v2.WithToolkit(tk), agent_v2.WithHooks(acceptAnyToolResult{}))
		if err != nil || m.idx != 2 {
			t.Fatalf("应回炉、第二轮给出合法 JSON 后收尾：模型被调 %d 次、err=%v", m.idx, err)
		}
		if fm := agent_v2.FinalMessage(evs); fm == nil || !strings.Contains(fm.Content, `"age":31`) {
			t.Errorf("最终答复应是第二轮的合法 JSON，实际 %v", fm)
		}
	})
}

// TestStructuredOutput_PostambleAttemptsOnlyThisCall：开了会话历史。第一回合 schema 失败一次后成功；第二回合
// 先 schema 失败一次，再以自由文本收尾——报的尝试次数应只算本回合（1 次）。曾经数的是含历史的整条消息
// 列表（3 次）。
func TestStructuredOutput_PostambleAttemptsOnlyThisCall(t *testing.T) {
	m := &structModel{responses: []*schema.Message{
		finalOutputCall(`{"name":"A"}`), finalOutputCall(`{"name":"A","age":3}`),
		finalOutputCall(`{"name":"B"}`), schema.AssistantMessage("这次我不给 JSON")}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("extract"), agent_v2.WithModel(m),
		agent_v2.WithHistory(agent_v2.NewInMemoryHistoryStore(), "u1"),
		agent_v2.WithOutputSchema("person", personSchema(), agent_v2.WithMaxValidationRetries(3)),
		agent_v2.WithMaxRounds(10))
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-1")
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(ctx, sess, schema.UserMessage("第一问")); err != nil {
		t.Fatalf("第一回合应成功：%v", err)
	}
	sess.NextTask()
	_, err := ag.Run(ctx, sess, schema.UserMessage("第二问"))
	var soe *agent_v2.StructuredOutputError
	if !errors.As(err, &soe) {
		t.Fatalf("第二回合以自由文本收尾，应返回 StructuredOutputError，实际 %v", err)
	}
	if soe.Attempts != 1 {
		t.Errorf("尝试次数应只算本回合（1 次），实际 %d", soe.Attempts)
	}
}
