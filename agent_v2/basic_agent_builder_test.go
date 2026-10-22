package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// Phase 2 第八编排：BasicAgentBuilder 端到端验证。
//
// 复用 PoC Step 4 已有的 mock LLM/tool helpers——之前是手写 graph，现在用
// BasicAgentBuilder 简化封装。

// TestBasicAgentBuilder_ReActLoop 用 builder 构造 ReAct，等价 Step 4 测试。
func TestBasicAgentBuilder_ReActLoop(t *testing.T) {
	llm := &mockLLM{}

	model := func(_ context.Context, _, msgs []*schema.Message) (*schema.Message, error) {
		return llm.Generate(context.Background(), msgs)
	}
	tools := func(ctx context.Context, toolCalls []schema.ToolCall, _ []context.Context) ([]*schema.Message, error) {
		var results []*schema.Message
		for _, tc := range toolCalls {
			out, err := mockTool(ctx, tc.Function.Name, tc.Function.Arguments)
			if err != nil {
				return nil, err
			}
			results = append(results, schema.ToolResultMessage(tc.ID, out))
		}
		return results, nil
	}

	g, err := agent_v2.NewBasicAgentBuilder(model, tools).WithMaxRounds(20).Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("北京天气怎么样？"))...),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs := agent_v2.BasicAgentMessages(cm.Channels())
	if len(msgs) != 4 {
		t.Fatalf("应 4 条 messages（user + 3 内部），实际 %d", len(msgs))
	}

	final := agent_v2.BasicAgentFinalMessage(cm.Channels())
	if final == nil {
		t.Fatal("应有 final message")
	}
	if !strings.Contains(final.Content, "20℃") {
		t.Fatalf("final 应含 mock LLM 第二轮答复，实际 %q", final.Content)
	}
	if llm.round != 2 {
		t.Fatalf("LLM 应调 2 次（tool_call + final），实际 %d", llm.round)
	}
}

// TestBasicAgentBuilder_DirectFinal 验证 LLM 第一轮直接返 final（无 tool_call）。
func TestBasicAgentBuilder_DirectFinal(t *testing.T) {
	llm := &directAnswerLLM{}

	model := func(_ context.Context, _, msgs []*schema.Message) (*schema.Message, error) {
		return llm.Generate(context.Background(), msgs)
	}
	g, _ := agent_v2.NewBasicAgentBuilder(model, nil).Compile() // 无 tool 也行（不会被调）

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("hi"))...),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if llm.round != 1 {
		t.Fatalf("直接 final 时 LLM 应只调 1 次，实际 %d", llm.round)
	}
	final := agent_v2.BasicAgentFinalMessage(cm.Channels())
	if final == nil || final.Content != "Hello!" {
		t.Fatalf("final 应是 'Hello!'，实际 %v", final)
	}
}

// TestBasicAgentBuilder_MaxRoundsDefense 验证 LLM 死循环（永远返 tool_call）时 maxRounds 防御。
func TestBasicAgentBuilder_MaxRoundsDefense(t *testing.T) {
	infiniteLLM := func(_ context.Context, _, _ []*schema.Message) (*schema.Message, error) {
		return schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "x", Function: schema.FunctionCall{Name: "noop", Arguments: "{}"}},
		}), nil
	}
	infiniteTool := func(_ context.Context, tcs []schema.ToolCall, _ []context.Context) ([]*schema.Message, error) {
		var out []*schema.Message
		for _, tc := range tcs {
			out = append(out, schema.ToolResultMessage(tc.ID, "noop_result"))
		}
		return out, nil
	}

	g, _ := agent_v2.NewBasicAgentBuilder(infiniteLLM, infiniteTool).WithMaxRounds(3).Compile()

	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("loop"))...),
	)
	if err == nil {
		t.Fatal("LLM 永远返 tool_call + maxRounds 应被拦下")
	}
	if !strings.Contains(err.Error(), "visited") {
		t.Fatalf("err 应含 'visited'，实际 %q", err.Error())
	}
}

// TestBasicAgentBuilder_Validators 验证编译期校验。
func TestBasicAgentBuilder_Validators(t *testing.T) {
	if _, err := agent_v2.NewBasicAgentBuilder(nil, nil).Compile(); err == nil {
		t.Error("无 model 应报错")
	}
}

// TestBasicAgentBuilder_HITL_InToolFn 验证 tool 内抛 ask_user → Resume 续跑。
func TestBasicAgentBuilder_HITL_InToolFn(t *testing.T) {
	llmRound := 0
	llm := func(_ context.Context, _, _ []*schema.Message) (*schema.Message, error) {
		llmRound++
		if llmRound == 1 {
			return schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "ask_call", Function: schema.FunctionCall{Name: "needs_confirm", Arguments: `{}`}},
			}), nil
		}
		return schema.AssistantMessage("done"), nil
	}
	tools := func(ctx context.Context, tcs []schema.ToolCall, _ []context.Context) ([]*schema.Message, error) {
		for _, tc := range tcs {
			if tc.Function.Name == "needs_confirm" {
				if ans, ok := compose_v2.GetResumeAnswerText(ctx, "tool_ask"); ok {
					return []*schema.Message{
						schema.ToolResultMessage(tc.ID, "confirmed:"+ans),
					}, nil
				}
				return nil, &compose_v2.InterruptError{
					Query: &compose_v2.AskQuery{InterruptID: "tool_ask", Question: "确认？"},
				}
			}
		}
		return nil, fmt.Errorf("unknown tool")
	}

	makeGraph := func() *compose_v2.Graph {
		g, _ := agent_v2.NewBasicAgentBuilder(llm, tools).Compile()
		return g
	}

	cm1 := compose_v2.NewChannelManager()
	err := compose_v2.Run(context.Background(), makeGraph(), cm1,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("test"))...),
	)
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("应中断，实际 %v", err)
	}
	snap, _ := cm1.Snapshot()

	// Run 2: 注入 answer
	cm2 := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), makeGraph(), cm2,
		compose_v2.WithResumeFrom(snap),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"tool_ask": schema.UserMessage("yes")}),
	); err != nil {
		t.Fatalf("Run 2: %v", err)
	}

	final := agent_v2.BasicAgentFinalMessage(cm2.Channels())
	if final == nil || final.Content != "done" {
		t.Fatalf("Resume 后应跑到 final='done'，实际 %v", final)
	}
}
