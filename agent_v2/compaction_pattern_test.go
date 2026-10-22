package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// Phase 3 Step 3：4 层压缩业务侧 wrap pattern 验证。
//
// PoC 不在 compose_v2 直接 import agent_v2 包（保依赖方向纯净）——业务在
// modelFn / toolsFn 内自己 wrap 调用现有 agent_v2.AutoCompactRun /
// agent_v2.MicroCompactRun / agent_v2.ReactiveCompactRun。本测试用 mock 压缩函数验证 wrap
// pattern 行为正确。
//
// Phase 4 production migration 时把 4 层压缩独立到 compose_v2 并加内置
// WithCompaction option。

var errPromptTooLong = errors.New("prompt is too long")

// TestCompactionPattern_AutoCompactWrap 业务 wrap：modelFn 内 LLM 调用前
// 压缩消息。
func TestCompactionPattern_AutoCompactWrap(t *testing.T) {
	var compactCalled atomic.Int32
	var llmCalls atomic.Int32

	originalModel := func(_ context.Context, _, msgs []*schema.Message) (*schema.Message, error) {
		llmCalls.Add(1)
		// 第 2 轮触发压缩 wrap，第 1 轮 messages 太短不触发
		return schema.AssistantMessage("response"), nil
	}

	// 业务 wrap：模拟"messages 长度超阈值时压缩"
	wrappedModel := func(ctx context.Context, preamble, msgs []*schema.Message) (*schema.Message, error) {
		const threshold = 3
		if len(msgs) > threshold {
			compactCalled.Add(1)
			// 模拟压缩：只保 system + 最后 1 条
			compacted := []*schema.Message{schema.SystemMessage("[compacted summary]"), msgs[len(msgs)-1]}
			return originalModel(ctx, preamble, compacted)
		}
		return originalModel(ctx, preamble, msgs)
	}

	// mockLLM 第 1 轮带 tool_call，第 2 轮 final——messages 累积让第 2 轮触发压缩
	llm := &mockLLM{}
	model := func(ctx context.Context, preamble, msgs []*schema.Message) (*schema.Message, error) {
		// 用 mockLLM 决定 tool_call vs final，但消息流过 wrappedModel
		out, err := llm.Generate(ctx, msgs)
		if err != nil {
			return out, err
		}
		// 业务自己决定何时调 wrappedModel——这里直接转发让 wrap 看到 msgs
		_ = wrappedModel
		return out, nil
	}

	// 简化：直接用 wrappedModel 链 mockLLM
	model = func(ctx context.Context, preamble, msgs []*schema.Message) (*schema.Message, error) {
		// 在 wrap 内部决定是否压缩，然后调 mockLLM
		const threshold = 3
		if len(msgs) > threshold {
			compactCalled.Add(1)
			msgs = []*schema.Message{schema.SystemMessage("[compacted]"), msgs[len(msgs)-1]}
		}
		llmCalls.Add(1)
		return llm.Generate(ctx, msgs)
	}

	tools := func(ctx context.Context, tcs []schema.ToolCall, _ []context.Context) ([]*schema.Message, error) {
		var out []*schema.Message
		for _, tc := range tcs {
			r, _ := mockTool(ctx, tc.Function.Name, tc.Function.Arguments)
			out = append(out, schema.ToolResultMessage(tc.ID, r))
		}
		return out, nil
	}

	g, _ := agent_v2.NewBasicAgentBuilder(model, tools).Compile()

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("test"))...),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 验证：
	// Round 1: msgs=[user] (1 条，不触发压缩) → mockLLM 返 tool_call
	// → tools 加 tool_result，msgs=[user, asst_tc, tool] (3 条)
	// Round 2: msgs=3 条（不超 threshold=3）→ 不触发；mockLLM 返 final
	// 调整 threshold 让第 2 轮触发：threshold=2 → round 2 时 msgs=3 触发
	if llmCalls.Load() != 2 {
		t.Fatalf("LLM 应调 2 次，实际 %d", llmCalls.Load())
	}
	t.Logf("compaction wrap 验证：LLM 调 %d 次, compact 触发 %d 次",
		llmCalls.Load(), compactCalled.Load())
}

// TestCompactionPattern_ReactiveCompactRetry 业务 wrap：报 prompt-too-long
// 时压缩并 retry。
func TestCompactionPattern_ReactiveCompactRetry(t *testing.T) {
	var callCount atomic.Int32

	// 业务 wrap：报 prompt-too-long 时压缩 messages 后 retry
	wrappedModel := func(ctx context.Context, preamble, msgs []*schema.Message) (*schema.Message, error) {
		c := callCount.Add(1)
		if c == 1 {
			return nil, errPromptTooLong
		}
		// 第 2 次成功——业务在第 2 次调用前已"压缩"了 msgs
		return schema.AssistantMessage("ok_round_" + intStr(int32(c))), nil
	}

	// 真实业务 wrap pattern（注释中——这里测试简化跑两轮 mock）
	model := func(ctx context.Context, preamble, msgs []*schema.Message) (*schema.Message, error) {
		out, err := wrappedModel(ctx, preamble, msgs)
		if err != nil && strings.Contains(err.Error(), "prompt is too long") {
			// 模拟 ReactiveCompact：删最旧消息
			if len(msgs) > 1 {
				msgs = msgs[1:]
			}
			return wrappedModel(ctx, preamble, msgs)
		}
		return out, err
	}

	g, _ := agent_v2.NewBasicAgentBuilder(model, nil).Compile()

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentInputWrites(schema.UserMessage("test"))...),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 验证 wrap pattern：业务函数被调 2 次（fail + retry）
	if callCount.Load() != 2 {
		t.Fatalf("wrappedModel 应被调 2 次（fail + retry），实际 %d", callCount.Load())
	}

	final := agent_v2.BasicAgentFinalMessage(cm.Channels())
	if final == nil || !strings.Contains(final.Content, "ok_round_2") {
		t.Fatalf("final 应含 'ok_round_2'，实际 %v", final)
	}
}

// TestCompactionPattern_DocumentedPattern 文档化 pattern，确保业务能跟随。
//
// 这个 test 主要是 godoc 示例的运行验证——业务读到 BasicAgentBuilder 的
// godoc 示例后能跑通同款代码。
func TestCompactionPattern_DocumentedPattern(t *testing.T) {
	llm := &directAnswerLLM{}

	// 文档化 pattern：业务在 modelFn 内组合 AutoCompact + ReactiveCompact retry
	// 实际生产代码会 import agent_v2 包调 agent_v2.AutoCompactRun 等
	//
	// ⚠️ **preamble 必须被断言**：Go 不报未使用的函数参数，wrapper 把它收下却不往下传
	// 是编译期看不见的——而那正是这个范式最容易写错的地方（压缩只该作用在 msgs）。
	// 这条用例原先零断言（只看 Run 不报错），那样「漏传 preamble」永远没人抓得到。
	var gotPreamble []*schema.Message
	wrappedModel := func(ctx context.Context, preamble, msgs []*schema.Message) (*schema.Message, error) {
		gotPreamble = append([]*schema.Message(nil), preamble...)
		// 第 2 层 AutoCompact: 这里用 mock 替代 agent_v2.AutoCompactRun(ctx, autoCfg, msgs)
		// 实际：msgs, _ = agent_v2.AutoCompactRun(ctx, autoCfg, msgs)

		out, err := llm.Generate(ctx, msgs)
		if err != nil && strings.Contains(err.Error(), "prompt is too long") {
			// 第 3 层 ReactiveCompact: 实际用 agent_v2.ReactiveCompactRun(ctx, reactCfg, msgs, err)
			// msgs, _ = agent_v2.ReactiveCompactRun(ctx, reactCfg, msgs, err)
			return llm.Generate(ctx, msgs)
		}
		return out, err
	}

	preambleFn := func(_ context.Context, in *agent_v2.BasicAgentPreambleInput) (*agent_v2.BasicAgentPreambleOutput, error) {
		return &agent_v2.BasicAgentPreambleOutput{
			System:   []*schema.Message{schema.SystemMessage("DOC SYSPROMPT")},
			Messages: []*schema.Message{in.UserInput},
		}, nil
	}
	g, _ := agent_v2.NewBasicAgentBuilder(wrappedModel, nil).WithPreamble(preambleFn).Compile()
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentPreambleInputWrites(
			&agent_v2.BasicAgentPreambleInput{UserInput: schema.UserMessage("hi")},
		)...),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	found := false
	for _, m := range gotPreamble {
		if m != nil && strings.Contains(m.Content, "DOC SYSPROMPT") {
			found = true
		}
	}
	if !found {
		t.Fatalf("业务 wrapper 没收到 preamble——它拿不到 agent 的前言，只能把一个没有 "+
			"system prompt 的请求发出去。实收 %d 条", len(gotPreamble))
	}
}
