package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// TestSequentialAgent_BasicLinearFlow 验证 SequentialAgent 端到端：
// 3 个 sub-agent 串成 sequential 链，每步产出喂下一步。
func TestSequentialAgent_BasicLinearFlow(t *testing.T) {
	stub1 := newStubModel([]*schema.Message{schema.AssistantMessage("step1 done")})
	stub2 := newStubModel([]*schema.Message{schema.AssistantMessage("step2 done")})
	stub3 := newStubModel([]*schema.Message{schema.AssistantMessage("step3 done")})

	a1 := agent_v2.NewBasicAgent(agent_v2.WithName("a1"), agent_v2.WithModel(stub1))
	a2 := agent_v2.NewBasicAgent(agent_v2.WithName("a2"), agent_v2.WithModel(stub2))
	a3 := agent_v2.NewBasicAgent(agent_v2.WithName("a3"), agent_v2.WithModel(stub3))

	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
		AddStep("s1", a1).
		AddStep("s2", a2).
		AddStep("s3", a3)

	sess := agent_v2.NewRunSession()
	events, err := seq.Run(context.Background(), sess, schema.UserMessage("hello"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("expected final message")
	}
	if final.Content != "step3 done" {
		t.Fatalf("expected final 'step3 done', got %q", final.Content)
	}
}

// TestSequentialAgent_StepInputComposition 验证 step 1+ 自动拼装"原始目标 + 上一步产出"
func TestSequentialAgent_StepInputComposition(t *testing.T) {
	// step 2 stub 的 model 把 input 直接当 output 返回——验证 step 2 input 内容
	stub1 := newStubModel([]*schema.Message{schema.AssistantMessage("X1_OUT")})
	var step2InputCapture string
	stub2 := &captureInputStub{captured: &step2InputCapture, response: schema.AssistantMessage("X2_OUT")}

	a1 := agent_v2.NewBasicAgent(agent_v2.WithName("a1"), agent_v2.WithModel(stub1))
	a2 := agent_v2.NewBasicAgent(agent_v2.WithName("a2"), agent_v2.WithModel(stub2))

	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq_compose")).
		AddStep("s1", a1).
		AddStep("s2", a2)

	_, err := seq.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("ORIGINAL_GOAL"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// step 2 input 应含 "原始目标：ORIGINAL_GOAL" + "上一步（s1）的产出：X1_OUT"
	if !strings.Contains(step2InputCapture, "ORIGINAL_GOAL") {
		t.Fatalf("step 2 input 缺原始目标: %q", step2InputCapture)
	}
	if !strings.Contains(step2InputCapture, "上一步（s1）的产出") {
		t.Fatalf("step 2 input 缺上一步标识: %q", step2InputCapture)
	}
	if !strings.Contains(step2InputCapture, "X1_OUT") {
		t.Fatalf("step 2 input 缺上一步内容: %q", step2InputCapture)
	}
}

// TestSequentialAgent_HITLResume 验证 sequential 中段 ask_user 后 Resume 闭环。
func TestSequentialAgent_HITLResume(t *testing.T) {
	stub1 := newStubModel([]*schema.Message{schema.AssistantMessage("s1 done")})

	// s2 用 ask_user tool（会抛 InterruptError）
	tk := tool.NewToolkit()
	tk.Register(agent_v2.NewAskUserTool())
	stub2 := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc_1", Function: schema.FunctionCall{Name: agent_v2.AskUserToolName, Arguments: `{"question":"ok?"}`}},
		}),
		schema.AssistantMessage("s2 done"),
	})

	a1 := agent_v2.NewBasicAgent(agent_v2.WithName("a1"), agent_v2.WithModel(stub1))
	a2 := agent_v2.NewBasicAgent(agent_v2.WithName("a2"), agent_v2.WithModel(stub2), agent_v2.WithToolkit(tk))

	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq_hitl")).
		AddStep("s1", a1).
		AddStep("s2", a2)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := &agent_v2.RunSession{RunID: "seq_hitl_sid"}

	// Run 1: s1 完成 + s2 触发 ask_user 中断
	_, err := seq.Run(ctx, sess, schema.UserMessage("go"))
	if err == nil {
		t.Fatal("expected InterruptError from s2 ask_user")
	}
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("expected InterruptError, got %v", err)
	}
	if ie.Query == nil || ie.Query.InterruptID == "" {
		t.Fatalf("InterruptError 缺 Query.InterruptID: %+v", ie)
	}

	// Run 2 (Resume): 给 ask_user 答案
	events, err := seq.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("yes"),
		}),
	)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("Resume 后 expected final message")
	}
	if final.Content != "s2 done" {
		t.Fatalf("Resume 后 final 期望 's2 done'，实际 %q", final.Content)
	}
}

// TestSequentialAgent_NestedAsAgentTool 验证嵌套：BasicAgent 把 SequentialAgent
// 包成 tool 调用——cp ns 物理分离。
func TestSequentialAgent_NestedAsAgentTool(t *testing.T) {
	stub1 := newStubModel([]*schema.Message{schema.AssistantMessage("inner1")})
	stub2 := newStubModel([]*schema.Message{schema.AssistantMessage("inner2")})

	innerA := agent_v2.NewBasicAgent(agent_v2.WithName("innerA"), agent_v2.WithModel(stub1))
	innerB := agent_v2.NewBasicAgent(agent_v2.WithName("innerB"), agent_v2.WithModel(stub2))
	innerSeq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("innerseq")).
		AddStep("ia", innerA).
		AddStep("ib", innerB)

	parentTk := tool.NewToolkit()
	parentTk.Register(agent_v2.AgentAsTool(innerSeq, agent_v2.WithAgentToolDescription("delegate to inner sequential")))

	parentStub := newStubModel([]*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "tc_1", Function: schema.FunctionCall{Name: "innerseq", Arguments: `{"input":"do nested"}`}},
		}),
		schema.AssistantMessage("parent done"),
	})
	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentStub),
		agent_v2.WithToolkit(parentTk),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := &agent_v2.RunSession{RunID: "nested_sid"}
	events, err := parent.Run(ctx, sess, schema.UserMessage("start"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "parent done" {
		t.Fatalf("expected parent done, got %v", final)
	}
}

// TestSequentialAgent_EmptyStepsFails 验证 fail-fast：未 AddStep 直接 Run。
func TestSequentialAgent_EmptyStepsFails(t *testing.T) {
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("empty_seq"))
	_, err := seq.Run(context.Background(), nil, schema.UserMessage("hi"))
	if err == nil {
		t.Fatal("expected error for empty steps")
	}
	if !errors.Is(err, err) || !strings.Contains(err.Error(), "no steps") {
		t.Fatalf("error 应含 'no steps': %v", err)
	}
}

// captureInputStub 把 model.Generate 收到的 last user message 内容捕获到 captured。
type captureInputStub struct {
	captured *string
	response *schema.Message
	calls    int
}

func (s *captureInputStub) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	s.calls++
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i] != nil && msgs[i].Role == schema.RoleUser {
			*s.captured = msgs[i].Content
			break
		}
	}
	return s.response, nil
}

func (s *captureInputStub) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (s *captureInputStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) {
	return s, nil
}

var _ model.ChatModel = (*captureInputStub)(nil)
