package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// capModel：每次 Generate 记下收到的 user 输入，首轮返 tool_call，之后返文本。
type capModel struct {
	toolName string
	seen     *[]string
	calls    int
}

func (m *capModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, mm := range msgs {
		if mm.Role == schema.RoleUser {
			*m.seen = append(*m.seen, mm.Content)
		}
	}
	m.calls++
	if m.calls == 1 {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "t1", Function: schema.FunctionCall{Name: m.toolName, Arguments: `{}`}}}), nil
	}
	return schema.AssistantMessage("done"), nil
}
func (m *capModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	out, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(out), nil
}
func (m *capModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestCrossTask_ResumeDoesNotAdoptAbandonedBranch 钉住**Resume 的后半程走进上个任务
// 遗留的分支时，不捡走它的现场**（backlog「Resume 路径不带新任务信号」那条的完整链）。
//
// 链条（同 sid——业务把 sid 当会话复用）：任务 A 在 step2 中断后被放弃（step2 槽里躺着
// A 的挂起档）→ 任务 B 新 Run 中断在 step1（fresh 全树，s2 没进过、A 的残留幸存）→
// Resume(B) 答完 step1 → **后半程进 step2** → step2 的 auto-load 看到的是 A 的档。
//
// Resume 路径**刻意**不 push「新开」信号（嵌套 HITL 靠 auto-load 恢复，红线），所以这里
// 承重的是 auto-load 闸的**身份维**：`Checkpoint.BelongsToTask`——A 的档 TaskID 是 A 的
// 任务代号，Resume(B) 从 B 的顶层档读回 B 代号沿 ctx 传下来，不等 ⟹ 不捡 ⟹ s2 干净新开。
//
// ⚠️ 变异实测（BelongsToTask 恒 true）：**B 的 step2 model 一次都不被调**（A 的档停在
// tools 中断、Resume 现场从 tools 恢复），B 的输入根本没进图、A 的待办在 B 的 Resume 里
// 被推进——正是这条要防的伤害。断言必须包含正向那半（「看到任务B」）：只断「没看到
// 任务A」的话，这个变异下 model 输入是**空的**、否定断言恒真。
func TestCrossTask_ResumeDoesNotAdoptAbandonedBranch(t *testing.T) {
	mkAsk := func(name string) tool.Tool {
		return tool.NewFuncTool("ask_"+name, "问", map[string]any{"type": "object"},
			func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
				if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: name + "?"}); err != nil {
					return nil, err
				}
				return tool.Success(name + " 确认"), nil
			})
	}
	var s1Seen, s2Seen []string
	mkStep := func(name string, seen *[]string) *agent_v2.BasicAgent {
		tk := agent_v2_newToolkit()
		tk.Register(mkAsk(name))
		return agent_v2.NewBasicAgent(agent_v2.WithName(name), agent_v2.WithToolkit(tk),
			agent_v2.WithMaxRounds(4),
			agent_v2.WithModel(&capModel{toolName: "ask_" + name, seen: seen}))
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
		AddStep("s1", mkStep("s1", &s1Seen)).AddStep("s2", mkStep("s2", &s2Seen))
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	// 任务 A：step1 中断 → 答 → step2 中断 → 放弃
	_, errA := seq.Run(ctx, sess, schema.UserMessage("任务A"))
	ieA1, ok := compose_v2.IsInterrupt(errA)
	if !ok {
		t.Fatalf("A step1 该中断: %v", errA)
	}
	_, errA2 := seq.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ieA1.Query.InterruptID: schema.UserMessage("好")}))
	if _, ok := compose_v2.IsInterrupt(errA2); !ok {
		t.Fatalf("A step2 该中断: %v", errA2)
	}
	pend, _ := cp.ListPending(ctx, sess.RunID)
	t.Logf("任务A放弃后残留 %d 份挂起档", len(pend))

	// **同 sid**（L72 的前提：业务把 sid 当会话复用、不调 NextTask）——A 的残留
	// 与 B 在同一个 sid 下，s2 槽里躺着 A 的挂起档。
	var b1Seen, b2Seen []string
	seqB := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
		AddStep("s1", mkStep("s1", &b1Seen)).AddStep("s2", mkStep("s2", &b2Seen))
	_, errB := seqB.Run(ctx, sess, schema.UserMessage("任务B"))
	ieB1, ok := compose_v2.IsInterrupt(errB)
	if !ok {
		t.Fatalf("B step1 该中断: %v", errB)
	}
	_, errBR := seqB.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ieB1.Query.InterruptID: schema.UserMessage("好")}))
	t.Logf("Resume(B): err=%v", errBR)
	t.Logf("B 的 step2 model 看到的输入: %v", b2Seen)
	joined := strings.Join(b2Seen, " | ")
	if strings.Contains(joined, "任务A") {
		t.Errorf("B 的 Resume 后半程进 step2 时捡走了任务 A 的现场（model 看到「任务A」）")
	}
	if !strings.Contains(joined, "任务B") {
		t.Errorf("B 的 step2 没看到自己的输入「任务B」——现场来路不明: %v", b2Seen)
	}
}
