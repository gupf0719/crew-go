package agent_v2_test

import (
	"context"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/skill"
	"crew-go/tool"
)

func TestToolMemo_ReplaysCompletedSkillReference(t *testing.T) {
	box := &skillRefReadCount{SkillBox: skillRefFixture(t)}
	m := &captureToolsModel{responses: []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{
			{ID: "read", Function: schema.FunctionCall{Name: "read_skill_resource", Arguments: `{"skill":"sample","path":"reference.md"}`}},
			{ID: "pause", Function: schema.FunctionCall{Name: "pause", Arguments: `{}`}},
		}), schema.AssistantMessage("done")}}
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("pause", "pause", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		a, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "continue?"})
		if err != nil {
			return nil, err
		}
		return tool.Success(a.Content), nil
	}))
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("reference_agent"), agent_v2.WithModel(m), agent_v2.WithSkills(skill.SkillSource{SkillBox: box}), agent_v2.WithToolkit(tk))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("request"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || box.calls.Load() != 1 {
		t.Fatalf("fixture err=%v reads=%d", err, box.calls.Load())
	}
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("yes")}))
	if err != nil {
		t.Fatal(err)
	}
	if box.calls.Load() != 1 || box.activations.Load() != 1 || len(m.inputs) != 2 {
		t.Fatalf("expected replay of completed reference, reads=%d activations=%d model calls=%d",
			box.calls.Load(), box.activations.Load(), len(m.inputs))
	}
}
