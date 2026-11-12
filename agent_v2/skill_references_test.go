package agent_v2_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/skill"
	"crew-go/tool"
)

const skillRefText = "reference body"

func skillRefFixture(t *testing.T) *skill.DefaultSkillBox {
	t.Helper()
	root := t.TempDir()
	for _, skillName := range []string{"sample", "private_sample"} {
		dir := filepath.Join(root, skillName)
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		settings := ""
		if skillName == "private_sample" {
			settings = "disable-model-invocation: true\n"
		}
		if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte("---\nname: "+skillName+"\ndescription: test\n"+settings+"---\ncore instructions"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "reference.md"), []byte("---\ndescription: reference description\nalways_load: true\n---\n"+skillRefText), 0644); err != nil {
			t.Fatal(err)
		}
	}
	box, err := skill.NewDefaultSkillBox(root)
	if err != nil {
		t.Fatal(err)
	}
	return box
}

type skillRefReadCount struct {
	skill.SkillBox
	calls       atomic.Int32
	activations atomic.Int32
	gate        bool
}

func (b *skillRefReadCount) ReadSkillResource(name, path string) string {
	b.calls.Add(1)
	return b.SkillBox.ReadSkillResource(name, path)
}

func (b *skillRefReadCount) ActivateSkill(ctx context.Context, name string) (*skill.ActiveSkill, error) {
	b.activations.Add(1)
	if b.gate {
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "allow reference read"}); err != nil {
			return nil, err
		}
	}
	return b.SkillBox.ActivateSkill(ctx, name)
}

func skillRefCall(id, name, args string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Function: schema.FunctionCall{Name: name, Arguments: args}}})
}

func TestSkillReferenceDoesNotBecomeToolFilterInput(t *testing.T) {
	box := skillRefFixture(t)
	model := &captureToolsModel{responses: []*schema.Message{
		skillRefCall("activate", "activate_skill", `{"skill":"sample"}`), schema.AssistantMessage("done"),
	}}
	seen := false
	a := agent_v2.NewBasicAgent(agent_v2.WithName("reference_agent"), agent_v2.WithModel(model), agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithToolFilter(func(_ context.Context, m *schema.Message) []string {
			if m != nil && strings.Contains(m.Content, skillRefText) {
				seen = true
			}
			return nil
		}))
	if _, err := a.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("request")); err != nil {
		t.Fatal(err)
	}
	if len(model.inputs) != 2 || !strings.Contains(concatContent(model.inputs[1]), skillRefText) {
		t.Fatalf("reference did not reach model: %v", model.inputs)
	}
	if seen {
		t.Fatal("automatic reference was treated as the last real message by ToolFilter")
	}
}

func TestSkillReferenceSharesReadsAcrossAutoAndManual(t *testing.T) {
	box := &skillRefReadCount{SkillBox: skillRefFixture(t)}
	model := &captureToolsModel{responses: []*schema.Message{
		skillRefCall("activate", "activate_skill", `{"skill":"sample"}`),
		skillRefCall("read1", "read_skill_resource", `{"skill":"sample","path":"reference.md"}`),
		skillRefCall("read2", "read_skill_resource", `{"skill":"sample","path":"reference.md"}`), schema.AssistantMessage("done"),
	}}
	a := agent_v2.NewBasicAgent(agent_v2.WithName("reference_agent"), agent_v2.WithModel(model), agent_v2.WithSkills(skill.SkillSource{SkillBox: box}))
	if _, err := a.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("request")); err != nil {
		t.Fatal(err)
	}
	if len(model.inputs) != 4 || !strings.Contains(concatContent(model.inputs[1]), skillRefText) {
		t.Fatalf("automatic reference did not reach model: %v", model.inputs)
	}
	if n := box.calls.Load(); n != 1 {
		t.Fatalf("one reference read in one task, got %d", n)
	}
}

func TestSkillReferenceInlineCatalogHonorsEndVeto(t *testing.T) {
	box := skillRefFixture(t)
	for _, inline := range []bool{false, true} {
		model := &captureToolsModel{responses: []*schema.Message{
			skillRefCall("activate", "activate_skill", `{"skill":"sample"}`), schema.AssistantMessage("done"),
		}}
		veto := &toolEndFunc{fn: func(_ context.Context, out *schema.ToolOutput) {
			if out.CallId == "activate" {
				out.Result.Success = false
				out.Result.ErrorCode = "test_denied"
				out.Result.Error = "policy rejected the activation"
			}
		}}
		a := agent_v2.NewBasicAgent(agent_v2.WithName("reference_agent"), agent_v2.WithModel(model), agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
			agent_v2.WithInlineSkillBody(inline), agent_v2.WithHooks(veto))
		if _, err := a.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("request")); err != nil {
			t.Fatal(err)
		}
		if len(model.inputs) != 2 {
			t.Fatalf("want two model calls, got %d", len(model.inputs))
		}
		got := concatContent(model.inputs[1])
		if !strings.Contains(got, "test_denied") {
			t.Fatalf("missing tool veto: %s", got)
		}
		if strings.Contains(got, "reference description") || strings.Contains(got, skillRefText) {
			t.Errorf("inline=%t: rejected activation catalog or reference exposed: %s", inline, got)
		}
	}
}

func TestSkillReferenceHiddenAndActivationGate(t *testing.T) {
	box := &skillRefReadCount{SkillBox: skillRefFixture(t), gate: true}
	model := &captureToolsModel{responses: []*schema.Message{
		skillRefCall("hidden", "read_skill_resource", `{"skill":"private_sample","path":"reference.md"}`),
		skillRefCall("visible", "read_skill_resource", `{"skill":"sample","path":"reference.md"}`), schema.AssistantMessage("done"),
	}}
	a := agent_v2.NewBasicAgent(agent_v2.WithName("reference_agent"), agent_v2.WithModel(model), agent_v2.WithSkills(skill.SkillSource{SkillBox: box}))
	c, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	c = compose_v2.WithCheckpointerCtx(c, compose_v2.NewInMemoryCheckpointer())
	s := agent_v2.NewRunSession()
	_, err := a.Run(c, s, schema.UserMessage("request"))
	pending, ok := compose_v2.IsInterrupt(err)
	if !ok || pending.Query.Question != "allow reference read" || box.activations.Load() != 1 || box.calls.Load() != 0 {
		t.Fatalf("hidden or unapproved reference touched read: err=%v activations=%d reads=%d", err, box.activations.Load(), box.calls.Load())
	}
	_, err = a.Resume(c, s.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{pending.Query.InterruptID: schema.UserMessage("yes")}))
	if err != nil || box.calls.Load() != 1 || len(model.inputs) != 3 || !strings.Contains(concatContent(model.inputs[2]), skillRefText) {
		t.Fatalf("authorized reference not returned on resume: err=%v reads=%d calls=%d", err, box.calls.Load(), len(model.inputs))
	}
}

func TestSkillReferenceAutoDedupAcrossResume(t *testing.T) {
	box := &skillRefReadCount{SkillBox: skillRefFixture(t)}
	model := &captureToolsModel{responses: []*schema.Message{
		skillRefCall("activate1", "activate_skill", `{"skill":"sample"}`),
		skillRefCall("pause", "pause_probe", `{}`),
		skillRefCall("activate2", "activate_skill", `{"skill":"sample"}`), schema.AssistantMessage("done"),
	}}
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("pause_probe", "pause fixture", map[string]any{"type": "object"}, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		a, e := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "continue run"})
		if e != nil {
			return nil, e
		}
		return tool.Success(a.Content), nil
	}))
	a := agent_v2.NewBasicAgent(agent_v2.WithName("reference_agent"), agent_v2.WithModel(model), agent_v2.WithSkills(skill.SkillSource{SkillBox: box}), agent_v2.WithToolkit(tk))
	c, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	c = compose_v2.WithCheckpointerCtx(c, compose_v2.NewInMemoryCheckpointer())
	s := agent_v2.NewRunSession()
	_, err := a.Run(c, s, schema.UserMessage("request"))
	pending, ok := compose_v2.IsInterrupt(err)
	if !ok || box.calls.Load() != 1 {
		t.Fatalf("expected pause after one read: %v, %d", err, box.calls.Load())
	}
	_, err = a.Resume(c, s.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{pending.Query.InterruptID: schema.UserMessage("yes")}))
	if err != nil || box.calls.Load() != 1 || len(model.inputs) != 4 || !strings.Contains(concatContent(model.inputs[3]), skillRefText) {
		t.Fatalf("auto reference lost or re-read on resume: err=%v reads=%d calls=%d", err, box.calls.Load(), len(model.inputs))
	}
}

func TestSkillReferenceDoesNotRereadAfterUncommittedToolNode(t *testing.T) {
	box := &skillRefReadCount{SkillBox: skillRefFixture(t)}
	model := &captureToolsModel{responses: []*schema.Message{
		skillRefCall("activate", "activate_skill", `{"skill":"sample"}`), schema.AssistantMessage("done"),
	}}
	a := agent_v2.NewBasicAgent(agent_v2.WithName("reference_agent"), agent_v2.WithModel(model), agent_v2.WithSkills(skill.SkillSource{SkillBox: box}))
	runThroughPostToolsPanic(t, a)
	if box.calls.Load() != 1 || len(model.inputs) != 2 || !strings.Contains(concatContent(model.inputs[1]), skillRefText) {
		t.Fatalf("auto reference lost or reread on uncommitted tools node resume: reads=%d calls=%d", box.calls.Load(), len(model.inputs))
	}
}

func TestSkillReferenceBudgetCountsSystemPreamble(t *testing.T) {
	root := skillRefFixture(t).RootDir()
	if err := os.WriteFile(filepath.Join(root, "sample", "reference.md"), []byte("---\ndescription: identifier\ntopics: [identifier]\n---\n"+skillRefText), 0644); err != nil {
		t.Fatal(err)
	}
	fixture, err := skill.NewDefaultSkillBox(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, prompt string
		reads        int32
	}{
		{"fits", "short system instruction", 1},
		{"full", strings.Repeat("long system instruction ", 100), 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			box := &skillRefReadCount{SkillBox: fixture}
			model := &captureToolsModel{responses: []*schema.Message{
				skillRefCall("activate", "activate_skill", `{"skill":"sample"}`), schema.AssistantMessage("done"),
			}}
			a := agent_v2.NewBasicAgent(agent_v2.WithName("reference_agent"), agent_v2.WithModel(model), agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
				agent_v2.WithSystemPrompt(test.prompt),
				agent_v2.WithAutoCompact(&agent_v2.AutoCompactConfig{Enabled: true, Model: &captureToolsModel{}, ModelContextWindow: 256, SummaryMaxTokens: 32, SummaryBufferTokens: 32}))
			if _, err := a.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("identifier")); err != nil {
				t.Fatal(err)
			}
			if got := box.calls.Load(); got != test.reads {
				t.Fatalf("auto reference reads=%d want %d with preamble size=%d", got, test.reads, len(test.prompt))
			}
		})
	}
}
