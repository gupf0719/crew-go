package agent_v2_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
	"crew-go/skill"
)

// actionSkillBox 建含 calc skill 的 box；withAction 时注册 go-func action add。
func actionSkillBox(t *testing.T, withAction bool) skill.SkillBox {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "calc")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"),
		[]byte("---\nname: calc\ndescription: 计算\n---\n计算助手\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var opts []skill.SkillBoxOption
	if withAction {
		opts = append(opts, skill.WithSkillActionFunc("calc",
			skill.SkillActionDescriptor{Name: "add", Description: "加法"},
			func(_ context.Context, args map[string]any) (*skill.SkillExecutionResult, error) {
				x, _ := args["x"].(string)
				return &skill.SkillExecutionResult{Success: true, Output: "sum=" + x}, nil
			}))
	}
	box, err := skill.NewDefaultSkillBox(root, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return box
}

// TestRunSkillAction_EndToEnd：有 action 的 skill → run_skill_action 进 bind list（条件注册）→
// LLM 调它 → action 跑 → 结果回灌（下一轮 model 输入含 sum=42）。
func TestRunSkillAction_EndToEnd(t *testing.T) {
	box := actionSkillBox(t, true)
	m := &captureToolsModel{responses: []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "tc1",
			Function: schema.FunctionCall{Name: "run_skill_action", Arguments: `{"skill":"calc","action":"add","args":{"x":"42"}}`}}}),
		schema.AssistantMessage("算好了"),
	}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ts_action"),
		agent_v2.WithModel(m),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithMaxRounds(5),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("算 42")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(m.bound) == 0 || !sliceHas(m.bound[0], "run_skill_action") {
		t.Errorf("run_skill_action 应在 bind list, 实际 %v", m.bound)
	}
	if len(m.inputs) < 2 {
		t.Fatalf("应 ≥2 轮 Generate，实际 %d", len(m.inputs))
	}
	found := false
	for _, msg := range m.inputs[1] {
		if strings.Contains(msg.Content, "sum=42") {
			found = true
		}
	}
	if !found {
		t.Error("round2 输入应含 action 结果 sum=42（action 真跑了）")
	}
}

// TestRunSkillAction_NonToolCallModeDispatch：非 toolCall mode（pre_match）+ 纯 actions + 无
// user toolkit → run_skill_action 仍可执行（audit MED：makeToolsFn early-return 不能漏掉 actions，
// 否则 LLM bind 了却无处 dispatch）。
func TestRunSkillAction_NonToolCallModeDispatch(t *testing.T) {
	box := actionSkillBox(t, true)
	m := &captureToolsModel{responses: []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "tc1",
			Function: schema.FunctionCall{Name: "run_skill_action", Arguments: `{"skill":"calc","action":"add","args":{"x":"9"}}`}}}),
		schema.AssistantMessage("done"),
	}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("prem_action"),
		agent_v2.WithModel(m),
		agent_v2.WithSkills(skill.SkillSource{
			SkillBox: box,
			Options:  &skill.SkillRuntimeOptions{ActivationMode: skill.SkillActivationModePreMatch},
		}),
		agent_v2.WithMaxRounds(5),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("算 9")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(m.inputs) < 2 {
		t.Fatalf("应 ≥2 轮 Generate，实际 %d", len(m.inputs))
	}
	found := false
	for _, msg := range m.inputs[1] {
		if strings.Contains(msg.Content, "sum=9") {
			found = true
		}
	}
	if !found {
		t.Error("非 toolCall mode + 纯 actions + 无 toolkit 下 run_skill_action 也应可执行（sum=9 回灌）")
	}
}

// TestRunSkillAction_NotRegisteredWithoutActions：没 action 的 skill box → run_skill_action 不挂。
func TestRunSkillAction_NotRegisteredWithoutActions(t *testing.T) {
	box := actionSkillBox(t, false)
	m := &captureToolsModel{responses: []*schema.Message{schema.AssistantMessage("done")}}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("no_action"),
		agent_v2.WithModel(m),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
		agent_v2.WithMaxRounds(3),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(m.bound) > 0 && sliceHas(m.bound[0], "run_skill_action") {
		t.Error("无 action 的 box 不该挂 run_skill_action")
	}
}
