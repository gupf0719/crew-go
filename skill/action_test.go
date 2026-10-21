package skill_test

import (
	"context"
	"testing"

	"crew-go/skill"
)

func mkSkill(t *testing.T, root, name string) {
	t.Helper()
	writeSkill(t, root, name, "---\nname: "+name+"\ndescription: test skill\n---\nbody\n")
}

// TestRunSkillAction_GoFunc：注册 go-func action → RunSkillAction 进程内调 → 拿结果；
// ListSkillActions 含注册的 action。
func TestRunSkillAction_GoFunc(t *testing.T) {
	root := t.TempDir()
	mkSkill(t, root, "calc")
	box, err := skill.NewDefaultSkillBox(root,
		skill.WithSkillActionFunc("calc", skill.SkillActionDescriptor{Name: "add", Description: "加法"},
			func(_ context.Context, args map[string]any) (*skill.SkillExecutionResult, error) {
				return &skill.SkillExecutionResult{Success: true, Output: "x=" + args["x"].(string)}, nil
			}))
	if err != nil {
		t.Fatalf("new box: %v", err)
	}
	res, err := box.RunSkillAction(context.Background(), "calc", "add", map[string]any{"x": "42"})
	if err != nil {
		t.Fatalf("RunSkillAction: %v", err)
	}
	if !res.Success || res.Output != "x=42" {
		t.Errorf("go action 结果错: %+v", res)
	}
	acts := box.ListSkillActions("calc")
	if len(acts) != 1 || acts[0].Name != "add" || acts[0].Runtime != "go" {
		t.Errorf("ListSkillActions 应含 add(go), 实际 %+v", acts)
	}
}
