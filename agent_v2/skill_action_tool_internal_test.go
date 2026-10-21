package agent_v2

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/skill"
	"crew-go/tool"
)

// mkActionBox 建一个含 calc skill 的 DefaultSkillBox；withAction 时注册 go-func action add，
// disabled 时 skill 标 disable-model-invocation。
func mkActionBox(t *testing.T, withAction, disabled bool) skill.SkillBox {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "calc")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	fm := "---\nname: calc\ndescription: 计算\n"
	if disabled {
		fm += "disable-model-invocation: true\n"
	}
	fm += "---\nbody\n"
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(fm), 0o644); err != nil {
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
		t.Fatalf("box: %v", err)
	}
	return box
}

// TestBoxHasActions：决定是否挂 run_skill_action——无 action false、有 action true、
// 仅 disabled skill 的 action false（跳过 DisableModelInvocation）。
func TestBoxHasActions(t *testing.T) {
	if boxHasActions(mkActionBox(t, false, false)) {
		t.Error("无 action 应 false")
	}
	if !boxHasActions(mkActionBox(t, true, false)) {
		t.Error("有 action 应 true")
	}
	if boxHasActions(mkActionBox(t, true, true)) {
		t.Error("仅 disabled skill 的 action 应 false")
	}
}

func TestSkillActionTool_Call_GoFunc(t *testing.T) {
	tl := newSkillActionTool(mkActionBox(t, true, false))
	res, err := tl.Call(context.Background(), map[string]any{
		"skill": "calc", "action": "add", "args": map[string]any{"x": "7"}})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	out, _ := res.Output.(string)
	if !res.Success || !strings.Contains(out, "sum=7") {
		t.Errorf("结果错: success=%v output=%q", res.Success, out)
	}
}

func TestSkillActionTool_Call_DisabledGate(t *testing.T) {
	tl := newSkillActionTool(mkActionBox(t, true, true))
	res, _ := tl.Call(context.Background(), map[string]any{"skill": "calc", "action": "add"})
	if res.Success || res.Error == nil || res.Error.Code != SkillDisabledErrCode {
		t.Errorf("disabled skill 的 action 应返 SkillDisabledErrCode, 实际 %+v", res)
	}
}

func TestSkillActionTool_Call_NotFound(t *testing.T) {
	tl := newSkillActionTool(mkActionBox(t, true, false))
	res, _ := tl.Call(context.Background(), map[string]any{"skill": "calc", "action": "nope"})
	if res.Success {
		t.Error("不存在的 action 应失败")
	}
}

func TestSkillActionTool_Call_MissingArgs(t *testing.T) {
	tl := newSkillActionTool(mkActionBox(t, true, false))
	res, _ := tl.Call(context.Background(), map[string]any{"skill": "calc"})
	if res.Success || res.Error == nil || res.Error.Code != tool.ErrCodeInvalidArguments {
		t.Errorf("缺 action 应返 InvalidArguments, 实际 %+v", res)
	}
}

// TestSkillActionTool_Call_InterruptPropagate：业务 ActionFunc 抛 InterruptError 做 HITL 审批 →
// Call 必须 propagate（res nil + IsInterrupt），否则 cp 不存、HITL 链断（同 activate_skill R24 协议）。
func TestSkillActionTool_Call_InterruptPropagate(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "calc")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"),
		[]byte("---\nname: calc\ndescription: x\n---\nbody\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	box, err := skill.NewDefaultSkillBox(root, skill.WithSkillActionFunc("calc",
		skill.SkillActionDescriptor{Name: "gate"},
		func(_ context.Context, _ map[string]any) (*skill.SkillExecutionResult, error) {
			return nil, &compose_v2.InterruptError{Query: &compose_v2.AskQuery{InterruptID: "gate1", Question: "approve?"}}
		}))
	if err != nil {
		t.Fatal(err)
	}
	res, callErr := newSkillActionTool(box).Call(context.Background(), map[string]any{"skill": "calc", "action": "gate"})
	if res != nil {
		t.Errorf("InterruptError 应 propagate（res nil），实际 res=%+v", res)
	}
	if _, isInt := compose_v2.IsInterrupt(callErr); !isInt {
		t.Errorf("应 propagate InterruptError，got %v", callErr)
	}
}

func TestFormatActionResult(t *testing.T) {
	if got := formatActionResult(&skill.SkillExecutionResult{Success: true, Output: "ok"}); got != "ok" {
		t.Errorf("success output: %q", got)
	}
	if got := formatActionResult(&skill.SkillExecutionResult{Success: false, Error: "boom", ExitCode: 2}); !strings.Contains(got, "boom") || !strings.Contains(got, "exit 2") {
		t.Errorf("failure 应含 error + exit code: %q", got)
	}
	if got := formatActionResult(&skill.SkillExecutionResult{Success: true}); !strings.Contains(got, "no output") {
		t.Errorf("空成功: %q", got)
	}
	if got := formatActionResult(&skill.SkillExecutionResult{Success: true, Stdout: "hi", Stderr: "warn"}); !strings.Contains(got, "hi") || !strings.Contains(got, "[stderr] warn") {
		t.Errorf("脚本 stdout/stderr: %q", got)
	}
}
