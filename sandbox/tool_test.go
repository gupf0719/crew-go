package sandbox_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/sandbox"
)

// stubSandbox 记录收到的 command，返固定 result——验 NewBashTool 转发正确，不依赖真实沙箱。
type stubSandbox struct {
	gotCmd string
	result *sandbox.ExecResult
	err    error
}

func (s *stubSandbox) Exec(_ context.Context, cmd string) (*sandbox.ExecResult, error) {
	s.gotCmd = cmd
	if s.err != nil {
		return nil, s.err
	}
	return s.result, nil
}

// TestNewBashTool_ForwardsCommandAndReturnsOutput 验 sandbox_bash 把 command 原样转发给
// Sandbox.Exec、并把 Output 回灌。
func TestNewBashTool_ForwardsCommandAndReturnsOutput(t *testing.T) {
	stub := &stubSandbox{result: &sandbox.ExecResult{Output: "hello world"}}
	tl := sandbox.NewBashTool(stub)
	if tl.Name() != sandbox.BashToolName {
		t.Fatalf("tool name = %q, want %q", tl.Name(), sandbox.BashToolName)
	}
	res, err := tl.Call(context.Background(), map[string]any{"command": "cat /workspace/x/SKILL.md"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if stub.gotCmd != "cat /workspace/x/SKILL.md" {
		t.Fatalf("Sandbox.Exec 收到的 command 不对: %q", stub.gotCmd)
	}
	if out, _ := res.Output.(string); !strings.Contains(out, "hello world") {
		t.Fatalf("tool result 不含 Exec Output: %v", res.Output)
	}
}

// TestNewBashTool_EmptyCommandRejected 验空 command 报参数错、且不调 Exec。
func TestNewBashTool_EmptyCommandRejected(t *testing.T) {
	stub := &stubSandbox{result: &sandbox.ExecResult{}}
	tl := sandbox.NewBashTool(stub)
	res, err := tl.Call(context.Background(), map[string]any{"command": "  "})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if res.Error == nil {
		t.Fatalf("空 command 应报参数错")
	}
	if stub.gotCmd != "" {
		t.Fatalf("空 command 不该调 Exec，got %q", stub.gotCmd)
	}
}

// TestNewBashTool_NonZeroExitAnnotated 验非零 exit 前缀标注（让 LLM 看到命令失败）。
func TestNewBashTool_NonZeroExitAnnotated(t *testing.T) {
	stub := &stubSandbox{result: &sandbox.ExecResult{Output: "boom", ExitCode: 2}}
	tl := sandbox.NewBashTool(stub)
	res, _ := tl.Call(context.Background(), map[string]any{"command": "false"})
	if out, _ := res.Output.(string); !strings.Contains(out, "exit 2") {
		t.Fatalf("非零 exit 应标注，got %q", res.Output)
	}
}

// TestNewBashTool_ExecErrorPropagates 验后端传输故障（非命令失败）冒泡为 error。
func TestNewBashTool_ExecErrorPropagates(t *testing.T) {
	stub := &stubSandbox{err: errors.New("connection refused")}
	tl := sandbox.NewBashTool(stub)
	_, err := tl.Call(context.Background(), map[string]any{"command": "echo hi"})
	if err == nil {
		t.Fatalf("Sandbox.Exec 传输故障应冒泡为 error")
	}
}

// TestNewBashTool_NilResultGuarded 验后端违约返 (nil, nil) 时报 error 不 panic。
func TestNewBashTool_NilResultGuarded(t *testing.T) {
	stub := &stubSandbox{result: nil} // Exec 返 (nil, nil)
	tl := sandbox.NewBashTool(stub)
	_, err := tl.Call(context.Background(), map[string]any{"command": "echo hi"})
	if err == nil {
		t.Fatalf("后端返 nil result 应报 error（防 formatResult panic）")
	}
}

// TestNewBashTool_FramesUntrustedOutput 验沙箱输出被包进带随机 nonce 的不可信数据标签（防
// file-content 注入）+ 工具描述含安全声明 + nonce 每次随机。
func TestNewBashTool_FramesUntrustedOutput(t *testing.T) {
	stub := &stubSandbox{result: &sandbox.ExecResult{Output: "hello"}}
	tl := sandbox.NewBashTool(stub)
	if !strings.Contains(tl.Description(), "不可信") {
		t.Fatalf("工具描述应含不可信输出安全声明")
	}
	res, _ := tl.Call(context.Background(), map[string]any{"command": "echo hello"})
	out, _ := res.Output.(string)
	if !strings.Contains(out, "<sandbox_output_") || !strings.Contains(out, "</sandbox_output_") {
		t.Fatalf("输出应包进 <sandbox_output_XXXX> 框定标签, got: %q", out)
	}
	if !strings.Contains(out, "hello") {
		t.Fatalf("框定应保留原输出, got: %q", out)
	}
	// nonce 每次随机——两次 Call 的框定不该完全相同（防沙箱内容伪造闭合标签逃逸）
	res2, _ := tl.Call(context.Background(), map[string]any{"command": "echo hello"})
	if out2, _ := res2.Output.(string); out == out2 {
		t.Fatalf("nonce 应每次随机（两次框定不该完全相同）")
	}
}
