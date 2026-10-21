//go:build integration

// 真 deepseek 集成验证：thinking 模型多轮 reasoning_content 回传（ISSUE-89 修复）。
// 默认 make test / go test ./... 不编译（integration tag）。
// 跑: go test -tags=integration ./model/deepseek -v
// 需 .env DEEPSEEK_API_KEY（envloader.Load 加载；worktree 里需 .env 在 go.mod 同级，
// 未设则 Skip）。
package deepseek_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"crew-go/example/envloader"
	"crew-go/model/deepseek"
	"crew-go/schema"
)

// TestDeepSeek_ThinkingMultiTurn_ReasoningPassback 端到端验：per-request 开 thinking
// （WithThinkingOption）→ 响应解析 reasoning_content（convertResponse）→ 多轮把上轮
// assistant 的 reasoning_content 原样回传（convertMessages）。修复前第 2 轮返
// 400 "The reasoning_content in the thinking mode must be passed back to the API."。
func TestDeepSeek_ThinkingMultiTurn_ReasoningPassback(t *testing.T) {
	envloader.Load()
	if os.Getenv("DEEPSEEK_API_KEY") == "" {
		t.Skip("DEEPSEEK_API_KEY 未设（.env 缺）")
	}
	m := deepseek.NewClient(os.Getenv("DEEPSEEK_API_KEY"), "")

	// 轮 1：per-request 开 thinking（覆盖构造期默认关）。
	resp1, err := m.Generate(context.Background(),
		[]*schema.Message{schema.UserMessage("3 的平方是多少？只回数字")},
		deepseek.WithThinkingOption(true))
	if err != nil {
		t.Fatalf("turn1: %v", err)
	}
	if resp1.ReasoningContent == "" {
		t.Fatalf("turn1 ReasoningContent 空——thinking 未开或未解析")
	}
	t.Logf("turn1 content=%q reasoning_len=%d", resp1.Content, len(resp1.ReasoningContent))

	// 轮 2：回传 resp1（带 reasoning_content）——ISSUE-89 关键，不回传则 400。
	history := []*schema.Message{
		schema.UserMessage("3 的平方是多少？只回数字"),
		resp1,
		schema.UserMessage("再乘以 2 呢？只回数字"),
	}
	resp2, err := m.Generate(context.Background(), history, deepseek.WithThinkingOption(true))
	if err != nil {
		t.Fatalf("turn2（多轮回传，若 400 说明 reasoning 回传失效）: %v", err)
	}
	if !strings.Contains(resp2.Content, "18") {
		t.Fatalf("turn2 应含 18（9×2），got %q", resp2.Content)
	}
	t.Logf("turn2 content=%q", resp2.Content)
}
