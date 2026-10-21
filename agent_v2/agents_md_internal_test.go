package agent_v2

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestLoadAgentsMD 验 loadAgentsMD：按序拼接 + trim + 缺文件/空内容跳过 + 全缺返空串。
func TestLoadAgentsMD(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "A.md")
	b := filepath.Join(dir, "B.md")
	if err := os.WriteFile(a, []byte("  rule A  "), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, []byte("rule B"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// 按序拼接 + 各自 trim + \n\n 分隔
	if got, want := loadAgentsMD(ctx, []string{a, b}), "rule A\n\nrule B"; got != want {
		t.Errorf("拼接 = %q; want %q", got, want)
	}
	// 缺文件静默跳过, 不中断后续
	if got := loadAgentsMD(ctx, []string{filepath.Join(dir, "missing.md"), b}); got != "rule B" {
		t.Errorf("缺文件应跳过, got %q; want %q", got, "rule B")
	}
	// 全缺 → 空串
	if got := loadAgentsMD(ctx, []string{filepath.Join(dir, "none.md")}); got != "" {
		t.Errorf("全缺应返空串, got %q", got)
	}
	// 空内容文件跳过 (不产生多余 \n\n)
	empty := filepath.Join(dir, "empty.md")
	if err := os.WriteFile(empty, []byte("   \n  "), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := loadAgentsMD(ctx, []string{empty, b}); got != "rule B" {
		t.Errorf("空内容文件应跳过, got %q; want %q", got, "rule B")
	}
}
