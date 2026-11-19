package skill_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"crew-go/skill"
)

func writeSkillFile(t *testing.T, root, path, text string) {
	t.Helper()
	file := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultSkillBoxRecursiveDiscoveryKeepsPackageBoundaries(t *testing.T) {
	root := t.TempDir()
	writeSkillFile(t, root, "index/SKILL.md", "---\nname: index\n---\nIndex")
	writeSkillFile(t, root, "ops/inventory/SKILL.md", "---\nname: inventory\n---\nInventory")
	writeSkillFile(t, root, "ops/inventory/example/SKILL.md", "---\nname: embedded\n---\nExample")
	flat, err := skill.NewDefaultSkillBox(root)
	if err != nil {
		t.Fatal(err)
	}
	if entries := flat.ListSkills(); len(entries) != 1 || entries[0].Name != "index" {
		t.Fatalf("默认发现应只加载一级目录: %v", entries)
	}
	box, err := skill.NewDefaultSkillBox(root, skill.WithRecursiveDiscovery(true))
	if err != nil {
		t.Fatal(err)
	}
	entries := box.ListSkills()
	if len(entries) != 2 || entries[0].Name != "index" || entries[1].Name != "inventory" {
		t.Fatalf("递归发现应穿过分类目录，并在 skill 包边界停止: %v", entries)
	}
	if resources := box.ListSkillResources("inventory"); len(resources) != 1 || resources[0].Path != filepath.Join("example", "SKILL.md") {
		t.Fatalf("包内部文件仍应作为资源: %v", resources)
	}
	allowed, err := skill.NewDefaultSkillBox(root, skill.WithRecursiveDiscovery(true), skill.WithAllowList([]string{"index", "embedded"}))
	if err != nil {
		t.Fatal(err)
	}
	if entries := allowed.ListSkills(); len(entries) != 1 || entries[0].Name != "index" {
		t.Fatalf("不在允许名单内的包也应保持发现边界: %v", entries)
	}
}

func TestDefaultSkillBoxReloadPublishesCompleteCatalog(t *testing.T) {
	root := t.TempDir()
	const inventory = "inventory/SKILL.md"
	writeSkillFile(t, root, inventory, "---\nname: inventory\n---\nfirst playbook")
	box, err := skill.NewDefaultSkillBox(root, skill.WithHotReload(true),
		skill.WithSkillActionFunc("inventory", skill.SkillActionDescriptor{Name: "count"},
			func(context.Context, map[string]any) (*skill.SkillExecutionResult, error) {
				return &skill.SkillExecutionResult{Stdout: "counted"}, nil
			}))
	if err != nil {
		t.Fatal(err)
	}
	old, version := box.Snapshot()
	writeSkillFile(t, root, inventory, "---\nname: inventory\n---\nsecond playbook")
	writeSkillFile(t, root, "review/SKILL.md", "---\nname: review\nwhen-to-use: first\nwhen_to_use: second\n---\nReview")
	if err := box.Reload(); err == nil || !strings.Contains(err.Error(), "同一字段重复声明") {
		t.Fatalf("随机覆盖的别名应阻止目录发布: %v", err)
	}
	current, retained := box.Snapshot()
	if retained != version || current.ReadSkillPrompt("inventory") != "first playbook" {
		t.Fatalf("失败重载改变了已发布目录: %q", retained)
	}
	writeSkillFile(t, root, "review/SKILL.md", "---\nname: review\n---\nReview")
	if err := box.Reload(); err != nil {
		t.Fatal(err)
	}
	current, updated := box.Snapshot()
	if updated == "" || updated == version || len(current.ListSkills()) != 2 ||
		current.ReadSkillPrompt("inventory") != "second playbook" || old.ReadSkillPrompt("inventory") != "first playbook" {
		t.Fatalf("目录发布或旧快照异常: old=%q new=%q skills=%v", version, updated, current.ListSkills())
	}
	if action, err := current.RunSkillAction(t.Context(), "inventory", "count", nil); err != nil || action == nil || action.Stdout != "counted" {
		t.Fatalf("重载后 Go action 丢失: result=%v err=%v", action, err)
	}
	if err := old.Reload(); err == nil {
		t.Fatal("固定快照应拒绝重载")
	}
}

func TestSkillResourcesUseScannedDirectory(t *testing.T) {
	root := t.TempDir()
	const manifest = "---\nname: inventory\n---\nInventory"
	writeSkillFile(t, root, "inventory/SKILL.md", manifest)
	writeSkillFile(t, root, "inventory/reference.md", "original")
	box, err := skill.NewDefaultSkillBox(root, skill.WithHotReload(true))
	if err != nil {
		t.Fatal(err)
	}
	view, _ := box.Snapshot()
	if got, complete, err := view.ReadSkillReference("inventory", "reference.md", 100); err != nil || !complete || got != "original" {
		t.Fatalf("原目录参考文件读取失败: %q %v %v", got, complete, err)
	}
	oldDir := filepath.Join(t.TempDir(), "inventory")
	if err := os.Rename(filepath.Join(root, "inventory"), oldDir); err != nil {
		t.Fatal(err)
	}
	writeSkillFile(t, root, "inventory/SKILL.md", manifest)
	writeSkillFile(t, root, "inventory/reference.md", "replacement")
	if got, _, err := view.ReadSkillReference("inventory", "reference.md", 100); got != "" || err == nil {
		t.Fatalf("旧快照从替换目录读取了参考文档: %q %v", got, err)
	}
	if err := box.Reload(); err != nil {
		t.Fatal(err)
	}
	if got := box.ReadSkillResource("inventory", "reference.md"); got != "replacement" {
		t.Fatalf("新快照应能读取新目录: %q", got)
	}
	remote, err := skill.NewInMemorySkillBox("remote", []skill.SkillContent{{
		Descriptor: skill.SkillDescriptor{Name: "inventory"}, Dir: oldDir,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := remote.ReadSkillResource("inventory", "reference.md"); got != "" || len(remote.ListSkillResources("inventory")) != 0 {
		t.Fatalf("内存来源的 Dir 仅作信息字段: %q", got)
	}
}
