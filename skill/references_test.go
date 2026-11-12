package skill_test

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"crew-go/skill"
)

func writeSkillResource(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDefaultSkillBox_ReferenceDiscoveryAndRead(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, "retention", "---\nname: retention\ndescription: 归档策略\n---\n核心规则")
	dir := filepath.Join(root, "retention")
	const policy = "保留期"
	const metadata = "---\ndescription: retention policy\nwhen: audit\ntopics: [retention, policy]\n---\n"
	writeSkillResource(t, dir, "references/a.md", metadata+policy)
	writeSkillResource(t, dir, "references/b.txt", metadata+"附录")
	writeSkillResource(t, dir, "references/always.txt", "---\ndescription: team checklist\nalways_load: true\n---\n核对清单")
	writeSkillResource(t, dir, "references/plain.txt", "普通说明")
	writeSkillResource(t, dir, "references/invalid.md", "---\ndescription: [wrong type]\n---\n备用正文")
	writeSkillResource(t, dir, "assets/sample.json", "{}")

	box, err := skill.NewDefaultSkillBox(root)
	if err != nil {
		t.Fatal(err)
	}
	active, err := box.ActivateSkill(t.Context(), "retention")
	if err != nil {
		t.Fatal(err)
	}
	matches := skill.MatchReferences("retention policy", active.Resources)
	var paths []string
	for _, match := range matches {
		paths = append(paths, match.Resource.Path)
	}
	if want := []string{"references/always.txt", "references/a.md", "references/b.txt"}; !slices.Equal(paths, want) {
		t.Fatalf("参考文档应按常载、相关性和路径排序，实际 %q，期望 %q", paths, want)
	}
	if ref := matches[1].Resource; ref.Description != "retention policy" || ref.When != "audit" ||
		!slices.Equal(ref.Topics, []string{"retention", "policy"}) || matches[1].Score <= 0 {
		t.Fatalf("扫描的元数据应参与匹配，实际 %+v", matches[1])
	}

	for _, tc := range []struct {
		path     string
		maxBytes int
		body     string
		complete bool
	}{
		{"references/a.md", len(policy), policy, true},
		{"references/a.md", 4, "保", false},
		{"references/plain.txt", 64, "普通说明", true},
		{"references/invalid.md", 64, "备用正文", true},
	} {
		body, complete, err := box.ReadSkillReference("retention", tc.path, tc.maxBytes)
		if err != nil || body != tc.body || complete != tc.complete {
			t.Errorf("读取 %s（%d 字节）实际正文 %q complete=%v err=%v，期望正文 %q complete=%v",
				tc.path, tc.maxBytes, body, complete, err, tc.body, tc.complete)
		}
	}
	if _, _, err := box.ReadSkillReference("retention", "references/a.md", 0); err == nil {
		t.Error("零读取预算应报错")
	}
	for _, path := range []string{"references/missing.md", "assets/sample.json"} {
		if _, _, err := box.ReadSkillReference("retention", path, 64); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("未登记为参考文档的 %s 应报不存在，实际 %v", path, err)
		}
	}
}

func TestDefaultSkillBox_ReferenceReadAfterFileChanges(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, "retention", "---\nname: retention\ndescription: 归档策略\n---\n核心规则")
	dir := filepath.Join(root, "retention")
	const removed = "references/removed.txt"
	const replaced = "references/replaced.txt"
	removedPath := writeSkillResource(t, dir, removed, "旧文档")
	replacedPath := writeSkillResource(t, dir, replaced, "旧文档")
	box, err := skill.NewDefaultSkillBox(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(removedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(replacedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(replacedPath, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := box.ReadSkillReference("retention", removed, 64); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("读取已移走的文件应报不存在，实际 %v", err)
	}
	if body, complete, err := box.ReadSkillReference("retention", replaced, 64); err == nil || complete || body != "" {
		t.Errorf("应拒绝已变为目录的资源，实际正文 %q complete=%v err=%v", body, complete, err)
	}
}
