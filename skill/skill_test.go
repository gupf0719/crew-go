package skill_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"crew-go/skill"
)

// writeSkill 在 rootDir/name/SKILL.md 写入内容，辅助测试。
func writeSkill(t *testing.T, rootDir, name, content string) {
	t.Helper()
	dir := filepath.Join(rootDir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(content), 0o644); err != nil {
		t.Fatalf("write SKILL.md: %v", err)
	}
}

func TestDefaultSkillBox_Load(t *testing.T) {
	rootDir := t.TempDir()

	writeSkill(t, rootDir, "code-review", `---
name: code-review
description: 对 Go 代码进行深度 review
whenToUse: 用户提交代码时
tags:
  - golang
  - review
multiTurn: true
maxTurns: 5
---

你是一位资深 Go 工程师，请对提交的代码进行 review。
`)

	writeSkill(t, rootDir, "translate", `---
name: translate
description: 翻译文本
tags:
  - translation
---

你是一位翻译专家。
`)

	// 无 SKILL.md 的目录，应被静默跳过
	if err := os.MkdirAll(filepath.Join(rootDir, "empty-dir"), 0o755); err != nil {
		t.Fatal(err)
	}

	box, err := skill.NewDefaultSkillBox(rootDir)
	if err != nil {
		t.Fatalf("NewDefaultSkillBox: %v", err)
	}
	if got := len(box.ListSkills()); got != 2 {
		t.Fatalf("expected 2 skills, got %d", got)
	}

	// ActivateSkill — 正常路径
	active, err := box.ActivateSkill(context.Background(), "code-review")
	if err != nil {
		t.Fatalf("ActivateSkill: %v", err)
	}
	if active.Skill.Name != "code-review" {
		t.Errorf("expected name code-review, got %q", active.Skill.Name)
	}
	if active.Prompt == "" {
		t.Error("expected non-empty prompt")
	}
	if len(active.Skill.Tags) != 2 {
		t.Errorf("expected 2 tags, got %v", active.Skill.Tags)
	}

	// ActivateSkill — 不存在
	if _, err := box.ActivateSkill(context.Background(), "nonexistent"); err == nil {
		t.Error("expected error for nonexistent skill")
	}
}

func TestDefaultSkillBox_MatchSkills(t *testing.T) {
	rootDir := t.TempDir()

	writeSkill(t, rootDir, "code-review", `---
name: code-review
description: review Go code quality
whenToUse: when user submits code
tags:
  - golang
  - code-quality
---
`)

	writeSkill(t, rootDir, "translate", `---
name: translate
description: translate text between languages
tags:
  - translation
  - language
---
`)

	box, err := skill.NewDefaultSkillBox(rootDir)
	if err != nil {
		t.Fatalf("NewDefaultSkillBox: %v", err)
	}

	results := box.MatchSkills(context.Background(), "review code golang")
	if len(results) == 0 {
		t.Fatal("expected at least one match")
	}
	// code-review 应排第一
	if results[0].Skill.Name != "code-review" {
		t.Errorf("expected code-review first, got %q", results[0].Skill.Name)
	}
	if results[0].Score <= 0 {
		t.Error("expected positive score")
	}

	// 无匹配
	noMatch := box.MatchSkills(context.Background(), "xyzzy foobar")
	if len(noMatch) != 0 {
		t.Errorf("expected no matches, got %d", len(noMatch))
	}
}

func TestDefaultSkillBox_CJKMatch(t *testing.T) {
	rootDir := t.TempDir()

	writeSkill(t, rootDir, "code-review", `---
name: code-review
description: 对代码进行深度审查
tags:
  - 代码审查
  - golang
---
`)

	box, _ := skill.NewDefaultSkillBox(rootDir)
	results := box.MatchSkills(context.Background(), "代码审查")
	if len(results) == 0 {
		t.Fatal("expected CJK match")
	}
	if results[0].Score <= 0 {
		t.Error("expected positive score for CJK query")
	}
}

func TestDefaultSkillBox_EmptyRoot(t *testing.T) {
	// rootDir 不存在：不报错，box 为空
	box, err := skill.NewDefaultSkillBox("/nonexistent/path/xyz_crew_test")
	if err != nil {
		t.Fatalf("expected no error for missing root, got: %v", err)
	}
	if len(box.ListSkills()) != 0 {
		t.Error("expected empty skill list")
	}
}

func TestDefaultSkillBox_AllowList(t *testing.T) {
	rootDir := t.TempDir()

	writeSkill(t, rootDir, "code-review", `---
name: code-review
description: review code
---
`)
	writeSkill(t, rootDir, "translate", `---
name: translate
description: translate text
---
`)

	box, err := skill.NewDefaultSkillBox(rootDir, skill.WithAllowList([]string{"translate"}))
	if err != nil {
		t.Fatalf("NewDefaultSkillBox: %v", err)
	}
	if len(box.ListSkills()) != 1 {
		t.Fatalf("expected 1 skill, got %d", len(box.ListSkills()))
	}
	if box.ListSkills()[0].Name != "translate" {
		t.Errorf("expected translate, got %q", box.ListSkills()[0].Name)
	}
}

func TestDefaultSkillBox_RunSkillAction_NotFound(t *testing.T) {
	box, _ := skill.NewDefaultSkillBox(t.TempDir())
	if _, err := box.RunSkillAction(context.Background(), "any", "action", nil); !errors.Is(err, skill.ErrSkillActionNotFound) {
		t.Errorf("空 box RunSkillAction 应返 ErrSkillActionNotFound, got %v", err)
	}
}

// TestParseFrontmatter_UserInvocableDefaultsTrue 验证 UserInvocable 未声明时默认 true。
//
// **bomb 防御**：UserInvocable 是 bool zero value=false 的字段，若 parseFrontmatter
// 没显式设默认，sanity-revert 删 `UserInvocable: true` 初始化后 frontmatter 无声明
// 时会落 false——business 期望"默认开启"行为破裂。
//
// 用 default-omitted SKILL.md 让缺省路径走，断言 desc.UserInvocable == true。
// sanity-revert 删 default true 初始化 → desc.UserInvocable == false → fail。
func TestParseFrontmatter_UserInvocableDefaultsTrue(t *testing.T) {
	rootDir := t.TempDir()
	// 故意不写 user-invocable / disable-model-invocation 任一字段
	writeSkill(t, rootDir, "default-skill", `---
name: default-skill
description: 默认状态
---
body
`)
	box, err := skill.NewDefaultSkillBox(rootDir)
	if err != nil {
		t.Fatalf("NewDefaultSkillBox: %v", err)
	}
	skills := box.ListSkills()
	if len(skills) != 1 {
		t.Fatalf("expected 1 skill, got %d", len(skills))
	}
	if !skills[0].UserInvocable {
		t.Errorf("UserInvocable 未声明应默认 true，实际 %v", skills[0].UserInvocable)
	}
	if skills[0].DisableModelInvocation {
		t.Errorf("DisableModelInvocation 未声明应默认 false，实际 %v", skills[0].DisableModelInvocation)
	}
}

// TestParseFrontmatter_TwoDimensionExplicit 验证显式声明的 4 种组合都正确解析。
//
// **bomb 防御（5 类炸弹·第 5 类——mock 不读 input 类比）**：
// fixture 4 case 让"显式 false 覆盖默认 true"路径必走——sanity-revert 把
// `parseBool(p.scalar)` 改成 `desc.UserInvocable = true` 强制赋 true 时，
// case "userOnly" / "neither" 必 fail。
func TestParseFrontmatter_TwoDimensionExplicit(t *testing.T) {
	rootDir := t.TempDir()

	cases := []struct {
		dirName           string
		fmExtra           string
		wantDisableModel  bool
		wantUserInvocable bool
	}{
		{
			dirName:           "both-default",
			fmExtra:           "", // 都不声明
			wantDisableModel:  false,
			wantUserInvocable: true,
		},
		{
			dirName:           "model-only",
			fmExtra:           "user-invocable: false",
			wantDisableModel:  false,
			wantUserInvocable: false,
		},
		{
			dirName:           "user-only",
			fmExtra:           "disable-model-invocation: true",
			wantDisableModel:  true,
			wantUserInvocable: true,
		},
		{
			dirName:           "neither",
			fmExtra:           "disable-model-invocation: true\nuser-invocable: false",
			wantDisableModel:  true,
			wantUserInvocable: false,
		},
	}

	for _, c := range cases {
		fm := "name: " + c.dirName + "\ndescription: 测试 " + c.dirName + "\n"
		if c.fmExtra != "" {
			fm += c.fmExtra + "\n"
		}
		writeSkill(t, rootDir, c.dirName, "---\n"+fm+"---\nbody\n")
	}

	box, err := skill.NewDefaultSkillBox(rootDir)
	if err != nil {
		t.Fatalf("NewDefaultSkillBox: %v", err)
	}
	got := make(map[string]skill.SkillDescriptor, len(cases))
	for _, s := range box.ListSkills() {
		got[s.Name] = s
	}
	for _, c := range cases {
		s, ok := got[c.dirName]
		if !ok {
			t.Errorf("case %s: skill 未加载", c.dirName)
			continue
		}
		if s.DisableModelInvocation != c.wantDisableModel {
			t.Errorf("case %s: DisableModelInvocation want %v got %v", c.dirName, c.wantDisableModel, s.DisableModelInvocation)
		}
		if s.UserInvocable != c.wantUserInvocable {
			t.Errorf("case %s: UserInvocable want %v got %v", c.dirName, c.wantUserInvocable, s.UserInvocable)
		}
	}
}

// TestParseFrontmatter_UserInvocableUnrecognizedPreservesDefault 验证 R12 audit MED-1：
// `user-invocable: <未识别值>`（如 typo / "maybe" / 中文）必须保留默认 true，
// 不能被旧 parseBool "未识别 → false" 语义静默翻转。
//
// **bomb 防御（5 类炸弹·第 5 类——mock 不读 input）**：
// fixture 写 `user-invocable: maybe`——若 parseBool（旧实现）跑，maybe 不在
// "true/yes/1" 集 → 返 false → desc.UserInvocable = false（与默认 true 相反）。
// 新实现 parseBoolOptional 返 (false, false) → caller 跳过赋值 → 保留默认 true。
//
// sanity-revert 改回 parseBool → UserInvocable=false → fail。
func TestParseFrontmatter_UserInvocableUnrecognizedPreservesDefault(t *testing.T) {
	rootDir := t.TempDir()
	cases := []struct {
		dirName string
		value   string
	}{
		{"maybe", "maybe"},
		{"typo", "ture"},      // truee 拼错
		{"chinese", "是的"},     // 中文
		{"random", "perhaps"}, // 随便填
	}
	for _, c := range cases {
		writeSkill(t, rootDir, c.dirName, "---\nname: "+c.dirName+"\ndescription: x\nuser-invocable: "+c.value+"\n---\nbody\n")
	}

	box, err := skill.NewDefaultSkillBox(rootDir)
	if err != nil {
		t.Fatalf("NewDefaultSkillBox: %v", err)
	}
	got := make(map[string]skill.SkillDescriptor)
	for _, s := range box.ListSkills() {
		got[s.Name] = s
	}
	for _, c := range cases {
		s, ok := got[c.dirName]
		if !ok {
			t.Errorf("case %s: skill 未加载", c.dirName)
			continue
		}
		if !s.UserInvocable {
			t.Errorf("case %s (user-invocable: %q)：未识别值应保留默认 true，实际 UserInvocable=false——"+
				"R12 audit MED-1 回归（parseBool 旧实现把未识别视为 false 静默翻转默认）",
				c.dirName, c.value)
		}
	}
}

// TestParseFrontmatter_BoolExtendedAliases 验证 parseBoolOptional 扩展别名：
// y/n/on/off 都能识别。
//
// **R13 audit INFO-1 fix**：旧字段名 `want bool` 双重否定（"是否翻转默认"）易让
// 后续贡献者误读。改正向 `wantUserInvocable bool` 直接表达期望值，断言无反逻辑。
func TestParseFrontmatter_BoolExtendedAliases(t *testing.T) {
	rootDir := t.TempDir()
	cases := []struct {
		dirName           string
		value             string
		wantUserInvocable bool // 正向期望——直接断言 UserInvocable == wantUserInvocable
	}{
		{"y", "y", true},      // user-invocable: y → true
		{"n", "n", false},     // user-invocable: n → false
		{"on", "on", true},    // user-invocable: on → true
		{"off", "off", false}, // user-invocable: off → false
		{"upper-Y", "Y", true},
		{"upper-N", "N", false},
	}
	for _, c := range cases {
		writeSkill(t, rootDir, c.dirName, "---\nname: "+c.dirName+"\ndescription: x\nuser-invocable: "+c.value+"\n---\nbody\n")
	}
	box, err := skill.NewDefaultSkillBox(rootDir)
	if err != nil {
		t.Fatalf("NewDefaultSkillBox: %v", err)
	}
	got := make(map[string]skill.SkillDescriptor)
	for _, s := range box.ListSkills() {
		got[s.Name] = s
	}
	for _, c := range cases {
		s := got[c.dirName]
		if s.UserInvocable != c.wantUserInvocable {
			t.Errorf("case %s (user-invocable: %q)：want UserInvocable=%v got %v",
				c.dirName, c.value, c.wantUserInvocable, s.UserInvocable)
		}
	}
}

// TestParseFrontmatter_ContextAgentModel 验证 P2.B/C 三新字段（context / agent / model）
// frontmatter 解析。
func TestParseFrontmatter_ContextAgentModel(t *testing.T) {
	rootDir := t.TempDir()
	writeSkill(t, rootDir, "research", `---
name: research
description: 调研子任务
context: fork
agent: research_specialist
model: gpt-4
---
你是调研专家。
`)
	writeSkill(t, rootDir, "translate-default", `---
name: translate-default
description: 默认 inline 翻译
---
你是翻译专家。
`)

	box, err := skill.NewDefaultSkillBox(rootDir)
	if err != nil {
		t.Fatalf("NewDefaultSkillBox: %v", err)
	}
	got := make(map[string]skill.SkillDescriptor)
	for _, s := range box.ListSkills() {
		got[s.Name] = s
	}
	r := got["research"]
	if r.Context != "fork" {
		t.Errorf("research.Context want 'fork' got %q", r.Context)
	}
	if r.Agent != "research_specialist" {
		t.Errorf("research.Agent want 'research_specialist' got %q", r.Agent)
	}
	if r.Model != "gpt-4" {
		t.Errorf("research.Model want 'gpt-4' got %q", r.Model)
	}
	// 默认值：未声明 → 三字段为空（caller 视为 inline / 无 sub-agent / parent model）
	td := got["translate-default"]
	if td.Context != "" {
		t.Errorf("translate-default.Context 未声明应为空，实际 %q", td.Context)
	}
	if td.Agent != "" {
		t.Errorf("translate-default.Agent 未声明应为空，实际 %q", td.Agent)
	}
	if td.Model != "" {
		t.Errorf("translate-default.Model 未声明应为空，实际 %q", td.Model)
	}
}

// TestParseFrontmatter_SnakeCaseAlias 验证 disable_model_invocation / user_invocable
// snake_case 别名也支持（YAML key 风格不一是 LLM 编辑常见情形）。
func TestParseFrontmatter_SnakeCaseAlias(t *testing.T) {
	rootDir := t.TempDir()
	writeSkill(t, rootDir, "snake", `---
name: snake
description: snake case
disable_model_invocation: true
user_invocable: false
---
body
`)
	box, _ := skill.NewDefaultSkillBox(rootDir)
	skills := box.ListSkills()
	if len(skills) != 1 {
		t.Fatalf("expected 1 skill, got %d", len(skills))
	}
	if !skills[0].DisableModelInvocation {
		t.Errorf("snake_case disable_model_invocation 应解析为 true")
	}
	if skills[0].UserInvocable {
		t.Errorf("snake_case user_invocable: false 应覆盖默认 true")
	}
}
