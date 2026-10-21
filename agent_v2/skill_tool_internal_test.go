package agent_v2

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"crew-go/skill"
	"crew-go/tool"
)

// formatSkillListing 是内部函数 + 纯算法 → 用 internal_test 白盒测三档。

// TestFormatSkillListing_Tier1_FullFitsBudget 验证档 1：全描述 fit 预算 → 全描述发。
func TestFormatSkillListing_Tier1_FullFitsBudget(t *testing.T) {
	skills := []skill.SkillDescriptor{
		{Name: "translate", Description: "中英文互译"},
		{Name: "code-review", Description: "对 Go 代码做深度 review"},
	}
	out := formatSkillListing(skills, 4000, 200)
	if !strings.Contains(out, "translate: 中英文互译") {
		t.Errorf("档 1 应保全描述，实际 %q", out)
	}
	if !strings.Contains(out, "code-review: 对 Go 代码做深度 review") {
		t.Errorf("档 1 应保全描述，实际 %q", out)
	}
	if strings.Contains(out, "…") {
		t.Errorf("档 1 不应有截断符，实际 %q", out)
	}
}

// TestFormatSkillListing_Tier2_DescriptionTruncated 验证档 2：超 budget → 描述按 maxDescLen 截断。
func TestFormatSkillListing_Tier2_DescriptionTruncated(t *testing.T) {
	longDesc := strings.Repeat("a", 200)
	skills := []skill.SkillDescriptor{
		{Name: "skill_a", Description: longDesc},
		{Name: "skill_b", Description: longDesc},
		{Name: "skill_c", Description: longDesc},
	}
	// budget 紧但 maxDescLen 仍 ≥ MIN（=20）
	// nameOverhead = (7+4)*3 + 2 = 35; budget=200 → availForDesc=165 → maxDescLen=55 ≥ 20
	out := formatSkillListing(skills, 200, 200)

	// 应每条都截断到 ~55 char
	for _, name := range []string{"skill_a", "skill_b", "skill_c"} {
		if !strings.Contains(out, name+": ") {
			t.Errorf("档 2 应保 name，缺 %q：%q", name, out)
		}
	}
	if !strings.Contains(out, "…") {
		t.Errorf("档 2 应有截断符 …，实际 %q", out)
	}
	// 总长不超 budget（精确 ≤ budget——audit H2 修复后 truncateWithEllipsis 已确保不超）
	if len(out) > 200 {
		t.Errorf("档 2 总长 %d > budget 200（每条截断的 ellipsis 应在预算内），实际 %q", len(out), out)
	}
}

// TestFormatSkillListing_Tier2_Chinese 验证档 2 截断中文 description 不破 UTF-8（audit H1 修复）。
func TestFormatSkillListing_Tier2_Chinese(t *testing.T) {
	longDesc := strings.Repeat("中文超长描述", 50) // 每字 3 byte UTF-8
	skills := []skill.SkillDescriptor{
		{Name: "skill_a", Description: longDesc},
		{Name: "skill_b", Description: longDesc},
		{Name: "skill_c", Description: longDesc},
	}
	out := formatSkillListing(skills, 200, 200)

	if !utf8.ValidString(out) {
		t.Fatalf("档 2 中文截断后 UTF-8 无效（audit H1 回归）：%q", out)
	}
	if !strings.Contains(out, "…") {
		t.Errorf("档 2 应有截断符 …，实际 %q", out)
	}
	if len(out) > 200 {
		t.Errorf("档 2 中文 byte 总长 %d > budget 200", len(out))
	}
}

// TestFormatSkillListing_PerEntry_Chinese 验证 perEntry 截断中文不破 UTF-8。
func TestFormatSkillListing_PerEntry_Chinese(t *testing.T) {
	longDesc := strings.Repeat("中", 100) // 300 byte
	skills := []skill.SkillDescriptor{
		{Name: "verbose", Description: longDesc},
	}
	// budget 大但 perEntry=30 byte 应截断中文 + UTF-8 安全
	out := formatSkillListing(skills, 10000, 30)

	if !utf8.ValidString(out) {
		t.Fatalf("perEntry 中文截断后 UTF-8 无效（audit H1 回归）：%q", out)
	}
	if !strings.Contains(out, "…") {
		t.Errorf("perEntry 超应截断带 …，实际 %q", out)
	}
}

// TestTruncateWithEllipsis_UTF8Safety 单元测 truncateWithEllipsis 边界。
func TestTruncateWithEllipsis_UTF8Safety(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		maxBytes int
		wantLen  int // 预期 ≤ maxBytes
	}{
		{"短字符串不截断", "hello", 100, 5},
		{"ASCII 截断", "abcdefghijklmnop", 8, 8},            // "abcde…" = 5+3=8 ✓
		{"中文截断到 10 byte", strings.Repeat("中", 50), 10, 9}, // "中中…" = 6+3=9
		{"中文截断到 9 byte", strings.Repeat("中", 50), 9, 9},   // "中中…" = 6+3=9
		{"中文截断到 8 byte", strings.Repeat("中", 50), 8, 6},   // "中…" = 3+3=6
		{"极小 maxBytes 返空", "any", 2, 0},
		{"刚好等于 maxBytes 不截断", "abc", 3, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := truncateWithEllipsis(c.input, c.maxBytes)
			if !utf8.ValidString(out) {
				t.Errorf("UTF-8 invalid: %q", out)
			}
			if len(out) > c.maxBytes {
				t.Errorf("超 maxBytes %d，实际 %d byte: %q", c.maxBytes, len(out), out)
			}
			if c.wantLen > 0 && len(out) != c.wantLen {
				t.Errorf("expected len %d, got %d: %q", c.wantLen, len(out), out)
			}
		})
	}
}

// TestFormatSkillListing_SingleSkill 验证单 skill corner case。
func TestFormatSkillListing_SingleSkill(t *testing.T) {
	out := formatSkillListing([]skill.SkillDescriptor{
		{Name: "only", Description: "the only skill"},
	}, 4000, 200)
	if out != "- only: the only skill" {
		t.Errorf("单 skill 应输出 '- only: the only skill'，实际 %q", out)
	}
}

// TestFormatSkillListing_BudgetEdgeCases 验证 budget 边界。
func TestFormatSkillListing_BudgetEdgeCases(t *testing.T) {
	skills := []skill.SkillDescriptor{
		{Name: "a", Description: "x"},
		{Name: "b", Description: "y"},
	}
	full := formatSkillListing(skills, 4000, 200)
	fullLen := len(full)

	// budget == fullTotal → 走档 1（<=）
	if out := formatSkillListing(skills, fullLen, 200); out != full {
		t.Errorf("budget == fullLen 应走档 1（==），实际 %q", out)
	}
	// budget == fullTotal-1 → 走档 2 截断
	out := formatSkillListing(skills, fullLen-1, 200)
	if out == full {
		t.Errorf("budget < fullLen 应触发档 2，实际未变 %q", out)
	}
}

// TestFormatSkillListing_Tier3_NamesOnly 验证档 3：maxDescLen < MIN → 全降级仅名字。
func TestFormatSkillListing_Tier3_NamesOnly(t *testing.T) {
	longDesc := strings.Repeat("x", 200)
	skills := make([]skill.SkillDescriptor, 20) // 多 skill 让 maxDescLen 必小于 20
	for i := range skills {
		skills[i] = skill.SkillDescriptor{
			Name:        "skill_aaaaaa_" + string(rune('a'+i)), // ~14 char
			Description: longDesc,
		}
	}
	// budget=300, 20 skill → nameOverhead = (14+4)*20 + 19 = 379 > 300 → 直接 names-only
	// 即使 nameOverhead < budget，maxDescLen = (300-379)/20 < 0 也是 names-only
	out := formatSkillListing(skills, 300, 200)

	// 应全是 "- name" 格式，无描述
	lines := strings.Split(out, "\n")
	if len(lines) != 20 {
		t.Errorf("应有 20 行，实际 %d 行：%q", len(lines), out)
	}
	for _, line := range lines {
		if strings.Contains(line, ":") {
			t.Errorf("档 3 应只列名字不带 description，实际行 %q", line)
		}
		if !strings.HasPrefix(line, "- skill_aaaaaa_") {
			t.Errorf("档 3 行应是 '- name' 格式，实际 %q", line)
		}
	}
}

// TestFormatSkillListing_PerEntryHardCap 验证单条 description 硬上限——单条超长被截断
// 即使总预算够。
func TestFormatSkillListing_PerEntryHardCap(t *testing.T) {
	longDesc := strings.Repeat("a", 500)
	skills := []skill.SkillDescriptor{
		{Name: "verbose", Description: longDesc},
	}
	// budget=10000 大于 description——但 perEntry 200 仍应截断
	out := formatSkillListing(skills, 10000, 200)
	if !strings.Contains(out, "…") {
		t.Errorf("单条超 perEntry 应截断，实际 %q", out)
	}
	// 截断后总长 ≤ 4 (- ) + 7 (verbose: ) + 200 + 4 (…) = ~215
	if len(out) > 250 {
		t.Errorf("单条 perEntry 截断后总长 ~215，实际 %d：%q", len(out), out)
	}
}

// TestFormatSkillListing_EmptyInput 验证空输入返空。
func TestFormatSkillListing_EmptyInput(t *testing.T) {
	if out := formatSkillListing(nil, 4000, 200); out != "" {
		t.Errorf("nil skills 应返空，实际 %q", out)
	}
	if out := formatSkillListing([]skill.SkillDescriptor{}, 4000, 200); out != "" {
		t.Errorf("空 slice 应返空，实际 %q", out)
	}
}

// TestFormatSkillListing_NoBudget 验证 budget=0 等于无上限（向后兼容）。
func TestFormatSkillListing_NoBudget(t *testing.T) {
	longDesc := strings.Repeat("a", 100)
	skills := []skill.SkillDescriptor{
		{Name: "skill_a", Description: longDesc},
		{Name: "skill_b", Description: longDesc},
	}
	out := formatSkillListing(skills, 0, 200)
	if !strings.Contains(out, "skill_a: "+longDesc) {
		t.Errorf("budget=0 应不裁剪，实际 %q", out)
	}
	if !strings.Contains(out, "skill_b: "+longDesc) {
		t.Errorf("budget=0 应不裁剪，实际 %q", out)
	}
}

// TestSkillTool_Description_RespectsBudget 验证 skillTool.Description() 真的走预算路径——
// 业务调 withListingBudget(100, 50) 后 Description 输出应符合档 2/3 行为。
func TestSkillTool_Description_RespectsBudget(t *testing.T) {
	// 用一个最小 fakeSkillBox 实现（包内不能用外部 fakeSkillBox）
	box := &mockSkillBox{
		skills: []skill.SkillDescriptor{
			{Name: "translate", Description: strings.Repeat("a", 200)},
			{Name: "code-review", Description: strings.Repeat("b", 200)},
		},
	}
	tool := newSkillTool(box).withListingBudget(100, 100)
	out := tool.Description()

	// 档 2 或 3 都行——关键是不应含全 200a 描述
	if strings.Contains(out, strings.Repeat("a", 200)) {
		t.Errorf("budget=100 应触发档 2/3 不应有全描述，实际 %q", out)
	}
}

// TestSkillTool_Description_NoBudget_FullDescription 验证默认（budget=SkillListingBudgetChars=4000）
// 时短 description 全发。
func TestSkillTool_Description_NoBudget_FullDescription(t *testing.T) {
	box := &mockSkillBox{
		skills: []skill.SkillDescriptor{
			{Name: "translate", Description: "中英文互译"},
		},
	}
	tool := newSkillTool(box)
	out := tool.Description()
	if !strings.Contains(out, "translate: 中英文互译") {
		t.Errorf("默认 budget 短 description 应全发，实际 %q", out)
	}
}

// mockSkillBox 是 internal_test 包内的最小 SkillBox 实现——白盒测不能依赖外部包的
// fakeSkillBox（在 _test 包）。
//
// **activateCalls 计数 + activateNames 记录**：用于验证"DisableModelInvocation 拒绝
// 路径"——assert ActivateSkill **没被调用**（filter 在 Call() 早期 fail，避免业务
// box 副作用）。无此 spy 的 mock 会让 sanity-revert 删 Call() 早期 filter 后测试
// 仍 PASS（活跃版 ActivateSkill 返 not impl error，转换成 NotFound result 看起来
// 也"成功拦截"——但语义已错——副作用已发生）。
type mockSkillBox struct {
	skills        []skill.SkillDescriptor
	activateCalls int
	activateNames []string
	// activateImpl 业务路径下 mock 返合法 ActiveSkill（disabled-skill 测试不会用到）
	activateImpl func(name string) (*skill.ActiveSkill, error)
}

func (b *mockSkillBox) Name() string                                                     { return "mock" }
func (b *mockSkillBox) RootDir() string                                                  { return "" }
func (b *mockSkillBox) ListSkills() []skill.SkillDescriptor                              { return b.skills }
func (b *mockSkillBox) MatchSkills(_ context.Context, _ string) []skill.SkillMatchResult { return nil }
func (b *mockSkillBox) ListSkillResources(_ string) []skill.SkillResource                { return nil }
func (b *mockSkillBox) ReadSkillResource(_, _ string) string                             { return "" }
func (b *mockSkillBox) ReadSkillPrompt(_ string) string                                  { return "" }
func (b *mockSkillBox) ListSkillActions(_ string) []skill.SkillActionDescriptor          { return nil }
func (b *mockSkillBox) ActivateSkill(_ context.Context, name string) (*skill.ActiveSkill, error) {
	b.activateCalls++
	b.activateNames = append(b.activateNames, name)
	if b.activateImpl != nil {
		return b.activateImpl(name)
	}
	return nil, errors.New("not impl")
}
func (b *mockSkillBox) RunSkillAction(_ context.Context, _, _ string, _ map[string]any) (*skill.SkillExecutionResult, error) {
	return nil, skill.ErrActionNotImplemented
}

var _ skill.SkillBox = (*mockSkillBox)(nil)

// TestSkillTool_Description_FiltersDisabledModelInvocation 验证 Description 输出
// 不含 DisableModelInvocation=true 的 skill 名。
//
// **bomb 防御（5 类炸弹·第 1 类——过滤后元素恰好原位）**：
// 旧 fixture 若 disabled 在 idx 0、enabled 在 idx 1，filter 走"all in-place keep
// idx 1"路径恰好等于"删除 idx 0"——但若 filter 完全未跑，输出 "skill_a: ..." +
// "skill_b: ..." 全有也假绿。这里特意 disabled 元素的 NAME 唯一（"hidden_for_model"）
// 让 Contains 检查能精确捕到："hidden_for_model" 字符串本身在输出里出现 = filter
// 没跑（HIGH 真红）。
//
// sanity-revert 删 filterModelInvocableSkills 后 → "hidden_for_model" 出现 → fail。
func TestSkillTool_Description_FiltersDisabledModelInvocation(t *testing.T) {
	box := &mockSkillBox{
		skills: []skill.SkillDescriptor{
			{Name: "hidden_for_model", Description: "this should NOT appear in listing", DisableModelInvocation: true},
			{Name: "visible_to_model", Description: "this SHOULD appear in listing"},
		},
	}
	tool := newSkillTool(box)
	out := tool.Description()

	if strings.Contains(out, "hidden_for_model") {
		t.Errorf("DisableModelInvocation=true skill 应被过滤 listing，实际输出含名字：%q", out)
	}
	if !strings.Contains(out, "visible_to_model") {
		t.Errorf("非 disabled skill 应在 listing 中，实际：%q", out)
	}
}

// TestSkillTool_Description_AllDisabled 验证全部 skill 都 disabled 时返"无可用技能"。
//
// **bomb 防御（5 类炸弹·第 2 类——早退兜底盖过 fix 路径）**：
// 旧 ListSkills 若全 disabled，未过滤前 len()>0 不走"无可用"路径——只有 filter
// 跑完看 visible 为空才能进早退。这里用 ALL DISABLED 让 filter 必须真跑才能让
// "无可用技能" 字符串出现。sanity-revert 删 filter → 进 formatSkillListing 输出
// 含 hidden_for_model + hidden_for_model_2 名字 → 不含"无可用技能" → fail。
func TestSkillTool_Description_AllDisabled(t *testing.T) {
	box := &mockSkillBox{
		skills: []skill.SkillDescriptor{
			{Name: "hidden_a", Description: "a", DisableModelInvocation: true},
			{Name: "hidden_b", Description: "b", DisableModelInvocation: true},
		},
	}
	tool := newSkillTool(box)
	out := tool.Description()

	if !strings.Contains(out, "无可用技能") {
		t.Errorf("全 disabled 应返'无可用技能'文案，实际 %q", out)
	}
	// 防御：业务名不应泄漏到 LLM
	if strings.Contains(out, "hidden_a") || strings.Contains(out, "hidden_b") {
		t.Errorf("disabled skill 名不应在 listing，实际 %q", out)
	}
}

// TestSkillTool_Call_RejectsDisabledSkill 验证 LLM 硬调 disabled skill 名时被拒。
//
// **bomb 防御（5 类炸弹·第 5 类——mock 必须读 input）**：
// activateCalls 计数器是关键 spy——sanity-revert 删 Call() 内 ListSkills 检查后，
// box.ActivateSkill 会被调（即便 mock 返 not impl，IsFrameworkError 会 ErrCodeNotFound
// 也"看起来失败"），但 activateCalls 必 ≥ 1，断言 == 0 必 fail。
//
// 不依赖业务自定义 ActivateSkill 副作用做断言（否则成"假功能"测试）。
func TestSkillTool_Call_RejectsDisabledSkill(t *testing.T) {
	box := &mockSkillBox{
		skills: []skill.SkillDescriptor{
			{Name: "disabled_skill", Description: "x", DisableModelInvocation: true},
		},
	}
	tool := newSkillTool(box)

	res, err := tool.Call(context.Background(), map[string]any{"skill": "disabled_skill"})
	if err != nil {
		t.Fatalf("Call 不应返 err（业务级失败用 result.Error），实际 %v", err)
	}
	if res == nil {
		t.Fatalf("Call 应返非 nil result")
	}
	if res.Success {
		t.Errorf("disabled skill 应返失败 result，实际 success=true")
	}
	if res.Error == nil || res.Error.Code != SkillDisabledErrCode {
		t.Errorf("应返 SkillDisabledErrCode (%q)，实际 %+v", SkillDisabledErrCode, res.Error)
	}
	// 关键 spy 断言：filter 在 Call() 早期生效，ActivateSkill 不应被调
	if box.activateCalls != 0 {
		t.Errorf("filter 应在 ActivateSkill 之前 fail-fast，实际 box.ActivateSkill 被调 %d 次：%v",
			box.activateCalls, box.activateNames)
	}
}

// TestSkillTool_Call_AllowsEnabledSkill 验证非 disabled skill 正常激活。
func TestSkillTool_Call_AllowsEnabledSkill(t *testing.T) {
	box := &mockSkillBox{
		skills: []skill.SkillDescriptor{
			{Name: "enabled", Description: "x"},
		},
		activateImpl: func(name string) (*skill.ActiveSkill, error) {
			return &skill.ActiveSkill{
				Skill:  skill.SkillDescriptor{Name: name},
				Prompt: "skill body",
			}, nil
		},
	}
	tool := newSkillTool(box)

	res, err := tool.Call(context.Background(), map[string]any{"skill": "enabled"})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if res == nil || !res.Success {
		t.Fatalf("非 disabled skill 应成功激活，实际 %+v", res)
	}
	if box.activateCalls != 1 {
		t.Errorf("enabled skill 应触发 ActivateSkill 1 次，实际 %d", box.activateCalls)
	}
}

// TestSkillDisabledErrCode_NotFrameworkError 锁住分类契约——R12 audit LOW-1：
// SkillDisabledErrCode 必须**不在** tool.IsFrameworkError 列表中。
//
// **R17 audit MED-1 澄清**：当前 agent_v2/compose_v2 框架代码无 abort 路径，所有
// tool 错误（含框架级 code）都按业务级反馈 LLM。本测试只锁"分类"契约——业务侧
// 用 IsFrameworkError 做日志分级 / 自定义路由时分类要正确。functional 上 inside
// vs outside 列表行为目前等价（都反馈 LLM）；将来若框架加 abort 路径本测试仍守住设计意图。
//
// 防回归：未来若有人手痒把 SkillDisabledErrCode 加进 tool.IsFrameworkError 列表，
// 此测试 fail 提示破坏分类约定。
func TestSkillDisabledErrCode_NotFrameworkError(t *testing.T) {
	r := &tool.Result{Success: false, Error: &tool.Error{Code: SkillDisabledErrCode, Message: "x"}}
	if tool.IsFrameworkError(r) {
		t.Errorf("SkillDisabledErrCode (%q) 不应被分类为 framework error——业务侧分类约定。"+
			"检查 tool.IsFrameworkError 列表是否被误扩。", SkillDisabledErrCode)
	}
}

// TestFilterModelInvocableSkills_Filter 单测 helper 自身——驱动 4 case 表覆盖
// "全保留 / 全过滤 / 部分过滤 / 空输入"边界。
func TestFilterModelInvocableSkills_Filter(t *testing.T) {
	cases := []struct {
		name      string
		input     []skill.SkillDescriptor
		wantLen   int
		wantNames []string
	}{
		{"empty", nil, 0, nil},
		{"all enabled", []skill.SkillDescriptor{
			{Name: "a"}, {Name: "b"},
		}, 2, []string{"a", "b"}},
		{"all disabled", []skill.SkillDescriptor{
			{Name: "a", DisableModelInvocation: true},
			{Name: "b", DisableModelInvocation: true},
		}, 0, nil},
		{"mixed: drop disabled", []skill.SkillDescriptor{
			{Name: "drop_me", DisableModelInvocation: true},
			{Name: "keep_me"},
			{Name: "drop_me2", DisableModelInvocation: true},
			{Name: "keep_me2"},
		}, 2, []string{"keep_me", "keep_me2"}},
		// **R19 audit MED-2 fix**：业务自定义 SkillBox 返 Name="" 脏数据时也应过滤
		// （与 selectSkillAdaptive R17-LOW-3 同等防御对称）
		{"r19-med-2: drop empty name", []skill.SkillDescriptor{
			{Name: "good", Description: "good skill"},
			{Name: "", Description: "dirty empty name should be filtered"},
			{Name: "another", Description: "another good"},
		}, 2, []string{"good", "another"}},
		// 边界：Name="" + DisableModelInvocation=true 同时——任一条件触发过滤
		{"r19-med-2: empty name + disabled", []skill.SkillDescriptor{
			{Name: "", Description: "empty"},
			{Name: "disabled", DisableModelInvocation: true},
			{Name: "keep"},
		}, 1, []string{"keep"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := filterModelInvocableSkills(c.input)
			if len(out) != c.wantLen {
				t.Fatalf("want len %d, got %d (%v)", c.wantLen, len(out), out)
			}
			for i, n := range c.wantNames {
				if out[i].Name != n {
					t.Errorf("idx %d: want name %q, got %q", i, n, out[i].Name)
				}
			}
		})
	}
}

// ── activate_skill Description 内部 helper 单测 ──

// TestSkillTool_Description_ListsAllSkills 验证 Description 列出全部 model-invocable skill。
func TestSkillTool_Description_ListsAllSkills(t *testing.T) {
	box := &mockSkillBox{
		skills: []skill.SkillDescriptor{
			{Name: "skill_a", Description: "desc_a"},
		},
	}
	tool := newSkillTool(box)
	out := tool.Description()
	if !strings.Contains(out, "skill_a") {
		t.Errorf("Description 应列 skill 名，实际 %q", out)
	}
}
