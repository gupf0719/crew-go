package skill_test

import (
	"strings"
	"testing"

	"crew-go/skill"
)

// TestParseSkillMD_UnknownContextRejectedAtParse 钉住**写错的 `context:` 在解析期就被拒**，
// 不是静默退化成 inline、也不是拖到激活时才说。
//
// ⚠️ 收件人是承重的那个维度：激活期拒绝（`activate_skill` 返 tool.Failure）说给 **LLM**
// 听，而写错 `context:` 的是**业务开发者**——他看不到那条 tool_result。所以断言选在
// 「`ParseSkillMD` 返不返 error」上，而不是「激活时会不会失败」。
//
// 历史：`fork_with_context` 一度是合法枚举值，被接受、走 fork 同一分支、什么也不复制、
// 无信号。2026-08-26 删除——`context` 这个键**不在 agentskills.io 标准里**，写它的两家
// 里一家只认 `fork`、另一家是这第三档的发明者，兼容对象不存在。
func TestParseSkillMD_UnknownContextRejectedAtParse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ctxVal  string
		wantErr bool
	}{
		{"空值等于 inline", "", false},
		{"inline", "inline", false},
		{"fork", "fork", false},
		{"已删除的第三档", "fork_with_context", true},
		{"纯拼错", "forK", true},
		{"别的生态的值", "subagent", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			md := "---\nname: demo\ndescription: d\n"
			if tc.ctxVal != "" {
				md += "context: " + tc.ctxVal + "\n"
			}
			md += "---\nbody\n"
			desc, _, err := skill.ParseSkillMD([]byte(md))
			if tc.wantErr {
				if err == nil {
					t.Errorf("context: %q 应在解析期被拒，实际 err=nil、desc.Context=%q——"+
						"写错的值静默活下来了，业务拿到的行为不是他写的那个",
						tc.ctxVal, desc.Context)
					return
				}
				if !strings.Contains(err.Error(), tc.ctxVal) {
					t.Errorf("错误正文里没有惹事的那个值 %q：%v——定位信息给不全，"+
						"业务不知道该改哪一行", tc.ctxVal, err)
				}
				return
			}
			if err != nil {
				t.Errorf("context: %q 是合法值，不该报错：%v", tc.ctxVal, err)
			}
		})
	}
}

// TestParseSkillMD_ListForms 钉住**两种 YAML 列表写法都收得到**，以及 `allowed-tools`
// 的处置档位（出声但放行）。
//
// ⚠️ flow 形式 `[a, b]` 一度**静默丢成空**——它掉进标量分支塞进 scalar，而 list 类字段
// （tags / examples / allowed-tools）只读 list ⟹ 业务写了一串工具名、desc 里是空切片、
// **两边都不报错**。而外部生态的示例恰恰用 flow 写。
//
// `allowed-tools` 本身是**出声但放行**，跟写错的 `context` 值（当场拒）分两档——判据是
// 「谁的错」：它是标准里的合法字段（还标着 Experimental），报错会让生态里正常的 SKILL.md
// 加载不了；而 `context: 拼错了` 是纯粹的作者笔误。
func TestParseSkillMD_ListForms(t *testing.T) {
	for _, c := range []struct {
		name string
		md   string
		want int
	}{
		{"flow 形式", "allowed_tools: [read_file, write_file]\n", 2},
		{"block 形式", "allowed_tools:\n  - read_file\n  - write_file\n", 2},
		{"空 flow 列表", "allowed_tools: []\n", 0},
		{"逗号裸串（不是 YAML 列表，不该猜）", "allowed_tools: read_file, write_file\n", 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			desc, _, err := skill.ParseSkillMD([]byte("---\nname: d\ndescription: x\n" + c.md + "---\nbody\n"))
			if err != nil {
				t.Fatalf("allowed-tools 是标准合法字段，不该拒：%v", err)
			}
			if len(desc.AllowedTools) != c.want {
				t.Errorf("期望 %d 项，实际 %v——静默丢成空时业务两边都不报错", c.want, desc.AllowedTools)
			}
		})
	}
}

// TestParseSkillMD_FlowListIsCaseInsensitive 钉住**键匹配对大小写与分隔符一视同仁**。
//
// ⚠️ 这条是审计抓到的回归留下的哨兵：当年判「这个键要不要收列表」的那份清单收的是
// **原始** key，而映射 switch 用的是小写化之后的 ⟹ `Tags:` / `allowedTools:` 在
// `[a, b]` 这种写法下静默丢成空，而同一个键写成 `- item` 仍然工作。
//
// 驼峰 `allowedTools` **正是 SkillDescriptor 自己的 JSON tag 写法**，业务照着结构体写
// frontmatter 就会踩；而「同一份 SKILL.md 换个列表写法结果不同、两边都不报错」正是
// 那一轮要消灭的形态。
//
// **根因已消**：那份清单和它要同步的另一处都随手写解析器一起删了，现在键归一化是
// 一条通用规则（小写 + 去 `-` `_`，见 `normalizeKey`），两种列表写法在 YAML 里本来
// 就是同一种节点。这条哨兵留着守归一化本身。
func TestParseSkillMD_FlowListIsCaseInsensitive(t *testing.T) {
	for _, c := range []struct {
		line     string
		wantTags []string
		wantAT   []string
	}{
		{"tags: [a, b]", []string{"a", "b"}, nil},
		{"Tags: [a, b]", []string{"a", "b"}, nil},
		{"TAGS: [a, b]", []string{"a", "b"}, nil},
		{"allowed_tools: [Read]", nil, []string{"Read"}},
		{"allowedTools: [Read]", nil, []string{"Read"}},
		{"Allowed-Tools: [Read]", nil, []string{"Read"}},
	} {
		d, _, err := skill.ParseSkillMD([]byte("---\nname: d\ndescription: x\n" + c.line + "\n---\nbody\n"))
		if err != nil {
			t.Errorf("%q: 不该报错：%v", c.line, err)
			continue
		}
		if len(c.wantTags) > 0 && len(d.Tags) != len(c.wantTags) {
			t.Errorf("%q: Tags=%v want %v——大小写不同就丢空了", c.line, d.Tags, c.wantTags)
		}
		if len(c.wantAT) > 0 && len(d.AllowedTools) != len(c.wantAT) {
			t.Errorf("%q: AllowedTools=%v want %v——大小写不同就丢空了", c.line, d.AllowedTools, c.wantAT)
		}
	}
}
