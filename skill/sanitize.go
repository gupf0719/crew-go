package skill

import (
	"strings"
	"unicode"
)

// skill 文本元数据清洗长度上限（按 rune 计）——防不可信来源（第三方 / 用户上传 skill
// 包、skill 市场）用超长元数据撑爆 prompt。listing 下游另有 per-entry 预算裁剪，这里只
// 兜底病态超长。
const (
	maxMetadataNameLen = 128
	maxMetadataTextLen = 1024
)

// sanitizeMetadata 清洗 skill 文本元数据，防不可信来源经 prompt 注入。
//
// skill 的 name / description 等元数据会被拼进 LLM 可见的 skill listing（"- name: desc"）——
// **激活前**仅靠安装 / 上架就进对话流。元数据里的换行 / 控制符能破坏 listing 的 markdown
// 结构：伪造独立 list item、伪造 "system:" 行，把指令注进 prompt。crew-go 的 skill
// listing 用 markdown 而非 XML，对应防护是折叠换行 / 控制符。
//
// 策略：所有空白（含换行 / tab / 回车）与控制符**折叠为单空格** + 去首尾 + 截断超长。结构
// 字符没了 → 元数据无法越出自己那一格。内部正常空格保留（"data science" 不变）。
//
// **边界**：只清洗"激活前展示给 LLM 的元数据"。skill 正文 body（激活时载入
// ActiveSkill.Prompt）是**激活后**的指令内容、含合法换行，不走这里——激活是 LLM 显式
// 决策、body 是 skill 用途本身（清洗它会破坏合法 skill）。防恶意 body 属 skill 能力沙箱 /
// 上架审核范畴，非本函数职责。
func sanitizeMetadata(s string, maxLen int) string {
	var b strings.Builder
	b.Grow(len(s))
	prevSpace := false
	for _, r := range s {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			// 连续空白 / 控制符折叠为单个空格
			if !prevSpace {
				b.WriteByte(' ')
				prevSpace = true
			}
			continue
		}
		b.WriteRune(r)
		prevSpace = false
	}
	out := strings.TrimSpace(b.String())
	if maxLen > 0 {
		if rs := []rune(out); len(rs) > maxLen {
			out = strings.TrimSpace(string(rs[:maxLen])) + "…"
		}
	}
	return out
}

// sanitizeMetadataSlice 对字符串切片逐项 sanitizeMetadata，丢弃清洗后为空的项。
func sanitizeMetadataSlice(ss []string, maxLen int) []string {
	if len(ss) == 0 {
		return ss
	}
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		if c := sanitizeMetadata(s, maxLen); c != "" {
			out = append(out, c)
		}
	}
	return out
}

// sanitizeDescriptor 清洗 SkillDescriptor 所有"激活前展示给 LLM / 回灌错误消息"的文本元数据
// （Name / Description / WhenToUse / Context / Agent / Model / Tags / Examples / AllowedTools），
// 防不可信来源（第三方 / marketplace skill 包）经描述文本 prompt 注入。
//
// **所有 SkillDescriptor 入库路径都应经此**：SKILL.md 解析（parseFrontmatter）、内存装配
// （NewInMemorySkillBox）、未来 marketplace / DB API。body 不在 desc 内（激活后才载入
// ActiveSkill.Prompt），不清洗（见 sanitizeMetadata godoc 边界）。幂等——重复调用安全。
func sanitizeDescriptor(desc SkillDescriptor) SkillDescriptor {
	desc.Name = sanitizeMetadata(desc.Name, maxMetadataNameLen)
	desc.Description = sanitizeMetadata(desc.Description, maxMetadataTextLen)
	desc.WhenToUse = sanitizeMetadata(desc.WhenToUse, maxMetadataTextLen)
	desc.Context = sanitizeMetadata(desc.Context, maxMetadataNameLen)
	desc.Agent = sanitizeMetadata(desc.Agent, maxMetadataNameLen)
	desc.Model = sanitizeMetadata(desc.Model, maxMetadataNameLen)
	desc.Tags = sanitizeMetadataSlice(desc.Tags, maxMetadataNameLen)
	desc.Examples = sanitizeMetadataSlice(desc.Examples, maxMetadataTextLen)
	desc.AllowedTools = sanitizeMetadataSlice(desc.AllowedTools, maxMetadataNameLen)
	return desc
}
