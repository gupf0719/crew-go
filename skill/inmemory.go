package skill

// SkillContent 是一份预解析的 skill 内容，供 NewInMemorySkillBox 从非文件系统来源
// （远端 skill 沙箱 / marketplace API / DB）装配 SkillBox。
//
// Descriptor 建议经 ParseSkillMD 解析 SKILL.md 原文得到（已含净化）；直接构造的 Descriptor
// 会在 NewInMemorySkillBox 内再净化一次（幂等，防绕过 SKILL.md 解析的入库路径漏净化）。
type SkillContent struct {
	// Descriptor skill 元数据。Name 为空的条目被跳过。
	Descriptor SkillDescriptor
	// Body SKILL.md 正文——激活时作 ActiveSkill.Prompt 注入 LLM。
	Body string
	// Dir skill 在来源侧的目录（如远端沙箱里的 skill 目录）——作 ActiveSkill.RootDir。
	// 本地文件系统不可读（无本地资源）；仅信息性 / 供来源侧路径推导。
	Dir string
}

// NewInMemorySkillBox 从内存中预解析的 skill 内容装配 SkillBox（不扫本地文件系统）——供远程
// skill 源（远程沙箱 / marketplace API）复用 DefaultSkillBox 的关键词匹配 / 激活 / fork /
// listing 机制，而非各自重实现 SkillBox 契约。
//
// 与 NewDefaultSkillBox 区别：
//   - 内容由 caller 提供而非磁盘扫描 rootDir/*/SKILL.md
//   - 无本地资源文件——ListSkillResources / ReadSkillResource 返空（远端资源经沙箱工具如
//     sandbox_bash 访问，不走本地路径）
//   - Descriptor 统一净化（防不可信来源注入）；空 Name / 未过 allow-list 的条目静默跳过
//
// 天然线程安全（装配后只读，同 DefaultSkillBox scan-once 模型）。WithSkillActionFunc 等
// SkillBoxOption 仍适用（go-func action 可注册到内存 skill）。
func NewInMemorySkillBox(name string, contents []SkillContent, opts ...SkillBoxOption) (*DefaultSkillBox, error) {
	b := &DefaultSkillBox{name: name}
	for _, opt := range opts {
		opt(b)
	}
	if b.name == "" {
		b.name = "default"
	}
	b.skills = make([]loadedSkill, 0, len(contents))
	for _, c := range contents {
		desc := sanitizeDescriptor(c.Descriptor)
		if desc.Name == "" {
			continue
		}
		if b.allowList != nil && !b.allowList[desc.Name] {
			continue
		}
		b.skills = append(b.skills, loadedSkill{
			descriptor: desc,
			dir:        c.Dir,
			prompt:     c.Body,
			matchText:  buildMatchText(desc),
			// resources nil——远端无本地文件；actions 由 mergePendingActions 从 WithSkillActionFunc 填
		})
	}
	b.mergePendingActions()
	return b, nil
}
