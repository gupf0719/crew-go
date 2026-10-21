package skill

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"log/slog"
	"strings"
)

// ParseSkillMD 解析 SKILL.md 内容，返回 (SkillDescriptor, body, error)。
// SKILL.md 格式：以 --- 包裹的 YAML frontmatter + 正文。
// 支持的 frontmatter 类型：string、bool、int、string list（`- item` 块状与 `[a, b]` flow 两种）。
// descriptor 元数据已净化（防不可信来源注入）；正文 body 原样返回。
//
// **导出**供远程 skill 源（远端沙箱 / marketplace API 拉到 SKILL.md 原文）复用同一
// 解析 + 净化逻辑，再经 NewInMemorySkillBox 装配 SkillBox。
func ParseSkillMD(data []byte) (SkillDescriptor, string, error) {
	content := string(data)

	// 提取 frontmatter
	fm, body := splitFrontmatter(content)
	desc, err := parseFrontmatter(fm)
	if err != nil {
		return SkillDescriptor{}, "", err
	}
	// **`context` 的值在这里校验，不在激活时。**
	//
	// 收件人不一样：激活期拒绝（`activate_skill` 返 tool.Failure）说给 **LLM** 听，
	// 而写错 `context:` 的是**业务开发者**，他看不到那条 tool_result。解析期返 error
	// 才送得到人手里。（这条是 SOSP'13「早检查」的直接应用：在**加载配置时**校验，
	// 不在**用到它时**。）
	//
	// ⚠️ 只拒**明确写错的值**，不碰未知键——两者的处置判据不同，见本函数返回值的 godoc。
	// `allowed-tools` 解析了但**不强制**——出声，别让业务以为写了就生效。
	// 为什么不强制、以及有作用域的替代品，见 [SkillDescriptor] 的 AllowedTools godoc。
	//
	// ⚠️ **用 warn 不用 error，判据是「标准自己认不认这个字段可以不被支持」**：
	//
	// 通行的判据是「这个键是不是**行为开关**」——不兑现就意味着业务要的行为百分之百没
	// 发生，那种该报错（`context` 写错值就是这么处理的）。按这条 `allowed-tools` 也是
	// 行为开关，似乎该报错。
	//
	// 但它有一处豁免：**agentskills.io 标准给这个字段标了「Experimental. Support for this
	// field may vary between agent implementations」**——标准自己声明了各家支持程度不同。
	// 对一个标准明说「可以不支持」的字段拒绝加载整份 SKILL.md，是把生态里合法的文件挡在
	// 门外。`paths` / `context` 没有这条豁免（前者标准里根本没有、后者是厂商扩展）。
	//
	// ⟹ 一句话：**报不报错看标准给没给「支持程度可变」的豁免，不看它是不是行为开关。**
	if len(desc.AllowedTools) > 0 {
		slog.Warn("skill: allowed-tools 已解析但本框架不强制——想按轮裁工具用 WithToolFilter",
			"skill", desc.Name, "allowed_tools", desc.AllowedTools)
	}

	if c := desc.Context; c != "" && c != "inline" && c != "fork" {
		return SkillDescriptor{}, "", fmt.Errorf(
			"skill %q: 不认识的 context 值 %q（只支持 inline / fork）", desc.Name, c)
	}

	return desc, strings.TrimSpace(body), nil
}

// splitFrontmatter 将内容拆分为 frontmatter（--- 之间）和 body。
// 若没有有效 frontmatter，全部视为 body。
func splitFrontmatter(content string) (fm, body string) {
	// **先剥 BOM。** `strings.TrimSpace` / `TrimLeft(" \t")` 都不吃 U+FEFF——它是 Cf 类
	// 不是 White_Space。Windows 编辑器保存出 BOM 是真实存在的形态，而漏掉它的后果是整份
	// frontmatter 不被识别，随后 skillbox 报的却是「缺 name，跳过」——**误导到另一个方向**。
	//
	// ⚠️ 换用真 YAML 解析器**不解决这条**：frontmatter 的切分在解析之前，是我们自己做的。
	content = strings.TrimPrefix(content, "\ufeff")
	if !strings.HasPrefix(strings.TrimLeft(content, " \t\r\n"), "---") {
		return "", content
	}
	// 找第一个 ---
	lines := strings.Split(content, "\n")
	start := -1
	for i, line := range lines {
		if strings.TrimSpace(line) == "---" {
			if start == -1 {
				start = i
			} else {
				// 找到第二个 ---，frontmatter 在 start+1 到 i-1 之间
				fm = strings.Join(lines[start+1:i], "\n")
				body = strings.Join(lines[i+1:], "\n")
				return fm, body
			}
		}
	}
	// 只有一个 ---，没有正确的 frontmatter
	return "", content
}

// parseFrontmatter 用 [gopkg.in/yaml.v3] 解析 frontmatter 为 SkillDescriptor。
//
// # 为什么不再手写
//
// 这里曾经是一个手写的 YAML 子集解析器，一轮审计在它身上抓出 **7 条同源缺陷，全部静默**：
// 嵌套 `metadata:` 的子键被拍平进顶层（标准恰恰规定厂商扩展放那儿 ⟹ 照标准写就踩，
// 实测能顶掉 skill 真实身份、也能让合规文件整份被拒）/ 多行标量 `|` `>` 变成字面量、
// 正文全丢 / 行尾注释吞进值或让列表整条丢空 / 引号里的逗号被切碎 / 别名表与映射 switch
// 是两份必须同步的拷贝（因此栽过一次大小写没对齐）/ 非列表键收到列表的闸只挂在两条产列表
// 分支的一条上。
//
// **最后那条尤其说明问题**：那两条分支（`- item` 块式 / `[a, b]` 流式）是**手写造出来的**
// ——YAML 里两者是同一种 sequence 节点。换成真解析器，「两条路」根本不存在，
// 闸只需要判一次「这个键要的是标量还是列表」。
//
// # 别名
//
// 归一化是**通用规则**（小写 + 去掉 `-` `_`），不是逐键列出的别名表——所以
// `whenToUse` / `when_to_use` / `when-to-use` / `WHEN-TO-USE` 全部落到同一个 case，
// 而新增字段不需要同步任何第二份清单。
//
// # 关键默认值
//
//   - UserInvocable 默认 true（未声明时即开启用户端可见性）
//   - DisableModelInvocation 默认 false（zero value 即可）
func parseFrontmatter(fm string) (SkillDescriptor, error) {
	desc := SkillDescriptor{UserInvocable: true}
	if strings.TrimSpace(fm) == "" {
		return desc, nil
	}

	var raw map[string]any
	if err := yaml.Unmarshal([]byte(fm), &raw); err != nil {
		return SkillDescriptor{}, fmt.Errorf("frontmatter 不是合法 YAML: %w", err)
	}

	// 收集所有键的错误再一起报，别只报撞上的第一个——作者一次改完比改三轮好。
	var errs []string
	bad := func(k string, v any, want string) {
		errs = append(errs, fmt.Sprintf("%s 要%s，收到 %s", k, want, yamlKind(v)))
	}
	// ⚠️ 下面四个 helper 只管**结构**（这个位置该是标量还是列表），**值怎么解释一字不改**
	// ——bool 的 y/on/yes 别名、无法识别时保默认、列表键收到裸标量时留空不报错，都是既有
	// 契约（各有哨兵）。换解析器是为了修结构层的 7 条缺陷，不是顺手改值策略。
	str := func(k string, v any, dst *string) {
		switch tv := v.(type) {
		case string:
			*dst = tv
		case bool, int, float64:
			*dst = fmt.Sprintf("%v", tv) // 手写解析器取的是原始文本，标量一律当字符串
		default:
			// **标量键收到列表/映射 ⟹ 报错。** 这是审计那条「闸只挂在两条产列表分支之一」
			// 的根治：换成真解析器之后 flow `[a]` 和 block `- a` 是同一种节点，
			// 这里判一次就覆盖了两种写法（原先 block 那条整个绕过去、静默走默认值）。
			bad(k, v, "一个标量值")
		}
	}
	boolean := func(k string, v any, dst *bool) {
		switch tv := v.(type) {
		case bool:
			*dst = tv
		case string:
			// YAML 1.2 只把 true/false 当布尔，y / on / yes 一律是字符串——既有契约要它们
			// 也生效，所以这一档自己认。无法识别的值**保默认不报错**（既有契约）。
			if b, ok := parseBoolOptional(tv); ok {
				*dst = b
			}
		default:
			bad(k, v, "一个布尔值")
		}
	}
	strList := func(k string, v any, dst *[]string) {
		items, ok := v.([]any)
		if !ok {
			// **列表键收到裸标量 ⟹ 留空、不报错**（既有契约：`allowed_tools: a, b` 这种
			// 逗号裸串不是 YAML 列表，「不该猜」）。跟上面标量键收到列表**不对称**是有意的：
			// 那边写错的是 `context` / `agent` / `model` 这类行为开关，不兑现 = 行为百分之百
			// 没发生；这边多是描述性字段，留空的代价小得多。
			return
		}
		out := make([]string, 0, len(items))
		for idx, it := range items {
			sv, ok := it.(string)
			if !ok {
				bad(fmt.Sprintf("%s[%d]", k, idx), it, "一个字符串")
				continue
			}
			out = append(out, sv)
		}
		*dst = out
	}

	for k, v := range raw {
		if v == nil { // `key:` 后面什么都没写——当没声明，不当空值
			continue
		}
		switch normalizeKey(k) {
		case "name":
			str(k, v, &desc.Name)
		case "description":
			str(k, v, &desc.Description)
		case "whentouse":
			str(k, v, &desc.WhenToUse)
		case "context":
			str(k, v, &desc.Context)
		case "agent":
			str(k, v, &desc.Agent)
		case "model":
			str(k, v, &desc.Model)
		case "tags":
			strList(k, v, &desc.Tags)
		case "examples":
			strList(k, v, &desc.Examples)
		case "allowedtools":
			strList(k, v, &desc.AllowedTools)
		case "disablemodelinvocation":
			boolean(k, v, &desc.DisableModelInvocation)
		case "userinvocable":
			boolean(k, v, &desc.UserInvocable)
		}
		// ⚠️ **未知键不报错也不 warn**，这是有意的：schema 归生态所有（agentskills.io 标准
		// 在演进、别家还有自己的厂商扩展），对一个我们不认识的键拒绝加载整份 SKILL.md，
		// 是把生态里合法的文件挡在门外。厂商扩展按标准放 `metadata:` 底下，那是一个嵌套
		// 映射——这里 case 里没有它，于是整块被忽略，**子键不会再泄漏到顶层**。
	}
	if len(errs) > 0 {
		return SkillDescriptor{}, fmt.Errorf("frontmatter 字段类型不对：%s", strings.Join(errs, "；"))
	}

	// 清洗所有"激活前展示给 LLM / 回灌错误消息"的文本元数据——SKILL.md 解析的 trust
	// boundary（一处净化，listing / 选择 hint / 错误消息所有下游注入点自动安全）。所有
	// SkillDescriptor 入库路径统一走 sanitizeDescriptor（含 NewInMemorySkillBox / marketplace）。
	//
	// ⚠️ 换解析器那次**把这一句漏掉了**，靠 TestParseFrontmatter_SanitizesControlChars
	// 当场抓住——它是安全边界，不是格式化。
	return sanitizeDescriptor(desc), nil
}

// yamlKind 把解析出来的值说成人话，用在类型不匹配的报错里——只说「类型不对」而不说
// 收到的是什么，作者得自己回去数缩进。
func yamlKind(v any) string {
	switch tv := v.(type) {
	case nil:
		return "空值"
	case string:
		return fmt.Sprintf("字符串 %q", tv)
	case bool:
		return fmt.Sprintf("布尔值 %v", tv)
	case int:
		return fmt.Sprintf("整数 %d", tv)
	case float64:
		return fmt.Sprintf("小数 %v", tv)
	case []any:
		return fmt.Sprintf("列表（%d 项）", len(tv))
	case map[string]any:
		return fmt.Sprintf("嵌套映射（%d 个子键）", len(tv))
	default:
		return fmt.Sprintf("%T", v)
	}
}

// normalizeKey 把 frontmatter 的键归一到 case 里那个写法：小写 + 去掉 `-` `_`。
//
// **通用规则而不是别名表**——加新字段时不需要同步第二份清单。曾经的写法是每个多词键
// 逐个列 `case "whentouse", "when_to_use", "when-to-use"`，而那份清单跟另一处判「这个键
// 要不要收列表」的清单必须保持一致，实测漂过一次。
func normalizeKey(k string) string {
	return strings.NewReplacer("-", "", "_", "").Replace(strings.ToLower(strings.TrimSpace(k)))
}

// parseBoolOptional 返 (val, recognized)——recognized=false 时 caller 应保留默认。
//
// **R12 audit MED-1 fix**：旧 parseBool "未识别值视为 false"语义对默认 false 字段
// 无伤，但对默认 true 字段（UserInvocable）会**静默翻转默认**。**R13 audit LOW-1
// 后**已移除旧 parseBool，所有 frontmatter bool 字段统一走 parseBoolOptional。
//
// 识别表：
//   - true:  "true" / "yes" / "y" / "1" / "on"（双向对称）
//   - false: "false" / "no" / "n" / "0" / "off"
//   - 其他（typo / "maybe" / 中文 / 空串）→ recognized=false
func parseBoolOptional(s string) (val, recognized bool) {
	lower := strings.ToLower(strings.TrimSpace(s))
	switch lower {
	case "true", "yes", "y", "1", "on":
		return true, true
	case "false", "no", "n", "0", "off":
		return false, true
	}
	return false, false
}
