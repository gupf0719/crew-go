package agent_v2

// 超大工具输出**转存**——结果产生的那一刻就把全文存起来，消息流里只留有界预览和取回定位符，模型需要时
// 用 [ReadToolOutputToolName] 分段读或按正则搜。不依赖压缩触发。
//
// # 为什么在结果产生时、而不是压缩时
//
// 一条几百 KB 的工具结果进了消息流，就跟着进了每一轮的 prompt、存档、会话历史，直到某一层压缩把它清掉。
// 压缩层（MicroCompact 的 offload）只在它清旧结果时才卸载，那之前的每一轮都在为它付费；而它清掉的时候
// 往往已经撑爆过一次上下文。产生时转存是追加式的：消息流里从一开始就只有预览，之后每一轮、每份存档、
// 会话历史都只带这个预览——也不会让已经发生过的前缀改变（不破坏 prompt 缓存）。
//
// # 形状
//
//   - **何时转存**：业务工具的结果（成功的 Output、失败的 message）超过 [ToolResultSpillConfig.Threshold] 字节。
//     框架铸的工具（取回工具自己、final_output、activate_skill……）与 return-direct 结果豁免——前者的输出是
//     结构化的或者本身就有上限，后者产出即答复、不进消息流。
//   - **在 OnToolEnd 之后转存**：钩子看到、改写的是全文，转存的是钩子改写之后的那份——模型能取回的就是钩子允许它
//     看的内容（结果脱敏放在 OnToolEnd 里照样生效）。后台任务的结果没有钩子这一步，结束时直接转存（见
//     background_task.go；那里的脱敏要放工具中间件）。
//   - **留下什么**：一段说明（多大、怎么取回）+ 头尾预览（4:1，共 [ToolResultSpillConfig.PreviewBytes]
//     字节）。消息上盖 [ToolResultSpillAnnotation]（值是取回 id）。id 按内容定：续跑时这一批重做，同样的内容
//     落同一个 id，不留孤儿。
//   - **压缩之后还取得回**：AutoCompact 把带取回 id 的结果摘要掉时，在摘要后面附一份「此前转存过的工具输出」索引
//     （工具名、大小、id）；ReactiveCompact 紧急丢组时同样补一条索引（见 spillIndex）。
//   - **存在哪**：`store.Store`（配置里给的，否则 ctx 里 WithStore 注入的那个），命名空间按调用方身份与会话
//     （没有会话 ID 时按任务）分，见 spillNamespace。
//   - **取回**：框架每次 Run 注入 `read_tool_output`，按 id 在**当前**会话（任务）的命名空间里找——别的会话
//     的 id 读不到。它的输出自带上限（每行最多 2000 字节、每次最多 16000 字节），不再被转存。
//   - **写失败 / 没有 store**：退化成截断到阈值（头尾保留、标明原始大小与「无法取回」），不留定位符。
//   - **生命周期**：配了 TTL 且后端支持就按 TTL 过期；否则不过期，业务按命名空间前缀 `_tool_spill` 自己清。
//     会话历史里的预览会一直在，过期之后取回会报「没找到」——那是有意的（会话结束时删会破坏续跑与回看）。

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// ReadToolOutputToolName 是取回转存内容的框架工具名。
const ReadToolOutputToolName = "read_tool_output"

// ToolResultSpillAnnotation 盖在被转存的那条 tool_result 上，值是取回 id（交给 read_tool_output 的那个）。
// 压缩层据它保留取回方式（清理、摘要、紧急丢掉这条结果时仍告诉模型怎么取回），业务据它找到全文。
const ToolResultSpillAnnotation = "crew_tool_result_spill"

// toolResultSpillInfoAnnotation 跟取回 id 盖在一起：「工具名，多少字节，多少行」——压缩之后的索引用它说明那是什么。
const toolResultSpillInfoAnnotation = "crew_tool_result_spill_info"

// spillIndexAnnotation 盖在带「此前转存过的工具输出」索引的摘要 / 索引消息上，值是索引条目（每行一条，见
// spillIndexLine）。下一次增量摘要从这里接着带——摘要模型可能改写正文里的那段。
const spillIndexAnnotation = "crew_tool_spill_index"

// spillIndexMax 是索引最多留多少条（最近的）。
const spillIndexMax = 50

// 默认值：阈值取 5 万字节（两家成熟实现分别独立落在 5 万字符 / 5 万字节）；预览 2000 字节（产生时转存、
// 只给小预览的那家用的就是 2KB）；取回一次最多 16000 字节、每行最多 2000 字节。
const (
	defaultSpillThreshold   = 50_000
	defaultSpillPreview     = 2_000
	readToolOutputMaxBytes  = 16_000
	readToolOutputLineBytes = 2_000
	readToolOutputMaxLines  = 2_000
	readToolOutputDefLines  = 200
)

// ToolResultSpillConfig 配超大工具输出转存，见 [WithToolResultSpill]。
type ToolResultSpillConfig struct {
	// Threshold 是单条业务工具结果超过多少字节就转存。<= 0 用默认 50000。
	Threshold int
	// PreviewBytes 是留在消息流里的预览字节数（头 4 : 尾 1）。<= 0 用默认 2000；大于 Threshold 时按 Threshold。
	PreviewBytes int
	// Store 是转存到哪。nil 时用 ctx 里注入的 store（[WithStore] / `store.WithContext`）；都没有时不转存、
	// 截断到阈值，并告警一次。
	Store store.Store
	// TTL 是转存内容的保留时长，后端支持 TTL（`store.TTLAware`）时生效；0 = 不过期。
	TTL time.Duration

	warnOnce sync.Once
}

// WithToolResultSpill 开启超大工具输出转存（仅 BasicAgent）：业务工具的结果（成功的 Output、失败的 message）
// 超过阈值时，全文存进 store，消息流里换成说明 + 头尾预览 + 取回 id，并注入 `read_tool_output` 工具让模型按需
// 分段读或搜索。
//
//	agent_v2.NewBasicAgent(
//	    agent_v2.WithStore(st),
//	    agent_v2.WithToolResultSpill(nil), // nil = 全用默认值
//	)
//
// 跟 [WithToolResultMaxBytes] 的关系：那个是截断（超出部分丢掉），这个是转存（全文可取回）。两个都配时
// 先判转存——超过转存阈值的结果换成预览，预览再按 ToolResultMaxBytes 截（预览通常远小于它）。
//
// 按大小处理（转存 / 截断）在这一批调用的 `OnToolEnd` 之后做：钩子看到全文，存进 store 的是钩子改写之后的那份——结果
// 脱敏在钩子里做就对取回也生效；钩子把结果改大了也照样转存。开了转存时 ToolResultMaxBytes 的截断也在钩子之后做。
func WithToolResultSpill(cfg *ToolResultSpillConfig) Option {
	return func(c *Config) {
		if cfg == nil {
			cfg = &ToolResultSpillConfig{}
		}
		c.ToolResultSpill = cfg
	}
}

func (c *ToolResultSpillConfig) threshold() int {
	if c.Threshold <= 0 {
		return defaultSpillThreshold
	}
	return c.Threshold
}

func (c *ToolResultSpillConfig) preview() int {
	p := c.PreviewBytes
	if p <= 0 {
		p = defaultSpillPreview
	}
	if t := c.threshold(); p > t {
		p = t
	}
	return p
}

// spillApplies 报这条结果能不能转存：框架铸的工具与 return-direct 结果豁免——跟源头截断同一组豁免与理由
// （见 truncMaxBytesFor）。
func spillApplies(minted bool, res *tool.Result) bool {
	if minted || res == nil {
		return false
	}
	if rd, ok := res.Metadata[tool.ReturnDirectMetaKey].(bool); ok && rd {
		return false
	}
	return true
}

// warnReadToolOutputOverride 在构造期告警「框架的 read_tool_output 顶掉了业务 toolkit 里的同名工具」——只顶
// 这个 agent 每次 Run 拼出来的那份工具表，业务的 toolkit 不动。
func warnReadToolOutputOverride(cfg *Config) {
	if cfg.ToolResultSpill != nil && cfg.Toolkit != nil && cfg.Toolkit.Get(ReadToolOutputToolName) != nil {
		slog.Warn("framework read_tool_output overrides business tool with the same name", "agent", cfg.Name)
	}
}

// spillStore 取这次转存用的 store：配置里给的优先，否则 ctx 里注入的。
func (c *ToolResultSpillConfig) spillStore(ctx context.Context) store.Store {
	if c.Store != nil {
		return c.Store
	}
	return store.FromContext(ctx)
}

// conversationScope 是一次调用所在的「调用方身份」与「会话（没有会话 ID 时是任务）」两段：转存的命名空间、后台
// 任务的作用域都按它分，取回 / 查询只认当前会话（任务）里的。
//
// 调用方身份取 ctx 里的调用主体（`compose_v2.GetOwner`，agui / a2a/server 会推）；没有时取这次会话历史的归属（开了
// 历史时，`WithHistoryOwnerFunc` 按请求解析的那个）——会话 ID 来自外部、不是凭据，两个租户用了同一个会话 ID 时
// 不能落进同一个作用域。两者都没有时按单租户处理。
//
// 两段都按哈希入：会话 ID、调用方身份都来自外部，可能含命名空间不允许的字符（`.`），也不该原样出现在存储键里。
func conversationScope(ctx context.Context) (owner, conv string) {
	if id := compose_v2.GetThreadID(ctx); id != "" {
		conv = "c" + shortHash(id)
	} else {
		conv = "t" + shortHash(currentHistoryTask(ctx))
	}
	o := compose_v2.GetOwner(ctx)
	if o == "" {
		if w := historyWriterFrom(ctx); w != nil {
			o = w.key.Owner
		}
	}
	return "o" + shortHash(o), conv
}

// spillNamespace 是转存内容的命名空间：("_tool_spill", 调用方身份, 会话)，见 conversationScope。取回工具用同一个
// 函数算，所以只能读到当前会话（任务）转存的内容。
func spillNamespace(ctx context.Context) store.Namespace {
	owner, conv := conversationScope(ctx)
	return store.NS("_tool_spill", owner, conv)
}

func shortHash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:16])
}

// spillID 是一段内容的取回 id：按内容哈希（64 位）。同一份内容在同一个会话里落同一个 id——续跑时这一批重做、
// 内容没变就是同一条记录，不留孤儿；不同会话的命名空间不同，同一个 id 也读不到别人的。
func spillID(content string) string {
	h := sha256.Sum256([]byte(content))
	return "sp_" + hex.EncodeToString(h[:8])
}

// spillToolResult 把超过阈值的内容转存，返回留在消息流里的那段（说明 + 预览）、取回 id 与说明（工具名、大小、
// 行数）。没有 store 或写失败时返回截断到阈值的内容、id 为空。
func spillToolResult(ctx context.Context, cfg *ToolResultSpillConfig, toolName, content string) (kept, id, info string) {
	st := cfg.spillStore(ctx)
	if st == nil {
		cfg.warnOnce.Do(func() {
			slog.WarnContext(ctx, "agent_v2: 开了 WithToolResultSpill 但没有可用的 store（配置里没给、ctx 里也没注入），"+
				"超大工具输出改为截断、无法取回全文", "tool", toolName)
		})
		return spillFallback(toolName, content, cfg.threshold()), "", ""
	}
	id = spillID(content)
	var opts []store.PutOption
	if cfg.TTL > 0 {
		if ta, ok := st.(store.TTLAware); ok && ta.SupportsTTL() {
			opts = append(opts, store.WithTTL(cfg.TTL))
		}
	}
	if err := st.Put(ctx, spillNamespace(ctx), id, content, opts...); err != nil {
		slog.WarnContext(ctx, "agent_v2: 超大工具输出转存失败，改为截断（无法取回全文）",
			"tool", toolName, "bytes", len(content), "err", err)
		return spillFallback(toolName, content, cfg.threshold()), "", ""
	}
	// 行数跟取回工具同一个口径：超长的行折成多行编号（见 foldLines）
	lines := len(foldLines(content, readToolOutputLineBytes))
	info = fmt.Sprintf("%s，%d 字节，%d 行", toolName, len(content), lines)
	var b strings.Builder
	fmt.Fprintf(&b, "【工具输出过大，已转存】%s 返回了 %d 字节（%d 行），下面只是开头和结尾的预览。\n", toolName, len(content), lines)
	fmt.Fprintf(&b, "要看完整内容，调用 %s，参数 id=%q：用 offset / limit 按行分段读，或用 pattern 按正则搜索匹配的行（每次最多返回 %d 字节）。\n",
		ReadToolOutputToolName, id, readToolOutputMaxBytes)
	b.WriteString("--- 预览 ---\n")
	b.WriteString(truncateMiddle(content, cfg.preview()))
	return b.String(), id, info
}

// spillIndexLine 是索引里的一条：取回 id 与说明。
func spillIndexLine(id, info string) string {
	if info == "" {
		return fmt.Sprintf("- id=%q", id)
	}
	return fmt.Sprintf("- id=%q：%s", id, info)
}

// spillIndex 收 msgs 里出现过的转存（带取回 id 的消息、上一份索引里的条目），按出现先后、去重，留最近的
// spillIndexMax 条。压缩层把这些消息摘要掉 / 丢掉时，用它留下取回方式（全文还在 store 里，不留 id 模型就取不回）。
func spillIndex(msgs []*schema.Message) []string {
	var lines []string
	seen := map[string]bool{}
	add := func(id, line string) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		lines = append(lines, line)
	}
	for _, m := range msgs {
		if m == nil || m.Annotations == nil {
			continue
		}
		if idx, _ := m.Annotations[spillIndexAnnotation].(string); idx != "" {
			for _, line := range strings.Split(idx, "\n") {
				if mm := spillIndexIDRe.FindStringSubmatch(line); mm != nil {
					add(mm[1], line)
				}
			}
		}
		if id, _ := m.Annotations[ToolResultSpillAnnotation].(string); id != "" {
			info, _ := m.Annotations[toolResultSpillInfoAnnotation].(string)
			add(id, spillIndexLine(id, info))
		}
	}
	if len(lines) > spillIndexMax {
		lines = lines[len(lines)-spillIndexMax:]
	}
	return lines
}

var spillIndexIDRe = regexp.MustCompile(`^- id="(sp_[0-9a-f]+)"`)

// spillIndexText 是索引的正文。
func spillIndexText(lines []string) string {
	return "此前转存过的工具输出（全文仍可用 " + ReadToolOutputToolName + " 取回）：\n" + strings.Join(lines, "\n")
}

// spillFallback 是转存不了时的退路：截断到阈值，标明原始大小、以及这次取不回全文。
func spillFallback(toolName, content string, threshold int) string {
	head := fmt.Sprintf("【工具输出过大，已截断】%s 返回了 %d 字节，下面只保留开头和结尾（这次未能转存，无法取回全文）。\n", toolName, len(content))
	budget := threshold - len(head)
	if budget < truncMiddleMinUseful {
		budget = truncMiddleMinUseful
	}
	return head + truncateMiddle(content, budget)
}

// newReadToolOutputTool 是框架铸的取回工具。
func newReadToolOutputTool(cfg *ToolResultSpillConfig) tool.Tool {
	schemaDef := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"id":      map[string]any{"type": "string", "description": "转存说明里给出的 id"},
			"offset":  map[string]any{"type": "integer", "description": "从第几行开始读（从 1 数），默认 1"},
			"limit":   map[string]any{"type": "integer", "description": fmt.Sprintf("最多读多少行，默认 %d，最多 %d", readToolOutputDefLines, readToolOutputMaxLines)},
			"pattern": map[string]any{"type": "string", "description": "只返回匹配这个正则（RE2 语法）的行；配合 offset 从某行之后接着搜"},
		},
		"required": []string{"id"},
	}
	desc := fmt.Sprintf("读取之前被转存的超大工具输出。工具输出超过上限时，结果里只有预览和一个 id；用本工具按行分段读全文"+
		"（offset / limit），或用 pattern 搜索匹配的行。超长的行按 %d 字节折成多行编号。每次最多返回 %d 字节。",
		readToolOutputLineBytes, readToolOutputMaxBytes)
	return markMinted(tool.NewFuncTool(ReadToolOutputToolName, desc, schemaDef,
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			return readToolOutput(ctx, cfg, args), nil
		}))
}

// readToolOutput 按 id 取回、按行切片或搜索。参数或内容有问题时返回给模型看的失败结果，不返回 error
// （那会让整轮失败）。
func readToolOutput(ctx context.Context, cfg *ToolResultSpillConfig, args map[string]any) *tool.Result {
	id, _ := args["id"].(string)
	if id == "" {
		return tool.Failure("invalid_arguments", "缺少 id——用转存说明里给出的那个")
	}
	st := cfg.spillStore(ctx)
	if st == nil {
		return tool.Failure("unavailable", "转存存储不可用")
	}
	item, err := st.Get(ctx, spillNamespace(ctx), id)
	if err != nil {
		return tool.Failure("unavailable", fmt.Sprintf("读取转存内容失败：%v", err))
	}
	content, ok := "", false
	if item != nil {
		content, ok = item.Value.(string)
	}
	if !ok {
		return tool.Failure("not_found", fmt.Sprintf("没找到 id=%q 的转存内容（可能已过期，或不属于当前会话）", id))
	}
	offset := intArg(args, "offset", 1)
	if offset < 1 {
		offset = 1
	}
	limit := intArg(args, "limit", readToolOutputDefLines)
	if limit < 1 {
		limit = readToolOutputDefLines
	}
	if limit > readToolOutputMaxLines {
		limit = readToolOutputMaxLines
	}
	var re *regexp.Regexp
	if p, _ := args["pattern"].(string); p != "" {
		re, err = regexp.Compile(p)
		if err != nil {
			return tool.Failure("invalid_arguments", fmt.Sprintf("pattern 不是合法的正则：%v", err))
		}
	}
	lines := foldLines(content, readToolOutputLineBytes)
	if offset > len(lines) {
		return tool.Failure("out_of_range", fmt.Sprintf("offset=%d 超出范围：共 %d 行", offset, len(lines)))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "id=%s 共 %d 行、%d 字节。", id, len(lines), len(content))
	header := b.Len()
	// 正文的预算：整段输出（抬头 + 行 + 结尾）不超过 readToolOutputMaxBytes。抬头的第二段与结尾按这次的参数算出上界
	// （里面的数字取可能的最大值）先留出来：正则经 %q 显示，转义之后可以比原文长好几倍，按固定余量留会超出上限（审计
	// 实测反斜杠多的正则下一次返回 16014 字节）。
	maxNum := len(lines) + 1
	pat, frame := "", 0
	if re != nil {
		pat = truncateWithEllipsis(re.String(), 200)
		frame = len(fmt.Sprintf("从第 %d 行起匹配 %q 的行（显示 %d 行）：\n", offset, pat, limit)) +
			max(len("（已搜到末尾，后面没有更多匹配的行）"), len(fmt.Sprintf("（还有更多可能匹配的行；接着搜用 offset=%d）", maxNum)))
	} else {
		frame = len(fmt.Sprintf("第 %d–%d 行：\n", offset, maxNum)) +
			max(len("（已到末尾）"), len(fmt.Sprintf("（后面还有 %d 行；接着读用 offset=%d）", maxNum, maxNum)))
	}
	budget := readToolOutputMaxBytes - header - frame
	shown, last, scanned := 0, offset-1, offset-1 // last：最后一行显示的；scanned：扫到哪（按正则搜时可能比 last 远）
	body := strings.Builder{}
	for i := offset - 1; i < len(lines) && shown < limit; i++ {
		if re != nil && !re.MatchString(lines[i]) {
			scanned = i + 1
			continue
		}
		row := fmt.Sprintf("%d| %s\n", i+1, lines[i])
		if body.Len()+len(row) > budget && shown > 0 {
			break
		}
		body.WriteString(row)
		shown++
		last, scanned = i+1, i+1
	}
	if re != nil {
		fmt.Fprintf(&b, "从第 %d 行起匹配 %q 的行（显示 %d 行）：\n", offset, pat, shown)
	} else {
		fmt.Fprintf(&b, "第 %d–%d 行：\n", offset, last)
	}
	b.WriteString(body.String())
	switch {
	case scanned >= len(lines) && re != nil:
		b.WriteString("（已搜到末尾，后面没有更多匹配的行）")
	case scanned >= len(lines):
		b.WriteString("（已到末尾）")
	case re != nil:
		fmt.Fprintf(&b, "（还有更多可能匹配的行；接着搜用 offset=%d）", last+1)
	default:
		fmt.Fprintf(&b, "（后面还有 %d 行；接着读用 offset=%d）", len(lines)-last, last+1)
	}
	return tool.Success(b.String())
}

// foldLines 按换行切行，超过 width 字节的行在 rune 边界上折成多行——单行的大 JSON 也能分段读到。
func foldLines(s string, width int) []string {
	raw := strings.Split(s, "\n")
	out := make([]string, 0, len(raw))
	for _, line := range raw {
		for len(line) > width {
			cut := width
			for cut > 0 && !utf8.RuneStart(line[cut]) {
				cut--
			}
			if cut == 0 {
				cut = width
			}
			out = append(out, line[:cut])
			line = line[cut:]
		}
		out = append(out, line)
	}
	return out
}

// intArg 取一个整数参数（JSON 解出来是 float64；模型偶尔把数字写成字符串，也认）；没给或类型不对时返回 def。
func intArg(args map[string]any, key string, def int) int {
	switch v := args[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	case int64:
		return int(v)
	}
	return def
}
