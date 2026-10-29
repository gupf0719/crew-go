package agent_v2

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// ReactiveCompact 被动压缩（ISSUE-68）
//
// 上下文压缩 4 层中的第 3 层——LLM 报 prompt-too-long 时兜底重试。
// AutoCompact 没拦住时（譬如未启用、阈值算偏、单条消息太大）由本层抢救。
//
// 触发：BasicAgent makeModelFn 内 callLLM retry 循环包围 streamOrGenerate 单次
// 调用，捕获 prompt-too-long 错误（IsPromptTooLongError 判定 11 条精确字符串）。
// 算法（emergencyDropOldestGroups 实际 8 步）：
//   1) 隔离前导 system 消息（永不删）
//   2) GroupMessagesByAPIRound 按 RunID 分组（user 总开新组）
//   3) 识别 droppable 组（跳过 AutoCompactSummaryAnnotation / StoreInjectedAnnotation 保护组）
//   4) 全保护时仍允许丢最旧组（兜底）
//   5) 解析错误的 token gap：有 gap 累加到 >= gap；无 gap 用 EmergencyDropPercent
//   6) 永远保留至少 1 个 droppable 组
//   7) 删后第一条不是 system 的若是 assistant，在它前面补 user marker（API 要求 user-first）
//   8) sys 拼回最前
// 删完用压缩后 messages 重试，最多 MaxAttempts 次（compacted == nil 或长度 >= curMsgs 时提前 break）。
// **不写回 channel** —— 仅 retry 循环局部变量 curMsgs 上覆盖，下轮 model Get 还是原 messages。

// ReactiveCompactConfig 控制 ReactiveCompact 行为。
type ReactiveCompactConfig struct {
	// Enabled 默认 false——需 WithReactiveCompact 显式开启。
	Enabled bool

	// MaxAttempts 同一轮 LLM 调用最大重试次数。<= 0 时使用默认值 3。
	MaxAttempts int

	// EmergencyDropPercent token gap 解析失败时按 N% 删除最旧消息组。
	// <= 0 时使用默认值 20。
	EmergencyDropPercent int

	// UserMarker 删消息后第一条不是 system 的若是 assistant，在它前面补这条 user 消息（API 要求 user 开头）。
	// 空时使用 defaultUserMarker。
	UserMarker string
}

const (
	defaultReactiveMaxAttempts = 3
	defaultReactiveDropPercent = 20
	defaultReactiveUserMarker  = "[continued after context overflow]"
)

// promptTooLongMarkers 跨 provider 已知的"上下文超限"错误特征短语（小写匹配）。
//
// 严格策略：必须是 LLM API 上下文超限明确报错的短语，不能用太宽的关键词
// （旧版有 "context window" / "exceeds maximum"，会误匹配业务错误如
// "value exceeds maximum allowed" → 误触发 ReactiveCompact 删历史，破坏会话）。
// 每条短语都对应至少一个真实 provider 的 prompt-too-long 错误格式。
var promptTooLongMarkers = []string{
	"prompt is too long",                // 通用
	"prompt too long",                   // 通用变体
	"input is too long",                 // Anthropic Claude
	"context length exceeded",           // OpenAI 早期
	"maximum context length",            // OpenAI: "This model's maximum context length is N tokens"
	"too many tokens in",                // 部分 provider
	"context_length_exceeded",           // OpenAI error code
	"reduce the length of the messages", // OpenAI 提示尾段
	"reduce the length of your messages",
	"range of input length should be", // 千问
	"request too large for",           // OpenAI rate limit + size
}

// IsPromptTooLongError 检测错误是否为上下文超限——跨 provider 字符串匹配。
//
// 公开是为了用户在自定义错误处理 / 自己的中间件里复用同一套判定（避免与
// 我们内部 retry 路径不一致）。误判会触发 ReactiveCompact 错删历史，所以
// 默认严格——宁可漏判（让错误按原样冒泡）也不误判。
func IsPromptTooLongError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, m := range promptTooLongMarkers {
		if strings.Contains(msg, m) {
			return true
		}
	}
	// 兜底：能解析出 token gap 数字的也算（OpenAI/Claude 格式）
	if ParsePromptTooLongGap(err) > 0 {
		return true
	}
	return false
}

// promptTooLongOpenAIRe 解析 OpenAI 风格："maximum context length is 8192 tokens.
// However, your messages resulted in 9123 tokens"
var promptTooLongOpenAIRe = regexp.MustCompile(`(?i)(\d+)\s+tokens\.\s+However,\s+your\s+messages\s+resulted\s+in\s+(\d+)`)

// promptTooLongClaudeRe 解析 Claude 风格："Input is too long: 234156 input tokens (max 200000)"
var promptTooLongClaudeRe = regexp.MustCompile(`(?i)(\d+)\s+input\s+tokens\s+\(max\s+(\d+)\)`)

// ParsePromptTooLongGap best-effort 解析错误中"超出多少 tokens"。
// 返回 0 表示无法解析，调用方走 dropPercent 兜底。
func ParsePromptTooLongGap(err error) int {
	if err == nil {
		return 0
	}
	msg := err.Error()

	if m := promptTooLongOpenAIRe.FindStringSubmatch(msg); len(m) == 3 {
		max, _ := strconv.Atoi(m[1])
		cur, _ := strconv.Atoi(m[2])
		if cur > max {
			return cur - max
		}
	}

	if m := promptTooLongClaudeRe.FindStringSubmatch(msg); len(m) == 3 {
		cur, _ := strconv.Atoi(m[1])
		max, _ := strconv.Atoi(m[2])
		if cur > max {
			return cur - max
		}
	}

	return 0
}

// emergencyDropOldestGroups 按 token gap 或固定百分比删最旧的消息组。
//
// 算法：
//  1. 隔离前导 system 消息（永不删——丢 system prompt 会让 LLM 行为崩溃）
//  2. 对 conv 部分做 GroupMessagesByAPIRound 分组——保 tool_use ↔ tool_result 配对完整
//  3. gap > 0：按 token 估算从最旧累加直到达到 gap
//     gap == 0：删最旧 dropPercent %
//  4. 至少保留 1 组——单组对话也要能跑下去
//  5. 删后第一条不是 system 的若是 assistant，在它前面补 user marker（API 要求 user 开头）
//  6. 把 system 重新拼回最前
func emergencyDropOldestGroups(messages []*schema.Message, dropPercent int, gap int, marker string) []*schema.Message {
	if len(messages) == 0 {
		return messages
	}
	// 步骤 1：隔离前导 system 消息。
	//
	// ⚠️ **它保护的不是 agent 自己的 system prompt**——那一段（`WithSystemPrompt` +
	// skill hint/body + `WithPromptTemplate` 渲染结果）**根本不在 messages 里**，走
	// `agent_v2.BasicAgentPreambleOutput` 的 System 那条流，压缩层结构上碰不到它。
	// 这里剩下的是「长期记忆注入」和「调用方自己塞在最前的 system」。
	//
	// **别再把这个前导块当成 agent 的 persona**：它曾经承担过那个角色，而判据是位置——
	// 模板一旦渲染出 few-shot，真 sysPrompt 就排在它后面、当场降级成普通可丢组，
	// 实测被直接丢掉且 `err == nil`（见 doc/description/three-way-comparison/context-compaction.md 结论五）。
	//
	// 注意：messages 元素可能为 nil（防御性）；nil 不算 system，停止扫描。
	sysEnd := 0
	for sysEnd < len(messages) && messages[sysEnd] != nil && messages[sysEnd].Role == schema.RoleSystem {
		sysEnd++
	}
	sys := messages[:sysEnd]
	conv := messages[sysEnd:]
	if len(conv) == 0 {
		return giveUpOrShrink(messages, gap)
	}

	groups := compose_v2.GroupMessagesByAPIRound(conv)
	if len(groups) <= 1 {
		return giveUpOrShrink(messages, gap)
	}

	// 步骤 1.5：识别 droppable 组——含 AutoCompactSummaryAnnotation 的组保护
	// （丢 summary 等于丢早期所有压缩历史，比丢一个普通 round 严重得多）。
	// droppableIdx 按从旧到新的顺序记录可丢弃组的全局索引。
	// 混着 system 的组可以丢，丢的时候 system 原地留下（见 isProtectedGroup 的「只有 system 的组」那一条）。
	var droppableIdx []int
	for i, g := range groups {
		if isProtectedGroup(g) {
			continue
		}
		droppableIdx = append(droppableIdx, i)
	}
	// **R21 audit HIGH-2 修订**：原"全 protected 兜底删 groups[0]"会破坏 SkillBody
	// 配对——若 groups[0] 是 [asst tool_call, tool_result(ack with marker)]，删后
	// kept[0]=[user(body of skill1)] 但配对 asst 已删，孤儿 user message；更糟
	// groups[0]=[asst tool_call] groups[1]=[tool_result(ack)] 时删 [0] 后 kept 起
	// 头是 RoleTool → API 422 "tool messages must immediately follow assistant"。
	//
	// 改：全 protected 时**不删**——返原 messages 让 callLLM 重试外层走 retry 失败
	// 路径，最终业务看到 prompt-too-long 错误（精准信号）而非 silent 破坏 pair。
	if len(droppableIdx) == 0 {
		return giveUpOrShrink(messages, gap)
	}

	var dropCount int
	if gap > 0 {
		acc := 0
		for _, i := range droppableIdx {
			_, rest := splitSystem(groups[i])
			acc += compose_v2.EstimateMessageTokens(rest)
			dropCount++
			if acc >= gap {
				break
			}
		}
	} else {
		dropCount = len(droppableIdx) * dropPercent / 100
		if dropCount < 1 {
			dropCount = 1
		}
	}
	if dropCount >= len(droppableIdx) {
		dropCount = len(droppableIdx) - 1
	}
	// 永远保留至少 1 组对话——单组 droppable 时 dropCount=0 → 不动
	if dropCount <= 0 {
		return giveUpOrShrink(messages, gap)
	}

	// 构造 toDrop set：droppableIdx 的最旧 dropCount 个
	toDrop := make(map[int]bool, dropCount)
	for _, i := range droppableIdx[:dropCount] {
		toDrop[i] = true
	}

	// 按原顺序保留未被 drop 的组
	keptGroups := make([][]*schema.Message, 0, len(groups)-dropCount)
	var dropped []*schema.Message
	for i, g := range groups {
		if !toDrop[i] {
			keptGroups = append(keptGroups, g)
			continue
		}
		sysMsgs, rest := splitSystem(g)
		if len(sysMsgs) > 0 {
			keptGroups = append(keptGroups, sysMsgs)
		}
		dropped = append(dropped, rest...)
	}
	kept := dropOrphanToolResults(compose_v2.FlattenGroups(keptGroups))
	// 丢掉的组里带取回 id 的（转存过的工具结果、摘要附的索引）：全文还在 store 里，补一条索引放在最前，模型照样取得回
	// （见 tool_spill.go 的 spillIndex）。它是 user 角色，保留部分以 assistant 开头时也就不用再补占位了。
	if idx := spillIndex(dropped); len(idx) > 0 {
		indexMsg := schema.UserMessage(spillIndexText(idx))
		indexMsg.Annotations = map[string]any{spillIndexAnnotation: strings.Join(idx, "\n")}
		kept = append([]*schema.Message{indexMsg}, kept...)
	}

	// 删后第一条不是 system 的是 assistant → 在它前面补 user marker（API 要求 user-first，system 之后；丢掉的组里留下的
	// system 可能排在它前面）
	for i, m := range kept {
		if m.Role == schema.RoleSystem {
			continue
		}
		if m.Role == schema.RoleAssistant {
			withMarker := make([]*schema.Message, 0, len(kept)+1)
			withMarker = append(withMarker, kept[:i]...)
			withMarker = append(withMarker, schema.UserMessage(marker))
			kept = append(withMarker, kept[i:]...)
		}
		break
	}

	// 步骤 6：sys 拼回最前
	out := make([]*schema.Message, 0, len(sys)+len(kept))
	out = append(out, sys...)
	out = append(out, kept...)
	return out
}

// isProtectedGroup 检查组内任意消息是否带受保护 marker；带 marker 的组视为
// 受保护（emergencyDrop 不删）。
//
// 五类受保护 marker：
//   - AutoCompactSummaryAnnotation：AutoCompact 产出的摘要消息——丢 summary
//     等于丢早期所有压缩历史
//   - StoreInjectedAnnotation：buildInitialMessages 注入的 StoreMemory 消息——
//     位于 system 之后（最旧组），ReactiveCompact emergencyDrop 优先删时会
//     直接删 StoreMemory 内容，下次 Run 重新 store.Search 又注入但若 Search
//     不稳定（语义匹配差异）跨 Run 内容会突变 → 用户感知"AI 突然失忆"
//   - SkillBodyAnnotation：toolCall 模式 LLM 调 activate_skill 后框架打的
//     marker——含 SKILL.md body 的 tool_result。AutoCompact 跨压缩保鲜（P1.C），
//     ReactiveCompact 也必须保护否则第 3 层删掉前功尽弃（P1.C audit H1 修复）
//   - **MicroCompactOffloadedAnnotation** (HIGH-2 audit B 修)：MicroCompact
//     offload 到 store 的占位符 — 删了 LLM 永久丢失 store key reference, 业务
//     "可重读 store 全文" 协议破裂。占位符 token 小 (~200 byte preview), 删了
//     换不来多少 token 节省, 代价是破坏 offload 协议
//   - **只有 system 的组**（无 marker 也保护）：`GroupMessagesByAPIRound` 让中段 system 平常单独成组，「一组全是
//     system」就是「这是一条指令消息」。**位置无关**——曾经只保护**前导**块，而那时 agent 自己的 system prompt
//     就在里面；它已经搬出这条消息流了（见 `BasicAgentPreambleOutput`），这里剩下的是长期记忆注入（另有 marker）
//     和**调用方自己塞的 system**。后者按位置判会漏：塞在中段就成了普通可丢组。业务把 system 插进一次调用与它的
//     结果之间时，它跟这一对同组：这种组**可以丢，丢的时候 system 原地留下**（保的是指令本身，不让它连带同组的
//     消息一起不可丢——曾经整组保护，钩子每轮都这么插时紧急压缩一条也丢不掉，审计实测）。
//   - **MicroCompactClearedAnnotation** (HIGH-2 audit B 修): MicroCompact 纯清
//     占位符 — 同上, 删了破坏审计 marker 链; 占位符 token 小不值得删
//
// **设计原则**：4 层压缩对所有"框架已 marker 的关键内容"必须一致保护——
// 任何一层保护别一层删都会破坏跨层不变量。**Trade-off**: 全 offloaded /
// cleared 场景下 emergencyDrop 全 protected 返原 messages → caller LLM 仍报
// prompt_too_long → ReactiveCompact exhausted error, 业务可观测 (这是 expected
// behavior — 业务应启 Read tool 让 LLM 重读 offloaded 全文降 token).
func isProtectedGroup(group []*schema.Message) bool {
	if sysMsgs, rest := splitSystem(group); len(sysMsgs) > 0 && len(rest) == 0 {
		return true
	}
	for _, m := range group {
		if m == nil || m.Annotations == nil {
			continue
		}
		if v, ok := m.Annotations[AutoCompactSummaryAnnotation].(bool); ok && v {
			return true
		}
		if v, ok := m.Annotations[StoreInjectedAnnotation].(bool); ok && v {
			return true
		}
		if v, ok := m.Annotations[SkillBodyAnnotation].(bool); ok && v {
			return true
		}
		// HIGH-2 audit B fix: 跨层 marker 一致防护 — MicroCompact 加的 2 个 marker
		// 必须 ReactiveCompact 同款保护, 否则 emergencyDrop 删占位符让 offload key
		// 丢失 + cleared marker 链破坏
		if v, ok := m.Annotations[MicroCompactOffloadedAnnotation].(bool); ok && v {
			return true
		}
		if v, ok := m.Annotations[MicroCompactClearedAnnotation].(bool); ok && v {
			return true
		}
	}
	return false
}

// ReactiveCompactRun 应用 ReactiveCompact 算法到 messages，返回压缩后切片。
//
// callErr 是触发本次压缩的 LLM 错误——用于解析 token gap。
// 返回 (newMessages, error)：error 仅在断言失败（如 cfg=nil）时返回；正常路径返回 nil。
//
// **并发设计**（ISSUE-70 审计 #31 复审结论）：
// 与 AutoCompact 不同——ReactiveCompact **不需要** callMu 串行化。原因：
//   - AutoCompact callMu 防的是自身断路器被并发分支击穿（断路器是自我保护机制）
//   - ReactiveCompact 没有断路器，"压缩失败"退化为 emergencyDrop（确定性删消息）
//   - retry 是 LLM 调用层 retry，分支独立——加 mu 会把 fan-out 串行化为
//     N×retry_time，性能严重下降
//   - 共享 cfg.Model 的 fan-out 同时打 Model 是 Model 层限流的责任，不是
//     ReactiveCompact 的关注点
//
// 所以这是 by design 不串行化，不要加 callMu。
func ReactiveCompactRun(ctx context.Context, cfg *ReactiveCompactConfig, messages []*schema.Message, callErr error) ([]*schema.Message, error) {
	if cfg == nil {
		return messages, errors.New("reactive compact: nil config")
	}
	dropPercent := cfg.EmergencyDropPercent
	if dropPercent <= 0 {
		dropPercent = defaultReactiveDropPercent
	}
	marker := cfg.UserMarker
	if marker == "" {
		marker = defaultReactiveUserMarker
	}
	gap := ParsePromptTooLongGap(callErr)
	// emit Before event (opt-in). Trigger="prompt_too_long" — 跟 caller modelFn
	// retry 循环对齐 (basic_agent.go 内 IsPromptTooLongError 才 retry).
	//
	// Sub-5d audit fix: 测 inputTokens + startNanos 让 After event 自带 InputTokens +
	// DurationMs (OTel self-contained).
	var inputTokens int
	var compactStartNanos int64
	if hasEventSink(ctx) {
		inputTokens = compose_v2.EstimateTokens(messages)
		compactStartNanos = time.Now().UnixNano()
		EmitCompactionEvent(ctx, &CompactionAction{
			Layer: CompactionLayerReactive,
			Type:  CompactionActionBefore,
			Before: &CompactionBeforeAction{
				Messages:    messages,
				InputTokens: inputTokens,
				Trigger:     "prompt_too_long",
			},
		})
	}
	out := emergencyDropOldestGroups(messages, dropPercent, gap, marker)
	if hasEventSink(ctx) {
		EmitCompactionEvent(ctx, &CompactionAction{
			Layer: CompactionLayerReactive,
			Type:  CompactionActionAfter,
			After: &CompactionAfterAction{
				Messages:     out,
				Kept:         len(out),
				Dropped:      len(messages) - len(out),
				InputTokens:  inputTokens,
				OutputTokens: compose_v2.EstimateTokens(out),
				DurationMs:   (time.Now().UnixNano() - compactStartNanos) / 1e6,
			},
		})
	}
	return out, nil
}

// groupContainsSystem 判断一组里**有没有** system 消息。
//
// 分组让中段 system 平常单独成组（「system 既开新组也关新组」，见 `compose_v2.GroupMessagesByAPIRound`）；例外是
// system 被插进一次调用与它的结果之间，那时它跟这一对同组。两层压缩据此**只保 system 本身**：AutoCompact 把 system
// 拿出来留下、其余照常摘要（`splitProtectedGroups`），ReactiveCompact 丢这种组时 system 原地留下（`splitSystem`）。
// 整组保护会让一条 system 把同组的别的消息一起变成不可压缩——曾经是真代价：实测摘要跑完入 30016 出 30059 字节反而
// 变大；分组改成按配对不切组之后，钩子每轮都在调用与结果之间插 system 时压缩完全失效。
//
// ⚠️ **它取代不了 marker 判据**：长期记忆注入**也是 system**，所以两条判据在它身上
// 结论一致（都说保护）——但**理由不同，别把 marker 那条当成冗余删掉**。marker 那条护的是
// 「跨 Run 内容稳定性」：删了下次 `store.Search` 会重新注入，而 Search 若不稳定内容就会
// 突变，用户感知是「AI 突然失忆」（见 `isProtectedGroup` 的 godoc）。本条护的是
// 「这是一条指令消息」。哪天注入形态不再是 system（比如改走别的 role 或别的通道），
// 本条就不再覆盖它，而 marker 那条仍然必须在。
func groupContainsSystem(group []*schema.Message) bool {
	for _, m := range group {
		if m != nil && m.Role == schema.RoleSystem {
			return true
		}
	}
	return false
}

// splitSystem 把一组拆成其中的 system 消息与其余消息，各保持原顺序。
func splitSystem(group []*schema.Message) (sysMsgs, rest []*schema.Message) {
	for _, m := range group {
		if m != nil && m.Role == schema.RoleSystem {
			sysMsgs = append(sysMsgs, m)
		} else {
			rest = append(rest, m)
		}
	}
	return sysMsgs, rest
}

// giveUpOrShrink 是**「一组也丢不动」时的统一兜底**：先试着就地把超大的单条挖空，
// 挖不动才返原样。
//
// ⚠️ **「丢不动」有四个出口**（对话段为空 / 只剩一组 / 全 protected / 算下来 dropCount 为 0），
// 四个都得走这里——只挂其中一个的话，另外几个照旧直接放弃，而它们同样可能面对
// 「里头有一条巨物」。**这条是写测试时撞出来的**：第一版只挂了「全 protected」那个，
// fixture 却先撞上「只剩一组」那个早退，于是被测路径根本没执行。
//
// 为什么就地挖空安全而「丢组」不安全：它**不动条数、不动 role 序、不碰 tool_call ID**
// ⟹ tool_use ↔ tool_result 的配对约束天然不受影响，而那正是丢组时最容易破坏的东西。
//
// ⚠️ **只做这一级，不做「再撤保护、连受保护的也丢」那一级**。查过六家：唯一有那一档的
// 那家，两条前提我们都不具备（它没有 error-reactive 路径、预算是软的 ×0.8）；而结构上跟
// 我们同构的两家答案跟我们一样——选不出来就把原始错误还给调用方。⚠️ 曾经以为另一家溢出时
// 「撤保护」，读了源码才发现它撤的是**保留尾部的预算**（那个参数的 godoc 原文写着
// "minimum recent tail budget retained verbatim"），而结构性的配对约束一步没放松。
//
// 挖不动时返原样是**有意的**：那时确实无能为力，把精确的 prompt-too-long 还给业务，
// 让它去换模型 / 开新会话，比静默降级值钱。
func giveUpOrShrink(messages []*schema.Message, gap int) []*schema.Message {
	shrunk := shrinkOversizedInPlace(messages, gap)
	if compose_v2.EstimateMessageTokens(shrunk) < compose_v2.EstimateMessageTokens(messages) {
		return shrunk
	}
	return messages
}

// emergencyShrinkKeepBytes 是就地挖空后单条至少保留的字节数。
//
// 取值理由：要放得下「头 + 标记 + 尾」还剩下有信息量的两段。再小的话头尾各剩几十字节，
// 模型既看不懂内容也判断不了要不要去读回原文——那时不如不留。
const emergencyShrinkKeepBytes = 2000

// shrinkOversizedInPlace 是「一组也丢不动」时的兜底：**不丢任何消息**，把超大的
// **工具结果**挖空，直到腾出 gap。返回新切片 + 新消息对象，不改调用方手里那份。
//
// # 只挖 tool_result，不挖 system / user / assistant
//
// 这不是保守，是**照着别人的实际代码定的**：查过的几家里，做单条截断的三家**无一例外
// 只截工具产出**——其中一家的分派函数把消息条目逐个类型列了一遍，消息正文、推理内容、
// 工具调用声明**全部原样透传**，进截断的只有「工具输出」那两个类型。
//
// ⚠️ **第一版是按位置遍历的，那是错的**：下标 0 恰好是 `emergencyDropOldestGroups`
// 隔离出来的前导 `sys` 块——丢组路径结构上碰不到它，挖空路径却恒第一个碰。实测
// 15038 字节的 system 指令被挖到 2000，而同一条消息流里 **200000 字节的受保护工具结果
// 一个字没动**。挖空路径专挑丢组路径保护的那个下手。
//
// 体积降序：同样的空间受害者最少。gap <= 0（错误串里解不出缺口）时**只挖最大的那一条**
// ——那时全力开火会砍掉 95%，而 retry 循环本来就会再来一次，逐次收紧更稳。
//
// ⚠️ **两份正文都要改**：`schema.ToolResultMessage` 造出来的消息把正文同时存在
// `Content` 和 `ContentBlocks[].Toolresult.Content` 里，只改一份会让落盘和上 wire
// 各取一份、出现两份不一致。**所有** block 都要过一遍，不只 `[0]`。
//
// ⚠️ **`Annotations` / `Metadata` 深拷**：`hook.Hooks.OnModelStart` 的 `input.Messages`
// 是 [消费] 档——契约在**邀请**业务改它。浅拷的话业务打个 tag 就写穿到还挂在 channel 上
// 的那条原消息里（实测过）。同包另两个 clone 函数早就深拷了，这里不该例外。
func shrinkOversizedInPlace(messages []*schema.Message, gap int) []*schema.Message {
	type cand struct {
		idx  int
		size int
	}
	var cands []cand
	for i, m := range messages {
		if m == nil || m.Role != schema.RoleTool {
			continue
		}
		if len(m.Content) > emergencyShrinkKeepBytes {
			cands = append(cands, cand{i, len(m.Content)})
		}
	}
	if len(cands) == 0 {
		return messages
	}
	sort.Slice(cands, func(a, b int) bool { return cands[a].size > cands[b].size })
	if gap <= 0 {
		cands = cands[:1]
	}

	out := make([]*schema.Message, len(messages))
	copy(out, messages)
	freed := 0
	for _, c := range cands {
		m := out[c.idx]
		before := compose_v2.EstimateMessageTokens([]*schema.Message{m})
		out[c.idx] = shrinkOneToolResult(m)
		freed += before - compose_v2.EstimateMessageTokens([]*schema.Message{out[c.idx]})
		if gap > 0 && freed >= gap {
			break
		}
	}
	return out
}

// shrinkOneToolResult 造一条正文被挖空的副本——原消息一个字段都不动。
func shrinkOneToolResult(m *schema.Message) *schema.Message {
	c := *m
	// **真原始从 annotation 读，不是 len(m.Content)**：这条正文可能在源头闸那里已经被截过
	// 一刀，那时 `len` 是残长而不是原始。上一刀留下的中缝标记正好落在这一刀挖走的中段里
	// 被吞掉，只靠 `len` 算的话报出来的量偏小、**方向还正相反**（模型会以为内容基本完整）。
	// 见 `TruncatedOriginalBytesAnnotation`。
	orig, _ := annotationInt(m, TruncatedOriginalBytesAnnotation)
	if orig <= 0 {
		orig = len(m.Content)
	}
	c.Content = truncateMiddleKnownOriginal(m.Content, emergencyShrinkKeepBytes, orig)
	annot := make(map[string]any, len(m.Annotations)+1)
	for k, v := range m.Annotations {
		annot[k] = v
	}
	// 把真原始继续往下传——万一还有第三刀，它读到的仍是真原始而不是这一刀之后的残长。
	annot[TruncatedOriginalBytesAnnotation] = orig
	c.Annotations = annot
	if len(m.ContentBlocks) > 0 {
		blocks := make([]*schema.ContentBlock, len(m.ContentBlocks))
		copy(blocks, m.ContentBlocks)
		for i, b := range blocks {
			if b == nil || b.Toolresult == nil {
				continue
			}
			tr := *b.Toolresult
			tr.Content = c.Content
			nb := *b
			nb.Toolresult = &tr
			blocks[i] = &nb
		}
		c.ContentBlocks = blocks
	}
	return &c
}

// dropOrphanToolResults 丢掉「发起它的那条 assistant 已经被丢了」的 tool_result。
//
// **为什么按组丢之后还要再过这一道**：分组按配对不切组（调用还在等后面的结果时不切，见
// `compose_v2.GroupMessagesByAPIRound`），往返本身不会被丢组劈开；这一道兜的是配对认不出的情形（结果的 ID 对不上任何
// 调用、输入里本来就有的孤儿）。曾经分组在 system 旁边切开，`[user][assistant tool_call][system][assistant][tool_result]`
// 切成三组，丢掉最旧那组之后剩下一条孤儿 `tool_result`，真实 API 直接 400/422，而框架这边一声不吭。
//
// ⚠️ **别把这道校验换成「不许在工具轮中间插 system」那种前提**。那是个会漂的假设：
// 今天框架自己不插，业务、历史回放、以及任何一次分组规则调整都可能让它成立。查过的
// 六家里唯一结构上免疫的那家，靠的正是「按切点上未闭合的 tool_call 余额判安全」——
// 从计数推，不从分组推。这道校验是同一个思路的最小版：**丢完再数一遍**。
//
// 反方向（assistant 带 tool_call 但结果被丢）不在这里处理：那是「悬空 tool_call」，
// 由 `dropDanglingToolCalls` 在消息汇合处（preamble 拼 prior 时）统一兜底。
// ⚠️ **那一处不在本轮的 retry 循环里**——它只在下一次进 preamble 时跑（`basic_agent.go`
// 那句注释自己写着「in-Run 轮内配对走 channel 不经此函数」），所以本轮压缩造出来的悬空
// tool_call 这一轮不会被清掉。
func dropOrphanToolResults(msgs []*schema.Message) []*schema.Message {
	calls := make(map[string]bool)
	for _, m := range msgs {
		if m == nil {
			continue
		}
		for _, tc := range schema.GetToolCalls(m) {
			if tc.ID != "" {
				calls[tc.ID] = true
			}
		}
	}
	out := msgs[:0:0]
	dropped := 0
	for _, m := range msgs {
		if m != nil && m.Role == schema.RoleTool {
			if id := schema.GetToolResultID(m); id != "" && !calls[id] {
				dropped++
				continue
			}
		}
		out = append(out, m)
	}
	if dropped == 0 {
		return msgs
	}
	return out
}

// annotationInt 读一个整数标注。
//
// 不能只认 `int`：消息经过存档往返（JSON）之后，`map[string]any` 里的数字是 `float64`。
// 只按 `.(int)` 读的话，续跑之后这个值就读不到了——这里的后果是第二刀把残长当成原始长度，
// 报给模型的省略量偏小、方向相反（实测）。
func annotationInt(m *schema.Message, key string) (int, bool) {
	switch v := m.Annotations[key].(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case json.Number:
		n, err := v.Int64()
		return int(n), err == nil
	}
	return 0, false
}
