package agent_v2

// 会话历史的**分页读**——长会话不再每次 Run 都把整场历史拉一遍。
//
// # 为什么要有它
//
// 实测（2026-10-20，后端按 JSON 存取、只算反序列化不算网络）：默认配置下（MaxHistoryPairs=20）
// 喂给模型的 prior 恒为最近 40 来条，而每个 BasicAgent 每次 Run 都把整场会话读一遍——1000 轮、
// 三个 agent 的顺序编排，一次 Run 读回 3 万条、72MB，读历史占 Run 耗时 99%，读回来的东西随即被
// 截断扔掉 99%。读的量随会话长度线性涨，用的量是常数。
//
// # 读法
//
// 从会话末尾往前一页一页翻（[HistoryQuery] 的 Before + Limit），凑够这次要用的就停：
//
//   - prompt 的 prior（[historyPrior]）：凑够 MaxHistoryPairs*2 条视图消息，再多读一页——
//     切点要往前拉到声明 tool_call 的那条 assistant（见 compose_v2.AdjustIndexToPreserveAPIInvariants），
//     多读的那页是给它留的余量。「只看自己」的视图按归属过滤（[HistoryQuery.Agent]），只读自己的记录——
//     自己发言少的 agent 进一个长会话时不用把别人的话一页页翻过去。
//   - 「这条记过没有」类的去重（[findHistory]）：直接问后端——按消息 ID、任务章（[HistoryTaskAnnotation]）、
//     归属过滤（[HistoryQuery]）。这些去重问的是「这个任务里记过 X 没有」（续跑重跑到同一步、转交的下一跳、
//     边界补记）与「X 是不是已经在别的归属下记过」，答案只在后端那份记录里：曾经由框架往前翻页、靠任务的
//     起点章判「任务从哪开始」，起点章又靠 session 对象上的标记判「这个任务写过没有」——拿不到原 session
//     对象的入口（无状态服务按 RunID 重建 session、session 传 nil 由框架补）就退回猜，猜错时同一句话记两份、
//     放弃时漏补悬空调用（审计实测）。按条件直接查，读的量只跟命中的记录有关，也不依赖任何对象上的状态。
//
// 游标是后端铸的绝对位置：往前翻的过程中别的分支往末尾追加，不影响已经翻过的位置——不漏、不重。

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// HistoryCursor 是会话里一条消息的位置，由后端铸造、对框架不透明。
//
// 后端自己决定它装什么（下标、自增序号、偏移量），要满足两条：同一场会话里按写入顺序**严格递增**
// （后端自己能比较即可，框架不比较）；追加新消息**不改变**已有消息的游标。空串保留给「会话末尾」。
type HistoryCursor string

// HistoryQuery 圈定 [HistoryStore.Load] 读哪一段。**零值 = 整场会话。**
//
// 三个过滤条件（MessageID / Task / Agent）非空时都要满足（逐字节相等，判据见 [HistoryQuery.Matches]）；Before /
// Limit 作用在过滤之后的序列上，[HistoryPage.More] 也按满足条件的算。任务章与归属在消息的 Annotations 里（值是
// 字符串），后端可以在写入时抽出来建索引——框架的去重只按这几个条件查、命中即停，读的量取决于后端按条件读时
// 要碰多少条。
type HistoryQuery struct {
	// Before 非空时只读这个位置**之前**（不含）的消息；空串 = 从会话末尾起。
	// 取值只来自同一场会话的 [HistoryPage.Start]。
	Before HistoryCursor
	// Limit > 0 时只返 Before 之前**最近的** Limit 条（满足过滤条件的）；<= 0 不限。
	Limit int
	// MessageID 非空时只返 Message.ID 等于它的记录。
	MessageID string
	// Task 非空时只返任务章（Annotations[HistoryTaskAnnotation]）等于它的记录。
	Task string
	// Agent 非空时只返归属（Annotations[HistoryAgentAnnotation]）等于它的记录。
	Agent string
}

// Matches 报 m 满不满足 q 的过滤条件（MessageID / Task / Agent；Before / Limit 不在这里）。在进程里过滤的后端
// 直接用它，判据就跟框架的一致：任务章与归属按字符串比，值不是字符串的不命中；nil 不命中。
func (q HistoryQuery) Matches(m *schema.Message) bool {
	if m == nil {
		return false
	}
	if q.MessageID != "" && m.ID != q.MessageID {
		return false
	}
	if q.Task != "" {
		if t, _ := m.Annotations[HistoryTaskAnnotation].(string); t != q.Task {
			return false
		}
	}
	if q.Agent != "" {
		if a, _ := m.Annotations[HistoryAgentAnnotation].(string); a != q.Agent {
			return false
		}
	}
	return true
}

// HistoryPage 是 [HistoryStore.Load] 读回的一段。
type HistoryPage struct {
	// Messages 按写入顺序（旧在前）。
	Messages []*schema.Message
	// Start 是 Messages[0] 在会话里的位置——拿它当下一次的 [HistoryQuery.Before]（过滤条件不变）就接着往前读。
	// Messages 为空时为空串。
	Start HistoryCursor
	// More 报 Start 之前还有没有更早的（满足过滤条件的）消息。
	More bool
}

// LoadAllHistory 读出整场会话（按写入顺序）——给业务回看、导出、审计用。
//
// 框架自己的读不走它：prompt 只要最近一段，去重按条件查（只看这个任务这一路写下的），见本文件头。
func LoadAllHistory(ctx context.Context, hs HistoryStore, key HistoryKey) ([]*schema.Message, error) {
	if isFrameworkNil(hs) {
		return nil, fmt.Errorf("history: nil HistoryStore")
	}
	page, err := hs.Load(ctx, key, HistoryQuery{})
	if err != nil {
		return nil, err
	}
	return page.Messages, nil
}

// HistoryTaskAnnotation 是历史记录上盖的**任务章**——值是写下这条记录的那个任务的 RunID。
//
// 框架用它把「这个任务里记过没有」类的去重限定在本任务的记录上（见本文件头），业务回看时也可以拿它
// 把一场会话按任务（一次 Run 连同它的续跑）分段。值是不透明标识，别拿它当授权凭据——会话的归属看
// [HistoryKey.Owner]。
const HistoryTaskAnnotation = "crew_history_task"

// historyOriginAnnotation 盖在经 DetachTask 开的独立任务写下的记录上：值是这个独立任务脱离自哪些任务——sid 的
// JSON 数组编码成的**字符串**（见 encodeHistoryOrigin）。存成字符串跟任务章、归属章一样：只保得住字符串标注的
// 后端（把别的类型转成文本存）也认得出；存成 []string 时那样的后端读回一个文本，透传的产出被当成没记过、记两份（审计实测）。
const historyOriginAnnotation = "crew_history_origin"

// encodeHistoryOrigin 把脱离链编码成来源章的值：没有脱离时为空串（不盖章）。
func encodeHistoryOrigin(sids []string) string {
	list := make([]string, 0, len(sids))
	for _, s := range sids {
		if s != "" {
			list = append(list, s)
		}
	}
	if len(list) == 0 {
		return ""
	}
	// 字符串切片编码不会失败
	b, _ := json.Marshal(list)
	return string(b)
}

// historyRecordOfTask 报一条记录是不是任务 task 这一路写下的：任务章是它，或者是从它脱离出去的独立任务写的。
// task 为空（任务不明）时都算。
func historyRecordOfTask(m *schema.Message, task string) bool {
	if task == "" {
		return true
	}
	if m == nil || m.Annotations == nil {
		return false
	}
	if t, _ := m.Annotations[HistoryTaskAnnotation].(string); t == task {
		return true
	}
	raw, _ := m.Annotations[historyOriginAnnotation].(string)
	if raw == "" {
		return false
	}
	var origins []string
	if err := json.Unmarshal([]byte(raw), &origins); err != nil {
		return false // 不是框架写的来源章，当没有
	}
	return slices.Contains(origins, task)
}

// currentWriterTask 是这次调用写历史时盖的任务章：有写入器时是写入器的，没有时同 [currentHistoryTask]。
func currentWriterTask(ctx context.Context) string {
	if w := historyWriterFrom(ctx); w != nil {
		return w.task
	}
	return currentHistoryTask(ctx)
}

// currentHistoryTask 是这次调用所在的任务：session 的 RunID（任务的身份就是 RunID），没有 session
// 时回落 ctx 上的 sid。
func currentHistoryTask(ctx context.Context) string {
	if s := GetSession(ctx); s != nil && s.RunID != "" {
		return s.RunID
	}
	return compose_v2.GetSessionID(ctx)
}

// historyPageMin / historyPageMax 是往前翻页的页长：第一页至少 historyPageMin 条，之后每页翻倍，
// 封顶 historyPageMax——长会话里要找的东西越在前面，往前翻得越快；页长有上限，单次读的量就有上限。
const (
	historyPageMin = 64
	historyPageMax = 1024
)

// historySource 是一次读历史的去处。
type historySource struct {
	store HistoryStore
	key   HistoryKey
	task  string
}

// resolveHistorySource 定这次读历史去哪：**跟写的去处是同一个**。
//
// 嵌套时写入器沿用父的（见 startHistory），所以读也优先走 ctx 里那个写入器的 (store, key)。子 agent
// 自己配的 `WithHistory` 在嵌套位置上不生效——写侧本来就忽略它，读侧若还认自己的配置，就会去一个
// 从来没人往里写过的 store 上读，读到空、还报「读成功」压掉 events 回落，模型直接失忆。
//
// 读之前先等自己这条流的 pending 写落盘：写是异步的，同一个 Run 里嵌套调用时（父建了写入器、子沿用），
// 不等就会读到父那批还没落盘的空档。
//
// 被摘除的分支（内部决策 LLM）跟历史层完全无关、读写两侧都是，返 false 让它跟「没配历史」表现一致。
func resolveHistorySource(ctx context.Context, hc *historyConfig) (historySource, bool) {
	if historySuppressed(ctx) {
		return historySource{}, false
	}
	if w := historyWriterFrom(ctx); w != nil {
		w.waitDrained(ctx)
		return historySource{store: w.store, key: w.key, task: w.task}, true
	}
	if hc == nil || hc.store == nil {
		return historySource{}, false
	}
	key, ok := historyKeyFrom(ctx, hc)
	if !ok {
		return historySource{}, false
	}
	return historySource{store: hc.store, key: key, task: currentHistoryTask(ctx)}, true
}

// errHistoryCursorStuck：后端报「前面还有」却给不出能往前走的游标——再翻就是死循环。
var errHistoryCursorStuck = fmt.Errorf("history: backend reported More but the cursor did not move")

// scanBackward 从会话末尾往前一页一页读，逐条（新 → 旧）交给 visit；visit 返 true 立即停。
// afterPage 在每页读完之后调，返 true 也停（nil 表示一直读到会话开头）。
//
// 后端违约（报 More 却给出空页或原地不动的游标）时返错，不死循环。
//
// grow 为 true 时页长逐页翻倍（封顶 historyPageMax）：要找的东西越在前面翻得越快。filter 的过滤条件带进每一页
// （Before / Limit 由这里定）。
//
// 返回的记录再按 filter 核一遍，不满足的不交给 visit：后端没实现过滤条件时（照旧接口写的，新字段被忽略），
// 框架拿到的是整页原样——不核的话「这个任务里记过这条没有」把任何一条记录都当命中，每条输入都判成记过、一条
// 不记。核了之后只是慢（按条件查退化成往前翻整场会话），并告警一次。
func (s historySource) scanBackward(ctx context.Context, filter HistoryQuery, firstPage int, grow bool, visit func(m *schema.Message) bool, afterPage func() bool) error {
	if firstPage < historyPageMin {
		firstPage = historyPageMin
	}
	filtered := filter.MessageID != "" || filter.Task != "" || filter.Agent != ""
	before := HistoryCursor("")
	size := firstPage
	for {
		q := filter
		q.Before, q.Limit = before, size
		page, err := s.store.Load(ctx, s.key, q)
		if err != nil {
			return err
		}
		for i := len(page.Messages) - 1; i >= 0; i-- {
			m := page.Messages[i]
			if m == nil {
				continue
			}
			if filtered && !filter.Matches(m) {
				warnUnfilteredHistoryBackend(ctx, s.store)
				continue
			}
			if visit(m) {
				return nil
			}
		}
		if !page.More {
			return nil
		}
		if len(page.Messages) == 0 || page.Start == "" || page.Start == before {
			return errHistoryCursorStuck
		}
		if afterPage != nil && afterPage() {
			return nil
		}
		before = page.Start
		if grow && size < historyPageMax {
			size *= 2
			if size > historyPageMax {
				size = historyPageMax
			}
		}
	}
}

// unfilteredBackendWarned：「后端没按条件过滤」只告警一次（每个进程）——它是后端实现的问题，每次读都报会刷屏。
var unfilteredBackendWarned atomic.Bool

func warnUnfilteredHistoryBackend(ctx context.Context, hs HistoryStore) {
	if unfilteredBackendWarned.Swap(true) {
		return
	}
	slog.WarnContext(ctx, "agent_v2: 会话历史后端没按 HistoryQuery 的过滤条件（MessageID / Task / Agent）读，"+
		"框架逐条核对、结果照旧正确，但按条件查退化成往前翻整场会话——后端请实现过滤条件"+
		"（跑 agent_v2/history_conformance.RunHistoryStoreTests 验证）",
		"backend", fmt.Sprintf("%T", hs))
}

// historyPrior 从历史层取出本 agent 该看到的跨 Run 历史（按写入顺序），投影成三种 handoff 视图之一。
// 第二个返回值报「历史层是否接管了这次取历史」——false 时调用方回落到 session events。
//
// need > 0 时只读**最近一段**：读到第 0 层截断（[truncatePriorPreservingMarkers]）在这一段上的切点
// 落在段内为止——凑够 need 条不带 skill 正文标记的视图消息，并且切点往前对齐 tool_call 配对之后
// 仍没碰到这一段的开头（见 [priorWindowSatisfied]）。在这一段上截与在整个视图上截结果相同（只差
// 更早激活的 skill 正文，见下）。need <= 0 读到会话开头（调用方明确不截断时）。
//
// ⚠️ 只读最近一段有一个可见的语义变化：**比这一段更早激活的 skill 正文不再进 prior**。第 0 层截断
// 原先会把整场会话里所有激活过的 skill 正文都保下来（随激活次数线性增长、没有上限）；现在保的是读到的
// 这一段里的。更早的那个 skill 模型从 skill 列表里重新激活即可。
//
// # 为什么要投影而不是原样返回
//
// 历史是**按会话**存的：同一场对话里所有 agent 的消息都在一条流上，靠 HistoryAgentAnnotation
// 区分谁说的。每个 agent 该看到什么，规则跟 session events 那套完全一样——自己的原样看，别人的按
// handoff 模式决定隐藏 / 改写 / 保留。这里复用的正是同一组改写函数，只是把「事件的 AgentName」
// 换成了「消息上的归属标注」。
func historyPrior(ctx context.Context, hc *historyConfig, ownerAgentName string, mode HandoffHistoryMode, need int) ([]*schema.Message, bool) {
	// 调用方提供了前文、这次运行却没有能以服务端为准的写入器：以客户端为准，历史层这次不读（见 caller_history.go）。
	// 报 false 让调用方回落到 session events，前文排在最前
	if _, declared := callerPrior(ctx); declared {
		if w := historyWriterFrom(ctx); w == nil || w.clientPrior {
			return nil, false
		}
	}
	src, ok := resolveHistorySource(ctx, hc)
	if !ok {
		return nil, false
	}
	var rev []*schema.Message     // 新 → 旧
	var base []*schema.Message    // 遇到的本 agent、同视图的压缩边界里的替代历史
	var callers []*schema.Message // 写进历史层的前文（新 → 旧），见 callerFirst
	counted := 0
	satisfied := false // 读够了：这一段上的第 0 层截断已经跟整个视图上的一样
	first := historyPageMin
	if need > 0 && 2*need > first {
		first = 2 * need
	}
	// 「只看自己」的视图就是归属为自己的那些记录（别人的一概不进视图），直接让后端按归属过滤
	var filter HistoryQuery
	if mode == HandoffHistorySelfOnly {
		filter.Agent = ownerAgentName
	}
	err := src.scanBackward(ctx, filter, first, true, func(m *schema.Message) bool {
		if isCompactionBoundary(m) {
			// 本 agent、同视图的最新一条边界：它之前的都已由它的替代历史代表，停。别的 agent 的、别的视图
			// 的，或者解不出来的，当它不存在（见 compaction_boundary.go）。边界本身不是谁说的话，不进任何
			// 视图——这里是唯一一处把它挡在视图外的地方。
			if owner, _ := m.Annotations[HistoryAgentAnnotation].(string); owner == ownerAgentName {
				if r, ok := decodeCompactionBoundary(m, mode); ok {
					base = r
					return true
				}
			}
			return false
		}
		if isCallerHistory(m) {
			// 前文不是哪个 agent 说的、不投影，最后去重排在最前（同 callerFirst）
			callers = append(callers, m)
			return false
		}
		v := projectHistoryForView(m, ownerAgentName, mode)
		if v == nil {
			return false
		}
		rev = append(rev, v)
		if !isPriorProtected(v) {
			counted++
		}
		return false
	}, func() bool {
		if need <= 0 || counted <= need {
			return false
		}
		satisfied = priorWindowSatisfied(reversed(rev), need)
		return satisfied
	})
	if err != nil {
		// 读失败跟写失败同档：吞 + warn 不打断 Run（让一次后端抖动把整个 Run 打挂，比让模型少看
		// 几条历史严重得多），但**必须**报 false 让调用方真的回落到 events——否则这条 warn 就是在
		// 承诺一件没发生的事。
		slog.WarnContext(ctx, "agent_v2: 读会话历史失败，本轮回落到 session events（内容残缺）",
			"key", src.key.String(), "err", err)
		return nil, false
	}
	if base != nil {
		// 前文已在边界的替代历史里（摘要那一刻它在消息流里），边界之后读到的只可能是并发写入器晚写的重复
		return append(base, reversed(rev)...), true
	}
	if satisfied {
		// 前文是这场对话最早的记录，排在视图最前：这一段已经读够、截断会把它们连同更早的一起截掉
		return reversed(rev), true
	}
	if mode == HandoffHistorySelfOnly {
		// 按归属读时前文不在里面（它的归属是调用方）。这一段没读够说明截断留得下它们，另按前文的归属查一次——
		// 只取回前文那几条，跟会话多长无关（见 callerHistoryOwner）
		callers = callers[:0]
		err = src.scanBackward(ctx, HistoryQuery{Agent: callerHistoryOwner}, historyPageMin, true, func(m *schema.Message) bool {
			if isCallerHistory(m) {
				callers = append(callers, m)
			}
			return false
		}, nil)
		if err != nil {
			slog.WarnContext(ctx, "agent_v2: 读会话历史失败，本轮回落到 session events（内容残缺）",
				"key", src.key.String(), "err", err)
			return nil, false
		}
	}
	return append(dedupCallers(reversed(callers)), reversed(rev)...), true
}

// dedupCallers 按 ID 给写进历史层的前文去重（按写入顺序，留最早那份）：几个写入器同时写过时有不止一份，ID 相同
// （见 seedCallerPrior）。
func dedupCallers(callers []*schema.Message) []*schema.Message {
	if len(callers) == 0 {
		return nil
	}
	out := make([]*schema.Message, 0, len(callers))
	seen := make(map[string]bool, len(callers))
	for _, m := range callers {
		if m.ID != "" {
			if seen[m.ID] {
				continue
			}
			seen[m.ID] = true
		}
		out = append(out, m)
	}
	return out
}

// priorWindowSatisfied 报：在 win（按写入顺序）上做第 0 层截断，结果是不是跟在整个视图上截一样——
// 两个条件都要满足：
//
//   - win 里最早那条不受保护的消息被截掉了：切点落在 win 内部，往前对齐（同 RunID 块连续）时没碰到
//     win 的开头；
//   - 截出来的结果里每条 tool_result 都找得到声明它的 assistant：切点所在那组调用的 assistant 若在
//     win 之外，在 win 上截会把那几条 tool_result 当孤儿留下，而在整个视图上截会把切点拉回到那条
//     assistant——两边不一样，得再往前读；
//   - win 里每条带 skill 正文标记的 tool_result 都在 win 里找得到声明者：第 0 层截断把这样的一对
//     成对保下（不论多早），win 的开头若恰好切在这一对中间，在 win 上截会把那条正文当孤儿截掉。
func priorWindowSatisfied(win []*schema.Message, need int) bool {
	inWin := make(map[string]bool)
	for _, m := range win {
		for _, tc := range schema.GetToolCalls(m) {
			inWin[tc.ID] = true
		}
	}
	for _, m := range win {
		if m.Role == schema.RoleTool && hasSkillMarker(m) && !inWin[schema.GetToolResultID(m)] {
			return false
		}
	}
	// （下面「最早那条不受保护的」与第 0 层截断用同一个保护判据 isPriorProtected。）
	kept := truncatePriorPreservingMarkers(win, need)
	in := make(map[*schema.Message]bool, len(kept))
	declared := make(map[string]bool)
	for _, m := range kept {
		in[m] = true
		for _, tc := range schema.GetToolCalls(m) {
			declared[tc.ID] = true
		}
	}
	for _, m := range kept {
		if id := schema.GetToolResultID(m); m.Role == schema.RoleTool && id != "" && !declared[id] {
			return false
		}
	}
	for _, m := range win {
		if isPriorProtected(m) {
			continue
		}
		return !in[m]
	}
	return false
}

// reversed 返回 rev 倒过来的新切片。
func reversed(rev []*schema.Message) []*schema.Message {
	out := make([]*schema.Message, len(rev))
	for i, m := range rev {
		out[len(rev)-1-i] = m
	}
	return out
}

// projectHistoryForView 把一条历史记录投影进 ownerAgentName 的视图：自己的原样；别人的按 mode
// 保留（Raw）、改写成第三人称（Rewrite）或隐藏（SelfOnly）。隐藏 / 改写后没有内容时返 nil。
func projectHistoryForView(m *schema.Message, ownerAgentName string, mode HandoffHistoryMode) *schema.Message {
	if m == nil {
		return nil
	}
	owner, _ := m.Annotations[HistoryAgentAnnotation].(string)
	if owner == ownerAgentName {
		return m
	}
	switch mode {
	case HandoffHistoryRaw:
		return markFromAgent(m, historyDisplayName(m))
	case HandoffHistoryRewrite:
		// 改写文案用**显示名**（agent 自己的 Name），不用位置——`[br1] said:` 模型看不懂
		return rewriteCrossAgentMessage(m, historyDisplayName(m))
	default: // HandoffHistorySelfOnly——别人的一概不看
		return nil
	}
}

// findHistory 在这场会话的历史里按条件找记录：后端按 q 过滤，逐条（新 → 旧，分页）交给 match，命中就返回 true。own 为
// true 时只看当前任务写下的（q.Task 取当前任务；任务不明——没有 session 也没有 sid——时看整场会话）。第二个
// 返回值报历史层是否接管了这次读（没开历史 / 读失败时为 false）。
//
// 读的量是命中条件的那些记录：按消息 ID 查时只有那几条，按任务查时是这个任务写下的——跟会话多长、中间夹了多少
// 别的任务的记录无关，也不依赖 session 对象上的任何状态（见本文件头）。
func findHistory(ctx context.Context, hc *historyConfig, q HistoryQuery, own bool, match func(m *schema.Message) bool) (bool, bool) {
	src, ok := resolveHistorySource(ctx, hc)
	if !ok {
		return false, false
	}
	if own {
		q.Task = src.task
	}
	hit := false
	if err := src.scanBackward(ctx, q, historyPageMin, true, func(m *schema.Message) bool {
		hit = match(m)
		return hit
	}, nil); err != nil {
		slog.WarnContext(ctx, "agent_v2: 读会话历史失败，按「这个任务里没记过」处理",
			"key", src.key.String(), "err", err)
		return false, false
	}
	return hit, true
}

// anyHistoryRecord 是「命中过滤条件就算」的 match（给 [findHistory]）。
func anyHistoryRecord(*schema.Message) bool { return true }
