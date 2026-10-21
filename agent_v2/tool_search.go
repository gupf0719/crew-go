package agent_v2

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"crew-go/schema"
	"crew-go/skill"
	"crew-go/tool"
)

// ToolSearchConfig 配置**工具级渐进披露**。工具多（尤其 MCP Hub 聚合多 server）时全量 schema
// 喂 LLM 爆 context——开启后 deferred 工具初始**不进 LLM 的 bind list**，LLM 必须先调 tool_search
// （检索）才揭示对应工具。揭示集在一个任务内累积（一次 Run 连同它的续跑），存在这个 agent 自己的图状态里
// （随存档走）：下一个任务从空开始，并行分支、嵌套的被调方各有各的，同名 agent 也不共享；会话 KV 上不留东西。
// 一批里检索解锁的工具从下一批起可调（下一轮 bind list 才有它）。
//
// 机制：meta-tool tool_search + 过滤 bind list（未揭示工具不喂 LLM，靠物理过滤而非 prompt 自觉）；
// 检索器默认关键词（复用 skill.KeywordScore，零向量依赖），可插拔语义检索（如包装 store.Store.Search）。
type ToolSearchConfig struct {
	// Enabled 总开关。
	Enabled bool

	// Deferred 判定哪个工具延迟披露（true=延迟）。常用 `mcp.IsRemoteTool` 延迟所有 MCP 工具。
	// nil 或返回全 false = 没有工具被延迟（等于关闭效果）。
	Deferred func(tool.Tool) bool

	// Retriever 检索器：给 query + 候选（被 Deferred 的）工具，返回匹配的工具名。候选以外的名字与
	// 重复的名字框架会丢掉（包装共享索引、在全量工具上检索时常会返回这些）；已经揭示过的照常回给
	// 模型，但不算新解锁。nil = 默认关键词检索（skill.KeywordScore over name+description）。业务可插
	// 语义检索（如包装 store.Store.Search）。
	Retriever func(ctx context.Context, query string, candidates []tool.Tool) []string

	// MaxResults 默认关键词检索单次最多揭示几个工具（默认 5）。自定义 Retriever 时忽略——返回几个由检索器自己
	// 决定（同 MinScore）。
	MaxResults int

	// MinScore 默认关键词检索的最低分阈值 [0,1]（默认 0=只要命中即算）。自定义 Retriever 时忽略。
	MinScore float64
}

const (
	toolSearchToolName = "tool_search"

	defaultToolSearchMaxResults = 5
)

// revealedNames 是一个只读的已解锁工具名集，从 BasicAgent 图的 basicRevealedCh（已提交的解锁集，随存档走）建：model 节点
// 拿它决定 bind list，tools 节点拿它当这一批的执行闸。
//
// **已解锁集只活在任务自己的状态里**：权威且唯一的副本是那个 channel；tools 节点这一批里检索解锁的名字先记在这次执行的
// [revealTurn] 里，节点成功后写进 channel。曾经会话 KV 上另有一份按调用流分键的缓存，它跟 channel 是两份拷贝：换任务后
// 不带原 session 续跑时两份分叉、原本能调的工具被静默拦下；它活得比任务长，上一个任务解锁的工具下一个任务直接能调、
// 会话 KV 随跑过的任务数增长（均审计实测）。去掉那份之后这几件事整类不存在——续跑带不带原 session 都只读 channel，
// 新任务的图从空的 channel 开始，会话 KV 上什么都不留；同名并行分支、嵌套的被调方各跑各的图，各有各的 channel。
//
// ⚠️ **这道 fail-closed 闸门是上下文预算机制，不是安全边界。**
//
// 它防的是模型幻觉调用没检索过的工具（省 bind list、省上下文），不防业务——可达集
// 由业务自己的 deferred 配置封顶，绕过它也只能拿到业务本来就注册了的东西。
//
// 别在它上面建授权。真要限制某个 agent 能调什么，那是 toolkit 装配和工具审批
// （[WithApprovalMode]）的事。
//
// # 为什么已解锁集不从消息历史重建
//
// 试过，撤了。历史重建看着很美（历史本来就跨 checkpoint 走，不用第二份持久化），
// 但它把「谁能解锁工具」这个问题的答案从**框架**手里交给了**消息内容**，而消息内容
// 有两个来源不受控：
//
//   - **外部数据**：MCP 远程工具的返回值、抓来的网页、读进来的文件，全都是外部可控
//     文本。实测一件普通业务工具返回一段以解锁表头开头的文字，就让从没检索过的工具
//     进了 bind list 并被执行——这是一条 prompt-injection 面。
//   - **别人的消息**：转交的 Raw 历史视图会把上一个 agent 的工具消息原样交给下一个，
//     于是 A 检索出来的解锁被 B 白拿（实测 B 从没调过 tool_search 却拿到了）。而
//     揭示集本来是按调用流隔离的（每个 BasicAgent 图一份 channel），历史扫描绕过了它。
//
// 用 tool_result 的 annotation 承载也不行——那半会被压缩层切掉、被任何从 wire 重建
// 消息的地方掉包（历史层 round-trip / a2a / AG-UI / 业务自实现的 HistoryStore）。
//
// 所以权威且唯一的载体是 channel state（见 basicRevealedCh）：它随 checkpoint 走、
// 跨轮累积、不受消息重写影响，而且**只有框架写得进**。业务自实现的 checkpointer 漏了
// 这个 channel 会退化成「揭示集为空」——那由 `checkpointer_conformance` 的 Channels
// round-trip 用例负责挡住，不该用一条能被外部数据撬开的兜底去补。
type revealedNames map[string]bool

func newRevealedNames(names []string) revealedNames {
	s := make(revealedNames, len(names))
	for _, n := range names {
		s[n] = true
	}
	return s
}

// revealedChKey 是 ctx-value key——model 节点与 tools 节点把 channel 里已提交的解锁集读出来放进 ctx，modelFn 与
// toolsFn 据它建 [revealedNames]。
//
// 为什么绕这一道：channel 的读写口只在节点函数手上，而 [BasicAgentModelFn] / [BasicAgentToolFn] 的签名拿不到 channel。
// 改那个签名是破坏性的（导出类型，业务自定义 modelFn 也在用），而这个值本来就是「本次调用的环境」，走 ctx 正合适。
type revealedChKey struct{}

func withRevealedFromChannel(ctx context.Context, names []string) context.Context {
	return context.WithValue(ctx, revealedChKey{}, names)
}

func revealedFromChannelCtx(ctx context.Context) []string {
	names, _ := ctx.Value(revealedChKey{}).([]string)
	return names
}

// revealTurn 记 tools 节点这一次执行里 tool_search 解锁了哪些名字。工具体拿不到 channel 写入口，记在这里，节点成功后
// 由 tools 节点写进 basicRevealedCh；节点失败或中断时随这次执行一起丢掉，续跑重跑这一批时检索重新记一遍（tool_search
// 不进工具账）。一批里几个 tool_search 并发跑（N≥2 走 goroutine），所以带锁。
type revealTurn struct {
	mu    sync.Mutex
	names []string
}

func (t *revealTurn) add(names ...string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.names = append(t.names, names...)
}

func (t *revealTurn) list() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.names...)
}

// revealTurnKey 把 [revealTurn] 交给这一批的工具体。它是执行状态：钩子返回别处存下的 ctx 时，采纳取框架这次的。
type revealTurnKey struct{}

func withRevealTurn(ctx context.Context, t *revealTurn) context.Context {
	return context.WithValue(ctx, revealTurnKey{}, t)
}

func revealTurnFrom(ctx context.Context) *revealTurn {
	t, _ := ctx.Value(revealTurnKey{}).(*revealTurn)
	return t
}

// splitCandidates 把检索器返回的名字分成候选（被延迟的工具）里的与候选以外的两份，各自去重、保序。检索器是业务
// 可插的，返回候选以外的名字时，那些名字要么不存在、要么本来就可见：揭示它们没有意义，回给模型的「已解锁」清单会
// 列出调不了的工具，还会让「这一批解锁了新工具」误判成立（审计实测：检索器每次吐一个没见过的名字，强制额度一直
// 不被消耗）。只揭示前一份；后一份只在什么都没解锁时回给模型看。
func splitCandidates(names []string, candidates []tool.Tool) (in, others []string) {
	allowed := make(map[string]bool, len(candidates))
	for _, c := range candidates {
		allowed[c.Name()] = true
	}
	seen := make(map[string]bool, len(names))
	for _, n := range names {
		if seen[n] {
			continue
		}
		seen[n] = true
		if allowed[n] {
			in = append(in, n)
		} else {
			others = append(others, n)
		}
	}
	return in, others
}

// newlyRevealed 从这一批检索解锁的名字里挑出之前没解锁过的，去重、保持先后。只有这些写进 basicRevealedCh：
// 检索到早已解锁的工具（检索器照常会返回它们）、同一批两次检索命中同一个工具，都不该让已解锁集里多一份。
// 两边都只有几个名字。
func newlyRevealed(names, have []string) []string {
	var out []string
	for _, n := range names {
		if !slices.Contains(have, n) && !slices.Contains(out, n) {
			out = append(out, n)
		}
	}
	return out
}

// isToolDeferred 判定一个工具是否被延迟披露（cfg 启用 + Deferred 谓词命中）。
func (c *ToolSearchConfig) isToolDeferred(t tool.Tool) bool {
	return c != nil && c.Enabled && c.Deferred != nil && c.Deferred(t)
}

// collectDeferred 从 toolkit 挑出所有被延迟的工具。
func (c *ToolSearchConfig) collectDeferred(toolkit *tool.Toolkit) []tool.Tool {
	if toolkit == nil {
		return nil
	}
	var out []tool.Tool
	for _, t := range toolkit.List() {
		// 框架铸的工具（final_output/ask_user/transfer_to_agent/route_to/...）不参与渐进披露——
		// 被 Deferred 谓词命中而延迟会让 filterVisibleTools 把它们从 bind list 剔除 → 结构化
		// 输出 / HITL / 转交静默失效。恒保留（与 filterToolsByName / 审批的豁免一致，都按实例判，
		// 见 frameworkMintedTool）。
		//
		// tool_search 这个**名字**另算：框架那件在本函数之后才注册（它拿本函数的结果当入参），
		// 业务同名那件届时会被它顶掉——不能进延迟名单，否则名单按名字把框架那件从 bind list
		// 里滤掉、模型永远调不了它（死锁）。这不是按名字认框架工具，是这个名字马上要被框架占用。
		if isFrameworkMinted(t) || t.Name() == toolSearchToolName {
			continue
		}
		if c.isToolDeferred(t) {
			out = append(out, t)
		}
	}
	return out
}

// filterVisibleTools 过滤 ToolInfos：保留「非延迟」+「已揭示的延迟工具」，丢弃「未揭示的延迟
// 工具」。这是工具级渐进披露的**核心强制**（业内统一：未揭示工具不进 LLM bind list）。
func filterVisibleTools(infos []*schema.ToolInfo, deferredNames map[string]bool, revealed revealedNames) []*schema.ToolInfo {
	if len(deferredNames) == 0 {
		return infos
	}
	out := make([]*schema.ToolInfo, 0, len(infos))
	for _, info := range infos {
		if !deferredNames[info.Name] { // 非延迟工具：恒可见
			out = append(out, info)
			continue
		}
		if revealed[info.Name] { // 已揭示的延迟工具：可见
			out = append(out, info)
		}
		// 未揭示的延迟工具：丢弃（不进 bind list）
	}
	return out
}

// filterToolsByName 供 BasicAgent 的 ToolFilter 裁剪本轮可见工具集：保留白名单 names 命中
// 的**业务工具** + **所有框架铸的工具**（tk 里这个名字下挂的那件带 frameworkMintedTool 标记，
// 与审批层豁免一致），其余业务工具裁掉（保持原顺序）。names 为空 = 裁掉全部业务工具、只留框架
// 工具；names 里不存在的名忽略。框架工具（ask_user/activate_skill/tool_search/route_to/...）
// 不受 names 影响——防业务白名单漏带导致搜索/技能/问用户/路由静默失效（footgun）。按实例判、
// 不按名字：业务工具恰好叫 final_output / respond 时照样受白名单约束。
func filterToolsByName(infos []*schema.ToolInfo, names []string, tk *tool.Toolkit) []*schema.ToolInfo {
	allow := make(map[string]bool, len(names))
	for _, n := range names {
		allow[n] = true
	}
	out := make([]*schema.ToolInfo, 0, len(infos))
	for _, info := range infos {
		if info == nil {
			continue
		}
		if allow[info.Name] || isFrameworkMintedIn(tk, info.Name) {
			out = append(out, info)
		}
	}
	return out
}

// defaultToolRetriever 默认关键词检索：对每个候选工具用 skill.KeywordScore(query, name+" "+desc)
// 评分，过 MinScore 后按分降序取前 MaxResults。
func defaultToolRetriever(minScore float64, maxResults int) func(context.Context, string, []tool.Tool) []string {
	return func(_ context.Context, query string, candidates []tool.Tool) []string {
		type scored struct {
			name  string
			score float64
		}
		var hits []scored
		for _, t := range candidates {
			score := skill.KeywordScore(query, t.Name()+" "+t.Description())
			if score > minScore || (minScore == 0 && score > 0) {
				hits = append(hits, scored{t.Name(), score})
			}
		}
		sort.Slice(hits, func(i, j int) bool {
			if hits[i].score != hits[j].score {
				return hits[i].score > hits[j].score
			}
			return hits[i].name < hits[j].name
		})
		if len(hits) > maxResults {
			hits = hits[:maxResults]
		}
		out := make([]string, len(hits))
		for i, h := range hits {
			out[i] = h.name
		}
		return out
	}
}

// toolSearchTool 是框架内置的 tool_search meta-tool。LLM 调它（传 query）→ 检索 deferred 工具 → 把匹配的记进
// 这一批的解锁记录（[revealTurn]，自带锁，并发 fanout 安全；tools 节点成功后写进 basicRevealedCh，下一轮 modelFn
// 过滤时这些工具进 bind list）→ 返回解锁了哪些工具给 LLM。
type toolSearchTool struct {
	cfg       *ToolSearchConfig
	deferred  []tool.Tool // 候选（被延迟的工具）
	retriever func(context.Context, string, []tool.Tool) []string
}

func newToolSearchTool(cfg *ToolSearchConfig, deferred []tool.Tool) *toolSearchTool {
	retriever := cfg.Retriever
	if retriever == nil {
		maxR := cfg.MaxResults
		if maxR <= 0 {
			maxR = defaultToolSearchMaxResults
		}
		retriever = defaultToolRetriever(cfg.MinScore, maxR)
	}
	return &toolSearchTool{cfg: cfg, deferred: deferred, retriever: retriever}
}

func (t *toolSearchTool) Name() string { return toolSearchToolName }

func (t *toolSearchTool) Description() string {
	return "按关键词检索并解锁工具。工具库很大、所需工具未在当前可见列表时**必须先调本工具**" +
		"（传 query 描述你要做的事），拿到解锁的工具后再调它们。"
}

func (t *toolSearchTool) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{
				"type":        "string",
				"description": "描述你要完成的任务 / 需要的工具能力，用于检索匹配的工具。",
			},
		},
		"required": []any{"query"},
	}
}

func (t *toolSearchTool) Call(ctx context.Context, args map[string]any) (*tool.Result, error) {
	query, _ := args["query"].(string)
	if strings.TrimSpace(query) == "" {
		return tool.Failure(tool.ErrCodeInvalidArguments, "tool_search 需要非空 query"), nil
	}
	turn := revealTurnFrom(ctx)
	if turn == nil {
		return tool.Failure(tool.ErrCodeInternal, "tool_search: 不在 BasicAgent 的 tools 节点里（框架内部错误）"), nil
	}
	matched, others := splitCandidates(t.retriever(ctx, query, t.deferred), t.deferred)
	if len(matched) == 0 {
		if len(others) > 0 {
			// 检索器返回的都不是要解锁的工具（共享索引、在全量工具上检索时常见）：多半本来就能直接调。说「可能不存在」
			// 会诱导模型放弃（审计实测：检索器只返回可见的工具时回的就是那句）。
			return tool.Success(fmt.Sprintf("检索到的工具不需要解锁：%s。在你可用工具列表里的直接调用；不在列表里的说明没有这个工具。",
				strings.Join(others, "、"))), nil
		}
		return tool.Success(fmt.Sprintf("没有匹配「%s」的工具。换个 query 再试，或所需能力可能不存在。", query)), nil
	}
	// 记进这一批的解锁记录，tools 节点成功后写进 basicRevealedCh（工具体拿不到 channel 写入口）。这一批里的
	// 执行闸不认它：闸只认这一批开始前已提交的解锁集（见 revealedNames）。
	turn.add(matched...)
	// 把解锁的工具名 + 描述回给 LLM，让它知道从下一轮起能调哪些。
	var b strings.Builder
	fmt.Fprintf(&b, "已解锁 %d 个工具，现在可直接调用：\n", len(matched))
	byName := make(map[string]tool.Tool, len(t.deferred))
	for _, dt := range t.deferred {
		byName[dt.Name()] = dt
	}
	for _, name := range matched {
		if dt, ok := byName[name]; ok {
			fmt.Fprintf(&b, "- %s：%s\n", name, dt.Description())
		} else {
			fmt.Fprintf(&b, "- %s\n", name)
		}
	}
	return tool.Success(b.String()), nil
}
