package agent_v2

import (
	"context"
	"encoding/json"
	"strings"

	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// AgentAsToolOption 配置 AgentAsTool 包装 (#7 F1+F5 fix).
type AgentAsToolOption func(*agentToolConfig)

type agentToolConfig struct {
	description       string
	customInputSchema map[string]any
	fullHistoryInput  bool
	callKey           func(args map[string]any) string
}

// WithAgentToolDescription override agent tool 的 description.
//
// 不传时默认从 `agent.Description()` 取——让业务**一处定义复用**
// (agent.Description() 同时供 LLMPlanner 用), 不再业务侧 caller 手动 wrap.
//
// **空 description 行为**: agent.Description() 空 + 业务也没传此 option → AgentAsTool
// 构造期 **panic** (agent tool 要求非空 Description). 业务 init 期发现, 不延迟到
// runtime LLM 调 tool 时.
func WithAgentToolDescription(description string) AgentAsToolOption {
	return func(c *agentToolConfig) { c.description = description }
}

// WithAgentToolInputSchema 业务自定义 sub-agent 输入 schema (默认 `{input: string}`).
//
// **why**: 业务想让 sub-agent 接受结构化输入 (如 `{query: string,
// max_results: int, format: string}`) 时不需要业务侧再 wrap 一层. caller 传定制
// schema, framework Call 时把 args 整体 marshal JSON 传给 sub-agent input.Content.
//
// **⚠️ 业务必在 sub-agent system prompt 提示 LLM "你将收到 JSON 格式参数"**——否则
// LLM 把 JSON 字符串当字面 user text 解读 silent 行为偏差. 范例:
//
//	subAgent := NewBasicAgent(
//	    WithName("search"),
//	    WithSystemPrompt("你将收到 JSON 格式参数, 格式: {\"query\": ..., \"top_k\": ...}.\n" +
//	                     "解析 JSON 后用 query 调 search tool, 取 top_k 个结果."),
//	    WithModel(m),
//	)
//	tool := AgentAsTool(subAgent, WithAgentToolInputSchema(map[string]any{
//	    "type": "object",
//	    "properties": map[string]any{
//	        "query": map[string]any{"type": "string"},
//	        "top_k": map[string]any{"type": "integer"},
//	    },
//	    "required": []string{"query"},
//	}))
//
// 与 WithAgentToolFullHistoryInput 互斥, 同时设时 schema 优先 (history 模式忽略).
func WithAgentToolInputSchema(schema map[string]any) AgentAsToolOption {
	return func(c *agentToolConfig) { c.customInputSchema = schema }
}

// WithAgentToolFullHistoryInput 用父 session 完整对话历史作 input.
//
// **why**: 业务想让 sub-agent 看父对话上下文 (e.g., ReflectAgent 评估 actor 输出
// 需看完整对话) 时不需要业务侧手动拼装. framework Call 时从 parentSess
// `HistoryFor("")` 取所有 messages 拼成 single user message 传给 sub-agent.
//
// 与 WithAgentToolInputSchema 互斥, 同时设时 schema 优先 (history 模式忽略).
func WithAgentToolFullHistoryInput() AgentAsToolOption {
	return func(c *agentToolConfig) { c.fullHistoryInput = true }
}

// WithAgentToolCallKey 声明每次调用的身份：从 args 里取一个跨续跑不变、又能区分不同调用的值
// （订单号、工单号……），效果同 [WithCallKey]。返回空串时按默认规则（按输入内容定身份）。
//
// **被注入的值落进了交给被调方的输入时必须给**：WithToolMiddleware 的中间件往 args 里塞每次都变的值
// （trace_id、时间、请求号），而输入取的正是那部分——自定义 schema 下整个 args 都序列化进输入；默认
// schema 下只有改写了 `input` 字段的文本本身才算（往别的字段注入不影响）。中间件每次执行都跑，那样每次
// 续跑算出的输入都不同，按内容定身份就是一次新调用：被调方从头跑、换一个 ID 重问，已完成的副作用重放，
// 永远收敛不了（实测续跑 4 轮仍在问）。取的值要**不含**被注入的那部分。`OnToolStart` 的注入不在此列：
// 一批调用的内容在第一次交给工具执行时定下、续跑沿用，它注入的值续跑不变（不声明 key 也一次收敛，实测）。
//
// **同一次调用的 key 必须每次都一样**：首跑返回空、续跑返回非空（或反过来）等于换了身份，被调方
// 从头再跑一遍（实测）。
//
// 全历史输入（[WithAgentToolFullHistoryInput]）本来就只按位置定身份，给了 key 时位置之外再带上它。
func WithAgentToolCallKey(fn func(args map[string]any) string) AgentAsToolOption {
	return func(c *agentToolConfig) { c.callKey = fn }
}

// AgentAsTool 把 Agent 包装为 tool.Tool——可注册到 toolkit 让另一 agent 的
// LLM 通过 tool_call 调用此 sub-agent。
//
// **#7 F1+F5 改造** (2026-05 audit): 签名从 `AgentAsTool(a, description string)`
// 改 `AgentAsTool(a, opts...)`. description 默认从 `a.Description()` 取，让业务
// **一处定义复用**, 不再 caller 手动 wrap.
// 业务想 override 用 `WithAgentToolDescription(desc)`. 业务想定制 input schema
// 用 `WithAgentToolInputSchema(schema)` 或 `WithAgentToolFullHistoryInput()`.
//
// **空 description 行为**: agent.Description() 空 + 业务也没传 `WithAgentToolDescription`
// → 构造期 **panic** (fail-fast). 业务 init 期发现, 不延迟到 runtime LLM 调 tool 时.
//
// **session 半共享语义**（ISSUE-79 修）：sub-agent
// 跑用 NewSharedChildSession 构造的 child sess——values map 与父共享指针（业务侧
// Skill listing / 自定义 KV 跨 sub-agent 累积 + StoreMemory 上追看父 history），events
// 独立（审计层不交叉污染），RunID 取所在的任务（见 [RunSubAgent]），parentSess
// 反向指针让业务自定义 Store 实现的 Search 可用 sess.RootSession() 沿链上追到根 sess。
//
// **半共享只在 AgentAsTool 边界生效，不递归到 sub-agent 内部嵌套编排**（R32 audit
// 视角 B HIGH-1 澄清）：业务 `AgentAsTool(SeqAgent)` 时 SeqAgent.Run 收到的 child
// sess 是半共享 child sess（events 独立父），但 SeqAgent 内部 step1/step2/step3
// 都共享同一 child sess.events（step2 LLM 看 step1 输出是 SeqAgent 协作语义的
// 设计意图，非 bug）。同理 LoopAgent body 跨轮累积 / ReflectAgent reflector 看
// actor 输出 / HandoffAgent hop2 看 hop1 输出 / RoutingAgent 同 Run 单 branch
// ——5 编排 events 共享是各自语义的设计意图。
//
// **唯一例外是 ParallelAgent**：branch 并发执行，ISSUE-80（R32）后用 lane events
// （ForkLane / CommitLaneEvents 物理隔离每 branch events）让各 branch 互不串台——业务可
// 正常用 ParallelAgent 不需要担心 final message 错置归属。详见 session.go ForkLane godoc。
//
// # 续跑时这次调用怎么认
//
// 每次 Call 是一次嵌套调用（同 [RunSubAgent] 的身份规则）。三件事跟直接调 RunSubAgent 不同：
//
//   - **完成记录只记最终那条消息**，是普通记录：父节点重跑时原样拿回、不再执行。`tool.Tool.Call`
//     没有地方传 [WithRerunnable]——产出要被条件边按 CondFailRerunFrom 判、否决后要重做的那一次，
//     改在节点体里用 RunSubAgent 调；中间件在 next 之后校验产出、不合格时返 error 也拿不到新的一份
//     （重跑拿回的是同一份），要换一份就返 `tool.Failure`，让模型重发一个 tool_call（新的一次调用）；
//   - **身份**：默认按交给被调方的输入定——默认 schema 下是 `input` 那一个字段（别的字段不进身份），
//     自定义 schema 下是整个 args。中间件往参数里注入每次都变的值（请求号、时间戳）时，只有它落进了
//     这些字段（自定义 schema，或改写了 `input` 的文本本身）才会让每次续跑都成为新调用，那时用
//     [WithAgentToolCallKey] 声明身份（`OnToolStart` 的注入续跑不变：一批调用的内容在第一次执行时
//     定下）。全历史输入（且没有自定义 schema）由框架从
//     session 事件现拼、续跑时可能不同，只按位置定；
//   - **会话历史**：它的产出会被 BasicAgent 的 tools 节点写成 tool_result 时不补记——tools 节点直接
//     调起、同一个 tool_call 里的转发与组合工具都在此列（自写 toolsFn 也一样，看的是 tools 节点，
//     不看有没有调 `compose_v2.WithCurrentToolCallContext`）；别处（GraphAgent 节点体、各种钩子里）
//     调时补记（同 RunSubAgent），不补的话非 BasicAgent 的被调方输入与产出一条不剩。组合工具要让
//     里面那次调用留痕，改用 RunSubAgent。
//
// 业务自定义 Store 实现想看父对话历史时（在 Search 内沿父 sess 链上追）：
//
//	func (s *MyStore) Search(ctx context.Context, ns store.Namespace, opts ...store.SearchOption) ([]*store.SearchItem, error) {
//	    sess := agent_v2.GetSession(ctx)
//	    if sess == nil { return nil, nil }
//	    rootHistory := sess.RootSession().HistoryFor("")
//	    // 用 rootHistory 调 semantic search backend (mem0 / pgvector ...)
//	    return s.backend.Search(ctx, ns, rootHistory, opts...)
//	}
//
// **values 共享 namespace 约定**：业务自管 key 前缀避冲突——多 sub-agent 共享同
// values map，如两 sub-agent 都用 `"current_step"` 作 key 会互相覆盖。建议加
// 「sub-agent 名」前缀，如 `"researcher.current_step"`。
func AgentAsTool(a Agent, opts ...AgentAsToolOption) tool.Tool {
	// audit⁵ HIGH-1 fix: isAgentNil 检 typed nil interface (五复证 — audit⁴ 漏此公开 API).
	if isAgentNil(a) {
		panic("AgentAsTool: nil agent (含 Go typed nil interface 陷阱)")
	}
	cfg := agentToolConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	// #7 F5 fix: description 默认从 agent.Description() 取.
	// 业务 override 用 WithAgentToolDescription. 空时构造期 panic 让业务 init 期发现.
	desc := cfg.description
	if desc == "" {
		desc = a.Description()
	}
	if desc == "" {
		panic("AgentAsTool: 'description' required (agent.Description() empty + no WithAgentToolDescription) — fail-fast: 业务想给 sub-agent 配 tool description 必填一处")
	}
	return &agentTool{
		agent:             a,
		description:       desc,
		customInputSchema: cfg.customInputSchema,
		fullHistoryInput:  cfg.fullHistoryInput,
		callKey:           cfg.callKey,
	}
}

type agentTool struct {
	agent             Agent
	description       string
	customInputSchema map[string]any
	fullHistoryInput  bool
	callKey           func(args map[string]any) string
}

// usesHistory 报这次的输入是不是框架从父 session 拼的全历史：两个选项同时设时自定义 schema 优先
// （输入是调用方给的 args），身份、派生标记与输入分支必须用同一个判据——曾经身份按
// fullHistoryInput 判、输入按 schema 优先，两个都设时身份只取位置，换参数重试的新参数永远进不了
// 被调方（实测 3 次尝试都续上第一次的失败存档）。
func (t *agentTool) usesHistory() bool {
	return t.fullHistoryInput && t.customInputSchema == nil
}

func (t *agentTool) Name() string        { return t.agent.Name() }
func (t *agentTool) Description() string { return t.description }
func (t *agentTool) InputSchema() map[string]any {
	// #7 F1 fix: 业务自定义 schema 优先
	if t.customInputSchema != nil {
		return t.customInputSchema
	}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"input": map[string]any{
				"type":        "string",
				"description": "要发送给代理的任务或问题。",
			},
		},
		"required": []string{"input"},
	}
}

func (t *agentTool) Call(ctx context.Context, args map[string]any) (*tool.Result, error) {
	// #7 F1 fix: 输入 schema 三模式
	//   1. customInputSchema 非 nil → args 整体 marshal JSON 作 input.Content
	//   2. fullHistoryInput 启用 → 从父 sess.HistoryFor("") 拼 messages 作 input.Content
	//   3. 默认 → args["input"] 字符串作 input.Content
	var inputStr string
	switch {
	case t.customInputSchema != nil:
		raw, err := json.Marshal(args)
		if err != nil {
			return tool.Failure(tool.ErrCodeInternal, "AgentAsTool: marshal custom args: "+err.Error()), nil
		}
		inputStr = string(raw)
	case t.usesHistory():
		// 父 sess 含完整对话历史 — 取所有 messages 拼成 single user message
		//
		// **别在这里按 annotation 过滤 skill marker**（原 #7 audit follow-up H2-a 加过，
		// 已随增量 listing 一并删）——本路径**读不到 annotation**：`HistoryFor("")` 的空
		// owner 让所有消息走 rewriteCrossAgentMessage，而它对 assistant/tool 是重建成
		// schema.UserMessage（见 session.go 末段），**整个 Annotations 丢掉**。当年那条
		// 过滤之所以看着"有效"，是因为它挡的 listing 恰好是 RoleSystem（非 assistant/tool
		// 原样返回、annotation 完好）；skill body 打在 tool_result 上，从来就没被挡住过。
		//
		// 真要挡父的框架元信息，得在 rewrite 之前按角色/内容判，或让 HistoryFor 带 owner。
		if parentSess := GetSession(ctx); parentSess != nil {
			// 调用方提供的前文不放进来：子 agent 读历史时照它自己的规则已经看得到它（以客户端为准时排在它的前文最前，以
			// 服务端为准时在写进历史层的记录里，见 caller_history.go），这里再放一份，同一次调用里子 agent 看到两遍
			msgs := parentSess.HistoryFor("")
			var sb strings.Builder
			first := true
			for _, m := range msgs {
				if m == nil {
					continue
				}
				if !first {
					sb.WriteString("\n")
				}
				sb.WriteString(string(m.Role))
				sb.WriteString(": ")
				sb.WriteString(m.Content)
				first = false
			}
			inputStr = sb.String()
		}
		// 父 sess 为空 (caller 裸调 tool.Call) → 退化空 input
		// **注意** (audit H2-b doc warn): FullHistoryInput 只取**当前 sess** events.
		// 多层嵌套 AgentAsTool(sub) → sub 内 AgentAsTool(grandchild) +
		// WithAgentToolFullHistoryInput() 时, grandchild 拿到的是 sub 的 child sess
		// events (NewSharedChildSession events 独立父, 入口空)——不含**根 sess**
		// 内容. 业务想完整对话走 `sess.RootSession().HistoryFor("")` 自管 wrap.
		// (同款限制: 只取当前 sess 的 messages, 不递归上追根 sess.)
	default:
		inputStr, _ = args["input"].(string)
	}

	subInput := schema.UserMessage(inputStr)
	// **fullHistoryInput 拼出来的那份要标派生**，否则子 agent 会把它当真实用户输入记进
	// 会话历史。判据是「拼进去的东西是不是已经在历史里」：这一份 100% 由框架从父对话
	// 拍扁而来，父自己的输入 / 每轮产出 / 工具结果都已各自记过，全可重算。
	//
	// 不标的后果是**超线性膨胀且跨 Run 累积**：blob 盖子 agent 的名字进它自己的 prior，
	// 第二次调用时 input 是「包含 blob₁ 全文的 blob₂」，同一个 Run 内多调几次就开始翻倍。
	//
	// 另两条路径**不标是对的**：`args["input"]` 与 custom schema 都是 LLM 在 tool_call
	// 里生成的——模型确实要求子 agent 做这件事，那是真实指令不是派生。
	if t.usesHistory() {
		// MarkDerivedInput 返回打了标记的拷贝、不改原对象——必须接住返回值
		subInput = MarkDerivedInput(subInput)
	}
	// **agent 被当工具用时，它的控制流决策不许逃出去**：把上层 HandoffAgent 的转交
	// 收件人摘掉。这一层要的是被包 agent 的**产出**，不是让它替调用方决定这场对话
	// 归谁——不摘的话它调一次 transfer 就能把祖父那层的路由夺走，而祖父自己明确没
	// 转交。被包的若本身是 HandoffAgent，它进去会 push 自己的收件人，内层转交照常。
	ctx = withoutHandoffDest(ctx)
	// 同款理由的另一半：路由决策也不许逃出去。两种控制信号必须一起摘——哨兵
	// TestToolBoundaries_StripBothControlChannels 盯着这条对账。
	ctx = withoutRouteDest(ctx)
	// audit #7 follow-up F4 fix: 3 件 ctx push (graphNS auto-fanout + NewSharedChildSession +
	// WithSubAgentStoreScope) 抽到 RunSubAgent 那个执行器, 业务自定义 GraphAgent NodeFunc
	// 嵌套也可调相同 helper 防漏 push (CLAUDE.md "协议变更四复证教训").
	//
	// 取舍见 AgentAsTool 的「续跑时这次调用怎么认」：
	//   - **记完成结果，只记最终那条消息**，不看这次 Call 是被谁调起的：tools 节点的工具账只在整个
	//     tool_call 完成时记，而 AgentAsTool 返回之后同一个 tool_call 还可能挂起（组合工具接着问人、
	//     装饰器 / 中间件在转发之后问人）——那时只有这份记录挡得住子 agent 整个重跑（实测：用户
	//     批准的是草稿 v1，续跑重新起草、执行的是 v2，`err == nil`）；
	//   - **身份**：全历史输入只按位置（它由框架从 session 事件现拼，事件不进存档，续跑时拼出来的
	//     可能不一样——按内容定身份就会换槽重问，最后在空输入上算出结果，实测）；其余按内容，业务用
	//     WithAgentToolCallKey 声明了 key 时按 key；
	//   - **补记历史只在产出不会成为 tool_result 的地方**：tools 节点里（直接调起、转发、组合工具），
	//     产出由 tools 节点写成 tool_result 进历史，再补一份就是同一条内容两个归属；在节点体、钩子里
	//     调时没有 tool_result 兜底，不补的话被调方的输入与产出在历史里一条不剩（非 BasicAgent 的
	//     被调方自己不记，实测 0/0）。判据看 tools 节点盖的章（outputRecordedAsToolResult），不看
	//     「在不在 tool_call 里」：自写 toolsFn 不调 WithCurrentToolCallContext 时后者是 false、产出
	//     照样成为 tool_result（实测记两份）；GraphAgent 节点体里自己盖 tool_call 章做扇出时后者是
	//     true、却没有 tools 节点记（实测一条不剩）。RoutingAgent 给每一档决策方注入的调用身份借的也是
	//     tool_call 那几维（后者同样是 true），决策方里调起的 AgentAsTool 同样没有 tools 节点记。
	var opts []RunOption
	if t.callKey != nil {
		if k := t.callKey(args); k != "" {
			opts = append(opts, WithCallKey(k))
		}
	}
	events, err := runNestedCall(ctx, t.agent.Name(), t.agent, subInput,
		nestedMode{positional: t.usesHistory(), remember: true, finalOnly: true, settle: !outputRecordedAsToolResult(ctx)},
		opts...)
	if err != nil {
		// **audit Phase 5.2 Bug #1 修复**：InterruptError 必须 propagate as error
		// 让上层框架 SaveNS cp——之前转 tool.Failure 静默吞会让 HITL 中断丢失。
		// 业务错（非 Interrupt）仍按 tool.Failure 让 LLM 看到错误信息继续 ReAct。
		// **控制流信号必 propagate，不转 tool.Failure**（判据走 `MustPropagate`，
		// **别直接调 `IsControlFlowSignal`**——理由见 errors.go 那条 godoc，agent_v2 有机械
		// 对账 `TestSwallowDecision_NoDirectControlFlowSignal` 挡着）：
		// InterruptError（框架要 SaveNS cp、父要挂起）/ ctx 取消（父也超时了，转错误让父
		// LLM 继续 ReAct 纯浪费）/ **陈旧写回**（另一个执行者已推进会话——转成 tool 错误
		// 会让父 LLM 当普通失败重试，而子 agent 的副作用已经落地了，重试即翻倍）。
		if compose_v2.MustPropagate(ctx, err) {
			return nil, err
		}
		return tool.Failure(tool.ErrCodeInternal, err.Error()), nil
	}
	out := FinalMessage(events)
	if out == nil {
		return tool.Success(""), nil
	}
	return tool.Success(out.Content), nil
}

var _ tool.Tool = (*agentTool)(nil)

// RunnableAsTool 把普通 Go 函数包装为 tool.Tool（对照 agent.RunnableAsTool）。
func RunnableAsTool(name, description string, fn func(ctx context.Context, input string) (string, error)) tool.Tool {
	// audit⁵ HIGH-1 fix: typed nil func 防 — `var fn func(...); RunnableAsTool(..., fn)`
	// 时 fn 是 typed nil func, 后续 tool.Call 调 fn(ctx, input) nil deref panic.
	if fn == nil {
		panic("RunnableAsTool: nil fn")
	}
	return tool.NewFuncTool(
		name,
		description,
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"input": map[string]any{
					"type":        "string",
					"description": "输入字符串。",
				},
			},
			"required": []string{"input"},
		},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			inputStr, _ := args["input"].(string)
			out, err := fn(ctx, inputStr)
			if err != nil {
				// review-v6 M-01 fix: 与 AgentAsTool (line 141-150) 协议对称 — Interrupt /
				// ctx.Err 是控制流必 propagate 让框架 SaveNS cp / 父 abort, 不转 tool.Failure.
				// 同 AgentAsTool：控制流（含陈旧写回）必 propagate，见那边的说明
				if compose_v2.MustPropagate(ctx, err) {
					return nil, err
				}
				return tool.Failure(tool.ErrCodeInternal, err.Error()), nil
			}
			return tool.Success(out), nil
		},
	)
}

// toolResultPosKey 是「这里跑的工具，产出会被 BasicAgent 的 tools 节点记成 tool_result」的章，值是
// 那个 tools 节点这次执行的位置（见 withToolResultRecorder）。
type toolResultPosKey struct{}

// execPosition 是一次节点执行在图里的位置：ns、节点、实例身份、访问序号。
type execPosition struct {
	ns, node, instance string
	visit              int
}

func currentExecPosition(ctx context.Context) execPosition {
	return execPosition{
		ns:       compose_v2.GetGraphNS(ctx),
		node:     string(compose_v2.GetCurrentNodeID(ctx)),
		instance: compose_v2.GetCurrentInstanceID(ctx),
		visit:    compose_v2.GetCurrentNodeVisit(ctx),
	}
}

// withToolResultRecorder 由 BasicAgent 的 tools 节点在调工具之前盖：记下自己这次执行的位置。
func withToolResultRecorder(ctx context.Context) context.Context {
	return context.WithValue(ctx, toolResultPosKey{}, currentExecPosition(ctx))
}

// outputRecordedAsToolResult 报这里调起的 AgentAsTool 的产出会不会被 tools 节点写成 tool_result：
// 有 tools 节点盖的章、而且当前位置就是那个 tools 节点这次执行（不是它下面另跑的嵌套图，也不是
// 交给被调方之后被调方在自己图外的那一段——那两处的 ns / 节点跟章不一样）。
//
// 比位置而不是只看章在不在：章是普通 ctx 值，会顺着 ctx 流进工具里另跑的嵌套图、流到被调方的
// agent 级钩子里，那些地方的产出不会成为谁的 tool_result。
func outputRecordedAsToolResult(ctx context.Context) bool {
	pos, ok := ctx.Value(toolResultPosKey{}).(execPosition)
	return ok && pos == currentExecPosition(ctx)
}
