package agent_v2

import (
	"context"
	"fmt"
	"sort"

	"crew-go/compose_v2"
	"crew-go/tool"
)

// HandoffTransferToolName 是内置转交工具名（LLM 自主转交，模型 B）。
//
// 命名对齐 OpenAI Agents SDK transfer_to_<name>——
// 业界做控制权转移一律走 tool-call（见 doc/v2/handoff.md §3.1 四方共识）。
const HandoffTransferToolName = "transfer_to_agent"

// HandoffTargetInvalidErrCode 是转交目标非法的 tool error code。
//
// LLM 转交到不在候选集的 agent（幻觉/拼错）时，工具返此 code **软失败回灌** LLM
// 让其换合法目标重选——不终止整个 Run（同项目 SkillDisabledErrCode 范式：软失败回灌而非
// 终止）。不在 tool.IsFrameworkError 列表（业务级失败）。
//
// 见 doc/v2/handoff.md §6.4 非法目标三来源处理表。
const HandoffTargetInvalidErrCode = "handoff_target_invalid"

// HandoffNoCoordinatorErrCode 是「这儿没人接转交」的 tool error code。
//
// transfer 工具找不到收件人时返此 code **软失败回灌** LLM——让它改成自己作答，
// 不终止整个 Run（同 HandoffTargetInvalidErrCode 范式）。两种情形会撞上：
//
//   - sub-agent 配了 WithHandoffTransfer() 却**没跑在 HandoffAgent 底下**
//     （业务复用同一个 agent 对象、有时独立跑，是合法用法）
//   - 它被**当成工具**跑（AgentAsTool / skill fork）——那一层把收件人摘了，见
//     [withoutHandoffDest]
//
// 不在 tool.IsFrameworkError 列表（业务级失败）。
const HandoffNoCoordinatorErrCode = "handoff_no_coordinator"

// newHandoffTransferTool 返回内置 transfer_to_agent 工具（标记 return-direct）。
//
// **只由 [handoffGrant] 的持有者构造**——BasicAgent 每轮 model 调用前，确认手上这份
// grant 是发给自己的之后才建它并注册进本轮 toolkit。没被授予的 agent 连这件工具都不
// 存在，它**产生不出**转交信号（而不是「产生了再被拒」）。
//
// candidates 从 grant 来（跟能力同生共死），不走 ctx：候选名单是授予内容的一部分。
// ⚠️ **传 nil 是「不校验目标」不是「回落到别处取名单」**——`candidates != nil &&
// !candidates[target]` 这个判据下，nil 让整段校验短路。生产路径恒给非 nil
// （HandoffAgent 铸 grant 时装上、regrant 原样继承），所以 nil 只出现在测试里。
//
// LLM 调 transfer_to_agent({agent_name, reason}) 转交控制权：
//  1. 取 HandoffAgent 放进 ctx 的收件人——没有就是「这儿没人接转交」，返
//     HandoffNoCoordinatorErrCode 软失败回灌让它自己作答
//  2. 验证 agent_name 在候选集——非法返 HandoffTargetInvalidErrCode 软失败回灌重选
//  3. 合法：把决定交给所在的那段执行（上行信号，见 side_signal.go），跟着这次工具调用的提交走
//     （同一批里两次 transfer 目标不一致时排在前面的生效，后面那次的结果换成一条「没有生效」的
//     失败结果，见 tool_batch_signals.go）+ 标记 tool.ReturnDirectMetaKey
//     让 BasicAgent 一调即出（§6.5 return-direct，避免转交后 LLM 继续唠叨）
func newHandoffTransferTool(candidates map[string]bool) *tool.FuncTool {
	return tool.NewFuncTool(
		HandoffTransferToolName,
		"把对话控制权转交给另一个专长 agent。当用户需求更适合别的 agent 处理时调用——转交后该 agent 接管对话。",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"agent_name": map[string]any{
					"type":        "string",
					"description": "目标 agent 名（必须是可转交候选之一）。",
				},
				"reason": map[string]any{
					"type":        "string",
					"description": "转交理由（可选，帮目标 agent 理解为何转来）。",
				},
			},
			"required": []string{"agent_name"},
		},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			return handoffTransferToolFunc(ctx, candidates, args)
		},
	)
}

// newHandoffTransferStub 是**没被授予**转交能力时挂在 dispatch 侧的软失败桩——
// 它永远返 [HandoffNoCoordinatorErrCode]，不读 ctx 里的收件人、不做出任何决定。
//
// # 为什么必须有它
//
// 授予之后 LLM 那侧（bind list）根本看不到 transfer_to_agent，理论上不会调它。
// 但**它仍然会被调到**，三条来路都真实：
//
//   - 业务照 [HandoffPromptPrefix] 拼了 system prompt，却把这个 agent 放在深层
//   - 会话历史/压缩摘要里残留着上次真能转交时的调用，模型照着学
//   - 纯幻觉
//
// 没有这个桩时它们的下场是**整个 Run 报错**（`tools called but BasicAgent has no
// tools fn`——没有业务 toolkit 的候选连 tools fn 都不会被建），哨兵
// TestHandoffTransfer_StrayCallHasNowhereToGo 钉的正是「该软失败回灌、不该整体失败」。
//
// ⚠️ **不能拿真工具当桩**：真工具会去读 ctx 里的收件人，而深层 agent 的 ctx 里恰好有一个
// （从直接候选继承下来的）——那等于把刚关上的穿透面原样打开。桩必须是另一个函数。
func newHandoffTransferStub() *tool.FuncTool {
	t := newHandoffTransferTool(nil)
	return tool.NewFuncTool(HandoffTransferToolName, t.Description(), t.InputSchema(),
		func(context.Context, map[string]any) (*tool.Result, error) {
			return tool.Failure(HandoffNoCoordinatorErrCode,
				"本 agent 没有被授予转交能力（转交由上层 HandoffAgent 在调用直接候选时授予）——请自己作答"), nil
		})
}

// handoffTransferToolFunc 是 transfer_to_agent 工具执行体。
func handoffTransferToolFunc(ctx context.Context, granted map[string]bool, args map[string]any) (*tool.Result, error) {
	target, _ := args["agent_name"].(string)
	if target == "" {
		return tool.Failure(HandoffTargetInvalidErrCode, "agent_name 不能为空"), nil
	}

	// 先看有没有人接——**这是结构性前提**，比「转给谁合法」更靠前。拿不到收件人的两种情形
	// （头顶没 HandoffAgent / 自己是被当工具用的那一层）都不该把信号交到任何地方去。
	dest, ok := getHandoffDest(ctx)
	if !ok {
		return noHandoffCoordinator(), nil
	}

	// 验证候选集——非法软失败回灌让 LLM 重选（不终止 Run）。**名单只从授予里来**（跟能力
	// 同生共死）；拿不到名单则跳过校验，交给下游 stepFn 兜底。曾经还有一条「granted 为 nil
	// 就回落读 ctx」的兜底，而那条读取点生产不可达（HandoffAgent 恒给非 nil 名单、regrant
	// 恒继承）——于是配套那次 ctx push 唯一的作用就是把这张**活 map** 暴露给业务：一行
	// `GetHandoffCandidates(ctx)["ghost"]=true` 就撑宽了本 Run 全部候选的授权范围，
	// 下游是 err=nil + 空 final。整条通道已删，哨兵 TestHandoffCandidates_NoExportedAccessor。
	candidates := granted
	if candidates != nil && !candidates[target] {
		names := make([]string, 0, len(candidates))
		for n := range candidates {
			names = append(names, n)
		}
		sort.Strings(names)
		return tool.Failure(HandoffTargetInvalidErrCode,
			fmt.Sprintf("目标 %q 不在可转交候选 %v 中，请换合法目标", target, names)), nil
	}

	// 把决定交给所在的那段执行（这次工具调用）：它跟着这次调用的提交走——调用成功并提交才
	// 算数，所在的节点续跑时命中工具账、函数体不再跑，账里记着它（见 side_signal.go）。
	// Origin 只用于报冲突时定位。
	if !compose_v2.EmitSignal(ctx, handoffSignal, compose_v2.Signal{Dest: dest, Value: target, Origin: signalOrigin(ctx, HandoffTransferToolName)}) {
		return noHandoffCoordinator(), nil
	}

	// 标记 return-direct（一调即出，§6.5）——BasicAgent 执行完本工具立即短路终止 ReAct，
	// 不回 model 让 LLM 继续生成。返回转交 ack 作为 tool_result。
	return &tool.Result{
		Success:  true,
		Output:   fmt.Sprintf("已转交给 %s。", target),
		Metadata: map[string]any{tool.ReturnDirectMetaKey: true},
	}, nil
}

// noHandoffCoordinator 是「这儿没人接转交」的软失败（见 [HandoffNoCoordinatorErrCode]）。
func noHandoffCoordinator() *tool.Result {
	return tool.Failure(HandoffNoCoordinatorErrCode,
		"当前没有可转交的上层编排（本次调用不在 HandoffAgent 的候选位上，或本 agent 是被当作工具调用的）——请自己作答")
}

// WithHandoffTransfer 声明「这个 agent 允许把对话交给别人」——**转交的两个必要条件之一**。
//
// 另一个是**被授予**：`HandoffAgent` 在调用候选的那一刻经 RunOption 发一份授予（见
// [RegrantHandoffTo]）。两个条件缺一不可：
//
//   - 配了它、但没被授予（跑在 HandoffAgent 之外、或是被当工具用的那一层）⟹ LLM 连
//     `transfer_to_agent` 都看不到，不会浪费一轮去调它
//   - 被授予了、但没配它 ⟹ 同样拿不到工具。**这正是本 option 的用处**：把某个候选做成
//     「只进不出」的终点（账单 agent 不许再把用户甩出去），而 `WithToolFilter` 做不到
//     这件事——框架工具恒豁免裁剪
//
// # 它不再往 toolkit 里塞东西
//
// 工具由框架在**每轮 model 调用前**注入（与 `activate_skill` / `tool_search` 同一形状），
// 所以**它跟 `WithToolkit()` 的先后顺序不再重要**——旧版是「注册进 cfg.Toolkit（nil 时
// 自建）」，于是 `WithHandoffTransfer()` 写在 `WithToolkit()` 前面会被后者整个覆盖掉、
// 静默失去转交。那个顺序坑随本次改动消失。
func WithHandoffTransfer() Option {
	return func(c *Config) { c.HandoffTransferOptIn = true }
}

// HandoffPromptPrefix 是参与 handoff 的 sub-agent system prompt 的推荐前缀——引导 LLM
// 理解"调 transfer_to_agent 工具转交控制权" + "转交对用户无缝、别提及"（借鉴 OpenAI
// Agents SDK RECOMMENDED_PROMPT_PREFIX，doc/v2/handoff.md §10）。
//
// 黑盒约束下框架无法强注入 sub-agent prompt，故提供常量 + HandoffPrompt helper 让业务
// 自愿拼（同 SDK 也是"推荐"非强制）。仅在 sub-agent 配了 WithHandoffTransfer 时有意义。
const HandoffPromptPrefix = `# 系统上下文
你是多 agent 系统的一员。当用户需求更适合别的 agent 处理时，调用 transfer_to_agent
工具把对话控制权转交给目标 agent。转交在后台无缝进行——不要在回复里向用户提及或解释
转交动作（不要说"我帮您转接"之类）。`

// HandoffPrompt 把 HandoffPromptPrefix 拼到业务 prompt 前。
//
// 非 Option 函数（返 string），故不用 With 前缀——用法：
//
//	agent_v2.WithSystemPrompt(agent_v2.HandoffPrompt("你是前台，负责..."))
func HandoffPrompt(userPrompt string) string {
	return HandoffPromptPrefix + "\n\n" + userPrompt
}

// HandoffHistoryMode 控制 handoff 转交后新接手 agent 看到的对话历史视图——业内
// （如 OpenAI Agents SDK）默认都"后接看前接"，crew-go 据此实现。
// 见 doc/v2/handoff.md §9。
type HandoffHistoryMode int

const (
	// HandoffHistorySelfOnly：后接 agent 只看自己产出的历史（owner-aware）+ 当前 input。
	// **是 GetHandoffHistoryMode 拿不到时的缺省（零值）**——保证非 handoff 场景（其他编排
	// 嵌 BasicAgent）零行为变化。handoff 场景显式设此 = 隔离（敏感数据不跨 agent）。
	HandoffHistorySelfOnly HandoffHistoryMode = iota
	// HandoffHistoryRaw：后接看完整共享历史，前接消息**保原 role** + 标 from_agent annotation
	// （= session.HistoryForRaw）。**opt-in**——保留 role 结构，适合看重"前接对话保原样"的场景。
	// 历史 ⚠️（悬空剥离 fix 后已解除）：前接 Exit 的 transfer tool_call 曾随 Raw 原样上 wire
	// 致真实 API 400（qwen 实测 "Can only get item pairs from a mapping"）；现
	// buildInitialMessages 汇合层统一剥离悬空 tool_call（dropDanglingToolCalls），Raw 不再
	// 400。**曾经的代价是「转交动作在后接视野零痕迹」（空文本 transfer assistant 整条被剥）
	// ——这条已不成立**：汇合层现在保留 assistant 并写一条记账文本（见
	// UnresolvedCallsRecordAnnotation），Raw 下留的是「此前发起过 transfer_to_agent(...)」
	// 而非零痕迹。两者的实际差别缩小到**措辞**：Rewrite 是面向后接的第三人称叙述
	// （"For context: [X] called tool..."），记账文本是面向「说不清成因」的中性陈述。
	// 默认仍 Rewrite——它是**为接力写的**，而记账文本是给「历史缺了一块」兜底的，见 §9。
	HandoffHistoryRaw
	// HandoffHistoryRewrite：后接看完整历史，前接消息改写第三人称 user "For context: [X]..."
	// （= session.HistoryFor）。**handoff 默认**——拍平前接 tool
	// 结构（含 transfer tool_call）成纯文本，保留面向后接的转交叙述（Raw 下汇合层留的是
	// 中性记账文本，措辞不为接力而写）；兑现"后接看前接"接力语义。见 §9。
	HandoffHistoryRewrite
)

type handoffHistoryModeKey struct{}

// WithHandoffHistoryModeCtx push 历史视图模式到 ctx——HandoffAgent stepFn 注入让后接
// BasicAgent 的 prior 据此在「只自己 / 看前接保 role / 看前接改写」间切换。
// 带 Ctx 后缀（对齐 WithThreadIDCtx 惯例）区分业务 option WithHandoffHistoryMode。
func WithHandoffHistoryModeCtx(ctx context.Context, mode HandoffHistoryMode) context.Context {
	return context.WithValue(ctx, handoffHistoryModeKey{}, mode)
}

// GetHandoffHistoryMode 读历史视图模式。**拿不到返 HandoffHistorySelfOnly（零值）**——
// 保证非 handoff 场景（BasicAgent 单独用 / 其他编排嵌 BasicAgent）prior 仍读自己历史，零回归。
func GetHandoffHistoryMode(ctx context.Context) HandoffHistoryMode {
	v, _ := ctx.Value(handoffHistoryModeKey{}).(HandoffHistoryMode)
	return v // 零值 = HandoffHistorySelfOnly
}
