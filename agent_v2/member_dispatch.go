package agent_v2

import (
	"context"
	"fmt"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 编排调用「别的 agent」只有两种语义，这个文件是它们各自唯一的实现。
//
// # 为什么收敛
//
// 收敛之前，每个编排在自己的节点里手写同一段派发代码，全仓 12 处（9 个成员派发 +
// 3 个内部决策 LLM）。那段代码承载 8 条协议：子 ns 派生（cp 隔离）/ 历史归属位置 /
// sub-agent store scope / 重试 / 转交能力转授 / 中断时落输入 / 产出提取 / 边界补记。
// **8 条 × 12 处 = 96 个必须逐个写对的地方**，而漏掉任何一条都不报错，只是各自静默退化。
//
// 这不是假想的风险：`5afb8eb` 给 6 个编排加 store scope 时漏了 ParallelAgent 和
// Routing 的 classifier，靠的是事后审计发现；此后那 15 个 push 点一直由注释里一句
// 人肉数出来的「协议覆盖 15/15」维持。加第 10 个编排时，作者要么把这 8 条全记住，
// 要么去抄一个现有编排——而抄哪一个决定了他会不会连同那一处的特殊之处一起抄走。
//
// # 两个函数就是两种语义
//
//   - [runMember]：编排的**成员**（Sequential 的一步、Parallel 的一支、Loop 的 body、
//     Routing 的分支、Reflect 的 actor、Plan 的 worker）。它代表编排对外说话，所以进
//     会话历史，也**可能**继承编排收到的转交 / 路由能力。
//   - [runDecisionAgent]：编排的**内部决策 LLM**（Routing 的分类器、Reflect 的评估器与
//     反馈教练）。它的产出是控制信号不是对话，所以不进历史，**永不**拿到转交能力；路由
//     能力只有 RoutingAgent 给自己的分类器新铸的那一份。
//
// 「内部决策 LLM 不该能发起转交」这件事，此前的形态是「那三处碰巧没写那一行」——
// 一次省略。现在它是一个函数的名字：选了 [runDecisionAgent] 就拿不到，想拿到得改成
// 另一个函数。下一个写编排的人面对的是一道有名字的选择题，不是 8 条要背的协议。

// memberSpec 是「编排调用它的一个成员」这件事的全部可变部分。
//
// 用结构体而不是位置参数，是因为 nsSuffix 和 historySlot 都是 string 却语义相反，
// 位置参数下写反了不报错——而它们**本来就经常不相等**（Loop 的 ns 带 round_N、位置
// 固定 "body"；Plan 的 ns 带 _step_N、位置是裸注册名），所以「两个值看着不一样」不能
// 当作写错的信号。具名字段让写反在字面上就不成立。
type memberSpec struct {
	// agent 是被调用的成员。
	agent Agent
	// sess 是成员事件最终提交到的会话。调 [runMember] 的 6 个编排（Sequential / Parallel /
	// Loop / Routing / Reflect / Plan——**不含 Handoff**，它的铸造点形状不同、单独留在原地，
	// 每次尝试同样经 attemptLanes 跑）传**父** session，只有 ParallelAgent 传分支自己的 lane
	// （分支间物理隔离）。成员每次尝试拿到的是从 sess 派生的一条暂存 lane（见 attemptLanes）：
	// 读得到 sess 已提交的事件（step1→step2 的 events 互相看得到、Loop body 跨轮累积），
	// 这次尝试写的事件等它有了结局才提交给 sess。
	// 跟 [RunSubAgent] 的差别就在这一项——那条路径建 NewSharedChildSession 做 events 隔离。
	sess *RunSession
	// input 是成员这一次的输入。
	input *schema.Message
	// nsSuffix 派生成员的 cp 命名空间：childNS = JoinNS(parentNS, nsSuffix)。
	// **必须逐次唯一**——同一个成员被调多次（Loop 每轮 / Plan 每步）时要带上轮次或步序，
	// 否则两次的 cp 互相覆盖。
	nsSuffix string
	// historySlot 是成员在会话历史里的归属位置。**跟 nsSuffix 是相反的要求**：ns 要逐次
	// 唯一，归属要**跨次稳定**——带上轮次的话同一个成员每轮归属都变，跨轮读不回自己的历史。
	historySlot string
	// retryCfg 是这次调用的重试策略。零值 = 不重试（见 compose_v2.RunWithRetry）。
	retryCfg compose_v2.RetryConfig
}

// runMember 跑编排的一个成员：派生子 ctx（cp 子 ns + 历史归属 + sub-agent store scope）、
// 按 retryCfg 重试、把编排收到的转交 / 路由能力转授给这个成员、成功与可 Resume 的退出两条路径
// 各自补记历史。
//
// # grants 为什么是位置参数而不是 memberSpec 的字段
//
// 空值是它的**合法值**——「这个成员不该有任何能力」（PlanAgent 的 TmpSubAgent 就是：
// 它是 planner 这个 LLM 当场决定造出来的执行体，「这一步是我的组成部分」这句话没有
// 业务说过；ParallelAgent 的分支也是：它们并发跑，而一个收件人同一超步里只能有一个
// 决定方）。而结构体字段省略掉**也是**空值。放进结构体就把「忘了想」和「想过了、
// 就是不给」压成同一个字面量；位置参数下前者编译不过，后者写成一个显眼的 `memberGrants{}`。
//
// 传进来的应当是**本编排已自查过**的那几份（`runCfg.grantsFor(自己)`）。转授的动作在本函数内做
// ——受让方从 m.agent 取，调用点写不错。
func runMember(ctx context.Context, grants memberGrants, m memberSpec) (*schema.Message, error) {
	// 跟 [runNestedCall] 对称的 fail-fast。少了它，typed nil 成员会在**错误路径**上炸——
	// `m.agent.Run` 自己的 recover 把 panic 转成 error，控制流进到下面的 err 分支，
	// 然后 `m.agent.Name()` 在 runMember 内部裸 panic（独立审计实测的调用栈）。
	// 今天所有注册入口都查过 isAgentNil 故不可达，但两个姊妹 helper 一个有一个没有
	// 本身就是下一个人的坑。
	if isAgentNil(m.agent) {
		return nil, fmt.Errorf("runMember: nil member agent (含 Go typed nil interface 陷阱)")
	}
	subCtx := compose_v2.WithGraphNS(ctx, compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), m.nsSuffix))
	subCtx = WithHistorySlot(subCtx, m.historySlot)
	// 标记「这次调用发生在成员位置上」。框架自己不读它（防重复写靠写入者维进 ns），
	// 留给业务的 store 后端按需区分（见 WithSubAgentStoreScope）。
	subCtx = WithSubAgentStoreScope(subCtx, true)
	// 把这次执行连同输入交给成员（compose_v2.HandOver）：成员的 agent 级钩子跑在它自己的图之外、
	// 编排节点的执行里——私有态有它自己的一格，提问与嵌套调用有它自己的计数与相位名，不占编排
	// 节点的序号、不跟成员图里同名节点的提问撞 ID。子 ns 已在上面推好。
	subCtx = compose_v2.HandOver(subCtx, m.nsSuffix)
	// 成员拿编排转授给它的那几份（签名），拿不到编排上方放给 RegrantFromNode 的那份（见 shadowNodeGrants）。
	subCtx = shadowNodeGrants(subCtx)

	// 每次尝试写进会话的事件先落在暂存 lane 上：被下一次从头再跑的尝试取代时作废，其余照常提交（见 attemptLanes）
	lanes := newAttemptLanes(subCtx, m.sess)
	defer lanes.finish()
	return compose_v2.RunWithRetry(subCtx, m.retryCfg, func(ctx context.Context) (*schema.Message, error) {
		var events []*AgentEvent
		err := lanes.run(func(s *RunSession) error {
			var runErr error
			events, runErr = m.agent.Run(ctx, s, m.input, grants.regrantOpts(m.agent)...)
			return runErr
		})
		if err != nil {
			// 可 Resume 的退出先落输入——输入的持久点在跨进边界那一刻，不等成功
			// （见 settleHistoryOnPause）。随后 propagate 让框架 SaveNS 存档。
			settleHistoryOnPause(ctx, m.agent.Name(), m.input, err)
			return nil, err
		}
		final := FinalMessage(events)
		if final == nil {
			// 给下游一个非 nil 的返回值——9 个调用点的下游（下一步的输入 / 聚合器 /
			// 循环判据）都不该收到 nil。
			//
			// ⚠️ 触发条件是「**没有 Exit event**」而不是「一条 event 都没有」：
			// [FinalMessage] 只认 `Action.Exit == true` 的那条。一个发了 OutputEvent
			// 却没发 ExitEvent 的成员，**内容会在这里被静默换成空串**，然后进下游和
			// 会话历史。而那正是本函数补记所服务的那批执行体（远程 / 自研 / 别的框架
			// 的壳）最容易写错的地方。这是收敛前就有的行为，此处只是把它写明。
			final = schema.AssistantMessage("")
		}
		// 成员可能不是 BasicAgent（远程 / 自研 / 别的框架的壳），那样它自己什么也不留。
		// 边界补上这一次的输入与产出——只补它没记的，见 settleHistorySlot。
		settleHistorySlot(ctx, m.agent.Name(), m.input, final)
		return final, nil
	})
}

// runDecisionAgent 跑编排的一个**内部决策 LLM**：分类器 / 评估器 / 反馈教练。
//
// 它跟 [runMember] 的三处差别都是同一个原因——**它的产出是控制信号不是对话**：
//
//   - 不进会话历史（WithoutHistoryCtx）：下游只需要那个判定结论，把评审全文灌进历史
//     会让下一轮模型把内部审议当成对话内容。这是「禁止」不是「当前没有」——嵌套的
//     agent 自配 WithHistory 也绕不过去。
//   - 永不拿转交能力：一个负责「判断该走哪个分支」的模型没有资格改写「这场对话归谁」。
//     编排收到的路由能力也不转给它；route 只是 RoutingAgent 给自己的分类器**新铸**的那一份
//     （地址是这个 RoutingAgent 自己的收件人），评估器 / 反馈教练传 nil。参数类型就是
//     `*routeGrant`——转交授予在类型上就传不进来。
//   - 不重试：重试策略是编排对**成员**的策略；决策 LLM 失败时各编排有自己的处置
//     （classifier 走 fallback 分支、evaluator 直接返错），不该被统一成一种。
//
// # 别给决策 LLM 配长期记忆
//
// 同一个原因的第四个推论，但框架**不强制**——它拦不住（`Agent` 接口看不出对方配没配
// [WithStoreMemory]，而装配期只拿得到接口）。所以这一条写在这儿而不是长在代码里。
//
// 「该走哪个分支」「够不够好」这类判定是**这一次**的控制信号，不是值得跨会话记住的
// 事实。给它配了 [WithStoreMemory] 会有两个后果：判定文本进长期记忆库污染真正的记忆；
// 以及多个决策 LLM 之间**撞键**——写入者维取自会话历史的归属位置，而决策 LLM 恰好
// 被 WithoutHistoryCtx 摘掉了归属 ⟹ 它们的记忆全落在基准 ns 本层、共用一套键。
//
// 「不进历史」顺带变成「没有记忆身份」，这本身就是一处**借用**（记忆的身份维借了历史层
// 的归属维，而两者的要求不一致）——但正解不是给决策 LLM 补一个记忆身份，是它压根
// 不该写。多个成熟框架的内部决策 LLM（记忆整合器、查询分析器）同样不写记忆。
//
// 返回原始 events 而不是 final：三个调用点要从里面取的东西各不相同（分类结果 / 达标
// 判定 + 批评全文 / 改进建议），提取逻辑留在各自那边。
func runDecisionAgent(ctx context.Context, nsSuffix string, ag Agent, sess *RunSession, input *schema.Message, route *routeGrant) ([]*AgentEvent, error) {
	subCtx := compose_v2.WithGraphNS(ctx, compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), nsSuffix))
	subCtx = WithSubAgentStoreScope(subCtx, true)
	subCtx = decisionCtx(subCtx)
	// 同 runMember：决策 LLM 的执行交给它、它的 agent 级钩子有自己的相位
	subCtx = compose_v2.HandOver(subCtx, nsSuffix)
	subCtx = shadowNodeGrants(subCtx)
	return ag.Run(subCtx, sess, input, withRouteGrant(route))
}

// decisionCtx 是框架调**内部决策方**时给的 ctx：编排的决策 agent（分类器、评审 / 评估 agent、反馈教练，见
// runDecisionAgent）与业务交给编排的决策函数（路由函数、路由决策方、规划器、Reflector / EvalFunc、Loop 的
// 退出判断、回合结束闸门）都用它。产出是内容而不是判定的回调（Parallel 的 Merger / Aggregator、Plan 的
// ExecInputBuilder）不在此列。两件事：
//
//   - 不进会话历史（[WithoutHistoryCtx]）：下游只需要它的判定，把它请教的对话灌进历史，下一回合模型会把
//     评审请求当成用户说过的话。
//   - 不取用户的运行中插话（见 [Inbox]）：插话给正在对话的那个 agent，一句话不该被决策方里跑的 agent 取走。
//
// 决策函数里经 RunSubAgent / 直接 Run 请教的 agent 都继承它。
func decisionCtx(ctx context.Context) context.Context {
	return withoutInbox(WithoutHistoryCtx(ctx))
}
