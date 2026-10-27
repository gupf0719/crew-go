package agent_v2

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math"
	"strconv"
	"strings"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// RunSubAgent 嵌套调用 sub-agent 的标准 helper, 自动 push 3 件 ctx 让业务无负担:
//
//  1. **身份与子 ns**：一次嵌套调用的身份是「位置 + 输入内容」（或业务给的 call key，见
//     [WithCallKey]），存档槽由身份派生（细节见 nestedCallNS）。位置 = 调用方所在的节点与相位
//     （节点体 / 某个图钩子 / 某条条件边，见 compose_v2.SubCallScope）+ `nodeID` + 本次调用的
//     实例维：图级 fanout 实例（`SendFrom` 同 Target 投递出的 N 个实例之一）→ 实例下标；所在的
//     tool_call → tool_call ID + 批内下标 + tools 节点的访问序号；两者兼有（实例节点里业务又
//     自己起 tool_call 并发）→ 实例下标与 tool_call 一起进位置；都不是（静态/cond 路径）→
//     本节点**已成功完成的访问次数**（visit），让父循环第 N 轮重进时子层与上一轮物理分离。
//     ⚠️ **这套派生是本 helper 的核心价值，别在业务侧手拼 ns 复现它**——每漏一维就是一类
//     事故（漏实例维 = 子 cp 互覆；漏 visit 维 = 第 2 轮的提问与第 1 轮撞同一个 InterruptID，
//     被答案账本判为已兑付、终局 `ErrRunAborted`；漏调用方节点 = 两个节点用同一份输入调同一个
//     子 agent 时撞成一次；漏输入内容 = 另一次调用续上这次的存档、新输入没进图，用户对这一次的
//     回答被套到另一次上），而维度还会继续增加。
//  2. **NewSharedChildSession**: 父 ctx 含 sess 时构造半共享 child sess (events 独立,
//     values 共享指针, RunID 沿用, parentSess 反向指针——ISSUE-79). 业务 store.Search
//     可走 `sess.RootSession()` 沿链上追看父 history.
//  3. **WithSubAgentStoreScope**: 标记「这次调用发生在成员位置上」。框架自己不读它；
//     业务的 store 后端可以据此给 sub-agent 的记忆换后端 / TTL（见 WithSubAgentStoreScope）。
//
// **why helper** (#7 audit follow-up F4 fix): GraphAgent 业务自定义 NodeFunc 嵌套
// sub-agent.Run 时必须手 push 上述 3 件 ctx, 漏 1 件就 silent bug——
//   - 漏 graphNS push → 子 cp 与父 cp 共用同 ns 互覆 (CLAUDE.md "实施前 checklist 第 1 条" 警示)
//   - 漏 NewSharedChildSession → events 串台, 父 LLM 看到 sub-agent 内部细节, AgentAsTool 抽象漏
//   - 漏 WithSubAgentStoreScope → 业务 store 后端按这个标记做的区分静默失效
//
// **vs 编排调自己成员的语义**: 那条路径走 `runMember`（见 member_dispatch.go，6 个编排——
// Handoff 的铸造点形状不同、单独留在原地），
// 它 push graphNS + 历史归属 + storeScope，但**不**建 NewSharedChildSession——成员
// 共享 parentSession 让协作语义生效（step1→step2 的 events 互相看得到、LoopAgent
// body 跨轮累积）。**这是本 helper 与那条路径在会话语义上的差别**，也是 7 编排不能直接改调
// 本 helper 的原因。业务想要"sub-agent events 独立于父"的语义才走这条
// （AgentAsTool / SkillTool / 本 helper）。嵌套调用另有自己的几样（身份、完成记录、输入拷贝、
// 任务章），见下。
//
// **BasicAgent + GraphAgent 不在 wrapper push 范围**:
//   - BasicAgent ReAct 无 sub-agent 嵌套语义 (preamble + model + tools + postamble 4 节点循环 ISSUE-74/75, 无 sub.Run 调用点)
//   - GraphAgent 业务自定义 NodeFunc 拓扑 — 用户自负责, 推荐走此 helper 一站式 push 3 件
//
// **父节点重跑时（中断 / 失败之后的续跑），每次调用按身份找回自己的东西**：
//
//   - **已完成的**直接拿回记下的结果，不再执行——子 agent 里的副作用不重放、问过的问题不重问
//     （记录在父节点这次执行的私有状态里，见 compose_v2.SaveStepResult）。它当初做出的转交与
//     路由决定（发给上层 HandoffAgent / RoutingAgent 的上行信号）一起记下、拿回时原样交出；
//   - **没完成的**续上自己的存档，只重跑失败 / 挂起的部分；
//   - **新的**从头跑。
//
// 节点体、`OnNodeStart` / `OnNodeEnd` 等节点级钩子、条件边里的调用都记在那个节点这次执行名下；
// 嵌套图的 `OnGraphStart` / `OnGraphEnd` 与入口条件边里的记在承载它的外层节点执行名下。顶层图的
// `OnGraphStart` / `OnGraphEnd` 与入口条件边没有可挂的节点执行，那里完成的调用在重跑时会再执行
// 一遍（没完成的照样续自己的存档）。
//
// **被调方属于调用方的任务**：本 helper 给被调方一个 RunID 就是所在任务的 session（任务的身份
// 就是 RunID；所在任务见 [DetachTask] 的说明——节点执行范围里是那张图的 sid，其次是编排入口与
// 嵌套调用执行器盖的任务章，ctx 上另推的 sid 不算），并替它盖任务章，被调方的入口据此不声明新
// 任务、续自己的存档——调用方是九个编排之一、业务自研的 agent，还是框架外业务自己的循环都一样。
// 业务在自己的循环里开新任务，照常换 RunID（`sess.NextTask()`）。
//
// 记录是整组事件（AgentAsTool 只记最终那条消息），一直留在内存里到本次 Run 结束（节点私有态不设
// 清除点，见 compose_v2 的 node_state.go），中断时随存档落盘——被调方的产出很大、父节点里又调很多
// 次时，内存与存档跟着变大。拿回的是经过存档往返（JSON）的那一份：`any` 类型的标注值变成 JSON
// 对应的类型（数字成 `float64`、切片成 `[]any`……），几个事件共享同一条消息的关系不保留；读标注时
// 按 JSON 类型读。
// 拿回时被调方不执行，它的 `OnAgentStart` / `OnAgentEnd` 也不触发。
//
// **输入不同的调用之间，身份跟调用的次序无关**：按进度跳过已完成的调用、条件分支增减调用都
// 不会让一个答案落到别的调用上。**输入完全相同**的几次调用（投票、冗余调用）只能按发起次序
// 区分——那种调用别自己按进度跳过，要跳过或重排就给每次一个 key（[WithCallKey]）。⚠️ 被调方
// 做出的转交与路由决定只在「再调一次、被拿回」时交出，**跳过就丢了**——有这种决定的调用别跳过，
// 已完成的框架已经记住，全部照调一遍即可。**输入变了就是另一次调用**：父节点重跑时给出了不同的输入（节点体自己直接
// 调了模型、读了当前时间，或输入里带着每次都变的请求号），下游会被当成新调用重新执行、重新
// 问人——把那个会变的值用 compose_v2.SaveStepResult 记住，或给调用一个 key。
//
// **重试**：`compose_v2.RunWithRetry` 包住本 helper 重试的是**同一次调用**——输入不变时每次
// 尝试算出同一个身份，已完成的拿回记下的结果、失败的续上自己的存档。所以「拿到产出、校验不合格
// 就重来」别用它，也别让节点返错再 Resume（已完成的调用会被原样拿回，永远是同一份产出）：
// 在节点里循环调，每一轮都是一次新的调用（同一份输入的第 k 次）；或者把校验放到条件边上、
// 声明 `compose_v2.CondFailRerunFrom`，并给产出那一次调用声明 [WithRerunnable]——条件边否决时
// 重做的是整次调用（它在自己的图之外记下的东西、它问的问题都换新）；没声明的调用（扣款、问过人的
// 把关）照常拿回。
//
// 交给被调方的是输入的一份独立拷贝（见 cloneCalleeInput）：消息结构逐层拷，标注与数据块里的值
// 按原类型递归深拷（切片、数组、map 一路拷下去，值结构体逐个拷导出字段、连同嵌入的结构体里的），
// 它的钩子原地改写这些不会写回调用方。**指针、函数、channel 原样共享**：被调方钩子经指针原地改
// 调用方交来的东西会写回，续跑时换身份重问；并发扇出时还会跟兄弟调用的摘要读撞成数据竞争。
//
// 被调方的 agent 级钩子（在它自己的图之外）跑在一个交接相位里（compose_v2.HandOver）：有自己的
// 提问与嵌套调用计数，问的问题带这个相位的名字，不占调用方的提问序号。
//
// **反模式警告** (#7 audit follow-up H1):
// 业务**不要**自己 `for i := 0; i < N; i++ { go RunSubAgent(ctx, "worker", sub, msg) }`
// 跑并发 — 这是绕开 BSP 框架的反模式 (crew-go 不支持). 同一次执行里起的 N 个
// goroutine 输入相同时按调度先后拿到「第几次」的编号，续跑时编号跟调用对不上——谁续上
// 谁的存档取决于调度.
// 业务想 fanout 走:
//   - 高阶: `ParallelAgent` 编排 (每个分支按 `JoinNS(parentNS, br.Name)` 派生 cp ns,
//     分支名互不相同故天然隔离)
//   - 中阶: framework `compose_v2.SendFrom(ctx, dirs...)` (framework 给每个派生实例
//     mint 一个 InstanceID, cp ns 据它派生 — 见 `InstanceFanoutIndex`)
//   - 不允许: 业务自管 goroutine 自己 `go RunSubAgent` (绕开 BSP)
//
// **nodeID 命名约定**:
//   - 业务想自定义 ns 编排时传业务自己的 nodeID 名 (e.g., "research_step")
//   - 业务直接转发 sub.Name() 时传 sub.Name() (典型 AgentAsTool 模式)
//   - nodeID 含 ns 分隔符（`/` `#` `@`）时本 helper 返 `ErrInvalidNodeIDForNS`
//     （`ValidateNodeIDForNS`）——传常量的话第一次调用就发现
//
// **返回**: 跟 sub.Run 返同款 (`[]*AgentEvent + error`). caller 自管错误处理
// (InterruptError / ctx.Err() 是控制流必 propagate, 业务错按业务策略).
func RunSubAgent(ctx context.Context, nodeID string, sub Agent, input *schema.Message, opts ...RunOption) ([]*AgentEvent, error) {
	return runNestedCall(ctx, nodeID, sub, input, nestedMode{settle: true, remember: true}, opts...)
}

// nestedMode 是一次嵌套调用在几件事上的取舍，由调用点给。
type nestedMode struct {
	// settle：这一层要不要在边界补记会话历史。
	//
	// RunSubAgent 恒补。skill fork 不补（它恒在 activate_skill 这个 tool_call 里，产出由 tools 节点
	// 写成 tool_result）。AgentAsTool 看 tools 节点盖的章（outputRecordedAsToolResult）：它的产出会被
	// tools 节点写成 tool_result 时不补——这一层再补一份就是同一条内容两个归属（实测：换名转发壳、
	// 中间件改名、自写 toolsFn 下各记两份）；别处调时补。组合工具要让里面那次调用在历史里留痕，
	// 改用 RunSubAgent。
	//
	// 不能看「祖先里有没有 tool_call」：那和「我这一层的产出会不会成为 tool_result」是两个问题。最初
	// 实测的形态是 `BasicAgent → AgentAsTool(GraphAgent) → NodeFunc → RunSubAgent(外部执行体)`：按
	// 祖先判时内层这次也被豁免，而外层 tool_result 只带图的聚合产出，内层执行体的输入和产出**一条
	// 都不剩**。tools 节点的章比的是位置，内层那次（在嵌套图的节点里）对不上、照补。
	settle bool
	// positional：身份只取位置、不看输入内容。
	//
	// 给输入由框架现拼的调用用：AgentAsTool 的全历史输入从 session 事件拼，事件不进存档，续跑时
	// （Resume 不带 WithResumeSession、并行分支里兄弟的事件后来提交了、两层 AgentAsTool 的内层）
	// 拼出来的不一样——按内容定身份就会换槽：子 agent 从头跑、换 ID 重问，最后在空输入上算出结果
	// 交给父模型（实测，`err == nil`）。skill fork 同理（一个 activate_skill 就是一次调用）。
	positional bool
	// remember：完成后记下结果、父节点重跑时拿回。三条执行路径都记——tools 节点的工具账只在整个
	// tool_call 完成时记，而这次调用返回之后同一个 tool_call 还可能挂起（组合工具接着问人、装饰器 /
	// 中间件在转发之后问人），那时只有这份记录挡得住子 agent 整个重跑。
	remember bool
	// finalOnly：完成记录只留最后那条退出事件。给只用得到最终消息的调用点（AgentAsTool）用，
	// 免得把整组事件在存档里再存一份。
	finalOnly bool
}

// runNestedCall 是 [RunSubAgent] / AgentAsTool / skill fork 共用的嵌套调用执行器。
func runNestedCall(ctx context.Context, name string, sub Agent, input *schema.Message, mode nestedMode, opts ...RunOption) ([]*AgentEvent, error) {
	if isAgentNil(sub) {
		return nil, fmt.Errorf("RunSubAgent: nil sub-agent (含 Go typed nil interface 陷阱)")
	}
	if err := compose_v2.ValidateNodeIDForNS(name); err != nil {
		return nil, fmt.Errorf("RunSubAgent: nodeID %q invalid for ns: %w", name, err)
	}

	// 1. 身份（位置 + 输入内容 / call key / 只取位置）与子 ns，见 enterNestedCall
	rc := applyRunOptions(opts)
	call, subCtx, err := enterNestedCall(ctx, name, rc.callKey, input, mode.positional, rc.rerunnable)
	if err != nil {
		return nil, fmt.Errorf("RunSubAgent: %w", err)
	}
	// 已完成过的调用：拿回记下的结果，不再执行（父节点重跑时）
	if mode.remember {
		if events, ok := recallNestedCall(ctx, call); ok {
			return events, nil
		}
	}
	// 归属位置：name（业务在自己的图里保证唯一）。**不用子 ns**——子 ns 带身份哈希、每次调用
	// 都不同，拿它当归属会让同一个 sub-agent 每次归属不同、跨轮读不回自己的历史。
	subCtx = WithHistorySlot(subCtx, name)
	// 被当成工具 / 嵌套调用的子 agent 不取用户的运行中插话：用户是在跟调用它的那个 agent 说话（见 Inbox）
	subCtx = withoutInbox(subCtx)
	// ⚠️ **AgentAsTool 传的 name 是 `t.agent.Name()`**（见 agent_tool.go），所以那条路径上
	// 子 agent 的位置就是它自己的名字。未命名的 agent 自动名每次构造都不同，位置跟着变、
	// 跨 Run 读不回自己的历史——这是 warnUnnamedAgentInHistory 要提醒的两档后果里更重的
	// 那一档。业务自己调本 helper 时同理：nodeID 要选**跨构造稳定**的值。

	// 2. NewSharedChildSession (ISSUE-79 半共享)。调用方 ctx 里没有 session 时（直接跑 compose_v2
	// 图、业务裸调）给被调方一个沿用所在任务 sid 的，见 sessionForCtx。
	var childSess *RunSession
	if parentSess := GetSession(subCtx); parentSess != nil {
		childSess = NewSharedChildSession(parentSess)
	} else {
		childSess = sessionForCtx(subCtx)
	}
	// 被调方的 Run 是调用方任务的一部分：RunID 取**所在的任务**（[enclosingTask]），不取父 session
	// 的——两者只在「业务直接跑 compose_v2 图、ctx 里另推了一个 session」时不同，那时任务是那张图
	// （它的 sid 是存档的主索引），被调方的存档跟着 session 走就落到了图的任务之外：续跑时找不到、
	// 从头再跑，还留一份孤儿存档（实测）。childSess 是框架自己的对象，改它不碰业务的。
	if task, _ := enclosingTask(subCtx); task != "" {
		childSess.RunID = task
	}
	// 替被调方盖任务章，它的入口据此不声明新任务、续自己的存档（见 enterRunTask）。调用方是不是
	// 九个编排之一都一样：业务自研的 agent、框架外业务自己的循环里发起的嵌套调用，续跑时同样要
	// 续得上；开新任务的方式跟别处一样是换 RunID（`sess.NextTask()`）。
	subCtx = withTaskScope(subCtx, childSess.RunID)

	// 3. 标记「这次调用发生在成员位置上」（框架不读，留给业务 store 后端，见 WithSubAgentStoreScope）
	subCtx = WithSubAgentStoreScope(subCtx, true)

	// 4. 把这次执行连同输入交给被调方（compose_v2.HandOver）：被调方在自己的图之外（它的 agent 级
	// 钩子）跑在一个交接相位里——私有态有它自己的一格、提问与嵌套调用有它自己的计数（不占调用方
	// 的序号）、不算调起它的那次 tool_call 本身；它的主图（落在子 ns 上）的输入算调用方产的，入口
	// 条件边否决时作废的是调用方按 Rerunnable 记下的产出。子 ns 已在 enterNestedCall 里推好。
	subCtx = compose_v2.HandOver(subCtx, call.identity)

	// 调用方输入的摘要交给被调方：记进它的存档、续跑时核对（见 withNestedCallDigest）。
	// 只取位置的调用不交——它的输入由框架现拼、续跑时可以合法地不同。
	if call.digest != "" {
		opts = append(opts[:len(opts):len(opts)], withNestedCallDigest(call.digest, !call.keyed))
	}
	// 这次调用的输入在之前一次暂停时已经落过历史的话，结算时只记产出（见 settledInput）
	settleInput := input
	if mode.settle && inputSettledBefore(ctx, call) {
		settleInput = nil
	}
	// 5. 这次调用做出的上行信号（转交 / 路由决定，见 side_signal.go）：成功时连同结果一起记下、再交给
	// 调用方所在的那段执行；失败 / 中断时随它作废——被调方停下时它图里已提交的那些留在它的存档里，
	// 续跑跑完时一起交上来。拿回记下的结果时原样交出（见 recallNestedCall）。
	var events []*AgentEvent
	// 这次调用交给前端的流单独记一份（成功时随完成记录声明，见 compose_v2.WithStreams），同时登记进所在的那段执行
	subCtx, streams := compose_v2.BeginStreamScope(subCtx)
	// 被调方拿调用方显式交给它的那几份（opts），拿不到调用方那一层放给 RegrantFromNode 的那份——孙辈
	// 拿到的是空的（见 shadowNodeGrants）。
	sigs, err := compose_v2.CaptureSignals(shadowNodeGrants(subCtx), func(c context.Context) error {
		var runErr error
		events, runErr = sub.Run(c, childSess, cloneCalleeInput(input), opts...)
		return runErr
	})
	streamIDs := streams.Take()
	compose_v2.RecordStreams(ctx, streamIDs...)
	if err != nil {
		// 可 Resume 的退出先落输入（见 settleHistoryOnPause）。开关同下面的成功补记。
		if mode.settle && compose_v2.LeavesResumableCheckpoint(err) {
			settleHistoryOnPause(subCtx, sub.Name(), settleInput, err)
			if historyWriterFrom(subCtx) != nil {
				markInputSettled(ctx, call)
			}
		}
		return events, err
	}
	// 6. 记住这次调用已经完成：父节点重跑时直接拿回结果，不再执行
	if mode.remember {
		rememberNestedCall(ctx, call, events, sigs, streamIDs, !streams.Uncertain(), mode.finalOnly)
	}
	compose_v2.EmitSignals(ctx, sigs)
	// 7. 边界补记：业务塞进来的 sub 可能不是 BasicAgent（远程 / 自研 / 别的框架的壳），
	// 那样它什么也不留。只补它自己没记的，见 settleHistorySlot。拿回记下的结果那条路不补
	// ——首次完成时已经补过。
	if mode.settle {
		settleHistorySlot(subCtx, sub.Name(), settleInput, FinalMessage(events))
	}
	return events, nil
}

// settledInputStepKey 是「这次调用的输入已经在一次暂停时落过历史」那条步骤记录的键。
func settledInputStepKey(call nestedCall) string {
	return "agent_v2.nested_call_input:" + identityHash(call.identity)
}

// nestedInputSettled 是那条步骤记录的值类型（私有具名类型：不跟业务注册的 `bool` 撞名）。
type nestedInputSettled bool

func init() {
	compose_v2.RegisterValueType[nestedInputSettled]("agent_v2.nestedInputSettled")
}

// inputSettledBefore 报这次调用的输入是不是已经在之前一次暂停时落过历史（markInputSettled）。
//
// 跨暂停认「同一条输入」只能按调用的身份认，不能按消息认：调用方常常每次执行都现拼一条输入
// （`schema.UserMessage(...)`，AgentAsTool 从参数现拼的也是），消息 ID 每次都不同，历史层按消息
// 认同一条的守卫认不出来——被调方中断一次、续跑完成后输入就记两份，每多一次中断多一份（实测）。
// 调用的身份跨重跑稳定（同一次调用续跑时算出同一个），记在调用方节点这次执行名下，随存档走。
func inputSettledBefore(ctx context.Context, call nestedCall) bool {
	done, _ := compose_v2.GetStepResult[nestedInputSettled](ctx, settledInputStepKey(call))
	return bool(done)
}

// markInputSettled 记下「这次调用的输入已经落过历史」（暂停时结算之后）。记不住（不在节点执行里）
// 时是 no-op：那里没有续跑这回事。
func markInputSettled(ctx context.Context, call nestedCall) {
	if err := compose_v2.SaveStepResult(ctx, settledInputStepKey(call), nestedInputSettled(true), compose_v2.WithStreams(nil)); err != nil {
		slog.WarnContext(ctx, "agent_v2: 记不住这次嵌套调用的输入已落过历史，续跑完成时它会再记一份",
			"ns", call.ns, "err", err)
	}
}

// nestedCall 是一次嵌套调用的身份与去向。
type nestedCall struct {
	// ns 是这次调用的存档槽（由身份派生）。
	ns string
	// identity 是身份串（见 nestedCallNS）。
	identity string
	// stepKey 是父节点这次执行里记住「这次调用已完成、结果是什么」的步骤键。
	stepKey string
	// digest 是调用方交来的输入的摘要（见 inputDigest）；只取位置的调用为空。
	digest string
	// keyed：身份由调用方给的 key 决定（[WithCallKey]），摘要只作核对。
	keyed bool
	// rerunnable：完成记录按「可重做」记（[WithRerunnable]）。
	rerunnable bool
}

// nestedCallRecord 是一次已完成嵌套调用的记录，存在父节点这次执行的私有状态里
// （compose_v2.SaveStepResult）：父节点重跑时直接拿回 Events，不再执行子 agent。
//
// 存整组 Events 而不只存最终消息：读中间事件的调用方在重跑时不该静默拿少了。拿回的是经过
// 存档往返的形态（见 [RunSubAgent] 的说明）。
type nestedCallRecord struct {
	Events []*AgentEvent `json:"events,omitempty"`
	// Digest 是完成时那份输入的摘要——带 key 的调用重跑时输入变了，据此告警。
	Digest string `json:"digest,omitempty"`
	// Signals 是这次调用做出的上行信号（转交 / 路由决定），拿回时原样交出：被调方不再执行，
	// 它当时做出的决定不会再做一次（实测：前台经 RunSubAgent 调的分诊已经转交、父节点挂起续跑
	// 后，不重放就静默丢了转交、前台自己作答）。
	Signals compose_v2.Signals `json:"signals,omitzero"`
}

func init() {
	compose_v2.RegisterValueType[*nestedCallRecord]("*agent_v2.nestedCallRecord")
}

// enterNestedCall 为一次嵌套调用定身份、push 子 ns，返回交给这次调用的 ctx——
// [RunSubAgent] / AgentAsTool / skill fork 三条路径共用这一份。
func enterNestedCall(ctx context.Context, name, key string, input *schema.Message, positional, rerunnable bool) (nestedCall, context.Context, error) {
	var digest string
	if !positional {
		digest = inputDigest(input)
	}
	ns, identity, err := nestedCallNS(ctx, name, key, digest, positional, rerunnable)
	if err != nil {
		return nestedCall{}, nil, err
	}
	call := nestedCall{
		ns:         ns,
		identity:   identity,
		stepKey:    "agent_v2.nested_call:" + identityHash(identity),
		digest:     digest,
		keyed:      key != "",
		rerunnable: rerunnable,
	}
	return call, compose_v2.WithGraphNS(ctx, ns), nil
}

// recallNestedCall 在父节点这次执行的记录里找这次调用：完成过就交出它当时做出的上行信号，返回
// 记下的 Events。
func recallNestedCall(ctx context.Context, call nestedCall) ([]*AgentEvent, bool) {
	rec, ok := compose_v2.GetStepResult[*nestedCallRecord](ctx, call.stepKey)
	if !ok || rec == nil {
		return nil, false
	}
	if call.keyed && rec.Digest != call.digest {
		slog.WarnContext(ctx, "agent_v2: 带 key 的嵌套调用这次的输入与它完成时不同，仍返回完成时记下的结果",
			"ns", call.ns, "saved_digest", rec.Digest, "input_digest", call.digest,
			"hint", "key 相同即认作同一次调用；真是另一件事的话换一个 key")
	}
	compose_v2.EmitSignals(ctx, rec.Signals)
	return rec.Events, true
}

// rememberNestedCall 记下这次调用已经完成（子 agent 跑完之后才记——记的是已经做成的事）。
// 记不住时（结果里有序列化不了的值）只告警：父节点重跑时这次调用会再执行一遍。
//
// 默认记成普通记录：父节点怎么重跑都拿回，里面的副作用不重放。调用方声明了
// [WithRerunnable] 时记成「可重做」的那种（compose_v2.Rerunnable）：父节点的条件出边按
// CondFailRerunFrom 否决、要求父节点重新产出时，它跟着作废、重跑时重新调用。finalOnly 时只留
// 最后那条退出事件（见 nestedMode.finalOnly）。
func rememberNestedCall(ctx context.Context, call nestedCall, events []*AgentEvent, signals compose_v2.Signals, streams []string, streamsKnown, finalOnly bool) {
	if finalOnly {
		events = lastExitEvent(events)
	}
	rec := &nestedCallRecord{Events: events, Digest: call.digest, Signals: signals}
	var opts []compose_v2.StepOption
	if call.rerunnable {
		opts = append(opts, compose_v2.Rerunnable())
	}
	// 这次调用交出的流：被调方这次是从存档续的话，续跑之前交出的不在 streams 里，不声明（拿回时按说不清处理）
	if streamsKnown {
		opts = append(opts, compose_v2.WithStreams(streams))
	}
	if err := compose_v2.SaveStepResult(ctx, call.stepKey, rec, opts...); err != nil {
		slog.WarnContext(ctx, "agent_v2: 记不住这次已完成的嵌套调用，父节点重跑时它会再执行一遍",
			"ns", call.ns, "err", err)
	}
}

// lastExitEvent 返回 events 里 [FinalMessage] 取的那一条退出事件（没有就返回 nil）。
func lastExitEvent(events []*AgentEvent) []*AgentEvent {
	for i := len(events) - 1; i >= 0; i-- {
		if e := events[i]; e != nil && e.Action != nil && e.Action.Exit && e.Output != nil {
			return []*AgentEvent{e}
		}
	}
	return nil
}

// nestedCallNS 派生一次嵌套调用的子 ns，同时返回它的身份串。
//
// 先定出这次调用的**位置**：
//
//   - 调用方在哪：所在的节点（compose_v2.GetCurrentNodeID）与相位（compose_v2.SubCallScope：
//     节点体 / 某个图钩子 / 某条条件边 / 某次工具钩子调用 / 交给被调方之后它在自己图外的那一段；
//     节点级钩子与条件边的相位名含所属节点与它的第几次执行，图级钩子的含这张图的 ns，工具钩子的
//     含钩子名、工具名与所属 tool_call，跨图的区分另由 parentNS 那一段保证）。没有这一维时，
//     两个节点用同一份输入调同一个子 agent 撞成一次；钩子相位里连节点与 visit 都读不到，同一个
//     节点第 2 次执行时钩子里的调用撞上第 1 次的（实测终局 ErrRunAborted，报「答案已兑付」）。
//   - 实例维三选一：
//     图级 fan-out 实例（`SendFrom` 同 Target 投递出的 N 个实例之一，ISSUE-96 侧面 2）→ 实例
//     下标；所在的 tool_call（同一 tools 节点里的 N 个 tool_call，ISSUE-78）→
//     [toolCallInstanceKey]，实例节点里业务又自己起了 tool_call 并发时两维一起进位置；都不是
//     （静态路径）→ visit，本节点已成功完成的访问次数：父循环第 N 轮重进时子层与上一轮物理
//     分离，否则第 2 轮的提问会撞上第 1 轮的身份、被答案账本判为已兑付、终局 ErrRunAborted
//     （实测）。visit 只在成功路径递增、随 cp 持久化，续跑时算出同一个值。
//
// 再定出**这是哪一次调用**：
//
//   - 给了 call key（[WithCallKey]）：（位置, key）。同一个计数作用域（节点执行 / 每个 tool_call
//     / 每次图钩子或工具钩子调用 / 被调方在自己图外的那一段）里同一个位置上两次给同一个 key 报
//     ErrRunAborted（两次调用会共用一个身份）；compose_v2.RunWithRetry 的每次尝试从同一个编号数起，
//     重试不算两次。⚠️ agent 级钩子（`OnAgentStart` / `OnAgentEnd`）是同一段（这次 agent 执行在图外
//     的那一段），两个钩子里调同一个 agent 别用同一个 key。
//   - 只取位置（全历史输入且没有自定义 schema 的 AgentAsTool、skill fork）：（位置, 这个位置上的
//     第几次）。
//   - 都没有：（位置, 输入摘要, 同一份输入是第几次）。输入不同的调用天然不同；输入完全相同的
//     几次（投票、冗余调用）按发起次序区分（[compose_v2.NextSubCallIndex]）。
//
// 声明了 [WithRerunnable] 的调用，身份最后再带上调用所在分域的重做代际
// （compose_v2.RerunGeneration）：条件边否决之后重做的那一次是另一次调用，见 WithRerunnable。
//
// 存档槽是 `JoinFanoutNS(parentNS, name, hash(身份))`：`#` 本来就表示「同一个名字的另一个
// 实例」，ns 语法不变。同一个名字下的几次调用是兄弟执行，一次成功清理时只删自己的子树
// （见 compose_v2.IsNSInSubtree）。槽号是身份的哈希（见 hashNSIndex），两次输入不同的调用撞进
// 同一个槽时，被调方续跑时的身份核对会发现并拒绝（见 compose_v2.WithInputIdentity）。
func nestedCallNS(ctx context.Context, name, key, digest string, positional, rerunnable bool) (ns, identity string, err error) {
	parentNS := compose_v2.GetGraphNS(ctx)
	caller := identityString(string(compose_v2.GetCurrentNodeID(ctx)), compose_v2.SubCallScope(ctx))
	fanoutIdx, isInstance := compose_v2.InstanceFanoutIndex(ctx)
	// 在不在 tool_call 里看章，不看 ID 非空：有的 provider 不回 tool_call ID，那时同一批两个
	// tool_call 只能靠批内下标区分（见 compose_v2.InToolCall）
	inTC := compose_v2.InToolCall(ctx)
	tcID := compose_v2.GetCurrentToolCallID(ctx)
	var base string
	switch {
	case isInstance && inTC:
		// 实例节点里业务自己起了 tool_call 并发（自写的 toolsFn）：两维都要进位置，只取实例
		// 下标的话，同一个实例里的两个 tool_call 撞在一起。
		//
		// **不叠 visit**（传 0）：visit 按节点 ID 共享、只由没有实例身份的成员推进。同一个节点
		// 既是静态边目标又是 SendFrom 目标时，静态兄弟成功会把它推高，中断的实例续跑时读到的
		// 值就变了、身份跟着漂（实测：续跑换了一个提问 ID 重问，子 agent 从头跑）。实例身份每次
		// 投递都新铸，代际本来就在里面——同 InterruptSite.InterruptID 与节点私有态键的规则。
		base = identityString("instance", parentNS, name, caller, strconv.Itoa(fanoutIdx),
			toolCallInstanceKey(tcID, compose_v2.GetCurrentToolCallIndex(ctx), 0))
	case isInstance:
		base = identityString("instance", parentNS, name, caller, strconv.Itoa(fanoutIdx))
	case inTC:
		base = identityString("toolcall", parentNS, name, caller,
			toolCallInstanceKey(tcID, compose_v2.GetCurrentToolCallIndex(ctx), compose_v2.GetCurrentNodeVisit(ctx)))
	default:
		base = identityString("static", parentNS, name, caller, strconv.Itoa(compose_v2.GetCurrentNodeVisit(ctx)))
	}
	switch {
	case key != "":
		identity = identityString("key", base, key)
		if compose_v2.NextSubCallIndex(ctx, identity) > 0 {
			return "", "", fmt.Errorf("(ns=%q) 本次执行里同一个位置上对 %q 的调用已经用过 call key %q——两次调用会共用一个"+
				"身份；给每次调用不同的 key（重试同一次调用用 compose_v2.RunWithRetry 包住）: %w",
				parentNS, name, key, compose_v2.ErrRunAborted)
		}
	case positional:
		at := identityString("position", base)
		identity = identityString("position", base, strconv.Itoa(compose_v2.NextSubCallIndex(ctx, at)))
	default:
		sameInput := identityString("content", base, digest)
		identity = identityString("content", base, digest, strconv.Itoa(compose_v2.NextSubCallIndex(ctx, sameInput)))
	}
	if rerunnable {
		identity = identityString("rerun", identity, strconv.Itoa(compose_v2.RerunGeneration(ctx)))
	}
	return compose_v2.JoinFanoutNS(parentNS, name, hashNSIndex(identity)), identity, nil
}

// identityString 把几段拼成一个无歧义的身份串：每段带长度前缀，任何一段里出现什么字符都
// 不会跟相邻段混淆——直接拿分隔符拼的话，一段的尾巴和下一段的开头可以互换而拼出同一个串。
func identityString(parts ...string) string {
	var b strings.Builder
	for _, p := range parts {
		b.WriteString(strconv.Itoa(len(p)))
		b.WriteByte(':')
		b.WriteString(p)
		b.WriteByte(';')
	}
	return b.String()
}

// toolCallInstanceKey 是 tool_call 路径上「这是哪一次 tool_call」的那一段身份。
//
// tool_call ID 是模型给的，框架不假设它跨轮或批内唯一（有的 provider 每轮都叫
// `call_0`）。只拿 ID 定身份时，跨轮复用会让第 2 轮撞上第 1 轮的槽（那里可能留着一份
// 可续的失败存档，或第 2 轮的提问撞上第 1 轮已兑付的答案位）；批内重复会让两个并发
// 调用写同一个槽。所以叠上批内下标与 tools 节点的访问序号：下标锚在存档里那条 assistant
// 消息上，访问序号随 `NodeVisits` 持久化，续跑时都算出同一个值。
func toolCallInstanceKey(tcID string, idx, visit int) string {
	return identityString(tcID, strconv.Itoa(idx), strconv.Itoa(visit))
}

// hashNSIndex 把身份串哈希成 `#` 下标能用的非负整数：fnv64a 的低 63 位（SplitNS 拒绝负
// 下标）。固定 63 位、跟平台字长无关——按 int 取位数的话 32 位进程只剩 31 位，两种字长的进程
// 共用一个持久后端时同一次调用算出两个槽号、续跑找不到存档。
//
// 槽号是身份的有损投影：两次不同的调用落进同一个槽，后一次就会接到前一次的存档。位数决定
// 这件事的概率——31 位时一万次各留着存档的调用里撞一次的概率就到了百分之几，63 位时可以
// 忽略。撞了也不静默：不带 key 的调用由被调方的身份核对发现并拒绝（见
// compose_v2.WithInputIdentity）。
func hashNSIndex(s string) int64 {
	h := fnv.New64a()
	// hash.Hash 的 Write 按契约永不返回错误
	_, _ = h.Write([]byte(s))
	return int64(h.Sum64() & math.MaxInt64)
}
