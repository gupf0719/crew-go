package agent_v2

// 会话历史的**归属身份**——「这条消息是谁说的」用什么来标。
//
// # 为什么不能用 agent 自己的名字
//
// `Agent.Name()` 是**标签**，框架从不校验它唯一：两个分支挂同名 agent（甚至同一个
// 实例）时归属就撞了，它们的产出在历史里合并成一条流，下一轮互相把对方的消息当自己的
// prior 读回来，静默无报错。
//
// 而编排注册 sub-agent 时给的那个名字（`AddBranch("br1", …)` / `AddStep` /
// `AddAgent` / `AddSubAgent`）**框架是强制唯一的**（重名当场 panic），它才是那个 agent
// 在这棵编排树里的位置。位置唯一，归属就唯一。
//
// # 形态：斜杠分隔的位置路径
//
//	""                       顶层（没有父编排）
//	"br1"                    ParallelAgent 的 br1 分支
//	"outer/inner/actor"      嵌套三层
//
// 路径由父编排在调 sub-agent 前 push 进 ctx，子 agent 记历史时读出来当归属。
//
// # 只放注册名，不放轮次 / 跳数 / 下标
//
// 这是跟 graphNS（cp 寻址用）的**关键区别**，也是这套设计能成立的前提：
//
//	graphNS:  agent_billing_hop2   round_3   plan_step_7
//	归属路径: agent_billing        body      plan
//
// graphNS 要区分「同一个位置的第 N 次执行」（不然 cp 互覆），所以带运行时序号；归属要
// 回答「谁说的」，而同一个 agent 在同一位置、第 1 轮和第 3 轮说的话**归属本来就该相同**
// ——Loop 的 body 跑十轮都是 body，Handoff 的 front 在第 0 跳和第 2 跳都是 front。
//
// 更要命的是跳数/轮次**跨 Run 会重置**（hops 存 ctx、每个 Run 从 0 起）：同一个 agent
// 在 Run 1 记成 `agent_billing_hop1`、Run 2 续接时变 `agent_billing_hop0`，按它归属就
// **读不回自己上一轮的历史**——那正是这一层存在的理由。所以带序号的段一律剔掉。
//
// # 代价：归属是编排结构的函数，改结构就是换身份
//
// 位置来自编排注册名和嵌套层次，所以**改分支名、改步骤名、加一层 wrapper、把 agent 从
// 顶层挪进编排（或反过来）**，都会让它跟旧历史里的归属对不上——旧消息还在，但按新归属
// 读不回来，模型退化成失忆。要跨 Run 续接同一场会话，编排树的注册名得保持稳定。
//
// 这比旧方案（用 `Agent.Name()`）的脆弱面**更大**：旧的只有改名一个触发点。但方向仍是
// 改进，因为失效模式换了档：
//
//	旧：静默**合并**——两个同名 agent 的历史混成一条，互相把对方的话当自己的读回来，
//	    污染模型输出，而且越聊越乱
//	新：静默**丢失**——读不到旧历史，模型从这轮起失忆，但不会读到错的东西
//
// 丢失比污染轻：前者是能力退化，后者是给模型喂假上下文。两者都不报错，这是这一层
// 「旁路」定位的固有代价（见 historyWriter godoc）。
//
// ⚠️ **失效时是彻底空，不是退化到 events**：默认视图（SelfOnly）下归属对不上的消息被
// 整条丢弃，而 `historyPrior` 仍报「读成功」（后端确实读到了，只是没有属于我的），于是
// 调用方的「读失败才回落 session events」那条路走不到。业务观察到的症状是**完全失忆**
// 而不是「历史少了几条」——排查时别按「后端出问题了」的方向找。
//
// # 同一个 agent 挂在两个位置时，历史是分开的
//
// 框架自身就能走到这个形状：Routing 的某分支和 fallback 用同一个 agent、Plan 的子 agent
// 同时兼任 executor、Sequential 把同一个 agent 挂两步。此时它在两个位置的历史**互不可见**。
//
// 这是「位置即身份」的直接推论，多数时候正是想要的——同一个 writer 挂 `draft` / `polish`
// 两步时，polish 不该把 draft 的产出当成自己上一轮说的话（单个 Run 内不受影响：编排会把
// 上一步产出当派生 input 喂进去）。但若业务**有意复用一个专家 agent** 跨多个位置、指望它
// 有一份连续记忆，那份记忆现在拿不到——没有报错，只是 prior 变空。
//
// # 顶层为空是有意的
//
// 顶层 agent 没有父编排、拿不到注册名，此时回落 `Agent.Name()`。顶层同一时刻只有一个
// agent 在跑，不存在"谁跟谁撞"的问题；而回落到名字还能让业务回看历史时读到有意义的
// 标识，比空串友好。

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sync/atomic"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// historySlotCtxKey 是归属路径的 ctx key（私有类型防碰撞）。
type historySlotCtxKey struct{}

// WithHistorySlot 声明「接下来这个 sub-agent 在编排树里的位置」。
//
// 编排在调 sub-agent 前 push，push 的是**本层的注册名**（`AddBranch` / `AddStep` /
// `AddAgent` 那个 name），框架自动拼上父路径。
//
//	subCtx := agent_v2.WithHistorySlot(ctx, br.Name)   // "br1"，父路径自动带
//
// 业务自写编排（GraphAgent 的 NodeFunc 里调 sub-agent）时同样该 push，否则子 agent 的
// 产出会回落到用 agent 名标归属——**同名就会合并**。`RunSubAgent` helper 已经代劳。
//
// name 为空时原样透传（不产生空段）。
//
// 同时开一张**回执**：从这里往下实际产生了多少条历史记录（见 historyReceipt）。边界补记
// 据此判断「这一步的执行体到底记了没有」。
func WithHistorySlot(ctx context.Context, name string) context.Context {
	if name == "" {
		return ctx
	}
	parent := HistorySlot(ctx)
	if parent != "" {
		name = parent + "/" + name
	}
	ctx = openHistoryReceipt(ctx)
	return context.WithValue(ctx, historySlotCtxKey{}, name)
}

// historyReceipt 记「从声明它的那一层往下，实际产生过多少条历史记录」。
//
// # 为什么不能用「位置有没有被标记过」
//
// 边界补记存在的唯一理由是**这一步的执行体可能什么都没记**（远程 agent / 自研 agent /
// 适配别的框架的壳，它们连历史层的存在都不知道）。原来的判据是「位置 X 有没有被标记过」，
// 而位置在**两个方向上同时是错的**：
//
//   - **太细**：叶子记在 `par/br1/s1`，中间那层的边界查的是它自己那一段，精确匹配对不上
//     → 同一条产出记两遍 → 第二份归属不同，读回时被判成「别人说的」走 Rewrite 变成
//     `For context: [X] said:`。模型会以为另一个 agent 独立得出了相同结论——**污染比丢失重**。
//   - **太粗**：Loop 的多轮、Handoff 的多跳共用同一个位置，第 1 轮标记之后第 2 轮起全被
//     挡掉 → 中间轮次的产出静默丢。
//
// 回执问的是另一个问题：**这次调用的范围内有没有真的产生记录**。它是运行期观测，不是静态
// 结构判断——这点很关键：`Agent` 是开放接口，业务能塞任何实现进编排，任何「框架静态遍历
// 编排树」的方案都存在探不到的层。回执**不遍历任何东西**，业务实现什么都没记就是计数为 0，
// 而那恰恰是它**应该被补记**的正确答案。
//
// # 为什么要沿父链上溯
//
// 外层问的是「我底下有没有发生过」，不是「紧邻我这一层有没有」。`Par(Seq(Basic))` 里叶子
// 记完，Sequential 和 Parallel 两层的边界都该判「记过了」。
//
// # 并发
//
// 每条分支各自派生 ctx ⟹ 各自拿到独立的回执，扇出时分支之间互不干扰。计数用 atomic：
// 同一张回执会被多个后代分支并发 bump。
type historyReceipt struct {
	parent *historyReceipt
	n      atomic.Int64
}

// bump 记一笔，并沿父链一路上报。
func (r *historyReceipt) bump() {
	for cur := r; cur != nil; cur = cur.parent {
		cur.n.Add(1)
	}
}

type historyReceiptCtxKey struct{}

// openHistoryReceipt 开一张新回执，父指向当前这张。
func openHistoryReceipt(ctx context.Context) context.Context {
	r := &historyReceipt{parent: historyReceiptFrom(ctx)}
	return context.WithValue(ctx, historyReceiptCtxKey{}, r)
}

func historyReceiptFrom(ctx context.Context) *historyReceipt {
	r, _ := ctx.Value(historyReceiptCtxKey{}).(*historyReceipt)
	return r
}

// bumpHistoryReceipt 在实际写入历史时记一笔。没有回执（顶层直接跑）时是 no-op。
func bumpHistoryReceipt(ctx context.Context) {
	if r := historyReceiptFrom(ctx); r != nil {
		r.bump()
	}
}

// producedHistoryHere 报「这次调用的范围内有没有产生过历史记录」。
func producedHistoryHere(ctx context.Context) bool {
	r := historyReceiptFrom(ctx)
	return r != nil && r.n.Load() > 0
}

// HistorySlot 返回当前 agent 在编排树里的位置路径；顶层返空串。
func HistorySlot(ctx context.Context) string {
	if v, ok := ctx.Value(historySlotCtxKey{}).(string); ok {
		return v
	}
	return ""
}

// historyOwner 定这一层记历史时该盖什么归属。
//
// 有位置就用位置（编排强制唯一），没有就回落 agent 名（顶层场景，见本文件头部说明）。
func historyOwner(ctx context.Context, agentName string) string {
	if slot := HistorySlot(ctx); slot != "" {
		return slot
	}
	return agentName
}

// settleHistoryOnPause 在子调用以「可 Resume 的方式」退出时结算**输入**——只落输入，
// 产出这时还没有。
//
// # 为什么中断时就要落，不等成功
//
// 输入的持久点应在**跨进边界那一刻**，不在「执行完成那一刻」——完成与否只影响产出，
// 不影响输入（对照文档「结论七」：查了四家，没有一家把输入的持久化系在执行完成上）。
// 边界补记原来把输入的落笔系在「子调用成功」上，于是外部执行体（远程 agent / 业务
// 自研壳——它们不知道历史层的存在）中断时，输入只活在父编排的调用栈里：经父 Resume
// 那条路能自愈（step 重跑成功、补记跟上），但调用方**直接** Resume 子时输入永久缺席，
// 历史落成「有答无问」。cp 那一半没有这个洞（原始输入在父的 channel 里随 cp 走、父
// Resume 时恢复），缺的只是历史层这半。
//
// # 判据与成功路径的输入半边同一套（走 settleHistorySlot）
//
//   - BasicAgent 子：preamble 在本次调用里已记 ⟹ 范围回执 >0 ⟹ 跳过，不双记
//   - 外部执行体：什么都没记 ⟹ 落输入，owner = 父给的位置
//   - 父 Resume 重跑成功后的补记：跨 Run 守卫（inputAlreadyInHistorySlot，同一条流上
//     查落盘记录）认出这条 ⟹ 不再记
//   - 再次中断（多轮 HITL）：同上被跨 Run 守卫挡住
//   - Loop 第 2 轮起的输入是上一轮产出（assistant 角色）⟹ Role==user 过滤掉，不落垃圾
//
// # 只在「这个错会留下可 Resume 的存档」时做
//
// 判据是 `LeavesResumableCheckpoint`——框架对这个错的**处置意图**，不是错的类型（枚举
// IsInterrupt 会漏软取消 / CondFailRerunFrom 两档，GraphAgent 那轮踩过）。纯业务硬错
// 不落：cp 标 Failed、这一层不可 Resume，「报错的外部执行体不记」是既有语义。
func settleHistoryOnPause(ctx context.Context, agentName string, input *schema.Message, err error) {
	if err == nil || !compose_v2.LeavesResumableCheckpoint(err) {
		return
	}
	settleHistorySlot(ctx, agentName, input, nil)
}

// settleHistorySlot 在编排调完 sub-agent 之后补记这一步——**只补执行体自己没记的**。
//
// # 为什么需要它
//
// 历史的写入点在 BasicAgent 的三个节点里。但编排收的是 `Agent` 接口，业务塞进来的可以是
// 任何实现：远程 agent、自研 agent、适配别的框架的壳——它们连历史层的存在都不知道，跑完
// 什么也不留。
//
// 后果不是「少一步」而是**整条断链**：`Sequential(远程A, 本地B)` 里，第一步的输入交给远程
// agent（没人记），第二步的输入是编排合成的派生输入（按判据跳过不记，因为「成分都能从历史
// 重算」——但那个前提此刻不成立了）。实测结果是历史里只剩本地 B 的一条产出，用户原话和远程
// 产出双双缺席，下一轮模型看到一个没有任何前因的结论。
//
// # 判据：输入和产出各用各的尺
//
// 两样东西问的不是同一个问题，用一把尺必然占其一失效模式：
//
//   - **输入**问「这一步归谁记」。执行体只要往历史里写过一条（`producedHistoryHere`，
//     见 historyReceipt），输入就是它的事，边界一律不插手。这条同时挡住一个安全问题：
//     `OnAgentStart` 是 [消费] 档，业务可以在那儿把消息整条换掉（脱敏是 CLAUDE.md 举的
//     例子），叶子记的是脱敏版，而边界握着的是**闭包里改写之前**那条、带原文——按范围
//     判就不会写进去。
//   - **产出**问「**这次调用范围内**有没有人记过这条消息」（范围 + 全局登记表按指针）。
//     透传上来的（执行体记过的同一条）跳过；执行体**加工过**再返回的是新的一条，照记
//     ——不然「既委派又加工」的执行体（业务 NodeFunc 拼接多路结果是常规用法）产出会丢。
//     **范围这道前提不能省**：执行体可以每轮返回同一个消息对象（`var okMsg = ...` 在 Go
//     里是很自然的写法），只问全局登记表就把「同一个对象」当成「同一条产出」，Loop 第 2
//     轮起静默丢——正是这套改动要消灭的形状换了个触发条件。
//
// 判据都不看 agent 类型——业务自研的 agent 只要自己接了历史层照样算「记过了」，不会被
// 当外部 agent 补一遍。
//
// 输入那条还有一道 `Role == user` 的过滤，跟 BasicAgent 自己记输入时同款
// （`basic_agent.go`）：编排把上一轮的 assistant 产出转手当下一轮的输入是常规形状，
// 那不是「用户说了句新的话」。
//
// ⚠️ **它现在是纵深防御，不是承重的那道**：消息的身份改成构造期铸造之后（见
// `rootInputID`），身份跨拷贝存活，上一轮产出转手当输入时按身份就认得出来，下面那道
// 认领已经接管了这个形状。实测撤掉这行**零测试失败**。留着是因为语义上它仍然对——
// assistant 消息不是「用户说了句新的话」——而且它挡的是「身份判据够不着」的残余情形
// （产出在 A 位置记的、转手到 B 位置当输入）。**改判据时别把它当哨兵**：它红不了。
//
// 输入那条**还要再问一遍身份，但问的是「在这条流里」**（`claimRecordHere`）：Loop 第 N
// 轮的输入就是第 N-1 轮的产出、同一条流 ⟹ 跳过；而 SelfOnly 扇出的兄弟分支共享同一条
// 输入身份、是不同的流 ⟹ 各记各的（每条分支流要各自完整，有问有答）。用全局判会变成
// 「谁先谁赢」，实测并发下份数随调度在 1~4 之间翻。
//
// 两处认领都是**查与登记一次完成**，不是「先查后写」——中间那个窗口正是随调度翻的来源。
//
// # 记什么
//
// 输入 + 最终产出，都盖这一步的位置。中间过程（工具往返、多轮）拿不到也不该编——外部
// agent 的内部过程本来就不属于这条会话，父这一层看到的就是「给了它什么、它返回了什么」。
//
// input 为 nil 或 final 为 nil 时各自跳过（有什么记什么）。
func settleHistorySlot(ctx context.Context, agentName string, input, final *schema.Message) {
	settleHistoryAt(ctx, HistorySlot(ctx), agentName, gateExtentProduced, input, final)
}

// settleHistoryTopLevel 让**顶层执行体自己**结算边界，位置取自己的名字。
//
// # 为什么需要它（而不是让顶层往 ctx 里推一个 slot）
//
// `settleHistorySlot` 的 `slot == "" → return` 那道守卫拦的是「顶层不归编排管」——那对
// 有静态子列表的 7 编排成立（叶子自己会记，wrapper 那层不需要记）。GraphAgent 不成立：
// 它的图可以全是 NodeFunc，一个会记的叶子都没有，而它自己的 InputWriter / OutputReader
// **就是**这条流的主路产出。
//
// 让它往 ctx 里推 `WithHistorySlot(ctx, name)` 也能过守卫，但**副作用是把自己的名字变成
// 所有后代归属的根段**：图里经 RunSubAgent 调起的子从 `worker` 变成 `<图名>/worker`。
// 实测那样有两个代价——(1) 顶层图改个名，它下面所有子的历史全部变孤儿（改动前改名零
// 影响）；(2) 同一张图，配置走 `WithHistoryCtx` 时归属带前缀、业务把 `WithHistory` 挂在
// 叶子上时归属是裸的，两种配法互不相认。而这两个代价换来的「跟嵌套时形状一致」其实只是
// 段数一致：嵌套时前缀取的是**父给的注册名**，顶层取的是**agent 自己的名字**，两个命名域。
//
// 所以位置只传给这一次结算（做认领的流键 + 过守卫），不进 ctx。归属仍按
// `historyOwner` 的既定回落取 agentName，后代归属逐字不变。
func settleHistoryTopLevel(ctx context.Context, agentName string, input, final *schema.Message) {
	settleHistoryAt(ctx, agentName, agentName, gateInputRecorded, input, final)
}

// inputGate 决定输入那条问哪个问题——两个调用方的**前提不同**，用同一个问题会错一个。
type inputGate int

const (
	// gateExtentProduced：「这次调用范围内有没有产生记录」。
	//
	// 边界补记（`settleHistorySlot`）用它，前提是**执行体收到的就是这条输入**：编排把
	// 输入原样传给子，子只要往历史里写过一条，输入就是它的事。7 编排都满足这个前提。
	gateExtentProduced inputGate = iota

	// gateInputRecorded：「这条输入本 Run 记过没有」。
	//
	// 顶层自结算（`settleHistoryTopLevel`）用它，因为上面那个前提在它身上**不成立**：
	// 那里的「执行体」是整张图，而图底下的子 agent 记的是它自己的**派生输入**。
	// 「有人记过东西」≠「有人记过这条输入」——图一委派派生任务，用户原话就 0 条
	//（实测：委派形状原话 0，转发形状原话 1；中断→Resume 那条更要紧，中断出口结算本来
	// 就是专门为「Resume 拿不到原始 input」加的，被同一道闸绕过就白加了）。
	//
	// ⚠️ **不能把边界补记也换成这个**：实测会红 5 条（改写 hook 防原文泄漏 ×2 /
	// 扇出每分支各记一份 / 转交跨进程不重复 / 扇出中断 Resume 与基线一致）——那道闸对
	// 边界补记是承重的。
	gateInputRecorded
)

// settleHistoryAt 是上面两者的共同实现——slot 与输入闸门显式传入。
func settleHistoryAt(ctx context.Context, slot, agentName string, gate inputGate, input, final *schema.Message) {
	w := historyWriterFrom(ctx)
	if w == nil || historySuppressed(ctx) || slot == "" {
		return
	}

	inputTakenCareOf := func() bool {
		if gate == gateInputRecorded {
			return recordedInThisRun(ctx, input)
		}
		return producedHistoryHere(ctx)
	}

	// 认领放在**最后一个条件**：抢到就一定会记，不会登记了却没写。
	var msgs []*schema.Message
	if input != nil && input.Role == schema.RoleUser && !isDerivedInput(input) &&
		!inputTakenCareOf() && !inputAlreadyInHistorySlot(ctx, slot, input) &&
		w.claimRecordHere(slot, input) {
		msgs = append(msgs, input)
	}
	// 产出只在「这次调用范围内确实记过东西」时才查全局登记表——见下方判据说明。
	// 产出也要跳过「可重算的合成物」——判据跟输入侧同源（成分都已在历史里）。典型是
	// ParallelAgent 默认 merger 拼出来的那条：不跳的话成分和聚合并存，模型读回时同一份
	// 内容看到两遍。编排在源头决定打不打标（真合成的产出不打，必须留档）。
	//
	// **还要挡住「产出就是刚记下的那条输入」**：业务的 `GraphOutputReader` 在出口 channel
	// 空时回落返 input（`if out == nil { return in }`）是相当自然的写法——它的 godoc 自己
	// 就建议别返 nil。那种情况下 msgs 会变成 `[input, input]`：上面那行的
	// claim 已经把登记表置上了，但 `producedHistoryHere` 在纯 NodeFunc 图里是 false，
	// `&&` 把整道守卫短路掉。后果是下一轮模型读到问题两遍、中间没有答案。
	if final != nil && !isDerivedInput(final) && !sameMessage(msgs, final) &&
		!(producedHistoryHere(ctx) && w.alreadyRecorded(final)) &&
		!finalAlreadyInOtherSlot(ctx, slot, final) {
		msgs = append(msgs, final)
	}
	if len(msgs) == 0 {
		return
	}
	recordHistory(ctx, historyOwner(ctx, agentName), agentName, msgs)
}

// inputAlreadyInAnyStream 报这条输入是不是已经在历史里了——**不限归属**。
//
// 只给**共享单流**的位置用（HandoffAgent 转交模式）：整场对话是一条流，原始输入在流里
// 任何归属下已在，就不该再落——「共享视图下同一条原始输入只记一份」是既有语义。
//
// ⚠️ **SelfOnly 扇出（Parallel 分支）不能用这个**：每条分支流要各自完整（有问有答），
// 兄弟分支先落的那份**不该**挡住我——那边走 per-slot 的 `inputAlreadyInHistorySlot`。
// 同一个「限不限归属」的开关，两类位置的正确答案相反（实测：Handoff 跨进程 Resume 用
// per-slot 查会把输入落成两份、扇出用 any-stream 查会让后到的分支流缺问）。
//
// 只在**当前任务**的记录里找（按消息 ID 与任务章查，见 [findHistory]）：要挡的是同一个任务里转交的下一跳、
// 续跑重跑到这一步时再记一份，那些记录都是本任务写的。
func inputAlreadyInAnyStream(ctx context.Context, input *schema.Message) bool {
	if input == nil {
		return false
	}
	nonce := rootInputID(input)
	if nonce == "" {
		return false
	}
	hit, _ := findHistory(ctx, nil, HistoryQuery{MessageID: nonce}, true, anyHistoryRecord)
	return hit
}

// finalAlreadyInOtherSlot 报这条产出是不是已经**在别的归属下**落盘了。
//
// **补的是「跨调用」这一维**：上面那两道守卫都只看本次调用——范围回执每次 Run/Resume 新建、
// 全局登记表挂在写入器上同样每次新建，而 Resume 不重跑已完成节点 ⟹ Run 里子 agent 记过的
// 产出，在 Resume 那次调用里两道守卫都「查无此记」，于是顶层出口又记一遍，同一指针两个归属
// （实测：中断跨 Resume 的透传形状，【子产出】记 2 条，owner 分别是 n1 和图名）。
//
// 顺序上放在最后：前两道是内存判断、这道要读历史。按消息 ID 查（[findHistory] 的 own=false），只认**这个任务
// 这一路**写下的：任务章是本任务的，或者是从本任务脱离出去的独立任务写的（节点里经 DetachTask 开的独立任务
// 推回同一个会话、它的产出被父图透传时，那份是独立任务写的，见 historyRecordOfTask）。别的任务写下的同一个
// ID 不算：执行体每次返回同一个消息对象时，前一个任务在 x 分支记过、这个任务在 y 分支产出它，那是 y 的回复、
// 不是透传——按整场会话查会把它吞掉（审计实测，会话停在 y 的提问上）。
func finalAlreadyInOtherSlot(ctx context.Context, slot string, final *schema.Message) bool {
	if final.ID == "" {
		return false
	}
	task := currentWriterTask(ctx)
	hit, _ := findHistory(ctx, nil, HistoryQuery{MessageID: final.ID}, false, func(m *schema.Message) bool {
		return historyRecordOfTask(m, task) && priorHasMessageIDInOtherSlot([]*schema.Message{m}, final.ID, slot)
	})
	return hit
}

// sameMessage 报 m 是不是已经在 msgs 里——同指针或同身份都算。
//
// 同身份也要算：业务可能把 input 拷一份再返（`*in` 解引用赋值是常见写法），拷贝跟原件
// 指针不同但 `Message.ID` 是构造期铸的、跨拷贝存活。
func sameMessage(msgs []*schema.Message, m *schema.Message) bool {
	for _, x := range msgs {
		if x == m {
			return true
		}
		if x.ID != "" && x.ID == m.ID {
			return true
		}
	}
	return false
}

// historyConfigProvider 让编排能看到 sub-agent 的历史配置。
//
// 编排收的是 `Agent` 接口，本来看不到配置——但**写入器必须由编排这一层拥有**，否则
// 编排调用「不是 BasicAgent 的执行体」时 ctx 里还没有写入器（它只在 BasicAgent.Run
// 里建、且活在它自己的子 ctx 里），边界补记无从落笔。
type historyConfigProvider interface {
	historyCfgForOrchestration() *historyConfig
}

// historySubsProvider 让借配置的那一趟能穿过嵌套的编排：编排交出自己的直接子执行体（见 history_subs.go）。
type historySubsProvider interface {
	historySubs() []Agent
}

// borrowHistoryConfig 从 subs 的整棵静态子树里借历史配置：BasicAgent 交出自己的，编排往下走自己的子执行体（嵌套的
// 编排也一样，见 walkHistorySubs）。整棵树都没配时返回 (nil, nil)；有两处不同时返回冲突的那两处。
//
// 往下走是为了让最外层的编排拥有写入器：只看直接子时，`Sequential[Sequential[chat]]` 的外层借不到配置、入口不开写入器，
// 内层在外层的节点里开——这场对话唯一的写入器成了嵌套写入器，调用方提供的前文与历史层就分不清谁说了算（见
// caller_history.go）。GraphAgent 没有静态子列表，走不进去；它的配置只能来自 WithHistoryCtx。
func borrowHistoryConfig(subs []Agent) (*historyConfig, *historyConflict) {
	var chosen *historyConfig
	var chosenBy string
	var conflict *historyConflict
	exhausted := walkHistorySubs(subs, func(sub Agent) bool {
		hp, ok := sub.(historyConfigProvider)
		if !ok {
			return true
		}
		hc := hp.historyCfgForOrchestration()
		switch {
		case hc == nil:
		case chosen == nil:
			chosen, chosenBy = hc, sub.Name()
		case !sameHistoryConfig(chosen, hc):
			conflict = &historyConflict{names: [2]string{chosenBy, sub.Name()}, cfgs: [2]*historyConfig{chosen, hc}}
			return false
		}
		return true
	})
	if exhausted && historyBorrowExhaustedWarned.CompareAndSwap(false, true) {
		slog.Warn("agent_v2: 借历史配置时子执行体超过上限还没走完——组合里多半有不可比较的值类型包装成的环，"+
			"更深处配的历史层借不到（本条每个进程只记一次）", "limit", historyBorrowMaxVisits)
	}
	if conflict != nil {
		return nil, conflict
	}
	return chosen, nil
}

// historyBorrowMaxVisits 是借配置这一趟最多看多少个子执行体。嵌套编排的规模远到不了这个数；到了多半是不可比较的值
// 类型包装成了环（认不出走过，分叉的环按层数算是指数级）。
const historyBorrowMaxVisits = 1024

var historyBorrowExhaustedWarned atomic.Bool

// walkHistorySubs 深度优先把 subs 的整棵静态子树逐个交给 visit（visit 返回 false 时停下）：编排往下走它的子执行体
// （historySubsProvider）。走过的不再走——同一个实例出现在几处、组合成了环（转交候选里有自己）：按接口值本身认，指针按
// 地址、内嵌编排指针的值类型包装按值。曾经只认指针，值类型包装成环时无限递归、整个进程栈溢出（不配历史层也一样，每个
// 编排的 Run 入口都要走这一趟）。不可比较的值认不出走过，靠总数上限停下。返回是不是因为看满上限停下的。
func walkHistorySubs(subs []Agent, visit func(Agent) bool) (exhausted bool) {
	visited := make(map[any]bool)
	budget := historyBorrowMaxVisits
	var walk func(subs []Agent) bool
	walk = func(subs []Agent) bool {
		for _, sub := range subs {
			if isAgentNil(sub) {
				continue
			}
			if reflect.ValueOf(sub).Comparable() {
				if visited[sub] {
					continue
				}
				visited[sub] = true
			}
			if budget == 0 {
				exhausted = true
				return false
			}
			budget--
			if !visit(sub) {
				return false
			}
			if sp, ok := sub.(historySubsProvider); ok && !walk(sp.historySubs()) {
				return false
			}
		}
		return true
	}
	walk(subs)
	return exhausted
}

// historyConflict 是借配置时两处不一样的历史配置：哪两个 agent、各自的配置。
type historyConflict struct {
	names [2]string
	cfgs  [2]*historyConfig
}

// Error 说清哪两个 agent、差在哪：owner 不同、owner 函数不是同一个、store 不是同一个。
func (c *historyConflict) Error() string {
	a, b := c.cfgs[0], c.cfgs[1]
	var diff string
	switch {
	case a.owner != b.owner:
		diff = fmt.Sprintf("owner %q vs %q", a.owner, b.owner)
	case !sameOwnerFunc(a, b):
		diff = "WithHistoryOwnerFunc 不是同一个函数（按函数值比：返回值相同的两个函数也算不同）"
	default:
		diff = "不是同一个 HistoryStore"
	}
	return fmt.Sprintf("sub-agents %q and %q declare conflicting history config (%s)", c.names[0], c.names[1], diff)
}

// startHistoryForOrchestration 让编排在自己这一层建历史写入器。
//
// 配置从 sub-agent 借（整棵静态子树，见 [borrowHistoryConfig]）：业务是在 BasicAgent 上配 `WithHistory` 的，编排自己
// 没有这个 option。「一场会话一条流」本来就意味着同一编排下的子 agent 共用一个写入器。
//
// # 配置不一致时 fail-fast，不静默挑一个
//
// 原来取的是「第一个配了的」，理由写作「谁的配置不重要」。**那句话在 owner 和 store
// 两个维度上不成立**：子 agent 配了不同的 store 或不同的 owner 时，整场对话落到哪里
// 取决于挑中了谁。而 Handoff / Plan 的子是 `map[string]Agent`，`for range` 顺序每次
// 都不同——实测连跑 6 次，同一套编排把对话写进了 A/A/B/B/A/A 两个不同租户的 store。
// 跨 Run 的后果更糟：Run 1 写租户 A、Run 2 写租户 B、Run 3 读到哪半看运气，而落进 B
// 的内容本身是 A 的对话。
//
// 所以配置只要有一处不同就直接报错。常见情形（所有子共用同一个 store 和 owner）不受
// 影响；`historySubs()` 那边也已经把 map 的迭代顺序定死，报错信息跨次稳定。
//
// 幂等：ctx 里已有写入器（更外层编排建过）或这条分支被摘除时直接返回。都没配历史时
// 返回原 ctx + no-op，零开销。
//
// **写入器的生命周期因此变成整个编排**而不是某一步：出口 closeAndJoin 排空所有子
// agent 的写入，这也是「一场会话一条流」该有的形状。
func startHistoryForOrchestration(ctx context.Context, subs []Agent, name string) (context.Context, func(), error) {
	noop := func() {}
	if historyWriterFrom(ctx) != nil || historySuppressed(ctx) {
		return ctx, noop, nil
	}
	// 运行入口声明优先——它更靠近这次运行，也是唯一能覆盖「编排里一个 BasicAgent 都
	// 没有」那种形状的声明方式。见 WithHistoryCtx。
	if hc := historyCfgFromCtx(ctx); hc != nil {
		c, stop := startHistory(ctx, hc, name)
		return c, stop, nil
	}

	chosen, conflict := borrowHistoryConfig(subs)
	if conflict != nil {
		// 报出冲突的是哪两个 agent、差在哪——不然业务在一棵嵌套的编排树里找不到
		return ctx, noop, fmt.Errorf("agent %q: %s — 一场会话只能有一条流，框架无法替业务选租户；"+
			"请让同一编排下（含嵌套的编排）的子 agent 用同一个 WithHistory 配置（同一个 store 实例、同一个 owner 或同一个 owner 函数）",
			name, conflict.Error())
	}
	if chosen == nil {
		return ctx, noop, nil
	}
	c, stop := startHistory(ctx, chosen, name)
	return c, stop, nil
}

// sameHistoryConfig 判两个子的历史配置是不是同一套。
//
// 只要有一处不同就算冲突——落错租户是静默的数据事故，宁可误报也不漏报。
func sameHistoryConfig(a, b *historyConfig) bool {
	if a.owner != b.owner || !sameOwnerFunc(a, b) {
		return false
	}
	return sameHistoryStore(a.store, b.store)
}

// sameOwnerFunc 判两份配置的 owner 函数是不是同一个（按函数值比）。
func sameOwnerFunc(a, b *historyConfig) bool {
	if (a.ownerFunc == nil) != (b.ownerFunc == nil) {
		return false
	}
	return a.ownerFunc == nil || reflect.ValueOf(a.ownerFunc).Pointer() == reflect.ValueOf(b.ownerFunc).Pointer()
}

// sameHistoryStore 判两个 store 是不是同一个。
//
// 不能直接用 `==`：`HistoryStore` 是接口，业务的实现若是不可比较的值类型（含 map /
// slice 字段的 struct 按值实现接口），`==` 会 panic。按种类分别处理，比不了的按类型
// 相同当作同一个——那一档宁可漏报也不误伤。
func sameHistoryStore(a, b HistoryStore) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	ra, rb := reflect.ValueOf(a), reflect.ValueOf(b)
	if ra.Type() != rb.Type() {
		return false
	}
	switch ra.Kind() {
	case reflect.Pointer, reflect.UnsafePointer, reflect.Chan, reflect.Map, reflect.Func:
		return ra.Pointer() == rb.Pointer()
	}
	return true
}

// inputAlreadyInHistorySlot 报这条输入是不是已经在**指定那条流**的历史里了。
//
// 转交场景下同一句原始输入会透传给每一跳，各跳都调边界补记——不查一下就会记 N 份。
// 判据跟 BasicAgent 那边同源：输入自带的身份（见 rootInputID），扫历史
// 找有没有同 nonce 的。没有 nonce 的输入（非转交场景）直接放行——那种情况下每个位置
// 收到的本来就是不同的输入。
// ⚠️ **slot 必须由调用方传进来，不能自己从 ctx 取**：`settleHistoryAt` 的 slot 是显式参数，
// 顶层自结算那条路上它等于 agentName 而 `HistorySlot(ctx)` 恒是空串。自己取 ctx 就变成
// 「拿空串去比对 owner=agentName 的记录」——恒不命中，整道守卫在顶层路径上是死代码
// （实测后果：同一个 sid 上 Run→中断→再 Run，即便调用方传的是**同一个 message 指针**，
// 用户那句话也会被记两遍）。
func inputAlreadyInHistorySlot(ctx context.Context, slot string, input *schema.Message) bool {
	nonce := rootInputID(input)
	if nonce == "" {
		return false
	}
	// 按流限定：跨 Run 续接时前一个 Run 在**这条流**上的记录能挡住重记，而兄弟分支
	// 刚写进去的那份挡不住我——每条分支流各自要完整（有问有答）。只查当前任务的记录：
	// 要挡的「同一个 sid 上 Run → 中断 → 再 Run」是同一个任务。归属在 match 里比——slot 为空时
	// 比的是「没有归属」，不能当成「不按归属过滤」。
	hit, _ := findHistory(ctx, nil, HistoryQuery{MessageID: nonce}, true, func(m *schema.Message) bool {
		return priorHasRootInputIDInSlot([]*schema.Message{m}, nonce, slot)
	})
	return hit
}

// resolveHistoryConfigForAbort 解析这次调用该往哪个历史里写——ctx 声明优先，
// 否则从 sub-agent 借（同 [startHistoryForOrchestration] 的判据，只是不建写入器）。
//
// 配置不一致时**不报错，返 nil**：这条路是 Abort 的收尾补记，不该在业务放弃任务的时候
// 再抛一个配置错。真正的 fail-fast 留在 `startHistoryForOrchestration`——正常跑一次就会撞上。
func resolveHistoryConfigForAbort(ctx context.Context, subs []Agent) *historyConfig {
	if hc := historyCfgFromCtx(ctx); hc != nil {
		return hc
	}
	chosen, _ := borrowHistoryConfig(subs) // 冲突时 chosen 为 nil：这条路上不报错，理由见上
	return chosen
}

// closeUnansweredToolCallsOnAbort 在**用户放弃任务**（`ResumeActionAbort`）那一刻，
// 给历史里没有配对结果的 tool_call 补上一条工具结果。
//
// # 为什么补在写侧而不是读侧
//
// 不补的话，历史（append-only）里**永久**留着一条 `assistant tool_calls=1 content=""`
// ——tools 节点是原子的，审批中断时结果那半根本没写过，而放弃之后也不会再有人写。
// 实测复现：审批中断 → `WithResumeAction(Abort)` → 历史停在 `[user, assistant(tc=1)]`。
//
// 框架下游对这种残缺**已经有一整套读侧修补**（`dropDanglingToolCalls` + 合成一条记账
// 文本），而那条记账文本是 **assistant + 零 tool_call**、正好长得像一条最终答复，于是
// 凡是「挑一条消息代表某件事」的地方都得记得跳过它。**在洞产生的那一刻把它填上，
// 上面整串防护就都不必存在。**
//
// # 文案不声称执行与否
//
// 只说「任务被放弃、这个调用没有结果」。中止发生在审批闸上时工具确实没跑，但这条路
// 是通用的（别的中断点也能走到 Abort），**框架在这一层不知道跑没跑**——写一句会被后来
// 的人当事实读的猜测，比不写更糟。
//
// # 失败只 warn
//
// 历史是旁路：写不进去不该让「用户放弃任务」这个操作失败（cp 已经删了，返错会让调用方
// 以为没放弃成功而重试）。
//
// # 只补被放弃的这个任务的
//
// 悬空的 tool_call 是这个任务在中断那一刻留下的，只在这个任务写下的记录里找（按任务章查，task 是被放弃
// 任务的 RunID）——任务挂着期间同一会话里别的任务写了多少都不用翻过去。补上的结果盖上声明它的那条
// assistant 的归属与任务章：归属对上了，那个 agent 的视图里配对才是完整的。
func closeUnansweredToolCallsOnAbort(ctx context.Context, hc *historyConfig, agentName, task string) {
	if hc == nil || hc.store == nil {
		return
	}
	key := HistoryKey{Owner: hc.resolveOwner(ctx), ConvID: compose_v2.GetThreadID(ctx)}
	if key.Owner == "" || key.ConvID == "" {
		return
	}
	src := historySource{store: hc.store, key: key, task: task}
	var msgs []*schema.Message // 新 → 旧
	err := src.scanBackward(ctx, HistoryQuery{Task: task}, historyPageMin, true, func(m *schema.Message) bool {
		msgs = append(msgs, m)
		return false
	}, nil)
	if err != nil {
		slog.WarnContext(ctx, "agent_v2: abort 收尾读历史失败，悬空 tool_call 未补",
			"agent", agentName, "key", key.String(), "err", err)
		return
	}
	if len(msgs) == 0 {
		return
	}

	answered := make(map[string]bool)
	for _, m := range msgs {
		if id := schema.GetToolResultID(m); id != "" {
			answered[id] = true
		}
	}
	var closers []*schema.Message
	for i := len(msgs) - 1; i >= 0; i-- { // 旧 → 新：补上的结果按调用出现的顺序排
		m := msgs[i]
		for _, tc := range schema.GetToolCalls(m) {
			if tc.ID == "" || answered[tc.ID] {
				continue
			}
			answered[tc.ID] = true // 同一个 id 只补一次
			closer := schema.ToolResultMessage(tc.ID,
				"任务已被放弃（调用方 Abort），此工具调用没有结果")
			owner, _ := m.Annotations[HistoryAgentAnnotation].(string)
			display, _ := m.Annotations[HistoryAgentDisplayAnnotation].(string)
			closers = append(closers, stampHistoryAgent(closer, owner, display, task))
		}
	}
	if len(closers) == 0 {
		return
	}
	if err := hc.store.Append(ctx, key, closers); err != nil {
		slog.WarnContext(ctx, "agent_v2: abort 收尾补记失败，历史里留着悬空 tool_call",
			"agent", agentName, "key", key.String(), "count", len(closers), "err", err)
	}
}
