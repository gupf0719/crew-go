package agent_v2

import (
	"context"
	"strconv"

	"crew-go/compose_v2"
)

// 转交与路由这两条控制决定走 compose_v2 的上行信号（见 compose_v2.CaptureSignals）：
//
//   - 写入者（transfer_to_agent / route_to 工具、[HandoffTo]）把决定交给**它所在的那段执行**，
//     收件人取自 ctx——HandoffAgent 调候选、RoutingAgent 调分类器之前把自己的收件人放进去；
//   - 决定跟着写入者那段执行的提交走：那段执行成功并提交，决定随它进存档；失败、被中断、被否决，
//     决定随它作废。工具调用的完成账与嵌套调用的完成记录把那段执行做出的决定连同产出一起记下，
//     拿回时交出；
//   - 一个收件人上是一个单值格子：先后做出的后者为准（Sequential 的第二步、Loop 的下一轮修正前面
//     的决定）；同一超步只能有一个成员做决定，两个都做了就是图的设计错误（compose_v2.SignalConflictError
//     并带 ErrRunAborted：不重试、清档，值相同也一样）——ParallelAgent 的分支因此拿不到决定权，能撞上
//     的只有业务显式转授出来的并发决定方；同一批 tool_call 是模型一条回复里给出的，排在前面的生效，
//     后面给出不同值的那个调用回一条「没有生效」的失败结果（见 tool_batch_signals.go）；
//   - 消费方用 CaptureSignals 包住它等的那次调用，成功之后取走发给自己的，剩下的交给自己所在的
//     那段执行（它们是发给更上方的）。
//
// 曾经两条决定写进消费方准备的活通道（每次尝试一个的收件槽、session KV），写入当场生效、不等写入者
// 提交：写入者那一步已提交、续跑不再执行它时决定丢失；失败那次尝试写下的照样生效；并发写同一格谁赢
// 随调度——全部 `err == nil`（实测）。哨兵在 side_signal_commit_test.go。
//
// **收件人由引擎铸**（compose_v2.SignalRecipient：带这个任务的密钥，随存档走）。曾经是
// `"handoff:" + 候选所在的 ns`、`名字@ns`，业务代码用 GetGraphNS 就拼得出来，手写一份 Signals 的
// JSON 再 EmitSignals 就转走了对话——没被授予转交能力的终点候选、被当工具用的孙辈、永不拿转交能力
// 的分类器，三处边界全部绕过，`err == nil`（实测）。种类放在不导出的变量里只挡得住正常入口，挡不住
// 手写的 JSON；挡住伪造的是收件人。收件人放进 ctx 时包一层遮罩（sealedRecipient），打印 ctx 读不出来。
// 防护范围到正常入口为止：故意从存档里读出密钥自己算的不在其内（见 compose_v2/signal.go 开头）。

var (
	// handoffSignal：转交决定（值是目标候选名）。transfer_to_agent 工具与 [HandoffTo] 做出的是
	// 同一类——一个决定就是一个决定，先后与并发按同一套规则裁决。
	//
	// ⚠️ 曾经两条路分成两类、「工具路径优先」：工具路径先做出、业务代码随后 HandoffTo 修正时，
	// 赢的是先做出的那个——跟「先后做出的后者为准」正好相反，而那条优先级的理由（「内层 LLM 的
	// 决定更具体」）找不到一个需要它的场景。
	handoffSignal = compose_v2.NewSignalKind("agent_v2.handoff")
	// routeSignal：route_to 工具做出的路由（值是目标分支名）。
	routeSignal = compose_v2.NewSignalKind("agent_v2.route")
)

// signalOrigin 拼写入者的位置（只用于报冲突时定位）：所在的 ns + 做出它的入口，在 tool_call 里时
// 再带上 tool_call 的身份。ns 带出了嵌套的全路径——只记 tool_call 的话，内层路由 agent 与外层
// 业务工具各自的 tool_call 常常都叫 call_1#0，报错里分不出是哪一层（实测）。
func signalOrigin(ctx context.Context, entry string) string {
	o := compose_v2.GetGraphNS(ctx)
	if o == "" {
		o = "(根)"
	}
	o += " " + entry
	if compose_v2.InToolCall(ctx) {
		o += " tool_call " + compose_v2.GetCurrentToolCallID(ctx) + "#" + strconv.Itoa(compose_v2.GetCurrentToolCallIndex(ctx))
	}
	return o
}

// ── 收件人在 ctx 上的形态 ──

// sealedRecipient 是放进 ctx 的收件人：打印 ctx 时只露出遮罩，不露出收件人本身。
//
// 收件人是别的代码算不出来的那个值（compose_v2.SignalRecipient），可 ctx 是**环境**——标准库 context
// 的 String() 会把链上每一层的值原样打印出来，ctx 键不导出也挡不住。曾经存的是裸 string：跑在这条
// ctx 下的业务代码 `fmt.Sprint(ctx)`（或者 slog 记一下 ctx）就读到了它，再手写一份信号交上去就伪造
// 出一条决定——没被授予转交能力的终点候选、被当工具用的孙辈（摘收件人只在子层盖了一个 nil，父层那串
// 照样打印）、分类器、候选自己的 OnAgentStart 钩子，全部转走了对话（审计实测，`err == nil`）。
//
// 存成指针：`%v` / `%s` 走 String() 拿到遮罩；`%#v` 打印 ctx 时，层里的值在深度 > 0 处只打印地址。
// 经 reflect / unsafe 硬读不在防护范围内——那跟直接改存档是一回事。
type sealedRecipient struct{ v string }

func (*sealedRecipient) String() string   { return "<recipient>" }
func (*sealedRecipient) GoString() string { return "<recipient>" }

func withRecipient(ctx context.Context, key any, dest string) context.Context {
	return context.WithValue(ctx, key, &sealedRecipient{v: dest})
}

// recipientFrom 读 key 上的收件人；没有、被摘掉（值为 nil）或为空时第二个返回值为 false。
func recipientFrom(ctx context.Context, key any) (string, bool) {
	r, ok := ctx.Value(key).(*sealedRecipient)
	if !ok || r == nil || r.v == "" {
		return "", false
	}
	return r.v, true
}

// ── 转交的收件人 ──

// handoffDestKey 是 ctx-value key——HandoffAgent 调候选之前放进自己的收件人，transfer 工具与
// [HandoffTo] 读它。值是 *sealedRecipient（打印 ctx 时不露出来）。
type handoffDestKey struct{}

// handoffDest 是 HandoffAgent 在某一跳上的收件人，在 step 节点的执行里铸（compose_v2.SignalRecipient：
// 位置含访问序号，每一跳一个）。续跑时同一跳算出同一个值（密钥与访问序号随存档恢复），所以上一个
// Run 写下、存在候选存档里的决定续跑后仍认得出是发给它的；节点外的代码算不出来。
func handoffDest(ctx context.Context, candidateNS string) string {
	r := compose_v2.SignalRecipient(ctx, candidateNS)
	if r == "" {
		return "" // 不在节点执行里：没有可挂的图，也就没有消费方
	}
	return "handoff:" + r
}

func withHandoffDest(ctx context.Context, dest string) context.Context {
	return withRecipient(ctx, handoffDestKey{}, dest)
}

// withoutHandoffDest 把收件人从 ctx 摘掉——**agent 被当工具用时，它的控制流决策不许逃出去**。
//
// 用在 `AgentAsTool` 和 skill fork 两处：那两处把一个 agent 降格成「一件能力」，调用方要的是它的
// **产出**，不是让它替调用方决定这场对话该归谁。不摘的话，被当工具用的孙辈调一次 transfer 就能把
// 它祖父那层的路由夺走——而祖父自己明确没转交（实测：frontdesk 答「我自己处理」，控制权却已经到了
// billing 手上，err 全程 nil）。摘掉之后孙辈的 transfer 拿到 [HandoffNoCoordinatorErrCode] 软失败
// 回灌，改成正常作答。
//
// **不摘的那些边界是有意的**：候选是 Sequential / Loop / Graph 等编排时，它的内层步骤替它做转交决定
// 是对的——那个编排**本身就是候选**，内层是它的组成部分而非它调用的一件工具。
func withoutHandoffDest(ctx context.Context) context.Context {
	if _, ok := getHandoffDest(ctx); !ok {
		return ctx
	}
	return context.WithValue(ctx, handoffDestKey{}, nil)
}

// getHandoffDest 读收件人；第二个返回值报有没有 HandoffAgent 在等（摘掉之后为 false）。
func getHandoffDest(ctx context.Context) (string, bool) {
	return recipientFrom(ctx, handoffDestKey{})
}

// takeHandoffTarget 从候选那次调用交上来的决定里取发给本 HandoffAgent（收件人 dest）的转交，
// 返回剩下的（发给更上方的）。
//
// 不在这里判冲突：一个收件人上至多一条——先后做出的已经按「后者为准」归并，并发做出的在合并那一刻
// 就处理了（同一批 tool_call 在 tools 节点按顺序第一个生效、同一超步两个成员都做了决定在引擎提交点
// 终局），走不到这里。
func takeHandoffTarget(sigs compose_v2.Signals, dest string) (string, compose_v2.Signals) {
	s, ok, rest := sigs.Take(handoffSignal, dest)
	if !ok {
		return "", rest
	}
	return s.Value, rest
}

// takeRouteTarget 从分类器那次调用交上来的决定里取发给本 RoutingAgent（收件人 dest）的路由，
// 返回剩下的。冲突同 takeHandoffTarget，在合并点就报了。
func takeRouteTarget(sigs compose_v2.Signals, dest string) (string, compose_v2.Signals) {
	s, ok, rest := sigs.Take(routeSignal, dest)
	if !ok {
		return "", rest
	}
	return s.Value, rest
}
