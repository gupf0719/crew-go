package compose_v2

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// NonRetryable 由错误类型**自己声明**「重试我没有意义」。
//
// # 为什么是接口不是哨兵清单
//
// `IsRetryable` 原本的形状是「不在这几个哨兵里就重试」，而那几个哨兵是被四次事故一个个
// 逼出来的——每次都是往清单里加一条，没人问过形状对不对。实测那个清单漏掉的不止一类：
// 轮次耗尽 / fan-out 宽度失控 / 结构化输出校验预算耗尽，**全部被重试 4 次**。
//
// 清单的两个毛病：
//   - **加的人和知道答案的人不是同一个**。「这个错重试有没有意义」在**定义它的地方**最
//     清楚，而清单在 compose_v2，逼着每个新错的作者跑到另一个包去登记（于是不会登记）。
//   - **跨不过分层**。`agent_v2.StructuredOutputError` 想进清单就得让 compose_v2 import
//     agent_v2——依赖方向反了。用哨兵包裹又只能选 [ErrRunAborted]，而那捆绑了「清 cp
//     子树」的语义，对不该清档的错是有害的。
//
// 接口两个毛病都没有：**在定义处声明**（作者手上就有答案），**跨得过分层**（下游包实现
// 上游包的接口是正常方向）。
//
// # 跟 ErrRunAborted 的区别
//
// [ErrRunAborted] = 不重试 **+ 清 cp 子树**。本接口只管前半。
// 清档是个独立且更强的决定——「四家零先例」（见 error-handling.md §7.2.2），别顺手捆绑。
//
// # 实现它意味着什么
//
// 只有一个断言：**重试这次调用不会得到不同结果，或者代价明显压过收益**。两类都算：
//   - 确定性错（协议违规 / 参数非法 / 身份冲突）——重跑必然一样
//   - 预算耗尽（轮次 / 校验次数）——⚠️ 这类**不是**确定性的（LLM 非确定，第二次可能
//     更少轮次答完），理由是**重放代价**：一次撞满预算的失败恰恰是**做下最多东西**的那种
//     （N 次 LLM 调用 + 全部工具副作用），代价已知是最大值而收益只是「也许这次少绕几圈」
type NonRetryable interface {
	error
	// NonRetryable 是标记方法，无行为。实现即声明。
	NonRetryable()
}

// MaxIterationsError 表示循环超过了其**迭代深度**限制——典型 LoopAgent 触顶 maxIterations、
// Graph 触顶 maxVisit、BasicAgent 跑满 maxRounds 时使用。
//
// errors.As / errors.Is 友好——caller 用 `var e *MaxIterationsError` 解析具体值。
//
// # Iterations 是「第几次迭代触顶的」，不是别的量
//
// 各处抛出时填的都是**迭代计数**，但**计数基准按抛出方**：Graph 的 maxVisit 填 `visits[id]`，
// 也就是**被拒的那一次访问**（= maxVisit+1）；BasicAgent 的 Run / Resume 出口填本 agent 自己**已跑满的
// 轮数**（= maxRounds+1）。同一次 BasicAgent 跑满会同时经过这两处（引擎先拒、出口再包一层），
// 而出口**重铸**了一个自己的 MaxIterationsError 挂在外层，所以 `errors.As` 取到的是**轮数**、跟错误正文
// 里那句「跑满 N 轮」一致（引擎的原错误仍经 `%w` 留在链上，诊断不丢）。
//
// ⚠️ **扇出宽度失控不用这个类型**，用 [MaxWidthError]：宽度（一个超步里的并发成员数）与深度（同一节点
// 被反复访问的轮数）是**两个正交维度**（`graph_loop.go` 那两段注释原话），而 `Iterations` 这个名字里没有
// 「宽度」的意思。曾经两者共用它、`Iterations` 一个装宽度一个装深度，调用方 `errors.As` 之后**分不出
// 拿到的是哪个量**——CLAUDE.md 「用 X 来做 Y 而 X 的名字里没有 Y 的意思」那条判别式的实例。
type MaxIterationsError struct {
	Iterations int
}

// Error 返人类可读错误信息。
func (e *MaxIterationsError) Error() string {
	return fmt.Sprintf("达到最大迭代：%d", e.Iterations)
}

// NonRetryable 实现 [NonRetryable]——预算耗尽属「重放代价已知是最大值」那一类。
func (e *MaxIterationsError) NonRetryable() {}

// MaxWidthError 表示一个超步的 fan-out **宽度**超过了 maxFrontierWidth（见 WithMaxFrontierWidth）。
//
// 它与 [MaxIterationsError] 是**两个正交维度**：宽度是「一个超步里有多少并发成员」，深度是「同一节点被
// 回边反复访问多少轮」。节点在自己的 NodeFunc 里 SendFrom 自己 N(≥2) 次时宽度逐超步相乘，业务终止条件
// 写错就是指数爆炸——而深度每超步只 +1，等 maxVisit 触顶时进程早已 OOM。
//
// 所以它要有自己的类型：共用 MaxIterationsError 的话，`Iterations` 一个装宽度一个装深度，调用方
// `errors.As` 之后分不出拿到的是哪个量，而两者的处置完全不同（宽度失控查业务的终止条件 / 深度触顶是
// 预算耗尽）。
//
// # 它是终局的，[MaxIterationsError] 不是
//
// 抛出点一并 `%w` 上 [ErrRunAborted]（深度那条没有）：存进档的 `CurrentFrontier` 就是那个超限的
// frontier 本身，而续跑闸门按内容判 ⟹ 不清档的话 auto-load 会捡起它、**节点一次没跑就在同一道闸上
// 再爆**，那个 (sid, ns) 槽从此是死的。深度触顶不是这个形状：它的 frontier 是正常的，续跑（调大预算）
// 走得下去。所以 `errors.Is(err, ErrRunAborted)` 对宽度为真、对深度为假。
type MaxWidthError struct {
	Width int
}

// Error 返人类可读错误信息。
func (e *MaxWidthError) Error() string {
	return fmt.Sprintf("扇出宽度超限：%d", e.Width)
}

// NonRetryable 实现 [NonRetryable]——宽度失控是业务的终止条件写错了，重跑必然一样。
func (e *MaxWidthError) NonRetryable() {}

// MaxRoundsError 表示代理超过了其最大允许轮数. **预留 typed 供业务自定义抛**——
// framework 生产路径不抛 (BasicAgent ReAct 触顶走 `WithMaxVisit(maxRounds * 2)` →
// `graph_loop.go::Run` 抛 `MaxIterationsError` 非 MaxRoundsError; grep 实证生产 0
// MaxRoundsError 调用). 业务自定义 NodeFunc 想表达"轮次耗尽"时可抛此错让 caller
// `errors.As(*MaxRoundsError)` 区分.
type MaxRoundsError struct {
	Rounds int
}

// Error 返人类可读错误信息。
func (e *MaxRoundsError) Error() string {
	return fmt.Sprintf("达到最大轮数：%d", e.Rounds)
}

// NonRetryable 实现 [NonRetryable]——预算耗尽属「重放代价已知是最大值」那一类。
func (e *MaxRoundsError) NonRetryable() {}

// ErrRunAborted 标记本次 Run **终局失败、不可 Resume**——Run 出口 defer 据此**主动**
// `DeleteSubtree(sid, graphNS)` 清掉该子树的全部 cp，而非仅仅"跳过写"。
//
// **为什么必须主动删、跳过写不够**：cp 是 (sid, ns) **单槽覆盖式**——"跳过写"只是不覆盖，
// **先前已存在的 cp 原样幸存且 Status=Pending**：
//   - 先前 HITL 中断存的档 → auto-load 捡起 → 业务重跑已成功的节点、**被重新问已经答过
//     的问题**（默认 durability=exit 下最主流的 HITL 流程即中招）
//   - durability=sync/async 每超步写的中途 cp → Resume 重跑整个上一超步（无需 HITL 也中招）
//   - 子图：子图跳过写但**父图照存**（父 frontier 非空）→ 反复 Resume 反复问同一问题，
//     **永远收敛不到终局失败**（livelock）
//
// 主动删与成功路径的 cleanupOnSuccess 同款——两者都是"这个 sid 的活干完了、cp 不该留"，
// 只是结论不同（成功 vs 终局失败）。cp 在本引擎是**单次 Run 的临时态**而非跨 run 资产
// （业务跨 Run 持久化用不同 sid，见 WithSessionID godoc）。
//
// **子图终局性自动向上传播**：子图 Run 返回的 abort 错沿错误链冒泡进父图 NodeFunc →
// 父图 Run 出口 defer 同样 `errors.Is` 命中 → 父 cp 子树一并清 → 逐层向上，不需要每层
// 显式协调。
//
// # 来源：别找清单，用判据加 grep
//
// **判据**：凡是「重跑或续跑都走不通」的失败。两种形状——业务代码违反框架协议（重跑必然同样失败），
// 以及矛盾/爆炸会随存档反复复现（写不写取决于数据，但留档续跑必然再撞同一个）。都不是瞬时故障。
//
// ⚠️ **下面是举例，不是全集**。全集这么取——`grep -rn "ErrRunAborted" --include="*.go" . | grep -v
// _test` 之后**逐条看**。⚠️ 别再往后接 `grep -E "%w|Unwrap"` 收窄：那要求两者在**同一行**，而多行
// `fmt.Errorf` 的参数在下一行（宽度、单写入者、cond 返回未注册 target 三处都这么写）——实测那样只找到
// 10 个、漏掉一半多。
//
// 2026-10-23 一次多行感知的对账数出 **27 个 wrap / Unwrap 点，横跨 compose_v2 / agent_v2 / a2a 三个包
// 的 10 个文件**，而当时仓里**三份**写着「全部来源」的清单（这里、[MustPropagate] 的 godoc、
// `doc/description/three-way-comparison/error-handling.md`）**一份都不完整**——漏的包括 `out_edges.go`
// 的「cond 返回未注册 target」和 agent_v2 / a2a 整两家（转交目标非法、session 的任务身份对不上、
// tool_call 与调用 ctx 个数不符……）。枚举式清单在这个位置注定漂：加 wrap 点的人改的是另一个文件。
// （顺带一条自证：这段话第一版把数字写成「13」、grep 写成收窄版——**当场又错了一次**，因为那条 grep
// 漏掉了多行调用。所以这里给的是**取法加它的坑**，不是数字。）
//
// 举例（按类目）：
//   - cond fn 两档：`CondFailAbort`（含 cond 抛 InterruptError 的协议违规拒绝）、返回未注册 /
//     不可路由的 target
//   - **业务抛的中断请求形状非法**——由 `validateInterrupt` 统一判定（typed-nil /
//     Query 与 Queries 同时填 / 一条 query 都没有 / InterruptID 为空 / 同一批内撞 ID /
//     Queries 含 nil 元素）
//   - **扇出宽度失控**——一个超步的 frontier 宽度超过 `WithMaxFrontierWidth`（[MaxWidthError]）。
//     理由同下面那条：存进档的 frontier 就是那个超限的 frontier，留档续跑只会在同一道闸上再爆
//   - **单写入者冲突**——同一超步两个成员（或成员与 SendFrom payload）写同一个 [LastValue]
//     channel，或两个成员都对同一个上行信号的收件人做了决定、业务经 [JoinSignals] 合并的几路
//     给出了不同的决定（[SignalConflictError]）。这一类的理由不是「重跑必然一样」（写不写可能
//     取决于数据），是**矛盾会随存档反复复现**：留档续跑只会让同一个矛盾再来一次
//
// 业务 `errors.Is(err, ErrRunAborted)` 可判"这次 Run 已终局、别再 Resume 了"。
//
// ⚠️ **按类目写、别数个数**：这份清单的枚举部分曾在写下的当天就漏了一条
// （`Queries` 含 nil 元素经 `ie.Validate()` 出口），而「共几条」这个数字在三个地方
// 写过三遍、三个数字互不相同。类目（"形状非法，由 validateInterrupt 判"）不会因为
// 里面多一条判断就失效，枚举会。
var ErrRunAborted = errors.New("compose_v2: run aborted (terminal failure, checkpoint purged)")

// ErrCheckpointNotPersisted 报「这次退出本该留下一份存档，但没写成」——Run 出口写档（或写档前的
// 快照）失败了。挂起类的退出（中断、软取消、调用方取消……）与业务报错退出都算：两者的档按设计都
// 续得上（失败档续跑 = 重试失败的分支）。
//
// **这次退出原本的错误照常写进错误文本，但挂起类的信号不在错误链上**：`IsInterrupt` 为假，
// `errors.Is(err, ErrSoftCanceled)`、`errors.Is(err, context.Canceled)` 也为假。它们都在说
// 「可以续」，而档没写成，续的时候要么找不到档，要么续进槽里更早的那份——续进用户回答之前那份，
// 已经答过的问题会被重问，再答一次，中间做过的副作用就会重做（实测）。业务错留在链上，照样能
// `errors.Is` / `errors.As`；写档失败的原因也留在链上（[ErrCheckpointStale]、[ErrGraphIDConflict]
// 照样判得出来）。
//
// 它同时带 [ErrRunAborted]：不自动重试、各吞错点不吞（[MustPropagate]），出口尽力删掉这一层的
// 存档子树（存储坏着时可能删不掉，删失败写进错误文本），嵌套时上层出口跟着清。**例外**：写档失败
// 是因为这个槽已经属于别人——别的执行推进了它（[ErrCheckpointStale]），或者它是另一张图的档
// （[ErrGraphIDConflict]）——时不带 ErrRunAborted、不删，那份档不是这次执行的。这时照样不自动重试
// （错误类型实现 [NonRetryable]）。
//
// 出口写档与清档不跟着调用方取消：调用方取消或超时导致的退出也要存下可以续跑的档，而持久后端拿到已经
// 结束的 ctx 一律写不进去。调用方结束之后每次存储调用最多再等 10 秒；调用方放弃之后，读、写、删每一类调用
// 失败所占的墙钟时间（重叠的只算一次、放弃之前的不算）各累计不超过 10 秒，用完之后同类调用不再发（后端挂住
// 或一直报错时 Run 照样很快返回，跟嵌套层数无关）。清档前的陈旧写回检查没做成时照常清。后端在出口存储
// 调用时 panic 同样按那次调用失败处理；cp 事件的钩子与日志在结论定下之后才调，它们的 panic 当场兜住、记日志，
// 不进返回的错误。
// 三条不变量见 exit_checkpoint.go。
var ErrCheckpointNotPersisted = errors.New("compose_v2: checkpoint not persisted")

// checkpointNotPersistedError 是出口写档失败时返回的错误，见 [ErrCheckpointNotPersisted]。
type checkpointNotPersistedError struct {
	// exit 是这次退出原本的错误。keepExit 为 false（挂起类信号）时只进文本，不进错误链。
	exit     error
	keepExit bool
	// cause 是写档（或快照）失败的原因，进错误链。
	cause error
	// aborted 为 true 时错误链上带 ErrRunAborted；槽属于别人时为 false。
	aborted bool
	// purgeSkipped 是尽力清档之前发现槽已被别的执行推进（ErrCheckpointStale）、因而没删的原因，进错误链。
	purgeSkipped error
	// purgeErr 是尽力清档失败的原因，只进文本（它可能带着后端返回的 ctx 错）；兜住的 panic 例外，挂在链上。
	purgeErr error
}

func (e *checkpointNotPersistedError) Error() string {
	var b strings.Builder
	b.WriteString("compose_v2: checkpoint not persisted: ")
	b.WriteString(e.cause.Error())
	b.WriteString(" (run exited with: ")
	b.WriteString(e.exit.Error())
	b.WriteString(")")
	if e.purgeSkipped != nil {
		b.WriteString("; older checkpoint kept: ")
		b.WriteString(e.purgeSkipped.Error())
	}
	if e.purgeErr != nil {
		b.WriteString("; purging the older checkpoint failed, it may still be resumed: ")
		b.WriteString(e.purgeErr.Error())
	}
	return b.String()
}

// Unwrap 交出哨兵、写档失败的原因、ErrRunAborted（aborted 时）、业务错（keepExit 时）、跳过清档的原因，
// 与清档时兜住的 panic（[PanicError]）。
func (e *checkpointNotPersistedError) Unwrap() []error {
	errs := make([]error, 0, 6)
	errs = append(errs, ErrCheckpointNotPersisted, e.cause)
	if e.aborted {
		errs = append(errs, ErrRunAborted)
	}
	if e.keepExit {
		errs = append(errs, e.exit)
	}
	if e.purgeSkipped != nil {
		errs = append(errs, e.purgeSkipped)
	}
	var pe *PanicError
	if e.purgeErr != nil && errors.As(e.purgeErr, &pe) {
		errs = append(errs, pe)
	}
	return errs
}

// NonRetryable 实现 [NonRetryable]：存储坏着时重跑一遍照样写不进去，槽属于别人时重跑会撞同一个槽，
// 而重跑会把这次已经做过的副作用再做一遍。
func (*checkpointNotPersistedError) NonRetryable() {}

// ErrCheckpointNotResumable 报「拿来续跑的这份存档自相矛盾」——它说有人在等回答
// （`PendingInterrupts` 非空），却没有任何地方能跑到那个提问（`CurrentFrontier` 空）。
//
// **名字说的是哪条不变量破了，不是「这东西坏了」**：存档的字节可能完好，只是这两个字段
// 凑不成一个能执行的状态。
//
// # 「什么都不剩」不是矛盾
//
// frontier 空**且**没有挂起 = 上次跑完了。对这样一份快照续跑返成功、不重跑，是幂等语义，
// **不报本错**。矛盾特指「有挂起、没地方跑」。
//
// # 引擎自然产不出这种矛盾
//
// 存档时 frontier 和挂起列表是一起写的，一个在另一个没了只能来自外部：
//   - 调用方手搓 / 拿一份被截断的档喂进 [WithResumeFrom]；
//   - 业务自己 `SaveNS` 拼了一份；
//   - **业务自研 backend 漏序列化 `CurrentFrontier`** —— 这一类最隐蔽也最常见：一次真
//     HITL 中断的档，挂起提问还在、`Status` 还是 Pending，只有起跑地址没了。
//     （`checkpointer_conformance` 有 round-trip 断言，前提是业务真跑了它。）
//
// # 为什么必须报错而不是静默返成功
//
// 这条路原先无差别 `return nil`（注释写「上次已完整跑完」）。那个前提对上面第三类不成立，
// 于是实际表现是：**Resume 返 nil、一个节点都没跑、用户的答案没送达、存档还被当成功
// 清掉** —— 四件事全静默，调用方拿到的每一个信号都说「成功」。
//
// # 现场保留（有一个前提，别当成结构保证）
//
// 校验放在 `cm.Restore` **之前**，所以 Run 独占那个 `ChannelManager` 时它还是空的，
// 出口「没 frontier 也没挂起就不写档」的早退成立 ⟹ 不会回写覆盖。也不 wrap
// [ErrRunAborted]（那会触发主动清子树）—— 存档不自洽时最不该做的就是毁掉唯一的
// 排查依据，而留着它也不会被误捡：下一次同样会在这儿被拒。
//
// ⚠️ **前提是「cm 是空的」，不是「校验在 Restore 之前」**。`cm` 由调用方给，而
// 「先 `cm.Restore(cp)` 渲染待答问题、再 `Run(..., WithResumeFrom(cp))`」是本包支持
// 的幂等用法（`Run` 认得已被 Restore 过的 cm，不会重复灌）。走那条路时 cm 非空 ⟹
// 出口早退不成立 ⟹ **存储里那份好的会被内存里这份坏的覆盖**，而覆盖出来的形态是
// 最坏的一档：**存档自己变成矛盾档**——frontier 被抹平、`Status` 仍是 `CPStatusPending`。
// 于是此后每一次续跑都被这同一道守卫拒，**backend 修好了也回不来**，而它在业务的
// 待处理列表里看着一切正常（实测：覆盖后 `pending frontier=0 pending=1`，修好
// backend 再跑，提问节点执行 0 次）。
//
// 两条路各自的结局要摆在一起看，别只记住一个：不加守卫是**静默**删掉整份并返成功；
// 走这条路是留下一份救不回来的档、但每次都明确报错。**别照着「结构保证」去推理**。
// 想两条都保住，读 `cp.PendingInterrupts` 字段渲染 UI，别先 Restore 进 cm。
//
// **祖先层不受这个前提影响**：本错在 `shouldStayPending` 里被归到「挂起」那一档，
// 所以嵌套时上面每一层的存档仍标 Pending——业务修好 backend 之后整条链还能续。
// 漏掉那一步的话，子层保住了、祖先层全被盖成失败现场，`agent_v2.Resume` 走的正是
// 祖先那层，会话就彻底救不回来（实测过）。
//
// 业务 `errors.Is(err, ErrCheckpointNotResumable)` 可判「这份档不能用来续跑」。
// 它**不可重试**（[IsRetryable] 排除它）——重跑必然拿到同一份档。
var ErrCheckpointNotResumable = errors.New("compose_v2: checkpoint has pending interrupts but no frontier to run them")

// ErrNoSubgraphCheckpoint 表示父 cp store 中未找到指定子图节点的嵌套 cp。
//
// 调用方应改走 RunSubgraph 重新执行——本错误让调用方决定 fallback 行为：
// 视为新执行 / 报错 / 重定向到不同子图。
//
// 典型触发：父 cp 已 LoadNS 成功 + 父期望子图也有 cp 但 SaveNS(parentSID, childNS)
// 之前父进程崩溃。errors.Is 友好。
var ErrNoSubgraphCheckpoint = errors.New("compose_v2: no subgraph checkpoint for node")

// GraphNodeError 包裹节点 NodeFunc 返回的业务错误，携带节点定位信息（出错节点 ID +
// 所在 graph 的 NS），让 caller 用 `errors.As(err, &e)` 定位"哪个 ns 的哪个节点错了"
// ——嵌套子图场景尤其有用（仅靠 `fmt.Errorf("node %q: ...")` 字符串无法结构化提取）。
//
// **不包裹 InterruptError**（控制流走 IsInterrupt 识别路径，保持 `fmt.Errorf` wrap）；
// 业务错 / ctx 错 / 节点 panic-转-error 才包。实现 Unwrap 让 errors.Is / errors.As
// 穿透到原始错误——如 `errors.Is(err, context.Canceled)`、`errors.As(err, &myBizErr)` 仍命中。
type GraphNodeError struct {
	NodeID  NodeID // 出错节点 ID
	GraphNS string // 节点所在 graph 的 ns（"" = 根图；嵌套子图为层级路径，如 "exec/researcher"）
	Err     error  // 原始业务错误
}

// Error 返人类可读信息——根图省略 ns 段。
func (e *GraphNodeError) Error() string {
	if e.GraphNS == "" {
		return fmt.Sprintf("node %q: %v", e.NodeID, e.Err)
	}
	return fmt.Sprintf("node %q (ns %q): %v", e.NodeID, e.GraphNS, e.Err)
}

// Unwrap 让 errors.Is / errors.As 穿透到原始业务错误。
func (e *GraphNodeError) Unwrap() error { return e.Err }

// MustPropagate 判断 err **绝不能被吞掉**——不能转成给模型看的 `tool.Failure`、
// 也不能 warn 一句然后降级继续。
//
// # 为什么这是一个独立的谓词，而不是复用 [IsControlFlowSignal]
//
// 「要不要吞」和「是不是控制流信号」是**两个问题**，它们在绝大多数输入上答案一致
// （中断：都别吞；普通业务错：都可以吞），**只在下面这几个哨兵上分叉**：
//
//   - [ErrRunAborted]：**不是**控制流信号（刻意的——把终局失败塞进去会让消费
//     `IsControlFlowSignal` 的那一层承担它不需要的语义。⚠️ 原话是「同时被 retry 层和
//     fallback 层消费」，fallback 今天改消费 [IsRetryFutile] 了），但它**必须**往上传。
//   - [ErrCheckpointNotResumable]：同上。下游（前端适配、A2A server）对它有专属分支，
//     被吞掉那些分支就永不触发。
//   - [ErrGraphIDConflict]：同上。嵌套的子 agent 撞上它只有一个含义——同一个父 sid 上先后
//     跑了拓扑不同的两棵编排树（见其 godoc），出路在编排侧，模型换什么参数重试都还是撞同一个
//     槽。曾经不在集合里：问这个函数的吞点照样把它转成工具失败，`err == nil`（AgentAsTool
//     上实测）。
//   - [ErrCheckpointNotPersisted]：同上。存档没写成时，被吞成工具失败的话模型会再调一次，子
//     agent 从头再跑、副作用再做一遍。它多数时候同时带着 ErrRunAborted，槽属于别人时不带，所以
//     单独列。
//
// 这条分叉的代价实测过：子 agent 返 `ErrRunAborted` 经 `AgentAsTool` 被转成
// `tool.Failure` + `err=nil` ⟹ 模型看到一段错误文本、重试同一个工具 ⟹ 子 agent 再跑、
// 再 abort ⟹ 循环到 `maxRounds` ⟹ **业务最终拿到的是 `MaxIterationsError`，跟真实原因
// 毫无关系**。而 `ErrRunAborted` 说的是「重跑或续跑都走不通」（**不是**「重跑必然一样」——它有
// 两种来源，另一种是「矛盾会随存档反复复现」，见那个哨兵的 godoc），让模型绕过它是保证浪费；
// 更要紧的是出口已经把存档清掉了，吞成工具失败等于让模型去重跑一个**状态已经不存在**的调用。
//
// # 判据：失败的是任务，还是代码
//
//   - **任务失败**（下游超时、参数不合适、外部服务拒绝）⟹ 可以吞：转成模型看得见的失败，
//     让它换条路，这正是工具错误对模型可见的意义。
//   - **代码/协议出错**（业务违反框架协议、存档自相矛盾、调用方喊停）⟹ 必须传：
//     让模型绕过一个**编程错误**，结果是业务拿到一个貌似合理的答案、
//     **永远不知道自己配错了**。
//
// `ErrRunAborted` 属后者——**别在这里重复它的来源清单**（那份清单连同这句话在 2026-10-23 的对账里
// 一起被证伪：当时这里写「全部来源 = cond 的 CondFailAbort 档 + validateInterrupt 五条」，而实际是
// 27 个 wrap / Unwrap 点、横跨三个包）。判据与取全集的办法都在 [ErrRunAborted] 自己的 godoc 里。
//
// # 不在集合里的：[ErrNoPendingCheckpoint]
//
// 它在吞点上出现时，说的是**被续的那个任务**的状态：已经跑完清掉了、从没跑过、或者存档里
// 没有可续的东西（它的产生点只有两处：编排的 Resume 预检，与引擎「带了答案却没有可续的档」）。
// 那是另一个任务的外部状态，不是本层调用方违反了协议——业务工具去续一个已经结束的流程时，
// 让模型知道「那件事已经没了」、据此改口，正是工具错误对模型可见的意义，属于任务失败。
// 顶层的消费方（前端适配）对它有专属分支，但那里不经过吞点，不受影响。
//
// # ctx 那一档为什么问 ctx 不问 error
//
// 跟 [IsRetryFutile] 同款理由：`errors.Is(err, context.Canceled)` 回答的是「错误链上**有没有**
// ctx 错」，不是「**我的** ctx 死了没」。ctx 为 nil 时退回链判据保守处理。
//
// ⚠️ **所以这里不能复用 [IsControlFlowSignal]**——它的 ctx 那一档走链判据，而这里要的是
// 归属判据。实测代价：给工具配了 [agent_v2.WithToolTimeout] 之后，工具自己那条派生 ctx
// 到期返 `context.DeadlineExceeded`，链判据把它当成「调用方喊停」⟹ **整个 Run 挂掉**，
// 而正确行为是「这次工具调用失败了，转成 tool_result 让模型换条路」。
// 本仓规约原话：`errors.Is(err, context.X)` 可以贴标签，不能用来决定要不要再做一次。
//
// 两个谓词的自铸哨兵部分仍然一致（中断 / 陈旧写回 / 软取消），只是各自列各自的——
// 让一个去调另一个，等于把它的 ctx 档一起借过来。
//
// # ⚠️ 新增「可能吞错误」的地方必须做两件事，而且机械闸只挡得住半件
//
//  1. 调这个函数，别直接调 [IsControlFlowSignal]——`agent_v2` 的机械对账
//     `TestSwallowDecision_NoDirectControlFlowSignal` 挡这半。
//  2. **配一条「必传错误穿过它」的行为哨兵**。这半机械闸**挡不住**：一个「谁都不问」
//     的吞点（拿到 err 直接转 tool.Failure / 返 nil）不含任何可 grep 的串，对按名字
//     搜误用的闸完全隐形——本仓实测过一次（工具超时收尾排在 err 判断之前，中断被吞后
//     挂起档为 0，靠人审才抓到）。查过同类产品：一家靠架构不给吞的机会（错误分发的
//     控制权根本不交给业务回调），一家靠注释约定（"without swallowing"）——**没有谁有
//     机械发现新吞点的办法**。所以这半只能靠纪律：吞点行为哨兵的现有落点是
//     `swallow_decision_test.go`（AgentAsTool、RoutingAgent 分类器、run_skill_action）/ `tool_middleware_test.go`（中间件 guard）/
//     `tool_timeout_test.go`（超时收尾）/ `skill_fork_hitl_test.go` /
//     `store_memory_error_propagation_test.go`，新吞点照它们的样子加。
func MustPropagate(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	var ie *InterruptError
	if errors.As(err, &ie) {
		return true
	}
	if errors.Is(err, ErrCheckpointStale) || errors.Is(err, ErrSoftCanceled) ||
		errors.Is(err, ErrRunAborted) || errors.Is(err, ErrCheckpointNotResumable) ||
		errors.Is(err, ErrGraphIDConflict) || errors.Is(err, ErrCheckpointNotPersisted) {
		return true
	}
	// **ctx 那一档问 ctx，不问 error。** ctx 为 nil 时退回链判据保守处理。
	if ctx == nil {
		return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	}
	return ctx.Err() != nil
}

// errUncatchableGraphStartInterrupt 是「没人接得住的 OnGraphStart 中断被拒」的错误类型。
//
// 实现 [NonRetryable]：gate 挂错了层是确定性的接线错误，重试只会原样再拒一遍
// （agent_v2 那层的同款拒绝实测过——不标时配了重试的编排会跑 4 次）。
//
// **不用 [ErrRunAborted]**：那捆绑「清 cp 子树」的语义，而本错抛在图开始执行之前、
// 一个字节都没写过，清档只会误伤同 sid 下与本次无关的存档。
type errUncatchableGraphStartInterrupt struct{ msg string }

func (e *errUncatchableGraphStartInterrupt) Error() string { return e.msg }

// NonRetryable 实现 [NonRetryable]——接线错误重试必然一样。
func (e *errUncatchableGraphStartInterrupt) NonRetryable() {}
