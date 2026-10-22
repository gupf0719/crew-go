package compose_v2

import (
	"context"
	"sync"

	"crew-go/internal/panicerr"
)

// PanicError 是框架兜住的一次 panic：panic 的原值（Value）与 panic 发生时的调用栈（Stack）。
//
// 框架把 panic 转成交给你的错误时，都把它并列挂在那个错误上（编排收尾时若已经带着别的错误返回，那个错误
// 不改，这次 panic 只记日志），调用方用 errors.As 取：
//
//	var pe *compose_v2.PanicError
//	if errors.As(err, &pe) {
//		// pe.Value 是 panic 的值，pe.Stack 是调用栈
//	}
//
// 三条约定（理由见 internal/panicerr 的包文档）：
//
//   - 错误文本 err.Error() 不含栈，与没挂它时逐字相同——错误文本会交给前端、对端、模型和 span。
//   - 它不实现 Unwrap：panic 值是 error 时，errors.Is / errors.As 认不认得出它，仍由各兜底点原来的写法
//     决定（多数用 %w 接上，OnGraphStart / OnNodeStart 钩子、async 写档等几处只取文本）。要判断 panic 值本身，
//     对 pe.Value 做 errors.Is / errors.As。
//   - 兜底点同时记一条带栈的 ERROR 日志。持锁的两处（业务 reducer 在 [ChannelManager.ApplyAll] /
//     [ChannelManager.ApplyPendingSendsAndClear] 里、业务 UnmarshalJSON 在 [ChannelManager.Restore] 里 panic）
//     当场不记，由 [Run] 出口补记；业务直接调这几个方法时没有 Run 出口，日志由调用方自己记。
//
// 挂了它之后，返回的错误用多分支的 Unwrap() []error，标准库单层的 errors.Unwrap 对它返回 nil；判断一律
// 用 errors.Is / errors.As。
type PanicError = panicerr.Error

// abnormalNodeExitKey / abnormalNodeExitSlot —— 本次 Run 的「节点异常终止」记录位。
//
// `runtime.Goexit` 掐断节点时函数**永不返回**，错误没法走返回值——但 defer 会跑，
// 而且跑在**同一条正在坍塌的栈**上。所以节点层的 defer 往这个槽里记一笔（带节点身份），
// Run 出口的 defer（更外层、更晚 unwind）读它，把「静默消失」变成带身份的 runErr。
//
// **per-Run**：Run 入口现建 + 塞 ctx，随 Run 结束丢弃——不挂 cm 上是防跨 Run 残留
// （fan-out 路径也会记一笔但 runErr 已非 nil 用不上，槽随 Run 消亡，不会污染下一次）。
// 嵌套子图各建各的（ctx 覆盖），子图的 Goexit 由子图自己的槽和出口处理，一层层向上。
type abnormalNodeExitKey struct{}

type abnormalNodeExitSlot struct {
	mu  sync.Mutex
	err error
}

// note 记第一笔（后到的丢弃——第一现场比后续 unwind 里的更接近根因）。
func noteAbnormalNodeExit(ctx context.Context, err error) {
	s, _ := ctx.Value(abnormalNodeExitKey{}).(*abnormalNodeExitSlot)
	if s == nil || err == nil {
		return
	}
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
}

// getAbnormalExitSlot / withAbnormalExitSlot —— 取出与重铺。
//
// **它是执行状态**：节点 ctx 会被业务 `OnNodeStart` 返回的 ctx 替换，业务若返了一个非派生的
// ctx（违约但真实发生），槽就跟着丢——那时 Goexit 的 note 无处可记，错误退化成没有节点身份的
// 通用文案。所以它跟 componentKind / instanceID / answerLedger 一样，在 hook 之前由
// applyFrameworkNodeCtx 铺上，采纳 hook 返回的 ctx 时经 [CallHook] 取回（abnormalNodeExitKey
// 在执行状态清单里）。
func getAbnormalExitSlot(ctx context.Context) *abnormalNodeExitSlot {
	s, _ := ctx.Value(abnormalNodeExitKey{}).(*abnormalNodeExitSlot)
	return s
}

func withAbnormalExitSlot(base context.Context, s *abnormalNodeExitSlot) context.Context {
	if s == nil {
		return base
	}
	return context.WithValue(base, abnormalNodeExitKey{}, s)
}

func (s *abnormalNodeExitSlot) take() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.err
	s.err = nil
	return err
}

// ── panic 恢复的分工：唯一权威说明 ──
//
// **这是「谁兜底什么」的单一真相源。别处不要复述这份分工，指向本文件即可。**
//
// 立这一处的理由是实测出来的：这份分工曾经散在 38 处注释里各说各的，于是每改一次行为就
// 漏掉几处。而漏掉的不只是「说法过时」——**我按其中一句不够准的话，把一条会毁存档的路径
// 当成了正确参照**（见下面「为什么出口和入口必须是两道」）。注释拦不住这种事，因为改代码
// 的人读的往往是另一处。
//
// ⚠️ 这一处自己也漂过：立完五个 commit 内就有三条与代码不符。**核它的办法是逐道去触发，
// 不是读代码比对**——两轮人工比对各漏了三条，实测一次全揪出来。所以下面每条都注明「怎么
// 触发它」，改动后照着跑一遍即可。
//
// # 按层看：谁接谁
//
// 本包非测试代码共 **21 处 `recover()`**，散在 8 个文件（2026-10-22 核对；之前写的「16 处、5 个文件」漏了出口
// 记账的 `exitStorageOp`、只作观测的 `recoverObserver`、async worker 的 loop、`ApplyPendingSendsAndClear` 几道，后来又加了
// 成功路径观测钩子的几道）。下面按「出事时谁先接到」分层，**不写行号**（行号本身就是最容易漂的东西，按函数名找）。
//
// ## 第 1 层：单个成员的执行内部
//
//   - `safeNodeRun` —— 收 **NodeFunc 本体**的 panic。前缀 `node panic recovered: `。
//     **这是最常见的一类**，`runNodeWithHooks` 调它，所以本体 panic 在里层就被接走了。
//   - `OnNodeStart` 的 inner defer —— 收该 hook 的 panic，转 `startErr` 走早退路径，
//     好让 `OnNodeEnd` 仍被配对调用。前缀 `OnNodeStart panic: `。
//   - `OnNodeEnd` **早退路径**的 inner defer —— ⚠️ **只打 WARN 日志，返回值里一个字都没有**。
//     早退时错误已定（`startErr`），这道只保证 hook 的 panic 不把它顶掉。
//   - `notifyNodeEnd` —— **节点体成功之后**的 `OnNodeEnd`：就地兜住、带栈记一条 ERROR，**不进错误**，
//     节点结果与写入照常提交（转成错误的话失败档被续跑或重试捡起，已经做完的节点体重跑、副作用重放）。
//     触发：节点成功、OnNodeEnd panic ⟹ Run 返 nil，日志里一条「OnNodeEnd 钩子 panic（节点体已经成功…）」。
//   - `nodeEventSink.Send` —— 节点里发事件时的 `OnNodeEvent`：同样就地兜住、只记 ERROR，Send 照常返回。
//   - `runNodeWithHooks` 整函数 defer —— 兜上面几道**之外**剩下的，实际主要是**节点体失败时的
//     `OnNodeEnd`**。前缀 `node %q hook/run panic recovered: `。
//     它跑在每个成员自己的 goroutine 里，所以跨 goroutine 也覆盖。
//     **它还有第三支**：`bodyReturned` 旗（testing.tRunner 的 `finished` 同款，由正常
//     控制流的普通语句置位——defer 里置位没用，Goexit 也跑 defer）。NodeFunc / OnNodeStart
//     hook 里 `Goexit` 时旗保持 false ⟹ 这一支给 OnNodeEnd 配对（带合成错误，否则每个
//     被掐断的节点漏一个不闭合的 span；这次配对调用另有一道 inner defer，只打 WARN）+ 往本次 Run 的
//     `abnormalNodeExitSlot` 记一笔带身份的错（本文件里那个类型）。成功路径 OnNodeEnd 自己 Goexit 不落这支——由第 3 层
//     的 `bodyReached` 兜住存档，只是错误正文没有节点身份。
//
// **别把这一层写成「整函数 defer 收 NodeFunc 与全部节点 hook」**——那是曾经写错过的说法，
// 四件事里三件落在别处。怎么分辨：看错误前缀。
//
// ## 出边计算里的条件边函数体：`safeCondRun`
//
// 只收 **cond 函数体本身**的 panic，前缀 `condition from %q panic recovered: `。它在出边计算里、
// `runNodeWithHooks` 返回之后才跑（fast path 在 Run 的 goroutine 上，fan-out 在成员的 goroutine 上），
// 所以不算第 1 层。框架随后对 cond 返回的错误做的链式检查在它之外，归下面第 2 层（fan-out）或第 3 层
// 出口那道（fast path）。
//
// ## 第 2 层：成员的 goroutine 顶上（fan-out 才有）
//
// `executeFrontier` 为每个成员起的 goroutine，defer 里有**两支**：
//
//   - `recover()` —— 收**出边计算**（`evalOutEdges` / `wrapCondFailure` 对业务 error 的
//     链式 unwrap）的 panic。它在第 1 层**之外**：`runNodeWithHooks` 那道随它 return 就
//     结束了。跨 goroutine 的 panic 谁都接不到，不自己兜就 **crash 整进程**。
//     前缀 `node %q out-edge panic recovered`。
//   - `normalReturn` 守的第三条出路 —— `runtime.Goexit`。它**会**跑 defer 但 `recover()`
//     返 nil，所以上一支接不住；而结果槽是预分配的零值 ⟹ `err == nil` ⟹ 被当成功，
//     连身份都是零值 ⟹ 幽灵成员进 `CompletedFrontier`（「已成功、永不重跑」）⟹ 分支跨
//     Run + Resume 永久消失。转成**普通业务错**（留 Failed 档、业务可 Resume 重试这一个
//     分支）——**不是 `ErrRunAborted`**：确定的只是 `Goexit` 这个机制，触发它的是业务
//     条件，框架判不出瞬时还是确定性，清档等于替业务做决定。
//     **不罕见**：`zap` 的 godoc 主动推荐在 `zap.WithFatalHook` 里调它（外部库 API，非本仓符号）。
//
// ⚠️ **fast path（frontier 只有一个成员）不起 goroutine**，这一层不存在：出边计算 panic
// 归第 3 层。**但 `Goexit` 在 fast path 上曾是全套里最重的一格**——这里原来写着「会直接
// 终止调用 Run 的 goroutine，不是静默成功，无需另兜」，**实测推翻**：生产里 Run 跑在
// HTTP handler / agent 的 goroutine 上（不是 main），Goexit 只掐那一条，而 **Run 的
// defer 全都会跑**、`runErr` 保持 nil ⟹ cleanup 打着 `reason=success` 删掉用户正等着
// 答的挂起档、`OnGraphEnd` 收到 nil。而 BasicAgent 的四个节点全是单节点超步，这是
// 生产最常走的路。现由两道旗补上（见第 1 层的 `bodyReturned` 与第 3 层的 `bodyReached`）；
// Run 本身仍然不返回——那是 Goexit 的语义（zap 那个配置的意图就是结束当前 goroutine），
// 修的是**状态不说谎**：存档留成 Failed 可续、OnGraphEnd 收到带身份的真实错误。
//
// ## 第 3 层：`Run` 的两道（顺序承重）
//
//   - **出口那道**（注册最晚 ⟹ LIFO 下最先跑）—— 收**图主体**的 panic（fast path 的出边
//     计算、`computeStartFrontier`、引擎自身），**先转成 `runErr`**，好让随后的 cleanup /
//     `OnGraphEnd` 看到真实结局。前缀 `compose_v2: graph body panic recovered: `。
//     **它还收图主体的 `Goexit`**：`bodyReached` 旗只在成功出口前置位（三处，勿漏——
//     初版按一个错误的 grep 写成「唯一」，实测漏了两处、把幂等 no-op 续跑误判成 Goexit），
//     「recover 返 nil + runErr 为 nil + 旗为 false」= 主体没走到任何出口就被掐了 ⟹
//     从 `abnormalNodeExitSlot` 取带身份的错（节点层记的），没有就给通用错。这一格是
//     **完整**的：图主体里任何位置（NodeFunc / hook / cond / 框架自身）的 Goexit 都落
//     在这儿，不必逐个调用点设防——节点层那道只是让错误带上身份、让 span 配对。
//   - **入口那道**（注册最早 ⟹ 最后跑）—— 收 **defer 链自身**抛的 panic，实际主要是**图失败时**的业务 `OnGraphEnd`
//     hook。出口记账的存储调用（写档、清档、清档前的检查）由 `exitStorageOp` 自己兜，cp 事件的钩子与出口
//     日志由 `recoverObserver` 兜（见第 4 层），都不落这道。
//     前缀 `compose_v2.Run panic recovered: `。
//   - **`OnGraphEnd` 成功路径那道** —— 图成功跑完时 `OnGraphEnd` 的 panic 就地兜住、带栈记一条 ERROR，
//     **不进错误**（理由同 `notifyNodeEnd`：它跑在出口记账之前，转成错误的话成功收尾时档已清掉，重试从头
//     再跑一遍）。
//
// `OnGraphStart` 另有自己的 inner defer（前缀 `OnGraphStart panic: `），同样是为了让
// `OnGraphEnd` 的配对承诺不被跳过。
//
// ## 第 4 层：旁路（不在 Run 的 defer 链上）
//
//   - `asyncCpWriter.writeOne`（`durability.go`）—— **`durability=async` 时 `SaveNS` 的
//     panic 落这儿**，不落第 3 层的入口那道：worker 跑在独立 goroutine，Run 出口那两道够不着。
//     前缀 `async durability writer panic: `。它外面的 `loop` 还有一道（理论上到不了，防 writeOne 被改坏）。
//     ⚠️ 中途写档三个档三个归属：`async` 落这儿，`sync` 的写在主体里、归第 3 层**出口**那道；Run 出口那次
//     写（三个档都有）归 `exitStorageOp`。别只记一个。
//   - `exitStorageOp`（`exit_checkpoint.go`）—— Run 出口记账的每一次存储调用（写档、清档、清档前的陈旧写回
//     检查）。自研后端 panic 按那一次调用失败处理，结论照常定（档没写成 / 清档失败 / 检查放行）。前缀
//     `compose_v2: <调用名> panicked`。怎么触发：自研 checkpointer 的 SaveNS / DeleteSubtree / LoadNS 在出口 panic。
//   - `recoverObserver`（`cp_event.go`）—— 只作观测的代码：cp 事件的钩子（`deliverCpEvent`、`EmitCpEvent`）与
//     Run 出口结论定下之后的日志（`flushNote`、`reportCpPurge`）。panic 只记一条带栈的日志、**不进任何错误**
//     ——结论已经定了，成功收尾时档也已清掉，报错只会引来从头重跑、副作用重放。怎么触发：`WithEmitCpEvent(true)`
//     + 在 cp 事件上 panic 的编排钩子（哨兵见 checkpoint_not_persisted_audit_test.go 与 observer_hook_panic_test.go）。
//   - `ChannelManager.ApplyAll` / `ApplyPendingSendsAndClear` —— 业务 reducer 在归并时 panic，由它自己接住转 error，
//     不走上面任何一道。**没有回滚**：归并跑在局部 staged 上、一个字节都不写回 cm，
//     写回是归并之后一次做完的纯赋值 ⟹ 掐断时压根没有半应用。
//   - `Restore`（`serialization.go`）—— 反序列化时 panic，回滚已改的字段再转 error。
//
// # 为什么出口和入口必须是两道（一道满足不了）
//
// defer 是 LIFO：**最后注册的最先跑**。入口那道要接住 defer 链自己的 panic 就必须跑得够晚，
// 于是它注册最早；可那样 cleanup 与 `OnGraphEnd` 都比它先跑，那时 `runErr` 还是 nil ⟹
// cleanup 走成功分支 `purgeExitSlot(reason="success")`。
//
// 实测后果：用户正等着答一个中断，下一次 Run 的主体 panic ⟹ 框架打印 `reason=success`
// **删掉挂起档**、告诉 `OnGraphEnd` 一切正常、同时返回 error。**会话就这么没了。**
//
// ⚠️ 之后会怎样，两层不一样，别混：
//
//   - `agent_v2` 9 编排的 `Resume` 返 `ErrNoPendingCheckpoint`（存档没了，报得出来）
//   - `compose_v2` 这层**不报错**——auto-load 找不到档就静默当新任务跑，还会换一批新的
//     `InterruptID` 重新问一遍，用户手上那个旧答案永远配不上。**比报错更难发现。**
//
// 哨兵见 out_edge_panic_test.go。
//
// # 两个前缀刻意不同
//
// 业务据此区分是哪一段炸的：
//
//   - `graph body panic recovered` —— 图主体炸了，**存档已正确存成 failed**，可 Resume
//   - `Run panic recovered` —— 收尾段（图失败时的 OnGraphEnd 钩子）炸了。出口的存档读写不在这里
//     兜（出口存储调用那道单独兜，结论按存储调用的结果定：写档 panic 报 `ErrCheckpointNotPersisted`），存档
//     状态以错误链上的结论为准
//
// 这两个前缀曾被一次无边界的字符串替换抹平，而**全套测试照常全绿**——没有任何用例断言过
// 它们可区分。现在有了（out_edge_panic_test.go 里那条），改前缀时它会红。
//
// # 不由 recover 兜的那些（有意）
//
//   - **业务 reducer 在 `ApplyAll` 里 panic** —— 走它自己那道 recover（见第 4 层），
//     不走 Run 的 defer 链。
//   - **业务 reducer 在出边叠加视图里 panic** —— `reduceFor` 自己的 recover **只补归因再抛回去**
//     （报文带 "reducer" 与 channel 名），落进 `safeCondRun`，变成「这个成员的 cond 失败」。跟上一条
//     同档处置：两边都不留半应用状态。（这里原先写的是「`reduceFor` 自己一处 recover 都没有」，补归因那道
//     加上之后没改。）
//   - **业务 reducer 在子图读路径的合成里 panic**（`composeRead`，reduce(baseline, own)，
//     reducer 的第三个调用点）—— 它发生在调 `Get` 的那条栈上：NodeFunc 里 ⟹ `safeNodeRun`
//     兜成「这个节点失败」；cond 里 ⟹ `safeCondRun`；hook 里 ⟹ Run 入口那道。`composeRead`
//     自己的 recover **只补归因再抛回去**（报文带 "reducer" 与 channel 名），不改变落点。
//   - **`runtime.Goexit` 在业务自己起的 goroutine 里** —— 那条 goroutine 直接消失，
//     框架看不见也管不着（图主体内的 Goexit 已由第 2/3 层的旗覆盖，这条只剩业务自旗下的）。
//   - **运行时 fatal**（map 并发写被检测到、栈溢出、`no goroutines`）—— Go 运行时直接终止，
//     **任何 recover 都 cover 不了**。
//
// # `runtime.Goexit` 的入口清单（sweep 过一遍，别再逐个挖）
//
// Goexit 跟 panic 的差别只有一条：**它跑 defer，但 `recover()` 返 nil**。于是所有
// 「靠 recover 判断出了事」的逻辑在它面前静默失效。这条在本仓一次挖出 **7 个入口**，
// 每次都是「修完一个以为完了」，所以清单列在这儿——**加新 goroutine 或新 recover-only
// 清理时对着它过一遍**：
//
// ## 形态 A：goroutine 的结果没写进去
//
// 预分配槽 + goroutine 内赋值 ⟹ 掐断后槽留零值 ⟹ 零值被读成「成功且无错」。
// 判据一律是**一道旗 + 普通语句置位**（defer 里置位没用，Goexit 也跑 defer）：
//
//   - `executeFrontier` fan-out 成员（`normalReturn`）
//   - `agent_v2` tool fanout 的 `outcomes[i]`（`finished`）
//   - `agent_v2` 流桥接（两道：无条件 `defer CloseSend` 防消费方永久阻塞 + `concluded`
//     旗防「截断冒充干净 EOF」——`io.EOF` 在「发完」与「被掐断」下一字不差，半截 chunk
//     会被聚合成 assistant 消息当最终答复。⚠️ 三道 defer 的**注册次序**承重：
//     `CloseSend` 注册最前 ⟹ 跑最后 ⟹ 错误先进流、流再闭合；挪了它，阻塞中的消费方
//     会先拿到 EOF 而修复静默失效，哨兵见 stream_bridge_goexit_internal_test.go 那条
//     「ErrorReachesBlockedConsumer」）
//   - `agent_v2` 会话历史 worker（`closed`/`inFlight`/`backlog` 三信号取或）
//   - `asyncCpWriter.loop`（`drained`）——**它是最后被发现的那个**，而历史 worker
//     早就做对了且注释点了名「兜不住 runtime.Goexit」。同一个仓里做对一处、漏一处，
//     正是「立守卫时必须列全所有入口」那条规矩的代价。
//   - `a2a/server` 执行 goroutine（`sent`）——不兜则 abandon 路径的裸 `<-done` 永久阻塞
//
// ## 形态 B：清理/回滚只挂在 recover 上
//
// 「原子性」若只写在 `if r := recover()` 里，Goexit 下整段跳过 ⟹ **半应用状态留下来**，
// 比崩掉更糟（`ApplyAll` 那处会被持久化进存档，Resume 重跑整超步 ⟹ Append/Sum 双计）：
//
//   - `ChannelManager.ApplyAll` 的归并/提交分离（**已无回滚栈**：归并不碰 cm，掐断时
//     没有半应用可回滚）
//   - `Restore` 的 `rollbackOnErr`（`restored`）
//
// ⚠️ **形态 B 有个反向陷阱**：这两个函数各有若干**正常 error 出口**，它们本来自带
// 回滚（或本来就刻意不回滚）。旗必须在**每一个**正常出口置位，漏一个就变成「多回滚
// 一次」——修 A 带出 B。`ApplyAll` 的 `return e` 路径历来不回滚，这次没改它。
//
// ## 不在清单里的（查过，不受影响）
//
//   - `model/openai` 的 `consumeStream`：最先注册的无条件 `CloseSend`，且无业务回调
//   - fast path（不起 goroutine）：Goexit 掐整条栈，由第 3 层 `bodyReached` 兜
//   - 兜底点记日志时调的日志 handler：跑在 `panicerr.Log` 自己起的 goroutine 里，Goexit 只结束那个 goroutine
//     （见上面「栈与日志」）
//
// # 栈与日志（[PanicError]）
//
// 本包把 panic 转成错误的兜底点，都在 recover 所在的 defer 里取栈（`internal/panicerr`），把「panic 值 + 栈」
// 作为 [PanicError] 并列挂到返回的错误上，同时记一条带栈的 ERROR 日志。错误文本不变、errors.Is 的判定
// 不变（各兜底点原来用 %w 还是 %v 照旧），理由见 PanicError 的 godoc。
//
//   - **持锁的两处**（`applyAllLocked`、`Restore`）当场不记日志：它们的 defer 跑在 cm 写锁里，而日志 handler
//     是业务代码。错误标成「待补记」，由 Run 出口那道（第 3 层）在锁外补记；业务直接调这两个方法时没有 Run
//     出口，日志由调用方自己记。
//   - **只记日志的几道**（`OnNodeEnd` 早退路径 / Goexit 收尾、async worker 的 loop）日志里同样带栈。
//   - **补归因再抛的两道**（`composeRead`、`reduceFor`）不用管：重抛时原 panic 的栈帧还在，最后接住它的那一道
//     取到的栈里有原 panic 处（实测）。
//   - **同一次 panic 只记一条**：兜底点当场记过的不再补记；嵌套图里内层 Run 出口补记过的，外层不再补记。
//     fan-out 多个成员都 panic 时 Run 只返其中一个的错误，其余的栈只在日志里——所以每个兜底点都要当场记，
//     不能只靠调用方拿错误去记。
//   - **日志 handler 碰不到兜底路径**：handler 与 panic 值的 Error / String 方法都是业务代码，而不少兜底点是
//     先记日志、后记结论（成员结果、runErr、收尾闭包里的 OnAgentEnd）。handler 调 runtime.Goexit（测试里
//     「一有错误日志就 t.FailNow」的 handler 就会）时 recover 拦不住，结论就没记下——审计实测 fan-out 成员
//     的失败变成幽灵成功、图主体 panic 被当成功、存档被清掉。所以 `panicerr.Log` 把格式化和调 handler 放在
//     它自己起、自己等的 goroutine 里，结构上隔开，不靠每个兜底点记得把日志排在最后。
//
// 哨兵见 panic_stack_test.go（每条断言栈里有 panic 发生处的函数名、错误文本不含栈、日志条数）。
//
// # 改这份分工时
//
//  1. 只改本文件 + 对应的 defer 块。**别去各处注释里同步复述**——那正是这一处要消灭的东西。
//  2. 改完**逐道去触发一遍**（各层的前缀就是判据），别靠读代码比对——那样漏过两轮。
//  3. 对一下总数（排除测试文件与注释行）：
//     `grep -n 'recover()' compose_v2/*.go | grep -v _test.go | grep -vE ':[0-9]+:[[:space:]]*//' | wc -l`，
//     新增的 recover 要在上面某一层里有名分。
//  4. 新增把 panic 转成错误的 recover，照上一节经 `internal/panicerr` 取栈、挂 PanicError、记日志。
