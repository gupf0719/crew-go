package compose_v2

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"crew-go/internal/panicerr"
	"crew-go/schema"
)

// RunOption 是 [Run] 的可选配置。用 With* 系列函数构造。
type RunOption func(*runOptions)

type runOptions struct {
	// importOverlay 见 withImportOverlay——**两条路径都摆**成 baseline（子图同步父图 import 值）。
	importOverlay []ChannelWrite
	// initialWrites 见 WithInitialWrites——**只在新开路径** apply 的种子写入。
	initialWrites []ChannelWrite
	// inputDigest 见 WithInputDigest / WithInputIdentity——本次调用输入的摘要。
	inputDigest string
	// inputIsIdentity：摘要就是这次调用的身份（WithInputIdentity），auto-load 时对不上是
	// 撞槽、拒绝（不可重试、不清档）；否则只告警。
	inputIsIdentity bool
	resumeFrom      *Checkpoint                // Phase 5.1.b.4：合并 CheckpointSnapshot 为单一 Checkpoint
	resumeAnswers   map[string]*schema.Message // by InterruptID — audit R9 B1
	resumeUserText  string                     // 非寻址型「用户这轮说了什么」，见 ResumeContext.UserText
	hooks           OrchestrationHooks

	// Phase 5.1.a Step 3+4：Checkpointer 集成
	// 业务通过 WithCheckpointer + WithSessionID 注入后子图 cp 写到 (sessionID, ns)
	// 物理分离独立 store——对齐 ISSUE-70 Phase 2 Step Ⅰ。
	// 不注入时子图嵌套场景下中断会丢失嵌套 cp（业务必须显式注入）。
	checkpointer Checkpointer
	sessionID    string
	graphNS      string // 本 graph 在拓扑中的 ns 位置（嵌入子图时由父注入）

	// Phase 5.1.a Step 5 audit Round 2：caller 显式跳过 auto-load 的 escape hatch
	// 典型用法：业务用同 sid 跑不同 graph（GraphID 防串台守卫 Phase 5.1.b 落地前
	// 的临时手段——caller 知道自己在跑新 graph 时显式 fresh）。
	freshRun bool

	// staleWriteGuard 见 WithStaleWriteGuard
	staleWriteGuard bool

	// emitCpEvent 是否 fire cp.save / cp.load / cp.cleanup EventSink event
	// (Phase 2 D2 Sub-3 trace 接入). 默认 false opt-in 跟 4 层 compact /
	// store / history.compact event 设计一致.
	emitCpEvent bool

	// durability 控制 cp 写入时机（"" 未显式设 / Exit 默认 / Sync / Async）——见
	// durability.go。"" 时 Run 入口 resolveDurability 兜底（父 ctx 继承 > 默认 Exit）。
	durability Durability

	// softCancel 软取消信号（nil = 无）——业务在另一 goroutine Trigger，Run 在超步边界
	// 检查、当前超步善终后优雅停并存档（见 soft_cancel.go）。
	softCancel *SoftCancelSignal

	// interruptBefore / interruptAfter 见 WithInterruptBefore / WithInterruptAfter：运行期断点，只本次
	// Run、只本层图，与图上编译期声明的取并集。
	interruptBefore []NodeID
	interruptAfter  []NodeID
}

// withImportOverlay 是**两条路径都生效**的 baseline：新开与续跑都在 `OnGraphStart` 之前、
// channel 注册之后摆好（只此一处；`Restore` 只写 values，不碰这一层，所以先后无关）。
//
// **不导出，框架内部独家**——生产侧唯一调用者是 `subgraph.go`：子图节点每次进入（新开
// 或续跑）都把父图当前的 import 值摆成子 cm 的底层。
//
// # 它是 baseline，不是写
//
// 摆的是 `ChannelManager.baseline` 那一层，**不经 reducer**（见 `setBaseline`）。子图自己的
// 写只进 `values`，读 = reduce(baseline, own)，export 只送 own。这一层不进 Snapshot——
// 每次进入都从父图重新摆，所以续跑天然拿到父图最新值，也不会把子图已有的进度盖回去。
//
// 它曾经实现成「经 reducer 的覆盖写入」。那个形态有过一次真退化：反思编排的 actor 输入
// 被 feedback 节点写成「原目标 + 上轮回答 + 评审反馈」，嵌套续跑时被覆写回原始目标 ⟹
// 重跑那轮看不见批评意见、等于把上一轮原样再做一遍，err 全程 nil。改成 baseline 后
// 那条路在结构上不存在了：父图的值在底层，子图的写在上层，谁也盖不掉谁。
//
// # 为什么不能做成公开选项
//
// 它跟 `WithInitialWrites`（只在新开路径 apply）的**定义域是包含关系**而非互斥：
// 「该用窄的却用了宽的」和「子图的正当用法」在调用点上**观测等价**——都是「续跑时往某个
// channel 放个值」。框架分辨不了，调用方也分辨不了（要分辨就得先知道自己在哪条路上，
// 而消灭那个判断正是拆分的理由）。**所以任何运行期校验都不可能存在**，误用只能是静默的
// ——而且 baseline 对「种子」这个用途是错的载体：它不进 Snapshot、不被 export，业务拿它
// 当种子会在续跑路径上得到一个「读得到但存不下」的幽灵值。
//
// 对照下来这个形状**零先例**：别家要么只有一个入口（无从选错），要么两个入口**互斥**
// （fresh-only ⊥ resume-only），没有一家把「宽的包含窄的」并列成公开 API。
func withImportOverlay(ws ...ChannelWrite) RunOption {
	return func(o *runOptions) {
		o.importOverlay = append(o.importOverlay, ws...)
	}
}

// WithCheckpointer 注入 Checkpointer 让子图 cp 写到 (sessionID, ns) 物理分离
// 路径（对齐 ISSUE-70 Phase 2 Step Ⅰ 设计）。
//
// 必须配合 WithSessionID。
// **嵌套子图场景必须注入**——否则中断会丢失嵌套 cp（无 fallback 嵌入式路径，
// Phase 5.1.a Step 4 删旧设计后）。
func WithCheckpointer(c Checkpointer) RunOption {
	return func(o *runOptions) {
		// audit¹¹ HIGH-2 fix: isFrameworkNil 检 typed nil Checkpointer (untyped nil 是
		// "无 cp" by-design idiom 保留; typed nil 业务 `var cp *MyCP; WithCheckpointer(cp)`
		// 漏防 → 后续 cp.SaveNS 远 stack nil deref). isFrameworkNil 区分 untyped/typed nil.
		if isFrameworkNil(c) {
			// 等价 untyped nil (无 cp idiom 保留, 不 panic)
			o.checkpointer = nil
			return
		}
		o.checkpointer = c
	}
}

// WithSessionID 设本次 Run 的 session ID——cp 主索引。必须配合 WithCheckpointer。
//
// **设计语义：sid lifetime = 根 graph Run lifetime**：
//   - 根 graph Run 成功后框架自动 DeleteSubtree(sid, "") 清整 sid 命名空间
//     （含本 Run 期间所有子图 cp）
//   - **业务在同 sid 下保存的无关 cp 也会被清**——业务跨 Run 持久化必须
//     用不同 sid
//   - 业务想用同 sid 跑不同 graph 时，传 WithFreshRun 显式跳过 auto-load
//     避免框架把上次 saved cp 强加到当前 graph 上
//
// sid 是**一次任务的 cp 键**（一次 Run → 零到多次 Resume → 终结；`agent_v2` 那边是
// `RunSession.RunID`，完整契约写在它的 godoc 里），不是会话主键——跨任务的对话身份走
// WithThreadIDCtx（observability 会话聚类）+ agent_v2 的会话历史层。
func WithSessionID(sid string) RunOption {
	return func(o *runOptions) {
		o.sessionID = sid
	}
}

// WithRunNS 设本次 Run 的 graph 在 graph tree 中的 ns 位置（典型用法：嵌入
// 子图时由父图自动注入，业务通常不直接调用）。
// 默认 "" = 根 graph。
//
// 区别于包级 helper WithGraphNS(ctx, ns)——后者是直接 ctx 注入的工具，本
// option 是通过 Run 入口注入。
func WithRunNS(ns string) RunOption {
	return func(o *runOptions) {
		o.graphNS = ns
	}
}

// freshRunCtxKey 是「本次调用是新开不是续跑」这个信号的 ctx-value key。
type freshRunCtxKey struct{}

// runTaskIDKey 携带**本次顶层任务的代号**，嵌套子图沿 ctx 继承——它是 auto-load 闸的
// 身份维（见 [Checkpoint.BelongsToTask]）与无档新开时的 TaskID 来源（子层不再各铸各的，
// 同一任务全树同代号）。引擎内部协议，在 runGraph 里 setTaskID 之后 push。
type runTaskIDKey struct{}

func withRunTaskID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, runTaskIDKey{}, id)
}

func getRunTaskID(ctx context.Context) string {
	v, _ := ctx.Value(runTaskIDKey{}).(string)
	return v
}

// WithFreshRunCtx 把「新开」信号透进 ctx，**嵌套子图会读它并同样跳过 auto-load**。
//
// # 为什么这个判断必须在入口做一次、往下传
//
// 「这次是续跑还是新开」的信息**只有入口有**。子层看自己的存档，只能看到「有一份挂起
// 的」，看不到「这份挂起属于上一个已被放弃的任务」——那是信息不对称，不是清理不及时。
//
// 我们踩过：顶层 `WithFreshRun` 只作用于自己，嵌套层刻意保留 auto-load（拿掉它嵌套 HITL
// 从 12/12 掉到 3/12）。于是被放弃任务留下的**子 cp 是 Pending 的**，下一个任务开始时
// 子 agent 把它捡起来接着跑——**新任务的输入根本没进图**，整个新任务被旧 cp 吞掉。
// 「上个任务批准的工具在新任务里静默执行」只是这件事最显眼的症状。
//
// 对照下来没有一家让子层自己判：都是入口决定一次、沿调用链往下传。
//
// # 为什么是信号而不是删子树
//
// 信号 **fail-safe**：没传到 → 子层退回原行为（照常 auto-load，可观测可测）。
// 删子树 **fail-open**：漏删一个 ns / 删失败 / 并发下漏删 → **静默吃掉新任务**，
// 正是要修的那个 bug。而且删是存储操作会失败会超时，传信号是纯内存的。
//
// **它不破坏嵌套 Resume**：auto-load 保留着，只是听父的——父 Resume 时不 push 这个信号
// （子照常捡自己的 cp），父开新任务时 push（子丢掉挂起、应用新输入）。
func WithFreshRunCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, freshRunCtxKey{}, true)
}

// IsFreshRunCtx 读「新开」信号。
//
// ⚠️ **这是框架内部协议，业务不要 push**——它是 **per-call** 的，而「这一次调用是不是
// 新任务」**只有入口知道**。业务在自己的 ctx 上 push 等于替框架回答一个它答不了的问题，
// 那条 ctx 后续所有嵌套 Resume 都会被它关掉。
//
// # 关于「它没有清除点」
//
// **这是作用域事实，不是缺口。** 信号的语义是「**这一次顶层调用**是新任务」，而它的
// 容器是 `runGraph` 里 `ctx = WithFreshRunCtx(ctx)` 这个**局部遮蔽**——调用方的 ctx
// 不受影响，函数返回即不可达。**容器寿命恰好等于语义寿命。**
//
// 对照下来，别家给同类信号加清除点，无一例外是因为**容器活得比语义长**：有的存在被
// 就地 mutate 且被子任务按引用继承的配置字典里（清点在超步末尾）、有的存在跨 run 复用
// 的实例字段里（清点在 finally）、有的存在跨循环迭代的局部变量里（清点在第一次消费后）。
// 三种时机看着矛盾，其实是同一条规则在不同容器上的投影。Go 的 ctx 把那个 finally
// 白送给我们了。
func IsFreshRunCtx(ctx context.Context) bool {
	v, _ := ctx.Value(freshRunCtxKey{}).(bool)
	return v
}

// WithInitialWrites 配置图启动前的种子写入——典型用法：用户输入消息、初始配置。
// 走相同 ChannelWrite 协议，不引入"特殊初始化路径"。
//
// **只在新开路径 apply。** 续跑时起点在 cp 里，再写一次要么无意义（LastValue 被同值
// 覆盖）要么有害——那条 channel 的当前值可能已被节点改写过（子图 import 曾经也这么
// 干过、把子图自己的进度盖了回去，见 `withImportOverlay` godoc；它现在改成不经 reducer
// 的 baseline，种子这条路没有那个出路，只能靠只在新开时 apply）。
//
// # 调用方不必知道自己在哪条路上
//
// 这正是它跟「两条路径都 apply」那个语义分家的理由：合在一起时，调用方要决定「传不传
// 播种」就**必须先自查这次算不算续跑**——而那个判断需要 `LoadNS`，可 `Run` 内部本来就要
// `LoadNS`。同一个决策被抄到调用方那边，抄本和正本失配的症状是「两头不着的 fresh run」
// （那边从 StartNode 起跑、这边不注入输入，节点拿到空输入；业务节点直接解引用就 panic，
// channel 是值类型则静默读到零值）。**本仓踩过四次。**
//
// 现在无条件传即可，「这次要不要 apply」这个判断收回引擎唯一一处。
//
// # 「传了种子 + 该 sid 正好有挂起存档」是正常形态，不是矛盾
//
// 收窄成「只在新开路径 apply」之后，种子的语义已经变成「**这是我给新开路径的输入**」
// ——它**不再声称「这是一个新任务」**。所以这个组合读作「**有东西可续就续、否则拿我这份
// 输入从头开始**」，两半意图并不打架。这也正是崩溃恢复的形态：开 `sync`/`async` 硬崩后
// 同 sid 再调一次同一个业务函数，那函数照常播种子。
//
// 续上存档时，这次的种子**哪怕内容跟新开时不同**也不 apply——续跑认的是存档所在的槽，
// 不是这次的种子。调用方给了输入摘要（[WithInputDigest] / [WithInputIdentity]）时，auto-load
// 捡起存档那一刻引擎比对，对不上怎么处理见那两个选项（显式 [WithResumeFrom] 是调用方自己选
// 的档，不比）。
//
// ⚠️ 曾经在这里加过一道「顶层 + 有种子 + 存档命中 ⟹ 报错」的守卫，**已撤**。三条理由，
// 每条都够：
//
//  1. **立论在语义收窄之后就不成立了**——种子早就不声称「这是新任务」，没有矛盾可拒。
//  2. **零真阳性**：9 个编排的顶层 `Run` 恒声明新任务（`agent_v2` 的新任务判定）根本进不了
//     这条闸门，所以守卫只可能对「直连 `compose_v2.Run` 且没传 `WithFreshRun`」的人开火
//     ——而那批人的意图 100% 是「恢复否则新开」。它拒掉的全是合法调用。
//  3. **它要求的合规方式不存在**：调用方唯一能做的是自己查存档，而它能用的
//     `ExistsNS` 跟引擎的判据（`HasResumableWork()`）**不是同一个谓词**——跑完了的快照
//     （frontier 与挂起都空）上两者分叉，实测调用方据此摘掉种子后节点拿到**空输入**且
//     `err == nil`，正是本仓踩过四次的「两头不着的 fresh run」。等于把框架的决策外包给调用方。
//
// ⟹ **守卫合法，当且仅当调用方能用它手上已有的信息满足它。** 见 CLAUDE.md 同名规约。
//
// # 想开新任务，说出来
//
// 复用 sid 开新任务时传 `WithFreshRun()`（或换一个 sid）。不传的默认是「恢复否则新开」，
// 而复用 sid + 上个任务停在中断且被放弃时，那个默认会续上旧的——这是单一 sid 语义下无法
// 消除的残余（要区分「重投同一任务」和「开新任务」，得有第二个标识把那一位比特传进来）。
// 现有缓解：成功即清存档 + 9 编排顶层恒声明新任务 + 这个显式出口。
func WithInitialWrites(ws ...ChannelWrite) RunOption {
	return func(o *runOptions) {
		o.initialWrites = append(o.initialWrites, ws...)
	}
}

// WithInputDigest 给出本次调用输入的摘要，作核对用。新开时它被记进存档
// （`Checkpoint.InputDigest`），此后续跑一路沿用；auto-load 捡起存档时拿本次的摘要跟存档里
// 的比，**对不上打一条告警、照样续跑**，本次的种子照样不 apply（见 [WithInitialWrites]）。
// 空串等于没给；存档里没有摘要（它新开时没给）时不比。
//
// 用它的前提是这次调用的**身份另有来源**——典型是调用方给的 key（`agent_v2.WithCallKey`）：
// 同一个身份下输入变了，是业务要知道的事，但身份是业务声明的，框架照它续。摘要本身就是身份
// 的，用 [WithInputIdentity]。
//
// # 摘要由调用方算
//
// 引擎不从种子里自己算：种子里常有不代表「输入」的东西。消息每次构造都会铸一个新的 `ID`，
// 同一句话重建一次摘要就变了；有的编排还把从持久层读回的状态一起播进来（转交编排的当前
// 成员），那个值合法地会变。只有调用方知道哪部分算输入——`agent_v2.RunSubAgent` 对调用方
// 交来的那条消息算（不含消息 ID、模型响应元数据与每次调模型铸的 `run_id` 标注）。
func WithInputDigest(digest string) RunOption {
	return func(o *runOptions) {
		o.inputDigest = digest
		o.inputIsIdentity = false
	}
}

// WithInputIdentity 同 [WithInputDigest]，但这份摘要**就是这次调用的身份**：存档槽由它派生
// （`agent_v2.RunSubAgent` 不带 call key 时按「位置 + 输入内容」定身份）。
//
// 同一个槽上本该只有同一份输入，auto-load 时对不上只可能是两次输入不同的调用撞进了同一个槽
// （身份哈希碰撞）。续下去就是拿别人的进度当自己的结果，所以**拒绝**，不是告警：返一个不可
// 重试的错误（[IsRetryable] 为 false），**不清档**——槽里那份存档属于另一次调用，本次在图开始
// 执行之前就停了、什么也没写。碰撞是确定性的，出路是给这两次调用各自一个
// `agent_v2.WithCallKey`。
//
// 输入相同、只是位置或次序不同的两次调用撞槽时摘要一样，这里看不出来——那要靠槽号本身够宽
// 让碰撞可以忽略（agent_v2 的槽号取身份哈希的 63 位，与平台字长无关）。
func WithInputIdentity(digest string) RunOption {
	return func(o *runOptions) {
		o.inputDigest = digest
		o.inputIsIdentity = true
	}
}

// WithFreshRun 显式声明「**这次是新任务，别捡任何存档**」。
//
// ⚠️ **首选是换一个 sid，不是传这个**。sid 是**一次任务**的身份（`agent_v2` 那边是
// `RunSession.RunID`，契约写在它的 godoc 里）；同一个 sid 开新任务这件事本身就越界，
// 而本选项只挡住「别捡旧存档」这半——**另一半挡不住**：本次跑成功时照样按 sid 清整棵
// 子树，把上个任务还挂着的存档一起删掉，`err` 里只字不提。
//
// 所以它的定位是**你已经复用了 sid 时的补救**，不是一种推荐写法。典型场景：
//   - 业务用同一 sid 顺序跑多个独立 graph（换不了 sid 时）
//   - 已知上次存档不适用（刚重新部署、graph 拓扑改了）
//
// **它同时落进 ctx**（见 `WithFreshRunCtx`），所以嵌套子图会跟着跳过 auto-load——
// 曾经只管本层，于是调用方照着说明传了它、子层却照旧把上个任务的挂起存档捡起来接着跑。
//
// **不影响存档写入**——本次 Run 失败/中断时仍照常存档（崩溃恢复不受影响）。
func WithFreshRun() RunOption {
	return func(o *runOptions) {
		o.freshRun = true
	}
}

// WithStaleWriteGuard 开启**陈旧写回检测**：Run 载入 cp 后、写回前，先确认槽里
// 的 cp 仍是自己载入的那一代；若已被别人推进，返 ErrCheckpointStale 而不是覆盖。
//
// **解决什么**：同一 sessionID 被并发 Run/Resume（两个进程副本同时处理同一会话）
// 时，双方各自载入同一份 cp、各自执行、各自写回——后写的静默覆盖先写的，**两边
// 都返回成功**，事后无从察觉。开启后至少有一方拿到明确错误。
//
// **它不阻止副作用双跑**（这一点必须清楚）：检测发生在写回时，那时两个副本都已
// 执行完毕，有副作用的节点该跑的都跑了。它只把最坏的"静默"变成"可见"。真要不
// 双跑，得在调用方保证同一 sessionID 串行——会话亲和路由 / 分布式锁 / 队列。
//
// **检测非原子**：框架在 SaveNS 前读一次比对，读与写之间仍有窗口，理论上两个副本
// 可能都通过检测。这把静默覆盖的概率大幅压低但不为零；要真原子需要 Checkpointer
// 后端支持条件写（`UPDATE ... WHERE epoch = ?`），那是 backend 的能力、不在本
// option 范围内。
//
// **默认关闭**：不开时框架照常写 Epoch 字段但不读取，行为与引入前完全一致。
//
// **新开的 Run 没有基线**：基线是这次 Run 载入（续跑 / auto-load）的那一代；带「新开」信号的 Run
// （[WithFreshRun]、`agent_v2` 的顶层 Run 恒如此）没有载入，守卫要等本次第一次写成功、基线推到自己写的那一代
// 之后才生效，第一次写本身不校验。于是默认的 exit 档（只在出口写一次）下新开的 Run 全程不受保护——成功清理时还会
// 把槽里别人的挂起档一并删掉；sync / async 档从本次第二次写起受保护。同一个 sid 上两个任务并发本身违反「一枚
// RunID 只服务一个任务」的契约，这个边界正落在那里。
//
// **本 RunOption 只作用于直接调 `compose_v2.Run` 的场景**——`agent_v2` 的 9 编排各自
// 内部拼 `[]compose_v2.RunOption`，不透传业务的 option，那条路要用
// `WithStaleWriteGuardCtx(ctx)` 开（嵌套子图也靠它继承）。
//
// 上层 `agui` / `a2a/server` 已各自拒绝同 threadID/taskID 并发，走那两条路的业务
// 通常不需要本 option。
func WithStaleWriteGuard() RunOption {
	return func(o *runOptions) {
		o.staleWriteGuard = true
	}
}

// WithEmitCpEvent 启用 cp.save / cp.load / cp.cleanup EventSink event fire
// (Phase 2 D2 Sub-3 trace 接入).
//
// 默认 false opt-in 跟 4 层 compact / store / history.compact event 设计一致.
// true 时 framework 在 graph_loop.go + subgraph.go + agent_v2 9 编排 Abort 路径
// cp 操作点 (SaveNS / LoadNS / DeleteSubtree) wrap 自动 fire `cp.{save,load,cleanup}`
// event 含 SessionID / NS / DurationMs / Ok / Err + 操作专属字段. 业务订阅
// OrchestrationHooks.OnNodeEvent 按 event.Type 分发处理.
//
// **业务订阅契约** (audit follow-up 文档化):
//
//   - **nodeID 区分 framework 级 vs 节点级 event**: framework cp 操作在 Run defer
//     scope (无 NodeFunc node) fire 时, OrchestrationHooks.OnNodeEvent 调用 nodeID
//     为空 ("") 标"非节点 framework 级 event". 业务订阅时按
//     `nodeID == "" && Event.Type == CpEventType` 识别 framework cp event,
//     `nodeID != ""` 是业务自定义 NodeFunc 内调 cper.
//
//   - **timing 契约**: cp.save / cp.cleanup fire 时序在 OnGraphEnd defer **之后**
//     (defer LIFO **后注册先跑**: cleanup 先注册 → 后跑; OnGraphEnd 后注册 → 先跑.
//     结论对但理由曾写反成「先注册先跑」——同一件事在下面 cleanup defer 处写对了,
//     哨兵见 hook_cp_event_order_test.go). 业务订阅时
//     若依"OnGraphEnd 收 = Run 完全结束"假设关 SSE / commit metric, 会在收到
//     cp.save/cleanup 时困惑. cp.load 在 OnGraphStart 之后 / 首 OnNodeStart 之前.
//
//   - **9 编排自动透传 (跨编排协议一致性)**: 业务用 `compose_v2.WithEmitCpEventCtx(ctx, true)`
//     一次注入 ctx, 所有 9 编排 wrapper (BasicAgent + 8 高阶) 自动透传到嵌套子 graph
//     (audit follow-up HIGH-2 fix, 避免 16 处散落 Option 透传). agent_v2.BasicAgent
//     也提供 `agent_v2.WithEmitCpEvent` Option 作便利 (内部 push 同款 ctx).
func WithEmitCpEvent(enabled bool) RunOption {
	return func(o *runOptions) {
		o.emitCpEvent = enabled
	}
}

// WithResumeFrom 从 cp 恢复执行。
//
// 调用方义务：
//  1. 在传入的 cm 上注册所有 channel（同 Run 1 时一致）
//  2. 把 cp 通过本 option 传入 Run（Run 内部自动调 cm.Restore + frontier 恢复）
//
// Run 看到 ResumeFrom != nil 时直接从 cp.CurrentFrontier 起跑（而非 StartNode）。
// frontier 为空 = 上次已跑完，直接返成功。
//
// Phase 5.1.b.4 后接收 *Checkpoint（之前 *CheckpointSnapshot——合并为单一类型）。
func WithResumeFrom(cp *Checkpoint) RunOption {
	return func(o *runOptions) {
		o.resumeFrom = cp
	}
}

// Run 执行 graph 直到所有路径收敛到 EndNode。
//
// **核心不变量**：
//
//  1. **isEnd 检测和 next-tasks 计算合并**——nextFrontier 算完立即判 done，
//     不留跨 iteration 窗口（plan_chat_web bug 类彻底消除）
//  2. **ctx 检查在 done 检查之后**——已完成的 graph 不被 ctx 状态影响；
//     还要继续干才检查 ctx（「决定继续之后」才 meaningful）
//  3. **统一同步点 ApplyAll**——所有 ChannelWrite 在合并点统一 reduce，
//     超步内多分支并发安全
//
// **中断保护**（Phase 1.0.b 加）：ctx canceled / deadline 时 cm 内会保存当前
// frontier，调用方可调 cm.Snapshot() 持久化；ResumeFrom 从该 frontier 起跑。
//
// OrchHooks / InterruptError ask_user / 子图独立 cp 均已在本 Run 接全（原 PoC 限制已解除）。
//
// # 在节点里跑嵌套图
//
// 节点体（或它的钩子）里再调 Run，那张图是这个节点这次执行的一部分：它的图级钩子与入口条件边记在
// 这次执行名下，按「同一个 ns 在这次执行里是第几遍」编号（节点重跑时从 0 重新数）。⚠️ 所以同一次
// 执行里对同一个 ns 跑好几遍时，**每一遍都照跑，别按进度跳过已经跑完的**：跳过前一遍的话后一遍的
// 编号往前挪一位，拿回的是前一遍记下的东西（入口条件边记的路由被照搬，实测）。要按进度跳过，给每一遍
// 不同的 ns（比如带上工单号）。
//
// **嵌套图跑完之后节点又失败或挂起**（不是条件边否决，是节点体后面的步骤出错、或后面又问了人）：续跑时节点重跑，
// 嵌套图的存档在它成功时已清，于是它**从头再跑**——一次重生：节点体副作用、草稿会再产一遍，里面的提问换新身份（这张图每跑完一次，
// 外层节点执行名下记一笔；从头再跑时按记下的次数换 ID，续跑它自己存档的那一遍次数不变），用户被问到的是新内容，
// 调用方累积传的旧答案不兑付给它（修之前只问过「批准 v1」，执行的是「v2 已批准」，实测）。重生沿外层链往里传，
// 更里层的提问一起换新。⚠️ 所以节点在嵌套图跑完之后每挂起一次，续跑时那张图的问题就重问一轮（内容可能已经变了，
// 拿旧答案不安全）；节点体自己的 Ask 身份不变、答案照旧拿回——它问的若依赖嵌套图的产出，同样要先把产出记下来。不想让它重跑：跑完就用 [SaveNodeState] / [SaveStepResult] 记下，续跑时跳过它；或者
// 把它包成 agent 经 `agent_v2.RunSubAgent` 调（嵌套调用记完成记录，续跑时拿回、不重跑）。⚠️ 自己跳过的话，
// 那张图做出的上行信号（转交 / 路由决定）不会再做一次：用 [CaptureSignals] 包住嵌套 Run、把 [Signals] 随记录
// 一起存下，跳过时 [EmitSignals] 交出，否则续跑后决定静默丢失（实测，`err == nil`；`RunSubAgent` 已经这样做）。
//
// 要开一个跟当前执行无关的独立运行，先 [DetachExecution]。
func Run(ctx context.Context, g *Graph, cm *ChannelManager, opts ...RunOption) (runErr error) {
	// 脱离出来的独立运行的根图（[DetachExecution] 之后、不在任何节点执行范围里）：它挂起时抛出的
	// 中断属于它自己那个任务，标上记号再往外交（原中断照样取得到）——调用方所在的任务从节点里把它
	// 返回时，引擎据此拒收（见 validateInterrupt）。记号打两处：外面包一层，再在交出去的中断对象上记一笔
	// ——业务常把 IsInterrupt 取出来的对象直接返回或再包一层，只有包装的话那两种写法认不出（实测活锁）。
	// 交出去的是原中断的拷贝（IsInterrupt / errors.As 走到包装时拿到它）：业务把中断对象当包级变量复用时
	// 改原对象会让记号漏进它以后的普通挂起（审计实测）。中断对象不进存档（存档只存 AskQuery）。
	// **第一个注册、最后一个跑**：看到的是本函数最终要返回的那个错。
	detachedRoot := isDetachedRoot(ctx)
	var detachedSID string
	defer func() {
		if !detachedRoot || runErr == nil {
			return
		}
		if ie, isInt := IsInterrupt(runErr); isInt {
			wrapped := &detachedInterruptError{err: runErr, sid: detachedSID}
			if ie != nil {
				marked := *ie
				marked.detached, marked.detachedSID = true, detachedSID
				wrapped.marked = &marked
			}
			runErr = wrapped
		}
	}()
	// 这次执行还在跑的标记（见 runAliveKey）：不在任何节点执行范围里的 Run 铸一个、出口置否，嵌套的 Run 继承。
	// 脱离时记进来源链，从存下来的 ctx 脱离的那一项在这次执行结束之后不再凭它算「仍在执行」。这样的 Run 配了 sid
	// 时还登记「这个任务有执行在跑」（见 executingTasks，要等下面解析出 sid 才登记）。置否与注销放在这个早注册、
	// 晚执行的 defer 里：出口的存档、OnGraphEnd 跑完之前，这次执行都还在跑。
	rootRun := !WithinNodeScope(ctx)
	var exitExecuting func()
	if rootRun {
		alive := new(atomic.Bool)
		alive.Store(true)
		ctx = context.WithValue(ctx, runAliveKey{}, alive)
		defer func() {
			alive.Store(false)
			if exitExecuting != nil {
				exitExecuting()
			}
		}()
	}
	// stayPendingFact 是「本次 Run 里有没有任何一个分支的退出方式值得把存档留在可恢复
	// 那一档」——**事实，不是报告**。出口的 Status 盖章读它，不读被仲裁挑出来的那一个
	// error（理由与实测见 frontierResult.StayPending）。
	//
	// 走局部变量而不是把它塞进返回的 error 里：后者等于让「报告」再次驮着「后果」，
	// 正是这轮要拆开的东西。
	var stayPendingFact bool
	// **本 defer 只收 defer 链自身抛的 panic**（业务 OnGraphEnd hook / 自定义 checkpointer
	// 的 SaveNS / DeleteSubtree）。完整分工见 panic_recovery.go——**别在这里复述它**。
	//
	// audit² 改: 用 defer recover 替代之前散落 21 个 per-hook safeOn* helper。
	//
	// **它现在只兜 defer 链自身的 panic**（业务 OnGraphEnd hook / 自定义 checkpointer 的
	// SaveNS / DeleteSubtree）。主体的 panic 由 Run 出口那个「主体 panic 先转 runErr」的
	// recover 先接走（它注册最晚 ⟹ 最先跑）；NodeFunc 的 panic 归 runNodeWithHooks。
	// 三者分工互不遮蔽，实测各接各的。
	//
	// **引擎深审 F-D fix**: runErr 已有值时用 errors.Join 保双链, 不无条件覆盖——
	// 之前节点抛 InterruptError 后 defer 链 panic (业务 OnGraphEnd hook / 自定义
	// checkpointer 的 Snapshot/SaveNS/DeleteSubtree), cleanup defer 已按原
	// runErr=InterruptError 存了 Pending cp, 但 recover 覆盖 runErr 让业务
	// IsInterrupt=false 判致命错不 Resume——cp 说挂起、返回值说失败, 状态矛盾。
	// 与 R12-2 (ApplyAll panic + 分支错的 errors.Join 双链决策, 见超步循环内
	// `errors.Join(branchErr, applyErr)`) 同一协议——panic 不销毁已产生的
	// typed error 链, IsInterrupt / errors.As 穿透 Join 两支都能命中。
	// 兑付集上冒 defer（**必须注册在下面那道 outer recover 之前**——LIFO 下它才真的
	// 殿后、看得见 defer 链自己抛的 panic 被转成的 runErr）：**成功出口**把本图 cm 的
	// 兑付集 record 进外层节点账本（嵌套时才有外层；根层 no-op）——否则子图的兑付
	// 记录随本层 cm/cp 一起消失，外层从此不记得那些位已兑付，撞 ID 的后续提问会把
	// 旧答案再吃一遍（answerLedger godoc「parent 链」）。
	//
	// ⚠️ **这里原先注册在 cleanup 之前、并声称「LIFO 整条链最后跑」——那是假的**：
	// outer recover 注册得更早，于是上冒排在它前面，看不到 defer 链自身的 panic
	// （业务 OnGraphEnd hook / 自研 checkpointer 的 SaveNS/DeleteSubtree）被转成的
	// 错误 ⟹ 一个**以失败告终**的 Run 照样把兑付集冒上去 ⟹ 后续合法的再问被判
	// 已兑付、终局失败。独立审计用两个 fixture 复现过这个症状。
	//
	// **独立于持久化配置**（故不放 cleanup defer 里——那里 cper/sid 缺省会早退）：
	// 账本不变量属于沿 ctx 活着的答案 map，不属于 cp；无 cper 的嵌套图同样要冒。
	// cm 为 nil 时 bubbleConsumedAnswersTo 自己 no-op，故注册在 nil 检查之前安全。
	//
	// **往哪儿冒，在这里、ctx 被换掉之前就取下来**：下面 `OnGraphStart` 返回的 ctx 会替换掉
	// `ctx` 这个变量，而钩子可能返回一个不是从入参派生的 ctx（违约，但实测有人沿用别处缓存的
	// ctx）。defer 里再读 `ctx` 的话读到的是钩子那一个——上面没有调用方的账本、也没有调用方在收
	// 的信号，图成功了它们也交不出去，`err == nil`（实测）。
	callerLedger := getAnswerLedger(ctx)
	callerSignals := getSignalScope(ctx)
	// endHookSignals 收 `OnGraphEnd` 做出的信号：它跑在本图交出信号之前（defer 后进先出），直接
	// 交给调用方的话会被本图交出的同一个收件人上的信号盖掉——而它是后做的。先收在这儿，排在本图
	// 的后面交（见 signal.go「钩子里做出的信号」）。
	endHookSignals := &signalScope{}
	// recordCompletion 在外层节点执行名下记一笔「这张嵌套图又跑完了一次」（见 nestedRegenKey），Run 入口算出 key 之后
	// 设好；放在这个 defer 里跟兑付上冒一起，看的是最终的 runErr。
	var recordCompletion func()
	defer func() {
		if runErr == nil {
			if recordCompletion != nil {
				recordCompletion()
			}
			bubbleConsumedAnswersTo(callerLedger, cm)
			// 上行信号同理：本图已提交的（含续跑时从存档恢复的）交给调用这张图的那段执行，
			// 失败 / 停下时不交（见 signal.go）。
			bubbleSignalsTo(callerSignals, cm)
		}
		// 图级钩子的信号属于调用方那段执行，图成没成都交（那段执行自己的提交决定它算不算）
		callerSignals.add(endHookSignals.drain()...)
	}()

	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			panicErr := panicerr.Wrap("compose_v2.Run panic recovered", pe)
			panicerr.Log(ctx, slog.LevelError, "compose_v2: Run 收尾段（图失败时的 OnGraphEnd 钩子）panic，已转成错误", pe)
			if runErr != nil {
				runErr = errors.Join(runErr, panicErr)
			} else {
				runErr = panicErr
			}
		}
	}()
	if g == nil {
		return errors.New("graph is nil")
	}
	if cm == nil {
		return errors.New("channel manager is nil")
	}
	// **记下「我是不是从某个节点体里被调进来的」**——这是 OnGraphStart 拒不拒中断的判据
	// （见下方使用点）。`clearInheritedNodePosition` 清的是节点**身份**维度，不清这枚章：
	// 嵌套图的图级钩子相位同样在外层节点的执行范围里（见 withinNodeScopeKey）。
	//
	// 用引擎盖的章、不用 `cfg.graphNS != ""`：ns 是**寻址**用的字符串，业务可以在顶层
	// 自己 push 一个（租户前缀），拿它当「有没有父节点接得住中断」用是借来的量——
	// 实测顶层带 ns 时中断被放行，而那一刻根本没有能落档的地方（IsInterrupt=true 但
	// 挂起档 0，正是要消灭的假暂停）。
	enteredFromNodeBody := WithinNodeScope(ctx)
	// 嵌套图的图级钩子（OnGraphStart / OnGraphEnd）记东西的地方：承载这张图的那个外层节点
	// 执行，在钩子自己的分域里（见 withHookPhase）。外层节点重跑时这张图跟着重跑、图级钩子再
	// 触发一次，钩子里记下的从那里读回。入口条件边同样记在这里。顶层图没有外层节点，它的图级
	// 钩子与入口条件边里没有可记的地方。
	outerAcc := nodeStateAccessorFrom(ctx)
	// hostAcc 是承载这张图的外层节点执行（所在分域）——**亲口新开也不改变它**：图的输入仍是它
	// 产出的（入口否决时推进的是它的代际；交接过来的主图例外，见下面 producer）、图里的提问仍在
	// 它的执行里（带它的代际）、图级相位仍按它编号。亲口新开只影响「记在它名下的图级相位记录拿
	// 不拿回」（下面把 outerAcc 置 nil）。
	hostAcc := outerAcc
	// 外层执行的计数器（下面给图级相位编号用）在清位置之前留住
	outerCtx := ctx

	// 进一张新图 ⟹ 外层图的节点位置作废。不清的话子图的 OnNodeStart 会读到父节点的
	// 身份（普通 ctx value 跨边界继承），算出的中断身份跨 Resume 会漂。
	ctx = clearInheritedNodePosition(ctx)
	// 出口读写的宽限状态：外层 Run 放过就沿用（后端在这个任务的出口挂住过，各层都不再等，见 exitGrace）
	ctx, releaseExitGrace := withExitGrace(ctx)
	defer releaseExitGrace()

	cfg := &runOptions{}
	for _, opt := range opts {
		opt(cfg)
	}
	// 运行期断点的节点名在这里校验——此刻一个字节都还没动，报错不碰存档（见 errInvalidBreakpoint）
	if err := checkBreakpointNodes(g.nodes, "before", cfg.interruptBefore); err != nil {
		return &errInvalidBreakpoint{msg: "compose_v2.WithInterruptBefore: " + err.Error()}
	}
	if err := checkBreakpointNodes(g.nodes, "after", cfg.interruptAfter); err != nil {
		return &errInvalidBreakpoint{msg: "compose_v2.WithInterruptAfter: " + err.Error()}
	}
	breakBefore := g.breakBefore.with(cfg.interruptBefore)
	breakAfter := g.breakAfter.with(cfg.interruptAfter)
	// **RunOption 版的「新开」声明立刻落进 ctx**——否则它只管本层，嵌套子图读不到。
	//
	// 子图的判据是 `IsFreshRunCtx(ctx)`（`subgraph.go`），而 `cfg.freshRun` 只在本文件的
	// auto-load 闸门被消费。不接这一步的话，调用方按 `WithFreshRun()` 的说明开新任务，
	// **子层照旧把上个任务的挂起存档捡起来接着跑**，且 `err == nil`。
	//
	// 两个入口说的是同一句话，就该汇成同一个信号——留两条路各管一半，正是本仓反复栽的
	// 那个形状。
	if cfg.freshRun {
		// 调用方在这一层**亲口**声明新开（ctx 上本来没有这个信号）：这次什么也不续，记在外层节点
		// 执行名下的图级相位记录（入口条件边、OnGraphStart / OnGraphEnd）也不拿回、不记。不然外层
		// 节点因为别的错重跑、嵌套图明明从头跑，入口条件边却拿回上一次的路由（实测）。继承来的信号
		// 不算：那是整个任务的首跑，外层节点执行名下还没有上一次可拿。
		if !IsFreshRunCtx(ctx) {
			outerAcc = nil
		}
		ctx = WithFreshRunCtx(ctx)
	}

	// **持久化配置单源化：RunOption 缺省时回落 ctx 形态，本入口是唯一的合并点。**
	//
	// 此前根层只认 RunOption、而子图层读 ctx（`subgraph.go` 的 `GetCheckpointer(ctx)`）——
	// 同一个「持久化开没开」两处各读各的。业务只用 `WithCheckpointerCtx` 调裸 Run 时会
	// 构造出**半持久化树**：根不存档不 auto-load，子图照存——实测后果是根槽恒空 ⟹ 每次
	// Run 新铸任务代号 ⟹ 子图挂起档的旧代号对不上 ⟹ 用户已答的问题被丢弃、同一问题换个
	// ID 再问一遍（`config_single_source_test.go` 钉着这条）。
	//
	// 回落之后两个方向都闭合：缺省从 ctx 取 ⟹ 根子必然同源；下方既有的 push-down 又把
	// 合并结果写回 ctx ⟹ 下游只见合并后的单一真相。**没有「显式禁用」档**——
	// `WithCheckpointer(nil)` 归一成「未设」（见其 typed-nil 归一），「根层明确不要持久化
	// 但 ctx 里有」这一档在项目里没有真实调用方，不为脑补场景加哨兵。
	//
	// graphNS 同批回落：不回落的话「ctx push 了 ns + 裸 Run」会让根档落在 ""、子档落在
	// `ctxNS/...`——根不是子的 ns 祖先，树自身错位。
	if cfg.checkpointer == nil {
		cfg.checkpointer = GetCheckpointer(ctx)
	}
	if cfg.sessionID == "" {
		cfg.sessionID = GetSessionID(ctx)
	}
	detachedSID = cfg.sessionID
	// 一个任务的一次执行从这里开始：登记它在跑（出口在上面那个 defer 里注销），并把继承来的来源链里它那几次已经结束
	// 的执行去掉——在它自己的执行里，它不是「脱离出来之前所在的任务」（见 withoutDetachedFrom）。
	if rootRun && cfg.sessionID != "" {
		exitExecuting = enterExecutingTask(cfg.sessionID)
		ctx = withoutDetachedFrom(ctx, cfg.sessionID)
	}
	if cfg.graphNS == "" {
		cfg.graphNS = GetGraphNS(ctx)
	}
	// 图级相位（OnGraphStart / OnGraphEnd / 入口条件边）的名字：这张图的 ns，嵌套时再加「外层这次
	// 执行里第几次跑这个 ns 的图」。它们记在承载这张图的外层节点执行名下，同一次外层执行在循环里
	// 对同一个 ns 跑两遍（两份相同的工单各跑一遍）时，只按 ns 分的话第二遍拿回第一遍的记录
	// （实测：路由结果被照搬）。编号按 ns 分开数、外层重跑时从 0 重新数，同 RunSubAgent 的次序维。
	//
	// 带不带编号只看「是不是嵌套」（hostAcc），不看亲口新开之后被置 nil 的 outerAcc：agent_v2 顶层
	// 任务首跑时 ctx 上带着新开信号、续跑时没有，按 outerAcc 判的话同一张嵌套图首跑带编号、续跑
	// 不带，图级相位里发起的调用在首跑与续跑之间换了身份（实测：同一个把关问题问了两遍）。
	graphPhaseNS := cfg.graphNS
	if hostAcc != nil {
		graphPhaseNS += "\x00" + strconv.Itoa(NextSubCallIndex(outerCtx, "\x00graph\x00"+cfg.graphNS))
	}
	// 这张图的输入是谁产的（入口条件边按 CondFailRerunFrom 否决时推进谁的代际）：默认是承载它的
	// 那个分域——节点体里直接跑的嵌套图，输入就是节点体给的。**交接过来的主图例外**：被调方的
	// 执行是调用方经 HandOver 交过来的，落在交接 ns 上的这张图是被调方的主图、输入是调用方给的，
	// 产出方是交接之前调用方的那一格（hostAcc 是封给被调方的那一格：推进它的话调用方按
	// Rerunnable 记下的产出原样拿回，续跑永不收敛，实测）。
	producer := hostAcc
	handoverMain := hostAcc != nil && hostAcc.handover != nil && hostAcc.handover.ns == cfg.graphNS
	if handoverMain {
		producer = hostAcc.handover.from
	}
	// 外层代际链：图里的提问带上承载它的每一层外层执行此刻的重做代际（见 InterruptSite 的
	// OuterRerunGens）。顶层图清空（ctx 可能是从别的执行里带出来的）。交接过来的主图不用特判：承载
	// 它的是交接那一格，那一格的有效代际从交接那一层数起、不算调用方那一层（见
	// nodeStateAccessor.handedOver）——调用方重做时「这是不是同一次调用」由调用的身份决定，身份没变、
	// 正挂着的调用里已经答过的问题不换 ID（曾经带上调用方那一层，实测被再问一遍）；调用方所在那张图
	// 之外的各层照带：那几层被否决重做时调用方的图从头跑、这次调用的完成记录跟着没了，被调方重新
	// 产出，提问必须换新（曾经交接主图把整条链清空，实测问过「批准 plan-v1 吗」，plan-v2 拿那个回答
	// 放行，err=nil）。
	cm.outerGens = outerRerunGensFor(hostAcc)
	ctx = withOuterRerunGens(ctx, cm.outerGens)
	// 重生次数：这张图（graphPhaseNS：ns 加这次外层执行里的第几遍）在承载它的执行里已经跑完过几次——从头再跑时提问换
	// 身份，续跑自己存档时次数没变（见 nestedRegenKey）。顶层图没有外层执行，链为空。
	regen := 0
	if hostAcc != nil {
		key := nestedRegenKey(graphPhaseNS)
		if n, ok := GetStepResult[nestedCompletions](outerCtx, key); ok {
			regen = int(n)
		}
		done := nestedCompletions(regen + 1)
		recordCompletion = func() {
			if err := SaveStepResult(outerCtx, key, done); err != nil {
				slog.WarnContext(outerCtx, "compose_v2: 记下嵌套图跑完失败，外层失败续跑时它重生出来的提问可能沿用旧 ID",
					"ns", cfg.graphNS, "err", err)
			}
		}
	}
	cm.outerRegens = outerRegensFor(hostAcc, regen)
	ctx = withOuterRegens(ctx, cm.outerRegens)
	// HITL: 注入 ResumeContext 让节点函数可读 user 答案
	//
	// **audit Round 9 Bug B4 修复**：caller 提供的 answers 与 cm 当前 pending
	// InterruptID 不匹配时 slog.WarnContext 提示——业务前端拼错 ID / 重复提交
	// silent 失败时有诊断信号，不再"为什么 Resume 后又中断"陷入排查死循环。
	// 两条通道独立：`resumeAnswers` 按 InterruptID 精确投递（寻址型），`resumeUserText`
	// 不指向任何提问（非寻址型）。任一非空就注入。
	if cfg.resumeAnswers != nil || cfg.resumeUserText != "" {
		ctx = withResumeContext(ctx, &ResumeContext{
			UserInputs: cfg.resumeAnswers,
			UserText:   cfg.resumeUserText,
		})
	}
	if cfg.resumeAnswers != nil {
		// **audit Round 10 Bug B5 防御**：WithResumeAnswers 无 cp 来源（既无
		// WithResumeFrom 又无 cper 自动 load 条件）时 caller 显然误用——typically
		// fresh Run 误传 answers 让业务节点 GetResumeContext 假阳性命中。
		if cfg.resumeFrom == nil && (cfg.checkpointer == nil || cfg.sessionID == "") {
			slog.WarnContext(ctx, "compose_v2: WithResumeAnswers 无 cp 来源——可能 caller 误用",
				"hint", "WithResumeAnswers 应配 WithResumeFrom 或 WithCheckpointer+WithSessionID 让框架 auto-load",
				"answer_count", len(cfg.resumeAnswers))
		}
		// 显式 WithResumeFrom 路径立即检查 unmatched ID（auto-load 路径在 cp 加载
		// 后另行检查——audit Round 10 Bug B1 修复）
		warnUnmatchedAnswerIDs(ctx, cfg.resumeFrom, cfg.resumeAnswers)
	}

	// Phase 5.1.a Step 3: 注入 Checkpointer / sessionID / graphNS 到 ctx
	// 让 subgraph 节点 + 业务自定义 NodeFunc 可读
	if cfg.checkpointer != nil {
		ctx = withCheckpointer(ctx, cfg.checkpointer)
	}
	if cfg.sessionID != "" {
		ctx = withSessionID(ctx, cfg.sessionID)
	}
	if cfg.graphNS != "" {
		ctx = WithGraphNS(ctx, cfg.graphNS) // 复用 ns_ctx.go 已有的 WithGraphNS helper
	}
	// durability 解析（RunOption 显式 > 父 ctx 继承 > 默认 Exit）+ push 给下游，让嵌套
	// 子图自动继承父档（各写自己 ns）。Exit 时下游也读到 Exit，行为与历史一致。
	cfg.durability = resolveDurability(ctx, cfg.durability)
	ctx = WithDurabilityCtx(ctx, cfg.durability)
	// 守卫同款解析 + push：RunOption 显式开 > 父 ctx 继承。嵌套子图各守自己 ns 的槽，
	// 不透传会让子 cp 被陈旧执行者覆盖而父层浑然不知（父子撕裂）。
	if !cfg.staleWriteGuard {
		cfg.staleWriteGuard = GetStaleWriteGuard(ctx)
	}
	if cfg.staleWriteGuard {
		ctx = WithStaleWriteGuardCtx(ctx)
	}
	// softCancel 解析（RunOption 显式 > 父 ctx 继承）+ push 给下游，让嵌套子图继承同一信号
	// （顶层 Trigger 优雅停整棵树）。
	cfg.softCancel = resolveSoftCancel(ctx, cfg.softCancel)
	if cfg.softCancel != nil {
		ctx = WithSoftCancelCtx(ctx, cfg.softCancel)
	}
	// Phase 2 Sub-3 audit fix: emitCpEvent 双路径——cfg.emitCpEvent (RunOption) 或
	// ctx flag (业务直接 compose_v2.WithEmitCpEventCtx(ctx, true) 一次注入).
	// 任一开 → cfg.emitCpEvent + ctx flag 都 true 让 Run defer + subgraph 都 fire.
	// 8 个非-BasicAgent 编排没有 WithEmitCpEvent Option, 只能靠 ctx flag (跨编排协议
	// 一致性); 9 编排 wrapper 都透传 ctx, 嵌套子 graph 继承.
	if cfg.emitCpEvent || isEmitCpEventCtx(ctx) {
		cfg.emitCpEvent = true
		ctx = WithEmitCpEventCtx(ctx, true)
	}

	// ctx-aware OrchHooks fallback——RunOption WithHooks 优先，未传时从 ctx 取
	// （compose_v2.WithOrchHooks(ctx, h) 注入）。让嵌套 sub-agent.Run 在 compose_v2
	// 引擎层自动继承父 hooks，业务不用每层显式 WithHooks RunOption。
	//
	// 优先级：cfg.hooks (RunOption) > ctx OrchHooks > NoOp
	// audit² 改: 直接调 raw 接口, hook panic 由 Run 入口 outer defer recover 兜底.
	if cfg.hooks == nil {
		if ctxHooks := GetOrchHooks(ctx); ctxHooks != nil {
			cfg.hooks = ctxHooks
		}
	}
	if cfg.hooks == nil {
		cfg.hooks = NoOpOrchestrationHooks{}
	}

	// 四编排 review RT-H1 fix：Run-level EventSink 兜底——ConditionalEdge cond fn
	// 跑在 Run-level ctx（不在任何 node scope），其内 framework event 此前 GetEventSink
	// 恒 nil 静默丢（框架靠这些事件自动记的指标随之缺失；嵌套时借父图 node sink 才 fire——
	// 同一逻辑两形态行为不一致）。
	// node 入口 withEventSink 会遮蔽本 sink（node 内 event 不双 fire），仅 cond fn /
	// 未来 out-of-node emitter 落到这里。nodeID "" 标"非节点 framework 级 event"
	//（对齐 Run defer cp event 的 OnNodeEvent(ctx, "", ev) 范式）。
	//
	// 注：本 fix 原举例的 "RoutingAgent classifierAsRouter 的 route_decision" 已不适用
	// ——RoutingAgent 的路由决策（含 EmitRouteDecisionEvent）已移到 classify node 内，
	// 走 node 自己的 EventSink。本兜底仍保留给业务自定义 cond fn 内的 event。
	//
	// OnGraphStart 采纳之后会换一次（见那里）：图级事件要带着图自己定下来的 ctx。
	ctx = withEventSink(ctx, graphEventSink(ctx, cfg.hooks, cfg.graphNS))

	// 注册所有声明的 channel
	for _, ch := range g.channels {
		if err := cm.register(ch); err != nil {
			return fmt.Errorf("register channel %q: %w", ch.name(), err)
		}
	}

	// 世代基线：本 Run 载入的是哪一代 cp（见 cm.cpEpochBaseline）。两条载入路径都要设
	// ——resumeFrom 在此、auto-load 在下方。全新 Run 留空 = 无基线可比，守卫跳过。
	if cfg.resumeFrom != nil {
		cm.setCpEpochBaseline(cfg.resumeFrom.Epoch)
		// **还要确认槽当前是不是空的**：caller 手里的 cp 可能来自别的 sid（fork / 从
		// 归档恢复到新 sid），此时目标槽本来就没东西。不探测的话 `slotWasEmpty` 恒
		// false，守卫看到「基线非空 + 槽为空」就判成"有人删过"——**全程只有一个执行者
		// 也会被误判**，而且是 fail-closed：写回被拒 ⟹ 存档根本没落盘，caller 却拿到
		// InterruptError 去问用户，答完 Resume 撞 ErrNoPendingCheckpoint，fork 出的会话
		// 永久不可 Resume。
		//
		// 只在守卫开启时探（多一次 LoadNS）——默认路径零额外 IO。
		if cfg.staleWriteGuard && cfg.checkpointer != nil && cfg.sessionID != "" {
			switch cur, err := cfg.checkpointer.LoadNS(ctx, cfg.sessionID, cfg.graphNS); {
			case err != nil:
				// **不能倒向任何一边**：当"空"会让普通 Resume 在读抖动时误判成"载入时
				// 空、现在非空"；维持默认（当"非空"）则槽真为空时报"别人删了"——一次
				// 读抖动就够让 fork 到空槽的会话拿到 stale + 存档不落盘 + 永久不可
				// Resume。标第三态，由 checkStale 按 fail-open 处置。
				slog.WarnContext(ctx, "compose_v2: stale-write guard 探测槽状态失败，按未知处理",
					"session", cfg.sessionID, "ns", cfg.graphNS, "err", err)
				cm.markCpSlotUnknown()
			case cur == nil:
				cm.markCpSlotEmpty()
			}
		}
	}

	// **audit³ A1 fix**：原代码 OnGraphEnd + cleanup 同 defer 单 statement——
	// OnGraphEnd panic 跳过 SaveNS/DeleteSubtree cleanup. 拆两 defer + inner
	// recover 让 hook panic 不冒泡跳过 cleanup. 同时 A3 fix: OnGraphStart 早
	// 退路径靠 defer 调 OnGraphEnd (不在早退路径手动调), 让 panic 自动配对.
	//
	// defer LIFO: cleanup 先注册 → 后跑; OnGraphEnd 后注册 → 先跑.
	// 保持原语义: OnGraphEnd 看 cleanup 前的 runErr; cleanup 可 wrap runErr.

	// cleanup defer: SaveNS/DeleteSubtree 根 cp。**别在注释里数「注册 N / 跑 M」**——这个
	// 函数里的 defer 不止表面这几道（顶部还有 outer recover 与兑付上冒），每加一道那些
	// 数字就全错一遍，本文件为此错过一次。要判先后直接看注册位置。
	defer func() {
		// **F4 修复**（Phase 5.1.a Step 5）：Run 出口自动持久化根 graph cp，
		// 让根 graph cp 协议与子图 cp 协议对称（之前只有子图 cp 自动 SaveNS，
		// 根 graph 失败时业务必须手动 cm.Snapshot + 自管 save——不一致）。
		//
		// 规则：
		//   - cper + sid 存在
		//   - runErr 命中 ErrRunAborted（终局失败）→ **主动 DeleteSubtree 清子树**（见下）
		//   - runErr != nil（其余可 Resume 的失败/中断）→ SaveNS(sid, graphNS) 写根 cp（Status=Pending）
		//   - runErr == nil → DeleteSubtree(sid, graphNS) 清根 cp 子树
		//     （含本 graph 自身 + 任何残留子图 cp）
		//   - 要写的档没写成 → ErrCheckpointNotPersisted（见 newCheckpointNotPersisted）
		if cfg.checkpointer == nil || cfg.sessionID == "" {
			return
		}
		// 出口的每一次存储读写都经 exitStorageOp：不跟着调用方取消（调用方结束之后再给一段宽限），后端
		// panic 转成错误（不然它冒到最外层的 recover、跟中断并列，中断还在错误链上）。**先定结论、再发
		// 事件**：cp 事件与清档日志在 runErr 定下、该清的档清完之后才发，钩子与日志 handler 的 panic 当场兜住、
		// 不进错误（见 exit_checkpoint.go 的三条不变量）。
		if runErr != nil {
			// **终局失败 → 主动清 cp 子树**（引擎深审 HIGH fix）：单槽覆盖式 cp 下
			// "跳过写" ≠ "没有 cp"——先前 HITL 中断存的档 / durability 每超步写的中途 cp
			// 会**原样幸存且 Status=Pending** → auto-load 照样捡起 → 业务重跑已成功节点、
			// 被重新问已答过的问题；子图场景（子图跳过写但父图照存）更退化成 livelock。
			// 故终局失败必须**主动删**，与成功路径同款 DeleteSubtree——两者都是"这个 sid
			// 的活干完了、cp 不该留"，只是结论不同。子图的 abort 错沿错误链冒泡到父
			// NodeFunc，父 Run 出口同样命中 → 终局性逐层向上传播，无需显式协调。
			// 详见 ErrRunAborted godoc。
			if errors.Is(runErr, ErrRunAborted) {
				// **陈旧执行者不许删别人的子树**：我基于过时状态跑出的终局失败，
				// 不构成删掉别人成果的理由。保留 ErrRunAborted 不换成 stale——
				// "终局失败别重试"对 caller 更要紧，陈旧只是附注。
				purge := purgeExitSlot(ctx, cfg, cm, "abort", "purge-abort", "")
				switch {
				case purge.staleErr != nil:
					// 双 %w：ErrRunAborted（"别重试"）和 ErrCheckpointStale（"你的账没记上"）
					// 业务都要能 errors.Is 判出来。用 %v 包 staleErr 会让 stale 判定恒 false，
					// 业务照文档写 errors.Is(err, ErrCheckpointStale) 在本路径直接漏判。
					runErr = fmt.Errorf("%w (跳过清理 cp: %w)", runErr, purge.staleErr)
				case purge.failure() != nil:
					// 删失败必须让业务知道——旧 cp 残留就会被 auto-load 捡起（本 fix 要防的正是它）。
					// 删除失败的原因只进文本（可能带着后端返回的 ctx 错），兜住的 panic 照样挂上。
					runErr = attachPanicOf(fmt.Errorf("%w (abort cp purge failed: %v — 旧 cp 可能残留并被 auto-load 捡起)",
						runErr, purge.failure()), purge.failure())
				}
				purge.report(ctx, cfg)
				return
			}
			var rootCp *Checkpoint
			snapErr := exitStorageOp(ctx, exitOpLocal, "checkpoint snapshot", func(context.Context) error {
				var err error
				rootCp, err = cm.Snapshot()
				return err
			})
			if snapErr != nil {
				// 快照不出来就写不了档，跟写档失败同样处置（同 F2 修复语义：不静默）。
				np := newCheckpointNotPersisted(runErr,
					fmt.Errorf("root cp snapshot failed (likely channel value type not registered): %w", snapErr))
				runErr = np
				if np.aborted {
					purge := purgeExitSlot(ctx, cfg, cm, "abort", "purge-not-persisted", "")
					np.recordPurge(purge)
					purge.report(ctx, cfg)
				}
				return
			}
			if !rootCp.HasResumableWork() {
				// 根 graph 没中断点 + 没 ask_user——视作不需要 cp（业务错快速失败场景）
				return
			}
			// **带着「不是控制流」的错退出 ⟹ 这是失败现场不是挂起**（见 CPStatusFailed）。
			//
			// 判据是 `shouldStayPending`（见 retry.go，**函数体是真相源、按类目读别数个数**），
			// 它**宽于** `IsControlFlowSignal`：
			//   - 控制流信号：HITL 中断（有人等答案）、软取消（业务喊停、之后要续跑，其
			//     godoc 明写「业务可 Resume 续跑」）、陈旧写回（另一个执行者推进了会话）；
			//   - **外加两类返业务错、但框架仍要它被重跑的**：`CondFailRerunFrom` 档的 cond
			//     报错、坏存档的 `ErrCheckpointNotResumable`。
			// 以上都算**挂起**，存档该被 auto-load 正常捡起。剩下的才是失败。
			// ⚠️ 只按「是不是控制流信号」判会把该留 Pending 的标成失败现场。后果**不是**
			// 续跑断掉——2026-08-13 起续跑闸门按内容判（`HasResumableWork`）、不读 Status——而是它在
			// `ListPending`（业务查「谁在等答案」的入口）里列不出来。
			//
			// **不看错误是瞬时还是确定性**——框架判不了这个（它看到的只是一个 error，
			// 判不了下次会不会好），也不必判：标出「这是失败现场」就够了，要不要再试是
			// 业务的事。存档照留，业务显式 Resume 仍能从失败点续跑。
			//
			// durability 中途写的不受影响：那条路走另一个写入点、恒 Pending——它是崩溃
			// 恢复点，必须能被 auto-load 捡起。
			// **判据是「有没有分支值得留」，不是「谁赢了仲裁」**：runErr 只是报告，
			// 而报告在部分失败时按 frontier 声明序挑第一个（见 frontierResult.StayPending）。
			// 仍然要 || runErr 那一半——单分支路径 / apply 阶段出错 / hook 错等
			// 不经 executeFrontier 收集的退出，事实位不会被置上。
			if runErr != nil && !stayPendingFact && !shouldStayPending(runErr) {
				rootCp.Status = CPStatusFailed
			} else {
				rootCp.Status = CPStatusPending
			}
			rootCp.GraphID = g.ID() // Phase 5.1.b ISSUE-50 P0 防串台
			// opt-in 的 cp.save 事件经 deliverCpEvent 直接调 hooks（不经 EventSink；钩子 panic 就地兜住、
			// 只记日志）。node ID 用 "" 标"非节点 framework 级 event".
			// 统一写入口：世代比对 + mint + SaveNS + 更新基线（见 guardedSaveNS）
			// 陈旧写回检查与写档各是一次出口存储调用（各有各的宽限）；检查要记的日志等结论定下之后再记。
			var notes exitNotes
			var saveStart time.Time
			saveErr, checkNotDone := exitStaleCheck(ctx, cfg, cm, "save", "", &notes)
			if checkNotDone != nil {
				// 写档前的检查没做成：放行（拦下会丢自己的档），记一条告警
				notes.add(func(c context.Context) {
					slog.WarnContext(c, "compose_v2: 出口写档前的陈旧写回检查没做成，放行写档",
						"sid", cfg.sessionID, "ns", cfg.graphNS, "err", checkNotDone)
				})
			}
			if saveErr != nil {
				saveStart = time.Now()
			} else {
				saveErr = exitStorageOp(ctx, exitOpWrite, "checkpoint save", func(c context.Context) error {
					var err error
					saveStart, err = writeCp(c, cfg, cm, rootCp)
					return err
				})
			}
			if saveErr == nil {
				notes.flush(ctx)
				emitCpSaveEvent(ctx, cfg, rootCp, saveStart, nil)
				return
			}
			// **档没写成就不能报告成可以续**：曾经把写档错误跟原错误一起 `%w`，中断仍在错误链上
			// ⟹ 前端照常请用户回答，答完续跑找不到档（实测工具 0 次执行），或续进槽里更早那份
			// ——已经答过的问题重问、再答一次副作用重做（实测）。现在返 ErrCheckpointNotPersisted：
			// 挂起类信号退出错误链、写档原因留在链上（`ErrCheckpointStale` / `ErrGraphIDConflict`
			// 照样 errors.Is 得出来），槽不属于别人时带 ErrRunAborted 并尽力清档。
			//
			// 入口的 GraphID 守卫只在 `Status == Pending` 时查，槽里是 Unset 的 cp（业务自己
			// Save 的 / 自研 backend 漏序列化 Status）就绕过去了，出口这次 SaveNS 是唯一防线。
			np := newCheckpointNotPersisted(runErr, fmt.Errorf("root cp save failed: %w", saveErr))
			runErr = np
			var purge *exitPurge
			if np.aborted {
				// 写档结果不明（宽限到点）时槽里可能就是这次写的那一代——rootCp.Epoch 在写之前铸好，清档前的检查认它
				// 作自己的；没走到写（被检查拦下、预算用完没发）时它是空的。
				purge = purgeExitSlot(ctx, cfg, cm, "abort", "purge-not-persisted", rootCp.Epoch)
				np.recordPurge(purge)
			}
			notes.flush(ctx)
			emitCpSaveEvent(ctx, cfg, rootCp, saveStart, saveErr)
			if purge != nil {
				purge.report(ctx, cfg)
			}
			return
		}
		// 成功路径：清根 cp 子树
		// **设计语义**：sid lifetime = 根 graph Run lifetime——graphNS=""（默认根）
		// 时 DeleteSubtree(sid, "") 清整 sid 命名空间，含本 Run 期间所有子图 cp +
		// 任何业务在同 sid 下保存的无关 cp（IsNSInSubtree("", X)=true 对所有 X 命中）。
		// 业务跨 Run 持久化用不同 sid。Phase 5.1.a 不引入 DeleteNS exact-match API，
		// 保持 PoC API 表面积。
		//
		// **cleanup 失败仅 warn**：失败不影响成功路径，但要 log 让业务可观测
		// （audit Round 2 修：之前注释承诺 warn 但实际 silent ignore）。与终局失败
		// （Abort）共用 purgeExitSlot —— 两者都是"这个 sid 的活干完了、cp 不该留"。
		// **陈旧执行者不许删别人的 cp**：删掉的可能是另一个执行者的 HITL 存档
		// ——它的用户答完回来会发现会话凭空消失（比覆盖更不可逆）。
		// 这里把"成功"降级为 ErrCheckpointStale：本次执行整个建立在过时状态上，
		// 它"成功"这件事本身就可疑（很可能重复做了别人已做过的事），业务必须知道。
		//
		// 删除失败（含后端 panic，exitStorageOp 已记带栈的日志）只记日志，不改成功的结局（见上）；清档前的检查没做成
		// 照常清（见不变量 2）。
		purge := purgeExitSlot(ctx, cfg, cm, "success", "purge-success", "")
		if purge.staleErr != nil {
			runErr = purge.staleErr
		}
		purge.report(ctx, cfg)
	}()

	// OnGraphEnd defer: 它自己抛的 panic 归 Run 入口那道（见 panic_recovery.go）。
	// cleanup 是独立 defer，unwind 期间仍跑，故无需 inner recover。
	//
	// **`graphStartFired` 守卫：start 没 fire 过就不 fire end。** 本 defer 注册得比
	// `OnGraphStart` 早（它要覆盖 start 自己失败的情形），而 start **之前**还有一段装配
	// （import baseline）也会早退——那条路上若无条件 fire end，hook 实现者拿到的是一次
	// 没有开头的结尾：start/end 成对管 span、metric inc-dec、资源 acquire-release 的写法
	// 会 double-release（独立审计实测 OnGraphStart=0 / OnGraphEnd=1）。
	//
	// 三方同款：LangGraph 把 `run_manager` 初始化成 None、`on_chain_start` 成功才赋值，
	// 末尾 `if run_manager is not None: run_manager.on_chain_error(e)` 显式守卫
	// （`pregel/main.py`）——「end 只在 start 之后」是这一类引擎的既有规则。
	graphStartFired := false
	defer func() {
		if !graphStartFired {
			return
		}
		sc := graphHookScope("OnGraphEnd", graphPhaseNS)
		endCtx := withSignalScope(withHookPhase(ctx, sc, scopedOf(outerAcc, sc)), endHookSignals)
		if runErr == nil {
			// 图已经成功跑完：OnGraphEnd 是观测钩子，它 panic 不把这次成功报成失败——就地兜住、带栈记一条 ERROR。转成错误的话
			// 存档已经按成功清掉，调用方一重试（或外层节点失败续跑）整张图从头再跑、副作用重放（实测）。失败时照旧交给 Run
			// 入口那道兜底并进错误里。
			defer func() {
				if r := recover(); r != nil {
					panicerr.Log(ctx, slog.LevelError, "compose_v2: OnGraphEnd 钩子 panic（图已经成功跑完，结果不变）", panicerr.New(r))
				}
			}()
		}
		_ = cfg.hooks.OnGraphEnd(endCtx, cm.Channels(), runErr)
	}()

	// **本 defer 收图主体的 panic 与 `runtime.Goexit`，先转成 runErr**，好让随后的
	// cleanup / OnGraphEnd 看到真实结局（否则它们看到 nil ⟹ cleanup 把用户正等着答的
	// 存档当成功清掉）。
	//
	// **位置承重**：注册在 cleanup 与 OnGraphEnd **之后** ⟹ LIFO 下整条链里最先跑。
	// 挪到它们之前就失效——那正是引入它要修的那个 bug。
	//
	// **`bodyReached` 是 Goexit 的判据**（testing.tRunner 的 `finished` 同款）：
	// Goexit 会跑 defer 但 `recover()` 返 nil，「没有 panic + 没有 error + 没走到成功
	// 出口」三者同时成立只剩这一条路（或错误出口返了 nil 的框架 bug——同样不能算成功）。
	// 判据在 **Run 这一层**收口是刻意的：图主体里任何位置（NodeFunc / hook / cond）的
	// Goexit 都在这一格被接住，不必逐个调用点设防；节点层另有一道更精确的（见
	// runNodeWithHooks）负责身份与 OnNodeEnd 配对，这道兜底负责**存档不说谎**。
	//
	// 完整分工与那次实测见 panic_recovery.go。
	bodyReached := false
	abnormalExit := &abnormalNodeExitSlot{}
	ctx = context.WithValue(ctx, abnormalNodeExitKey{}, abnormalExit)
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			panicErr := panicerr.Wrap("compose_v2: graph body panic recovered", pe)
			panicerr.Log(ctx, slog.LevelError, "compose_v2: 图主体 panic，已转成错误", pe)
			if runErr != nil {
				runErr = errors.Join(runErr, panicErr)
			} else {
				runErr = panicErr
			}
		} else if runErr == nil && !bodyReached {
			if aerr := abnormalExit.take(); aerr != nil {
				runErr = aerr // 节点层记了身份，用它
			} else {
				runErr = errors.New("compose_v2: graph body 异常终止：既没走到成功出口也没带错误" +
					"（runtime.Goexit——典型来源是 zap.WithFatalHook、testify 的 require.*、t.FailNow）")
			}
		}
		// 持锁区里兜住的 panic（业务 reducer 在提交 / 种子写入里、业务 UnmarshalJSON 在续跑恢复里）当场没能记
		// 日志——日志 handler 是业务代码，回调进来会撞上 cm 的写锁。它们都以本次 Run 的错误返回，在这里补记。
		panicerr.LogPending(ctx, "compose_v2: 持锁区里兜住的 panic（补记），已转成错误", runErr)
	}()

	// **子图 import baseline 在 OnGraphStart 之前摆好、且只在这一处摆**（新开与续跑共用）。
	//
	// 能收成一处，靠的是 baseline 与 Restore 互不相干：Restore 只写 values，baseline 是另一层，
	// 先摆后摆结果一样。曾经分成 fresh/resume 两处、都在 OnGraphStart 之后——于是子图的
	// OnGraphStart hook 里 `Get(importCh)` 拿到 `ok=false`（独立审计实测），hook 看不到自己
	// 这张图的输入。channel 注册在上面已经做完，未注册的 import 在这儿就拒。
	if len(cfg.importOverlay) > 0 {
		if err := cm.setBaseline(cfg.importOverlay); err != nil {
			return fmt.Errorf("import overlay: %w", err)
		}
	}

	// OnGraphStart hook — audit² ctx-based: hook 返新 ctx propagate 给下游节点.
	// audit³ A3 fix: inner recover 让 hook panic 转 startErr, 走早退路径让 defer
	// OnGraphEnd 自动配对调用 (避免 panic 跳过 OnGraphEnd 配对承诺).
	// 放标记、调、采纳收在 callHookOnto 一处（见 CallHook）；采纳之后图接着用的执行状态由 onto 铺（下面的注释）。
	var startErr error
	newCtxNil := false
	ctx, startErr = callHookOnto(ctx, func(c context.Context) (got context.Context, err error) {
		defer func() {
			if r := recover(); r != nil {
				pe := panicerr.New(r)
				err = panicerr.WrapOpaque("OnGraphStart panic", pe)
				panicerr.Log(c, slog.LevelError, "compose_v2: OnGraphStart 钩子 panic，已转成错误", pe)
			}
		}()
		// 置位在**调用之前**：start 自己失败（返 err 或 panic）时仍要 fire end——
		// 那是既有的配对承诺（「OnGraphStart 返 error 时 OnGraphEnd 仍会调」，
		// 见 OrchestrationHooks godoc）。守卫只挡「start 压根没被调到」那条路。
		graphStartFired = true
		sc := graphHookScope("OnGraphStart", graphPhaseNS)
		got, err = cfg.hooks.OnGraphStart(withHookPhase(c, sc, scopedOf(outerAcc, sc)), cm.Channels())
		newCtxNil = got == nil && err == nil
		return got, err
	}, func(in context.Context) context.Context {
		// 钩子之后图接着用的执行状态，采纳钩子返回的 ctx 时取这一份：
		//   - 钩子那次调用的相位（计数器 / 相位名 / 记步骤的地方）只属于钩子，不往图里带——节点各有自己的；
		//   - 图的 sid：它是这张图存档的主索引、节点执行范围记下的「所在任务」。钩子往 ctx 上推了别的
		//     sid（比如把会话 ID 当 sid 推）时，节点里认出的任务跟存档所在的任务就不是同一个——嵌套调用
		//     被误拒、被调方的存档落到别的 sid 下（续跑时找不到、留孤儿档），另一个任务成功收尾时把它
		//     清掉（实测）。
		graphCtx := withAccessor(withBodyPhase(context.WithValue(in, subCallCounterKey{}, (*subCallCounter)(nil))), nil)
		if cfg.sessionID != "" {
			graphCtx = withSessionID(graphCtx, cfg.sessionID)
		}
		return graphCtx
	})
	if startErr != nil {
		// 不手动调 OnGraphEnd — defer 已注册会自动调 (with runErr=startErr after wrap).
		//
		// **顶层图的 OnGraphStart 抛中断在这里被拒**（转非中断错误、不 %w——cond 先例：
		// %w 会让 IsInterrupt 仍 true 而档没落，调用方渲染问题、用户答完 Resume 死锁）。
		// 这一刻 frontier 还没算，本层没有能挂这条中断的地方；「落初始档」被引擎自己的
		// 不变量拒绝（pending interrupts but no frontier）。嵌套子图照旧放行——中断冒给
		// 父节点路径落父档（同 agent_v2.WrapAgentStartErr 的判据与理由，两层互为镜像——
		// 判据都是**引擎盖的「在节点执行范围内」章**（WithinNodeScope），见 enteredFromNodeBody）。
		if ie, isInt := IsInterrupt(startErr); isInt && !enteredFromNodeBody {
			return &errUncatchableGraphStartInterrupt{msg: fmt.Sprintf(
				"OnGraphStart: 顶层 hook 抛了中断（reason=%q）——图还没开始跑，"+
					"没有可落的存档；要在图开始前把关，用第一个节点做 gate（图内、可落档）", ie.Reason)}
		}
		return fmt.Errorf("OnGraphStart: %w", startErr)
	}
	// nil 守卫：业务 `OnGraphStart` 返 `(nil, nil)`（忘了返 ctx 的常见写法）时，
	// 原先 `ctx = newCtx` 直接把 nil 往下传——实测节点体拿到 nil ctx 一路 panic 到
	// 「nil pointer dereference」，而真正的原因（hook 返了 nil）在错误里一个字都看不到。采纳时已沿用框架的。
	if newCtxNil {
		slog.WarnContext(ctx, "compose_v2: OnGraphStart 返回了 nil ctx，已沿用框架自己的 ctx（交给钩子之前的那个）",
			"hint", "hook 的契约是返回「基于入参派生的 ctx」")
	}
	// 图级 sink 换成图自己定下来的 ctx：条件边、节点级钩子、OnGraphEnd 里发的事件是图级事件（nodeID 为空），交给
	// OnNodeEvent 时带着 OnGraphStart 推的值（比如图的 span），跟节点体发的事件带着 OnNodeStart 推的值一样。曾经
	// 一直是 Run 入口建的那个，业务按自己推的 span 挂事件时挂到了调用方的 span 上（审计实测）。OnGraphStart 钩子
	// 自己发的那一刻图的 ctx 还没定下，拿到的是调它之前的。
	ctx = withEventSink(ctx, graphEventSink(ctx, cfg.hooks, cfg.graphNS))

	// **F4 修复**（Phase 5.1.a Step 5）：caller 传了 WithCheckpointer+WithSessionID
	// 但没传 WithResumeFrom 时，自动从 (sid, graphNS) 取 saved root cp——
	// 让 caller 用法对称（业务只需 cper+sid，框架自动 save/load）。
	//
	// 规则：
	//   - cfg.resumeFrom != nil → caller 显式提供，优先用（保留 caller 控制）
	//   - cfg.freshRun → caller 显式跳过 auto-load（escape hatch，audit Round 2 加）
	//   - cfg.resumeFrom == nil + !freshRun + cper + sid + saved cp Status=Pending
	//     → 自动 Resume
	//   - 否则 fresh run
	//   - `IsFreshRunCtx(ctx)` → **父声明了这是新任务**，嵌套层同样跳过（见 WithFreshRunCtx）
	//
	// noResumeReason 记「auto-load 为什么没给出可续的档」，供下方「答案无档」守卫按
	// 处境说话。空 = 没走到 auto-load（本次声明了新开）。
	noResumeReason := "本次声明了新开（WithFreshRun / 父层已声明），auto-load 整块跳过——" +
		"注意档很可能还在，只是本次不捡它"
	if cfg.resumeFrom == nil && !cfg.freshRun && !IsFreshRunCtx(ctx) &&
		cfg.checkpointer != nil && cfg.sessionID != "" {
		// opt-in 的 cp.load 事件（经 deliverCpEvent，钩子 panic 就地兜住、只记日志）
		loadStart := time.Now()
		rootCp, loadErr := cfg.checkpointer.LoadNS(ctx, cfg.sessionID, cfg.graphNS)
		// 比对基线（WithStaleWriteGuard）——**两个分支都要设**：
		//   载入到 cp → 记它那一代；载入到空槽 → 记"槽当时是空的"（后来变非空即冲突）
		// 漏掉 nil 分支的两个后果：① 会话第一轮完全裸奔（双方基线都空、谁也发现不了
		// 对方）；② 复用 cm 时上个 Run 的残留基线不被重置 → 串行零并发也误报 stale。
		if rootCp != nil {
			cm.setCpEpochBaseline(rootCp.Epoch)
		} else {
			cm.markCpSlotEmpty()
		}
		if cfg.emitCpEvent && cfg.hooks != nil {
			// audit follow-up: 计 WouldResume — cp 有可续内容 + GraphID 不冲突
			// 才真走 Resume. 业务订阅区分 "看到 cp / 真用 cp".
			// 判据与下方真闸门同一个谓词（HasResumableWork，按内容判）——两处漂移
			// 会让观测跟行为对不上。
			wouldResume := false
			if rootCp.HasResumableWork() && rootCp.BelongsToTask(getRunTaskID(ctx)) {
				if rootCp.GraphID == "" || rootCp.GraphID == g.ID() {
					wouldResume = true
				}
			}
			loadAction := &CpLoadAction{
				SessionID:   cfg.sessionID,
				NS:          cfg.graphNS,
				Found:       rootCp != nil,
				WouldResume: wouldResume,
				Source:      "auto_load",
				DurationMs:  time.Since(loadStart).Milliseconds(),
				Ok:          loadErr == nil,
			}
			if rootCp != nil {
				loadAction.Status = rootCp.Status.String()
			}
			if loadErr != nil {
				loadAction.Err = loadErr.Error()
			}
			deliverCpEvent(ctx, cfg, Event{
				Type:    CpEventType,
				Content: string(CpOperationLoad),
				Metadata: map[string]any{
					"action":   &CpAction{Type: CpOperationLoad, Load: loadAction},
					"graph_ns": cfg.graphNS,
				},
			})
		}
		if loadErr != nil {
			return fmt.Errorf("auto-load root cp: %w", loadErr)
		}
		// 记下「为什么没捡到」供下方守卫按处境说话——三种处境对调用方是三件事
		// （sid 找错 vs 槽里存了份跑完的快照 vs 这档是别的任务的），一句话套三种会
		// 把人支到错误的排查方向上（`commonResumePrep` 同款分档）。
		switch {
		case rootCp == nil:
			noResumeReason = "该 (sid, ns) 没有存档（sid 拼错 / 从未跑过 / 上次正常跑完已清）"
		case !rootCp.HasResumableWork():
			noResumeReason = fmt.Sprintf("存档在（status=%s）但没有可续的内容"+
				"（frontier 与挂起清单都是空——像是一份跑完了的快照被存进了续跑的槽）", rootCp.Status)
		case !rootCp.BelongsToTask(getRunTaskID(ctx)):
			noResumeReason = fmt.Sprintf("存档属于另一个任务（档上代号 %q，本次 %q）——"+
				"同 sid 复用时上个被放弃任务的残留", rootCp.TaskID, getRunTaskID(ctx))
		}
		// **身份维**（BelongsToTask）：内容判据回答「有没有可续的」，身份判据回答
		// 「是不是本任务的」——上个被放弃任务的残留对前者不可区分（见谓词 godoc）。
		if rootCp.HasResumableWork() && rootCp.BelongsToTask(getRunTaskID(ctx)) {
			// **捡不捡按内容判，不按档位判**（HasResumableWork godoc 有三条理由与实测）。
			// 失败档（frontier 还指着失败分支）从此也被捡起——Resume 只重跑失败成员、
			// 成功兄弟不重放；曾按 `Status == Pending` 挡它，代价是嵌套 Resume 时
			// 子图捡不起自己的失败子档 ⟹ fresh run ⟹ 已完成超步副作用重放。
			//
			// Phase 5.1.b ISSUE-50 P0 防串台守卫（fail-fast）：
			// cp.GraphID 与当前 g.ID() 不匹配时拒绝 auto-load——业务用同
			// (sid, ns) 跑两个不同 graph 时，silent 用旧 cp restore 会让
			// 新 graph frontier/channel 错位崩盘。撞上之后的两条出路和各自的
			// 代价见 ErrGraphIDConflict godoc（**其中一条会毁掉槽里那份存档**，
			// 所以这里不替调用方选）。
			//
			// 兼容旧 cp（cp.GraphID == ""）跳过校验——不破坏 Phase 5.1.a
			// Step 1-5 已存的 cp。
			if rootCp.GraphID != "" && rootCp.GraphID != g.ID() {
				// **正文只陈述事实，出路进 ErrGraphIDConflict 的 godoc**。
				//
				// 这里原先写着「应显式 WithFreshRun」——而那个动作会让本次跑成功后
				// 把槽里这份别人的存档连同整个 sid 子树一起清掉。**框架检测到「这儿有
				// 别人的东西」，然后教调用方执行一个毁掉它的动作**，照做的人不会知道
				// 自己刚删了什么。两条出路的代价差别很大且其中一条毁数据，该选哪条只有
				// 调用方知道，所以正文不选、只把定位信息给全。
				return fmt.Errorf("%w: session %q ns %q holds a checkpoint for graph %q, cannot auto-load into graph %q",
					ErrGraphIDConflict, cfg.sessionID, cfg.graphNS, rootCp.GraphID, g.ID())
			}
			cfg.resumeFrom = rootCp // Phase 5.1.b.4：直接用 *Checkpoint，无需桥接
			// **audit Round 10 Bug B1 修复**：auto-load 路径同显式 WithResumeFrom
			// 路径一样需要检查 unmatched answer IDs——R9 B4 fix 仅覆盖显式路径
			// （cfg.resumeFrom 在 :172 入口检查时仍 nil），auto-load 路径的 caller
			// 拼错 ID silent 通过陷入"为什么 Resume 后又中断"排查死循环。
			warnUnmatchedAnswerIDs(ctx, cfg.resumeFrom, cfg.resumeAnswers)
			// 输入摘要对不上：摘要即身份的是撞槽、拒绝（不清档），否则照样续跑、只告警（见
			// checkInputDigestOnAutoLoad）。只在 auto-load 这条路上比：「恢复否则新开」是引擎
			// 替调用方做的决定，显式 WithResumeFrom 是调用方自己选的档。
			if err := checkInputDigestOnAutoLoad(ctx, cfg, rootCp); err != nil {
				return err
			}
		}
	}

	// **答案无档 fail-fast**（原 R11-4 只 WARN，已升级）：cper+sid 给齐 +
	// WithResumeAnswers，但走到这儿 cfg.resumeFrom 仍 nil——auto-load 没找到可续的档
	// （sid 拼错 / 已清 / 从未跑过 / 档里没有可续内容），或本次声明了新开（fresh）
	// 却同时传了答案（两个意图矛盾）。
	//
	// 调用方攥着框架发的 InterruptID 在答题，而没有任何档在等这道题——继续跑的结局
	// 实测过：fresh run 重新提问一个**新 ID** 的同样问题，手里的答案永远配不上、
	// 静默蒸发，且 err 看着像正常中断（调用方陷入「为什么答完又问」死循环）。原 WARN
	// 的另一半危害是 ResumeContext 照注入 ⟹ 业务节点 GetResumeContext 假阳性误读。
	//
	// 哨兵复用 [ErrNoPendingCheckpoint]（语义完全一致：没有可续的档；`agent_v2` 九编排
	// Resume 的预检返的也是它，业务已有 errors.Is 分支）。agent_v2 Resume 正常流不会
	// 走到这儿（commonResumePrep 已先验档存在才进引擎），只有预检与执行之间档被并发
	// 清掉的窗口会命中——返同一个哨兵，调用方处置不变。
	//
	// 判据用 `len(...) > 0` 不用 `!= nil`：**空 map 等于没给**是全仓既有约定（9 编排的
	// `len(rcfg.userInputs) > 0` 都这么写）。按 `!= nil` 判会拒掉
	// `answers := map[...]{}; for ... { answers[id] = ... }` 然后无条件传的写法——
	// 一条答案都没给，「攥着 ID 在答题」这个前提根本不成立，错误正文自己会说
	// 「提供了 0 条答案」。
	if len(cfg.resumeAnswers) > 0 && cfg.resumeFrom == nil &&
		cfg.checkpointer != nil && cfg.sessionID != "" {
		ids := make([]string, 0, len(cfg.resumeAnswers))
		for id := range cfg.resumeAnswers {
			ids = append(ids, id)
		}
		return fmt.Errorf("WithResumeAnswers 提供了 %d 条答案（ID=%v），但 (sid=%q, ns=%q) 这次没有可续的档：%s: %w",
			len(cfg.resumeAnswers), ids, cfg.sessionID, cfg.graphNS, noResumeReason, ErrNoPendingCheckpoint)
	}

	// **任务代号，全仓唯一一处判定**：两条载入路径（显式 WithResumeFrom / auto-load）
	// 到这儿都已经把 `cfg.resumeFrom` 填好，所以「这次是续跑还是新开」在此有唯一答案。
	//
	// 续跑 → 从载入的那份 cp 读回（**不重铸**，否则中断 ID 跨 Resume 就漂了，而调用方
	// 正攥着它等人回答）；新开 → 铸一枚新的。
	//
	// ⚠️ 别在别处再判一次这件事——「同一个判断在两处各判各的」是本仓踩过四次的形状。
	if cfg.resumeFrom != nil {
		// 续跑：**原样沿用存档里的那枚，哪怕它是空的**。
		//
		// ⚠️ 空的时候绝不能铸新——那样每次 Resume 都换一枚 ⟹ 中断 ID 每次都变 ⟹ 调用方
		// 攥着的答案永远配不上 ⟹ **节点反复重问、永不收敛**（实测：连跑两轮，两次算出
		// 不同 ID）。空 TaskID 的来源是「升级期的旧存档 / 业务自研 backend 漏序列化 /
		// 业务自己 `&Checkpoint{}` 拼的」，那几种情况下退化成纯位置派生（= 引入这一维
		// 之前的行为，安全侧）才是对的。
		cm.setTaskID(cfg.resumeFrom.TaskID)
		// 输入摘要同样沿用存档里的：进度是在那份输入上建的，后面再续也要跟它比。
		cm.setInputDigest(cfg.resumeFrom.InputDigest)
	} else {
		// 新开：优先继承 ctx 里的顶层任务代号（嵌套子层与顶层同代号——同一任务全树
		// 一枚，重试第 2 次尝试才认得出「第 1 次尝试的进度是自己的」）；没有（本层
		// 就是顶层 / 直连 Run 的裸调用）才铸新。
		if tid := getRunTaskID(ctx); tid != "" {
			cm.setTaskID(tid)
		} else {
			cm.setTaskID(newEpoch())
		}
		cm.setInputDigest(cfg.inputDigest)
		// 复用 cm 跑新任务时，上一次运行已提交的上行信号不该在这次成功时再交一次
		cm.resetSignalsForFreshRun()
	}
	// push 给嵌套子层（续跑路径 push 的是读回的那枚——顶层 Resume 时子层拿到的代号
	// 与各自存档里的一致，中断 ID 的 TaskID 维跨 Resume 不漂）。
	ctx = withRunTaskID(ctx, cm.TaskID())

	// 计算初始 frontier：Resume 路径从 cp.CurrentFrontier 起；正常路径从 StartNode 起。
	var frontier []FrontierMember
	// judgeStart：入口边界要不要过断点。新开路径恒要；续跑时只有「恢复出来的这一超步还没开始执行、
	// 上次也不是停在它的断点上」才要（软取消 / ctx 取消停在边界、崩溃恢复）——见 breakpointStop。
	judgeStart := false
	if cfg.resumeFrom != nil {
		judgeStart = !cfg.resumeFrom.FrontierStarted && !hasBreakpointStop(cfg.resumeFrom.PendingInterrupts)
		// audit follow-up: 显式 WithResumeFrom 路径 fire cp.load event (Source=explicit)
		// 之前只 auto-load 路径 fire, 业务统计 "Resume 次数" 时漏算显式路径.
		if cfg.emitCpEvent && cfg.hooks != nil {
			// **判据必须跟真闸门同一个**：业务按这个字段统计「这次会不会真的续上」，
			// 而引擎捡不捡档读的是 `HasResumableWork()`（按内容判）。这里曾按
			// `Status == Pending` 算——那是档位 gate 撤销**之前**的判据，于是失败档
			// 上报 `WouldResume=false` 而引擎照样续跑，业务的统计跟实际行为对不上。
			wouldResume := cfg.resumeFrom.HasResumableWork() &&
				(cfg.resumeFrom.GraphID == "" || cfg.resumeFrom.GraphID == g.ID())
			deliverCpEvent(ctx, cfg, Event{
				Type:    CpEventType,
				Content: string(CpOperationLoad),
				Metadata: map[string]any{
					"action": &CpAction{Type: CpOperationLoad, Load: &CpLoadAction{
						SessionID:   cfg.sessionID,
						NS:          cfg.graphNS,
						Found:       true,
						Status:      cfg.resumeFrom.Status.String(),
						WouldResume: wouldResume,
						Source:      "explicit",
						// 显式 resumeFrom 本身不读 backend。**守卫开启时例外**：那条
						// 路径会多探一次槽状态（见上方 markCpSlotUnknown 段），那次
						// IO 不计在这里——业务按 cp event 统计 cp 层 IO 时需知道。
						DurationMs: 0,
						Ok:         true,
					}},
					"graph_ns": cfg.graphNS,
				},
			})
		}
		// **audit Round 8 Bug 4 修复**：WithResumeFrom 显式路径同 auto-load 路径
		// 一样应做 GraphID 冲突守卫——之前只 auto-load 检查，业务跨进程
		// MarshalCheckpoint→wire→UnmarshalCheckpoint→WithResumeFrom 时若误 mix
		// 不同 graph 的 cp，silent restore 让 frontier/channel 错位崩盘。
		//
		// 兼容旧 cp（cp.GraphID == ""）跳过校验——业务用裸 cm.Snapshot 拿到的 cp
		// 没 GraphID（Bug 5 已修后 Snapshot 可选填）；空 GraphID 仍允许通过保
		// 向后兼容。
		if cfg.resumeFrom.GraphID != "" && cfg.resumeFrom.GraphID != g.ID() {
			return fmt.Errorf("%w: WithResumeFrom cp.GraphID=%q vs g.ID()=%q: caller 跨 graph mix cp 拒绝静默 restore",
				ErrGraphIDConflict, cfg.resumeFrom.GraphID, g.ID())
		}
		// **续跑之前先问一句：这份档里有东西可跑吗**。
		//
		// 位置是承重的：**必须在 `cm.Restore` 之前**。Restore 会把存档里的挂起提问灌回
		// cm，之后再报错的话，Run 出口那条「没 frontier 也没挂起就不写档」的早退不再成立
		// ⟹ 照常 SaveNS ⟹ **把这份不自洽的原始现场覆盖掉**，覆盖出来的那份**自己就是
		// 矛盾档**（frontier 抹平、Status 仍 Pending）⟹ 此后每次续跑都被这同一道守卫
		// 拒死，而它在业务的待处理列表里看着一切正常。
		//
		// ⚠️ 但真正承重的是「**cm 是空的**」——cm 由调用方给，它先 Restore 过再进来时
		// 这个前提就没了（那是文档化支持的幂等用法）。完整说明见 ErrCheckpointNotResumable
		// godoc「现场保留（有一个前提）」那节。
		//
		// 判据是**自相矛盾**，不是「frontier 空」：这份档说有人在等回答
		// （`PendingInterrupts` 非空），却没有任何地方能跑到那个提问（frontier 空）。
		//
		//   - **frontier 空 + 没有挂起 = 跑完了**，那是合法的：对一份已完成的快照续跑
		//     返成功、不重跑，是幂等语义（有专门用例钉着）。**不能一并拒掉。**
		//   - **frontier 空 + 有挂起 = 不可能自然产生**。引擎存档时两者一起写，一个在
		//     另一个没了只能是外部原因——最隐蔽的那类是**业务自研 backend 漏序列化
		//     `CurrentFrontier`**：一次真 HITL 中断的档，挂起提问还在、Status 还是
		//     Pending，只有起跑地址没了。放它过去的表现是 Resume 返 nil、一个节点都没跑、
		//     用户的答案没送达、存档还被当成功清掉——四件事全静默。
		//   - **不看 Status**：`WithResumeFrom` 这条路根本不校验 Status，拿 `Resumable()`
		//     当门槛会被 Unset 的档绕过去。矛盾本身不需要 Status 来判定。
		if len(cfg.resumeFrom.CurrentFrontier) == 0 && len(cfg.resumeFrom.PendingInterrupts) > 0 {
			return fmt.Errorf("%w: sid=%q ns=%q graph=%q status=%s pending_interrupts=%d channels=%d",
				ErrCheckpointNotResumable, cfg.sessionID, cfg.graphNS, cfg.resumeFrom.GraphID,
				cfg.resumeFrom.Status, len(cfg.resumeFrom.PendingInterrupts), len(cfg.resumeFrom.Channels))
		}
		// Resume：自动调 cm.Restore（在 register 之后，让内部 channels 也能恢复）。
		// 这样 caller 不需手动 Restore——尤其当编排有私有 channels（如 Plan 的
		// __plan_current__ 等）时，caller 不知道要注册啥。
		// 幂等：caller 已 Restore 过的话，再调用同 snap 覆盖回相同值（无副作用）。
		if err := cm.Restore(cfg.resumeFrom); err != nil {
			return fmt.Errorf("auto-restore: %w", err)
		}
		// 从存档续跑：已提交的进度原样拿回、不重新交出流，所在那段执行取代的东西一个都不撤（见 stream_attempt.go）
		markStreamsUncertain(ctx)
		// import baseline 已在 OnGraphStart 之前摆好（F5 不变量：续跑读到父图**最新**值——
		// 那层不进 cp，每次进入都从父图重摆；Restore 只写 values，不会盖到它）。
		if len(cfg.resumeFrom.CurrentFrontier) == 0 {
			// 走到这儿说明**既没有 frontier 也没有挂起**（有挂起的矛盾档在 Restore 之前
			// 已被拒）——这份档就是「上次跑完了」，续跑它是幂等 no-op。
			bodyReached = true // 成功出口之二（共三处，见 Goexit 判据 defer）
			return nil
		}
		frontier = make([]FrontierMember, len(cfg.resumeFrom.CurrentFrontier))
		copy(frontier, cfg.resumeFrom.CurrentFrontier)
		// Resume 路径**无条件**清空旧 pending 列表 (跟 channel_manager.go::clearInterrupts
		// godoc "Resume 路径无条件调用" 对齐). 任何 Resume 调用 (含 WithResumeAnswers 传答案
		// + 纯 retry 无答案 + sub-agent 透传 ResumeContext) 都清 — 节点重跑时若拿到答案
		// 不再抛 InterruptError; 若仍漏答案重新抛新 Interrupt 走原流程.
		cm.clearInterrupts()
	} else {
		judgeStart = true
		// 新开路径：import baseline 已在 OnGraphStart 之前摆好；这里只剩种子写入。
		// **只有这条路径**才 apply 种子写入——续跑那半刻意不做（见 WithInitialWrites）。
		if len(cfg.initialWrites) > 0 {
			if err := cm.ApplyAll(cfg.initialWrites); err != nil {
				return fmt.Errorf("initial writes: %w", err)
			}
		}
		var isEnd bool
		var err error
		// 入口条件边跟图级钩子一样记在承载这张图的外层节点执行名下（顶层图没有，见 outerAcc）；
		// 否决时推进产出这张图输入的那个分域（见 producer）
		frontier, isEnd, err = computeStartFrontier(ctx, g, cm,
			scopedOf(outerAcc, graphHookScope("entry", graphPhaseNS)), producer)
		if err != nil {
			return fmt.Errorf("initial out-edges from StartNode: %w", err)
		}
		if isEnd {
			// StartNode → EndNode 极端图，直接返成功
			bodyReached = true // 成功出口之三（共三处，见 Goexit 判据 defer）
			return nil
		}
	}

	// 中断保护：每帧推进时记录 frontier 到 cm，让 Snapshot 能拿到中断点
	cm.updateFrontier(frontier)

	// **F-B fix**：补 apply 上次没能提交的 SendDirective payload——必须在
	// 跑任何节点**之前**（frontier 里就有等着读这些 payload 的 target）。fresh run
	// 恒空 → no-op 零开销。
	//
	// **位置在 updateFrontier 之后**——但理由已经不是原来那条了。
	//
	// 原注释写「Restore 不恢复 frontierAtExit，所以这之间任何 early return 都会让
	// Snapshot 拿到空 CurrentFrontier 覆盖掉原 cp」。**那句已失效**：`Restore` 现在
	// 恢复它（serialization.go 里 `cm.frontierAtExit = cp.CurrentFrontier` 那段，
	// 修复记录就写在它上面——「曾是唯一漏的一个」「修在 Restore 而不是修那个 early
	// return，让现在和将来的所有窗口一次性免疫」）。
	//
	// 留这句是因为**那条 stale 前提正是这段代码的摆放理由**，照它搬块的人会踩坑：
	// 现在这个位置是可选的（免疫已在 Restore 里做掉），不是承重的。
	//
	// 补 apply 失败不吞：业务 reducer 若是确定性坏的，重跑必然同样坏——**报错**
	// 比让 target 读零值静默出错诚实（pendingSendWrites 未清 → 存回 cp，业务修完
	// reducer 再 Resume 即可续跑）。
	//
	// ⚠️ **先读后清，不是先取走再放回**：`take` 是破坏性的，而「放回去」那步挂在
	// `err != nil` 上——`runtime.Goexit` 掐在 ApplyAll 里时它跑不到（既不返回也不产生
	// err）⟹ payload 从 cm 里消失、又没 apply 成 ⟹ 存档里 `pendingSendWrites=0`，
	// Resume 时 target 作普通节点执行、**静默读到零值**。那正是 F-B 当初消灭掉的形态
	// 在 Goexit 路径复活。
	//
	// ⚠️⚠️ **这里故意「保源头」，跟 SendDirective 那处的判据是反的**——别照着改。
	// 那处踩过的坑是「源头与下家同时留在存档 ⟹ 双放」，所以那边源头一转移完就清。
	// 这里能保源头而不双放，靠的是两条**必须写出来**的条件：
	//
	//  1. 下家（`cm.values`）的写是**原子**的——panic 与 Goexit 都整批回滚，
	//     所以「源头还在 + 下家没写成」不构成两份；
	//  2. 「写落地」与「清掉待补记录」必须是**一件事**——两者之间只要还有一行能被
	//     掐断的代码，存档里就会同时有「已落地」和「还欠着」两份，Resume 补一次多算
	//     一次（实测每 Resume 一次翻一倍）。这条现在收在
	//     `ApplyPendingSendsAndClear` 一个临界区里。⚠️ 那是**局部**保证不是结构性
	//     保证——刀插进那个方法内部照样撕（实测）；真正掐不进来靠的是那段里没有任何
	//     可能跑业务代码的调用，见它的 godoc。
	//
	// ⚠️ **同款约束曾经还有四处，现已全部收进临界区**（`commitSuperstepPartial` /
	// `commitSuperstepAdvance`，见它们的 godoc）。留个案底，因为形状会复发：
	//
	//   1. 提交方法返回 → 成功兄弟记账：掐进去，成功兄弟的出边
	//      与 sends 整批丢失，那个后继节点一次都没跑而 `err` 全程 nil；
	//   2. pre-arm 那两句之间（`updateFrontier` ↔ `setPendingSendWrites`）：target 被
	//      调度了却读到零值；
	//   3. 待办清空 → 那处 pre-arm（源头已清、下家还只在局部变量里，
	//      两头都不在 cm 上）；
	//   4. 写入落地 → `updateFrontier(nil)`／`(nextFrontier)`：全员成功路径的双计窗口，
	//      横跨一大段。
	//
	// 四处都只有框架代码够得着（业务 reducer 跑在规划阶段、那时 cm 一字节没动，实测
	// 等价于「什么都没发生」）。所以它一直是「下一个改这段代码的人往里插一行」的风险，
	// 而不是业务能触发的 bug——`TestCommitRegion_CallsNothingBusinessReachable` 现在
	// 把两个提交方法都纳入管辖，往里加任何可能跑业务代码的调用会当场红。
	if err := cm.ApplyPendingSendsAndClear(); err != nil {
		return fmt.Errorf("re-apply pending send writes: %w", err)
	}

	// **收件人密钥一个任务一把**（见 signal.go「收件人密钥」一节）：嵌套图沿用上层传下来的，顶层续跑用
	// 上面 Restore 恢复的、新开时铸；推给本图的节点，它们里面跑的嵌套图沿用这一把。位置在 Restore 之后：
	// 更早的话，顶层续跑会先铸一把新的推下去、随后被 Restore 盖掉，嵌套图拿到的就跟本图用的不是同一把。
	ctx = withTaskRecipientKey(ctx, cm.adoptRecipientKey(taskRecipientKeyFrom(ctx)))

	// durability=async：进超步循环前启单 worker 后台 cp writer。出口 join defer 必须在
	// cleanup defer（成功 DeleteSubtree / 失败兜底 SaveNS，line ~325）**之前**跑——靠 Go
	// defer LIFO（本 defer 后注册先执行）——drain 所有 pending 写后才 cleanup，否则
	// DeleteSubtree 可能先于某 pending 写落盘 → 残留 cp。后台写失败经 join 浮现并 abort。
	var asyncWriter *asyncCpWriter
	if cfg.durability == DurabilityAsync && cfg.checkpointer != nil && cfg.sessionID != "" {
		asyncWriter = newAsyncCpWriter(ctx, cfg, cm)
		defer func() {
			// runErr==nil 守卫：主流程已 error/interrupt 时主错优先（更可操作；终态 cp 由
			// cleanup defer 同步写同 key 覆盖、其成败已浮现，丢的只是会被覆盖的中途写）。
			// 主流程成功收尾时才让后台写失败浮现——"compute 成功但 durability 写失败"也 abort 不静默。
			//
			// ⚠️ **这个守卫看不到「主体 panic」**：本 defer 注册得比出口那个「主体 panic 先转
			// runErr」的 recover 晚 ⟹ LIFO 下比它先跑，那时 runErr 还是 nil。于是三重合取
			// （async 档 + 后台写失败 + 主体 panic）下后台写错会抢到"主错"位、panic 排第二。
			// 两条都进 errors.Join，`errors.Is` 不受影响，只是 Error() 首行把次要原因摆前面。
			//
			// 没改结构：本 defer 在函数体更靠后处注册，无法排到那个 recover 之前——真要治得
			// 把顺序反过来，代价大于这点显示失真。
			if joinErr := asyncWriter.closeAndJoin(); joinErr != nil && runErr == nil {
				runErr = fmt.Errorf("async durability writer: %w", joinErr)
			}
		}()
	}

	// 入口那一批也过断点（见 judgeStart）。停在这里的断点没法续：嵌在别的节点里跑、又没有 checkpointer
	// 的图，续跑时从头再跑、又停在同一个断点（见 breakpointStop 的 unresumable）。
	unresumableBreakpoint := enteredFromNodeBody && (cfg.checkpointer == nil || cfg.sessionID == "")
	if judgeStart {
		if err := breakpointStop(cm, cfg.graphNS, breakBefore, breakAfter, frontier, nil, unresumableBreakpoint); err != nil {
			return err
		}
	}

	visits := make(map[NodeID]int)
	for {
		// **关键不变量 #2**：done 已在上一次 iteration 末尾判过——这里只查 ctx
		if err := ctx.Err(); err != nil {
			return err
		}

		// 软取消：业务请求优雅停。在超步边界检查（ctx.Err 硬停之后）——上一超步已善终、
		// frontierAtExit 已是当前 frontier → 返回触发 Run-defer SaveNS 存档（cp.CurrentFrontier
		// 指向本超步），业务可 Resume 续跑。当前若没下一超步要跑（isEnd 在上轮已 return），不会到这。
		if cfg.softCancel != nil {
			if triggered, reason := cfg.softCancel.Triggered(); triggered {
				return fmt.Errorf("%w: %s", ErrSoftCanceled, reason)
			}
		}

		if len(frontier) == 0 {
			return errors.New("frontier is empty but isEnd was false")
		}

		// fan-out 宽度防御——**与下面的访问次数是两个正交维度**：这里拦宽度（一个超步
		// 里的并发成员数），下面拦深度（同一节点被反复访问的轮数）。
		//
		// 少了这道，节点在自己的 NodeFunc 里 SendFrom 自己 N(≥2) 次时宽度逐超步相乘，
		// 业务终止条件写错就是指数爆炸；而深度每超步只 +1，等 maxVisit 触顶时进程早已
		// OOM（实测 N=2/maxVisit=6 执行 63 次，默认 maxVisit=100 是 2^100）。
		if len(frontier) > g.maxWidth {
			// **也 `%w ErrRunAborted`**：不重试（`IsRetryable` 已排除它）+ Run 出口清 cp 子树
			// + 各吞错点不吞（[MustPropagate]）。
			//
			// ⚠️ 理由同单写入者冲突那条，**不是「重跑必然一样」**：扇出几路取决于运行时数据
			// （节点按 LLM 给的清单 SendFrom），换批数据就不触发。真正的理由是**这个爆炸会随
			// 存档反复复现**——存进档的 `CurrentFrontier` 就是那个超限的 frontier 本身，而续跑
			// 闸门按内容判（`HasResumableWork`，不读 Status）⟹ auto-load 捡起 ⟹ **节点一次没跑
			// 就在同一道闸上再爆**，那个 (sid, ns) 槽从此是死的，且每次重爆都往持久后端写一遍
			// 那份（默认 maxWidth=10000 时上万条）frontier（审计实测：Run2 的 ran=0、Width 与
			// Run1 逐字相同；逃生舱 `WithFreshRun` 存在但要靠调用方知道，而 abort 买的正是
			// 「不用调用方知道」）。清掉档之后下一次 Run 就是 fresh run——数据变了它可能本来就
			// 不该失败，这正是想要的。
			return fmt.Errorf("superstep frontier width %d exceeds max %d (fan-out 宽度失控——"+
				"节点自己 SendFrom 自己时宽度逐超步相乘，检查业务的终止条件；确需更大并发用 "+
				"WithMaxFrontierWidth): %w: %w",
				len(frontier), g.maxWidth, &MaxWidthError{Width: len(frontier)}, ErrRunAborted)
		}

		// 节点访问次数防御——返 typed `*MaxIterationsError` 让业务用 errors.As 解析。
		//
		// **按 distinct NodeID 计数，一个超步至多 +1**（ISSUE-96 侧面 4）：maxVisit 限的
		// 是**迭代深度**（同一节点被回边反复访问多少轮），不是 fan-out **宽度**（一个超步
		// 里同一 NodeID 有多少并发实例）。按 frontier 条目计数会让 SendFrom 同 Target 投
		// N 次直接把计数推 N 格——业务把并发数调到 maxVisit 以上，整张图在第一个超步就假
		// 触顶、worker 一次都没跑。宽度本就该由业务的 SendFrom 决定，不归 maxVisit 管。
		for _, id := range distinctNodeIDs(frontier) {
			visits[id]++
			if visits[id] > g.maxVisit {
				return fmt.Errorf("node %q visited %d times (max %d): %w",
					id, visits[id], g.maxVisit, &MaxIterationsError{Iterations: visits[id]})
			}
		}

		// 执行当前 frontier（per-branch delta + hook 集成）。先记下「这一超步开始执行了」：之后在这个超步里
		// 中断 / 失败留下的档，续跑时不再判它的 before 断点（见 judgeStart）
		cm.markFrontierStarted()
		res := executeFrontier(ctx, g, frontier, cm, cfg.hooks)

		// **单写入者冲突：在任何提交之前拦下。**
		//
		// 用 `%w ErrRunAborted`：不重试（`IsRetryable` 已排除它）+ Run 出口清 cp 子树。
		//
		// ⚠️ **理由不是「重跑必然一样」**——独立审计实测证伪过两次：(a) 写入是不是发生
		// 取决于运行时数据（条件写），同一张图换批数据就 `err == nil`；(b) 冲突方之一
		// 第一次恰好失败时守卫压根看不到它（失败分支的 writes 在检测前就丢了），
		// Resume 只重跑那一个 ⟹ 同一个接线错误整个逃逸。
		//
		// 真正的理由是**这个矛盾会随存档反复复现**：留 Pending 档 ⟹ auto-load 捡起 ⟹
		// 再撞同一个矛盾，而单槽覆盖式下它还占着位置。加上赢家由 frontier 索引序决定、
		// 跟业务意图无关，所以任何一次「侥幸没撞上」都不代表修好了。
		//
		// ⚠️ **代价要认**：清子树会把该 (sid,ns) 下**已经问过人、人也答了**的挂起档
		// 一并清掉，之前超步的副作用在重跑时会重放。撞上它就是得改图重跑，没有原地续。
		//
		// 位置在 recordInterrupt 之前：既然整个 Run 终局，挂起清单没有存在的意义——
		// 存了反而让业务看到一份「答了也没用」的提问。
		if res.SingleWriterConflictErr != nil {
			return res.SingleWriterConflictErr
		}
		// 上行信号同样是单写入者（见 signal.go「单值格子」）：同一超步两个成员都对同一个收件人做了
		// 决定 ⟹ SignalConflictError（类型本身带着 ErrRunAborted），跟上面那条同一个位置、同一个理由——
		// 并发的成员之间没有先后，留档续跑只会让同一个矛盾再来一次。
		//
		// 部分失败续跑时，本次成功的成员与先前记在一边的成功兄弟同属一个超步，在这里再合并一次——
		// 合并结果跟没停过时一样，不随停没停过、谁先成功变。
		if res.SignalConflictErr == nil {
			if done := cm.completedSignalsSnapshot(); len(done) > 0 {
				res.Signals, res.SignalConflictErr = joinSignalBranches("同一超步的成员", true, []signalBranch{
					{label: "（部分失败前已成功的）", items: done},
					{label: "（续跑时成功的）", items: res.Signals},
				})
			}
		}
		if res.SignalConflictErr != nil {
			return res.SignalConflictErr
		}

		// **audit Round 10 Bug D-N1 修复**：先 record AllInterrupts 再尝试 ApplyAll——
		// R9 D1 让 ApplyAll panic 转 error 返，但 graph_loop 旧顺序"ApplyAll → 失败
		// return → 永远走不到 recordInterrupt"让 fan-out 部分中断 + 兄弟 reducer
		// panic 时中断 query 永久丢失（caller 看 InterruptError 但 cm.PendingInterrupts
		// 空 → WithResumeAnswers 无 ID 可填 → 永久 deadlock）。
		//
		// 顺序调整：失败/中断分支的 query 先 record（即使本批 ApplyAll 失败 atomic
		// 回滚 cm.values，pending interrupt 仍要让 caller 看到 — 否则下次 Resume
		// 没法配答案）。
		var dupErr error // 撞 ID 事实：记在这儿，到最终 return 处才替换
		// **pre-arm / post-disarm**：frontier 的窄化意图先记下来，等 SuccessWrites 的
		// ApplyAll **真的成了**再落地。nil = 不窄化（保持整超步，失败时的安全态）。
		// 理由见下方赋值处——补偿逻辑在 Goexit 下跑不到，所以安全态必须是默认。
		var narrowFrontierTo []FrontierMember
		if res.FirstErr != nil {
			// **事实先记，再谈报告**：本超步只要有一个分支值得留 Pending 就置位，
			// 跟下面 exitErr() 最终挑中谁无关。放在最早的失败分叉点，覆盖本块
			// 所有下游 return（dupErr / applyErr / exitErr 三条）。
			if res.StayPending {
				stayPendingFact = true
			}
			// **audit Round 6 Bug 3 修复**：收集 fan-out 所有并发分支抛的
			// InterruptError query 让 cm 全部 recordInterrupt——之前只记 first
			// 导致 N-1 条 query 永久丢失。
			//
			// **ISSUE-81 fix**：用 AllQueries 协议——branch 内可能本身是 toolsFn
			// fanout 抛多 query (MergeInterrupts 聚合到 ie.Queries)，需要 record
			// 全部不只第一条。
			batch := make([]*AskQuery, 0, len(res.AllInterrupts))
			for _, ie := range res.AllInterrupts {
				batch = append(batch, ie.AllQueries()...)
			}
			// ⚠️ **撞 ID 只记下事实，不在这儿 return**：下面还有 SuccessWrites 的
			// ApplyAll 和 appendCompletedSiblingsLocked——从这儿跳出去会让成功兄弟的 writes
			// 没 commit、身份和 sends 没记，留下半应用状态。在最终 return 处替换错误。
			dupErr = cm.recordInterruptBatch(batch)
			// ⚠️ **窄化推迟到下面 ApplyAll 成功之后**（pre-arm / post-disarm）。
			//
			// 这里曾经就地 `updateFrontier(res.FailedBranches)`，靠 ApplyAll 的 error
			// 分支再 `updateFrontier(frontier)` 补偿回来。**`runtime.Goexit` 下那条补偿
			// 天生跑不到**——它既不返回也不产生 err，于是「成功兄弟的 writes 被回滚」
			// 与「frontier 里没有它」同时成立 ⟹ **那些写入永久丢失**。
			//
			// 实测（同一 fixture 只换掐断方式）：panic 下 Resume 后 log=[good bad]、
			// Goexit 下 log=[bad]。而且这是**补上 ApplyAll 的 Goexit 回滚才暴露的**——
			// 在那之前，半应用的写入侥幸留在存档里、它的生产者又不重跑，两个错正好抵消。
			// 修一个就露出另一个，典型的「改 A 带出 B」。
			//
			// 所以次序反过来：**失败时的安全态（整超步重跑）是默认，成功了才收窄**。
			narrowFrontierTo = res.FailedBranches
		}

		// **提交计划先算完，再一次落地。**
		//
		// 全员成功时「下一个 frontier 是什么、SendFrom 的 payload 是哪些」全都是纯计算
		// （出边已由各成员在自己的执行里算完，assembleFrontier 只是去重 + 判 End）。
		// 把它们提到提交之前，`commitSuperstepAdvance` 就能在**一个临界区**里做完
		// 写入落地 / 代际推进 / 答案兑付 / 待办清空 / frontier 前移 / payload 折叠。
		//
		// 原先这些散落在提交之后的 150 行里，中间任何一处被掐断都会留下半应用的存档
		// （独立审计逐点注入实测：成功兄弟的出边整批丢失、target 读到零值、写入双计）。
		var (
			nextFrontier []FrontierMember
			isEnd        bool
			dirWrites    []ChannelWrite
			// completedEarlier：部分失败续跑时，这个超步先前已成功的成员（after 断点要算上它们）
			completedEarlier []FrontierMember
		)
		if res.FirstErr == nil {
			// PA-H1：读取上次部分失败超步的成功成员及其**当时就算好的出边目标**。
			//
			// 它们的出边**不在这里重算**——出边是「那个成员成功」的一部分，在它成功的那一刻
			// 就随它的执行算完了（算不出来的话它当时根本不算成功）。重算的话读到的是**它跑完
			// 之后**的状态（含兄弟与重跑成员的产出），跟它自己当时看到的不是一份东西。
			// audit² H1：peek 非破坏性读——本超步后续失败时记录原样留 cm 进 cp。
			var deferredSends []SendDirective
			var completedTargets []NodeID
			completedEarlier, deferredSends, completedTargets = cm.peekCompletedSiblings()

			nextFrontier, isEnd = assembleFrontier(append(append([]NodeID{}, res.Targets...), completedTargets...))

			// PA-H1：重放成功成员的 SendDirectives——**前置**合并（audit² L1：deferred
			// 属更早成员，无中断执行时其 sends 先于本超步新 sends 进流，保 Append 等
			// 顺序敏感 reducer 与无中断路径顺序一致）。
			if len(deferredSends) > 0 {
				res.SendDirectives = append(deferredSends, res.SendDirectives...)
			}

			// **SendFrom 动态投递**：合并 directives 的 Targets 进 next frontier，
			// Payload 通过 apply 注入 channel state（在下一超步入口可见）。
			// 重复 Target 不去重——caller 想 N 个并发 worker 时按 N 次调用。
			//
			// **audit Bug A 修复**：仅当至少有一个 valid Target 时才设 isEnd=false——
			// 之前循环内每条 directive 都设（即使 Target 被 filter 掉），导致 caller
			// 错调 SendFrom([Target=EndNode]) 时 nextFrontier 空 + isEnd=false →
			// "frontier is empty but isEnd was false" 错。
			// payloadOwners: channel 名 → 第一条写它的 directive 下标。
			var payloadOwners map[string]int
			for di, d := range res.SendDirectives {
				if d.Target == "" || d.Target == EndNode {
					continue // 防御：不投递空/终止节点
				}
				nextFrontier = append(nextFrontier, FrontierMember{NodeID: d.Target, InstanceID: d.instanceID})
				// **payload 灌的是共享 channel，不是「这条 directive 的私有入参」。**
				//
				// N 条 directive 的 payload 在下一超步开始前一次性归并进同一个格子，
				// 所以它们写同一个单写入者 channel 时**只剩最后一条**，N 个 target
				// 全看到同一个值。这正是本包 godoc 一度教的 map-reduce 写法
				// （每条 `Write(workerInputCh, t)` + 一句「如 LastValue 单实例」），
				// 实测三个 worker 全看到第三个 task 而 err == nil。
				//
				// 跟成员间那道检测是两件事：那道按「谁写的」分组，而 N 条 directive
				// 常常出自**同一个**节点（map-reduce 就是），按成员分组一条都抓不到。
				for _, w := range d.Payload {
					if w == nil {
						continue
					}
					name := w.channelName()
					// **成员写 vs payload 写**：本超步某成员写了 name，而这条指令的 payload 也写它
					// ——payload 走第二次 apply，会静默盖掉成员的写（LastValue 下）。发指令的
					// 成员自己既写又发 payload 是它自己的显式意图，不算。
					if m, wrote := res.WriteOwners[name]; wrote && res.SingleWriterConflictErr == nil &&
						m != (FrontierMember{NodeID: d.owner, InstanceID: d.ownerInstanceID}) &&
						cm.isSingleWriterChannel(name) {
						res.SingleWriterConflictErr = fmt.Errorf(
							"channel %q 是单写入者（LastValue），本超步成员 %s 写了它，第 %d 条 SendFrom "+
								"指令（%s → %s）的 payload 也写它。payload 在成员写入之后归并，会静默盖掉成员"+
								"的写、赢家由 apply 顺序决定。要么别让 payload 碰这个 channel，要么换能合并的"+
								"channel（Append / Sum / 自定义 reducer）: %w",
							name, formatFrontierMember(m), di, formatFrontierMember(FrontierMember{NodeID: d.owner, InstanceID: d.ownerInstanceID}),
							d.Target, ErrRunAborted)
					}
					prev, seen := payloadOwners[name]
					if !seen {
						if payloadOwners == nil {
							payloadOwners = map[string]int{}
						}
						payloadOwners[name] = di
						continue
					}
					if prev == di || res.SingleWriterConflictErr != nil {
						continue // 同一条 directive 内连写两次是它自己的显式意图
					}
					if !cm.isSingleWriterChannel(name) {
						continue
					}
					res.SingleWriterConflictErr = fmt.Errorf(
						"channel %q 是单写入者（LastValue），却被同一超步的两条 SendFrom "+
							"指令的 payload 写了（第 %d 条 → %s、第 %d 条 → %s）。"+
							"payload 灌的是**共享** channel 不是每条指令的私有入参，"+
							"N 条一起归并进同一个格子后只剩最后一条，N 个目标会全看到同一个值。"+
							"想给每个目标不同的入参：给它们**不同的 target**（同 target 扇出的实例"+
							"拿不到自己是第几个，没法各取各的；见 SendDirective godoc）: %w",
						name, prev, res.SendDirectives[prev].Target, di, d.Target, ErrRunAborted)
				}
				dirWrites = append(dirWrites, d.Payload...)
				isEnd = false // 有 valid directive 就不是 end（continue 保证只 valid 生效）
			}
			if res.SingleWriterConflictErr != nil {
				return res.SingleWriterConflictErr
			}
		}

		{
			var commitErr error
			if res.FirstErr != nil {
				commitErr = cm.commitSuperstepPartial(res.SuccessWrites, narrowFrontierTo,
					res.SucceededBranches, res.ConsumedAnswers, res.Signals, res.SendDirectives, res.Targets)
			} else {
				commitErr = cm.commitSuperstepAdvance(res.SuccessWrites,
					res.SucceededBranches, res.ConsumedAnswers, res.Signals, nextFrontier, dirWrites)
			}
			if commitErr != nil {
				// **audit Round 11 Bug R11-1+R11-3 修复**：ApplyAll panic atomic
				// 回滚（R9 D1）让成功分支 writes 也丢失 + 类型链销毁让 caller
				// errors.As(err, &InterruptError) 失败。修复：
				//
				// 1. 若分支已有错（含 InterruptError），把它并进来作主 error 让
				//    errors.As 仍能命中 InterruptError 类型
				// 2. ApplyAll panic 让所有 SuccessWrites 一条都没提交——成功分支
				//    的状态被株连，必须把整 frontier 替换为本超步全节点（含成功+失败）
				//    让 Resume 重跑全本超步，恢复 v_B 等丢失的成功 writes
				//
				// **audit Round 12 Bug R12-2 修复**：用 errors.Join 替代 fmt.Errorf
				// %w + %v 嵌入——secondary error（reducer panic 业务 typed err）
				// 用 %v 让 errors.As 链断，业务无法 unwrap 拿原 typed err 做差异化处理。
				// errors.Join (Go 1.20+) 双链都保 unwrap：errors.As(&InterruptError)
				// 与 errors.As(&BizErr) 都能命中。
				//
				// ⚠️ **dirWrites 那次 apply 失败时不能走这条**——那时成功成员的写入
				// 已经落地、frontier 已前移，整超步重跑会双计。所以那条路在
				// commitSuperstepAdvance 里保留现场，由下面单独返回。
				var sendErr *sendPayloadApplyErr
				if errors.As(commitErr, &sendErr) {
					return fmt.Errorf("apply send directives: %w", commitErr)
				}
				cm.updateFrontier(frontier) // 整 frontier 重跑（含成功分支）
				applyErr := fmt.Errorf("apply writes: %w", commitErr)
				if branchErr := res.exitErr(); branchErr != nil {
					return errors.Join(branchErr, applyErr)
				}
				return applyErr
			}
		}

		// **提交点：本超步哪些成员的成功已成终局，上面那一次调用里一并定死**。
		//
		// 一个成员算成功的三件事——NodeFunc 跑通、出边算得出来（两者都在 executeFrontier
		// 里，失败的已进 FailedBranches）、写入提交成功——到这一行全齐了。代际（visit）
		// 就在那一刻推进，早一步晚一步都会开出「半成功」的窗：
		//
		//   - **早了**（曾经在节点函数一返回就 ++）：reducer panic 会整超步回滚重跑，
		//     而 visit 已经变了 ⟹ 重跑时中断身份漂移，用户答过的问题被重新问。
		//   - **晚了**（曾经放在「全员成功」那段）：部分失败超步里成功的那个成员，写入
		//     已提交、且进 CompletedFrontier 后**永不重跑** ⟹ 它的代际再没机会推进。
		//     后果实测：该节点被回边重访时的**新提问**算出与上一轮**完全相同**的
		//     InterruptID，业务累积传的旧答案把它静默满足——用户根本没被问，`err == nil`。

		// 失败/中断 return（已 record 完 interrupts + 更新 frontier + 记完成兄弟）
		if res.FirstErr != nil {
			if dupErr != nil {
				// 撞 ID 是业务自身矛盾、重跑必然一样 ⟹ 终局。挂起列表清掉：留着半份
				// （撞点之前那些）会让 LoadAllInSession 的全景查询看到一份不完整的清单。
				cm.clearInterrupts()
				return dupErr
			}
			// 中断优先 + 其余取第一个，判据与理由见 exitErr。
			//
			// 中断这一半 cp 侧早就记全了（recordInterrupt 遍历 AllInterrupts），返回值那条路
			// 若少给，业务照 AllQueries() 建答案 map 就会**系统性少答**：cp 里挂着 N 个中断、
			// 业务只看得见 1 个，答完 Resume 又撞上没答的那些。框架知道有 3 个却只告诉调用方
			// 1 个——两边数字不一致本身就是个坑。
			return res.exitErr()
		}

		if isEnd {
			bodyReached = true // 成功出口之一（共三处：本处 + 空档幂等 no-op + Start→End 极端图）
			return nil
		}
		frontier = nextFrontier

		// 新算出的边界先过断点：after 看刚整体提交的这个超步（含部分失败时先成功的兄弟），before 看下一
		// 超步要执行的。**排在 durability 中途写档与下一轮循环顶的软取消 / ctx 检查之前**——那些停止先落档
		// 的话，续跑恢复出这个边界时不再判 after，after 断点就被越过去；before 会重判，但要先多停一次。
		if err := breakpointStop(cm, cfg.graphNS, breakBefore, breakAfter, frontier,
			append(completedEarlier, res.SucceededBranches...), unresumableBreakpoint); err != nil {
			return err
		}

		// durability：每超步边界持久化。本超步 writes 已 apply + 下一 frontier 已算出，
		// 写的 cp = "崩这里则从 nextFrontier Resume"。Exit 档不写（走 Run-defer）；
		// isEnd 成功路径(上面)不写（会被 cleanup 清）。
		if cfg.durability != DurabilityExit && cfg.checkpointer != nil && cfg.sessionID != "" {
			switch cfg.durability {
			case DurabilitySync:
				// 同步阻塞写——写完才进下一超步。
				if err := saveStepCheckpoint(ctx, g, cm, cfg); err != nil {
					return err // 写失败 abort（cp 写失败即终止 run）
				}
			case DurabilityAsync:
				// 同步 snapshot 捕获一致态 → 交后台 worker 写 I/O，不阻塞下一超步。
				cp, err := snapshotStepCheckpoint(g, cm)
				if err != nil {
					return err // snapshot 失败 abort（与 sync 一致）
				}
				if err := asyncWriter.submit(cp); err != nil {
					return err // 之前的后台写失败浮现 → abort
				}
			}
		}
	}
}

// frontierResult 是 executeFrontier 的多返回值——支持 per-branch delta 语义。
//
// **Phase 1.2.a per-branch delta**：
// fan-out 部分失败时，成功分支 writes 应保留（commit 到 cm），失败分支信息
// 单独记录让 Resume 只重跑失败分支。
//
// 四编排 review LOW-3 修正：我们成功 writes **立即 apply 进 cm**（不攒到超步末
// 统一 apply），Resume 时只执行失败/无 writes 的分支。当前缺 mid-superstep
// 持久化（进程 crash 时本超步已成功分支的 writes 会丢，这条 gap 见
// checkpoint-storage.md）。
type frontierResult struct {
	// SuccessWrites 成功分支收集的 writes（按 frontier 索引序合并，非完成顺序；
	// 见 state-model.md 决策 6）
	//
	// **合并次序现在是确定的**（2026-08-27 修）：frontier 里静态边 / cond 边成员的次序
	// 来自**出边的声明序**（`Graph.adj` 是按 `AddEdge` 序的 slice），`assembleFrontier` 去重时
	// 保留首次出现序；`SendFrom` 派生的成员按 directive 声明序 append 在后。两段都确定。
	//
	// ⚠️ 此前这里灌进 map 再 range，实测同一张图 300 次出现 3 种合并顺序、分布 73/15/12。
	// 那段文字曾写着「故 Append/Sum 这类顺序敏感的 reducer 在静态 fan-out 下结果不可复现」
	// ——**现在可复现了**。但 channel.go 的 reducer 约定（要求满足交换律）不因此放松：
	// 确定的是**框架这一侧**的合并次序，业务仍不该把语义压在它上面（比如靠顺序做 last-win）。
	SuccessWrites []ChannelWrite
	// FailedBranches 失败分支 ID（含 InterruptError + 业务错 + hook 错）
	// 仅 Resume 时用——下次 Run 从这些分支重跑（不重跑成功分支）
	FailedBranches []FrontierMember
	// SingleWriterConflictErr 是「两个成员在同一超步写了同一个单写入者 channel」的
	// 检测结果（`LastValue` 家族）。非 nil 时**整超步不提交**——这是图接线上的矛盾，
	// 重跑必然一样，故 wrap ErrRunAborted。检测点在这儿而不是归并处：归并那层拿到的
	// writes 已经扁平化，看不见谁写的，而错误正文没有「是哪两个成员」就没法排查。
	//
	// **覆盖边界**（如实写，别假装全覆盖）：
	//   - 只看**跨成员**。同一个成员在自己的 []ChannelWrite 里连写两次是有序且确定的，
	//     那是它的显式意图。
	//   - 宽度 1 的超步整个不检测（executeFrontier 有 fast path）。那里只有一个成员，
	//     跨成员冲突不可能发生，跳过是对的——但写测试时**必须用宽度 ≥2 的 fixture**，
	//     否则测的是快路径、守卫改没改都绿（实测栽过）。
	//   - `SendDirective.Payload` 现在**也在检测范围内**，两条：directive 之间（按 directive
	//     分组——N 条常出自同一个节点，按成员分组一条都抓不到）、以及成员写 vs payload 写
	//     （payload 走第二次 apply，会静默盖掉成员的写）。三段检测的豁免键各不相同，
	//     因为它们守的不是同一件事：前者守「并发赢家不确定」，后两者守「payload 不是
	//     per-directive 私有入参」——`Channel.single` 这一位被两种语义共用。
	SingleWriterConflictErr error

	// WriteOwners 是本超步**每个被成员写过的 channel → 第一个写它的成员**（只在扇出路径填，
	// 宽度 1 的 fast path 为 nil）。给后面 payload 那道检测用：成员写与某条 SendFrom
	// 指令的 payload 写同一个单写入者 channel 时，payload 在第二次 apply 里**静默盖掉**
	// 成员的写——那也是「两个写入者、赢家由 apply 顺序决定」。
	WriteOwners map[string]FrontierMember

	// ConsumedAnswers 是本超步全体成功成员**兑付掉的答案位**（见 answer_consumption.go）。
	// 与 Targets 同档：只收完全成功的成员，失败成员读过的答案不算兑付（它会重跑、
	// 必须还能拿到同一个答案）。
	ConsumedAnswers []string

	// Signals 是本超步全体成功成员做出的上行信号（见 signal.go），按单写入者合并过（一个收件人只能
	// 有一个成员做决定）。与 ConsumedAnswers 同档：只收完全成功的成员——失败成员做出的随它的这次
	// 执行作废。部分失败续跑时，主循环再把它跟先前记在一边的成功兄弟的合并一次（同一超步）。
	Signals []SignalRecord

	// SignalConflictErr 是「同一超步的两个成员都对同一个收件人做了决定」（[*SignalConflictError]，
	// 类型本身带着 [ErrRunAborted]，值相同也算）。跟 SingleWriterConflictErr 同一档：主循环在任何提交、任何
	// 提问记录之前返回它，Run 出口清档。
	SignalConflictErr error

	// Targets 是本超步全体成功成员算出来的出边目标（含 EndNode，未去重）。
	//
	// **它是「成功」的一部分**：一个成员算成功，当且仅当它的 NodeFunc 成功**且**它的
	// 出边算得出来。所以这里只会有完全成功的成员的贡献——cond 报错的成员已经进了
	// FailedBranches，它的 writes / directives / targets 一并丢弃。
	//
	// 去重与 isEnd 判定交给 assembleFrontier（纯机械那一半）。
	Targets []NodeID

	// SucceededBranches 成功分支 ID（PA-H1，仅部分失败时消费）——记入
	// cm.completedFrontier 让 Resume 重跑失败分支成功后，这些成员参与同超步
	// 出边计算（writes 已 commit，不重跑 NodeFunc）
	SucceededBranches []FrontierMember
	// FirstErr 第一个分支错（business / interrupt / infrastructure），按 frontier
	// 索引序取（不是完成顺序）。
	//
	// **它是"一个可判定的根因"，不是"本超步失败的全貌"**——要拿全部失败分支各自的
	// 原因走观测通道：`OnNodeEnd(ctx, nodeID, chs, nodeErr)` 对**每个**失败分支各
	// fire 一次，带分支身份和它自己的错。理由见 exitErr。
	//
	// ⚠️ **报哪个错现在是确定的**（2026-08-27 修）：SendFrom 派生的成员按 directive 声明序排，
	// 静态边 / cond 边成员按**出边的声明序**（`assembleFrontier` 去重时保留首次出现序，不再
	// range map）。此前实测同一张图 300 次出现 3 种结果、分布 73/15/12——那个偏斜比五五开更
	// 难查：5 次的 CI 大概率一次都撞不上、线上按流量撞。
	// 与 SuccessWrites 的合并顺序同源（见其说明）。
	FirstErr error
	// ExhaustedErr 是本超步里第一个带「重试用完」标记（RetriesExhaustedError）的分支错：交出 FirstErr 时据此带上标记，
	// 外层不再重试（见 pickFanoutError）。
	ExhaustedErr error
	// AllInterrupts 本超步所有抛 InterruptError 的分支 query 列表（按 frontier
	// 顺序）。HITL 路径用——多 worker 并发抛中断时全部 recordInterrupt 让
	// caller 拿全 N 条 PendingInterrupt 配答。**audit Round 6 Bug 3 修复**：
	// 之前只记 FirstInterrupt 导致 fan-out 多 ask_user 时 N-1 条 query 永久丢失。
	//
	// AbortErr 是本超步里**第一个终局失败**（`ErrRunAborted`）分支的错。
	//
	// **它跟 `exitErr()` 返什么是两件事**——这正是本字段存在的理由。`exitErr` 是
	// 「报告」：调用方看到哪一个。而「终局失败」要驱动的是**后果**（清不清 cp 子树 /
	// 能不能重试），后果不该取决于谁赢了那场仲裁。
	//
	// 两条实测伤害都源于把后果骑在仲裁值上：
	//   - 同超步「终局失败 + 普通业务错」，返谁靠 map 迭代序 ⟹ 实测 40 次里
	//     34 次清档、6 次留 `failed` 死档且被判可重试。**同一份记录、两种后果。**
	//   - 同超步「终局失败 + 合法中断」，中断优先 ⟹ 终局失败这一轮**不可见**，
	//     用户被问了一个注定答不成的问题；等他答完，下一轮才撞上 abort，
	//     purge 把**他刚给的答案连同兄弟已完成的工作**一起清掉。
	//
	// 有了这个字段，两种形态都变成：这一轮就清、这一轮就说「终局失败」，不再取决于
	// 兄弟分支恰好是什么。
	AbortErr error

	// StayPending 报「本超步**有没有任何一个分支**的退出方式值得把存档留在可恢复那一档」。
	//
	// 跟 AbortErr 同一个道理、同一个形状：它是**事实**，而 `exitErr()` 是**报告**。
	// 存档标 Pending 还是 Failed 属于后果，不该取决于哪个分支恰好赢了那场仲裁。
	//
	// 实测的伤害：一分支以控制流信号退出（软取消 / ctx 超时 / `CondFailRerunFrom` /
	// 陈旧写回…）、另一分支是普通业务错时，返谁按 frontier 索引序，而静态边成员的
	// 次序来自对 map 的遍历 ⟹ **同一份记录、同一次超时，40 次里 34 次存档 Pending
	// （可续、历史留住用户原话）、6 次 Failed（永久不可续、历史里那句话彻底没有）**。
	//
	// 有了这个字段，「留不留 Pending」只看有没有分支值得留，不看谁赢仲裁。
	StayPending bool

	// NotResumableErr 报「本超步有分支撞上了**自相矛盾的存档**」（`ErrCheckpointNotResumable`）。
	//
	// 跟 AbortErr / StayPending 同一个形状：**事实**，不是报告。
	//
	// 为什么必须是独立字段而不是复用 AbortErr：那个字段的语义捆绑了 **Run 出口主动清 cp
	// 子树**，而这份坏档**要留着排查**（`ErrCheckpointNotResumable` 的 godoc 明写这一点）。
	// 借它会把「不重试」和「毁现场」一起买下来。
	//
	// 实测的伤害：同超步「分支甲的存档坏了 + 分支乙正常中断」⟹ 仲裁按「中断优先」返中断，
	// 存档问题**这一轮完全不可见**（`errors.Is(NotResumable)=false`、`IsInterrupt=true`）。
	// 而 a2a server 正是从**返回值**算这两个判据的：吞掉之后 `keepPending=true` ⟹ 服务端
	// 把任务挂成 `input-required`、把问题发给远端、等一个**注定再撞同一份坏档**的续发。
	//
	NotResumableErr error

	// NotPersistedErr 报「本超步有分支里的嵌套执行出口写档失败」（`ErrCheckpointNotPersisted`）。
	//
	// 同 NotResumableErr：事实，仲裁时排在中断之前——那个分支的档没写成，用户答完兄弟的提问续跑，
	// 那个分支也续不上。多数时候它同时带着 ErrRunAborted（AbortErr 先接走）；槽属于别人（陈旧写回 /
	// 另一张图的档）时不带，靠这个字段保证它不被兄弟的中断盖掉。曾经陈旧写回跟中断在同一分支里双
	// `%w`、靠多错误树的 OR 保住；现在中断不在它的链上，那个机制不成立。
	NotPersistedErr error

	AllInterrupts []*InterruptError
	// interruptRaws 是各中断分支返出来的**原始 error**，与 AllInterrupts 按下标一一
	// 对应：后者是 `IsInterrupt` 从链上抠出来的裸 `*InterruptError`，前者是没动过的
	// 整条链。
	//
	// 必须两个都留：中断错常常并列挂着**第二个 sentinel**（子 agent 撞上陈旧写回时
	// 抛的是 `errors.Join(interrupt, ErrCheckpointStale)`），而 a2a/server、agui、
	// observability 三处都把 stale 判在 IsInterrupt 之前——只返裸 ie 会让它们退化成
	// 正常 HITL：留 entry 等续发、导出 status=blocked 的绿 span，而那个中断的 cp 根本
	// 没落盘，用户被反复问同一个问题。
	//
	// **存的是列表而不只是第一条**：多分支同时中断时 `MergeInterrupts` 新建一个
	// `InterruptError` 装合并后的 query 列表，而 `*InterruptError` 没有 `Unwrap`——
	// 只返它等于把 N 条链全扔了。守住一条扔掉其余，跟只守单中断那档没有本质区别。
	interruptRaws []error
	// SendDirectives 成功分支通过 SendFrom 投递的动态 fan-out 指令
	// 失败分支的 directives 自然丢弃（节点失败 writes 不 commit）。
	// 发出者身份在 SendDirective.owner（runNodeWithHooks 成功路径 stamp）——
	// 不再用并列 slice：并列 slice 在 deferred 前置合并（下方 :768）后即永久
	// 索引错位，只是恰好之后没人再用（issue-95#1 顺带根治）。
	SendDirectives []SendDirective
}

// exitErr 定本超步失败时返给调用方的那**一个** error。
//
// # 终局失败 > 坏存档 > 档没写成 > 中断 > 第一个业务错
//
//	有分支终局失败 → 那条分支的原始 error（`ErrRunAborted`，见 AbortErr）
//	有分支存档坏了 → 那条分支的原始 error（见 NotResumableErr）
//	有分支档没写成 → 那条分支的原始 error（见 NotPersistedErr）
//	1 个分支中断   → 那条分支的**原始 error**（不抠不重建，见 interruptRaws）
//	多个分支中断   → errors.Join(合并成一个带 N 条 query 的 InterruptError, 各分支原始链...)
//	其余           → FirstErr
//
// # 为什么终局失败排在中断前面
//
// **不是「换一种偏袒」，是它严格支配**：终局失败意味着这次 Run 已经定了（业务违反协议、
// 重跑必然一样），**不存在「用户答了就有用」的未来**。把中断报上去的代价实测过——
// 同超步 A 合法提问、B 空 ID 违规时：调用方看到的是 A 的问题、去问了人、人答了；
// 下一轮才撞上 B 的终局失败，而出口的 purge 把**用户刚给的答案连同 A 已完成的工作**
// 一起清掉。一次白问 + 一次销毁，换来的信息量为零。
//
// 排前面之后同一形态变成：这一轮就报终局失败、这一轮就清，人不会被问一个注定答不成的
// 问题。**「谁赢仲裁」这件事本身仍不该决定后果**——那是 AbortErr 这个字段的职责，
// 它把「本超步有没有分支终局失败」记成独立事实，不随仲裁结果漂。
//
// **中断和业务错的待遇不对称是有意的**，两者回答的问题不同：中断是「等你回答的问题
// 清单」，少一条就少一次交互——所以必须收全；业务错是「为什么失败」，调用方需要的是
// 一个**可判定的根因**，不是一堆并列事实。
//
// 中断优先这条尤其关键：改之前是「≥2 个中断才合并，否则返 FirstErr」，于是 1 个分支
// 中断 + 2 个分支业务错时返的是业务错，`IsInterrupt` 为 false——调用方按「Run 失败了」
// 处理不会去 Resume，而 cp 里正挂着那个等回答的问题，永远没人答。同一个「部分失败」
// 的形状，中断个数不同丢的东西相反（1 个丢中断、2 个丢业务错），那不是取舍是漏想。
//
// # 为什么不把所有错聚合成多错误树
//
// 试过，会引入一个比它解决的更隐蔽的问题：`errors.Join` 的 `Unwrap() []error` 让
// `errors.Is/As` 遍历**所有**分支，判断语义从「这个错是不是 X」静默变成「**有没有
// 哪个分支**是 X」。实测：3 个分支里 2 个是可重试的业务错、1 个 wrap 了自己下游调用的
// `context.DeadlineExceeded`（全局 ctx 完全健康），聚合后 `IsControlFlowSignal` 返
// true → `IsRetryable` 返 false → 那 2 个本该重试的分支再也不会重试。不聚合时这个
// 误判只有 1/3 概率撞上，聚合让它必现。
//
// 编译器和现有测试都不会提醒这类翻转——调用方那些 `errors.Is(err, ErrXxx)` 全都还
// 编译得过、也还返 true，只是意思变了。
//
// # 中断优先会盖掉非中断兄弟分支上的 sentinel
//
// 「中断优先」是**有意的不对称**，代价是：某个分支成功但它的存档陈旧（返裸
// `ErrCheckpointStale`）、同超步又有分支中断时，那条 stale 透不出来——返回值只有一个
// 位置，中断占了。
//
// 这不是「业务错就该让位」那套理由的延伸：`ErrCheckpointStale` 是控制流信号、下游三处
// 专门判它，跟「A 因为配额超了」不是一类东西。选择让位是因为**两者要求的动作相同**——
// 都是「别当成正常完成，去 Resume」，而中断额外带着调用方必须看到的问题清单。真要两个
// 都拿到，读 `OnNodeEnd`。
//
// # 那「每个分支各自为什么失败」怎么拿
//
// 走观测通道：`OnNodeEnd(ctx, nodeID, chs, nodeErr)` 对**每个**失败分支各 fire 一次，
// 带分支身份和它自己的错。返回值负责「给一个能判定的根因」，观测通道负责「给全貌」——
// 两件事分两条路，而不是把全貌塞进返回值再让每个调用方去拆。
//
// 被返回值省略的那些分支也不会永久静默：它们都在 FailedBranches 里，Resume 时会重跑。
func (r frontierResult) exitErr() error {
	// 终局失败严格支配「等回答」——理由见上。这一档是本函数独有的（扇出调用方没有
	// cp 子树要清），故留在这儿；`AbortErr` 与 `NotResumableErr` 之后的那两档（中断 / 第一个业务错）跟框架里别处的扇出仲裁**共用同一份判据**，
	// 见 PickFanoutError。
	if r.AbortErr != nil {
		return r.AbortErr
	}
	// **存档自相矛盾排在中断之前**，判据跟 AbortErr 同一条：「回答这个问题有没有可能有用」。
	//
	// 有中断的兄弟时报中断看着更「友好」，实际更糟——那个提问**注定答不成**（存档里没有可
	// 执行的东西），用户答完只会再撞同一份坏档。消费方（a2a server）从返回值算
	// `keepPending`，报中断就把任务挂成 input-required、把问题发给远端，等一个不可能成功
	// 的续发。报坏档至少让人当场知道该修什么。
	//
	// ⚠️ 跟 AbortErr 的区别在**后果**不在报告：那个会让 Run 出口主动清 cp 子树，而这份坏档
	// **要留着排查**。所以它有自己的事实位（NotResumableErr），只借用这里的顺序。
	if r.NotResumableErr != nil {
		return r.NotResumableErr
	}
	// **档没写成同样排在中断之前**：判据同上，见 NotPersistedErr。
	if r.NotPersistedErr != nil {
		return r.NotPersistedErr
	}
	return pickFanoutError(r.interruptRaws, r.FirstErr, r.ExhaustedErr)
}

// executeFrontier 执行当前帧所有节点，收集 per-branch 结果。
//
// 调度策略：
//   - len(frontier)==1：fast path 顺序执行，省 goroutine 开销
//   - len(frontier)>1：并发执行，每个节点独立 goroutine，wg.Wait 后汇总
//
// **hook 集成**（Phase 1.1）：
//   - OnNodeStart 在 NodeFunc 调用前；返 error 视为基础设施错跳过执行
//   - OnNodeEnd 在 NodeFunc 返回后（含 OnNodeStart 失败路径——配对承诺）
//   - OnNodeEvent 通过 ctx 注入 EventSink，节点内 Send 实时投递
//
// **per-branch delta**（Phase 1.2.a）：
//   - 业务错 best-effort 不 cancel 兄弟（对齐现有 superstep.go ISSUE-46）
//   - 成功分支 writes 收集到 SuccessWrites
//   - 失败分支 ID 收集到 FailedBranches（让 Resume 只重跑这些）
//   - first error 仍返出（caller 决定是否中断 Run）
//
// recordWriteOwners 把一个成员的写登记进「channel 名 → 第一个写它的成员」——**首写者胜**。
//
// 两条执行路径共用（扇出的收集循环、宽度 1 的 fast path）：这张表喂的是下游「成员写 vs
// payload 写」那道检测，而那道检测的对手可能是上个超步存档、本超步重放的 payload，
// 跟本超步有几个成员无关。分成两处写过一次，fast path 那半忘了填（实测漏检）。
func recordWriteOwners(owners map[string]FrontierMember, m FrontierMember, ws []ChannelWrite) map[string]FrontierMember {
	for _, w := range ws {
		if w == nil {
			continue
		}
		name := w.channelName()
		if _, seen := owners[name]; seen {
			continue
		}
		if owners == nil {
			owners = map[string]FrontierMember{}
		}
		owners[name] = m
	}
	return owners
}

func executeFrontier(ctx context.Context, g *Graph, frontier []FrontierMember, cm *ChannelManager, hooks OrchestrationHooks) frontierResult {
	chs := cm.Channels()

	// fast path: 单节点顺序
	if len(frontier) == 1 {
		id := frontier[0].NodeID
		fn, ok := g.nodes[id]
		if !ok {
			return frontierResult{
				FailedBranches: []FrontierMember{frontier[0]},
				FirstErr:       fmt.Errorf("node %q not registered", id),
			}
		}
		ws, directives, consumed, signals, err := runNodeWithHooks(ctx, id, fn, g.nodeComponents[id], g.nodePolicies[id], chs, hooks, cm, frontier[0].InstanceID)
		var targets []NodeID
		if err == nil {
			// 出边算在**提交之前**、且算在这个成员名下——失败即本成员失败。
			var condSignals []SignalRecord
			targets, condSignals, err = evalMemberOutEdges(ctx, g, cm, frontier[0], ws)
			// cond 在节点体之后算，它做出的是后做的
			for _, sg := range condSignals {
				signals = putSignal(signals, sg)
			}
			if err == nil {
				err = validateSendTargets(g, id, directives)
			}
		}
		// 成员以「它名下嵌套图的入口被否决」失败：推进产出那份输入的分域的代际
		applyNestedEntryRejections(ctx, cm, frontier[0], err)
		if err != nil {
			res := frontierResult{
				FailedBranches: []FrontierMember{frontier[0]},
				FirstErr:       err,
			}
			if errors.Is(err, ErrRunAborted) {
				res.AbortErr = err
			}
			if errors.Is(err, ErrCheckpointNotResumable) {
				res.NotResumableErr = err
			}
			if errors.Is(err, ErrCheckpointNotPersisted) {
				res.NotPersistedErr = err
			}
			if shouldStayPending(err) {
				res.StayPending = true
			}
			if ie, isInt := IsInterrupt(err); isInt {
				res.AllInterrupts = []*InterruptError{ie}
				res.interruptRaws = []error{err}
			}
			return res
		}
		return frontierResult{
			SuccessWrites:     ws,
			SendDirectives:    directives,
			SucceededBranches: []FrontierMember{frontier[0]},
			Targets:           targets,
			ConsumedAnswers:   consumed,
			Signals:           signals,
			// **宽度 1 也要填**：跨成员冲突在这里确实不可能，但 `WriteOwners` 还喂着
			// 下游「成员写 vs payload 写」那道检测——那道检测的对手是**上个超步**存进
			// DeferredSends、本超步重放的 payload，跟本超步有几个成员无关。
			// 漏填的后果实测过：兄弟恰好失败过一次 ⟹ 续跑时 frontier 缩成 1 ⟹ 走这条
			// fast path ⟹ 检测失效、成员的写被 payload 静默盖掉且 err == nil。
			// 守卫的结论取决于「兄弟是不是恰好失败过」，正是它要消灭的那种侥幸。
			WriteOwners: recordWriteOwners(nil, frontier[0], ws),
		}
	}

	// fan-out: 多节点并发执行。同 NodeID 的多个并发实例靠各自的 InstanceID 区分
	// （ISSUE-96）——原先用"在 frontier 里的位置"当序号，部分失败时 frontier 缩成
	// 失败子集、位置重排，身份跟着漂。
	type branchOutput struct {
		member     FrontierMember
		writes     []ChannelWrite
		directives []SendDirective
		targets    []NodeID
		consumed   []string
		signals    []SignalRecord
		err        error
	}
	results := make([]branchOutput, len(frontier))
	var wg sync.WaitGroup
	for i, m := range frontier {
		fn, ok := g.nodes[m.NodeID]
		if !ok {
			results[i] = branchOutput{member: m, err: fmt.Errorf("node %q not registered", m.NodeID)}
			continue
		}
		// 同 NodeID 多次出现按出现顺序分 branch idx 0,1,2...（首次=0）
		i, m, fn := i, m, fn
		wg.Add(1)
		go func() {
			defer wg.Done()
			// **本 goroutine 顶上必须自带 recover**：`runNodeWithHooks` 那道随它 return 就
			// 结束，而下面的 `evalOutEdges` 在它**之外**（`safeCondRun` 只裹 cond 的函数体，
			// 框架对业务 error 的链式 unwrap 是裸的）。跨 goroutine 的 panic 谁都接不到，
			// 不自己兜就 **crash 整进程**——业务给一个 `Unwrap()` 会 panic 的 error 即可。
			//
			// 见 panic_recovery.go。
			//
			// **`normalReturn` 守的是第三条出路**：goroutine 既没正常返回、也没 panic
			// ——`runtime.Goexit`。它**会**跑 defer，但 `recover()` 返 nil，所以上面那支
			// 接不住；而 `results[i]` 是预分配的零值 ⟹ `err == nil` ⟹ **被当成功**，
			// 且 `member` 也是零值 ⟹ 一个 `NodeID:""` 的幽灵成员进 `CompletedFrontier`
			//（那个集合的语义是「已成功、永不重跑」）⟹ 这条分支连同下游跨 Run + Resume
			// **永久消失**，最后存档被当成功清掉，全程 `err == nil`。实测过。
			//
			// **不罕见**：`zap` 的 godoc 主动推荐在 `zap.WithFatalHook` 里调 `runtime.Goexit`
			//（「Fatal 别杀进程、只结束当前 goroutine」是服务端常见诉求）；`os/user` 在
			// Windows 上有一条（`user.Current()` 走得到）；testify 的 `require.*` 全系同理。
			//
			// **不转发 Goexit**：实测在主 goroutine 上转发会
			// `fatal error: no goroutines (main called runtime.Goexit) - deadlock!`。
			// 标准库 testing.tRunner（errNilPanicOrGoexit）与 iter.Pull（seqDone）
			// 各有一份同款 flag，也都是**转成错误**而不是转发。
			//
			// **`member: m` 两支都要带**——身份跟结果是同一次赋值写的（`:1639`），零值
			// 连身份都没有，那正是幽灵成员的根因。
			normalReturn := false
			defer func() {
				if r := recover(); r != nil {
					pe := panicerr.New(r)
					perr := panicerr.Wrap(fmt.Sprintf("node %q out-edge panic recovered", m.NodeID), pe)
					panicerr.Log(ctx, slog.LevelError, "compose_v2: 出边计算 panic，已转成该成员失败", pe, panicerr.KeyError, perr.Error())
					results[i] = branchOutput{member: m, err: perr}
					return // panic 已归档，别让下面那支拿 Goexit 的说法盖掉它
				}
				if !normalReturn {
					// **不 wrap `ErrRunAborted`**：那个哨兵的判据是「重跑必然一样」，而这里
					// 确定的只是 `runtime.Goexit` 这个**机制**——触发它的是业务条件
					//（`logger.Fatal` 可能因为下游挂了而走到），框架判不出它是瞬时还是确定性。
					// 按框架一贯的原则「分不出瞬时故障还是确定性错误，那是业务的知识」，
					// 这里返普通业务错：留 Failed 档 ⟹ 业务可 Resume 重试**这一个**失败分支
					//（成功兄弟不重放）。确定性的话重跑还是同样报错，偶发的话重跑就过了；
					// 清掉档则两种都不给选。
					results[i] = branchOutput{member: m, err: fmt.Errorf(
						"node %q (ns=%q instance=%q) 的 goroutine 异常终止：既没正常返回也没 panic"+
							"（runtime.Goexit——典型来源是 zap 的 WithFatalHook、testify 的 require.*、t.FailNow）",
						m.NodeID, GetGraphNS(ctx), m.InstanceID)}
				}
			}()
			ws, directives, consumed, signals, err := runNodeWithHooks(ctx, m.NodeID, fn, g.nodeComponents[m.NodeID], g.nodePolicies[m.NodeID], chs, hooks, cm, m.InstanceID)
			var targets []NodeID
			if err == nil {
				// **每个实例各算各的出边**，视图只含它自己的写入——兄弟实例的写入看不见。
				// 这是「路由归成员」的直接后果，也让 cond 失败能精确归因到某一个实例
				//（先前 cond 读合并后的 channel、按 NodeID 只算一次，无从归因是哪个实例）。
				var condSignals []SignalRecord
				targets, condSignals, err = evalMemberOutEdges(ctx, g, cm, m, ws)
				for _, sg := range condSignals {
					signals = putSignal(signals, sg)
				}
				if err == nil {
					err = validateSendTargets(g, m.NodeID, directives)
				}
			}
			applyNestedEntryRejections(ctx, cm, m, err)
			results[i] = branchOutput{member: m, writes: ws, directives: directives, targets: targets, consumed: consumed, signals: signals, err: err}
			normalReturn = true
		}()
	}
	wg.Wait()

	// 收集 per-branch delta：
	// - 成功分支 → SuccessWrites + SendDirectives
	// - 失败分支 → FailedBranches（writes/directives 自然丢弃）
	// - 第一个 error 单独记录（caller 决定怎么响应）
	// - **audit Round 6 Bug 3 修复**：所有 InterruptError 收集到 AllInterrupts
	//   让 caller 用 cm.recordInterrupt 注册全部 query；之前只 FirstInterrupt
	//   → fan-out 多 worker 同时抛 ask_user 时 N-1 条 query 永久丢失，Resume
	//   只能配第 1 条答案，其它 worker 重跑时再次抛中断（但 caller 看不到 question）
	var out frontierResult
	// writeOwners: channel 名 → 第一个写它的成员。懒建——多数超步宽度为 1，不会用到。
	var writeOwners map[string]FrontierMember
	// sigBranches：成功成员各自做出的信号，按 frontier 顺序，循环之后一并合并。
	var sigBranches []signalBranch
	for _, r := range results {
		if r.err != nil {
			out.FailedBranches = append(out.FailedBranches, r.member)
			if out.FirstErr == nil {
				out.FirstErr = r.err
			}
			// 「下层已经重试用完」独立记账：交出的错带不带这个标记不随分支先后变（见 pickFanoutError）
			if out.ExhaustedErr == nil && retriesExhausted(r.err) {
				out.ExhaustedErr = r.err
			}
			// 「终局失败」独立记账——它驱动后果，不参与「报告」的仲裁
			if out.AbortErr == nil && errors.Is(r.err, ErrRunAborted) {
				out.AbortErr = r.err
			}
			// 「存档自相矛盾」同款独立记账——它是**事实**，不该被兄弟的中断吞掉。
			// 消费方（a2a server）从返回值算 keepPending，吞掉之后会把任务挂成
			// input-required 等一个注定再撞同一份坏档的续发。
			if out.NotResumableErr == nil && errors.Is(r.err, ErrCheckpointNotResumable) {
				out.NotResumableErr = r.err
			}
			// 「档没写成」同款：槽属于别人时它不带 ErrRunAborted，不独立记账就会被兄弟的中断盖掉。
			if out.NotPersistedErr == nil && errors.Is(r.err, ErrCheckpointNotPersisted) {
				out.NotPersistedErr = r.err
			}
			// 「值得留 Pending」同款独立记账：**任何一个**分支值得留就留
			if shouldStayPending(r.err) {
				out.StayPending = true
			}
			if ie, isInt := IsInterrupt(r.err); isInt {
				out.AllInterrupts = append(out.AllInterrupts, ie)
				out.interruptRaws = append(out.interruptRaws, r.err)
			}
			continue
		}
		// **单写入者冲突**：`LastValue` 的 reducer 是 `return next`，两个成员在同一
		// 超步写同一个 channel 时静默丢一个，赢家由 frontier 索引序决定。签名正是
		// 「N=1 正确、N≥2 静默错、err == nil」——所以在这儿拦下，趁还知道是谁写的。
		//
		// 只看**跨成员**：同一个成员在自己的 []ChannelWrite 里连写两次是有序且确定的，
		// 那是它自己的显式意图，不算冲突。
		for _, w := range r.writes {
			if w == nil {
				continue
			}
			name := w.channelName()
			prev, seen := writeOwners[name]
			if !seen {
				continue // 登记统一在循环之后做（见 recordWriteOwners）
			}
			if prev == r.member || out.SingleWriterConflictErr != nil {
				// 同一成员连写两次是它自己的显式意图；已报过一条就不再覆盖——
				// 正文里点出**头两个**冲突者足够定位，全列出来会让错误正文随并发
				// 宽度膨胀。修掉头两个之后第三个会在下一次跑时接着报。
				continue
			}
			if !cm.isSingleWriterChannel(name) {
				continue
			}
			out.SingleWriterConflictErr = fmt.Errorf(
				"channel %q 是单写入者（LastValue），却被同一超步的两个成员写了：%s 与 %s。"+
					"LastValue 的 reducer 是「后写的赢」，并发写时会静默丢一个——赢家由 frontier "+
					"索引序决定（确定，但跟你的意图无关：声明在后的那个赢）。"+
					"出路二选一：改用能合并的 channel（Append / Sum / 自定义 reducer），"+
					"或确认这两个成员本就不该同时写它。真想要「谁最后写谁赢、我不在乎是谁」，"+
					"用 Custom(name, func(_, next T) T { return next }, zero) 显式声明: %w",
				name, formatFrontierMember(prev), formatFrontierMember(r.member), ErrRunAborted)
		}
		// 登记本成员的写——**唯一一处**。放在检测之后：同一成员连写两次时第二条看到
		// `seen == false`、走不到冲突分支，与「同成员豁免」等价。
		writeOwners = recordWriteOwners(writeOwners, r.member, r.writes)

		out.SuccessWrites = append(out.SuccessWrites, r.writes...)
		out.SendDirectives = append(out.SendDirectives, r.directives...)
		out.SucceededBranches = append(out.SucceededBranches, r.member)
		out.Targets = append(out.Targets, r.targets...)
		out.ConsumedAnswers = append(out.ConsumedAnswers, r.consumed...)
		if len(r.signals) > 0 {
			sigBranches = append(sigBranches, signalBranch{label: formatFrontierMember(r.member), items: r.signals})
		}
	}
	out.WriteOwners = writeOwners
	// 同一超步的成员是**并发**的：一个收件人只能有一个成员做决定，值相同也不行（见 signal.go「单值
	// 格子」）。曾经按 frontier 顺序拼起来、由消费方取最后一个——赢家由声明顺序决定，部分失败续跑
	// 之后还会翻转（成功兄弟先并入、续跑的后并入；实测同一张图停没停过结果不同）。
	out.Signals, out.SignalConflictErr = joinSignalBranches("同一超步的成员", true, sigBranches)
	return out
}

// validateInterrupt 校验业务抛出的中断请求是否**形状合法**——所有产生中断的入口共用这一份。
//
// ⚠️ **别在第二处另写一遍**：这套校验原先只长在节点返回那条路上，于是 `OnNodeStart` 抛的
// 中断整条绕过去，空 ID 的挂起照样落进存档（实测两分支各抛一次 ⟹ 两条挂起 ID 都是空、
// 彼此不可区分）。同一份判断散在两处，漏掉一处就是一个静默入口。
//
// 下面每一条都是**业务违反了中断协议**（不是运行时故障），所以一律 `%w` 上 `ErrRunAborted`：
// ⚠️ **别在注释里数「共几条」**——这个数在三处各写过一遍、三个数字互不相同（实测）。
// 重跑必然同样违规，给重试 / Resume 机会只会把副作用再放一遍（实测配了 retry 的编排下
// 一次这样的 bug 扣了 4 次款）。同文件 cond 抛中断的协议违规走的是同一条路。
//
// ⚠️ 返的错**不 `%w` 原 InterruptError**——否则 `IsInterrupt` 仍为 true 但它已不会被
// record，调用方拿不到任何 ID 可配，Resume 死锁。
//
// ⚠️ 同超步里若有兄弟分支抛了合法中断，「中断优先」会让这条违规错**不出现在返回值里**，
// 调用方看到一次正常 HITL、答完下一轮才撞上它。要当场看见得读 `OnNodeEnd`。
func validateInterrupt(ctx context.Context, id NodeID, err error, ie *InterruptError) error {
	where := func() string {
		if ns := GetGraphNS(ctx); ns != "" {
			return fmt.Sprintf("node %q (ns %q)", id, ns)
		}
		return fmt.Sprintf("node %q", id)
	}
	// **别的任务的挂起不收**：节点里用 DetachExecution（agent_v2.DetachTask）开的独立运行挂起时，
	// 它的中断属于那个任务——答案只能在顶层用那个任务自己的 sid 续跑时送进去。这个任务收下它的话，
	// 续跑时答案进的是本节点的 ctx、被脱离剥掉，独立运行按新任务从头再跑、换一个 ID 再问一遍，
	// 永不收敛（实测：每次续跑换一个 ID，孤儿存档逐轮累积，全程没有报错）。
	//
	// 原样返回时链上有那层包装；取出里面的 *InterruptError 返回、再包一层、合并进别的中断时包装没了，
	// 认对象上的记号（见 InterruptError.detached）。
	if sid, ok := foreignInterrupt(err, ie); ok {
		return fmt.Errorf("%w: %s: 返回了一个独立运行（DetachTask / DetachExecution 之后开的，sid=%q）挂起时"+
			"抛出的中断——它属于那个任务，这个任务答不了它。那个任务的挂起在顶层用它自己的 sid 续"+
			"（agent.Resume(ctx, sid, ...)）；节点里别把它的中断返回（原样、取出来、再包一层都不行），把 sid 交给"+
			"该处理它的地方，这个节点照常返回", ErrRunAborted, where(), sid)
	}
	if ie == nil {
		// typed-nil：业务写 `var ie *InterruptError; return nil, ie` 时 IsInterrupt 返
		// (nil, true)。放过去的话后面按「中断」处理却没有任何 query 可挂。
		return fmt.Errorf("%w: %s: 返回了 nil 的 *InterruptError", ErrRunAborted, where())
	}
	if vErr := ie.Validate(); vErr != nil {
		return fmt.Errorf("%w: %s: %v", ErrRunAborted, where(), vErr)
	}
	queries := ie.AllQueries()
	if len(queries) == 0 {
		return fmt.Errorf("%w: %s: InterruptError 里没有 AskQuery"+
			"——caller 要靠它配答案，reason=%q", ErrRunAborted, where(), ie.Reason)
	}
	seen := make(map[string]string, len(queries))
	for _, q := range queries {
		if q == nil {
			continue // Validate() 已拦 nil 元素，这里只是防御
		}
		if q.InterruptID == "" {
			return fmt.Errorf("%w: %s: AskQuery.InterruptID 为空——提问走 compose_v2.Ask"+
				"（算 ID 与用 ID 必须是同一次调用，理由见 Ask godoc），"+
				"或自填一个跨 Resume 稳定的 ID。question=%q reason=%q",
				ErrRunAborted, where(), q.Question, ie.Reason)
		}
		// **同一批内重复 = 业务自身矛盾**，必须拒。
		//
		// 放过的话第二条会在落档时被丢掉（这道校验加进来之前，记录那步撞 ID 只 warn 然后
		// skip）：调用方从 err 看到 2 条、存档里只有 1 条 ⟹ 用户按「批准转账」那条答，而读
		// 同一个 ID 的代码可能执行的是「删除历史」。
		//
		// ⚠️ **这里只管同一批，而同样的伤害有一条本校验够不着的路**：两个**不同**提问在
		// **不同超步**复用同一个自填 ID。实测「s1 问『确认转账 5000』、s2 问『确认删除全部
		// 历史』，业务两处都写 `"CONFIRM"`」⟹ 用户只答了转账，s2 的 `GetResumeAnswer` 拿同
		// 一个 key 命中同一个答案 ⟹ **两件事都执行，`err == nil`**。
		//
		// 本校验看不见它（不同批）；框架也分不出「同一处提问重跑」（合法，Resume 后节点
		// 重跑重算出同一个 ID）和「两个提问撞了 ID」——**自填 ID 不携带位置信息，两者在数据
		// 里长得一模一样**。走 `Ask` 时按构造就不会撞（ID 由位置派生），所以这条风险的完整
		// 表述是：**只要业务自填 ID，跨超步撞 ID 就是敞开的**，而 hook 相位（`OnNodeStart`）
		// 恰恰强制自填且框架一维身份都不给，仓里的例子只能编常量。
		//
		// **已由答案兑付账本兜住**（见下方 `answerAlreadyConsumed`）：同一个答案位被第二次兑付时
		// 当场 `ErrRunAborted`，不再静默生效。剩下的已知边界是业务裸读
		// `GetResumeContext(ctx).UserInputs[k]` 绕过记账，见 `answer_consumption.go` godoc。
		// 别把这段读成「跨超步复用是正当的」——那是这条注释之前的写法，已被实测推翻。
		if prev, dup := seen[q.InterruptID]; dup {
			return fmt.Errorf("%w: %s: 两条 query 用了相同的 InterruptID %q（%q 与 %q）"+
				"——第二条会被静默丢弃，用户答一条却可能触发另一条。"+
				"每条提问给一个自己的 ID，或都走 compose_v2.Ask（它按执行位置自动区分）",
				ErrRunAborted, where(), q.InterruptID, prev, q.Question)
		}
		seen[q.InterruptID] = q.Question

		// **跨超步 / 跨 Resume 的同一伤害，这里能看见了**：这一位如果已经被兑付过，
		// 说明有**两个不同的提问算出了同一个身份**——上面那条只管同一批，这条管的是
		// 「上一次的答案已经付给别人了，你又来领同一张票」。
		//
		// 为什么是终局失败而不是「再问一次」：身份是位置的函数，同一位置重跑会算出同一个
		// ID（合法，答案还没兑付、照常命中）；而**已兑付**的位再次出现，只可能是位置维算错
		// 或自填 ID 撞车，**重跑必然一样**。再问一次会变成「调用方答了又被拒」的死循环，
		// 报错才能让人当场看见。
		if answerAlreadyConsumed(ctx, q.InterruptID) {
			return fmt.Errorf("%w: %s: InterruptID %q 上的答案已经被兑付过了，这条新提问（%q）"+
				"落在同一个答案位上——两个不同的提问算出了同一个身份。\n"+
				"  走 compose_v2.Ask 时它由执行位置派生，出现这个说明位置维算错了（框架 bug，请报）；\n"+
				"  自填 ID 时说明两处提问撞了同一个 ID——给每处一个自己的 ID，或改走 Ask",
				ErrRunAborted, where(), q.InterruptID, q.Question)
		}
	}
	return nil
}

// notifyNodeEnd 在节点体成功之后调 OnNodeEnd：它 panic 时就地兜住、带栈记一条 ERROR，这次执行照旧算成功（理由见
// runNodeWithHooks 的调用点）。
func notifyNodeEnd(logCtx context.Context, hooks OrchestrationHooks, hookCtx context.Context, id NodeID, chs Channels) {
	defer func() {
		if r := recover(); r != nil {
			panicerr.Log(logCtx, slog.LevelError, "compose_v2: OnNodeEnd 钩子 panic（节点体已经成功，结果与写入照常提交）", panicerr.New(r), "node", id)
		}
	}()
	_ = hooks.OnNodeEnd(hookCtx, id, chs, nil)
}

// runNodeWithHooks 是单节点执行的 hook 集成 helper：
//  1. OnNodeStart 失败 → 跳过 NodeFunc + 调 OnNodeEnd 配对
//  2. ctx 注入 EventSink + sendSink + node interrupt counter + visit + 实例身份
//  3. NodeFunc panic 由 safeNodeRun 转 error
//  4. OnNodeEnd 总会调（含失败路径）传 nodeErr 让 hook 知晓原因
//  5. 节点 return 后 drain sendSink 收 directives——失败时 directives 不返出（自然丢弃）
//  6. **节点成功完成后**递增 cm.visitCount[id]——失败/中断时不递增让 Resume 重跑
//     时 visit 不变保 InterruptID 稳定（audit Round 3 Bug #1 修复）
//
// instanceID：本次执行的实例身份（caller 从 FrontierMember 取，ISSUE-96）——
// SendFrom 同 Target 投递多次时每个并发 worker 拿到各自的 token，业务工具算
// InterruptID 时编入此维度防串台；静态边节点恒空串（它们本就单实例）。
func runNodeWithHooks(ctx context.Context, id NodeID, fn NodeFunc, componentKind string, policy nodePolicy, chs Channels, hooks OrchestrationHooks, cm *ChannelManager, instanceID string) (writes []ChannelWrite, directives []SendDirective, consumed []string, signals []SignalRecord, err error) {
	// audit² 修 (B-HIGH-3 + C-HIGH-2): 整函数 defer recover 兜底 hook panic. 跨
	// goroutine (fan-out frontier N>=2 时 executeFrontier 内 spawn worker
	// goroutine 调本函数) 不能依赖 compose_v2.Run 入口 outer recover (cross-
	// goroutine 不 cover). 这道兜节点体失败时的 OnNodeEnd（OnNodeStart 另有 inner recover
	// 转 startErr、走配对）；节点体成功之后的 OnNodeEnd（notifyNodeEnd）与 nodeEventSink.Send
	// 只作观测，各自就地兜住、只记日志，到不了这里.
	//
	// **第三支：`runtime.Goexit`**（`bodyReturned` 是 testing.tRunner 的 `finished`
	// 同款——旗必须由**正常控制流的普通语句**置位，不能放 defer 里置，Goexit 也跑
	// defer）。业务代码（NodeFunc / OnNodeStart hook）Goexit 时函数永不返回、错误
	// 走不了返回值，这一支干两件事：给 OnNodeEnd 配对（带合成错误，否则每个被掐断
	// 的节点漏一个不闭合的 span）+ 往 Run 的记录位记一笔带身份的错（fast path 下
	// Run 出口靠它把「静默成功」变成 Failed 存档；fan-out 下 goroutine 自己那道旗
	// 已经把错写进 results，这笔用不上但无害）。
	//
	// 覆盖边界如实说：旗在 `safeNodeRun` 返回后、成功路径 OnNodeEnd **之前**置位，
	// 所以「成功路径的 OnNodeEnd hook 自己 Goexit」不落这支——那由 Run 层的
	// `bodyReached` 兜（存档不说谎），只是错误正文没有节点身份。
	bodyReturned := false
	// 这次执行的代际与钩子相位。visit 在这里读一次、全函数共用：它只在超步的提交点推进，
	// 本函数执行期间不会变（见函数末尾）。
	visit := cm.GetVisitCount(id)
	// 钩子相位记东西的地方：本节点这次执行，按相位分域（见 withHookPhase）——碰不到节点体
	// 那一格。节点重跑时钩子也再触发一次，钩子里记下的从这里读回。
	hookPhase := func(base context.Context, kind string) context.Context {
		sc := nodeHookScope(kind, id, instanceID, visit)
		var a *nodeStateAccessor
		if cm != nil {
			// 经 scopedOf 构造：分域链里要有这一层，这个分域被推进的代际才读得到（直接填 scope
			// 的字面量链是空的：钩子里嵌套图的入口否决推进了钩子这一格，钩子自己看不见，按
			// Rerunnable 记的产出原样拿回、续跑永不收敛；草稿不记时嵌套图里的提问不换 ID，对 v1
			// 的批准兑付给了 v3——均实测）
			a = scopedOf(&nodeStateAccessor{cm: cm, key: nodeStateKey(id, instanceID, visit)}, sc)
		}
		return withHookPhase(base, sc, a)
	}
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			err = panicerr.Wrap(fmt.Sprintf("node %q hook/run panic recovered", id), pe)
			panicerr.Log(ctx, slog.LevelError, "compose_v2: 节点钩子（节点体失败时的 OnNodeEnd）panic，已转成错误", pe, panicerr.KeyError, err.Error())
			return
		}
		if bodyReturned {
			return
		}
		gerr := fmt.Errorf("node %q (ns=%q instance=%q) 异常终止：既没正常返回也没 panic"+
			"（runtime.Goexit——典型来源是 zap.WithFatalHook、testify 的 require.*、t.FailNow）",
			id, GetGraphNS(ctx), instanceID)
		err = gerr
		// OnNodeEnd 配对——包一层 recover：这段跑在 goroutine teardown 里，业务 hook
		// 再炸不能把 teardown 变成 panic 传播
		func() {
			defer func() {
				if r := recover(); r != nil {
					panicerr.Log(ctx, slog.LevelWarn, "compose_v2: OnNodeEnd panic during goexit teardown", panicerr.New(r), "node", id)
				}
			}()
			_ = hooks.OnNodeEnd(hookPhase(ctx, "OnNodeEnd"), id, chs, gerr)
		}()
		noteAbnormalNodeExit(ctx, gerr)
	}()

	// OnNodeStart hook — audit² ctx-based: hook 返新 ctx propagate 给 NodeFunc + OnNodeEnd.
	// **audit³ A2 fix**: outer defer recover 太粗——OnNodeStart panic 直接被
	// outer 接走, 跳过 OnNodeEnd 配对调用 (line 856). 用 inner recover 让
	// OnNodeStart panic 转 startErr, 走早退路径调 OnNodeEnd 真配对.
	if hooks == nil {
		hooks = NoOpOrchestrationHooks{}
	}
	// Phase 2: node 组件类型透传 (GraphAgent 显式编排路径) — fire OnNodeStart 前 set 到 ctx,
	// observability OnNodeStart 读它把 node span operation.name 标对应 kind
	// (retrieval/embedding/guardrail). component 只影响 operation.name 展示;
	// node span start/end **配对靠 registry kind=node 锚点** (跟 component 无关, OnNodeEnd
	// 不读 component), 故下面 consume 清除继承不影响配对.
	//
	// **总是 set (含空), 不加 `if != ""` 守卫** — consume 语义防跨层泄漏: component 只作用于
	// 声明它的那一层. 无声明 node set "" 覆盖**继承的父 component**, 否则父 node 的 component
	// 顺 ctx 泄漏到 NodeFunc 内启动的子 graph / sub-agent (如 retrieval node 内部 sub-agent
	// 被误标 retrieval). 每进一个 node 按本 node 声明 set, 是清除继承的关键防线.
	// 框架自己要往节点 ctx 上挂的东西，收在一处——**hook 之前铺一次**；hook 返回的 ctx 经
	// [CallHook] 采纳，执行状态取这里铺好的（见下方 OnNodeStart 之后那段）。
	//
	// ISSUE-96 侧面 6：这些必须**先于 OnNodeStart** 进 ctx——观测层在 OnNodeStart 里读实例身份
	// （节点 span 的 crew.node.instance）；答案兑付账本要覆盖 hook 相位的提问（那一层
	// 框架一维身份都不给、业务只能自填常量 ID，最容易撞）。
	// parent 链让子层判定看得见祖先已提交的兑付（本节点若是嵌套图的宿主，图内节点的
	// 账本经此链上溯到这里、再到更外层——见 answerLedger godoc「parent 链」）。
	ledger := &answerLedger{cm: cm, parent: getAnswerLedger(ctx)}
	// 这次执行做出的上行信号（见 signal.go）：成功时取出、随本超步的提交并入 cm；失败 / 中断时
	// 随这次执行一起作废。跟账本一样挂在框架那几个值里——业务 hook 返非派生 ctx 时重铺回去。
	sigScope := &signalScope{}
	// 槽在 Run 层建、随 ctx 流下来——**入口先取出来**，好在 hook 换了 ctx 之后重铺回去
	// （业务返非派生 ctx 时它会跟着丢，那时 Goexit 的 note 无处可记 ⟹ 错误丢节点身份）。
	abnormalSlot := getAbnormalExitSlot(ctx)
	// 「在节点执行范围内」那枚章带着这次 Run 的 session ID，同样**入口先取**：钩子返回的 ctx
	// 上可能已经推了别的 sid（见 NodeScopeSessionID）。进节点时 ctx 上的就是图自己的 sid——
	// Run 入口推过，采纳 OnGraphStart 返回的 ctx 之后又重铺过一次。
	scopeSID := GetSessionID(ctx)
	applyFrameworkNodeCtx := func(base context.Context) context.Context {
		base = WithComponentKind(base, componentKind)
		base = WithNodeInstanceID(base, instanceID)
		base = withAbnormalExitSlot(base, abnormalSlot)
		// **「有人接得住中断」的章**，与下方 `withNodeInterruptCounter` 盖的身份章
		// 是**两个信号回答两个问题**：这一个说「本层之上有个节点会接住并落档」，
		// 那一个说「中断身份的维度从哪来」。两者的盖章时机要求相反——身份章必须晚到
		// `OnNodeStart` 之后（hook 相位框架一维身份都不给，是刻意的），而落档能力
		// 在进节点那一刻就成立（早退路径同样会校验中断并冒泡落档，见下方
		// `validateInterrupt` 调用点）。
		//
		// 合一的代价实测过：`OnNodeStart` 里跑的子运行，其 `OnGraphStart` 抛的中断
		// 被判成「没人接得住」而拒绝——可外层早退路径明明接得住，挂起档 0、白拒一次。
		base = withinNodeScope(base, scopeSID)
		// 节点体、嵌套图读到的 sid 也跟章一致：OnNodeStart 往 ctx 上推的别的 sid 不往下流（节点
		// 里直接跑的嵌套图不显式给 sid 时取 ctx 上的，那个 sid 就是它存档的主索引）
		base = withSessionID(base, scopeSID)
		// 钩子返回的 ctx 通常派生自钩子相位的那个——相位的东西不往节点体带（节点体在下面
		// 另配自己的计数器与访问器）
		base = withAccessor(withBodyPhase(base), nil)
		base = context.WithValue(base, subCallCounterKey{}, (*subCallCounter)(nil))
		base = withSignalScope(base, sigScope)
		return withAnswerLedger(base, ledger)
	}
	ctx = applyFrameworkNodeCtx(ctx)
	// 答案兑付账本同理**必须先于 OnNodeStart**：hook 相位也会提问、也会读答案，而它恰恰是
	// 框架里**唯一还强制业务自填 ID** 的地方（框架一维身份都不给，例子只能编常量）——
	// 也就是最容易撞 ID、最需要这道兜底的地方。晚一步注入，hook 那条路就整条绕过：
	// 记不上（兑付无人知）也拦不住（撞了不报）。
	//
	// ⚠️ 这跟上面实例身份那条是**同一个教训的第二次**：同一个函数、同一条早退路径，
	// 上一轮空 ID 校验漏的也是 OnNodeStart（见下方 validateInterrupt 调用点的注释）。
	// 放标记、调、采纳收在 CallHook 一处；钩子 panic 转成 startErr（走下面的早退，OnNodeEnd 照样配对）。
	var startErr error
	newCtxNil := false
	ctx, startErr = CallHook(ctx, func(c context.Context) (got context.Context, err error) {
		defer func() {
			if r := recover(); r != nil {
				pe := panicerr.New(r)
				err = panicerr.WrapOpaque("OnNodeStart panic", pe)
				panicerr.Log(c, slog.LevelError, "compose_v2: OnNodeStart 钩子 panic，已转成错误", pe, "node", id)
			}
		}()
		got, err = hooks.OnNodeStart(hookPhase(c, "OnNodeStart"), id, chs)
		newCtxNil = got == nil && err == nil
		return got, err
	})
	if startErr != nil {
		bodyReturned = true // 正常早退，不是 Goexit（见函数头 defer 第三支）
		// 早退用入参 ctx 调 OnNodeEnd (业务 OnNodeStart 返 (nil, err) 时 newCtx=nil)
		// audit³ A2: OnNodeEnd 也用 inner recover——避免 panic 跳过 return
		func() {
			defer func() {
				if r := recover(); r != nil {
					panicerr.Log(ctx, slog.LevelWarn, "compose_v2: OnNodeEnd panic in early-exit path", panicerr.New(r), "node", id)
				}
			}()
			_ = hooks.OnNodeEnd(hookPhase(ctx, "OnNodeEnd"), id, chs, startErr)
		}()
		// **hook 抛的中断走同一套校验**——它原先整条绕过（早退在这儿、校验在下面几十行），
		// 于是空 ID 的挂起照样落进存档：实测两个分支各抛一次 ⟹ 两条挂起 ID 都是空、
		// 彼此不可区分。那正是空 ID 拒绝要消灭的形态，从这个入口原样存活。
		if ie, isInt := IsInterrupt(startErr); isInt {
			if vErr := validateInterrupt(ctx, id, startErr, ie); vErr != nil {
				return nil, nil, nil, nil, vErr
			}
		}
		return nil, nil, nil, nil, fmt.Errorf("node %q OnNodeStart: %w", id, startErr)
	}
	// **框架的 ctx 值不由业务 hook 决定存亡**。
	//
	// 按 [CallHook] 的规则采纳（上面已经采纳过）：业务在钩子里推的值照收（那是它的正当用途：OTel 把 span
	// push 进去、下游要拿得到），框架的身份与位置一律取 hook 之前的这个 ctx；钩子返回的 ctx
	// 不是从入参派生的（`context.Background()`、或从自己池子里拿的）时，环境与取消也取这个。
	// 否则框架在 hook 之前注入的东西**全部静默消失**：实例身份没了（N 个并发实例落同一个槽、
	// 子 cp 互覆、InterruptID 撞车）、答案兑付账本没了（那道兜底对这个节点整条失效）。两条都实测过。
	if newCtxNil {
		slog.WarnContext(ctx, "compose_v2: OnNodeStart 返回了 nil ctx，已沿用框架自己的 ctx（交给钩子之前的那个）",
			"node", id,
			"hint", "hook 的契约是返回「基于入参派生的 ctx」")
	}

	// 这次执行交给前端的流单独记一份，结束时处置（见 settleNodeStreams）：上一次执行同一个节点留下、这次没再登记的撤回。
	// 要在建节点 sink 之前开，节点 sink 发片段时记进它
	streamNodeKey := nodeStateKey(id, instanceID, visit)
	ctx, nodeStreamRec, prevStreams := beginNodeStreams(ctx, cm, streamNodeKey)

	// 节点入口注入 EventSink + sendSink + per-node interrupt counter
	// graphNS 节点入口时 ctx 已含 (Run 入口 line 255 push cfg.graphNS), 缓存避免每
	// chunk Send 走 ctx.Value lookup——业务 LLM 流式高频热路径敏感.
	nodeCtx := withEventSink(ctx, &nodeEventSink{
		// OnNodeEvent 也是钩子相位：这次执行的所有事件共用一个相位（一个计数器），事件按
		// 同样的次序触发时编号跨续跑稳定
		ctx:     eventHookCtx(hookPhase(ctx, "OnNodeEvent")),
		hooks:   hooks,
		nodeID:  id,
		graphNS: GetGraphNS(ctx),
	})
	// per-node interrupt counter + nodeID + visit——业务工具算稳定 ID 用（实例身份
	// 已在更早处注入，见 WithNodeInstanceID 调用点）：防多节点 / 多次访问串台
	nodeCtx = withNodeInterruptCounter(nodeCtx, id, visit)
	nodeCtx = withCurrentTaskID(nodeCtx, cm.TaskID())
	// 节点私有中断态访问器（见 node_state.go）：键在这里**算一次**、读写共用，
	// 不给「读按一套算、写按另一套算」留缝。绑 cm ⟹ 只落本层 cp，不向上冒泡。
	nodeCtx = withNodeStateAccessor(nodeCtx, cm, id, instanceID, visit)

	// 节点函数本身：按节点声明的超时 / 重试跑（见 node_policy.go；没声明时就是一次调用）。SendFrom
	// 收集器每次尝试一个，拿回来的是成功那次的。
	var sink *sendSink
	writes, sink, err = runNodeBody(nodeCtx, id, fn, chs, policy)
	settleNodeStreams(nodeCtx, cm, streamNodeKey, nodeStreamRec, prevStreams, err, GetEventSink(nodeCtx), string(id))
	// **普通语句置旗，不能放 defer**：NodeFunc 里 Goexit 时 runNodeBody（里面的 safeNodeRun）
	// 永不返回、这一行执行不到 ⟹ 旗保持 false ⟹ 函数头 defer 第三支接手；panic 则被
	// safeNodeRun 转成 err 正常返回到这里 ⟹ 旗置位 ⟹ 不误判。
	bodyReturned = true
	// 节点体成功时，OnNodeEnd 是观测钩子（返回值不往后流、不影响执行）：它 panic 不让这次执行作废——就地兜住、带栈记一条
	// ERROR，结果与写入照常提交（见 notifyNodeEnd）。节点体失败时照旧：panic 交给函数头那道兜底、转成这个节点的错误。
	//
	// 曾经成功路径也转成错误（「钩子 bug 要让业务看见，损 writes 可接受」）：失败档会被续跑与重试捡起（auto-load 按内容
	// 判、RunWithRetry 默认判据认它可重试），已经做完的节点体重跑、副作用重放——实测同一个节点体跑 3～4 次。看得见靠那条
	// ERROR 日志。
	if err == nil {
		notifyNodeEnd(ctx, hooks, hookPhase(ctx, "OnNodeEnd"), id, chs)
	} else {
		_ = hooks.OnNodeEnd(hookPhase(ctx, "OnNodeEnd"), id, chs, err)
	}

	if err != nil {
		// 业务抛的中断先过一道形状校验（没有 query / Query 与 Queries 同时填 /
		// 空 ID / 批内撞 ID / Queries 含 nil 元素），全在 validateInterrupt 里——**与 OnNodeStart 那条路
		// 共用同一份**，别在任何一处另写一遍。
		if ie, isInt := IsInterrupt(err); isInt {
			if vErr := validateInterrupt(nodeCtx, id, err, ie); vErr != nil {
				return nil, nil, nil, nil, vErr
			}
			// InterruptError 是控制流——保持 fmt.Errorf wrap 让 IsInterrupt 链可读，不包 GraphNodeError
			return nil, nil, nil, nil, fmt.Errorf("node %q: %w", id, err)
		}
		// 业务错 / ctx 错 / safeNodeRun panic-转-error → typed *GraphNodeError 携带定位信息：
		// caller errors.As(&GraphNodeError) 拿 NodeID/GraphNS（嵌套子图定位）；Unwrap 让
		// errors.Is(ctx.Canceled) / errors.As(bizErr) 穿透原错。
		return nil, nil, nil, nil, &GraphNodeError{NodeID: id, GraphNS: GetGraphNS(nodeCtx), Err: err}
	}
	// 成功路径才 drain sink + 递增 visitCount——失败时不递增让 Resume 重跑时
	// visit 不变保 InterruptID 稳定（同位置 ID 跨 retry 一致）
	directives = drainSendSink(sink)
	// 本次执行兑付掉的答案位——只在该成员最终成功、且本超步提交成功时才并入 cm
	consumed = drainAnswerLedger(ledger)
	// 上行信号同一档：成员最终成功（出边也算得出来）且本超步提交成功时才并入 cm
	signals = sigScope.drain()
	// **owner + instanceID 的唯一 stamp 点**（issue-95#1 / ISSUE-96）：fast path 与
	// fan-out 都经本函数、且所有 error 路径都在上面 return 了，一处盖全部。框架无条件
	// 盖写——业务构造 SendDirective 时这两个字段不导出、伪造不了。
	//
	// **mint 在这里 = 代际语义的来源**：节点每跑一次都是新一批 directive、新一批
	// instanceID。父节点重跑 ⟹ 子实例全新 ⟹ 挂在旧实例上的答案自然失配、重新问
	// （与 LangGraph fork 换 checkpoint_id、eino 换 callID 同一原则：身份自然变，
	// 不需要显式作废逻辑）。deferred send 从 cp 重放时不经本函数 → 身份不变，正确。
	for i := range directives {
		directives[i].owner = id
		directives[i].ownerInstanceID = instanceID // 发出者自己的身份（purge 精确匹配用）
		directives[i].instanceID = newInstanceID() // 派生出的 target 实例的身份
	}
	// **visit 不在这里推进**——它在超步的提交点跟写入提交一起生效（见主循环）。
	//
	// 曾经在这儿 ++（节点函数一返回就推进），而那时这一步远没有定局：出边还没算、
	// 写入还没提交。任何一处随后失败都留下「代际已推进、但这一步没成」的窗口，重跑时
	// 中断身份跟着漂——用户答过的问题被重新问、节点私有态的键位对不上。
	return
}

// safeNodeRun 把 NodeFunc panic 转 error，并挂上 panic 的值与栈（[PanicError]）、记一条带栈的日志。
//
// **audit Round 10 Bug D-N3 修复**：panic value 实现 error 接口时用 %w 保
// errors.Is/As chain。
func safeNodeRun(ctx context.Context, id NodeID, fn NodeFunc, chs Channels) (writes []ChannelWrite, err error) {
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			err = panicerr.Wrap("node panic recovered", pe)
			panicerr.Log(ctx, slog.LevelError, "compose_v2: 节点函数 panic，已转成错误", pe, "node", id)
		}
	}()
	return fn(ctx, chs)
}

// safeCondRun 把 ConditionalEdge cond fn panic 转 error——**R32+ 视角 3 HIGH-1 fix**
// (与 safeNodeRun 同模式)。
//
// 之前裸调 e.cond 让业务 cond fn panic 时 goroutine crash 直接传出 Run, 绕过
// graph_loop defer 的 SaveNS / OnGraphEnd hook 兜底——caller 看 panic stack
// trace 而非 typed error, 无法 errors.As 区分。
//
// 业务 cond fn = 业务经 AddConditionalEdge 注入的路由判断函数。任何业务自定义 cond
// 都可能 panic。
//
// 注：framework 自带 7 处 cond（basic model/tools、loop/handoff/reflect 的 exitChoice、
// plan、routing）**全是纯读 channel 判方向**——干活的逻辑一律在 node（如 routing 的
// classifier 在 classify node、loop 的 ExitCondition 在 exitCheck node），且 cond 禁止
// interrupt（见 [wrapCondFailure]）。本 recover 主要兜业务自定义 cond。
func safeCondRun(ctx context.Context, cond func(context.Context, Channels) (NodeID, error), chs Channels, from NodeID) (target NodeID, err error) {
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			err = panicerr.Wrap(fmt.Sprintf("condition from %q panic recovered", from), pe)
			panicerr.Log(ctx, slog.LevelError, "compose_v2: 条件边 panic，已转成错误", pe, panicerr.KeyError, err.Error())
		}
	}()
	return cond(ctx, chs)
}

// warnUnmatchedAnswerIDs 检查 caller 提供的 answers 与 cp 中 pending interrupts
// ID 集合的双向差异：
//   - **unmatched** (answers 多余, pending 没): 业务拼错 ID 或 cp 中无此 pending
//     interrupt——audit Round 9 Bug B4 + Round 10 Bug B1 修复共用 helper
//   - **missing** (pending 多余, answers 没, audit #5 视角 8 F9 fix): 业务部分
//     回答场景, fanout 多 query 时只答 M (≤N) 个, framework 让未答 query 自动
//     re-raise InterruptError 触发 cp 再次 save——业务前端需知"还有 N-M 个未答"
//     做 UI 渲染对比
//
// 覆盖显式 WithResumeFrom + auto-load 两条 Resume 路径。
//
// resumeFrom 为 nil（fresh Run 误传 answers）时不检查——B5 路径已在入口 warn。
// resumeFrom 无 PendingInterrupts 时不检查（cp 已完成 / 业务空 cp）。
func warnUnmatchedAnswerIDs(ctx context.Context, resumeFrom *Checkpoint, answers map[string]*schema.Message) {
	if resumeFrom == nil {
		return
	}
	// **档里一条待答问题都没有、调用方却带了答案**——这一格以前整个静默：早退在
	// 「pending 为空就 return」上，而另一道 R11-4 warn 要求 `resumeFrom == nil`，
	// 档被捡起后也不触发。失败档（`PendingInterrupts` 恒 0，中断优先赢仲裁）从
	// 2026-08-13 起会被捡起来续跑，于是这一格从「够不着」变成常规路径。
	//
	// ⚠️ **别把这条 warn 读成「答案必然丢」**——这里曾写「答案不会被消费」，被独立审计
	// 实测推翻：失败档若是「提问节点被掐在重新提问之前」（如 Goexit 顶掉 Pending 档），
	// 重跑会以**同一个位置派生 ID** 重新问出来，ResumeContext 里的答案照样配上。
	// 真会丢的是另一类：重跑分支根本不提问（纯业务错重试），答案没有认领人。
	// 框架分不出这两类（取决于节点重跑后干什么），所以只提示、不拒绝。
	if len(resumeFrom.PendingInterrupts) == 0 {
		if len(answers) > 0 {
			ids := make([]string, 0, len(answers))
			for id := range answers {
				ids = append(ids, id)
			}
			slog.WarnContext(ctx, "compose_v2: 带了答案但这份存档里没有待答问题",
				"answer_ids", ids, "cp_status", resumeFrom.Status.String(),
				"hint", "这份档是「有活没跑完」而不是「有人在等回答」。两种可能：(a) 重跑分支会以"+
					"同 ID 重新提问（提问节点被掐在重问之前）——答案会配上，这条 warn 可忽略；"+
					"(b) 重跑分支不提问（业务错重试）——答案没有认领人。真要答问题请确认 ID "+
					"来自本轮抛出的中断")
		}
		return
	}
	pendingIDs := make(map[string]bool, len(resumeFrom.PendingInterrupts))
	// 断点停下的那一条不需要答案（续跑就是放行）：给它答了算配上（方向 1 不报），没答也不算「未答」（方向 2 不报）
	breakpointIDs := map[string]bool{}
	for _, q := range resumeFrom.PendingInterrupts {
		if q != nil {
			pendingIDs[q.InterruptID] = true
			if q.Breakpoint != nil {
				breakpointIDs[q.InterruptID] = true
			}
		}
	}

	// 方向 1: answers ∖ pending — 多余 ID
	if len(answers) > 0 {
		var unmatched []string
		for id := range answers {
			if !pendingIDs[id] {
				unmatched = append(unmatched, id)
			}
		}
		if len(unmatched) > 0 {
			slog.WarnContext(ctx, "compose_v2: WithResumeAnswers 含 unmatched InterruptIDs",
				"unmatched", unmatched, "pending_count", len(pendingIDs),
				"hint", "三种场景: (a) 业务拼错 ID / cp 无对应 pending — 真异常; (b) 业务把历次答案累积在一个 map 里一起传 — 这是预期, 上几轮的 ID 自然 unmatched (example/compose/identity_guard_smoke 与 example/orchestration/nested_smoke 都是这个形态, 后者是必跑冒烟); (c) 同一个问题每次续跑都换了 ID、永远答不完 — 典型是嵌套调用的输入每次都变（工具中间件往 AgentAsTool 的参数里注入时间戳、trace_id），每次续跑都被当成一次新调用重问：给调用声明身份（agent_v2.WithAgentToolCallKey / agent_v2.WithCallKey）. 业务监控可按场景区分。**别升级成 error**: (b) 合法且真实存在, 拒掉会当场打断那条闭环")
		}
	}

	// 方向 2 (audit #5 视角 8 F9 fix): pending ∖ answers — 未答 ID (partial answer
	// 场景). framework 让未答 query 自动 re-raise 触发 cp 再次 save, 业务前端可监听
	// 此 warn 做"还有几个未答"UI 渲染.
	if len(answers) > 0 {
		var missing []string
		for id := range pendingIDs {
			if _, ok := answers[id]; !ok && !breakpointIDs[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			slog.WarnContext(ctx, "compose_v2: WithResumeAnswers 部分回答, pending 仍含未答 InterruptIDs",
				"missing", missing, "answered_count", len(answers), "pending_count", len(pendingIDs),
				"hint", "framework 会让未答 query 自动 re-raise InterruptError 触发 cp 再次 save, 业务可多轮 Resume 收敛")
		}
	}
}
