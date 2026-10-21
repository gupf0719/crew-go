// Package compose_v2 是 channel-based 重设计 PoC。
//
// **PoC 目的**（详见 doc/v2/channel-based-design.md）：
// 验证 Go 泛型能否做出干净且编译期类型安全的 channel API。
// 验证通过后进 Phase 1-4 整体重写；不通过退路线 C 混合。
//
// **不在 PoC 范围**：性能 / 9 编排迁移 / cp / Resume / HITL / 4 层压缩适配。
//
// **本文件职责**：定义 Channel[T] 泛型类型 + reducer 工厂。
package compose_v2

import (
	"reflect"
)

// Reducer 把 prev 值与 next 增量合并为新值。
//
// reducer 必须是**纯函数**：相同输入相同输出、无副作用，且**不就地改 prev / next**。
// 「不就地改」这条对切片尤其容易违反——见下方 Append 的写法。
//
// 典型实现：
//
//   - LastValue: func(_, next T) T { return next }      // 覆盖
//
//   - Sum:       func(p, n int) int { return p + n }
//
//   - OnceFlag:  func(prev, _ T) T { if prev != zero { return prev }; return next } // 单调
//
//   - Append:    **必须新建 backing，不能 `append(prev, next...)`**：
//
//     func(prev, next []T) []T {
//     out := make([]T, 0, len(prev)+len(next))
//     out = append(out, prev...)
//     return append(out, next...)
//     }
//
//     `append(prev, next...)` 在 `cap(prev) > len(prev)` 时**写进 prev 的底层数组**。
//     框架自带的 [Append] 早已是上面这个 make+copy 版；这段示例曾经写作前者，
//     而它在下面第 5 条那条并发路径上是确定性 data race（`-race` 10/10 复现）。
//
// **额外约定**：
//
//  1. **Reducer 内禁止访问 cm**（C1）：reducer 不可在内部调 Get/Has/PendingInterrupts
//     等 cm 公开方法——提交路径（`ApplyAll` 的归并段）持 `cm.mu.Lock()` 调 reducer，reducer 再请求
//     `cm.mu.RLock()` 同 goroutine 重入会**死锁**（sync.RWMutex 不支持 reentrant）。
//     Reducer 输入只用 prev/next 两个参数。
//
//  2. **顺序无关性**（D3）：同一次 Run 内 reduce 顺序**确定**（executeFrontier 按
//     frontier 索引收集 writes，非 goroutine 完成顺序）。跨 Resume 分两种行为：
//     - **只重跑失败分支**（业务 error / InterruptError 含 HITL 的主路径）：成功
//     分支 writes 先 commit、失败分支推迟到 Resume 才并入，reduce 顺序与"一次
//     跑完"**不同**。
//     - **整超步重跑**（ApplyAll 归并段报错或 reducer panic 后，那一批一条都没提交）：
//     整个 frontier 重跑、按 frontier 索引重新 reduce，顺序与首次**一致**。
//     因此只有**用于 fan-out 多分支写同一 channel 且可能走"只重跑失败"路径**的
//     reducer 才需顺序无关，否则跨 Resume 结果不一致：
//     - 严格满足交换律：Sum（整数加）、OnceFlag（单调或）。
//     - **LastValue / Append 数学上不满足交换律**（LastValue 返 next、Append 顺序
//     敏感），靠**使用场景**安全：LastValue 通常单分支写；Append 收集结果带标识
//     （如 ParallelBranchResult.Name）、消费方不依赖顺序。
//     - 业务 Custom reducer 若顺序敏感（如 (a+b)/2 平均、减法）又用于上述 fan-out
//     场景，行为未定义（框架不检查，业务自负）；单分支顺序写的 channel 不受影响。
//
//  3. **无 error 返回路径**（D2）：Reducer 接口不含 error 返回——业务失败应
//     类型保护在 Write 之前（编译期）；reducer panic 由 ApplyAll 的 defer recover
//     转成 error，此时那一批写**一条都没提交**（归并跑在局部 staged 上）。
//
//  4. **闭包不持久化跨 Resume**（**ISSUE-87 LOW-3 fix 文档化**）：业务自定义
//     `Custom(name, reducer, zero)` reducer 闭包捕获 mutable state（如
//     `var counter atomic.Int64; reducer := func(p, n T) T { counter.Add(1); ... }`）
//     时, counter **不在 cp 协议内**, 跨 Resume 丢。Reducer 应**无副作用 + 闭包
//     不持久化**——所有跨 Resume 状态用 channel value 自身承载（Channel[counterStruct]
//     存计数器, reducer 纯函数读 prev.Count + 1）。
//
//  5. **reducer 可能被并发调用，且调用次数多于「每次提交一次」**。它有三条调用路径：
//
//     - **提交路径**（`ApplyAll` 的归并段）：持 `cm.mu.Lock()`，全进程串行。**次数不保证
//     「每次提交恰好一次」**——整批里靠后那条写不合法时（channel 未注册），靠前那些
//     已经归并过了，只是没提交；
//     - **条件边的叠加视图路径**：条件边跑在**各成员自己的 goroutine** 里（见
//     [GraphBuilder.AddConditionalEdge]），它读 channel 时要把本成员那批还没提交的
//     写入叠上去，那次 reduce **不持锁**、且 N 个成员并发发生；
//     - **子图的读路径**：被 [WithImport] 进子图、且子图自己也写过的 channel，子图内
//     **每次** `Get` 都算一次 reduce(baseline, own)（`ChannelManager.read`），**不持锁**、
//     且同一子图节点被扇出成 N 个实例时 N 路并发——每个实例的 baseline 是父图**同一份**值，
//     原地改 prev 的 reducer 在这条路上会让 N 个实例同时写父图那块 backing（实测 -race 命中）。
//
//     纯函数（不改 prev/next、不碰外部状态）照此写就无需任何同步——这也是上面把
//     Append 的示例订正成 make+copy 版的原因：`append(prev, next...)` 会有多个 goroutine
//     同时写同一段 backing。
//
//     顺带一个可观测差异：被条件边读到的 channel，其 reducer 会**每次 cond 读各跑一遍**、
//     外加提交点那一次。纯 reducer 看不出区别；第 4 条那种闭包计数器的读数会变——那正是
//     第 4 条已经不推荐的写法。
type Reducer[T any] func(prev, next T) T

// Channel 是 typed state channel：业务声明一次，节点读写 type-safe。
//
// **设计决策**（针对 audit F1 EndChannel reducer 选错）：
// reducer 由用户显式声明，不内嵌"默认 LastValue"——避免 EndChannel 用 LastValue 时
// 可被覆盖为 false 的语义陷阱。需要单调结束语义请用 OnceFlag reducer。
type Channel[T any] struct {
	n  string     // channel 名（拓扑唯一）
	fn Reducer[T] // 合并函数
	z  T          // 零值（read 未写入时返回）
	// single 声明「一个超步里只该有一个写入者」。LastValue 家族置位——它的 reducer
	// 是 `return next`，两个并发分支同时写时**静默丢一个**，赢家由 frontier 索引序
	// 决定。引擎据此在超步归并前拒掉这种图（见 frontierResult.SingleWriterConflictErr）。
	//
	// 只对**跨成员**生效：同一个节点在自己的 []ChannelWrite 里连写两次是有序且确定的，
	// 不算冲突。
	single bool
}

// Name 返回 channel 名（用于诊断日志，不暴露内部结构）。
func (c *Channel[T]) Name() string { return c.n }

// asAny 实现 anyChannel 接口（隐藏在 channel_manager.go），
// 让 ChannelManager 能持 []anyChannel 而不暴露泛型 T。
func (c *Channel[T]) asAny() anyChannel { return &channelHandle[T]{c} }

// channelHandle 是 Channel[T] 的类型擦除适配器。
// **关键**：每个具体 T 的 Channel[T] 实现 anyChannel 时拿到自己的 T，
// reduce / zero / typeRep 都用 typed 路径——只在 ChannelManager 持有时擦除。
type channelHandle[T any] struct{ ch *Channel[T] }

func (h *channelHandle[T]) name() string { return h.ch.n }

// singleWriterPerStep 返 channel 是否声明「一个超步里只该有一个写入者」。
func (h *channelHandle[T]) singleWriterPerStep() bool { return h.ch.single }

// typeRep 返 Channel[T] 的 T 类型代表。
//
// **ISSUE-87 LOW-2 fix**: 用 `reflect.TypeOf((*T)(nil)).Elem()` 拿 T 自身,
// 不依赖零值——之前 `reflect.TypeOf(h.ch.z)` 在 T 是 interface 类型 + 零值是
// nil interface 时返 nil (`reflect.TypeOf(zero_interface) == nil`), 让 register
// 比对 `typesEqual(existing, ch)` 比 `nil==nil` 全过, 两个异 interface T
// (`Channel[Animal]` vs `Channel[Vehicle]`) 同名注册不报错 → 后续 Get cast 错。
//
// 当前生产无 interface T 通道, 但 godoc 没说明此限制——主动加防御。
func (h *channelHandle[T]) typeRep() reflect.Type { return reflect.TypeOf((*T)(nil)).Elem() }

func (h *channelHandle[T]) zeroAny() any { return h.ch.z }

// reduceAny 接收 prev/next 都是 any，cast 回 T 调 typed reducer，返回 any。
// **错误处理**（针对 audit F4 dynamic channel 类型错可观测性）：
//   - prev/next 类型不匹配 T 时直接 panic——开发期立即暴露
//   - 不返 error 走 graceful 路径——类型错是 programmer error 不是 runtime error
func (h *channelHandle[T]) reduceAny(prev, next any) any {
	var pT, nT T
	if prev != nil {
		var ok bool
		pT, ok = prev.(T)
		if !ok {
			panic(typeMismatchError(h.ch.n, h.ch.z, prev))
		}
	}
	if next != nil {
		var ok bool
		nT, ok = next.(T)
		if !ok {
			panic(typeMismatchError(h.ch.n, h.ch.z, next))
		}
	}
	return h.ch.fn(pT, nT)
}

// ── 包级 reducer 工厂（覆盖 90% 场景，用户不需要自己写 reducer）──

// LastValue 创建 LastValue 语义的 channel：next 覆盖 prev。
// 适用：节点输出类如"最近一次 LLM 回复"。
//
// **它是「单写入者」channel**：一个超步里有两个成员同时写它时，引擎**拒掉整个 Run**
// （wrap [ErrRunAborted]），而不是静默丢掉其中一个。同一超步里两条 [SendFrom] 指令的
// payload 写它也一样——那批 payload 灌的是共享 channel，N 条归并后只剩最后一条。
//
// ⚠️ [ErrRunAborted] 意味着**清 cp 子树**：该 (sid,ns) 下已经问过人、人也答了的挂起档
// 会一并清掉，之前超步的副作用重跑时会重放。撞上它就是得改图重跑，没有原地续。
//
// 理由是 reducer 本身：`return next` 在并发下没有良定义的结果——赢家由 frontier 索引序决定（**确定但与业务意图无关**——
// 声明在后的那个赢），而两个分支都成功、`err == nil`，业务看到的是「另一个分支的写入凭空消失了」。这是
// 「N=1 正确、N≥2 静默错」那一族里最难查的一档：接一个并发分支时它一直是对的。
//
// 撞上时的出路：
//   - 想合并 → 换 [Append] / [Sum] / [Custom] 带真 reducer 的 channel；
//   - 本来就不该同时写 → 那是接线错了，错误正文里点出了是哪两个成员；
//   - 真的要「谁最后写谁赢、我不在乎是谁」→ [Custom] 显式声明同一个 reducer：
//     `Custom(name, func(_, next T) T { return next }, zero)`。显式写下来的不拒。
//
// 同一个成员在自己的 []ChannelWrite 里连写两次不受影响（有序、确定）。
func LastValue[T any](name string) *Channel[T] {
	var zero T
	return &Channel[T]{
		n:      name,
		fn:     func(_, next T) T { return next },
		z:      zero,
		single: true,
	}
}

// Append 创建 append 语义的 channel：next 追加到 prev。
// 适用：消息累积、事件流。
//
// **audit Round 9 Bug C3 修复**：reducer 不复用 prev 的 underlying array——
// 之前 `append(prev, next...)` 在 prev cap > len 时直接写老 array，业务调用方
// 之前 Get(chs, ch) 拿到的 slice 与 cm.values 内 slice 共享底层数组 → 并发
// Get + Write 同 channel 触发 -race report，或业务持有的 slice cap-extension
// 槽位被串入新 write 内容。
//
// 改为预分配新 backing：先 `make + copy(prev)` 再 append next——保证返回的
// slice 与 prev 不共享底层。代价是每次 Append 多一次 alloc + memcpy，对消息累积
// 类典型 channel 影响可忽略；换来并发安全 + 业务 Get 拿 slice 视为只读时有保证。
func Append[T any](name string) *Channel[[]T] {
	return &Channel[[]T]{
		n: name,
		fn: func(prev, next []T) []T {
			out := make([]T, 0, len(prev)+len(next))
			out = append(out, prev...)
			return append(out, next...)
		},
		z: nil,
	}
}

// Sum 创建累加语义的 int channel。
// 适用：token usage 计数、节点访问数等。
func Sum(name string) *Channel[int] {
	return &Channel[int]{
		n:  name,
		fn: func(p, n int) int { return p + n },
		z:  0,
	}
}

// OnceFlag 创建单调 bool 语义：一旦写入 true 就锁定，后续 update 忽略。
// 用法：终止信号、不可逆状态标记。
//
// **设计决策**（针对 audit F1 EndChannel 用 LastValue 漏洞）：
// LastValue[bool] 允许 prev=true 被 next=false 覆盖——破坏"已结束不可撤销"语义。
// OnceFlag 显式表达单调性，PoC 用它实现 EndChannel 等终止信号。
func OnceFlag(name string) *Channel[bool] {
	return &Channel[bool]{
		n: name,
		fn: func(prev, next bool) bool {
			if prev {
				return prev // 已 set，锁定
			}
			return next
		},
		z: false,
	}
}

// Custom 创建自定义 reducer 的 channel——业务表达不在 LastValue/Append/Sum/OnceFlag
// 的语义时使用。
func Custom[T any](name string, reducer Reducer[T], zero T) *Channel[T] {
	// audit⁹ HIGH-3 fix: isFrameworkNil 检 typed nil Reducer fn (协议九复证).
	// 业务 `var r Reducer[int]; Custom("x", r, 0)` typed nil → 后续 ch.fn(prev, next)
	// 远 stack nil deref. 与 NodeFunc / cond fn (audit⁷ HIGH-1/2/3) 同协议.
	if isFrameworkNil(reducer) {
		panic("Custom: nil reducer (含 Go typed nil func 陷阱)")
	}
	return &Channel[T]{n: name, fn: reducer, z: zero}
}

// ── Phase 1.2.b: dynamic channel runtime register ──
//
// 默认场景：业务在 init() 用 LastValue/Sum/Append/OnceFlag/Custom 工厂静态
// 声明 channel——拿到 *Channel[T] 引用，节点 Get/Write 编译期类型保护，
// PoC 设计文档承诺的"90% 静态声明"。
//
// 但 10% 场景需要运行时动态注册：根据用户 query 类型决定声明哪些 metric
// channel / 业务半结构化字段。此时无法在 init() 静态声明——需要 runtime
// register API。
//
// 设计决策（针对 audit F4 dynamic channel 类型错可观测性）：
//   - DynamicChannel[T any](name) 返同款 *Channel[T] —— 类型擦除只在
//     Channels.cm 内部，对节点 API 仍 typed
//   - 运行时类型 mismatch fail-fast（panic）—— 与静态注册的同源行为
//   - 用法相同：拿到 *Channel[T] 引用 → cm.RegisterChannel → 节点
//     Get/Write 用法不变
//
// 命名差异化（DynamicChannel vs LastValue/Sum/Append）：让用户读代码时
// 知道这是 escape hatch，不是首选——优先静态声明。

// DynamicChannel 是动态 channel 工厂——签名同 Custom 但语义强调"运行时创建"。
//
// 用例：业务循环里根据条件 spawn 多个 channel。
//
//	for _, m := range metricsByType {
//	    ch := compose_v2.DynamicChannel("metric_"+m, m.Reducer, m.Zero)
//	    compose_v2.RegisterChannel(cm, ch)
//	    // 之后节点用 ch 读写
//	}
//
// 实现等同 Custom——独立函数仅为 API 命名清晰（让代码 review 时一眼能看出
// 这是 dynamic 路径）。
func DynamicChannel[T any](name string, reducer Reducer[T], zero T) *Channel[T] {
	// audit⁹ HIGH-4 fix: isFrameworkNil 检 typed nil Reducer fn (与 Custom 协议对称).
	if isFrameworkNil(reducer) {
		panic("DynamicChannel: nil reducer (含 Go typed nil func 陷阱)")
	}
	return &Channel[T]{n: name, fn: reducer, z: zero}
}

// DynamicLastValue / DynamicAppend / DynamicSum 是常用 reducer 的 dynamic 版本。
// 签名与静态版本相同，仅语义命名差异化。

// DynamicLastValue 是动态版的 LastValue——运行时创建。
func DynamicLastValue[T any](name string) *Channel[T] {
	var zero T
	return &Channel[T]{n: name, fn: func(_, next T) T { return next }, z: zero, single: true}
}

// DynamicAppend 是动态版的 Append——运行时创建。
//
// 同 Append 一样不复用 prev underlying array（audit Round 9 Bug C3）。
func DynamicAppend[T any](name string) *Channel[[]T] {
	return &Channel[[]T]{
		n: name,
		fn: func(p, n []T) []T {
			out := make([]T, 0, len(p)+len(n))
			out = append(out, p...)
			return append(out, n...)
		},
		z: nil,
	}
}

// DynamicSum 是动态版的 Sum——运行时创建。
func DynamicSum(name string) *Channel[int] {
	return &Channel[int]{n: name, fn: func(p, n int) int { return p + n }, z: 0}
}
