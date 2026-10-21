package compose_v2

import "fmt"

// Channels 是 NodeFunc 收到的只读 channel 访问视图。
// 通过 ChannelManager.Channels() 获取，不可由用户直接构造。
//
// **API 决策**（针对 audit F9 builder 类型擦除卖点打折）：
// 用户读 channel 必须走包级泛型函数 Get[T] / Has[T] / MustGet[T]——绕开 Go 泛型方法
// 不能引入新类型参数的限制，调用点编译期类型推断。
//
// 用户写 `compose_v2.Get(chs, TokenUsageCh)` 拿到 *Channel[int] 引用（编译期类型安全、
// IDE 重命名安全），不靠字符串 key + 反射读 channel。
type Channels struct {
	cm *ChannelManager

	// pending 非 nil 时，读到的是「已提交态 + 某个成员自己那批还没提交的写入」。
	// 只有出边计算路径会拿到这种视图（见 [pendingView]）；NodeFunc 收到的恒为 nil，
	// 即节点读的永远是已提交态——超步内 state 不变这条不受影响。
	pending *pendingView
}

// read 是所有读的单一出口：有叠加层时先把本成员自己的写叠上去。
//
// 读的入口只有这一个 ⟹ `Get` / `MustGet` / `Has` 不会各自实现一套「要不要叠」。
func (chs Channels) read(name string) (any, bool) {
	if chs.cm == nil {
		return nil, false
	}
	base, ok := chs.cm.read(name)
	if chs.pending == nil {
		return base, ok
	}
	return chs.pending.overlay(chs.cm, name, base, ok)
}

// has 同 read，只是不算值。
func (chs Channels) has(name string) bool {
	if chs.cm == nil {
		return false
	}
	if chs.cm.has(name) {
		return true
	}
	return chs.pending.hasWriteTo(name)
}

// ── 包级泛型 helper（用户主要 API）──

// Get 读取 channel 当前值。第二个返回值表示该 channel 是否被写过。
//
// **类型保护**：cast 失败 panic（programmer error）——一般不会发生，因为 channel
// 在 register 阶段类型唯一，但 dynamic channel 路径有可能（PoC F4 验证场景）。
func Get[T any](chs Channels, ch *Channel[T]) (T, bool) {
	// audit²⁴ HIGH-1 fix: ch nil 检 — 业务 `var ch *Channel[int]; Get(chs, ch)` typed nil
	// → ch.z 立即 nil deref panic. 显式 panic 让 msg 含 framework 信息.
	if ch == nil {
		panic("compose_v2.Get: nil *Channel[T]")
	}
	raw, ok := chs.read(ch.n)
	if !ok {
		return ch.z, false
	}
	v, castOk := raw.(T)
	if !castOk {
		panic(fmt.Errorf("channel %q type cast failed: stored %T, requested %T", ch.n, raw, ch.z))
	}
	return v, true
}

// MustGet 是 Get 的"已写入则返"版本——未写入直接 panic。
// 用于节点确定性已知 channel 必有值的场景（依赖编辑保证）。
func MustGet[T any](chs Channels, ch *Channel[T]) T {
	// audit²⁵ HIGH-1 fix: ch nil 检 — Get 内已检 panic, 但 panic msg 含 framework 信息更清晰.
	if ch == nil {
		panic("compose_v2.MustGet: nil *Channel[T]")
	}
	v, ok := Get(chs, ch)
	if !ok {
		panic(fmt.Errorf("channel %q has no value yet", ch.n))
	}
	return v
}

// Has 仅返回该 channel 是否被写过——比 Get 省 value 拷贝。
func Has[T any](chs Channels, ch *Channel[T]) bool {
	// audit²⁵ HIGH-2 fix: ch nil 检 — ch.n 立即 nil deref.
	if ch == nil {
		return false
	}
	return chs.has(ch.n)
}

// ── 写：通过 ChannelWrite 接口实现 ──

// ChannelWrite 是节点向 channel 写入的指令。
// NodeFunc 返回 []ChannelWrite，由 graphLoop 在超步合并点统一 ApplyAll。
//
// **不暴露 apply 方法的 implementer**：用户只能通过 Write[T] 函数构造，确保类型
// 安全（Write 编译期保证 value 与 channel T 一致）。
//
// **归并本身不在这里**：三种实现曾各自「查表 → 读 prev → reduce → 写回」，四行
// 抄了三份。现在归并统一在 [ChannelManager.ApplyAll] 里做，本接口只回答「你要写哪个
// channel」「你要写什么值」「找不到那个 channel 时该怎么说」。
type ChannelWrite interface {
	// channelName 返回此 write 目标 channel 名。
	channelName() string
	// valueAny 返回本 write 的裸 value——cp 序列化路径用（PA-H1 DeferredSends
	// 持久化要把 SendDirective.Payload 编码为 (channel, taggedValue) 对），
	// 同时也是 ApplyAll 归并时喂给 reducer 的 next。
	valueAny() any
	// notRegisteredErr 返回「目标 channel 没注册」时该报的错。**留给实现自己说**：
	// 子图 export 撞上这个时，要指出的是「父图缺声明」而不是泛泛的「没注册」——
	// 前者直接告诉业务该去哪儿加一行。
	notRegisteredErr() error
}

// channelWrite 是 ChannelWrite 的私有实现。
type channelWrite[T any] struct {
	ch  *Channel[T]
	val T
}

func (w *channelWrite[T]) channelName() string { return w.ch.n }

func (w *channelWrite[T]) notRegisteredErr() error {
	return fmt.Errorf("channel %q not registered", w.ch.n)
}

func (w *channelWrite[T]) valueAny() any { return w.val }

// rawChannelWrite 是 cp 反序列化路径的 ChannelWrite 实现（PA-H1 DeferredSends
// 重放）：从 wire 恢复的 (channelName, value) 对，归并走 ApplyAll 复用
// reducer 协议（与泛型 Write 同一合并点语义）。不导出——业务写路径仍强制
// 泛型 Write[T] 编译期类型保护。
type rawChannelWrite struct {
	name string
	val  any
}

func (w *rawChannelWrite) channelName() string { return w.name }
func (w *rawChannelWrite) valueAny() any       { return w.val }

func (w *rawChannelWrite) notRegisteredErr() error {
	return fmt.Errorf("channel %q not registered", w.name)
}

// Write 创建一条 ChannelWrite 指令——节点调用，但不立即生效，直到 graphLoop
// 在超步合并点 ApplyAll。
//
// **类型保护**：value 必须与 ch 的 T 一致——编译期保证（`Write(intCh, "str")` 飘红）。
func Write[T any](ch *Channel[T], value T) ChannelWrite {
	// audit²⁴ HIGH-2 fix: ch nil 检 — 业务传 typed nil ch → ApplyAll 时 ch.fn / ch.n nil deref.
	if ch == nil {
		panic("compose_v2.Write: nil *Channel[T]")
	}
	return &channelWrite[T]{ch: ch, val: value}
}
