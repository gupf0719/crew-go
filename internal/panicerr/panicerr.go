// Package panicerr 是框架兜住 panic 时共用的那一份做法：在 recover 所在的 defer 里取调用栈，
// 把「panic 值 + 栈」作为一条记录并列挂到兜底点返回的错误上，再记一条带栈的日志。
//
// 全仓把 panic 转成错误、或在兜底点记日志的 recover() 都经这里。例外有两类：补归因或清理后原样重抛的几道
// （compose_v2 的 composeRead / reduceFor、hook 的 ChainHooks.OnModelEndStream、钩子链用的 [CallEach]——栈由最后接住它的那一道取），
// 和有意不记的 agent_v2 marshalRecovered（算输入摘要时把 MarshalJSON 的 panic 当成「这个值编码不了」）。
// compose_v2 内谁兜底什么见 compose_v2/panic_recovery.go，其余包的兜底点在各自的 recover 处说明。
// 对外导出的名字是 compose_v2.PanicError（本包的 Error 的类型别名），调用方用 errors.As 取。
//
// # 三条约定
//
//   - **错误文本不含栈**：兜底点返回的错误文本与原来逐字相同。错误文本会交给前端（agui 的 RUN_ERROR）、
//     对端（a2a 的 failed 状态）、模型（AgentAsTool 转成的工具失败）和 span 的 error.message，
//     栈放进去就是把内部调用栈发到了外面。
//   - **记录不改错误的判定**：Error 不实现 Unwrap。panic 值是 error 时能不能经 errors.Is / errors.As
//     认出来，仍由兜底点原来那个错误决定（有的兜底点用 %w 接上，有的只取文本）。框架靠错误类型判控制流
//     （中断、ctx 错、软取消、陈旧写回……），让记录再把 panic 值露一次，会把「某处 panic 了一个中断」
//     变成真中断。
//   - **兜底点当场记日志**：有几条路径会把错误吃掉、只留文本（AgentAsTool 转工具失败、AutoCompact
//     失败、fan-out 多分支失败只返一个……），只挂在错误上的话栈在那些路径上照丢。兜底点在框架持锁区里的
//     不能当场记（日志 handler 是业务代码），改由收到错误的一方补记，见 WrapPending。日志 handler 在 Log
//     自己起的 goroutine 里跑，它 panic 或调 runtime.Goexit 都碰不到兜底路径，见 Log。
package panicerr

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
)

// 日志字段名。
const (
	KeyPanic = "panic" // panic 的值（已格式化成文本）
	KeyStack = "stack" // panic 发生时的调用栈
	KeyError = "err"   // 兜底点据此造出的错误文本（带着是哪一处兜住的；已有错误时是那个错误）
	// KeyHookIndex 是钩子链里第几个钩子（从 0 数），见 [CallEach]。
	KeyHookIndex = "hook_index"
)

// Error 记一次被兜住的 panic：panic 的值与调用栈。
//
// 它不实现 Unwrap（理由见包文档）：要 panic 的原值读 Value，要判断它是什么错误就对 Value 自己做
// errors.Is / errors.As。
type Error struct {
	// Value 是 panic 的原值。
	Value any
	// Stack 是兜住 panic 的那个 defer 里取的调用栈（debug.Stack 的结果），panic 发生处的栈帧都在。
	// 运行时打栈只留最内 50 帧和最外 50 帧，中间省略。
	Stack string
}

// Error 返回 "panic: <值>"，不含栈。
func (e *Error) Error() string { return fmt.Sprintf("panic: %v", e.Value) }

// New 取当前调用栈造一条记录。必须在兜住 panic 的那个 defer 里调（直接调或经它调的函数调都行）：那时
// panic 发生处的栈帧还没展开；defer 返回之后再取，就只剩调用方自己的栈了。
func New(v any) *Error {
	return &Error{Value: v, Stack: string(debug.Stack())}
}

// CallEach 依次调 call(0), …, call(n-1)，给钩子链的观测一侧用：一个钩子 panic 不该让排在它后面的钩子静默跳过。
//
// 某一个 panic 时，后面的在这个 panic 自己的 defer 里接着调，调完它照常往上走：调用点拿到的就是原来那个值，panic 现场的栈帧
// 也还在（调用点在 defer 里取栈看得到 panic 的那个钩子），跟没有这条链时一样。曾经先兜住、调完其余的再抛一个带栈的包装：
// 业务自己 recover 钩子链时拿到的是不导出的类型，r.(error)、errors.Is、== 都对不上，JSON 日志打成 {}（审计实测）。
//
// 排在后面的又 panic 时记一条带栈的日志（ctx 只用来记日志），交给调用点的仍是第一个。排在后面的调了 runtime.Goexit 时，第一个
// panic 照样交给调用点，之后 Goexit 照常结束这个 goroutine。某一个 Goexit 时后面的照样调到。
func CallEach(ctx context.Context, n int, call func(i int)) {
	i := 0
	defer func() {
		if i >= n {
			return
		}
		r := recover() // nil：call(i) 调了 Goexit（或在 GODEBUG=panicnil=1 下 panic(nil)，同样当作没有 panic）
		rest := func() {
			for j := i + 1; j < n; j++ {
				callLogged(ctx, j, call)
			}
		}
		if r == nil {
			rest()
			return
		}
		// 在这个 defer 里原样再抛：panic 现场的栈帧此刻还没展开。放进 defer 是为了排在后面的 Goexit 时也照样抛出。
		defer panic(r)
		rest()
	}()
	for ; i < n; i++ {
		call(i)
	}
}

// callLogged 调 call(j)，它 panic 时记一条带栈的日志、到此为止（排在前面的已经 panic，交给调用点的是那一个）。
func callLogged(ctx context.Context, j int, call func(i int)) {
	defer func() {
		if r := recover(); r != nil {
			Log(ctx, slog.LevelError, "panicerr: 钩子链里排在前面的钩子已经 panic，这个钩子又 panic（只记日志，交给调用点的是前面那个）",
				New(r), KeyHookIndex, j)
		}
	}()
	call(j)
}

// Wrap 按「panic 值进链」的写法造兜底点返回的错误：文本是 "<prefix>: <panic 值>"，panic 值是 error 时
// 用 %w 接上（errors.Is / errors.As 认得出它），否则用 %v；再并列挂上 e。
func Wrap(prefix string, e *Error) error {
	if err, ok := e.Value.(error); ok {
		return Attach(fmt.Errorf("%s: %w", prefix, err), e)
	}
	return Attach(fmt.Errorf("%s: %v", prefix, e.Value), e)
}

// WrapOpaque 按「panic 值不进链」的写法造兜底点返回的错误：文本同 Wrap，panic 值只取文本（%v），
// errors.Is / errors.As 认不出它。
func WrapOpaque(prefix string, e *Error) error {
	return Attach(fmt.Errorf("%s: %v", prefix, e.Value), e)
}

// Attach 把 e 并列挂到 err 上：Error() 与 err 相同，errors.Is / errors.As 照旧沿 err 走，另外多一条
// 分支能 errors.As 到 e。
//
// 返回的错误用多分支的 Unwrap() []error，标准库单层的 errors.Unwrap 对它返回 nil——判断用
// errors.Is / errors.As。
func Attach(err error, e *Error) error {
	return &attached{err: err, rec: e}
}

// WrapPending 同 Wrap，另外标记「日志待补记」：兜底点在框架的持锁区里，当场不能记日志——日志 handler
// 是业务代码，回调进来会撞上不可重入的锁。收到这个错误的一方在锁外调 LogPending 补记。
func WrapPending(prefix string, e *Error) error {
	a := Wrap(prefix, e).(*attached)
	a.pending.Store(true)
	return a
}

// LogPending 给 err 这棵错误树里每条「日志待补记」的记录补记一条 ERROR 日志。每条只补记一次：
// 补记过的不再记，并发调用也一样。
func LogPending(ctx context.Context, msg string, err error) {
	walk(err, func(x error) {
		if a, ok := x.(*attached); ok && a.pending.CompareAndSwap(true, false) {
			Log(ctx, slog.LevelError, msg, a.rec, KeyError, a.err.Error())
		}
	})
}

// Log 记一条带 panic 值与栈的日志。msg 说清是哪一处兜住的；args 是定位用的键值对（slog 的写法），
// 本函数在后面补上 panic 值与栈。
//
// 它跑在兜住 panic 的那条路径上，而那条路径往往在记日志之后才记下结论（成员的结果、Run 的错误、
// OnAgentEnd）。日志 handler 和 panic 值的 Error / String 方法都是业务代码，它们 panic 或调 runtime.Goexit
// （测试里「一有错误日志就 t.FailNow」的 handler 就会）时不能掐断那条路径——recover 拦不住 Goexit，被掐断
// 的话结论就没记下：fan-out 成员的失败变成幽灵成功、图主体的 panic 被当成功、存档被清掉。所以格式化和调
// handler 都放在这里起的一个 goroutine 里，等它结束才返回：panic 由它自己兜住、这条日志丢弃，Goexit 只结束
// 它自己。只有兜住 panic 时才走到这里，多起一个 goroutine 的开销可以忽略。
//
// panic 值先用 fmt 格式化成文本再交给 handler：fmt 会兜住值的方法里的 panic（比如 panic 了一个 nil 指针
// error），这条日志照常写出来。标准库的 handler 写值时自己也兜，业务自研或第三方的不一定。
func Log(ctx context.Context, level slog.Level, msg string, e *Error, args ...any) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			// 格式化或 handler panic 时这条日志只能丢弃：没有别的地方可报了
			_ = recover()
		}()
		all := make([]any, 0, len(args)+4)
		all = append(all, args...)
		all = append(all, KeyPanic, fmt.Sprintf("%v", e.Value), KeyStack, e.Stack)
		slog.Log(ctx, level, msg, all...)
	}()
	<-done
}

// attached 是兜底点返回的错误：原来那个错误加上并列挂着的 panic 记录。
type attached struct {
	err     error       // 兜底点原来造的错误：文本与 errors.Is / errors.As 的语义都由它定
	rec     *Error      // panic 的值与栈
	pending atomic.Bool // 日志待补记（见 WrapPending）
}

func (a *attached) Error() string   { return a.err.Error() }
func (a *attached) Unwrap() []error { return []error{a.err, a.rec} }

// walk 按 errors.Is 的遍历方式（单链 Unwrap() error 与多分支 Unwrap() []error）走遍 err 这棵树。
func walk(err error, visit func(error)) {
	for err != nil {
		visit(err)
		switch u := err.(type) {
		case interface{ Unwrap() []error }:
			for _, e := range u.Unwrap() {
				walk(e, visit)
			}
			return
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		default:
			return
		}
	}
}
