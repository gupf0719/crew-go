package panicerr_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"

	"crew-go/internal/panicerr"
)

var errSentinel = errors.New("sentinel boom")

// explodeHere 是 panic 发生处：断言栈里有它的函数名。不让它内联，免得栈里的名字随编译器决定变。
//
//go:noinline
func explodeHere(v any) { panic(v) }

// recoverInto 模拟一个兜底点：在 recover 所在的 defer 里取栈。
func recoverInto(v any) (pe *panicerr.Error) {
	defer func() {
		if r := recover(); r != nil {
			pe = panicerr.New(r)
		}
	}()
	explodeHere(v)
	return nil
}

func TestNew_StackHasPanicSite(t *testing.T) {
	pe := recoverInto("boom")
	if pe == nil {
		t.Fatal("没兜住 panic")
	}
	if !strings.Contains(pe.Stack, "panicerr_test.explodeHere") {
		t.Fatalf("栈里没有 panic 发生处的函数名 explodeHere：\n%s", pe.Stack)
	}
	if pe.Value != "boom" {
		t.Errorf("Value = %v，想要 panic 的原值 boom", pe.Value)
	}
	if strings.Contains(pe.Error(), "goroutine") {
		t.Errorf("Error() 不该带栈：%q", pe.Error())
	}
}

func TestWrap_ErrorValueStaysInChain(t *testing.T) {
	pe := recoverInto(errSentinel)
	err := panicerr.Wrap("node panic recovered", pe)

	if want := fmt.Errorf("node panic recovered: %w", errSentinel).Error(); err.Error() != want {
		t.Errorf("文本 = %q，想要与原写法逐字相同 %q", err.Error(), want)
	}
	if !errors.Is(err, errSentinel) {
		t.Error("Wrap 是「panic 值进链」的写法：errors.Is 该认得出 panic 值")
	}
	var got *panicerr.Error
	if !errors.As(err, &got) {
		t.Fatal("errors.As 取不到并列挂着的 *panicerr.Error")
	}
	if got != pe || !strings.Contains(got.Stack, "panicerr_test.explodeHere") {
		t.Errorf("取到的记录不对：%+v", got)
	}
}

func TestWrap_NonErrorValue(t *testing.T) {
	err := panicerr.Wrap("node panic recovered", recoverInto(42))
	if err.Error() != "node panic recovered: 42" {
		t.Errorf("文本 = %q", err.Error())
	}
	var got *panicerr.Error
	if !errors.As(err, &got) || got.Value != 42 {
		t.Errorf("errors.As 取到 %+v", got)
	}
}

// TestWrapOpaque_ValueNotInChain 钉住「记录不改错误的判定」：原来用 %v 的兜底点，panic 值是 error 时
// errors.Is 认不出它——挂上记录之后也一样认不出（记录本身不露出 panic 值）。
func TestWrapOpaque_ValueNotInChain(t *testing.T) {
	pe := recoverInto(errSentinel)
	err := panicerr.WrapOpaque("OnNodeStart panic", pe)

	if want := fmt.Errorf("OnNodeStart panic: %v", errSentinel).Error(); err.Error() != want {
		t.Errorf("文本 = %q，想要 %q", err.Error(), want)
	}
	if errors.Is(err, errSentinel) {
		t.Error("WrapOpaque 是「panic 值不进链」的写法：errors.Is 不该认出 panic 值")
	}
	if errors.Is(pe, errSentinel) {
		t.Error("记录本身不该露出 panic 值（不实现 Unwrap）")
	}
	var got *panicerr.Error
	if !errors.As(err, &got) || got != pe {
		t.Error("errors.As 取不到并列挂着的记录")
	}
}

// TestLogPending_EachRecordOnce 钉住补记：错误树里每条待补记的记录各记一次，不待补记的不记，补记过的
// 再调也不记。
func TestLogPending_EachRecordOnce(t *testing.T) {
	logs := captureLogs(t)

	pendingA := panicerr.WrapPending("ApplyAll: reducer panic, no writes committed", recoverInto("a"))
	logged := panicerr.Wrap("node panic recovered", recoverInto("b"))
	pendingC := panicerr.WrapPending("Restore panic (rolled back)", recoverInto("c"))
	tree := errors.Join(pendingA, logged, fmt.Errorf("auto-restore: %w", pendingC))

	panicerr.LogPending(context.Background(), "compose_v2: 补记", tree)
	panicerr.LogPending(context.Background(), "compose_v2: 补记", tree)

	recs := logs.records()
	if len(recs) != 2 {
		t.Fatalf("想要 2 条补记（a、c 各一条），实际 %d 条：%v", len(recs), recs)
	}
	for i, wantPanic := range []string{"a", "c"} {
		if got := attr(recs[i], panicerr.KeyPanic); got != wantPanic {
			t.Errorf("第 %d 条的 panic = %q，想要 %q", i, got, wantPanic)
		}
		if !strings.Contains(attr(recs[i], panicerr.KeyStack), "panicerr_test.explodeHere") {
			t.Errorf("第 %d 条的栈里没有 panic 发生处", i)
		}
	}
	if !strings.Contains(attr(recs[0], panicerr.KeyError), "ApplyAll") {
		t.Errorf("补记该带上是哪一处兜住的：%q", attr(recs[0], panicerr.KeyError))
	}
}

// badErr 的 Error 方法会 panic（nil 接收者解引用）——panic 值就是它时，记日志不能再 panic 出去。
type badErr struct{ msg string }

func (e *badErr) Error() string { return e.msg }

// naiveHandler 写值时直接调 error 值的 Error()、不兜 panic——标准库的 handler 会兜，业务自研或第三方的不一定。
type naiveHandler struct{ buf *bytes.Buffer }

func (h naiveHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h naiveHandler) Handle(_ context.Context, rec slog.Record) error {
	rec.Attrs(func(a slog.Attr) bool {
		h.buf.WriteString(a.Key + "=")
		if err, ok := a.Value.Any().(error); ok {
			h.buf.WriteString(err.Error())
		} else {
			h.buf.WriteString(a.Value.String())
		}
		h.buf.WriteString(" ")
		return true
	})
	return nil
}
func (h naiveHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h naiveHandler) WithGroup(string) slog.Handler      { return h }

// TestLog_BadValueStillLogged：panic 值是 nil 指针 error（Error 方法会 panic）时，交给不兜 panic 的 handler，
// 这条日志也要照常写出来——值没先格式化的话，handler 调 Error() 时 panic，这条日志就被丢掉了。
func TestLog_BadValueStillLogged(t *testing.T) {
	var buf bytes.Buffer
	restore := setDefaultHandler(naiveHandler{buf: &buf})
	defer restore()

	var nilErr *badErr
	panicerr.Log(context.Background(), slog.LevelError, "x", recoverInto(nilErr))

	if !strings.Contains(buf.String(), panicerr.KeyStack+"=") {
		t.Fatalf("日志该照常写出来：%q", buf.String())
	}
}

type panickingHandler struct{ slog.Handler }

func (panickingHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (panickingHandler) Handle(context.Context, slog.Record) error { panic("handler boom") }

func TestLog_DoesNotPanicWhenHandlerPanics(t *testing.T) {
	restore := setDefaultHandler(panickingHandler{})
	defer restore()
	panicerr.Log(context.Background(), slog.LevelError, "x", recoverInto("v")) // 跑完不 panic 即通过
}

// goexitHandler 收到记录就调 runtime.Goexit——测试里「一有错误日志就 t.FailNow」的 handler 就是这个形状。
type goexitHandler struct{}

func (goexitHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (goexitHandler) Handle(context.Context, slog.Record) error { runtime.Goexit(); return nil }
func (h goexitHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h goexitHandler) WithGroup(string) slog.Handler           { return h }

// TestLog_HandlerGoexitDoesNotEscape：Log 跑在兜住 panic 的那条路径上、往往排在「记下结论」之前——handler 里
// 的 runtime.Goexit 要是掐断了调用方，后面记结论的语句就不执行了（fan-out 成员变幽灵、存档被当成功清掉）。
func TestLog_HandlerGoexitDoesNotEscape(t *testing.T) {
	restore := setDefaultHandler(goexitHandler{})
	defer restore()

	pe := recoverInto("v")
	done := make(chan struct{})
	after := false
	go func() {
		defer close(done)
		panicerr.Log(context.Background(), slog.LevelError, "x", pe)
		after = true
	}()
	<-done
	if !after {
		t.Fatal("handler 的 Goexit 掐断了调用方：Log 之后的语句没执行")
	}
}

// doublyBadErr 的 Error 方法 panic 出一个 Error 方法也会 panic 的值——fmt 兜住第一层之后，格式化第二层时
// 会把 panic 再抛出来。
type doublyBadErr struct{}

func (doublyBadErr) Error() string { panic(innerBadErr{}) }

type innerBadErr struct{}

func (innerBadErr) Error() string { panic("inner boom") }

func TestLog_ValueFormattingPanicDoesNotEscape(t *testing.T) {
	restore := setDefaultHandler(&logRecorder{})
	defer restore()
	panicerr.Log(context.Background(), slog.LevelError, "x", recoverInto(doublyBadErr{})) // 跑完不 panic 即通过
}

// ── 日志捕获 ──

type logRecorder struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (r *logRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *logRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	r.recs = append(r.recs, rec.Clone())
	r.mu.Unlock()
	return nil
}
func (r *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *logRecorder) WithGroup(string) slog.Handler      { return r }

func (r *logRecorder) records() []slog.Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]slog.Record(nil), r.recs...)
}

func captureLogs(t *testing.T) *logRecorder {
	t.Helper()
	rec := &logRecorder{}
	t.Cleanup(setDefaultHandler(rec))
	return rec
}

// setDefaultHandler 换掉 slog 的默认 handler，返回还原函数。log 包的输出也要一起还原：slog.SetDefault 换成
// 自定义 handler 时会把 log 包的输出改道过去，而换回原来的默认 logger 时不会改回来。
func setDefaultHandler(h slog.Handler) func() {
	oldDefault := slog.Default()
	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	slog.SetDefault(slog.New(h))
	return func() {
		slog.SetDefault(oldDefault)
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	}
}

func attr(rec slog.Record, key string) string {
	var v string
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v = a.Value.String()
			return false
		}
		return true
	})
	return v
}
