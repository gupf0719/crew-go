package compose_v2

import (
	"runtime"
	"testing"
)

// ApplyAll 的原子性：一批写要么全落地要么一条都不落地。
//
// 这条守的是**归并与提交分开**这个结构——归并（跑业务 reducer，会 panic / 会被
// runtime.Goexit 掐断）算出的新值全留在 staged 里，提交（纯赋值）在循环外一次做完。
// 把写回挪回循环里（两者混在一起）时本用例双双报红：pa/pb 已落地而 pc 没有，
// 正是「半批」的形态。
//
// ⚠️ 它**不是**在验某次修复的行为差异——改造前靠回滚栈 + Goexit 兜底旗两道修补
// 也能达到同样结果。它验的是那两道修补不再被需要：掐断时没有半应用可回滚。
//
// fixture：3 条写，前两条正常、第三条的 reducer 掐断。
func probeBatch(t *testing.T, kill func()) (afterA, afterB int, hadC bool) {
	t.Helper()
	chA, chB := Sum("pa"), Sum("pb")
	chC := &Channel[int]{n: "pc", fn: func(p, n int) int {
		kill() // panic 或 Goexit
		return n
	}, z: 0}
	cm := NewChannelManager()
	for _, r := range []func() error{
		func() error { return RegisterChannel(cm, chA) },
		func() error { return RegisterChannel(cm, chB) },
		func() error { return RegisterChannel(cm, chC) },
	} {
		if err := r(); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	// 先各写一次建立 prev，确认基线
	if err := cm.ApplyAll([]ChannelWrite{Write(chA, 1), Write(chB, 2)}); err != nil {
		t.Fatalf("setup: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cm.ApplyAll([]ChannelWrite{Write(chA, 100), Write(chB, 200), Write(chC, 300)})
	}()
	<-done

	cm.mu.RLock()
	defer cm.mu.RUnlock()
	afterA, _ = cm.values["pa"].(int)
	afterB, _ = cm.values["pb"].(int)
	_, hadC = cm.values["pc"]
	return
}

func TestApplyAll_Atomic_OnPanic(t *testing.T) {
	a, b, hadC := probeBatch(t, func() { panic("reducer boom") })
	t.Logf("panic 后: pa=%d pb=%d pc存在=%v", a, b, hadC)
	if a != 1 || b != 2 || hadC {
		t.Errorf("批次被掐断却有写入落地（pa=%d 期望 1, pb=%d 期望 2, pc存在=%v 期望 false）", a, b, hadC)
	}
}

func TestApplyAll_Atomic_OnGoexit(t *testing.T) {
	a, b, hadC := probeBatch(t, runtime.Goexit)
	t.Logf("Goexit 后: pa=%d pb=%d pc存在=%v", a, b, hadC)
	if a != 1 || b != 2 || hadC {
		t.Errorf("批次被掐断却有写入落地（pa=%d 期望 1, pb=%d 期望 2, pc存在=%v 期望 false）", a, b, hadC)
	}
}

// nil write 必须被跳过而不是炸掉。
//
// **这条以前零覆盖**：把 ApplyAll 里那句 `if w == nil { continue }` 整个删掉，
// `go test ./compose_v2/` 仍然全绿（审计实测）。而协议是依赖它的——
// serialization.go 里两处注释明写「对齐 ApplyAll 对 nil write 的 skip 语义」，
// 真实产出方是业务 NodeFunc 返回 []ChannelWrite{nil}。
func TestApplyAll_SkipsNilWrites(t *testing.T) {
	ch := Sum("nilskip")
	cm := NewChannelManager()
	if err := RegisterChannel(cm, ch); err != nil {
		t.Fatalf("register: %v", err)
	}
	// nil 夹在中间——两侧的写都必须落地
	if err := cm.ApplyAll([]ChannelWrite{Write(ch, 3), nil, Write(ch, 4)}); err != nil {
		t.Fatalf("nil write 该被跳过，实际返错: %v", err)
	}
	cm.mu.RLock()
	got, _ := cm.values["nilskip"].(int)
	cm.mu.RUnlock()
	if got != 7 {
		t.Errorf("nil 两侧的写没都落地：got %d 期望 7", got)
	}
	// 全 nil：不该报错，也不该建出 channel 值
	if err := cm.ApplyAll([]ChannelWrite{nil, nil}); err != nil {
		t.Errorf("全 nil 批次该是 no-op，实际返错: %v", err)
	}
}
