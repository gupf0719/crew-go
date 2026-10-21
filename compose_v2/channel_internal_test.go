package compose_v2

import (
	"strings"
	"testing"
)

// 白盒测试：访问 ChannelManager 内部 channels map 验证 audit F4
// "dynamic channel 类型错可观测性" —— error 信息必须含 channel 名 + 期望/实际类型，
// 让开发者能从 error 直接定位 bug 位置（不依赖 stack trace）。

// TestApplyOne_TypeMismatch_PanicMessage 验证 reduce 内 type cast 失败时的 panic
// 信息包含足够诊断信息——撞 audit F4。
//
// 验 reducer 撞类型不匹配时**裸 panic 的信息质量**（channel 名 + 期望类型 + 实际
// 类型三样都在）——这是排错时唯一能指路的东西。ApplyAll 会把它 recover 成 error
// 交给 caller（见 TestApplyAll_RecoversTypeMismatch），但信息是从这里来的。
//
// 白盒直打 reduceAny：它就是被验的那一层，中间隔一个 helper 只会让「谁 panic 的」
// 变模糊。
func TestReduceAny_TypeMismatch_PanicMessage(t *testing.T) {
	intCh := Sum("metric_x")
	cm := NewChannelManager()
	if err := RegisterChannel(cm, intCh); err != nil {
		t.Fatalf("register: %v", err)
	}
	h, ok := cm.channels["metric_x"]
	if !ok {
		t.Fatal("fixture 没搭对：channel 没注册进 cm")
	}

	// 故意类型错：prev=10（走 prev cast 分支）、next 塞 string 给 int channel
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("类型不匹配应 panic，实际未 panic")
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("panic 值应是 error，实际 %T: %v", r, r)
		}
		msg := err.Error()
		if !strings.Contains(msg, "metric_x") {
			t.Fatalf("panic 信息应含 channel 名 metric_x，实际 %q", msg)
		}
		if !strings.Contains(msg, "int") {
			t.Fatalf("panic 信息应含期望类型 int，实际 %q", msg)
		}
		if !strings.Contains(msg, "string") {
			t.Fatalf("panic 信息应含实际类型 string，实际 %q", msg)
		}
	}()
	_ = h.reduceAny(10, "bad value")
}

// TestApplyAll_RecoversTypeMismatch 验证 ApplyAll 路径下 panic 被 defer recover
// 转成 error——撞 audit F4 的"dynamic channel 类型错可观测性"。
//
// 业务视角：用户可能在动态注册 channel 后误传错类型，框架不应该 crash 整个
// graph，而是返 error 让 caller 决定怎么处理（log / retry / abort）。
func TestApplyAll_RecoversTypeMismatch(t *testing.T) {
	intCh := Sum("counter")
	cm := NewChannelManager()
	if err := RegisterChannel(cm, intCh); err != nil {
		t.Fatalf("register: %v", err)
	}

	// 用 typedWrite[T] 内部构造类型错的 ChannelWrite
	bad := &channelWrite[string]{
		ch:  &Channel[string]{n: "counter"}, // 注意：这个 ch 没注册，但 name 与 intCh 撞
		val: "bad",
	}

	err := cm.ApplyAll([]ChannelWrite{bad})
	if err == nil {
		t.Fatal("类型不匹配应被 ApplyAll 转为 error，实际 nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "counter") {
		t.Fatalf("error 应含 channel 名，实际 %q", msg)
	}
}

// TestRegister_TypeMatchExact 验证注册类型相等检查（ptr 相等）。
// reflect.TypeOf 对相同 underlying type 的多次调用返回相同 ptr——这是 Go 反射
// 保证。基于此 typesEqual 用 == 比较即可，不需要 .Kind() 深比较。
func TestRegister_TypeMatchExact(t *testing.T) {
	cm := NewChannelManager()
	chA := Sum("c1")
	chB := Sum("c1") // 同 type[int]
	if err := RegisterChannel(cm, chA); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := RegisterChannel(cm, chB); err != nil {
		t.Fatalf("second (same type): %v", err)
	}
	// 同 type[int] 但不同 reducer (Sum vs LastValue)——当前未拦，仅类型
	chC := LastValue[int]("c1") // 同 T=int 但 reducer 不同
	if err := RegisterChannel(cm, chC); err != nil {
		t.Fatalf("third (same T, different reducer) PoC 阶段不拦：%v", err)
	}
	// 已知限制：reducer 不同但 type 相同时第二次注册被忽略，原 reducer 留用
	// 这是 PoC 已知行为；生产版本应在 register 时也比较 reducer 函数 ptr
	// 或要求用户明确 unregister 再 register
}
