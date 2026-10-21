package compose_v2_test

import (
	"strings"
	"sync"
	"testing"

	"crew-go/compose_v2"
)

// 黑盒测试：覆盖公开 API（reducer 工厂 / Register / Get / Has / Write / ApplyAll）。
// 内部分支（类型 cast panic 信息等）走 channel_internal_test.go 白盒测试。

// ── 基础 reducer 工厂 ──

func TestLastValue_Overwrite(t *testing.T) {
	ch := compose_v2.LastValue[int]("counter")
	cm := compose_v2.NewChannelManager()
	mustRegister(t, cm, ch)

	v, ok := compose_v2.Get(cm.Channels(), ch)
	if ok || v != 0 {
		t.Fatalf("未写入应返 (0, false)，实际 (%d, %v)", v, ok)
	}

	mustApply(t, cm, compose_v2.Write(ch, 1), compose_v2.Write(ch, 42))
	if v := compose_v2.MustGet(cm.Channels(), ch); v != 42 {
		t.Fatalf("LastValue 覆盖后应是 42，实际 %d", v)
	}
}

func TestSum_Aggregation(t *testing.T) {
	ch := compose_v2.Sum("token_usage")
	cm := compose_v2.NewChannelManager()
	mustRegister(t, cm, ch)

	mustApply(t, cm,
		compose_v2.Write(ch, 100),
		compose_v2.Write(ch, 250),
		compose_v2.Write(ch, 50),
	)
	if v := compose_v2.MustGet(cm.Channels(), ch); v != 400 {
		t.Fatalf("Sum 累加应是 400，实际 %d", v)
	}
}

func TestAppend_Aggregation(t *testing.T) {
	ch := compose_v2.Append[string]("messages")
	cm := compose_v2.NewChannelManager()
	mustRegister(t, cm, ch)

	mustApply(t, cm,
		compose_v2.Write(ch, []string{"a", "b"}),
		compose_v2.Write(ch, []string{"c"}),
	)
	v := compose_v2.MustGet(cm.Channels(), ch)
	if len(v) != 3 || v[0] != "a" || v[2] != "c" {
		t.Fatalf("Append 应是 [a b c]，实际 %v", v)
	}
}

// ── audit F1: OnceFlag 单调性 ──

// TestOnceFlag_MonotonicLock 验证"一旦 set 即锁定"——撞 audit F1 EndChannel
// 用 LastValue[bool] 漏洞（多分支并发写时 false 能撤销 true）。
//
// 修复前用 LastValue：本测试 fail（最终 v=false）。
// 修复后用 OnceFlag：本测试 pass（最终 v=true）。
func TestOnceFlag_MonotonicLock(t *testing.T) {
	ch := compose_v2.OnceFlag("__end__")
	cm := compose_v2.NewChannelManager()
	mustRegister(t, cm, ch)

	mustApply(t, cm,
		compose_v2.Write(ch, true),  // false → true: lock
		compose_v2.Write(ch, false), // true → ignored
		compose_v2.Write(ch, true),  // 仍 true
		compose_v2.Write(ch, false), // 仍 ignored
	)
	if v := compose_v2.MustGet(cm.Channels(), ch); !v {
		t.Fatalf("OnceFlag 写入 true 后应永远 true，实际 %v", v)
	}
}

func TestOnceFlag_FalseDoesNotLock(t *testing.T) {
	ch := compose_v2.OnceFlag("__flag__")
	cm := compose_v2.NewChannelManager()
	mustRegister(t, cm, ch)

	mustApply(t, cm,
		compose_v2.Write(ch, false), // 起点 false
		compose_v2.Write(ch, true),  // 可升 true
	)
	if v := compose_v2.MustGet(cm.Channels(), ch); !v {
		t.Fatalf("OnceFlag false→true 升级应成功，实际 %v", v)
	}
}

// ── audit F4: 重复注册同名异类型 fail-fast ──

func TestRegister_DuplicateNameSameType_Idempotent(t *testing.T) {
	ch1 := compose_v2.Sum("counter")
	ch2 := compose_v2.Sum("counter")
	cm := compose_v2.NewChannelManager()

	mustRegister(t, cm, ch1)
	if err := compose_v2.RegisterChannel(cm, ch2); err != nil {
		t.Fatalf("同名同类型注册应幂等，实际 %v", err)
	}
}

func TestRegister_DuplicateNameDifferentType_Error(t *testing.T) {
	chInt := compose_v2.Sum("conflict")
	chStr := compose_v2.LastValue[string]("conflict")
	cm := compose_v2.NewChannelManager()

	mustRegister(t, cm, chInt)
	err := compose_v2.RegisterChannel(cm, chStr)
	if err == nil {
		t.Fatal("同名异类型注册应返 error")
	}
	if !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("error 应含 channel 名 conflict，实际 %q", err.Error())
	}
}

// ── audit F9: Write 编译期类型保护（编译过即证明）──

// TestWrite_TypeSafety 是 audit F9 验证：Write[T] 必须编译期保证 value 与
// channel T 一致。本测试编译过即证明类型保护——错配版本在注释里展示
// （取消注释会编译失败）。
func TestWrite_TypeSafety(t *testing.T) {
	intCh := compose_v2.Sum("ints")
	strCh := compose_v2.LastValue[string]("strs")
	msgCh := compose_v2.Append[string]("msgs")

	cm := compose_v2.NewChannelManager()
	mustRegister(t, cm, intCh)
	mustRegister(t, cm, strCh)
	mustRegister(t, cm, msgCh)

	// 编译期类型保护：每条 Write 第二参数必须匹配 channel T
	mustApply(t, cm,
		compose_v2.Write(intCh, 42),                 // *Channel[int] + int  ✓
		compose_v2.Write(strCh, "hello"),            // *Channel[string] + string  ✓
		compose_v2.Write(msgCh, []string{"a", "b"}), // *Channel[[]string] + []string  ✓
	)

	// 取消注释以下任一行会编译失败（cannot use ... as ...）：
	// _ = compose_v2.Write(intCh, "wrong") // ✗ string 给 *Channel[int]
	// _ = compose_v2.Write(strCh, 42)      // ✗ int 给 *Channel[string]
	// _ = compose_v2.Write(msgCh, "x")     // ✗ string 给 *Channel[[]string]

	if v := compose_v2.MustGet(cm.Channels(), intCh); v != 42 {
		t.Fatalf("intCh 应 42 实际 %d", v)
	}
	if v := compose_v2.MustGet(cm.Channels(), strCh); v != "hello" {
		t.Fatalf("strCh 应 'hello' 实际 %q", v)
	}
	if v := compose_v2.MustGet(cm.Channels(), msgCh); len(v) != 2 || v[0] != "a" {
		t.Fatalf("msgCh 应 [a b] 实际 %v", v)
	}
}

// ── 异构 fan-in 验证（多类型 channel 并发写）──

func TestHeterogeneousFanIn(t *testing.T) {
	intCh := compose_v2.Sum("count")
	strCh := compose_v2.LastValue[string]("name")
	msgCh := compose_v2.Append[string]("trace")

	cm := compose_v2.NewChannelManager()
	mustRegister(t, cm, intCh)
	mustRegister(t, cm, strCh)
	mustRegister(t, cm, msgCh)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		mustApply(t, cm, compose_v2.Write(intCh, 1), compose_v2.Write(msgCh, []string{"branch_1"}))
	}()
	go func() {
		defer wg.Done()
		mustApply(t, cm, compose_v2.Write(intCh, 2), compose_v2.Write(strCh, "second"))
	}()
	go func() {
		defer wg.Done()
		mustApply(t, cm, compose_v2.Write(intCh, 3), compose_v2.Write(msgCh, []string{"branch_3"}))
	}()
	wg.Wait()

	if v := compose_v2.MustGet(cm.Channels(), intCh); v != 6 {
		t.Fatalf("Sum 累加 1+2+3=6，实际 %d", v)
	}
	if v := compose_v2.MustGet(cm.Channels(), strCh); v != "second" {
		t.Fatalf("LastValue 应 'second' 实际 %q", v)
	}
	if v := compose_v2.MustGet(cm.Channels(), msgCh); len(v) != 2 {
		t.Fatalf("Append 应 2 条实际 %d (%v)", len(v), v)
	}
}

// ── 测试 helper（黑盒 package _test 可见）──

func mustRegister[T any](t *testing.T, cm *compose_v2.ChannelManager, ch *compose_v2.Channel[T]) {
	t.Helper()
	if err := compose_v2.RegisterChannel(cm, ch); err != nil {
		t.Fatalf("RegisterChannel(%q): %v", ch.Name(), err)
	}
}

func mustApply(t *testing.T, cm *compose_v2.ChannelManager, ws ...compose_v2.ChannelWrite) {
	t.Helper()
	if err := cm.ApplyAll(ws); err != nil {
		t.Fatalf("ApplyAll: %v", err)
	}
}
