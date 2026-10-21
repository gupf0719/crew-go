// OnAgentEnd defer panic 兜底契约测试（白盒——用未导出 recoverToErr）。
//
// ────────────────────────────────────────────────────────────────────────
// 修的 gap：9 编排 Run/Resume 的两个 defer——开头 `defer recoverToErr`(最早注册
// → LIFO 最后跑)把 panic 转 retErr;OnAgentEnd defer(后注册 → 先跑)读 retErr 记
// trace。panic 时 OnAgentEnd defer 先跑,此刻 recoverToErr 还没跑、retErr 仍 nil →
// OnAgentEnd 误以为"成功"、trace 漏 error.message(对照 eino runner 同款缺陷 + 比
// LangGraph `except: on_chain_error(e); raise` 同块模式差一截)。
//
// 修法(对齐 LangGraph 同块): OnAgentEnd defer 闭包**第一行**先 recoverToErr 设
// retErr,再 WithEndError + OnAgentEnd → hook 看到 panic 转的真错误。开头
// recoverToErr 后跑时 panic 已被消费 → recover() 返 nil no-op。
//
// **触发面**(R3 audit 修正早期"业务无法触发"误判): gap 主要由框架自身 wrapper 代码
// (buildComposeGraph / collectEvents / 预检 cper.LoadNS / compose_v2.Get, 均
// compose_v2.Run recover 之外)panic 暴露; **业务侧也有窄触发面**——业务自实现
// Checkpointer.LoadNS(BasicAgent runGraph 预检) + GraphAgent inputWriter/outputReader
// (业务必填回调)panic 时同样直达 OnAgentEnd defer(runGraph 无自己的 recover)。仅
// model/tool/hook/NodeFunc panic 才走 graph 引擎或 runOne recover 转 err 的正常路径。
// 本测试**模拟 9 编排 defer 结构**验证修法模式(BasicAgent 框架自身 panic 端到端难注入,
// 业务 panic Checkpointer/inputWriter 可端到端触发——可另加 e2e 测坐实); 9 编排真实修改
// 靠 build + 全回归 + diff review 保证。
// ────────────────────────────────────────────────────────────────────────
package agent_v2

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

// TestOnAgentEndDefer_SeesWrapperPanic 验证修法核心契约：OnAgentEnd defer 闭包内
// 先 recoverToErr,能让"读 retErr 的收尾回调"看到 wrapper 内 panic 转的 err。
// sanity-revert：注释掉闭包内 recoverToErr → seenByEnd 为 nil → 本测试 FAIL,精确
// 指向"OnAgentEnd 看不到 panic"。
func TestOnAgentEndDefer_SeesWrapperPanic(t *testing.T) {
	ctx := context.Background()
	var seenByEnd error // 模拟 OnAgentEnd hook(经 WithEndError)读到的 retErr

	// 复刻 9 编排 Run/Resume 的 defer 结构 + wrapper 内框架 panic。
	captured := func() (retErr error) {
		defer recoverToErr(ctx, "Test.Run", &retErr) // 开头兜底(最早注册→最后跑)
		defer func() {                               // OnAgentEnd defer(后注册→先跑)
			recoverPanicToErr(ctx, "Test.OnAgentEnd", recover(), &retErr) // 修法：闭包内直接 recover() 设 retErr
			seenByEnd = retErr                                            // 模拟 OnAgentEnd 读 retErr
		}()
		panic(errors.New("buildComposeGraph boom")) // 模拟 wrapper 内框架自身 panic
	}
	finalErr := captured()

	// 核心断言：OnAgentEnd 必须看到 panic 转的 err(修法生效)。
	if seenByEnd == nil {
		t.Fatal("OnAgentEnd defer 未看到 wrapper panic 转的 err — 修法失效(retErr=nil 误报成功)")
	}
	if !strings.Contains(seenByEnd.Error(), "boom") {
		t.Errorf("OnAgentEnd 看到的 err 不含 panic 内容: %v", seenByEnd)
	}
	// 函数最终也返回该 err(panic 被兜,业务收到,不 crash)。
	if finalErr == nil {
		t.Error("函数应返回 panic 转的 err(不 crash)")
	}
	// 开头 recoverToErr 后跑时 panic 已被闭包内消费 → 不重复 wrap(验证 no-op)。
	if strings.Count(finalErr.Error(), "panic recovered") != 1 {
		t.Errorf("panic 应只被 wrap 一次(闭包内先消费,开头 no-op), got: %v", finalErr)
	}
}

//go:noinline
func explodeForRecoverHelpers() { panic(errors.New("wrapper boom")) }

// TestRecoverHelpers_AttachStack：两个编排兜底 helper 都把 panic 发生处的栈挂到错误上（compose_v2.PanicError）。
// 栈要在 recover 所在的 defer 里取——挪到 defer 返回之后取，栈里就没有 explodeForRecoverHelpers 了。
func TestRecoverHelpers_AttachStack(t *testing.T) {
	ctx := context.Background()
	cases := map[string]func() error{
		"recoverToErr": func() (retErr error) {
			defer recoverToErr(ctx, "Test.Run", &retErr)
			explodeForRecoverHelpers()
			return nil
		},
		"recoverPanicToErr": func() (retErr error) {
			defer func() { recoverPanicToErr(ctx, "Test.OnAgentEnd", recover(), &retErr) }()
			explodeForRecoverHelpers()
			return nil
		},
	}
	for name, run := range cases {
		err := run()
		var pe *compose_v2.PanicError
		if !errors.As(err, &pe) {
			t.Errorf("%s：errors.As 取不到 PanicError：%v", name, err)
			continue
		}
		if !strings.Contains(pe.Stack, "agent_v2.explodeForRecoverHelpers") {
			t.Errorf("%s：栈里没有 panic 发生处：\n%s", name, pe.Stack)
		}
		if strings.Contains(err.Error(), "goroutine ") {
			t.Errorf("%s：错误文本不该带栈：%q", name, err.Error())
		}
	}
}

// nilDerefErr 的 Error 方法解引用接收者——typed-nil 时会 panic。
type nilDerefErr struct{ msg string }

func (e *nilDerefErr) Error() string { return e.msg }

// TestRecoverPanicToErr_ExistingErrorFormattingDoesNotEscape：*retErr 已经有错误时这次 panic 只记日志、不覆盖
// 它。记日志时不能在调用处先求值已有错误的 Error()——它是 typed-nil 时会 panic，新 panic 逃出收尾闭包，
// 后面的 OnAgentEnd 就不调了。
func TestRecoverPanicToErr_ExistingErrorFormattingDoesNotEscape(t *testing.T) {
	var existing *nilDerefErr
	retErr := error(existing)
	escaped := func() (r any) {
		defer func() { r = recover() }()
		recoverPanicToErr(context.Background(), "Test.OnAgentEnd", "boom", &retErr)
		return nil
	}()
	if escaped != nil {
		t.Fatalf("记日志时 panic 逃了出来：%v", escaped)
	}
	if retErr != error(existing) {
		t.Errorf("已有错误不该被覆盖，实际 %v", retErr)
	}
}
