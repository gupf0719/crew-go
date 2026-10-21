package compose_v2

import (
	"errors"
	"testing"
)

// TestExitErr_NotResumableNotSwallowedBySiblingInterrupt 钉住**坏存档不被兄弟的中断吞掉**。
//
// 场景：同超步「分支甲撞上自相矛盾的存档 + 分支乙正常中断」。仲裁若按「中断优先」返中断，
// 存档问题这一轮**完全不可见**。
//
// ⚠️ 为什么这比「报中断」更对：那个提问**注定答不成**（存档里没有可执行的东西），用户答完
// 只会再撞同一份坏档。而 a2a server 从**返回值**算 `keepPending`——报中断就把任务挂成
// `input-required`、把问题发给远端，等一个不可能成功的续发。
//
// ⚠️ 断言两个方向都要有：坏档在时报坏档，**坏档不在时中断照常胜出**（否则就是把中断也
// 一起压掉了）。
func TestExitErr_NotResumableNotSwallowedBySiblingInterrupt(t *testing.T) {
	bad := errors.New("branch a: " + ErrCheckpointNotResumable.Error())
	badWrapped := errors.Join(bad, ErrCheckpointNotResumable)
	ie := &InterruptError{Reason: "hitl", Query: &AskQuery{InterruptID: "q1", Question: "确认吗"}}

	// ① 坏档 + 兄弟中断 ⟹ 报坏档
	r := &frontierResult{
		FirstErr:        badWrapped,
		NotResumableErr: badWrapped,
		AllInterrupts:   []*InterruptError{ie},
		interruptRaws:   []error{ie},
	}
	got := r.exitErr()
	if !errors.Is(got, ErrCheckpointNotResumable) {
		t.Errorf("坏档被兄弟的中断吞掉了——消费方据此把任务挂成 input-required，"+
			"等一个注定再撞同一份坏档的续发。实际 %v", got)
	}

	// ② 只有中断（没有坏档）⟹ 中断照常胜出
	r2 := &frontierResult{
		FirstErr:      ie,
		AllInterrupts: []*InterruptError{ie},
		interruptRaws: []error{ie},
	}
	if _, isInt := IsInterrupt(r2.exitErr()); !isInt {
		t.Errorf("没有坏档时中断该照常胜出，实际 %v", r2.exitErr())
	}

	// ③ 终局失败仍排在最前（它要驱动清档）
	abort := errors.New("aborted: " + ErrRunAborted.Error())
	r3 := &frontierResult{
		AbortErr:        errors.Join(abort, ErrRunAborted),
		NotResumableErr: badWrapped,
		FirstErr:        badWrapped,
	}
	if !errors.Is(r3.exitErr(), ErrRunAborted) {
		t.Errorf("终局失败该排在最前（它驱动 cp 清理），实际 %v", r3.exitErr())
	}
}

// TestPickFanoutError_NotResumableAlsoBeatsInterrupt 钉住**编排层的仲裁器有同一档**。
//
// ⚠️ 这条是审计抓到的：框架里回答「一批并行子调用同时出错报哪个」的仲裁器有**两个**
// ——引擎层的 `exitErr` 和编排层的 `PickFanoutError`（生产调用点是 BasicAgent 的 toolsFn
// 扇出 + 记忆分片）。我第一次只改了引擎那个，**同样的伤害在 toolsFn 那条路上原样发生**，
// 而且被端到端复现了：工具甲返坏档 + 工具乙 `Ask` ⟹ Run 返中断 ⟹ 用户答了 ⟹ Resume
// 才撞上坏档，那一问白问。
//
// 这就是「立一条新规矩，同一次就要问『这个东西还能从哪儿冒出来』」——判据长在其中一个
// 仲裁器上，另一个照常放行。
func TestPickFanoutError_NotResumableAlsoBeatsInterrupt(t *testing.T) {
	bad := errors.Join(errors.New("branch a"), ErrCheckpointNotResumable)
	ie := &InterruptError{Reason: "hitl", Query: &AskQuery{InterruptID: "q1", Question: "批准吗"}}
	abort := errors.Join(errors.New("branch c"), ErrRunAborted)

	// ① 坏档 + 中断 ⟹ 报坏档（两种下标序都要成立）
	for _, errs := range [][]error{{bad, ie}, {ie, bad}} {
		if got := PickFanoutError(errs); !errors.Is(got, ErrCheckpointNotResumable) {
			t.Errorf("坏档被兄弟的中断吞掉了（下标序 %T,%T）：%v", errs[0], errs[1], got)
		}
	}
	// ② 终局失败仍严格排在坏档之前（它捆绑清档，是更终局的处置）
	for _, errs := range [][]error{{bad, abort}, {abort, bad}} {
		if got := PickFanoutError(errs); !errors.Is(got, ErrRunAborted) {
			t.Errorf("终局失败该排在坏档之前（下标序变了就翻）：%v", got)
		}
	}
	// ③ 没有坏档时中断照常胜出
	if _, isInt := IsInterrupt(PickFanoutError([]error{errors.New("plain"), ie})); !isInt {
		t.Errorf("没有坏档时中断该照常胜出")
	}
}
