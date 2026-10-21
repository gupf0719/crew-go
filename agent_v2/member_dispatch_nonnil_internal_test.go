package agent_v2

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// runMember 在 err==nil 时**恒返非 nil**——这条不变量有三处代码依赖它。
//
// PlanAgent 的三个 worker 原先各有一段 `if out == nil { return "", nil }`。收敛到
// runMember 之后那三段成了死代码（成功路径合成空 assistant，失败路径被上一行的
// `if err != nil` 接住），于是删掉了——**留着比删掉更糟：它让读的人以为 runMember
// 成功路径可能返 nil。**
//
// 但「不可达」当时是**读代码推出来的**。本用例把它压成测出来的：四种成员行为
// （零 events / 只发 Output 不发 Exit / 返业务错 / 抛中断）逐个跑，确认
// **「err==nil 且 out==nil」那一格不存在**。哪天有人改了 runMember 的返回约定
// （比如把那个空 assistant 合成拿掉），这条会红，而那三处删除的前提正好就没了。
type dbAgent struct {
	name string
	mode string // "empty"=零 events, "nooutput"=只发非 Exit event, "err"=返错, "interrupt"=中断
}

func (a *dbAgent) Name() string        { return a.name }
func (a *dbAgent) Description() string { return "probe" }
func (a *dbAgent) Resume(ctx context.Context, sid string, o ...ResumeOption) ([]*AgentEvent, error) {
	return nil, nil
}
func (a *dbAgent) Run(ctx context.Context, s *RunSession, in *schema.Message, o ...RunOption) ([]*AgentEvent, error) {
	switch a.mode {
	case "empty":
		return nil, nil
	case "nooutput":
		return []*AgentEvent{NewOutputEvent(a.name, schema.AssistantMessage("我说了话但没标 Exit"))}, nil
	case "err":
		return nil, errors.New("业务错")
	case "interrupt":
		return nil, &compose_v2.InterruptError{
			Reason: "ask",
			Query:  &compose_v2.AskQuery{InterruptID: "id1", Question: "?"},
		}
	}
	return nil, nil
}

func TestDeadBranch_RunMemberNeverReturnsNilOnSuccess(t *testing.T) {
	for _, mode := range []string{"empty", "nooutput", "err", "interrupt"} {
		t.Run(mode, func(t *testing.T) {
			out, err := runMember(context.Background(), memberGrants{}, memberSpec{
				agent: &dbAgent{name: "p", mode: mode}, sess: NewRunSession(),
				input: schema.UserMessage("x"), nsSuffix: "n", historySlot: "s",
			})
			t.Logf("mode=%s → out==nil? %v   err=%v", mode, out == nil, err)
			// 被删的那三处形如 `if out == nil { return "", nil }`，前面已有
			// `if err != nil { return "", err }`。所以只有「err==nil 且 out==nil」
			// 这一格是它曾经覆盖、而现在没人管的。
			if err == nil && out == nil {
				t.Errorf("mode=%s 命中被删的那格：err==nil 但 out==nil——"+
					"删掉 `if out == nil` 之后调用方会对 nil 解引用", mode)
			}
		})
	}
}

// nil 成员必须 fail-fast 返 error，不能裸 panic——收敛时漏了这道守卫，
// typed nil 会在**错误路径**上炸（`m.agent.Run` 自己的 recover 把 panic 转成 error，
// 控制流进 err 分支，然后 `m.agent.Name()` 在 runMember 内部裸 panic）。
func TestDeadBranch_NilMemberReturnsErrNotNilPair(t *testing.T) {
	out, err := runMember(context.Background(), memberGrants{}, memberSpec{
		agent: nil, sess: NewRunSession(), input: schema.UserMessage("x"),
		nsSuffix: "n", historySlot: "s",
	})
	t.Logf("nil 成员 → out==nil? %v  err=%v", out == nil, err)
	if err == nil {
		t.Fatal("nil 成员必须返 error")
	}
}
