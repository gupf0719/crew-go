package agent_v2

import (
	"context"
	"strings"
	"testing"

	"crew-go/schema"
)

// runMember 对 typed nil 成员必须 fail-fast，不能在错误路径上裸 panic。
//
// 复现（独立审计实测的调用栈）：`m.agent.Run` 自己的 recover 把 panic 转成 error ⟹
// 控制流进 err 分支 ⟹ `m.agent.Name()` 在 runMember 内部炸，而这一层没有 recover。
// 姊妹 helper（嵌套调用的执行器 runNestedCall，原名 runSubAgent）一直有这道守卫，runMember 收敛时漏了。
func TestRunMember_TypedNilMemberFailsFast(t *testing.T) {
	for _, tc := range []struct {
		name string
		ag   Agent
	}{
		{"typed nil *BasicAgent", (*BasicAgent)(nil)},
		{"裸 nil 接口", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("runMember 裸 panic 了（应当 fail-fast 返 error）：%v", r)
				}
			}()
			_, err := runMember(context.Background(), memberGrants{}, memberSpec{
				agent: tc.ag, sess: NewRunSession(), input: schema.UserMessage("x"),
				nsSuffix: "n", historySlot: "s",
			})
			if err == nil || !strings.Contains(err.Error(), "nil member agent") {
				t.Fatalf("要的是 nil member agent 的 fail-fast，拿到 err=%v", err)
			}
		})
	}
}
