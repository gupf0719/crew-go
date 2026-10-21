package agui_test

// 守「存档被引擎特意保住时，前端仍然有一条路能把它清掉」。
//
// **为什么这条容易写错**（我写错过一次）：紧挨着的 `ErrCheckpointStale` 分支删 entry 是
// 无损的——它那种情况下存档根本没落盘。照抄那个形状到「存档被特意保住」的分支上，就把
// 唯一的清理路掐死了：AG-UI 协议里**「放弃」和「回答」是同一个端点**，删了 entry 两者
// 一起没，那份 Status=挂起 的档从此谁也够不着、还永远留在业务的待处理列表里。

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
)

// dropFrontierOnLoad 模拟业务自研 backend 漏序列化 `CurrentFrontier`。
type dropFrontierOnLoad struct {
	compose_v2.Checkpointer
}

func (d *dropFrontierOnLoad) LoadNS(ctx context.Context, sid, ns string) (*compose_v2.Checkpoint, error) {
	cp, err := d.Checkpointer.LoadNS(ctx, sid, ns)
	if err != nil || cp == nil {
		return cp, err
	}
	clone := *cp
	clone.CurrentFrontier = nil
	return &clone, nil
}

// TestEscapeHatch_AbandonStaysReachableOnUnusableState 前端「放弃」必须**不经过坏掉的
// 那条路**——`agent_v2.Resume` 的 Abort 分支在载入存档之前就 DeleteSubtree 返回了，
// 所以只要这一层别提前销毁句柄，它就通。
//
// sanity-revert：把那条分支的 `h.keepSuspended(key, sess)` 换回 `h.removeEntry(key)`
// → 放弃那一轮拿到 no_session，残留存档 1 份。
func TestEscapeHatch_AbandonStaysReachableOnUnusableState(t *testing.T) {
	inner := compose_v2.NewInMemoryCheckpointer()
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("esc"),
			agent_v2.WithModel(&askThenAnswerModel{}), agent_v2.WithAskUser(),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(3))
	}, agui.WithCheckpointer(&dropFrontierOnLoad{Checkpointer: inner}))

	// 轮 1：正常中断（写进去的档是完整的，读回来才缺字段）
	body1 := postAGUI(h, `{"threadId":"T","runId":"r1","messages":[{"role":"user","content":"hi"}]}`).Body.String()
	iid := lastInterruptID(body1)
	if iid == "" {
		t.Fatalf("fixture 没搭对：轮 1 该中断并给出 interruptId: %s", body1)
	}

	// 轮 2：用户回答 → 撞守卫。这一轮报错是对的，但它**不能**顺手掐掉逃生口。
	body2 := postAGUI(h, `{"threadId":"T","runId":"r2","resume":[{"interruptId":"`+iid+
		`","status":"resolved","payload":"y"}],"messages":[{"role":"user","content":"y"}]}`).Body.String()
	if !strings.Contains(body2, `"code":"checkpoint_not_resumable"`) {
		t.Fatalf("轮 2 该报 checkpoint_not_resumable: %s", body2)
	}

	// 轮 3：用户放弃（全 cancelled）——**唯一**能清掉那份档的路
	body3 := postAGUI(h, `{"threadId":"T","runId":"r3","resume":[{"interruptId":"`+iid+
		`","status":"cancelled"}],"messages":[{"role":"user","content":"算了"}]}`).Body.String()
	if strings.Contains(body3, "no_session") {
		t.Fatalf("**逃生口被掐死**：句柄在报错那轮被销毁了，于是放弃这条路也走不通。\n"+
			"AG-UI 把「回答」和「放弃」放在同一个端点上，删 entry 会连坐掉逃生口。\n实际: %s", body3)
	}

	// 承重断言：**那份档真的没了**。只断言「没报错」不够——「什么都没发生」时它也为真。
	left, err := inner.Cleanup(context.Background(), 0)
	if err != nil {
		t.Fatalf("cleanup 计数: %v", err)
	}
	if left != 0 {
		t.Errorf("用户放弃之后还残留 %d 份存档——它 Status 是挂起，会永远留在业务的待处理列表里", left)
	}
}

// lastInterruptID 从 SSE 流里取最后一个 interrupt 的 id。
func lastInterruptID(sse string) string {
	last := ""
	for _, part := range strings.Split(sse, `"id":"`) {
		if i := strings.Index(part, `"`); i > 0 && !strings.Contains(part[:i], "{") {
			last = part[:i]
		}
	}
	return last
}
