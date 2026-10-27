package agui_test

// 续跑时作废上一个请求已经交给前端的答复；钩子把答复换成一条共享的固定回复。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// askReject 第一遍问人；续跑拿到「否」就拒掉答复（返回错误 → 答复作废、retractModelAnswer）。
type askReject struct{ hook.NoOpHooks }

func (askReject) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	if len(schema.GetToolCalls(out.Message)) > 0 {
		return ctx, nil
	}
	ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "发出去吗"})
	if err != nil {
		return ctx, err
	}
	if ans != nil && ans.Content == "否" {
		return ctx, errors.New("rejected by reviewer")
	}
	return ctx, nil
}

func toWire(cms []clientMsg) []map[string]any {
	var msgs []map[string]any
	for _, cm := range cms {
		msgs = append(msgs, map[string]any{"id": cm.id, "role": cm.role, "content": cm.content})
	}
	return msgs
}

// 续跑请求里，上一个请求已经交给前端的答复（在请求原文里）被审核钩子拒掉、作废：快照里不再带它（曾经只撤这个 run 发出的，
// 被拒的答复留在前端、下一轮又作为前文回到模型，审计实测）。
func TestResetSnapshot_ResumeDiscardRemovesShownMessage(t *testing.T) {
	m := &scriptStream{script: func(n int, send func(*schema.Message)) error { send(text(fmt.Sprintf("secret%d", n))); return nil }}
	h := ownedHandler(func(ui hook.Hooks) agent_v2.Agent {
		step := agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithModel(m),
			agent_v2.WithHooks(hook.NewChainHooks(ui, askReject{})), agent_v2.WithMaxRounds(3))
		return step
	}, agui.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()))
	start := []clientMsg{{id: "u1", role: "user", content: "go"}}
	ev1 := parseSSE(postAGUI(h, `{"threadId":"t1","runId":"r1","messages":[{"id":"u1","role":"user","content":"go"}]}`).Body.String())
	iid := findInterruptID(ev1)
	after1 := applyAGUI(start, ev1)
	body, _ := json.Marshal(map[string]any{"threadId": "t1", "runId": "r2", "messages": toWire(after1),
		"resume": []map[string]any{{"interruptId": iid, "status": "resolved", "payload": "否"}}})
	ev2 := parseSSE(postAGUI(h, string(body)).Body.String())
	got := applyAGUI(after1, ev2)
	for _, cm := range got {
		if cm.content == "secret0" {
			t.Errorf("被拒掉（作废）的答复 secret0 仍留在前端：%+v", got)
		}
	}
}

// 钩子把答复换成一条共享的固定回复（包级变量）：框架给答复盖 run_id 时不碰钩子交来的对象（曾经直接改它的标注，并发调用
// 互相改写，-race 实测）。
var canned = &schema.Message{Role: schema.RoleAssistant, Content: "blocked"}

type cannedHook struct{ hook.NoOpHooks }

func (cannedHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	out.Message = canned
	return ctx, nil
}

func TestResetSnapshot_SharedReplacementNotMutated(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m := &scriptStream{script: func(_ int, send func(*schema.Message)) error { send(text("secret")); return nil }}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("b"), agent_v2.WithModel(m),
				agent_v2.WithHooks(cannedHook{}), agent_v2.WithMaxRounds(2))
			_, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go"))
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if canned.Annotations != nil {
		t.Errorf("框架改写了钩子交来的共享消息对象的 Annotations：%v", canned.Annotations)
	}
}
