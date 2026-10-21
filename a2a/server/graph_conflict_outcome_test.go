package server_test

// `ErrGraphIDConflict` 走到 a2a server 时要以失败收尾、文案脱敏，且「中断 + 写档撞身份冲突」不能
// 当成 input-required 留着等续发（那份中断的存档没写成）。文案里有 sid / ns / 两张图的 GraphID，不该
// 跨进程回传给远端对端。

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

type graphConflictAgent struct{ withInterrupt bool }

func (a *graphConflictAgent) Name() string        { return "conflict_agent" }
func (a *graphConflictAgent) Description() string { return "returns ErrGraphIDConflict" }

func (a *graphConflictAgent) err() error {
	conflict := fmt.Errorf("%w: sid=%q ns=%q existing=%q new=%q",
		compose_v2.ErrGraphIDConflict, "sess-secret-77", "sub#9", "graph-A", "graph-B")
	if !a.withInterrupt {
		return conflict
	}
	ie := &compose_v2.InterruptError{Reason: "ask",
		Query: &compose_v2.AskQuery{InterruptID: "q1", Question: "继续吗？"}}
	return fmt.Errorf("%w (root cp save failed: %w)", ie, conflict) // 引擎出口的形状
}

func (a *graphConflictAgent) Run(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return nil, a.err()
}

func (a *graphConflictAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, a.err()
}

func TestServer_GraphIDConflict_FailedAndSanitized(t *testing.T) {
	for _, tc := range []struct {
		name          string
		withInterrupt bool
	}{
		{"纯身份冲突", false},
		{"中断 + 写档撞身份冲突——不能当 input-required", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ag := &graphConflictAgent{withInterrupt: tc.withInterrupt}
			base := serveAgent(t, ag)
			ctx := context.Background()
			client, err := a2aclient.NewFromCard(ctx, server.Card(ag, base))
			if err != nil {
				t.Fatalf("NewFromCard: %v", err)
			}
			msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("hi"))
			var states []a2atypes.TaskState
			var texts []string
			for ev, e := range client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
				if e != nil {
					t.Fatalf("stream: %v", e)
				}
				var st *a2atypes.TaskStatus
				switch v := ev.(type) {
				case *a2atypes.TaskStatusUpdateEvent:
					st = &v.Status
				case *a2atypes.Task:
					st = &v.Status
				}
				if st == nil {
					continue
				}
				states = append(states, st.State)
				if st.Message != nil {
					txt, _ := partsText(st.Message.Parts)
					texts = append(texts, txt)
				}
			}
			sawFailed := false
			for _, s := range states {
				if s == a2atypes.TaskStateInputRequired {
					t.Errorf("不能当 input-required——那份中断的存档没写成，对端续发也续不上（states=%v）", states)
				}
				if s == a2atypes.TaskStateFailed {
					sawFailed = true
				}
			}
			if !sawFailed {
				t.Errorf("应以 failed 收尾，实际 states=%v", states)
			}
			for _, txt := range texts {
				for _, leak := range []string{"sess-secret-77", "sub#9", "graph-A", "graph-B"} {
					if strings.Contains(txt, leak) {
						t.Errorf("回传给对端的文案泄漏内部信息 %q: %s", leak, txt)
					}
				}
			}
		})
	}
}
