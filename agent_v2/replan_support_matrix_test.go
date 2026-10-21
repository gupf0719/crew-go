package agent_v2_test

// `ResumeActionReplan` 在 9 个编排上的接受 / 拒绝，逐条钉死。
//
// # 为什么补这张表
//
// 「PlanAgent / BasicAgent / GraphAgent 接受，其余 6 个 fail-fast」是一条写在 CLAUDE.md
// 和 `resume_helpers.go` godoc 里的**完备性断言**，但此前**一行测试都没有**——9 个编排
// 各自实现，靠人记得。
//
// 最糟的失败形态不是"该接受的拒绝了"（会立刻报错），而是**静默忽略**：某个编排既不
// 接受也不 fail-fast，业务传了 Replan 却按 Continue 跑，拿到一个看起来正常的结果，而
// 重新规划根本没发生。
//
// 这类断言的通病是"加编排时忘了跟上"。有了这张表，新编排不进表就没被验证过，进了表
// 就必须回答"你支不支持 Replan"。

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

func TestReplan_SupportMatrix_AllOrchestrations(t *testing.T) {
	// **sub-agent 必须真中断**：跑完就被 cleanupOnSuccess 清 cp，Resume 会先撞
	// ErrNoPendingCheckpoint——那条路径在拒绝块被删掉时**照样报错**，测试就成了假绿
	// （实测过：删掉 sequential 的拒绝块，测试仍 PASS）。这是 CLAUDE.md「5 类炸弹」
	// 第 2 类"早退兜底盖过 fix 路径"。有真 cp 时，拒绝块是唯一的报错来源。
	echo := &interruptingAgent{name: "replan_sub"}
	cases := []struct {
		name    string
		agent   agent_v2.Agent
		accepts bool
	}{
		{"basic", agent_v2.NewBasicAgent(
			agent_v2.WithName("replan_basic"),
			agent_v2.WithAskUser(),
			agent_v2.WithModel(newStubModel([]*schema.Message{
				schema.AssistantToolCallMessage([]schema.ToolCall{{
					ID: "q1", Type: "function",
					Function: schema.FunctionCall{
						Name: agent_v2.AskUserToolName, Arguments: `{"question":"?"}`,
					},
				}}),
			})),
		), true},
		{"plan", agent_v2.NewPlanAgent(
			func(_ context.Context, _ *agent_v2.RunSession, _ string, _ []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
				return nil, "done", nil
			},
			agent_v2.PlanName("replan_plan"),
			agent_v2.WithSubAgents(map[string]agent_v2.Agent{"sub": echo}),
		), true},
		{"graph", buildR33EchoGraph(t, nil), true},

		{"sequential", agent_v2.NewSequentialAgent(
			agent_v2.WithSequentialName("replan_seq")).AddStep("s1", echo), false},
		{"parallel", agent_v2.NewParallelAgent(
			agent_v2.WithParallelName("replan_par")).AddBranch("b1", echo), false},
		{"loop", agent_v2.NewLoopAgent(
			agent_v2.WithLoopName("replan_loop"),
			agent_v2.WithLoopMaxIterations(1)).WithBody(echo), false},
		{"routing", agent_v2.NewRoutingAgent(
			agent_v2.WithRoutingName("replan_route"),
			agent_v2.WithRouter(func(_ context.Context, _ *schema.Message) (compose_v2.NodeID, error) {
				return "b1", nil
			})).AddBranch("b1", echo), false},
		{"reflect", agent_v2.NewReflectAgent(
			agent_v2.WithReflectName("replan_reflect")).WithActor(echo), false},
		{"handoff", agent_v2.NewHandoffAgent(
			agent_v2.WithHandoffName("replan_handoff"),
			agent_v2.WithHandoffInitial("a1")).AddAgent("a1", echo), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cper := compose_v2.NewInMemoryCheckpointer()
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
			const sid = "replan_sid"

			// 先跑到中断，留下真 pending cp
			sess := agent_v2.NewRunSession()
			sess.RunID = sid
			_, runErr := tc.agent.Run(ctx, sess, schema.UserMessage("go"))
			if !tc.accepts {
				// **只有 reject 档需要真 cp**：没 cp 时 Resume 先撞
				// ErrNoPendingCheckpoint，拒绝块在不在都会报错 → 假绿。
				// accepts 档的断言是"没被 not supported 拒"，不依赖 cp。
				if _, isInt := compose_v2.IsInterrupt(runErr); !isInt {
					t.Fatalf("%s 应中断留下 cp（否则本测试退化成假绿），got %v", tc.name, runErr)
				}
				if ok, _ := cper.ExistsNS(ctx, sid, ""); !ok {
					t.Fatalf("%s 的 cp 没落盘——拒绝块在不在都一样，测试就白做了", tc.name)
				}
			}

			_, err := tc.agent.Resume(ctx, sid, agent_v2.WithResumeAction(agent_v2.ResumeActionReplan))

			if !tc.accepts {
				// 拒绝档：必须**显式报错**。静默按 Continue 跑是最糟的——业务以为
				// 重新规划了，其实什么都没变，而且没有任何信号。
				if err == nil {
					t.Fatalf("%s 不支持 Replan，但静默接受了（err=nil）——"+
						"业务会以为重新规划发生了，实际按 Continue 跑", tc.name)
				}
				// **只认 not supported**：cp 确实存在（上面断言过），所以撞
				// ErrNoPendingCheckpoint 说明走到了别的路径，不算这条断言成立
				if !strings.Contains(err.Error(), "not supported") {
					t.Errorf("%s 应返 \"ResumeActionReplan not supported\"，got %v", tc.name, err)
				}
				return
			}

			// 接受档：**不能**因为 Replan 这个 action 本身而被拒
			if err != nil && strings.Contains(err.Error(), "not supported") {
				t.Errorf("%s 应接受 Replan，却返回 not supported: %v", tc.name, err)
			}
		})
	}
}

// TestNewLLMPlanner_TypedNilModel：工厂档也要守 typed-nil。
//
// 同包同类型的 `WithPlanModel` / `WithModel` 都守了，`NewLLMPlanner` 是 50 个"接受
// interface 的导出入口"里唯一漏的——CLAUDE.md 的「协议变更四复证教训」到这里是第七复。
//
// 不守的后果不是"没防御"这么抽象：typed nil 会一路活到 planner 首次被调用，业务看到
// 的是 `node "plan": node panic recovered: invalid memory address or nil pointer
// dereference`，零线索指向"你传了个 typed nil model"。
func TestNewLLMPlanner_TypedNilModel(t *testing.T) {
	var m *sharedPtrModel // typed nil：interface header 有 type tag，`m == nil` 是 false
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("NewLLMPlanner 收到 typed nil ChatModel 应构造期 panic fail-fast——" +
				"否则 nil deref 会在 planner 首次调用时才炸，业务从 stack 看不出根因")
		}
		if !strings.Contains(toStr(r), "typed nil") {
			t.Errorf("panic 信息应点明 typed nil 陷阱，got %v", r)
		}
	}()
	_ = agent_v2.NewLLMPlanner(m)
}

func toStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if e, ok := v.(error); ok {
		return e.Error()
	}
	return ""
}
