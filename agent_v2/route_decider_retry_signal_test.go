package agent_v2_test

// 升级链以失败收尾之后，classifier 换一代从头跑（存档命名空间带 _retry<代数>）。它在新一代里做出的路由
// 要照样交到 RoutingAgent 手上，中断续跑之后也一样：收件人在 classify 节点这次执行里铸，跟 classifier
// 的代数无关——收件人要是跟着变，路由就丢在半路，以 RouteUndecidedError 收尾。
//
// 既有的重试用例测不出这件事：它们的 classifier 是 BasicAgent + route_to，最终消息就是那条 route_to，
// 收件人对不上时兜底解析照样把目标救回来（审计实测：让收件人每次执行都不同，那些用例全绿）。这里的
// classifier 最终产出恒不是分支名、也不配兜底，路由只可能来自上行信号。

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// retrySignalRouting 建一个只有 classifier、没有兜底的 RoutingAgent，分支 math / chat 记各自被执行几次。
func retrySignalRouting(cls agent_v2.Agent) (*agent_v2.RoutingAgent, *effReply, *effReply) {
	math := &effReply{name: "math", reply: "MATH 已处理"}
	chat := &effReply{name: "chat", reply: "CHAT 已处理"}
	rt := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("rt"), agent_v2.WithRoutingClassifier(cls)).
		AddBranch("math", math).AddBranch("chat", chat)
	return rt, math, chat
}

// regrantRoute 在 classifier 里经 RunSubAgent 把路由能力显式转授给 router，由它定路由。
func regrantRoute(ctx context.Context, router agent_v2.Agent, c string) error {
	_, err := agent_v2.RunSubAgent(ctx, router.Name(), router, schema.UserMessage(c), agent_v2.RegrantFromNode(ctx, router)...)
	return err
}

// driveRetrySignal 先 Run、再逐步 Resume，每一步按 steps 断言：undecided（RouteUndecidedError）、
// interrupt（停在提问上，答「是」累积进答案）、math（路由到 math 收尾）。
func driveRetrySignal(t *testing.T, ag agent_v2.Agent, steps ...string) {
	t.Helper()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	var events []*agent_v2.AgentEvent
	var err error
	for i, want := range steps {
		if i == 0 {
			events, err = ag.Run(ctx, sess, schema.UserMessage("算一下 1+1"))
		} else {
			opts := []agent_v2.ResumeOption{agent_v2.WithResumeSession(sess)}
			if len(answers) > 0 {
				opts = append(opts, agent_v2.WithResumeUserInputs(answers))
			}
			events, err = ag.Resume(ctx, sess.RunID, opts...)
		}
		switch want {
		case "undecided":
			var ue *agent_v2.RouteUndecidedError
			if !errors.As(err, &ue) {
				t.Fatalf("第 %d 步应以 RouteUndecidedError 收尾，实际 err=%v", i, err)
			}
		case "interrupt":
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("第 %d 步应停在提问上，实际 err=%v", i, err)
			}
			for _, q := range ie.AllQueries() {
				answers[q.InterruptID] = schema.UserMessage("是")
			}
		case "math":
			if err != nil {
				t.Fatalf("第 %d 步应路由到 math 收尾，实际 err=%v", i, err)
			}
			if final := agent_v2.FinalMessage(events); final == nil || final.Content != "MATH 已处理" {
				t.Fatalf("第 %d 步应路由到 math，实际最终答复 %+v", i, final)
			}
		}
	}
}

// TestRouteDecider_RetryGenerationThenPause_RouteDelivered：classifier 第一代没给出分支 ⟹ RouteUndecidedError；
// 重试那一代经转授定了 math、再问人 ⟹ 中断；答完续跑 ⟹ 路由到 math。路由模型只被调一次（续跑时从完成
// 记录拿回，那次的路由随记录交出）。
func TestRouteDecider_RetryGenerationThenPause_RouteDelivered(t *testing.T) {
	var body atomic.Int32
	router, routerModel := effRouter("router_llm", "math")
	cls := effGraph(t, "classifier", func(ctx context.Context, c string) (string, error) {
		if body.Add(1) == 1 {
			return "想不好", nil
		}
		if err := regrantRoute(ctx, router, c); err != nil {
			return "", err
		}
		return "已分好", effAsk(ctx, true, "确认路由吗？")
	})
	rt, math, chat := retrySignalRouting(cls)
	driveRetrySignal(t, rt, "undecided", "interrupt", "math")
	if n := routerModel.calls.Load(); n != 1 {
		t.Errorf("路由模型应只被调一次（续跑时从完成记录拿回），实际 %d 次", n)
	}
	if math.runs.Load() != 1 || chat.runs.Load() != 0 {
		t.Errorf("应只跑 math 一次：math %d 次、chat %d 次", math.runs.Load(), chat.runs.Load())
	}
}

// TestRouteDecider_FailedGenerationRouteVoided：两步的 classifier，第一代在第一步定了 chat、第二步报错 ⟹
// 那一代失败，它定的 chat 随之作废；重试那一代在第一步定 math、第二步问人 ⟹ 中断；续跑 ⟹ 路由到 math，
// chat 一次也不跑。
func TestRouteDecider_FailedGenerationRouteVoided(t *testing.T) {
	var first atomic.Int32
	toChat, _ := effRouter("router_chat_llm", "chat")
	toMath, _ := effRouter("router_math_llm", "math")
	cls := newTwoNodeAgent(t, "classifier",
		func(ctx context.Context, c string) (string, error) {
			if first.Add(1) == 1 {
				return "chat-routed", regrantRoute(ctx, toChat, c)
			}
			return "math-routed", regrantRoute(ctx, toMath, c)
		},
		func(ctx context.Context, _, mid string) (string, error) {
			if mid == "chat-routed" {
				return "", errors.New("classifier backend 503")
			}
			return "已分好", effAsk(ctx, true, "确认路由吗？")
		})
	rt, math, chat := retrySignalRouting(cls)
	driveRetrySignal(t, rt, "undecided", "interrupt", "math")
	if math.runs.Load() != 1 || chat.runs.Load() != 0 {
		t.Errorf("失败那一代定的 chat 不该生效：math %d 次、chat %d 次", math.runs.Load(), chat.runs.Load())
	}
}

// TestRouteDecider_RetryGeneration_NoRegrantNoRoute 是对照：重试那一代调的 agent 没被转授，定不了路由，
// 两代都以 RouteUndecidedError 收尾——上面两条里的 math 只可能来自 classifier 交上来的路由。
func TestRouteDecider_RetryGeneration_NoRegrantNoRoute(t *testing.T) {
	var body atomic.Int32
	router, _ := effRouter("router_llm", "math")
	cls := effGraph(t, "classifier", func(ctx context.Context, c string) (string, error) {
		if body.Add(1) == 1 {
			return "想不好", nil
		}
		_, err := agent_v2.RunSubAgent(ctx, router.Name(), router, schema.UserMessage(c))
		return "已分好", err
	})
	rt, math, _ := retrySignalRouting(cls)
	driveRetrySignal(t, rt, "undecided", "undecided")
	if n := math.runs.Load(); n != 0 {
		t.Errorf("没被转授的 agent 定不了路由，math 却跑了 %d 次", n)
	}
}
