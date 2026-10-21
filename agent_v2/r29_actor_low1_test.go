package agent_v2_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// nsCapturingActorAgent 是 ReflectAgent actor 测试 stub——OnAgentStart 时通过
// hook 捕获 ctx graphNS。直接读 ctx 而非通过 BasicAgent 嵌套（避免引入 model
// 依赖让测试更聚焦）。
//
// 用途：R29 视角4 HIGH-2 测 actor 跨多轮 ctx graphNS 是否含 round_<N>。
type nsCapturingActorAgent struct {
	mu       *sync.Mutex
	captured *[]string
	round    *int
}

func (a *nsCapturingActorAgent) Run(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.mu.Lock()
	*a.captured = append(*a.captured, compose_v2.GetGraphNS(ctx))
	*a.round++
	r := *a.round
	a.mu.Unlock()
	// 每轮返不同 content
	return []*agent_v2.AgentEvent{
		agent_v2.NewExitEvent("actor", schema.AssistantMessage("draft "+itoaSimple(r))),
	}, nil
}
func (a *nsCapturingActorAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *nsCapturingActorAgent) Name() string        { return "actor" }
func (a *nsCapturingActorAgent) Description() string { return "" }

func itoaSimple(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if neg {
		return "-" + string(digits)
	}
	return string(digits)
}

// TestReflectAgent_R29HIGH2_ActorCrossRoundNSDistinct 是 R29 视角4 HIGH-2 回归——
// 验证 ReflectAgent 跨多轮调 actor ctx graphNS 含 round_<N>。
//
// **Bug 历史**：原 actor 路径调 b.actor(ctx, input) 不 push round NS——跨多轮
// actor ctx graphNS 全相同（都是 parentNS）→ actor sub-agent 内 ask_user 算 InterruptID
// 全相同 → ResumeContext 跨轮误读上轮答案 → LLM 看到"用户对新问题答了上轮答案"。
//
// **修法**：agent_v2/reflect_builder.go::actorFn 在 b.actor 调用前 push
// actorCtx = WithGraphNS(ctx, JoinNS(GetGraphNS(ctx), fmt.Sprintf("round_%d", round)))。
//
// **测试设计**：actor 用 nsCapturingActorAgent 直接读 ctx graphNS——多轮调用应不同。
// 与 R30A HIGH-1 (eval 跨轮) 区分：本测专测 actor 路径，不是 evalFn 路径。
func TestReflectAgent_R29HIGH2_ActorCrossRoundNSDistinct(t *testing.T) {
	captured := []string{}
	roundCounter := 0
	mu := sync.Mutex{}
	actor := &nsCapturingActorAgent{
		mu:       &mu,
		captured: &captured,
		round:    &roundCounter,
	}

	// EvalFunc 强制 N 轮跑：前 2 轮不通过、第 3 轮通过
	evalCount := 0
	evalFn := func(_ context.Context, _ *schema.Message) (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		evalCount++
		return evalCount >= 3, nil
	}

	r := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("review"),
		agent_v2.WithReflectMaxRounds(5),
		agent_v2.WithEvalFunc(evalFn),
	).WithActor(actor)

	_, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("draft a doc"))
	if err != nil {
		t.Fatalf("Run err: %v", err)
	}

	mu.Lock()
	ns := append([]string(nil), captured...)
	mu.Unlock()
	if len(ns) < 2 {
		t.Fatalf("actor 应被调 >= 2 次（多轮 reflect）；实际 %d 次 NS=%v", len(ns), ns)
	}

	// **核心验证**：跨轮 NS 应不同（含 round_<N>）
	for i := 0; i < len(ns)-1; i++ {
		if ns[i] == ns[i+1] {
			t.Errorf("R29 视角4 HIGH-2 回归：跨轮 actor ctx graphNS 应不同（含 round 序号）；"+
				"实际 round %d / round %d 同 NS=%q\n"+
				"sanity-revert 删 actorCtx round NS push → 跨轮 NS 复用 → 测试 fail",
				i, i+1, ns[i])
		}
	}
	// 验证 NS 含 round_ 字符串
	if !strings.Contains(ns[0], "round_") {
		t.Errorf("R29 视角4 HIGH-2 回归：NS 应含 'round_' 序号；实际 %q", ns[0])
	}
}

// 注：R29 视角2 LOW-1 (AutoCompact.shouldTrigger 跳 StoreInjectedAnnotation marker)
// 测试在 `r29_low1_autocompact_internal_test.go` (package agent_v2 white-box)——
// shouldTrigger 是 unexported function，必须 internal test 测。

// 引用 hook 包避免 unused import lint
var _ hook.Hooks = nil
var _ = strings.Contains
