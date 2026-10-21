package agent_v2_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

// 不变量：重试同一次调用 ⟹ 同一套提问 ID，已答的不再问；失败那次尝试的兑付不挡重试。
//
// 编排按 retryCfg 重试成员（runMember / HandoffAgent 的 step 节点）时，重试的是同一次调用。成员在自己的
// 图之外（agent 级钩子、自研 agent 的 Run 体）问过、答过之后这次尝试以可重试的错失败，下一次尝试里同一问
// 拿到同一个 ID、同一个答案——不重问，也不被答案账本判成落在已兑付的位上。

// raFlakyAsker 是非图执行体：Run 体里（自己的图之外）先问 Q1，拿到答案后第一次瞬时失败；再问 Q2。got 记下
// 两问拿到的答案。
type raFlakyAsker struct {
	name   string
	flaked atomic.Bool
	mu     sync.Mutex
	got    []string
}

func (a *raFlakyAsker) Run(ctx context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a1, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: a.name + " Q1 开工前确认？"})
	if err != nil {
		return nil, err
	}
	if a.flaked.CompareAndSwap(false, true) {
		return nil, errors.New("下游抖了一下")
	}
	a2, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: a.name + " Q2 交付前确认？"})
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.got = []string{a1.Content, a2.Content}
	a.mu.Unlock()
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, schema.AssistantMessage(a.name+" done"))}, nil
}
func (a *raFlakyAsker) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, errors.New("raFlakyAsker: 不支持 Resume")
}
func (a *raFlakyAsker) Name() string        { return a.name }
func (a *raFlakyAsker) Description() string { return a.name }

// check 断言 fixture 真的失败过一次、两问各问一次、各拿到自己的答案。
func (a *raFlakyAsker) check(t *testing.T, asked map[string]int) {
	t.Helper()
	if !a.flaked.Load() {
		t.Fatalf("fixture 没搭对：%s 一次都没失败过", a.name)
	}
	q1, q2 := a.name+" Q1 开工前确认？", a.name+" Q2 交付前确认？"
	if asked[q1] != 1 || asked[q2] != 1 {
		t.Errorf("%s 的两问应各问一次，实际 %v——重试换了 ID 重问", a.name, asked)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if want := []string{"答:" + q1, "答:" + q2}; !slices.Equal(a.got, want) {
		t.Errorf("%s 拿到的答案 = %q，want %q——有答案落到了别的问题上", a.name, a.got, want)
	}
}

// raDrive 跑到收敛：每次停在提问上，把问到的问题答成「答:<问题>」并累积着传。只在中断时续跑——别的错原样
// 返回，失败信息指向第一个出错的地方（接着续跑的话，终局失败之后看到的是「没有存档」）。
func raDrive(t *testing.T, ag agent_v2.Agent) (map[string]int, error) {
	t.Helper()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	asked := map[string]int{}
	_, err := ag.Run(ctx, sess, schema.UserMessage("go"))
	for round := 0; round < 6; round++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break
		}
		for _, q := range ie.AllQueries() {
			asked[q.Question]++
			answers[q.InterruptID] = schema.UserMessage("答:" + q.Question)
		}
		_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	}
	return asked, err
}

// TestRetry_SequentialMemberOutOfGraphQuestion_NotReasked（编排成员图外提问 + 可重试失败）。
//
// 修之前第二次尝试的那一问换了 ID：Q1 被重问；累积传答案时重问拿到的回答在下一次续跑里落到 Q2 的位置上，
// Q2 被 Q1 的答案静默满足（实测）。
func TestRetry_SequentialMemberOutOfGraphQuestion_NotReasked(t *testing.T) {
	m := &raFlakyAsker{name: "m"}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("ra_seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).AddStep("s1", m)
	asked, err := raDrive(t, seq)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %v）", err, asked)
	}
	m.check(t, asked)
}

// TestRetry_ParallelBranchesOutOfGraphQuestion_NotReasked（并行分支）：两个分支各自在图外问、各自失败一次。
func TestRetry_ParallelBranchesOutOfGraphQuestion_NotReasked(t *testing.T) {
	a, b := &raFlakyAsker{name: "a"}, &raFlakyAsker{name: "b"}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("ra_par"),
		agent_v2.WithParallelRetryConfig(compose_v2.SimpleRetry(2))).AddBranch("a", a).AddBranch("b", b)
	asked, err := raDrive(t, par)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %v）", err, asked)
	}
	a.check(t, asked)
	b.check(t, asked)
}

// TestRetry_HandoffCandidateOutOfGraphQuestion_NotReasked：HandoffAgent 的 step 节点在重试外开交接相位，同一形状。
func TestRetry_HandoffCandidateOutOfGraphQuestion_NotReasked(t *testing.T) {
	front := &raFlakyAsker{name: "front"}
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("ra_desk"), agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffRetryConfig(compose_v2.SimpleRetry(2))).AddAgent("front", front)
	asked, err := raDrive(t, h)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %v）", err, asked)
	}
	front.check(t, asked)
}

// raStartAsker 在 worker 的 OnAgentStart（它自己的图之外）问一次人。
type raStartAsker struct {
	hook.NoOpHooks
}

func (raStartAsker) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if info.Name != "worker" {
		return ctx, nil
	}
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "worker 开工前确认？"})
	return ctx, err
}

// raFlakyModel 第一次调用返回 503（可重试），之后答 "worker done"。
type raFlakyModel struct {
	calls atomic.Int32
}

func (m *raFlakyModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	if m.calls.Add(1) == 1 {
		return nil, &model.APIError{Provider: "fake", StatusCode: 503, Body: "overloaded"}
	}
	return schema.AssistantMessage("worker done"), nil
}
func (m *raFlakyModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *raFlakyModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestRetry_BasicAgentMemberStartHookQuestion_NotReasked：成员是 BasicAgent，在 OnAgentStart 里问过、答过之后，
// 它的模型返回 503、这次尝试失败；重试时 OnAgentStart 再触发一次，那一问不该重问。
func TestRetry_BasicAgentMemberStartHookQuestion_NotReasked(t *testing.T) {
	llm := &raFlakyModel{}
	worker := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithDescription("w"),
		agent_v2.WithHooks(raStartAsker{}), agent_v2.WithModel(llm))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("ra_seq_basic"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).AddStep("s1", worker)
	asked, err := raDrive(t, seq)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %v）", err, asked)
	}
	if n := llm.calls.Load(); n != 2 {
		t.Fatalf("fixture 没搭对：模型应被调 2 次（503 一次 + 重试成功一次），实际 %d", n)
	}
	if asked["worker 开工前确认？"] != 1 {
		t.Errorf("OnAgentStart 那一问应只问一次，实际 %v", asked)
	}
}

// TestRetry_CompletedCallInRetriedAttempt_AnswerStaysConsumed：重试的第一次尝试里经 RunSubAgent 调一个会问人的
// 子 agent（答完、完成），后面一步瞬时失败；第二次尝试拿回那次调用记下的结果、不再执行它。重试返回之后，子
// agent 兑付过的那个答案仍算兑付——节点拿同一个 ID 再取，取不到（一个答案只能满足一个提问）。
//
// 后一次尝试看不见前一次尝试的兑付（那是同一次调用的同一批提问），但前一次尝试并没有白做：它完成的调用被
// 拿回来用了。收尾时不把它的兑付并回外层的话，这个答案在账本里就像从没被用过。
func TestRetry_CompletedCallInRetriedAttempt_AnswerStaysConsumed(t *testing.T) {
	var askerRuns atomic.Int32
	asker := newSingleNodeAgent(t, "asker", "work", nil, func(ctx context.Context, c string) (string, error) {
		askerRuns.Add(1)
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"})
		if err != nil {
			return "", err
		}
		return "确认了:" + c + "(" + ans.Content + ")", nil
	})
	var flaked atomic.Bool
	var askID string
	var reuse []bool
	host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, c string) (string, error) {
		out, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (string, error) {
			events, err := agent_v2.RunSubAgent(ctx, "ask", asker, schema.UserMessage(c))
			if err != nil {
				return "", err
			}
			if flaked.CompareAndSwap(false, true) {
				return "", errors.New("后面一步抖了一下")
			}
			return nestedFinal(events), nil
		})
		if err != nil {
			return "", err
		}
		if askID != "" {
			_, ok := compose_v2.GetResumeAnswer(ctx, askID)
			reuse = append(reuse, ok)
		}
		return out, nil
	})
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在子 agent 的提问，got %v", err)
	}
	askID = ie.Query.InterruptID
	events, err := host.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{askID: schema.UserMessage("好")}),
		agent_v2.WithResumeSession(sess))
	if err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	if got := nestedFinal(events); got != "确认了:go(好)" {
		t.Fatalf("最终产出 = %q", got)
	}
	if n := askerRuns.Load(); n != 2 || !flaked.Load() {
		t.Fatalf("fixture 没搭对：子 agent 的节点应跑 2 次（首跑停下 + 续跑答完）、重试应发生，实际跑 %d 次、失败过 %v",
			n, flaked.Load())
	}
	if !slices.Equal(reuse, []bool{false}) {
		t.Errorf("重试返回之后节点再取子 agent 那一问的答案 = %v，want [false]——前一次尝试兑付过的答案没并回外层账本", reuse)
	}
}

// TestRetry_RunSubAgentCalleeAsksThenFails_RetryReusesAnswer：节点体用 RunWithRetry 包住 RunSubAgent，被调方在
// 自己的图之外问过、答过之后失败一次（a2a RemoteAgent 的 human gate 遇上网络抖动就是这个形状）。重试时被调方
// 再跑一次，同一问拿到同一个答案、不重问。
//
// 被调方每次调用都有自己的交接相位、提问从 0 数起，所以修之前这里不是重问：它第一次兑付的答案直接记进了节点的
// 账本，重试里取同一个 ID 被当成已兑付，整个任务终局失败（变异实测）。
func TestRetry_RunSubAgentCalleeAsksThenFails_RetryReusesAnswer(t *testing.T) {
	gate := &raFlakyAsker{name: "gate"}
	host := hoGraphHost(t, "ra_host", "call", func(ctx context.Context, in string) (string, error) {
		return compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (string, error) {
			events, err := agent_v2.RunSubAgent(ctx, "gate", gate, schema.UserMessage(in))
			if err != nil {
				return "", err
			}
			return nestedFinal(events), nil
		})
	})
	asked, err := raDrive(t, host)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %v）", err, asked)
	}
	gate.check(t, asked)
}
