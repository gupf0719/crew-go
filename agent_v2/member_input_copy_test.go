package agent_v2_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

// 编排交给成员的输入是独立拷贝：成员（连同它的钩子）原地改它，碰不到调用方手里那条，也碰不到兄弟成员拿到的那条。
//
// 交原件时：并行分支的钩子同时原地改同一条消息（-race 报数据竞争），后到的分支看到前面分支加的后缀，调用方手里
// 那条也被改掉；路由的分类器改了输入，被选中的分支收到的是分类器改过的那条。

// suffixHook 在 OnAgentStart 里给输入的最后一条消息原地加一个后缀（[消费]档允许的改写）。
type suffixHook struct {
	hook.NoOpHooks
	suffix string
}

func (h suffixHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	if n := len(in.Messages); n > 0 {
		in.Messages[n-1].Content += h.suffix
	}
	return ctx, nil
}

// lastUserModel 记下每次调用时最后一条 user 消息的内容，返回固定答复。
type lastUserModel struct {
	mu    sync.Mutex
	seen  []string
	reply string
}

func (m *lastUserModel) Generate(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for i := len(in) - 1; i >= 0; i-- {
		if in[i].Role == schema.RoleUser {
			m.mu.Lock()
			m.seen = append(m.seen, in[i].Content)
			m.mu.Unlock()
			break
		}
	}
	return schema.AssistantMessage(m.reply), nil
}

func (m *lastUserModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *lastUserModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *lastUserModel) seenCopy() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.seen...)
}

func TestMemberInput_ParallelBranchesEachGetOwnCopy(t *testing.T) {
	const branches = 4
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par"))
	models := make([]*lastUserModel, branches)
	for i := range models {
		models[i] = &lastUserModel{reply: "ok"}
		name := string(rune('a' + i))
		par.AddBranch("br_"+name, agent_v2.NewBasicAgent(
			agent_v2.WithName("agent_"+name), agent_v2.WithModel(models[i]),
			agent_v2.WithHooks(suffixHook{suffix: "!"}), agent_v2.WithMaxRounds(2)))
	}
	in := schema.UserMessage("q")
	if _, err := par.Run(context.Background(), agent_v2.NewRunSession(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if in.Content != "q" {
		t.Errorf("调用方手里的输入被成员的钩子改掉了：%q，want %q", in.Content, "q")
	}
	for i, m := range models {
		seen := m.seenCopy()
		if len(seen) != 1 || seen[0] != "q!" {
			t.Errorf("分支 %d 的模型看到 %q，want [\"q!\"]——只该有它自己钩子加的那一个后缀", i, seen)
		}
	}
}

func TestMemberInput_ClassifierEditDoesNotReachBranch(t *testing.T) {
	branchModel := &lastUserModel{reply: "branch-answer"}
	classifier := agent_v2.NewBasicAgent(agent_v2.WithName("cls"),
		agent_v2.WithModel(&lastUserModel{reply: "math"}),
		agent_v2.WithHooks(suffixHook{suffix: " [classified]"}), agent_v2.WithMaxRounds(2))
	r := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("router"), agent_v2.WithRoutingClassifier(classifier)).
		AddBranch("math", agent_v2.NewBasicAgent(agent_v2.WithName("math_agent"),
			agent_v2.WithModel(branchModel), agent_v2.WithMaxRounds(2)))
	in := schema.UserMessage("1+1")
	if _, err := r.Run(context.Background(), agent_v2.NewRunSession(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if in.Content != "1+1" {
		t.Errorf("调用方手里的输入被分类器的钩子改掉了：%q", in.Content)
	}
	seen := branchModel.seenCopy()
	if len(seen) != 1 || seen[0] != "1+1" {
		t.Errorf("被选中的分支看到 %q，want [\"1+1\"]——分类器的改写不该流到分支", seen)
	}
}

// 共享视图下同一条原始输入只记一份——输入没有消息 ID（业务直接写字面量）时也一样。交给成员的是拷贝之后，
// 各分支手里的指针不同，认领不能再靠指针。
func TestMemberInput_SharedViewIDLessInputRecordedOnce(t *testing.T) {
	const branches = 3
	hs := &gatedHistoryStore{
		HistoryStore: agent_v2.NewInMemoryHistoryStore(),
		gate:         newFanoutBarrier(branches),
	}
	mk := func(name, reply string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName(name),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage(reply)})),
			agent_v2.WithHistory(hs, "acme"), agent_v2.WithMaxRounds(2))
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("br1", mk("a1", "答案一")).
		AddBranch("br2", mk("a2", "答案二")).
		AddBranch("br3", mk("a3", "答案三"))
	ho := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("ho"), agent_v2.WithHandoffInitial("front")).
		AddAgent("front", par)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-noid")
	in := &schema.Message{Role: schema.RoleUser, Content: "没有身份的问题"}
	if _, err := ho.Run(ctx, agent_v2.NewRunSession(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "acme", ConvID: "conv-noid"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	n := 0
	for _, m := range msgs {
		if m.Role == schema.RoleUser && strings.Contains(m.Content, "没有身份的问题") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("没有消息 ID 的原始输入在共享视图里记了 %d 份，want 1", n)
	}
}

// mutatingAgent 是一个不经框架入口的自研成员：在 Run 里直接原地改收到的输入，再把它当答复交回。框架自带的 agent
// 在 Run 入口自己拷一份，碰不到调用方那条；这类成员靠的是派发点交出去的拷贝。
type mutatingAgent struct {
	name  string
	reply string
}

func (a *mutatingAgent) Name() string        { return a.name }
func (a *mutatingAgent) Description() string { return a.name }
func (a *mutatingAgent) Run(_ context.Context, _ *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	in.Content += "!"
	reply := a.reply
	if reply == "" {
		reply = in.Content
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, schema.AssistantMessage(reply))}, nil
}
func (a *mutatingAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

func TestMemberInput_CustomParallelMembersGetOwnCopy(t *testing.T) {
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par"),
		agent_v2.WithAggregator(func(_ context.Context, results map[string]string) (*schema.Message, error) {
			for name, r := range results {
				if r != "q!" {
					return schema.AssistantMessage("bad:" + name + "=" + r), nil
				}
			}
			return schema.AssistantMessage("all-ok"), nil
		}))
	for _, n := range []string{"a", "b", "c", "d"} {
		par.AddBranch("br_"+n, &mutatingAgent{name: "m_" + n})
	}
	events, err := par.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("q"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "all-ok" {
		t.Errorf("每个成员都该只看到自己加的那一个后缀，聚合结果 %v", final)
	}
}

func TestMemberInput_CustomClassifierEditDoesNotReachBranch(t *testing.T) {
	branchModel := &lastUserModel{reply: "branch-answer"}
	r := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("router"),
		agent_v2.WithRoutingClassifier(&mutatingAgent{name: "cls", reply: "math"})).
		AddBranch("math", agent_v2.NewBasicAgent(agent_v2.WithName("math_agent"),
			agent_v2.WithModel(branchModel), agent_v2.WithMaxRounds(2)))
	if _, err := r.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("1+1")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if seen := branchModel.seenCopy(); len(seen) != 1 || seen[0] != "1+1" {
		t.Errorf("被选中的分支看到 %q，want [\"1+1\"]——分类器在 Run 里的原地改写不该流到分支", seen)
	}
}

// transferringMutator 是自研的转交候选：原地改收到的输入，再转交给 next。
type transferringMutator struct{ next string }

func (a *transferringMutator) Name() string        { return "front_custom" }
func (a *transferringMutator) Description() string { return "front_custom" }
func (a *transferringMutator) Run(ctx context.Context, _ *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	in.Content += " [front]"
	if err := agent_v2.HandoffTo(ctx, opts, a, a.next); err != nil {
		return nil, err
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.Name(), schema.AssistantMessage("转给你了"))}, nil
}
func (a *transferringMutator) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// 转交的每一跳拿到的是原始输入的拷贝：前一跳在 Run 里原地改了它，后一跳看到的仍是原样。
func TestMemberInput_HandoffHopEditDoesNotReachNextHop(t *testing.T) {
	billingModel := &lastUserModel{reply: "已受理"}
	ho := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("ho"), agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffHistoryMode(agent_v2.HandoffHistorySelfOnly)).
		AddAgent("front", &transferringMutator{next: "billing"}).
		AddAgent("billing", agent_v2.NewBasicAgent(agent_v2.WithName("billing"),
			agent_v2.WithModel(billingModel), agent_v2.WithMaxRounds(2)))
	if _, err := ho.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("我要退款")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	seen := billingModel.seenCopy()
	if len(seen) != 1 {
		t.Fatalf("fixture：billing 应接手并调一次模型，got %q", seen)
	}
	if seen[0] != "我要退款" {
		t.Errorf("后一跳看到 %q，want %q——前一跳的原地改写流到了后一跳", seen[0], "我要退款")
	}
}

// 编排自己的 OnAgentStart 钩子改的是它自己那份，调用方手里那条不变。
func TestMemberInput_OrchestrationHookDoesNotTouchCaller(t *testing.T) {
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq"),
		agent_v2.WithSequentialHooks(suffixHook{suffix: " [seq]"})).
		AddStep("s1", agent_v2.NewBasicAgent(agent_v2.WithName("s1"),
			agent_v2.WithModel(&lastUserModel{reply: "ok"}), agent_v2.WithMaxRounds(2)))
	in := schema.UserMessage("q")
	if _, err := seq.Run(context.Background(), agent_v2.NewRunSession(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if in.Content != "q" {
		t.Errorf("编排自己的钩子改到了调用方手里的输入：%q", in.Content)
	}
}

// 两跳转交、共享视图、输入没有消息 ID：原始输入只记一份。每一跳拿到的是转交编排那份输入的拷贝，认得出它们是
// 同一条靠的是转交编排在入口补的那个 ID。
func TestMemberInput_HandoffTwoHopsIDLessInputRecordedOnce(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithModel(&dupProbeModel{reply: "TRANSFER"}),
		agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3), agent_v2.WithHistory(hs, "acme"))
	billing := agent_v2.NewBasicAgent(agent_v2.WithName("billing"), agent_v2.WithModel(&dupProbeModel{reply: "已受理"}),
		agent_v2.WithMaxRounds(3), agent_v2.WithHistory(hs, "acme"))
	ho := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("ho"), agent_v2.WithHandoffInitial("front")).
		AddAgent("front", front).AddAgent("billing", billing)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-2hop-noid")
	in := &schema.Message{Role: schema.RoleUser, Content: "我要退款"}
	if _, err := ho.Run(ctx, agent_v2.NewRunSession(), in); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "acme", ConvID: "conv-2hop-noid"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	n, answered := 0, false
	for _, m := range msgs {
		if m.Role == schema.RoleUser && m.Content == "我要退款" {
			n++
		}
		if m.Role == schema.RoleAssistant && m.Content == "已受理" {
			answered = true
		}
	}
	if !answered {
		t.Fatalf("fixture：billing 没接手（历史里没有它的答复）：%d 条记录", len(msgs))
	}
	if n != 1 {
		t.Errorf("没有消息 ID 的原始输入两跳之后记了 %d 份，want 1", n)
	}
	if in.ID != "" {
		t.Errorf("调用方手里的消息长出了 ID %q——ID 只该补在框架自己那份上", in.ID)
	}
}
