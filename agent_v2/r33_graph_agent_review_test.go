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

// 四编排三方对照 review（R33）GraphAgent 组回归测试。
//
// GA-H1: 嵌套 HITL auto-resume 时 InputWriter 写被双重应用——BasicAgent 有
// 播种守卫，GraphAgent 漏（"修一个漏一个"四复证同模式）。
// GA-M1: input 审计事件在 OnAgentStart 之前加——hook 改写不进审计 + hook
// 拒绝路径留垃圾 input 事件（与其余 7 编排序相反）。

func init() {
	compose_v2.RegisterValueType[[]string]("r33.acc_strings")
}

// buildR33AppendGraph 构造单节点 graph：InputWriter 把 input 种子写 Append channel，
// 节点首跑抛 InterruptError，Resume 后把 Append 内容拼进输出。
func buildR33AppendGraph(t *testing.T, graphName string) *agent_v2.GraphAgent {
	t.Helper()
	accCh := compose_v2.Append[string]("__r33_acc__")
	outCh := compose_v2.LastValue[*schema.Message]("__r33_out__")

	bld := compose_v2.NewBuilder()
	compose_v2.WithChannel(bld, accCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("work", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if msg, ok := compose_v2.GetResumeAnswer(ctx, "r33q1"); !ok || msg == nil {
			return nil, &compose_v2.InterruptError{
				Reason: "need approval",
				Query:  &compose_v2.AskQuery{InterruptID: "r33q1", Question: "go?"},
			}
		}
		acc, _ := compose_v2.Get(chs, accCh)
		out := schema.AssistantMessage("acc=[" + strings.Join(acc, ",") + "]")
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, out)}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "work")
	bld.AddEdge("work", compose_v2.EndNode)
	bld.WithName(graphName)
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	return agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("r33_ga"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			// 业务把 input 种子写进 Append channel——合法用法（messages 列表惯用形）
			return []compose_v2.ChannelWrite{compose_v2.Write(accCh, []string{in.Content})}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
	)
}

// TestR33_GraphAgent_NestedResume_NoDoubleApplyInitialWrites 复现 GA-H1：
//
// Sequential[step "g" = GraphAgent]，graph 内 InputWriter 写 Append channel（种子），
// 节点首跑抛 InterruptError；外层 seq.Resume 重跑 step → ga.Run → compose_v2.Run
// auto-load 恢复 cp（Append 已含种子）→ 守卫缺失时又 ApplyAll(initialWrites) →
// 种子重复 append（acc=[SEED,SEED]）。
//
// fix 后两者共用同一条路径：播种无条件走 compose_v2.WithInitialWrites，而**引擎只在
// 新开路径 apply 它**——续跑时不注入，让 cm.Restore 单源恢复。GraphID 不参与这个决策
// （当年那版按 GraphID 跳过播种的 agent 侧自查已删）。
// sanity-revert：让引擎在续跑路径上也 apply initialWrites → 本测试 fail（SEED 出现 2 次）。
func TestR33_GraphAgent_NestedResume_NoDoubleApplyInitialWrites(t *testing.T) {
	ga := buildR33AppendGraph(t, "r33_dapply_graph")
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("r33_seq")).AddStep("g", ga)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := seq.Run(ctx, sess, schema.UserMessage("SEED"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("expect interrupt, got %v", err)
	}

	events, err := seq.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("yes")}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("no final message")
	}
	if got := strings.Count(final.Content, "SEED"); got != 1 {
		t.Fatalf("GA-H1 double-apply: seed 应恰出现 1 次（cm.Restore 单源恢复），got %d 次: %q",
			got, final.Content)
	}
}

// rewriteStartHooks OnAgentStart 把 input 替换成 REWRITTEN（模拟 PII filter）。
type rewriteStartHooks struct {
	hook.NoOpHooks
}

func (rewriteStartHooks) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	in.Messages = []*schema.Message{schema.UserMessage("REWRITTEN")}
	return ctx, nil
}

// rejectStartHooks OnAgentStart 直接拒绝。
type rejectStartHooks struct {
	hook.NoOpHooks
}

func (rejectStartHooks) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	return ctx, context.DeadlineExceeded // 任意业务错
}

// buildR33EchoGraph 最小单节点 echo graph（无 interrupt）。
func buildR33EchoGraph(t *testing.T, hooks hook.Hooks) *agent_v2.GraphAgent {
	t.Helper()
	inCh := compose_v2.LastValue[*schema.Message]("__r33e_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__r33e_out__")
	bld := compose_v2.NewBuilder()
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("echo", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("echo:"+in.Content))}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "echo")
	bld.AddEdge("echo", compose_v2.EndNode)
	bld.WithName("r33_echo_graph")
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	gopts := []agent_v2.GraphAgentOption{
		agent_v2.WithGraphAgentName("r33_echo"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
	}
	if hooks != nil {
		gopts = append(gopts, agent_v2.WithGraphAgentHooks(hooks))
	}
	return agent_v2.NewGraphAgent(g, gopts...)
}

// TestR33_GraphAgent_InputEventAfterHookRewrite 复现 GA-M1（改写面）：
//
// input 审计事件必须在 OnAgentStart 消费**之后**加（与其余 7 编排同序）——hook
// 改写（PII filter）须进审计视图，否则审计存原文而 graph 用脱敏版，嵌套时经
// HistoryFor 把原文泄漏给兄弟 step 的 LLM。
// sanity-revert：把 AddEvent 移回 OnAgentStart 之前 → 审计事件是 ORIGINAL → fail。
func TestR33_GraphAgent_InputEventAfterHookRewrite(t *testing.T) {
	ga := buildR33EchoGraph(t, rewriteStartHooks{})
	sess := agent_v2.NewRunSession()
	events, err := ga.Run(context.Background(), sess, schema.UserMessage("ORIGINAL"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 第一个事件应是 input 审计事件，内容 = hook 改写后
	if len(events) == 0 {
		t.Fatal("no events")
	}
	first := events[0]
	if first.Output == nil || first.Output.Message == nil {
		t.Fatal("first event has no message")
	}
	if got := first.Output.Message.Content; got != "REWRITTEN" {
		t.Fatalf("GA-M1: input 审计事件应是 hook 改写后内容 REWRITTEN（审计视图=LLM 输入视图），got %q", got)
	}
	// graph 真用了改写后 input
	final := agent_v2.FinalMessage(events)
	if final == nil || final.Content != "echo:REWRITTEN" {
		t.Fatalf("graph 应消费改写后 input，got %v", final)
	}
}

// TestR33_GraphAgent_NoInputEventWhenStartHookRejects 复现 GA-M1（拒绝面）：
// OnAgentStart 报错时不应留"从未跑过的 run"的 input 审计垃圾（其余 7 编排此路径
// session 干净）。
func TestR33_GraphAgent_NoInputEventWhenStartHookRejects(t *testing.T) {
	ga := buildR33EchoGraph(t, rejectStartHooks{})
	sess := agent_v2.NewRunSession()
	events, err := ga.Run(context.Background(), sess, schema.UserMessage("ORIGINAL"))
	if err == nil {
		t.Fatal("expect OnAgentStart error")
	}
	for _, ev := range events {
		if ev.Output != nil && ev.Output.Message != nil && ev.Output.Message.Content == "ORIGINAL" {
			t.Fatalf("GA-M1: OnAgentStart 拒绝路径不应留 input 审计事件，got %+v", ev)
		}
	}
}

// fixedReplyAgent 极简 Agent stub：恒返固定文本（产 ExitEvent）。
type fixedReplyAgent struct {
	name  string
	reply string
}

func (f *fixedReplyAgent) Name() string        { return f.name }
func (f *fixedReplyAgent) Description() string { return "fixed reply" }
func (f *fixedReplyAgent) Run(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	ev := agent_v2.NewExitEvent(f.name, schema.AssistantMessage(f.reply))
	sess.AddEvent(ev)
	return []*agent_v2.AgentEvent{ev}, nil
}
func (f *fixedReplyAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// TestR33_Parallel_NilMergerNormalized 复现 PA-M1：
//
// 业务 merger 返 (nil, nil)（自然写法如 `if len(results)==0 { return nil, nil }`
// 的兜底分支）时，修前 ParallelAgent 不 emit 自己的 ExitEvent → 嵌套消费方
// FinalMessage(events) 反向扫捞到**最后提交分支**的 ExitEvent，把单分支输出
// 静默当聚合结果。修后 compose finalizeFn 把 nil 归一化空 assistant →
// ParallelAgent 恒 emit 父 Exit。
// sanity-revert：删 finalizeFn nil 归一化 → FinalMessage 捞到分支文本 → fail。
func TestR33_Parallel_NilMergerNormalized(t *testing.T) {
	par := agent_v2.NewParallelAgent(
		agent_v2.WithParallelName("r33_par"),
		agent_v2.WithParallelMerger(func(_ context.Context, _ []agent_v2.ParallelBranchResult) (*schema.Message, error) {
			return nil, nil // 业务自然写法：无聚合产出
		}),
	).
		AddBranch("a", &fixedReplyAgent{name: "br_a", reply: "BRANCH_A_RESULT"}).
		AddBranch("b", &fixedReplyAgent{name: "br_b", reply: "BRANCH_B_RESULT"})

	sess := agent_v2.NewRunSession()
	events, err := par.Run(context.Background(), sess, schema.UserMessage("go"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		t.Fatal("PA-M1: merger 返 nil 应归一化空 assistant（父 Exit 必 emit），got nil")
	}
	if strings.Contains(final.Content, "BRANCH_") {
		t.Fatalf("PA-M1 串台: FinalMessage 捞到分支 Exit 当聚合结果，got %q", final.Content)
	}
}

// annotatedReplyAgent classifier stub：返带 route_target annotation 的消息（信号优先级 2）。
type annotatedReplyAgent struct {
	name   string
	target string
}

func (a *annotatedReplyAgent) Name() string        { return a.name }
func (a *annotatedReplyAgent) Description() string { return "annotated classifier" }
func (a *annotatedReplyAgent) Run(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	msg := schema.AssistantMessage(a.target)
	msg.Annotations = map[string]any{"route_target": a.target}
	ev := agent_v2.NewExitEvent(a.name, msg)
	sess.AddEvent(ev)
	return []*agent_v2.AgentEvent{ev}, nil
}
func (a *annotatedReplyAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// r33EventRecorder 记录 OnNodeEvent 的高层 OrchestrationHooks stub。
type r33EventRecorder struct {
	agent_v2.NoOpOrchestrationHooks
	mu     sync.Mutex
	events []agent_v2.Event
}

func (h *r33EventRecorder) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, ev)
	return ctx
}

// countRouteDecisions 数 route_decision event 数。
func countRouteDecisions(rec *r33EventRecorder) int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	n := 0
	for _, ev := range rec.events {
		if ev.Type == agent_v2.RouteDecisionEventType {
			n++
		}
	}
	return n
}

// TestR33_RouteDecision_TopLevelFires 复现 RT-H1：
//
// route_decision 在 ConditionalEdge cond fn 内 fire——cond fn 跑在 Run-level ctx
// （不在 node scope），修前 GetEventSink 恒 nil → 顶层 RoutingAgent.Run 时 event
// 静默丢、crew.route.decisions 永远 0（框架该自动记的指标静默缺失）；嵌套时借父图
// node sink 才 fire——同一逻辑两形态不一致。
// 修后 compose_v2.Run 入口 push Run-level EventSink 兜底。
// sanity-revert：删 Run-level sink push → 顶层 0 event → fail。
func TestR33_RouteDecision_TopLevelFires(t *testing.T) {
	rec := &r33EventRecorder{}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("r33_route"),
		agent_v2.WithRoutingClassifier(&annotatedReplyAgent{name: "cls", target: "a"}),
	).AddBranch("a", &fixedReplyAgent{name: "wk_a", reply: "A_DONE"})

	ctx := agent_v2.WithCtxOrchHooks(context.Background(), rec)
	if _, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := countRouteDecisions(rec); got != 1 {
		t.Fatalf("RT-H1: 顶层 RoutingAgent route_decision 应恰 fire 1 次，got %d", got)
	}
}

// TestR33_RouteDecision_NestedFiresOnce 验证 RT-H1 修复不引入双 fire：
// 嵌套 Sequential[RoutingAgent] 时内层 Run-level sink 最近（遮蔽外层 node sink），
// route_decision 仍恰 1 次。
func TestR33_RouteDecision_NestedFiresOnce(t *testing.T) {
	rec := &r33EventRecorder{}
	r := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("r33_route_n"),
		agent_v2.WithRoutingClassifier(&annotatedReplyAgent{name: "cls_n", target: "a"}),
	).AddBranch("a", &fixedReplyAgent{name: "wk_n", reply: "A_DONE"})
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("r33_seq_route")).AddStep("route", r)

	ctx := agent_v2.WithCtxOrchHooks(context.Background(), rec)
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := countRouteDecisions(rec); got != 1 {
		t.Fatalf("RT-H1: 嵌套 RoutingAgent route_decision 应恰 fire 1 次（不双 fire），got %d", got)
	}
}
