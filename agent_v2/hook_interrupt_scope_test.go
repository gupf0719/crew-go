package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// pauseOnStartHook 在 flag 亮时于 OnAgentStart 抛中断（plan_chat_web 暂停 gate 范式）。
type pauseOnStartHook struct {
	hook.NoOpHooks
	flag *atomic.Bool
}

func (h *pauseOnStartHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	if h.flag.Load() {
		h.flag.Store(false)
		return ctx, &compose_v2.InterruptError{Reason: "user_pause",
			Query: &compose_v2.AskQuery{InterruptID: "pause-1", Question: "已暂停，继续吗？"}}
	}
	return ctx, nil
}

// TestHookInterrupt_TopLevelRejected 钉住**顶层 OnAgentStart 抛中断被拒、不再伪装成可续的中断**。
//
// ⚠️ 修复前的现状（实测）：`IsInterrupt=true` 但挂起档为 0——前端照常渲染问题、用户答完
// Resume 撞 `ErrNoPendingCheckpoint`，被误导到最后一步才失败。中断的契约是「抛出 ⟹ 有档
// 可续」，而 OnAgentStart 在图之前、落档的机器没起来。
//
// 「落初始档」不可行：引擎自己的不变量拒绝「pending interrupts but no frontier」的档。
// 所以拒绝，文案引导挂对位置（OnModelEnd / sub-agent）。
func TestHookInterrupt_TopLevelRejected(t *testing.T) {
	var flag atomic.Bool
	flag.Store(true)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithMaxRounds(3),
		agent_v2.WithHooks(&pauseOnStartHook{flag: &flag}),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("done")})),
	)
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	if err == nil {
		t.Fatal("顶层 hook 中断该被拒成错误，实际 err=nil")
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Errorf("拒绝必须转成**非中断**错误（cond 先例：%%w 保链会让 IsInterrupt 仍 true "+
			"而档没落，用户答完 Resume 死锁）——实际 IsInterrupt=true: %v", err)
	}
	if !strings.Contains(err.Error(), "OnModelEnd") {
		t.Errorf("错误文案该引导挂对位置，实际 %v", err)
	}
	if pend, _ := cp.ListPending(ctx, sess.RunID); len(pend) != 0 {
		t.Errorf("拒绝路径不该留档，实际 %d 份", len(pend))
	}
}

// TestHookInterrupt_NestedStillLandsCheckpoint 钉住**嵌套的 OnAgentStart 中断照旧放行且落档**
// ——interruptHook 范式（挂 sub-agent 的暂停 gate）靠这条工作：中断冒出 sub.Run 被父图的
// 节点路径接住 ⟹ 校验 + 落父档 ⟹ 可续。顶层拒绝不能把这条也拒了。
func TestHookInterrupt_NestedStillLandsCheckpoint(t *testing.T) {
	var flag atomic.Bool
	flag.Store(true)
	child := agent_v2.NewBasicAgent(
		agent_v2.WithName("child"), agent_v2.WithMaxRounds(3),
		agent_v2.WithHooks(&pauseOnStartHook{flag: &flag}),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("child done")})),
	)
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("s1", child)
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	_, err := seq.Run(ctx, sess, schema.UserMessage("hi"))
	if _, isInt := compose_v2.IsInterrupt(err); !isInt {
		t.Fatalf("嵌套 hook 中断该照旧作为中断冒出（范式路径），实际 %v", err)
	}
	pend, _ := cp.ListPending(ctx, sess.RunID)
	if len(pend) == 0 {
		t.Fatal("嵌套 hook 中断该落档（父图节点路径），实际 0 份——范式被顶层拒绝误伤了")
	}
	// 闭环：Resume 后 flag 已清，child 正常跑完
	evs, errR := seq.Resume(ctx, sess.RunID)
	if errR != nil {
		t.Fatalf("Resume: %v", errR)
	}
	if fm := agent_v2.FinalMessage(evs); fm == nil || !strings.Contains(fm.Content, "child done") {
		t.Errorf("Resume 后该正常收尾，实际 %v", fm)
	}
}

// nsProbeHook 记录 OnAgentStart 看到的 graphNS——顶层拒绝判据的地基。
type nsProbeHook struct {
	hook.NoOpHooks
	label string
	seen  *map[string]string
}

// OnAgentStart 记本次调用的**判据取值**：引擎盖的节点执行章（`GetCurrentNodeID`）。
func (h *nsProbeHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	(*h.seen)[h.label] = string(compose_v2.GetCurrentNodeID(ctx))
	return ctx, nil
}

// TestHookInterrupt_ScopeJudgmentBasis 钉住**「有没有人接得住」判据的地基**：
// `GetCurrentNodeID(ctx)`（引擎在节点入口盖、进新图即清的执行章）在子 agent 调用
// 路径上必须非空，只有真顶层才是空。
//
// ⚠️ 为什么单独钉：`WrapAgentStartErr` 拒不拒中断**全靠这一个判据**。任何一条子 agent
// 路径上章为空，那条路的 hook 中断就会被**误判成没人接而拒**——而挂 sub-agent 的暂停
// gate（interruptHook 范式）正是文档推荐的用法，误伤它等于废掉范式。
//
// 判据曾是 `GetGraphNS(ctx)`（ns 字符串空不空），两侧各有误差——见
// TestHookInterrupt_JudgmentBasisIsEngineStampNotNS 钉的两个形态。
func TestHookInterrupt_ScopeJudgmentBasis(t *testing.T) {
	seen := map[string]string{}
	mkChild := func(label string) *agent_v2.BasicAgent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("child_"+label), agent_v2.WithDescription("子任务"),
			agent_v2.WithMaxRounds(3),
			agent_v2.WithHooks(&nsProbeHook{label: label, seen: &seen}),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})),
		)
	}

	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
		AddStep("s1", mkChild("orchestration"))
	if _, err := seq.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("编排路: %v", err)
	}

	tk := agent_v2_newToolkit()
	tk.Register(agent_v2.AgentAsTool(mkChild("agent_as_tool")))
	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("child_agent_as_tool"), schema.AssistantMessage("done")})),
	)
	if _, err := parent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("AgentAsTool 路: %v", err)
	}

	if _, err := mkChild("top").Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("顶层路: %v", err)
	}

	for _, label := range []string{"orchestration", "agent_as_tool"} {
		if stamp, ok := seen[label]; !ok || stamp == "" {
			t.Errorf("子 agent 路径 %q 没有节点执行章（ok=%v）——这条路上的 hook 中断会被"+
				"误判成没人接而拒，挂 sub-agent 的暂停 gate 范式在这条路上被废掉", label, ok)
		}
	}
	if stamp, ok := seen["top"]; !ok || stamp != "" {
		t.Errorf("顶层不该有节点执行章，实际 %q（ok=%v）——顶层拒绝会失效，"+
			"假暂停（IsInterrupt=true 但无档）回来", stamp, ok)
	}
}

// interruptAtStartHook 在 OnAgentStart 抛中断——暂停 gate 的最小形态。
type interruptAtStartHook struct {
	hook.NoOpHooks
}

func (h *interruptAtStartHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	return ctx, &compose_v2.InterruptError{
		Reason: "user paused",
		Query:  &compose_v2.AskQuery{InterruptID: "pause-gate", Question: "继续?"},
	}
}

// TestHookInterrupt_JudgmentBasisIsEngineStampNotNS 钉住换判据修掉的**两侧误差**——
// 这两个形态在旧判据（`GetGraphNS(ctx) == ""`）下都给出相反答案。
//
//	形态 A（旧判据放行、实为假暂停）：业务在**顶层**自己 push 了 ns（租户前缀等），
//	  旧判据看 ns 非空 ⟹ 当成嵌套放行 ⟹ IsInterrupt=true 但没有任何档，用户答完
//	  Resume 撞 ErrNoPendingCheckpoint。新判据看章（顶层无章）⟹ 拒。
//	形态 B（旧判据误拒、实为父节点接得住）：NodeFunc 里裸调 sub.Run 不 push ns，旧判据
//	  看 ns 空 ⟹ 当成顶层拒 ⟹ 父节点本来接得住的中断被废。新判据看章（在节点执行范围内，
//	  章非空）⟹ 放行，中断冒进父图节点路径落档。
func TestHookInterrupt_JudgmentBasisIsEngineStampNotNS(t *testing.T) {
	mkGated := func(name string) *agent_v2.BasicAgent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(name), agent_v2.WithDescription("gate"),
			agent_v2.WithMaxRounds(3),
			agent_v2.WithHooks(&interruptAtStartHook{}),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})),
		)
	}

	// ── 形态 A：顶层 + 业务自己 push 了 ns ⟹ 必须拒（且不可重试）──
	ctxA := compose_v2.WithGraphNS(context.Background(), "tenant-a")
	_, errA := mkGated("gated_top").Run(ctxA, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if errA == nil {
		t.Fatalf("形态 A: 顶层 hook 中断该被拒")
	}
	if _, isInt := compose_v2.IsInterrupt(errA); isInt {
		t.Errorf("形态 A: 业务 push 了 ns 就放行 ⟹ 假暂停（IsInterrupt=true 但无档）: %v", errA)
	}
	if compose_v2.IsRetryable(errA) {
		t.Errorf("形态 A: 接线错误该不可重试（实测配了 retry 的编排会重试 4 次）: %v", errA)
	}

	// ── 形态 B：NodeFunc 里裸调 sub.Run（不 push ns）⟹ 必须放行并落档 ──
	// gate 用**一次性** flag：Resume 时越过 gate，才测得到存档那一步的真实行为
	// （无条件 gate 会在 Resume 时又中断一次，把后面的断言变成空转）。
	cper := compose_v2.NewInMemoryCheckpointer()
	var bFlag atomic.Bool
	bFlag.Store(true)
	sub := agent_v2.NewBasicAgent(
		agent_v2.WithName("gated_child"), agent_v2.WithDescription("gate"),
		agent_v2.WithMaxRounds(3),
		agent_v2.WithHooks(&pauseOnStartHook{flag: &bFlag}),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("ok")})),
	)
	bld := compose_v2.NewBuilder()
	bld.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 故意不 push ns（旧判据在这儿把它误判成顶层）；不传 session ⟹ 它是这张图那个任务的一部分
		_, err := sub.Run(ctx, nil, schema.UserMessage("go"))
		return nil, err
	})
	bld.AddEdge(compose_v2.StartNode, "host")
	g, err := bld.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	sid := "stamp-basis-sid"
	errB := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	if _, isInt := compose_v2.IsInterrupt(errB); !isInt {
		t.Fatalf("形态 B: 节点内裸调 sub.Run 的 hook 中断该放行（父节点路径接得住）, got %v", errB)
	}
	// 正向断言：真的落了档、真的有那条挂起——否则「放行」只是把假暂停换个地方
	pending, err := cper.ListPending(context.Background(), sid)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(pending) == 0 {
		t.Fatalf("形态 B: 放行后必须有挂起档（否则仍是假暂停）")
	}
	// ⚠️ **放行 ≠ 这个形态可用**（已知限制，这一段钉住它）：子层是同一个任务的一部分，却没有自己的
	// ns——它的图跑在父图自己的槽 (sid, "") 上。续跑时父图的存档还在那个槽里，子层的图（BasicAgent，
	// 身份跟父图不同）撞上它 ⟹ ErrGraphIDConflict。两张图身份恰好相同时（比如父图也是 BasicAgent 的）
	// 守卫拦不住，子层直接接着跑父图的存档。给新 session 则入口直接拒（它不属于所在的任务）。真要
	// 闭环得走 RunSubAgent（它给子层一个自己的槽）。
	resumeErr := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{"pause-1": schema.UserMessage("继续")}))
	if !errors.Is(resumeErr, compose_v2.ErrGraphIDConflict) {
		t.Fatalf("已知限制变了：裸调 sub.Run 没有自己的 ns，续跑时子层的图该撞上父图留在同一个槽里的存档"+
			"（ErrGraphIDConflict），got %v——若这是有意修复，同步 enterRunTask 的说明与 todo 里那一条", resumeErr)
	}
}
