package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// 会话历史的归属改用**编排注册位置**（分支名 / 步骤名 / 转交目标名），不再用 agent 自己的
// 名字。这组用例钉住四条：同名实例分得开、位置不含运行时序号、顶层回落 agent 名、跨 Run 稳定。

func slotOwners(t *testing.T, hs agent_v2.HistoryStore, conv string) map[string]int {
	t.Helper()
	msgs, err := hs.Load(context.Background(),
		agent_v2.HistoryKey{Owner: "t", ConvID: conv})
	if err != nil {
		t.Fatalf("Load(%s): %v", conv, err)
	}
	out := map[string]int{}
	for _, m := range msgs {
		if m == nil {
			continue
		}
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		out[o]++
	}
	return out
}

// TestHistorySlot_SameNameDistinctInstances_NotMerged：两个**同名**的独立 agent 挂两个
// 分支时，归属必须分开。
//
// 用 agent 名标归属时这两个分支都是 "worker"，产出合并成一条流、下一轮互相把对方的消息
// 当自己的 prior 读回来。框架只校验**分支名**唯一，从不校验 agent 名——所以靠"拒绝同实例"
// 兜不住这个形状（它们是两个不同的对象）。
//
// **sanity-revert**：把 basic_builder.go 的 `historyOwner(ctx, name)` 改回 `name`，
// 本测试 FAIL 并报归属只有 1 种。
func TestHistorySlot_SameNameDistinctInstances_NotMerged(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	mk := func() agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("worker"), // 故意同名
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("done")})),
			agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("br1", mk()).AddBranch("br2", mk())

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-dupname")
	if _, err := par.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("Q")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := slotOwners(t, hs, "conv-dupname")
	if len(got) != 2 {
		t.Errorf("归属 %d 种 %v, want 2（br1 / br2 各一种）——两个同名 agent 的产出合并成"+
			"一条流了，下一轮会互相把对方的消息当自己的历史读回", len(got), got)
	}
	for _, want := range []string{"br1", "br2"} {
		if got[want] == 0 {
			t.Errorf("归属里没有 %q（实际 %v）——归属该是分支注册名，不是 agent 名", want, got)
		}
	}
}

// TestHistorySlot_SameInstanceMultiBranch_NotMerged：同一个实例挂两个分支同样分得开。
//
// 这是 best-of-N 采样的形状（ISSUE-80 用 lane 隔离根治 events 串台后解锁的能力）。
func TestHistorySlot_SameInstanceMultiBranch_NotMerged(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	// **必须用无状态替身**：两个分支共享同一个 agent 实例 ⟹ 共享同一个 model，
	// 而 newStubModel 有可变的 idx 字段、无锁，并发调会被 -race 判定为数据竞争
	// （那是测试替身的性质，不是被测逻辑的问题）。
	shared := agent_v2.NewBasicAgent(
		agent_v2.WithName("worker"),
		agent_v2.WithModel(&statelessStub{reply: "done"}),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("br1", shared).AddBranch("br2", shared)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-sameinst")
	if _, err := par.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("Q")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := slotOwners(t, hs, "conv-sameinst"); len(got) != 2 {
		t.Errorf("归属 %d 种 %v, want 2——同实例多分支该按分支位置分开", len(got), got)
	}
}

// TestHistorySlot_HandoffOwnerHasNoHop：转交的归属不能带跳数。
//
// graphNS 是 `agent_<name>_hop<N>`（每跳 cp 要隔离，必须带）；归属**必须不带**——hops
// 存 ctx、每个 Run 从 0 重新计数，同一个 agent 在 Run 1 记成 hop1、Run 2 续接时变 hop0，
// 带上它就跨 Run 读不回自己的历史，而那正是这一层存在的理由。
//
// **sanity-revert**：把 handoff_agent.go 的 `WithHistorySlot(subCtx, activeName)` 改成
// 传 childNS，本测试 FAIL 并报归属含 hop。
func TestHistorySlot_HandoffOwnerHasNoHop(t *testing.T) {
	// **必须真转交**：只挂一个 agent 从不转交的话 hop 恒为 0，"不含 hop" 的断言平凡
	// 通过、抓不到实现里带了跳数。这里 front 转交给 billing 让第二跳的 hop 真是 1。
	// 两个 agent 还故意**同名**，顺带验证归属靠位置而非名字区分。
	hs := agent_v2.NewInMemoryHistoryStore()
	front := agent_v2.NewBasicAgent(agent_v2.WithName("sameName"),
		agent_v2.WithModel(newStubModel([]*schema.Message{transferToolCall("tc1", "billing")})),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(3))
	billing := agent_v2.NewBasicAgent(agent_v2.WithName("sameName"),
		agent_v2.WithModel(&statelessStub{reply: "billing done"}),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(3))
	ho := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("ho"),
		agent_v2.WithHandoffInitial("front")).
		AddAgent("front", front).AddAgent("billing", billing)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-hop")
	if _, err := ho.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("Q")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := slotOwners(t, hs, "conv-hop")
	if len(got) == 0 {
		t.Fatal("历史为空——用例没跑到写入点")
	}
	for owner := range got {
		if strings.Contains(owner, "hop") {
			t.Errorf("归属 %q 含 hop（全部：%v）——hops 每个 Run 从 0 重算，"+
				"带上它跨 Run 就读不回自己的历史", owner, got)
		}
	}
	// 两跳都该在，且各自是自己的注册名——两个 agent 的 Name() 都是 "sameName"，
	// 归属能分开只可能靠位置。
	for _, want := range []string{"front", "billing"} {
		if got[want] == 0 {
			t.Errorf("归属里没有 %q（实际 %v）——第二跳没跑到，或归属没用 AddAgent 的注册名",
				want, got)
		}
	}
	if got["sameName"] > 0 {
		t.Errorf("归属里出现了 agent 名 sameName（实际 %v）——两个 agent 同名，"+
			"用它做归属会合并成一条流", got)
	}
}

// TestHistorySlot_TopLevelFallsBackToAgentName：顶层没有父编排，回落 agent 名。
//
// 顶层同一时刻只有一个 agent 在跑、不存在撞的问题；回落到名字比空串对业务回看友好。
func TestHistorySlot_TopLevelFallsBackToAgentName(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	solo := agent_v2.NewBasicAgent(agent_v2.WithName("solo"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("done")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-top")
	if _, err := solo.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("Q")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := slotOwners(t, hs, "conv-top")
	if len(got) != 1 || got["solo"] == 0 {
		t.Errorf("顶层归属 %v, want 只有 \"solo\"——顶层该回落 agent 名，不能是空串", got)
	}
}

// TestHistorySlot_StableAcrossRuns：跨 Run 重建 agent，归属必须不变。
//
// per-connection 重建是这一层的主场景：每轮都新建 agent 实例，若归属跟着变就再也读不回
// 上一轮的历史。位置来自构造期声明的注册名，天然稳定。
func TestHistorySlot_StableAcrossRuns(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	newPar := func() agent_v2.Agent {
		mk := func() agent_v2.Agent {
			return agent_v2.NewBasicAgent(agent_v2.WithName("w"),
				agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("done")})),
				agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
		}
		return agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
			AddBranch("br1", mk()).AddBranch("br2", mk())
	}
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-2run")
	for i := 0; i < 2; i++ {
		if _, err := newPar().Run(ctx, agent_v2.NewRunSession(),
			schema.UserMessage(fmt.Sprintf("Q%d", i))); err != nil {
			t.Fatalf("Run %d: %v", i, err)
		}
	}
	got := slotOwners(t, hs, "conv-2run")
	if len(got) != 2 {
		t.Errorf("两轮后归属 %d 种 %v, want 2——跨 Run 重建 agent 后归属该保持稳定", len(got), got)
	}
	// 两轮都写了，每个位置的条数应当翻倍（不是第二轮换了个归属另起炉灶）
	for owner, n := range got {
		if n < 2 {
			t.Errorf("归属 %q 只有 %d 条——第二轮可能记到别的归属下了（全部：%v）", owner, n, got)
		}
	}
}

// statelessStub 是无状态的 model 替身——可安全并发调用（同一个 agent 实例挂多个分支时
// 两个分支会共享它）。
type statelessStub struct{ reply string }

func (m *statelessStub) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(m.reply), nil
}

func (m *statelessStub) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *statelessStub) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHistorySlot_RewriteUsesDisplayName：跨归属改写的文案用 agent 名，不用位置。
//
// 归属键改成位置（br1 / step2）解决了唯一性，但那个值直接进 `For context: [X] said:`
// 就成了模型看不懂的槽位名。所以位置和显示名分开存：**位置用于比较，名字用于显示**。
//
// **sanity-revert**：把 history.go 里 `rewriteCrossAgentMessage(m, historyDisplayName(m))`
// 的第二参改回 `owner`，本测试 FAIL 并报文案里是 br1 而不是 agent 名。
func TestHistorySlot_RewriteUsesDisplayName(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	mk := func(n string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName(n),
			agent_v2.WithModel(&statelessStub{reply: n + " 的产出"}),
			agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("br1", mk("researcher")).AddBranch("br2", mk("reviewer"))

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-display")
	if _, err := par.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("Q")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 历史里两个字段都该有：归属是位置，显示名是 agent 名
	msgs, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-display"})
	var hasDisplay bool
	for _, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		d, _ := m.Annotations[agent_v2.HistoryAgentDisplayAnnotation].(string)
		if o == "br1" && d == "researcher" {
			hasDisplay = true
		}
	}
	if !hasDisplay {
		t.Errorf("没找到「归属=br1 且显示名=researcher」的消息——位置与显示名该分开存\n%s",
			dumpHist(msgs))
	}

	// 第三方 agent 用 Rewrite 视图读，文案里该出现 agent 名而不是 br1
	probe := &promptRecorder{resp: []*schema.Message{schema.AssistantMessage("ok")}}
	reader := agent_v2.NewBasicAgent(agent_v2.WithName("reader"),
		agent_v2.WithModel(probe), agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	rctx := agent_v2.WithHandoffHistoryModeCtx(ctx, agent_v2.HandoffHistoryRewrite)
	if _, err := reader.Run(rctx, agent_v2.NewRunSession(), schema.UserMessage("看历史")); err != nil {
		t.Fatalf("reader.Run: %v", err)
	}
	var rewritten []string
	for _, turn := range probe.seen {
		for _, m := range turn {
			if strings.Contains(m.Content, "For context") {
				rewritten = append(rewritten, m.Content)
			}
		}
	}
	if len(rewritten) == 0 {
		t.Fatal("没产生跨归属改写——用例没走到 Rewrite 路径")
	}
	joined := strings.Join(rewritten, "\n")
	for _, want := range []string{"researcher", "reviewer"} {
		if !strings.Contains(joined, want) {
			t.Errorf("改写文案里没有 agent 名 %q——模型看到的会是位置名，看不懂那是谁\n%s",
				want, joined)
		}
	}
	for _, bad := range []string{"[br1]", "[br2]"} {
		if strings.Contains(joined, bad) {
			t.Errorf("改写文案里出现了位置名 %q——该用显示名\n%s", bad, joined)
		}
	}
}

// TestHistorySlot_PlanExecutorNoCollisionWithSubAgentName：Plan 的默认 executor 位置
// 不能跟用户注册的 sub-agent 名撞。
//
// 用户注册名（AddSubAgent）和 executor 的位置在**同一个命名空间**里。executor 那处若用
// 裸字面量 "executor"，业务真注册一个叫 "executor" 的 sub-agent 时两者归属相同——两个
// 不同 agent 的历史合并成一条流，正是这套改动要消灭的形状。而 AddSubAgent 只查 NS 合法性
// 和重名，没有保留名检查，撞了不会 fail-fast。
//
// **sanity-revert**：把 plan_agent.go 里 `string(executorDispatchWorker)` 改回裸
// "executor"，本测试 FAIL 并报两个 agent 的产出落进了同一归属。
func TestHistorySlot_PlanExecutorNoCollisionWithSubAgentName(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	registered := agent_v2.NewBasicAgent(agent_v2.WithName("regAgent"),
		agent_v2.WithModel(&statelessStub{reply: "OUT_FROM_REGISTERED"}),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	fallbackExec := agent_v2.NewBasicAgent(agent_v2.WithName("execAgent"),
		agent_v2.WithModel(&statelessStub{reply: "OUT_FROM_DEFAULT_EXECUTOR"}),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))

	// 关键：业务把 sub-agent 注册成 "executor"——跟框架内部那个位置同名。
	//
	// Plan 是 **replan-每步**：每执行完一步就重新问 planner 要剩余步骤。所以要让两步都
	// 真跑，得按已执行步数返回剩余——一次性返两步再无脑收尾的话第二步根本不执行，断言
	// 就永远看不到碰撞（这个 fixture 第一版就栽在这，测试假绿）。
	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string,
		executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		switch len(executed) {
		case 0:
			return []agent_v2.PlanStep{{Description: "S1", SubAgentName: "executor"}}, "", nil
		case 1:
			return []agent_v2.PlanStep{{Description: "S2"}}, "", nil // 走默认 executor
		default:
			return nil, "done", nil
		}
	}
	pa := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("plan"))
	pa.AddSubAgent("executor", registered)
	pa.SetExecutor(fallbackExec)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-execcollide")
	if _, err := pa.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("Q")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	msgs, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: "conv-execcollide"})
	byOwner := map[string][]string{}
	for _, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		byOwner[o] = append(byOwner[o], strings.TrimSpace(m.Content))
	}
	for owner, contents := range byOwner {
		joined := strings.Join(contents, " | ")
		if strings.Contains(joined, "OUT_FROM_REGISTERED") &&
			strings.Contains(joined, "OUT_FROM_DEFAULT_EXECUTOR") {
			t.Errorf("归属 %q 下同时有注册 sub-agent 和默认 executor 的产出——"+
				"两个不同 agent 的历史合并成一条流了\n  %s", owner, joined)
		}
	}
}
