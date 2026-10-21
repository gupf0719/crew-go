package agent_v2_test

// 两轮独立审计对 `settleHistoryOnPause`（38f897b）各报的 finding 的交叉验证 + 回归。
//
//   - 审计一（份数视角）：Plan sub-agent / executor 两条 worker 路径 Resume 后任务输入
//     重复、随 HITL 轮次线性增长（1→2→3）。根因：worker 输入是节点内**每次执行现拼**的
//     （planner 被摘除、任务描述只在这里落地），Resume 重跑就重铸新身份 ⟹ 按身份认的
//     跨 Run 守卫恒不命中。其余编排的输入走 channel 随 cp round-trip、身份稳定，没这个洞。
//   - 审计二（判据边界视角）：Handoff 的 any-stream 守卫不看模式——SelfOnly 下把第二跳的
//     pause 结算整个挡掉（那条流「有答无问」，恰是 pause 要消灭的形状）；同根的镜像错活在
//     成功路径里（pre-existing）：共享模式第二跳**外部执行体**成功时输入落两份（per-slot
//     守卫认不出第一跳归属下那条）。矩阵四格两格各错一个方向。
//
// 修法：Handoff 两个 settle 位置按 historyMode 选守卫；Plan 首版用 (RunID, childNS, 内容)
// 哈希身份顶着，**后被追问「为什么要重造」推翻**——拼输入上移独立 plan_build_input 节点、
// 写 planBuiltInputCh 提交（Reflect feedback 同款、也是业内「上游拼一次状态承载」的形状），
// worker 重跑从 cp 恢复同一条，重造消除、身份天然稳定、哈希退役。本文件的份数断言全部
// 不看机制，两版下都得成立。

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// countUserBySlot 数 user 角色记录的 per-归属份数（只统计内容含 sub 的）。
func countUserBySlot(msgs []*schema.Message, sub string) map[string]int {
	out := map[string]int{}
	for _, m := range msgs {
		if m.Role != schema.RoleUser {
			continue
		}
		if sub != "" && !containsStr(m.Content, sub) {
			continue
		}
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		out[o]++
	}
	return out
}

func containsStr(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// mkPausePlan 造单步 Plan：planner 出一步给 name，执行完收尾。
func mkPausePlan(name string, w agent_v2.Agent) *agent_v2.PlanAgent {
	planner := agent_v2.Planner(func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			return []agent_v2.PlanStep{{Description: "查库存", SubAgentName: name}}, "", nil
		}
		return nil, "完成", nil
	})
	p := agent_v2.NewPlanAgent(agent_v2.Planner(planner), agent_v2.PlanName("planP"))
	p.AddSubAgent(name, w)
	return p
}

// TestHistPauseX_Plan_WorkerPauseResume_NoDup 审计一主 finding：中断→Resume 后任务输入
// 应恰 1 份（修前 2 份）。
func TestHistPauseX_Plan_WorkerPauseResume_NoDup(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ext := &pausingExternalAgent{name: "wExt", qid: "plan_pq"}
	p := mkPausePlan("w", ext)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_plan_pause")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	sess := agent_v2.NewRunSession()
	if _, err := p.Run(ctx, sess, schema.UserMessage("盘点仓库")); err == nil {
		t.Fatal("Run 应中断")
	}
	if _, err := p.Resume(ctx, sess.RunID); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	msgs := pauseHist(t, hs, ctx, "c_plan_pause")
	if got := countUserBySlot(msgs, "查库存")["w"]; got != 1 {
		t.Errorf("任务输入应恰 1 份（owner=w），实际 %d 份\n"+
			"  2 份 = worker 输入每次执行现拼、Resume 重跑重铸身份 ⟹ 跨 Run 守卫恒不命中\n"+
			"  ⟹ 挂起时落的那份挡不住 Resume 后的补记（同内容同归属重复，模型读两遍）", got)
		pauseDump(t, msgs)
	}
}

// TestHistPauseX_Plan_MultiRoundHITL_NoLinearGrowth 审计一实测的线性增长（1→2→3）。
func TestHistPauseX_Plan_MultiRoundHITL_NoLinearGrowth(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	// 前两次调用各中断一次、第三次成功
	calls := 0
	ext := &roundPausingAgent{name: "wExt2", qid: "plan_pq2", pauseAt: 1, calls: &calls}
	// roundPausingAgent 只在 pauseAt 那次中断一次；要两轮中断得再包一层
	w := &doublePausingAgent{inner: ext}
	p := mkPausePlan("w", w)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_plan_pause2")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	sess := agent_v2.NewRunSession()
	if _, err := p.Run(ctx, sess, schema.UserMessage("盘点仓库")); err == nil {
		t.Fatal("Run 应中断（第 1 次）")
	}
	if _, err := p.Resume(ctx, sess.RunID); err == nil {
		t.Fatal("Resume 1 应再次中断")
	}
	if _, err := p.Resume(ctx, sess.RunID); err != nil {
		t.Fatalf("Resume 2: %v", err)
	}

	msgs := pauseHist(t, hs, ctx, "c_plan_pause2")
	if got := countUserBySlot(msgs, "查库存")["w"]; got != 1 {
		t.Errorf("两轮 HITL 后任务输入应仍恰 1 份，实际 %d 份（修前随轮次线性增长 1→2→3）", got)
		pauseDump(t, msgs)
	}
}

// doublePausingAgent 前两次 Run 各抛一次中断，之后透传给 inner。
type doublePausingAgent struct {
	inner *roundPausingAgent
	n     int
}

func (a *doublePausingAgent) Run(_ context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.n++
	if a.n <= 2 {
		return nil, &compose_v2.InterruptError{
			Reason: "double_pause",
			Query:  &compose_v2.AskQuery{InterruptID: a.inner.qid, Question: "确认?"},
		}
	}
	out := schema.AssistantMessage("【外部产出】")
	sess.AddEvent(agent_v2.NewExitEvent(a.inner.name, out))
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.inner.name, out)}, nil
}

func (a *doublePausingAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *doublePausingAgent) Name() string        { return a.inner.name }
func (a *doublePausingAgent) Description() string { return "两轮中断的外部 agent" }

// TestHistPauseX_Plan_TwoRuns_DistinctGoals 锁「新 Run 各记各的」：每次 Run 里 build 节点
// 重新构造消息（新身份）⟹ 不被上一轮的记录吞掉——重复提问也得再记（哈希身份那版靠
// RunID 维度保这条，build 节点版靠「新 Run 新构造」天然成立；断言两版通用）。
func TestHistPauseX_Plan_TwoRuns_DistinctGoals(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ext := &externalAgent{name: "wExt3"}
	planner := agent_v2.Planner(func(_ context.Context, _ *agent_v2.RunSession, goal string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			return []agent_v2.PlanStep{{Description: "处理:" + goal, SubAgentName: "w"}}, "", nil
		}
		return nil, "完成", nil
	})
	p := agent_v2.NewPlanAgent(agent_v2.Planner(planner), agent_v2.PlanName("planT"))
	p.AddSubAgent("w", ext)

	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_plan_2runs")

	// 第三轮**重复目标甲**：内容与第一轮逐字相同——每次 Run 是新的一轮对话，重复提问也得
	// 各记各的（「这个 Run 记过没有」而非「曾经在历史里吗」——b469db8 的既有语义）。
	for _, goal := range []string{"目标甲", "目标乙", "目标甲"} {
		if _, err := p.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(goal)); err != nil {
			t.Fatalf("Run(%s): %v", goal, err)
		}
	}
	msgs := pauseHist(t, hs, ctx, "c_plan_2runs")
	for want, wantN := range map[string]int{"处理:目标甲": 2, "处理:目标乙": 1} {
		n := 0
		for _, m := range msgs {
			if m.Role == schema.RoleUser && containsStr(m.Content, want) {
				n++
			}
		}
		if n != wantN {
			t.Errorf("%q 应恰 %d 份，实际 %d 份\n"+
				"  少 = 身份派生没带 RunID，后一轮的任务被前一轮的记录吞掉", want, wantN, n)
			pauseDump(t, msgs)
		}
	}
}

// TestHistPauseX_Plan_ExecutorPauseResume_NoDup executor 路径同款（审计一实测同机制）。
func TestHistPauseX_Plan_ExecutorPauseResume_NoDup(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ext := &pausingExternalAgent{name: "execExt", qid: "plan_pq3"}
	planner := agent_v2.Planner(func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			return []agent_v2.PlanStep{{Description: "查库存"}}, "", nil // 无 SubAgentName ⟹ 走 executor
		}
		return nil, "完成", nil
	})
	p := agent_v2.NewPlanAgent(agent_v2.Planner(planner), agent_v2.PlanName("planE"))
	p.SetExecutor(ext)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_plan_exec")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	sess := agent_v2.NewRunSession()
	if _, err := p.Run(ctx, sess, schema.UserMessage("盘点仓库")); err == nil {
		t.Fatal("Run 应中断")
	}
	if _, err := p.Resume(ctx, sess.RunID); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	msgs := pauseHist(t, hs, ctx, "c_plan_exec")
	total := 0
	for _, m := range msgs {
		if m.Role == schema.RoleUser && containsStr(m.Content, "查库存") {
			total++
		}
	}
	if total != 1 {
		t.Errorf("executor 路径任务输入应恰 1 份，实际 %d 份", total)
		pauseDump(t, msgs)
	}
}

// signalingExternalAgent 成功并经 [agent_v2.HandoffTo] 转交给 target——业务自实现
// Agent（非 BasicAgent）的转交路径。原先写产出消息的 annotation，那条通道已删。
type signalingExternalAgent struct {
	name   string
	target string
}

func (a *signalingExternalAgent) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	out := schema.AssistantMessage("【" + a.name + "转交】")
	if err := agent_v2.HandoffTo(ctx, opts, a, a.target); err != nil {
		return nil, err
	}
	sess.AddEvent(agent_v2.NewExitEvent(a.name, out))
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, out)}, nil
}

func (a *signalingExternalAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *signalingExternalAgent) Name() string        { return a.name }
func (a *signalingExternalAgent) Description() string { return "转交信号外部 agent" }

// TestHistPauseX_Handoff_SelfOnly_PauseKeepsBranchInput 审计二 finding 1（本 commit 新引入）：
// SelfOnly 下每条流各自完整，第二跳中断时它的那份输入必须落——any-stream 守卫会把它整个
// 挡掉（front 那份同 nonce），那条流「有答无问」。
func TestHistPauseX_Handoff_SelfOnly_PauseKeepsBranchInput(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	front := &signalingExternalAgent{name: "front", target: "billing"}
	billing := &pausingExternalAgent{name: "billing", qid: "ho_so_pq"}
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("hoSO"),
		agent_v2.WithHandoffInitial("front"),
		agent_v2.WithHandoffHistoryMode(agent_v2.HandoffHistorySelfOnly),
	).AddAgent("front", front).AddAgent("billing", billing)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_ho_so")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	sess := agent_v2.NewRunSession()
	if _, err := ho.Run(ctx, sess, schema.UserMessage("我的原话")); err == nil {
		t.Fatal("Run 应在 billing 中断")
	}

	mid := countUserBySlot(pauseHist(t, hs, ctx, "c_ho_so"), "我的原话")
	if mid["front"] != 1 || mid["billing"] != 1 {
		t.Errorf("SelfOnly 挂起时每条流各自完整应 {front:1 billing:1}，实际 %v\n"+
			"  billing:0 = any-stream 守卫不看模式，front 那份把 billing 的 pause 结算挡掉\n"+
			"  ——SelfOnly 语义里 front 那份**不该**挡 billing 的（每条分支流要有问有答）", mid)
		pauseDump(t, pauseHist(t, hs, ctx, "c_ho_so"))
	}

	if _, err := ho.Resume(ctx, sess.RunID); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	fin := countUserBySlot(pauseHist(t, hs, ctx, "c_ho_so"), "我的原话")
	if fin["front"] != 1 || fin["billing"] != 1 {
		t.Errorf("SelfOnly Resume 后应仍 {front:1 billing:1}，实际 %v", fin)
	}
}

// TestHistPauseX_Handoff_Shared_ExternalSecondHop_SingleInput 审计二 finding 2
// （pre-existing，成功路径）：共享视图（默认 Rewrite）整场一条流，「同一条原始输入只记
// 一份」——第二跳外部执行体**成功**时 per-slot 补记认不出 front 那条，落成两份。
func TestHistPauseX_Handoff_Shared_ExternalSecondHop_SingleInput(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	front := &signalingExternalAgent{name: "front", target: "billing"}
	billing := &externalAgent{name: "billing"}
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("hoSH"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)

	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_ho_sh")

	if _, err := ho.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("我的原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	total := 0
	msgs := pauseHist(t, hs, ctx, "c_ho_sh")
	for _, m := range msgs {
		if m.Role == schema.RoleUser && m.Content == "我的原话" {
			total++
		}
	}
	if total != 1 {
		t.Errorf("共享单流里原始输入应恰 1 份，实际 %d 份\n"+
			"  2 份 = 成功路径的补记是 per-slot 的、认不出第一跳归属下那条——跟首版 pause\n"+
			"  犯的是同一个错，只是活在成功分支里", total)
		pauseDump(t, msgs)
	}
}
