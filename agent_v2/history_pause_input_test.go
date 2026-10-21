package agent_v2_test

// 「输入的持久点在跨进边界那一刻，不在执行完成那一刻」的回归（对照文档「结论七」）。
//
// 问题形状：编排里的**外部执行体**（远程 agent / 业务自研壳——不知道历史层存在）抛中断时，
// 边界补记是成功后才做的、这次没触发，输入只活在父编排的调用栈里。经父 Resume 那条路能
// 自愈（step 重跑成功、补记跟上），但调用方直接 Resume 子时输入永久缺席——历史落成
// 「有答无问」。修法：11 个边界位置在「可 Resume 的退出」时也结算一次输入
// （settleHistoryOnPause），判据复用成功路径输入半边的同一套。
//
// **核心断言是「中断那一刻输入就已在历史里」**——不是等 Resume 之后才查。这正是主流性质：
// 完成与否只影响产出，不影响输入。

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// pausingExternalAgent 第一次 Run 抛中断（自填稳定 InterruptID），之后成功。
// 模拟「远程执行体自己管挂起、被再次调用时续上」的外部实现。
type pausingExternalAgent struct {
	name   string
	qid    string
	paused bool
}

func (a *pausingExternalAgent) Run(_ context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	if !a.paused {
		a.paused = true
		return nil, &compose_v2.InterruptError{
			Reason: "external_pause",
			Query:  &compose_v2.AskQuery{InterruptID: a.qid, Question: "外部执行体要确认?"},
		}
	}
	out := schema.AssistantMessage("【外部产出】" + strings.TrimSpace(in.Content))
	sess.AddEvent(agent_v2.NewExitEvent(a.name, out))
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, out)}, nil
}

func (a *pausingExternalAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *pausingExternalAgent) Name() string        { return a.name }
func (a *pausingExternalAgent) Description() string { return "会中断的外部 agent" }

func pauseHist(t *testing.T, hs agent_v2.HistoryStore, ctx context.Context, conv string) []*schema.Message {
	t.Helper()
	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{Owner: "t", ConvID: conv})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return msgs
}

func pauseCount(msgs []*schema.Message, role schema.MessageRole, exact string) int {
	n := 0
	for _, m := range msgs {
		if m.Role == role && m.Content == exact {
			n++
		}
	}
	return n
}

func pauseDump(t *testing.T, msgs []*schema.Message) {
	t.Helper()
	for i, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		t.Logf("  [%d] %s owner=%s %s", i, m.Role, o, m.Content)
	}
}

// TestHistPause_Sequential_InputDurableAtInterrupt 核心用例：外部 step 中断后、**任何
// Resume 之前**，用户原话已在历史里；父 Resume 成功后不重复。
func TestHistPause_Sequential_InputDurableAtInterrupt(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ext := &pausingExternalAgent{name: "extPause", qid: "seq_pq"}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seqP"))
	seq.AddStep("theStep", ext)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_seq_pause")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	sess := agent_v2.NewRunSession()
	if _, err := seq.Run(ctx, sess, schema.UserMessage("我的原话")); err == nil {
		t.Fatal("Run 应中断")
	}

	// **挂起那一刻**输入就得是事实——这是本轮修复的核心性质
	mid := pauseHist(t, hs, ctx, "c_seq_pause")
	if got := pauseCount(mid, schema.RoleUser, "我的原话"); got != 1 {
		t.Errorf("中断后、Resume 前：用户原话应已在历史（1 条），实际 %d 条", got)
		pauseDump(t, mid)
	}
	// 归属 = 父给的位置（跟成功路径的补记一致）
	if len(mid) > 0 {
		if o, _ := mid[0].Annotations[agent_v2.HistoryAgentAnnotation].(string); o != "theStep" {
			t.Errorf("挂起时落的输入归属应是父给的位置 theStep，实际 %q", o)
		}
	}

	// 父 Resume：step 重跑成功、补记跟上——输入不重复、产出落下
	if _, err := seq.Resume(ctx, sess.RunID); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	fin := pauseHist(t, hs, ctx, "c_seq_pause")
	if got := pauseCount(fin, schema.RoleUser, "我的原话"); got != 1 {
		t.Errorf("Resume 后用户原话应仍恰 1 条（跨 Run 守卫挡重复），实际 %d 条", got)
		pauseDump(t, fin)
	}
	hasOut := false
	for _, m := range fin {
		if strings.Contains(m.Content, "【外部产出】") {
			hasOut = true
		}
	}
	if !hasOut {
		t.Errorf("Resume 成功后外部产出应在历史里")
		pauseDump(t, fin)
	}
}

// TestHistPause_BasicAgentStep_NoDouble 对照面：子是 BasicAgent（preamble 在原 Run 里
// 已记输入）时，pause 结算不得再落一份。
func TestHistPause_BasicAgentStep_NoDouble(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	// stub model 第一轮发 tool_call、工具抛中断；Resume 后终稿（history_resume_test 同款范式）
	askTool := tool.NewFuncTool("askp", "问一下", map[string]any{"type": "object",
		"properties": map[string]any{"q": map[string]any{"type": "string"}}},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "bp_ask"); ok {
				return tool.Success("用户回答：" + ans), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "bp_ask", Question: "确认下?"},
			}
		})
	tk := tool.NewToolkit()
	tk.Register(askTool)
	leaf := agent_v2.NewBasicAgent(
		agent_v2.WithName("leafP"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "tc_bp", Function: schema.FunctionCall{Name: "askp", Arguments: `{"q":"x"}`},
			}}),
			schema.AssistantMessage("终稿"),
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(4))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seqBP"))
	seq.AddStep("s1", leaf)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_bp")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	sess := agent_v2.NewRunSession()
	_, err := seq.Run(ctx, sess, schema.UserMessage("我的原话"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("Run 应中断, got %v", err)
	}

	mid := pauseHist(t, hs, ctx, "c_bp")
	if got := pauseCount(mid, schema.RoleUser, "我的原话"); got != 1 {
		t.Errorf("中断时输入应恰 1 条（preamble 已记，pause 结算被范围回执挡住），实际 %d 条", got)
		pauseDump(t, mid)
	}

	if _, err := seq.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
		map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("确认")})); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	fin := pauseHist(t, hs, ctx, "c_bp")
	if got := pauseCount(fin, schema.RoleUser, "我的原话"); got != 1 {
		t.Errorf("Resume 后输入应仍恰 1 条，实际 %d 条", got)
		pauseDump(t, fin)
	}
}

// TestHistPause_Parallel_PerBranch SelfOnly 扇出语义不变：中断分支落自己那份、
// 兄弟分支不受影响；Resume 后每分支各恰 1 份输入。
func TestHistPause_Parallel_PerBranch(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ext := &pausingExternalAgent{name: "extB", qid: "par_pq"}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("parP"))
	par.AddBranch("bPause", ext)
	par.AddBranch("bOK", &externalAgent{name: "extOK"})

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_par_pause")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	sess := agent_v2.NewRunSession()
	if _, err := par.Run(ctx, sess, schema.UserMessage("我的原话")); err == nil {
		t.Fatal("Run 应中断")
	}

	// 挂起时：中断分支那份输入已落（owner=bPause）
	mid := pauseHist(t, hs, ctx, "c_par_pause")
	perSlot := map[string]int{}
	for _, m := range mid {
		if m.Role == schema.RoleUser && m.Content == "我的原话" {
			o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
			perSlot[o]++
		}
	}
	if perSlot["bPause"] != 1 {
		t.Errorf("挂起时中断分支应已落自己那份输入，实际 %v", perSlot)
		pauseDump(t, mid)
	}

	if _, err := par.Resume(ctx, sess.RunID); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	fin := pauseHist(t, hs, ctx, "c_par_pause")
	perSlot = map[string]int{}
	for _, m := range fin {
		if m.Role == schema.RoleUser && m.Content == "我的原话" {
			o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
			perSlot[o]++
		}
	}
	if perSlot["bPause"] != 1 || perSlot["bOK"] != 1 {
		t.Errorf("Resume 后每分支各恰 1 份输入（SelfOnly 每条分支流各自完整），实际 %v", perSlot)
		pauseDump(t, fin)
	}
}

// TestHistPause_Loop_Round2_NoGarbage Loop 第 2 轮中断：那轮的「输入」是上一轮产出
// （assistant 角色）——pause 结算不得把它当用户的话落进去。
func TestHistPause_Loop_Round2_NoGarbage(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	// 第 1 轮成功、第 2 轮中断、第 3 轮成功
	var calls int
	body := &roundPausingAgent{name: "extLoop", qid: "loop_pq", pauseAt: 2, calls: &calls}
	lp := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("loopP"),
		agent_v2.WithLoopMaxIterations(2),
	)
	lp.WithBody(body)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_loop_pause")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	sess := agent_v2.NewRunSession()
	if _, err := lp.Run(ctx, sess, schema.UserMessage("我的原话")); err == nil {
		t.Fatal("Run 应在第 2 轮中断")
	}

	mid := pauseHist(t, hs, ctx, "c_loop_pause")
	users := 0
	for _, m := range mid {
		if m.Role == schema.RoleUser {
			users++
		}
	}
	if users != 1 {
		t.Errorf("挂起时 user 记录应恰 1 条（原话；第 2 轮的输入是上轮产出、Role 过滤掉），实际 %d 条", users)
		pauseDump(t, mid)
	}

	if _, err := lp.Resume(ctx, sess.RunID); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	fin := pauseHist(t, hs, ctx, "c_loop_pause")
	users = 0
	for _, m := range fin {
		if m.Role == schema.RoleUser {
			users++
		}
	}
	if users != 1 {
		t.Errorf("Resume 后 user 记录应仍恰 1 条，实际 %d 条", users)
		pauseDump(t, fin)
	}
}

// roundPausingAgent 在第 pauseAt 次调用时中断一次，其余成功。
type roundPausingAgent struct {
	name    string
	qid     string
	pauseAt int
	calls   *int
	paused  bool
}

func (a *roundPausingAgent) Run(_ context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	*a.calls++
	if *a.calls == a.pauseAt && !a.paused {
		a.paused = true
		return nil, &compose_v2.InterruptError{
			Reason: "round_pause",
			Query:  &compose_v2.AskQuery{InterruptID: a.qid, Question: "第二轮要确认?"},
		}
	}
	out := schema.AssistantMessage("【第" + strings.Repeat("I", *a.calls) + "轮】")
	sess.AddEvent(agent_v2.NewExitEvent(a.name, out))
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, out)}, nil
}

func (a *roundPausingAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *roundPausingAgent) Name() string        { return a.name }
func (a *roundPausingAgent) Description() string { return "按轮中断的外部 agent" }

// TestHistPause_Handoff_FirstHopExternal Handoff 的两半都要钉住：
//   - 第一跳是外部执行体、什么都没记就中断 ⟹ any-stream 查不到 ⟹ pause 落输入（1 份）
//   - 共享单流语义：Resume 后整场仍只有 1 份原始输入
//
// 它是 handoff 位置那道 any-stream 守卫的专属哨兵——方向写反（换成 per-slot 或恒 true/false）
// 时要么这条红（落不下）、要么 XProcResume_NoDupInput 红（落两份）。
func TestHistPause_Handoff_FirstHopExternal(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ext := &pausingExternalAgent{name: "extHop", qid: "ho_pq"}
	ho := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("hoP"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", ext)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_ho_pause")
	ctx = compose_v2.WithCheckpointerCtx(ctx, cper)

	sess := agent_v2.NewRunSession()
	if _, err := ho.Run(ctx, sess, schema.UserMessage("我的原话")); err == nil {
		t.Fatal("Run 应中断")
	}

	mid := pauseHist(t, hs, ctx, "c_ho_pause")
	if got := pauseCount(mid, schema.RoleUser, "我的原话"); got != 1 {
		t.Errorf("外部第一跳中断时输入应落 1 份（什么都没记过，any-stream 查不到），实际 %d 份", got)
		pauseDump(t, mid)
	}

	if _, err := ho.Resume(ctx, sess.RunID); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	fin := pauseHist(t, hs, ctx, "c_ho_pause")
	if got := pauseCount(fin, schema.RoleUser, "我的原话"); got != 1 {
		t.Errorf("Resume 后共享单流里原始输入应仍恰 1 份，实际 %d 份", got)
		pauseDump(t, fin)
	}
}
