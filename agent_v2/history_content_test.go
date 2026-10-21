package agent_v2_test

// 历史层的**内容**保全——这一族测的都不是「出口合法吗」而是「该留的内容还在吗」。
//
// 两者差得很远：出口那层无条件把流修成合法形状，所以上游任何「保住合法但丢内容」的退化，
// 合法性断言一律看不见。实测过：把截断的切点感知去掉、把 marker 的连带抽取去掉、把 skill
// listing 的历史写入删掉——现有那批断言全绿，而模型实际看到的东西已经缺了一块。
//
// 丢内容的表现形式有两种，都要断言：
//   - 真结果整条消失
//   - 真结果被换成「这次工具没返回结果」的占位——模型据此决定重试或放弃，等于告诉它一件
//     没发生的事

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// stampOwner 给消息盖上归属，让它能被 owner 为该 agent 的 prior 读回。
func stampOwner(owner string, m *schema.Message) *schema.Message {
	if m.Annotations == nil {
		m.Annotations = map[string]any{}
	}
	m.Annotations[agent_v2.HistoryAgentAnnotation] = owner
	return m
}

// dumpPrompt 打印模型实际收到的消息流——失败时没有这个基本没法定位。
func dumpPrompt(t *testing.T, tag string, msgs []*schema.Message) {
	t.Helper()
	t.Logf("%s（%d 条）:", tag, len(msgs))
	for i, m := range msgs {
		if m == nil {
			t.Logf("  [%2d] <nil>", i)
			continue
		}
		var tcs string
		for _, tc := range schema.GetToolCalls(m) {
			tcs += "<tc:" + tc.ID + ">"
		}
		t.Logf("  [%2d] %-9s %q %s %s", i, m.Role, m.Content, tcs, schema.GetToolResultID(m))
	}
}

const placeholderMark = "[未完成]"

// TestHistory_Truncation_CutPointKeepsRealResult：截断的切点必须避开配对。
//
// prior 换源之后历史里有工具往返，于是「保留最后 N 条」这种按条数切的做法会切在 assistant
// 和它的 tool_result 中间：assistant 被切走，结果留在头部成孤儿，出口清洗时被丢。合法性
// 完全没破——只是那一轮的工具结果**悄悄少了一条**。
//
// 这条此前零覆盖：实测把切点的配对感知去掉（直接按条数硬切），整套配对测试全绿。
func TestHistory_Truncation_CutPointKeepsRealResult(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	key := agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-cut"}
	seed := []*schema.Message{
		stampOwner("clerk", schema.UserMessage("旧问题")),
		stampOwner("clerk", schema.AssistantMessage("旧答复")),
		stampOwner("clerk", schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "T1", Type: "function",
			Function: schema.FunctionCall{Name: "查库存", Arguments: `{"sku":"A"}`},
		}})),
		stampOwner("clerk", schema.ToolResultMessage("T1", "查库存", "REAL_RESULT_MARK")),
		stampOwner("clerk", schema.AssistantMessage("跟进答复")),
		stampOwner("clerk", schema.UserMessage("较新问题")),
		stampOwner("clerk", schema.AssistantMessage("较新答复")),
		stampOwner("clerk", schema.UserMessage("最新问题")),
		stampOwner("clerk", schema.AssistantMessage("最新答复")),
	}
	if err := hs.Append(context.Background(), key, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	probe := &pairingProbeModel{noTools: true}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("clerk"),
		agent_v2.WithModel(probe),
		agent_v2.WithHistory(hs, "tenant-a"),
		// 9 条 prior、上限 6 条 ⟹ 朴素切点恰好落在 tool_result 那一格上。
		agent_v2.WithMaxHistoryPairs(3),
	)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-cut")
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("新问题")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	seen := probe.seen[0]
	dumpPrompt(t, "模型看到", seen)
	if !containsMsg(seen, "REAL_RESULT_MARK") {
		t.Error("窗口内的真工具结果丢了——切点切在了 assistant 和它的结果之间，" +
			"assistant 被切走、结果成孤儿被清掉。出口仍然合法，内容悄悄少一条")
	}
	if containsMsg(seen, placeholderMark) {
		t.Error("出现了占位符——真结果就在窗口内，不该被换成「这次没返回结果」")
	}
	if bad := assertPairingValid(seen); bad != "" {
		t.Errorf("配对违规: %s", bad)
	}
}

// TestHistory_MarkerHoist_KeepsSiblingRealResult：marker 提升不能把兄弟结果落下。
//
// 带 marker 的消息（skill body）会被连同声明它的 assistant 一起抽到队首保护。但那条
// assistant 可能同时声明了**别的**工具调用——它们的结果不带 marker，留在原地，等着被截断
// 切走。抽出来的那一块于是变成「声明了两个、只带回一个」，另一个被补成占位。
//
// 与既有的 MarkerSiblingNotSeparated 的区别：那条验的是「紧跟没被打断」，占位符也算紧跟，
// 所以兄弟结果被换成占位它看不见。这条补的是内容维度。
func TestHistory_MarkerHoist_KeepsSiblingRealResult(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	key := agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-sibling"}

	call := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "skill-1", Type: "function",
			Function: schema.FunctionCall{Name: "activate_skill", Arguments: `{"skill":"x"}`}},
		{ID: "biz-2", Type: "function",
			Function: schema.FunctionCall{Name: "查库存", Arguments: `{"sku":"A"}`}},
	})
	body := schema.ToolResultMessage("skill-1", "activate_skill", "SKILL_BODY_MARK")
	body.Annotations = map[string]any{agent_v2.SkillBodyAnnotation: true}
	biz := schema.ToolResultMessage("biz-2", "查库存", "BIZ_RESULT_MARK")

	var seed []*schema.Message
	// 算式（改动请一并核）：3 对更旧(6) + marker 轮(3) + 2 对较新(4) = 13 条；
	// WithMaxHistoryPairs(4) → limit=8 → 朴素尾截窗口是 prior[13-8:] = prior[5:]，
	// marker 轮在下标 6/7/8 —— **落在窗口内**。
	//
	// 所以本用例钉的**不是** marker 保护窗口（那个另有覆盖：见 history_pairing_test.go
	// 的 marker 在下标 0-1 那两条、以及 maxhistory_marker_internal_test.go），而是
	// **前置 + 认领**这条路径：protected 把 call/body 提到队首、biz 落在后面，跨距离
	// 的认领逻辑真的被走到了。
	//
	// ⚠️ 别为了"让 marker 真出窗口"把较新加到 4 对——实测过：那样 marker 是被保住了，
	// 但它的兄弟 biz 结果（**本来就没 marker**）被切走变成 [未完成]，本用例
	// KeepsSiblingRealResult 的主题直接不成立。两件事要分开测。
	for i := 0; i < 3; i++ { // 更旧的对话
		seed = append(seed,
			stampOwner("clerk", schema.UserMessage("更旧问题")),
			stampOwner("clerk", schema.AssistantMessage("更旧答复")))
	}
	seed = append(seed,
		stampOwner("clerk", call),
		stampOwner("clerk", body),
		stampOwner("clerk", biz))
	for i := 0; i < 2; i++ {
		seed = append(seed,
			stampOwner("clerk", schema.UserMessage("较新问题")),
			stampOwner("clerk", schema.AssistantMessage("较新答复")))
	}
	if err := hs.Append(context.Background(), key, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	probe := &pairingProbeModel{noTools: true}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("clerk"),
		agent_v2.WithModel(probe),
		agent_v2.WithHistory(hs, "tenant-a"),
		agent_v2.WithMaxHistoryPairs(4),
	)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-sibling")
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("新问题")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	seen := probe.seen[0]
	dumpPrompt(t, "模型看到", seen)
	if !containsMsg(seen, "SKILL_BODY_MARK") {
		t.Error("被保护的 skill body 没了——保护反转成了删除")
	}
	if !containsMsg(seen, "BIZ_RESULT_MARK") {
		t.Error("同一条 assistant 的兄弟业务结果没了——marker 只挂在其中一个结果上，" +
			"另一个不带 marker 留在原地被截断切走")
	}
	if containsMsg(seen, placeholderMark) {
		t.Error("出现了占位符——两个结果都在历史里，不该有任何一个被换成「这次没返回结果」")
	}
	if bad := assertPairingValid(seen); bad != "" {
		t.Errorf("配对违规: %s", bad)
	}
}

// TestHistory_DerivedInput_NotRecorded：框架拼出来的 sub-agent 输入不进历史。
//
// 流水线式的编排（Sequential / Reflect / Plan）step 之间不共享对话，下一步看到什么由编排
// 显式构造——"原始目标：X\n\n上一步的产出：Y"。这条消息**没有作者**：不是用户说的（用户
// 只说了原始目标那一句），也不是哪个 agent 说的（产出部分是引用的）。
//
// 它进历史的两个后果，当初把它移出 Sequential 消息流时就写清楚了：每条产出被复述两遍；
// 以及**以用户口吻再现别人的话**——回放这份记录的人会读到用户从没说过的段落。历史层是
// 后开的第二条记录，同一个判断该一起适用。
//
// 而它随时能从「原始目标 + 各步产出」重算：两样都在历史里，编排每轮也都重新拼。
func TestHistory_DerivedInput_NotRecorded(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	mkStep := func(name, reply string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(name),
			agent_v2.WithModel(&promptRecorder{resp: []*schema.Message{schema.AssistantMessage(reply)}}),
			agent_v2.WithHistory(hs, "tenant-a"),
		)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).
		AddStep("s1", mkStep("s1", "S1_OUT")).
		AddStep("s2", mkStep("s2", "S2_OUT"))

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-derived")
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("ORIGINAL_GOAL")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-derived"})
	dumpPrompt(t, "历史", got)

	// 形状：一条真实用户输入 + 两步各自的产出。跟 Sequential 自己那条消息流一致。
	if len(got) != 3 {
		t.Fatalf("历史 %d 条, want 3（原始目标 / s1 产出 / s2 产出）", len(got))
	}
	var s1OutSeen int
	for _, m := range got {
		if strings.Contains(m.Content, "S1_OUT") {
			s1OutSeen++
		}
	}
	if s1OutSeen != 1 {
		t.Errorf("第一步的产出在历史里出现 %d 次, want 1——多出来的那份嵌在第二步的"+
			"派生输入里，是同一句话的逐字副本", s1OutSeen)
	}
	// 派生模板的特征词不该出现——它是框架拼的，不是任何人说的。
	for _, m := range got {
		if strings.Contains(m.Content, "上一步（") {
			t.Errorf("派生模板进了历史: [%s] %q\n"+
				"  它以 user 角色再现了上一步 agent 说的话，回放这份记录的人会读到"+
				"用户从没说过的段落", m.Role, m.Content)
		}
	}
	// 真实用户输入必须还在——别把「不记派生」实现成「不记 step 1+ 的任何输入」。
	if !containsMsg(got, "ORIGINAL_GOAL") {
		t.Error("原始用户输入没了——step 0 的输入是真实发生的那一句，不是派生物")
	}
	// 归属仍然分得清谁说的。
	for _, m := range got {
		if _, ok := m.Annotations[agent_v2.HistoryAgentAnnotation].(string); !ok {
			t.Errorf("消息没有归属标记: %q", m.Content)
		}
	}
}

// TestHistory_DerivedInput_Kept_WhenNotRecomputable：拼进去的东西不在历史里时，这条派生
// 输入必须留下。
//
// 判据不是「是不是框架拼的」，是「拼的成分可不可重算」。Reflect 的反馈轮拼「原始目标 +
// 上轮输出 + **评审反馈**」——前两样在历史里，第三样不在：critique 来自 reflector，那条
// 分支已被 WithoutHistoryCtx 摘除。
//
// 丢了这条，历史里就只剩「actor 说了 A，然后又说了 B」——看不出中间发生过什么、B 为什么
// 跟 A 不一样。内部决策 LLM 的产出被挡在历史外，这条派生输入是它唯一的落脚点。
func TestHistory_DerivedInput_Kept_WhenNotRecomputable(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	actor := agent_v2.NewBasicAgent(
		agent_v2.WithName("actor"),
		agent_v2.WithModel(&promptRecorder{resp: []*schema.Message{
			schema.AssistantMessage("初稿"),
			schema.AssistantMessage("改进稿"),
		}}),
		agent_v2.WithHistory(hs, "tenant-a"),
	)
	var round int
	rf := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("rf"),
		agent_v2.WithReflectMaxRounds(2),
		agent_v2.WithReflector(func(_ context.Context, _, _ *schema.Message) (*agent_v2.ReflectOpinion, error) {
			round++
			return &agent_v2.ReflectOpinion{GoalMatched: round >= 2, Opinion: "CRITIQUE_MARK"}, nil
		}),
	).WithActor(actor)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-reflect")
	if _, err := rf.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("REFLECT_GOAL")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-reflect"})
	dumpPrompt(t, "历史", got)
	if !containsMsg(got, "CRITIQUE_MARK") {
		t.Error("评审反馈没进历史——它来自被摘除的 reflector，这条派生输入是它唯一的落脚点。" +
			"丢了之后历史里只剩「actor 说了 A 然后说了 B」，看不出中间发生过什么")
	}
	if !containsMsg(got, "改进稿") {
		t.Error("第二轮的产出没进历史")
	}
}

// TestHistory_DerivedInput_Kept_Plan：Plan 同理——「你的任务」来自被摘除的 planner。
//
// 三条执行路径共用同一个 ExecInputBuilder，这里覆盖注册的 sub-agent 与临时 sub-agent 两条
// （executor 那条互斥，见下一个用例）。历史开在顶层，Plan 底下所有子都沿用同一个写入器——
// 不这么套的话临时 sub-agent 那条压根不写历史，对它的断言就成了空的。
func TestHistory_DerivedInput_Kept_Plan(t *testing.T) {
	steps := []agent_v2.PlanStep{
		{Description: "STEP1_TASK_MARK", SubAgentName: "worker"},
		{Description: "STEP2_TASK_MARK", TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "tmp"}},
	}
	// Plan 是每步 replan：每轮返回**剩余**步骤。返回 nil 就直接收尾了，第二步不会执行。
	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) >= len(steps) {
			return nil, "全部完成", nil
		}
		return steps[len(executed):], "", nil
	}

	hs := agent_v2.NewInMemoryHistoryStore()
	worker := agent_v2.NewBasicAgent(
		agent_v2.WithName("worker"),
		agent_v2.WithModel(&promptRecorder{resp: []*schema.Message{schema.AssistantMessage("W_OUT")}}),
	)
	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("plan"),
		agent_v2.WithPlanModel(&promptRecorder{resp: []*schema.Message{schema.AssistantMessage("TMP_OUT")}}),
	).AddSubAgent("worker", worker)

	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(pa, agent_v2.WithAgentToolDescription("跑计划")))
	top := agent_v2.NewBasicAgent(
		agent_v2.WithName("top"),
		agent_v2.WithModel(&promptRecorder{resp: []*schema.Message{
			{Role: schema.RoleAssistant, ContentBlocks: []*schema.ContentBlock{{
				Type: "tool_call",
				Toolcall: &schema.ToolcallContentBlock{
					Id: "t1", Name: pa.Name(), Arguments: `{"input":"PLAN_GOAL"}`,
				},
			}}},
			schema.AssistantMessage("顶层收工"),
		}}),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHistory(hs, "tenant-a"),
	)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-plan")
	if _, err := top.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("外层请求")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-plan"})
	dumpPrompt(t, "历史", got)
	for _, mark := range []string{"STEP1_TASK_MARK", "STEP2_TASK_MARK"} {
		if !containsMsg(got, mark) {
			t.Errorf("%s 没进历史——任务描述来自被摘除的 planner，"+
				"这条派生输入是它唯一的落脚点，丢了就看不出每步被要求做什么", mark)
		}
	}
	// 两条路径都得真的走到，否则上面的断言是空的。
	for _, mark := range []string{"W_OUT", "TMP_OUT"} {
		if !containsMsg(got, mark) {
			t.Errorf("%s 没进历史——这条执行路径没被真正走到，对它的断言等于没测", mark)
		}
	}
}

// TestHistory_DerivedInput_Kept_PlanExecutor：Plan 的第三条执行路径（自定义 executor）。
//
// 单独一条是因为它跟前两条互斥——SetExecutor 接管所有步骤，注册的 sub-agent 和临时
// sub-agent 都不会被调到。
func TestHistory_DerivedInput_Kept_PlanExecutor(t *testing.T) {
	steps := []agent_v2.PlanStep{{Description: "EXEC_TASK_MARK"}}
	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) >= len(steps) {
			return nil, "完成", nil
		}
		return steps[len(executed):], "", nil
	}

	hs := agent_v2.NewInMemoryHistoryStore()
	executor := agent_v2.NewBasicAgent(
		agent_v2.WithName("executor"),
		agent_v2.WithModel(&promptRecorder{resp: []*schema.Message{schema.AssistantMessage("EXEC_OUT")}}),
	)
	pa := agent_v2.NewPlanAgent(planner, agent_v2.PlanName("plan")).SetExecutor(executor)

	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(pa, agent_v2.WithAgentToolDescription("跑计划")))
	top := agent_v2.NewBasicAgent(
		agent_v2.WithName("top"),
		agent_v2.WithModel(&promptRecorder{resp: []*schema.Message{
			{Role: schema.RoleAssistant, ContentBlocks: []*schema.ContentBlock{{
				Type: "tool_call",
				Toolcall: &schema.ToolcallContentBlock{
					Id: "t1", Name: pa.Name(), Arguments: `{"input":"EXEC_GOAL"}`,
				},
			}}},
			schema.AssistantMessage("顶层收工"),
		}}),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHistory(hs, "tenant-a"),
	)

	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-plan-exec")
	if _, err := top.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("外层请求")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, _ := hs.Load(ctx, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-plan-exec"})
	dumpPrompt(t, "历史", got)
	if !containsMsg(got, "EXEC_TASK_MARK") {
		t.Error("任务描述没进历史——它来自被摘除的 planner")
	}
	if !containsMsg(got, "EXEC_OUT") {
		t.Error("executor 的产出没进历史——这条路径没被真正走到，断言等于没测")
	}
}

// contentsOfTest 打印用（history_shape_matrix_test.go 里那个 contentsOf 在白盒包）。
func contentsOfTest(msgs []*schema.Message) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		if m == nil {
			out = append(out, "<nil>")
			continue
		}
		out = append(out, "  ["+string(m.Role)+"] "+m.Content)
	}
	return out
}
