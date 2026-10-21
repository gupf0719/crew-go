package agent_v2_test

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// PlanAgent 的临时子 agent 配置随这一步进存档（planExecStep.TmpSubAgent）：下面几条钉的是「读回来的就是规划时
// 那一份」——字段逐个经存档往返、规划器之后再改自己的对象不影响、两个分支共用一个 PlanAgent 实例时各读各的。

// tmpCall 是捕获模型一次 Generate 看到的东西。
type tmpCall struct {
	label         string
	system        []string
	users         []string
	tools         []string
	hasToolResult bool
}

type tmpRecorder struct {
	mu    sync.Mutex
	calls []tmpCall
}

func (r *tmpRecorder) add(c tmpCall) {
	r.mu.Lock()
	r.calls = append(r.calls, c)
	r.mu.Unlock()
}

func (r *tmpRecorder) all() []tmpCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]tmpCall(nil), r.calls...)
}

// tmpCapModel 记下每次调用看到的 system / user / 绑定的工具（并发安全）；reply 决定返回什么，nil 时回显 system。
type tmpCapModel struct {
	rec   *tmpRecorder
	label string
	tools []string
	reply func(tmpCall) *schema.Message
}

func (m *tmpCapModel) Generate(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	c := tmpCall{label: m.label, tools: append([]string(nil), m.tools...)}
	for _, msg := range in {
		switch msg.Role {
		case schema.RoleSystem:
			c.system = append(c.system, msg.Content)
		case schema.RoleUser:
			c.users = append(c.users, msg.Content)
		case schema.RoleTool:
			c.hasToolResult = true
		}
	}
	m.rec.add(c)
	if m.reply != nil {
		return m.reply(c), nil
	}
	return schema.AssistantMessage("echo:" + strings.Join(c.system, "|")), nil
}

func (m *tmpCapModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m *tmpCapModel) WithTools(infos []*schema.ToolInfo) (model.ChatModel, error) {
	names := make([]string, 0, len(infos))
	for _, i := range infos {
		names = append(names, i.Name)
	}
	sort.Strings(names)
	cp := *m
	cp.tools = names
	return &cp, nil
}

// tmpToolkit 注册几个什么都不干的工具。
func tmpToolkit(names ...string) *tool.Toolkit {
	tk := tool.NewToolkit()
	for _, n := range names {
		n := n
		tk.Register(tool.NewFuncTool(n, "tool "+n, map[string]any{"type": "object", "properties": map[string]any{}},
			func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("ok:" + n), nil }))
	}
	return tk
}

// 配置经存档往返之后逐字段读得回：在拼输入的那个节点（ExecInputBuilder）里中断、换一个 PlanAgent 实例续跑——
// 续跑时输入（Prompt 前缀）与临时子 agent（Name / System / ToolNames）都在读回的配置上重新构造，四个字段都承重。
// 只在子 agent 的工具里中断的话，续跑时子 agent 靠自己的存档恢复，System / Prompt 不再被读，丢了也测不出来。
func TestPlan_TmpConfigFieldsSurviveCheckpoint(t *testing.T) {
	want := agent_v2.TmpSubAgentConfig{Name: "tmp_named", System: "你是审计员 <SYS> & \"q\"", Prompt: "PROMPT-前缀", ToolNames: []string{"t2", "t1"}}
	rec := &tmpRecorder{}
	var buildMu sync.Mutex
	var seen []agent_v2.TmpSubAgentConfig
	mk := func(label string) *agent_v2.PlanAgent {
		mdl := &tmpCapModel{rec: rec, label: label}
		planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
			if len(executed) == 0 {
				c := want
				c.ToolNames = append([]string(nil), want.ToolNames...)
				return []agent_v2.PlanStep{{Description: "做第一件事", TmpSubAgent: &c}}, "", nil
			}
			return nil, "completed:" + executed[0].Result, nil
		}
		builder := func(ctx context.Context, _ string, _ []agent_v2.ExecutedStep, cur agent_v2.PlanStep, _ []int) (*schema.Message, error) {
			if cur.TmpSubAgent != nil {
				buildMu.Lock()
				seen = append(seen, *cur.TmpSubAgent)
				buildMu.Unlock()
			}
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗"}); err != nil {
				return nil, err
			}
			return schema.UserMessage("INPUT"), nil
		}
		return agent_v2.NewPlanAgent(planner,
			agent_v2.PlanName("plan_tmp_wire"),
			agent_v2.WithPlanModel(mdl),
			agent_v2.WithPlanToolkit(tmpToolkit("t1", "t2")),
			agent_v2.WithExecInputBuilder(builder),
		).SetExecutor(agent_v2.NewBasicAgent(agent_v2.WithName("dummy"), agent_v2.WithModel(mdl)))
	}
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := mk("first").Run(ctx, sess, schema.UserMessage("任务"))
	ie, isInt := agent_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("前置：首跑应停在拼输入那一问：%v", err)
	}
	events, err := mk("second").Resume(ctx, sess.RunID,
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("批准")}))
	if err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	if len(seen) != 2 || !reflect.DeepEqual(seen[1], want) {
		t.Fatalf("续跑时拼输入看到的配置应与规划时逐字段相同：%+v（期望 %+v）", seen, want)
	}
	var sub *tmpCall
	for _, c := range rec.all() {
		if c.label == "second" {
			c := c
			sub = &c
		}
	}
	if sub == nil {
		t.Fatal("续跑后临时子 agent 应调过模型")
	}
	if strings.Join(sub.system, "|") != want.System {
		t.Errorf("临时子 agent 的 system 应是配置的 System：%q", sub.system)
	}
	if len(sub.users) == 0 || sub.users[len(sub.users)-1] != want.Prompt+"\n\nINPUT" {
		t.Errorf("临时子 agent 的输入应带 Prompt 前缀：%q", sub.users)
	}
	if !reflect.DeepEqual(sub.tools, []string{"t1", "t2"}) {
		t.Errorf("临时子 agent 应绑定配置的工具：%q", sub.tools)
	}
	named := false
	for _, e := range events {
		named = named || e.AgentName == want.Name
	}
	if !named {
		t.Errorf("续跑的事件里应有临时子 agent 的名字 %q", want.Name)
	}
}

// 规划器返回之后又改了自己手里那个配置对象（包括 ToolNames 的底层数组）：临时子 agent 用的仍是规划时的内容。
// 框架写进计划的是一份拷贝——直接存规划器的指针时，规划器复用并改写同一个对象（并行分支各规划一次）就会串。
func TestPlan_TmpConfigIsCopiedAtPlanning(t *testing.T) {
	rec := &tmpRecorder{}
	mdl := &tmpCapModel{rec: rec, label: "m"}
	shared := &agent_v2.TmpSubAgentConfig{Name: "tmp_copy", System: "ORIG", ToolNames: []string{"t1"}}
	planner := func(_ context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		if len(executed) == 0 {
			return []agent_v2.PlanStep{{Description: "d", TmpSubAgent: shared}}, "", nil
		}
		return nil, "completed:" + executed[0].Result, nil
	}
	builder := func(context.Context, string, []agent_v2.ExecutedStep, agent_v2.PlanStep, []int) (*schema.Message, error) {
		shared.System = "MUTATED"  // 这一步写进计划之后，规划器又改了自己的对象
		shared.ToolNames[0] = "t2" // 同一个底层数组
		return schema.UserMessage("IN"), nil
	}
	pa := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("plan_tmp_copy"),
		agent_v2.WithPlanModel(mdl),
		agent_v2.WithPlanToolkit(tmpToolkit("t1", "t2")),
		agent_v2.WithExecInputBuilder(builder),
	).SetExecutor(agent_v2.NewBasicAgent(agent_v2.WithName("dummy"), agent_v2.WithModel(mdl)))
	if _, err := pa.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("x")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	calls := rec.all()
	if len(calls) != 1 {
		t.Fatalf("临时子 agent 应调模型 1 次：%+v", calls)
	}
	if strings.Join(calls[0].system, "|") != "ORIG" {
		t.Errorf("临时子 agent 用上了规划之后被改写的 System：%q", calls[0].system)
	}
	if !reflect.DeepEqual(calls[0].tools, []string{"t1"}) {
		t.Errorf("临时子 agent 绑定了规划之后被改写的工具：%q（期望 [t1]）", calls[0].tools)
	}
}

// 同一个 PlanAgent 实例挂在 ParallelAgent 的两个分支上，两个分支的临时步骤描述相同、配置不同：R 分支先成功收尾，
// L 分支停在子 agent 的提问上；换新实例续跑，L 读到自己的配置、R 不重跑。配置放在实例上的表里时，R 收尾的清理
// 曾经误清 L 还要用的那份。
func TestPlan_TmpConfigSharedInstanceParallelBranches(t *testing.T) {
	rec := &tmpRecorder{}
	reply := func(c tmpCall) *schema.Message {
		sys := strings.Join(c.system, "|")
		if strings.Contains(sys, "ASK") && !c.hasToolResult {
			return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "tc_" + sys, Type: "function",
				Function: schema.FunctionCall{Name: "ask", Arguments: `{}`}}})
		}
		return schema.AssistantMessage("final:" + sys)
	}
	askTK := tool.NewToolkit()
	askTK.Register(tool.NewFuncTool("ask", "ask user", map[string]any{"type": "object", "properties": map[string]any{}},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗"})
			if err != nil {
				return nil, err
			}
			return tool.Success("answered:" + ans.Content), nil
		}))
	mk := func() *agent_v2.ParallelAgent {
		mdl := &tmpCapModel{rec: rec, label: "m", reply: reply}
		planner := func(ctx context.Context, _ *agent_v2.RunSession, _ string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
			if len(executed) > 0 {
				return nil, "done:" + executed[0].Result, nil
			}
			ns := compose_v2.GetGraphNS(ctx)
			if ns[strings.LastIndex(ns, "/")+1:] == "L" {
				return []agent_v2.PlanStep{{Description: "same", TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "tmpL", System: "SYS-L ASK", ToolNames: []string{"ask"}}}}, "", nil
			}
			return []agent_v2.PlanStep{{Description: "same", TmpSubAgent: &agent_v2.TmpSubAgentConfig{Name: "tmpR", System: "SYS-R"}}}, "", nil
		}
		pa := agent_v2.NewPlanAgent(planner,
			agent_v2.PlanName("plan_tmp_parallel"),
			agent_v2.WithPlanModel(mdl),
			agent_v2.WithPlanToolkit(askTK),
		).SetExecutor(agent_v2.NewBasicAgent(agent_v2.WithName("dummy"), agent_v2.WithModel(mdl)))
		return agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).AddBranch("L", pa).AddBranch("R", pa)
	}
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := mk().Run(ctx, sess, schema.UserMessage("go"))
	ie, isInt := agent_v2.IsInterrupt(err)
	if !isInt || len(ie.AllQueries()) != 1 {
		t.Fatalf("前置：首跑应只因 L 的子 agent 提问而中断：%v", err)
	}
	events, err := mk().Resume(ctx, sess.RunID,
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("批准")}))
	if err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	if agent_v2.FinalMessage(events) == nil {
		t.Fatal("续跑应产出 final")
	}
	counts := map[string]int{}
	for _, c := range rec.all() {
		counts[strings.Join(c.system, "|")]++
	}
	if counts["SYS-L ASK"] != 2 || counts["SYS-R"] != 1 {
		t.Errorf("L 应读到自己的配置、R 不重跑：%v（期望 SYS-L ASK=2、SYS-R=1）", counts)
	}
}
