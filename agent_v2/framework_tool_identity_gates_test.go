package agent_v2_test

// 「是不是框架工具」按实例判、不按名字判的哨兵：业务工具恰好叫框架工具的名字（respond / plan 是规划器的
// 内部名，final_output / ask_user 在没配 WithOutputSchema / WithAskUser 时是普通名字）时，每一道豁免框架
// 工具的闸都照常管它。曾经这几道闸按名字判，业务工具就此绕过审批、续跑重复执行、跳过中间件（均实测）。
// 框架工具自己仍然豁免——那一半由各闸既有的用例钉着（它们用的是 NewAskUserTool / WithAskUser 这些真框架工具）。

import (
	"context"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// gateModel 按脚本逐轮回放（超出脚本返回文本答复「完」），记下每轮绑到的工具名与收到的消息。
type gateModel struct {
	mu     sync.Mutex
	script []*schema.Message
	n      int
	bound  []string
	rounds [][]string
	inputs [][]*schema.Message
}

func (m *gateModel) Generate(_ context.Context, in []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rounds = append(m.rounds, append([]string(nil), m.bound...))
	m.inputs = append(m.inputs, append([]*schema.Message(nil), in...))
	out := schema.AssistantMessage("完")
	if m.n < len(m.script) {
		out = m.script[m.n]
	}
	m.n++
	return out, nil
}

func (m *gateModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *gateModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	names := make([]string, 0, len(tools))
	for _, t := range tools {
		if t != nil {
			names = append(names, t.Name)
		}
	}
	m.mu.Lock()
	m.bound = names
	m.mu.Unlock()
	return m, nil
}

func (m *gateModel) round(t *testing.T, i int) ([]string, []*schema.Message) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if i >= len(m.rounds) {
		t.Fatalf("fixture 没搭对：模型只被调了 %d 次，拿不到第 %d 轮", len(m.rounds), i+1)
	}
	return m.rounds[i], m.inputs[i]
}

func tcall(id, name, args string) schema.ToolCall {
	return schema.ToolCall{ID: id, Function: schema.FunctionCall{Name: name, Arguments: args}}
}

func callMsg(calls ...schema.ToolCall) *schema.Message { return schema.AssistantToolCallMessage(calls) }

// namedBizTool 是业务自己写的工具（不带框架标记），每次执行计一次数、返回 content。
func namedBizTool(name string, n *atomic.Int32, content string) tool.Tool {
	return tool.NewFuncTool(name, "业务工具 "+name, map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			n.Add(1)
			return tool.Success(content), nil
		})
}

func kitOf(tools ...tool.Tool) *tool.Toolkit {
	kit := tool.NewToolkit()
	kit.RegisterAll(tools...)
	return kit
}

// TestIdentityGate_ApprovalCoversBusinessToolNamedRespond：ApprovalAsk 下，业务工具叫 respond 也停在审批、工具体
// 没跑。曾经按名字豁免，它不经审批就执行了。
func TestIdentityGate_ApprovalCoversBusinessToolNamedRespond(t *testing.T) {
	var body atomic.Int32
	m := &gateModel{script: []*schema.Message{callMsg(tcall("c1", "respond", "{}"))}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("ap"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(kitOf(namedBizTool("respond", &body, "ok"))), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if ie, ok := compose_v2.IsInterrupt(err); !ok || ie.Reason != "tool_approval" {
		t.Fatalf("业务工具叫 respond 也该停在审批，实际 err=%v", err)
	}
	if n := body.Load(); n != 0 {
		t.Errorf("审批之前工具体不该跑，跑了 %d 次", n)
	}
}

// TestIdentityGate_MemoCoversBusinessToolNamedPlan：同一批里兄弟工具（ask_user）中断，续跑之后业务工具 plan 不再
// 执行一次——它进了工具账。曾经按名字排除在工具账外，续跑时又执行一次（扣款类工具就是重复扣款）。
func TestIdentityGate_MemoCoversBusinessToolNamedPlan(t *testing.T) {
	var body atomic.Int32
	m := &gateModel{script: []*schema.Message{callMsg(
		tcall("p1", "plan", "{}"), tcall("a1", agent_v2.AskUserToolName, `{"question":"颜色？"}`))}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("memo"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(kitOf(namedBizTool("plan", &body, "已排期"))), agent_v2.WithAskUser())
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：应停在 ask_user，实际 err=%v", err)
	}
	answers := map[string]*schema.Message{}
	for _, q := range ie.AllQueries() {
		answers[q.InterruptID] = schema.UserMessage("蓝")
	}
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers)); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if n := body.Load(); n != 1 {
		t.Errorf("兄弟工具中断续跑后，业务工具 plan 应只执行 1 次（进了工具账），实际 %d 次", n)
	}
}

// TestIdentityGate_MiddlewareCoversBusinessToolNamedRespond：业务工具叫 respond 也过工具中间件。曾经按名字跳过。
func TestIdentityGate_MiddlewareCoversBusinessToolNamedRespond(t *testing.T) {
	var body, mw atomic.Int32
	m := &gateModel{script: []*schema.Message{callMsg(tcall("c1", "respond", "{}"))}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("mw"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(kitOf(namedBizTool("respond", &body, "ok"))),
		agent_v2.WithToolMiddleware(func(next agent_v2.ToolEndpoint) agent_v2.ToolEndpoint {
			return func(ctx context.Context, name string, args map[string]any) (*tool.Result, error) {
				mw.Add(1)
				return next(ctx, name, args)
			}
		}))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if body.Load() != 1 || mw.Load() != 1 {
		t.Errorf("业务工具叫 respond 也该过中间件：工具体 %d 次、中间件 %d 次（应各 1 次）", body.Load(), mw.Load())
	}
}

// TestIdentityGate_ToolFilterCoversBusinessToolNamedFinalOutput：没配 WithOutputSchema 时，业务工具叫 final_output
// 也受 ToolFilter 白名单约束。曾经按名字豁免、恒保留。
func TestIdentityGate_ToolFilterCoversBusinessToolNamedFinalOutput(t *testing.T) {
	var n atomic.Int32
	m := &gateModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("tf"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(kitOf(namedBizTool(agent_v2.FinalOutputToolName, &n, "x"), namedBizTool("beta", &n, "y"))),
		agent_v2.WithToolFilter(func(context.Context, *schema.Message) []string { return []string{"beta"} }))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if bound, _ := m.round(t, 0); !slices.Equal(bound, []string{"beta"}) {
		t.Errorf("白名单只有 beta，业务的 final_output 应被裁掉，实际绑到 %v", bound)
	}
}

// TestIdentityGate_ToolSearchCoversBusinessToolNamedAskUser：没配 WithAskUser 时，业务工具叫 ask_user 也参与渐进
// 披露——被 Deferred 命中就先不进绑定列表。曾经按名字豁免、恒在列表里。
func TestIdentityGate_ToolSearchCoversBusinessToolNamedAskUser(t *testing.T) {
	var n atomic.Int32
	m := &gateModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("ts"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(kitOf(namedBizTool(agent_v2.AskUserToolName, &n, "x"), namedBizTool("mcp_x", &n, "y"))),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: func(tool.Tool) bool { return true }}))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if bound, _ := m.round(t, 0); !slices.Equal(bound, []string{"tool_search"}) {
		t.Errorf("业务的 ask_user 被 Deferred 命中，首轮应只看到 tool_search，实际 %v", bound)
	}
}

// TestIdentityGate_RepeatDetectionCoversBusinessToolNamedFinalOutput：没配 WithOutputSchema 时，业务工具叫
// final_output 也计入重复调用检测——同样参数连调 3 次后下一轮收到提醒。曾经按名字对链透明，永远不提醒。
func TestIdentityGate_RepeatDetectionCoversBusinessToolNamedFinalOutput(t *testing.T) {
	var n atomic.Int32
	same := func(id string) *schema.Message { return callMsg(tcall(id, agent_v2.FinalOutputToolName, `{"q":1}`)) }
	m := &gateModel{script: []*schema.Message{same("c1"), same("c2"), same("c3")}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("rp"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(kitOf(namedBizTool(agent_v2.FinalOutputToolName, &n, "x"))),
		agent_v2.WithRepeatToolReminder(&agent_v2.RepeatToolConfig{Enabled: true}))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_, in := m.round(t, 3)
	for _, msg := range in {
		if msg != nil && msg.Annotations[agent_v2.RepeatToolReminderAnnotation] == true {
			return
		}
	}
	t.Errorf("业务工具 final_output 同样参数连调 3 次，第 4 轮应收到重复调用提醒，实际没有")
}

// TestIdentityGate_TruncationCoversBusinessToolNamedFinalOutput：没配 WithOutputSchema 时，业务工具叫
// final_output 的结果也按 WithToolResultMaxBytes 截断。曾经按名字豁免、原样塞进上下文。
func TestIdentityGate_TruncationCoversBusinessToolNamedFinalOutput(t *testing.T) {
	var n atomic.Int32
	m := &gateModel{script: []*schema.Message{callMsg(tcall("c1", agent_v2.FinalOutputToolName, "{}"))}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("tr"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(kitOf(namedBizTool(agent_v2.FinalOutputToolName, &n, strings.Repeat("长", 3000)))),
		agent_v2.WithToolResultMaxBytes(300))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_, in := m.round(t, 1)
	for _, msg := range in {
		if msg != nil && msg.Role == schema.RoleTool {
			if len(msg.Content) >= 9000 {
				t.Errorf("业务工具 final_output 的结果应被截断，实际 %d 字节", len(msg.Content))
			}
			return
		}
	}
	t.Fatal("fixture 没搭对：第 2 轮输入里没有工具结果")
}

// catTool 是实现了 tool.Categorizer 的业务工具。
type catTool struct{ tool.Tool }

func (catTool) Category() string { return "retrieval" }

type categoryHooks struct {
	hook.NoOpHooks
	mu   sync.Mutex
	cats []any
}

func (h *categoryHooks) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	h.mu.Lock()
	h.cats = append(h.cats, in.Extra["category"])
	h.mu.Unlock()
	return ctx, nil
}

func (h *categoryHooks) seen() []any {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]any(nil), h.cats...)
}

// TestToolCategory_FollowsDispatchedInstance：工具的 trace 类型跟着实际派发的那件走——框架的 ask_user 顶掉了
// 业务同名、实现了 Categorizer 的工具时不标类型；没配 WithAskUser 时那件业务工具照常标它自己的类型。曾经在 Run
// 入口扫业务 toolkit 按名字建表，框架那件被派发时也被标成业务那件的 retrieval。
func TestToolCategory_FollowsDispatchedInstance(t *testing.T) {
	run := func(t *testing.T, withAskUser bool) []any {
		var n atomic.Int32
		h := &categoryHooks{}
		opts := []agent_v2.Option{agent_v2.WithName("cat"),
			agent_v2.WithModel(&gateModel{script: []*schema.Message{callMsg(tcall("a1", agent_v2.AskUserToolName, `{"question":"颜色？"}`))}}),
			agent_v2.WithToolkit(kitOf(catTool{namedBizTool(agent_v2.AskUserToolName, &n, "biz")})), agent_v2.WithHooks(h)}
		if withAskUser {
			opts = append(opts, agent_v2.WithAskUser())
		}
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
		_, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
		if _, isInt := compose_v2.IsInterrupt(err); isInt != withAskUser {
			t.Fatalf("fixture 没搭对：withAskUser=%v 时是否中断应与之相同，实际 err=%v", withAskUser, err)
		}
		return h.seen()
	}
	if got := run(t, true); len(got) != 1 || got[0] != nil {
		t.Errorf("派发的是框架的 ask_user，不该标业务同名工具的类型，实际 %v", got)
	}
	if got := run(t, false); len(got) != 1 || got[0] != "retrieval" {
		t.Errorf("没配 WithAskUser 时派发的是业务那件，应标它的 retrieval，实际 %v", got)
	}
}

// TestToolCategory_NotInheritedFromCaller：嵌套时 ctx 从调用方继承——本 agent 的工具不该按名字查到调用方的类型表
// （这里直接往 ctx 推一张「什么都标 retrieval」的表模拟）。曾经本 agent 没有 Categorizer 工具时不推自己的表，
// 继承了调用方的。
func TestToolCategory_NotInheritedFromCaller(t *testing.T) {
	var n atomic.Int32
	h := &categoryHooks{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("child"),
		agent_v2.WithModel(&gateModel{script: []*schema.Message{callMsg(tcall("c1", "lookup", "{}"))}}),
		agent_v2.WithToolkit(kitOf(namedBizTool("lookup", &n, "ok"))), agent_v2.WithHooks(h))
	ctx := compose_v2.WithToolCategoryLookup(context.Background(), func(string) string { return "retrieval" })
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := h.seen(); len(got) != 1 || got[0] != nil {
		t.Errorf("本 agent 的 lookup 没实现 Categorizer，不该继承调用方的类型，实际 %v", got)
	}
}

// TestIdentityGate_GrantedTransferSurvivesFilterAndDeferral：被授予转交的候选，transfer_to_agent 在 ToolFilter 白名单
// 不含它、渐进披露的 Deferred 也命中它时，照样留在绑定列表里——绑定侧注册的那件带着框架标记（这两道闸在绑定侧
// 按标记豁免）。绑定侧曾经注册的是不带标记的那件，靠名字豁免。
func TestIdentityGate_GrantedTransferSurvivesFilterAndDeferral(t *testing.T) {
	var n atomic.Int32
	m := &gateModel{}
	front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithModel(m), agent_v2.WithHandoffTransfer(),
		agent_v2.WithToolkit(kitOf(namedBizTool("beta", &n, "y"), namedBizTool("mcp_x", &n, "z"))),
		agent_v2.WithToolFilter(func(context.Context, *schema.Message) []string { return []string{"beta", "mcp_x"} }),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true,
			Deferred: func(tl tool.Tool) bool { return tl.Name() != "beta" }}))
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("front")).
		AddAgent("front", front).
		AddAgent("billing", agent_v2.NewBasicAgent(agent_v2.WithName("billing"), agent_v2.WithModel(&gateModel{})))
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if bound, _ := m.round(t, 0); !slices.Contains(bound, agent_v2.HandoffTransferToolName) {
		t.Errorf("被授予的 transfer_to_agent 应豁免 ToolFilter 与渐进披露、留在绑定列表里，实际 %v", bound)
	}
}

// TestIdentityGate_ShadowedBusinessToolNotSearchable：业务 toolkit 里有一件叫 transfer_to_agent 的工具，而这个候选被
// 授予了转交——那件已被框架的顶掉，tool_search 的检索结果里不该再出现它（连同它的描述）。派发侧的延迟名单要在
// 框架工具都挂好之后才算；先算的话被顶掉的那件还在名单里，模型检索时看到的是一件已经不存在的工具。
func TestIdentityGate_ShadowedBusinessToolNotSearchable(t *testing.T) {
	var n atomic.Int32
	m := &gateModel{script: []*schema.Message{callMsg(tcall("s1", "tool_search", `{"query":"transfer_to_agent 业务工具"}`))}}
	front := agent_v2.NewBasicAgent(agent_v2.WithName("front"), agent_v2.WithModel(m), agent_v2.WithHandoffTransfer(),
		agent_v2.WithToolkit(kitOf(namedBizTool(agent_v2.HandoffTransferToolName, &n, "biz"), namedBizTool("mcp_x", &n, "z"))),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: func(tool.Tool) bool { return true }}))
	h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("front")).
		AddAgent("front", front).
		AddAgent("billing", agent_v2.NewBasicAgent(agent_v2.WithName("billing"), agent_v2.WithModel(&gateModel{})))
	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_, in := m.round(t, 1)
	for _, msg := range in {
		if msg != nil && msg.Role == schema.RoleTool {
			if strings.Contains(msg.Content, "业务工具 "+agent_v2.HandoffTransferToolName) {
				t.Errorf("被框架顶掉的业务 transfer_to_agent 不该出现在检索结果里，实际：%s", msg.Content)
			}
			return
		}
	}
	t.Fatal("fixture 没搭对：第 2 轮输入里没有 tool_search 的结果")
}

// TestIdentityGate_BusinessToolNamedToolSearchStillCallable：业务工具叫 tool_search，Deferred 也命中它，而它是唯一
// 一件被延迟的——框架那件 tool_search 不注册（没有可检索的），这件业务工具照常绑定、调用照常执行。派发侧曾经
// 重新跑 Deferred 谓词、判它「延迟且未揭示」而一律拒绝，模型被告知「先调 tool_search」也就是它自己，循环到轮次
// 上限（审计实测）。
func TestIdentityGate_BusinessToolNamedToolSearchStillCallable(t *testing.T) {
	var body atomic.Int32
	m := &gateModel{script: []*schema.Message{callMsg(tcall("c1", "tool_search", "{}"))}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("tsb"), agent_v2.WithModel(m),
		agent_v2.WithToolkit(kitOf(namedBizTool("tool_search", &body, "biz-ok"))),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: func(tool.Tool) bool { return true }}))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := body.Load(); n != 1 {
		_, in := m.round(t, 1)
		t.Errorf("业务的 tool_search 应照常执行 1 次，实际 %d 次；第 2 轮输入：%v", n, in[len(in)-1].Content)
	}
}
