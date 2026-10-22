package agent_v2_test

// 采纳业务钩子返回的 ctx：执行状态一律取框架自己的（compose_v2.CallHook）。钩子违约、返回别处缓存
// 的 ctx 时，曾经各个采纳点各铺各的几样，漏掉的那几样各自静默出错——下面每条都是当时实测出来的形态，
// 都配一个钩子不换 ctx 的对照组。

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	specModel "crew-go/modules/model"
	"crew-go/schema"
)

// seqTransferModel：第 n 次被调（且最后一条不是工具结果）时转交给 targets[n-1]。
type seqTransferModel struct {
	calls   atomic.Int32
	targets []string
}

func (m *seqTransferModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if len(msgs) > 0 && msgs[len(msgs)-1].Role == schema.RoleTool {
		return schema.AssistantMessage("done"), nil
	}
	n := min(int(m.calls.Add(1)), len(m.targets))
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("fd_call_%d", n), Type: "function",
		Function: schema.FunctionCall{Name: agent_v2.HandoffTransferToolName, Arguments: `{"agent_name":"` + m.targets[n-1] + `"}`}}}), nil
}
func (m *seqTransferModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *seqTransferModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHookCtx_StaleCtxAcrossHops_TransferDelivered：frontdesk → billing → frontdesk → 转给 sales。
// frontdesk 的 OnToolStart 返回它第一次 OnModelStart 时存下的 ctx——上面的收件人是第 0 跳的。
// 曾经只铺回了「信号交给哪段执行」、收件人没铺回：第 2 跳的转交发给了第 0 跳的收件人，没人取，
// transfer 工具回了「已转交给 sales」、sales 一次没跑，`err == nil`。
func TestHookCtx_StaleCtxAcrossHops_TransferDelivered(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("OnToolStart返回缓存的ctx=%v", stale), func(t *testing.T) {
			fdModel := &seqTransferModel{targets: []string{"billing", "sales"}}
			fd := agent_v2.NewBasicAgent(agent_v2.WithName("frontdesk_llm"), agent_v2.WithModel(fdModel),
				agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3), agent_v2.WithHooks(&staleCtxHooks{toolStart: stale}))
			billing := agent_v2.NewBasicAgent(agent_v2.WithName("billing_llm"), agent_v2.WithModel(sigTransferModel("frontdesk")),
				agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
			sales := &effReply{name: "sales", reply: "SALES 已处理"}
			desk := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("frontdesk")).
				AddAgent("frontdesk", fd).AddAgent("billing", billing).AddAgent("sales", sales)
			got := effDrive(t, desk, "我要退款")
			if fdModel.calls.Load() != 2 {
				t.Fatalf("前提不成立：frontdesk 应上场两次（第 0 跳、第 2 跳），实际模型被调 %d 次", fdModel.calls.Load())
			}
			if got != "SALES 已处理" || sales.runs.Load() != 1 {
				t.Errorf("第 2 跳 frontdesk 的转交应送到 sales：最终 %q（sales %d 次）", got, sales.runs.Load())
			}
		})
	}
}

// seqRouteModel：第 n 次被调（且最后一条不是工具结果）时 route_to targets[n-1]。
type seqRouteModel struct {
	calls   atomic.Int32
	targets []string
}

func (m *seqRouteModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if len(msgs) > 0 && msgs[len(msgs)-1].Role == schema.RoleTool {
		return schema.AssistantMessage("done"), nil
	}
	n := min(int(m.calls.Add(1)), len(m.targets))
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: fmt.Sprintf("rt_call_%d", n), Type: "function",
		Function: schema.FunctionCall{Name: agent_v2.RouteTool, Arguments: `{"target":"` + m.targets[n-1] + `"}`}}}), nil
}
func (m *seqRouteModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *seqRouteModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHookCtx_StaleCtxAcrossRoutingRuns_RouteDelivered：同一个 RoutingAgent 在 Sequential 里上场两次，
// 分类器是一层壳（显式转授给 cls、自己的最终消息不带目标——排除「从分类器最终消息里解析出目标」那道
// 兜底把丢失盖住）。cls 的 OnToolStart 返回第一次缓存的 ctx：第二次上场的路由曾经发给第一次的收件人，
// 走了兜底（math 1 次、chat 0 次、兜底 1 次），`err == nil`。
func TestHookCtx_StaleCtxAcrossRoutingRuns_RouteDelivered(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("OnToolStart返回缓存的ctx=%v", stale), func(t *testing.T) {
			cls := agent_v2.NewBasicAgent(agent_v2.WithName("cls_llm"), agent_v2.WithModel(&seqRouteModel{targets: []string{"math", "chat"}}),
				agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(3), agent_v2.WithHooks(&staleCtxHooks{toolStart: stale}))
			shell := &effFunc{name: "shell"}
			shell.fn = func(ctx context.Context, self agent_v2.Agent, in *schema.Message, opts []agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
				if _, err := agent_v2.RunSubAgent(ctx, "cls", cls, in, agent_v2.RegrantRouteTo(opts, self, cls)...); err != nil {
					return nil, err
				}
				return effExit("shell", "已分好"), nil
			}
			rt, math, chat, fb := grantRouting(shell)
			seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("twice")).AddStep("first", rt).AddStep("second", rt)
			effDrive(t, seq, "hi")
			if math.runs.Load() != 1 || chat.runs.Load() != 1 || fb.runs.Load() != 0 {
				t.Errorf("两次上场应分别路由到 math、chat：math %d 次、chat %d 次、兜底 %d 次",
					math.runs.Load(), chat.runs.Load(), fb.runs.Load())
			}
		})
	}
}

// staleModelHooks：第一次 OnModelStart 存下 ctx，之后（含下一次 Run）都返回那一个。
type staleModelHooks struct {
	hook.NoOpHooks
	stale  bool
	mu     sync.Mutex
	cached context.Context
}

func (h *staleModelHooks) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cached == nil {
		h.cached = ctx
		return ctx, nil
	}
	if h.stale {
		return h.cached, nil
	}
	return ctx, nil
}

type countReplyModel struct{ n atomic.Int32 }

func (m *countReplyModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(fmt.Sprintf("A%d", m.n.Add(1))), nil
}
func (m *countReplyModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *countReplyModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHookCtx_ModelHookStaleCtx_HistoryKept：同一场会话跑两个 Run，第二个 Run 的 OnModelStart 返回第一个
// Run 存下的 ctx。模型节点曾经直接换成钩子的 ctx、一样都不铺回：第二个 Run 的答复记进了第一个 Run 那个
// 已经收尾的历史写入器，会话历史里只有 [Q1 A1 Q2]，A2 丢了，`err == nil`。
func TestHookCtx_ModelHookStaleCtx_HistoryKept(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("OnModelStart返回上一次Run的ctx=%v", stale), func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("bot"), agent_v2.WithModel(&countReplyModel{}),
				agent_v2.WithHistory(hs, "u1"), agent_v2.WithHooks(&staleModelHooks{stale: stale}))
			ctx := compose_v2.WithThreadIDCtx(context.Background(), "c1")
			for i := 1; i <= 2; i++ {
				if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(fmt.Sprintf("Q%d", i))); err != nil {
					t.Fatalf("Run %d: %v", i, err)
				}
			}
			msgs, err := agent_v2.LoadAllHistory(context.Background(), hs, agent_v2.HistoryKey{Owner: "u1", ConvID: "c1"})
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, m := range msgs {
				got = append(got, m.Content)
			}
			if want := "[Q1 A1 Q2 A2]"; fmt.Sprint(got) != want {
				t.Errorf("会话历史应为 %s，实际 %v", want, got)
			}
		})
	}
}

type plainReplyModel struct{}

func (plainReplyModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage("我自己处理"), nil
}
func (plainReplyModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m plainReplyModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// modelEndHandoffHooks：OnModelStart 返回一段已经收完的执行的 ctx（stale 时）；OnModelEnd 里经
// RegrantFromNode + HandoffTo 做出转交（BasicAgent 图里的钩子是 RegrantFromNode 的合法位置）。
type modelEndHandoffHooks struct {
	hook.NoOpHooks
	stale      bool
	self       agent_v2.Agent
	handoffErr error
	endCalls   atomic.Int32
}

func (h *modelEndHandoffHooks) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	if !h.stale {
		return ctx, nil
	}
	var stale context.Context
	if _, err := compose_v2.CaptureSignals(ctx, func(c context.Context) error { stale = c; return nil }); err != nil {
		return nil, err
	}
	return stale, nil
}

func (h *modelEndHandoffHooks) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	h.endCalls.Add(1)
	h.handoffErr = agent_v2.HandoffTo(ctx, agent_v2.RegrantFromNode(ctx, h.self), h.self, "billing")
	return ctx, nil
}

// TestHookCtx_ModelHookStaleCtx_HandoffDelivered：OnModelStart 返回一段已经收完的执行的 ctx，之后
// OnModelEnd 里做出的转交曾经交进了那段执行：HandoffTo 返回 nil（「已转交」），对话却没转走，
// `err == nil`。
func TestHookCtx_ModelHookStaleCtx_HandoffDelivered(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("OnModelStart返回收完的ctx=%v", stale), func(t *testing.T) {
			h := &modelEndHandoffHooks{stale: stale}
			fd := agent_v2.NewBasicAgent(agent_v2.WithName("frontdesk_llm"), agent_v2.WithModel(plainReplyModel{}),
				agent_v2.WithMaxRounds(3), agent_v2.WithHooks(h))
			h.self = fd
			hd, billing, _ := sigDesk(fd)
			got := effDrive(t, hd, "我要退款")
			if h.endCalls.Load() == 0 || h.handoffErr != nil {
				t.Fatalf("前提不成立：OnModelEnd 应调到、HandoffTo 应返回 nil，实际 %d 次 / %v", h.endCalls.Load(), h.handoffErr)
			}
			if got != "BILLING 已处理" || billing.runs.Load() != 1 {
				t.Errorf("OnModelEnd 做出的转交应送到 billing：最终 %q（billing %d 次）", got, billing.runs.Load())
			}
		})
	}
}

// adoptStaleNodeStart：第一次（满足 match 的）OnNodeStart 存下 ctx，之后（stale 时）都返回那一个。
type adoptStaleNodeStart struct {
	agent_v2.NoOpOrchestrationHooks
	stale  bool
	match  func(ctx context.Context, nodeID string) bool
	mu     sync.Mutex
	cached context.Context
}

func (h *adoptStaleNodeStart) OnNodeStart(ctx context.Context, nodeID string) (context.Context, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.match != nil && !h.match(ctx, nodeID) {
		return ctx, nil
	}
	if h.cached == nil {
		h.cached = ctx
		return ctx, nil
	}
	if h.stale {
		return h.cached, nil
	}
	return ctx, nil
}

// adoptStaleGraphStart：第一次 OnGraphStart 存下 ctx，之后（stale 时）都返回那一个。
type adoptStaleGraphStart struct {
	agent_v2.NoOpOrchestrationHooks
	stale  bool
	mu     sync.Mutex
	cached context.Context
}

func (h *adoptStaleGraphStart) OnGraphStart(ctx context.Context) (context.Context, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cached == nil {
		h.cached = ctx
		return ctx, nil
	}
	if h.stale {
		return h.cached, nil
	}
	return ctx, nil
}

// historyOfTwoRuns 跑同一场会话的两个 Run，返回会话历史的内容。
func historyOfTwoRuns(t *testing.T, orch agent_v2.OrchestrationHooks) []string {
	t.Helper()
	hs := agent_v2.NewInMemoryHistoryStore()
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("bot"), agent_v2.WithModel(&countReplyModel{}), agent_v2.WithHistory(hs, "u1"))
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "c1")
	for i := 1; i <= 2; i++ {
		if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(fmt.Sprintf("Q%d", i)), agent_v2.WithOrchHooks(orch)); err != nil {
			t.Fatalf("Run %d: %v", i, err)
		}
	}
	msgs, err := agent_v2.LoadAllHistory(context.Background(), hs, agent_v2.HistoryKey{Owner: "u1", ConvID: "c1"})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, m := range msgs {
		got = append(got, m.Content)
	}
	return got
}

// TestHookCtx_OrchNodeStartStaleCtx_HistoryKept：编排钩子 OnNodeStart 返回上一次 Run 存下的 ctx。引擎这层的
// 采纳点曾经只认引擎自己的执行状态，agent_v2 的（历史写入器、session……）取的是钩子那份：第二个 Run 的
// 历史全丢（[Q1 A1]）、事件为空、`err == nil`。
func TestHookCtx_OrchNodeStartStaleCtx_HistoryKept(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("OnNodeStart返回上一次Run的ctx=%v", stale), func(t *testing.T) {
			if got := historyOfTwoRuns(t, &adoptStaleNodeStart{stale: stale}); fmt.Sprint(got) != "[Q1 A1 Q2 A2]" {
				t.Errorf("会话历史应为 [Q1 A1 Q2 A2]，实际 %v", got)
			}
		})
	}
}

// TestHookCtx_OrchGraphStartStaleCtx_HistoryKept：同上，返回缓存 ctx 的是 OnGraphStart。
func TestHookCtx_OrchGraphStartStaleCtx_HistoryKept(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("OnGraphStart返回上一次Run的ctx=%v", stale), func(t *testing.T) {
			if got := historyOfTwoRuns(t, &adoptStaleGraphStart{stale: stale}); fmt.Sprint(got) != "[Q1 A1 Q2 A2]" {
				t.Errorf("会话历史应为 [Q1 A1 Q2 A2]，实际 %v", got)
			}
		})
	}
}

// TestHookCtx_OrchNodeStartStaleCtx_TransferDelivered：frontdesk → billing → frontdesk → 转给 sales，返回缓存
// ctx 的是 frontdesk 图里 tools 节点的 OnNodeStart（编排钩子经 ctx 下发给候选）。收件人是 agent_v2 的执行
// 状态，引擎那层的采纳点曾经不认它：第 2 跳的转交发给了第 0 跳的收件人，sales 一次没跑、`err == nil`。
func TestHookCtx_OrchNodeStartStaleCtx_TransferDelivered(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("OnNodeStart返回缓存的ctx=%v", stale), func(t *testing.T) {
			h := &adoptStaleNodeStart{stale: stale, match: func(ctx context.Context, nodeID string) bool {
				return nodeID == "tools" && strings.Contains(compose_v2.GetGraphNS(ctx), "frontdesk")
			}}
			fdModel := &seqTransferModel{targets: []string{"billing", "sales"}}
			fd := agent_v2.NewBasicAgent(agent_v2.WithName("frontdesk_llm"), agent_v2.WithModel(fdModel),
				agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
			billing := agent_v2.NewBasicAgent(agent_v2.WithName("billing_llm"), agent_v2.WithModel(sigTransferModel("frontdesk")),
				agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
			sales := &effReply{name: "sales", reply: "SALES 已处理"}
			desk := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("desk"), agent_v2.WithHandoffInitial("frontdesk")).
				AddAgent("frontdesk", fd).AddAgent("billing", billing).AddAgent("sales", sales)
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			events, err := desk.Run(agent_v2.WithCtxOrchHooks(ctx, h), agent_v2.NewRunSession(), schema.UserMessage("我要退款"))
			if err != nil {
				t.Fatal(err)
			}
			if fdModel.calls.Load() != 2 {
				t.Fatalf("前提不成立：frontdesk 应上场两次，实际模型被调 %d 次", fdModel.calls.Load())
			}
			got := ""
			if m := agent_v2.FinalMessage(events); m != nil {
				got = m.Content
			}
			if got != "SALES 已处理" || sales.runs.Load() != 1 {
				t.Errorf("第 2 跳 frontdesk 的转交应送到 sales：最终 %q（sales %d 次）", got, sales.runs.Load())
			}
		})
	}
}

// streamABModel 流式吐两个片段 "A"、"B"。
type streamABModel struct{}

func (streamABModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage("AB"), nil
}
func (streamABModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, send := schema.NewStreamReader[*schema.Message](4)
	send(&schema.Message{Role: schema.RoleAssistant, Content: "A"})
	send(&schema.Message{Role: schema.RoleAssistant, Content: "B"})
	schema.CloseSend(sr)
	return sr, nil
}
func (m streamABModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// adoptStaleStream：第一次 OnModelEndStream 存下 ctx，之后（stale 时）都返回那一个；reader 原样返回。
type adoptStaleStream struct {
	hook.NoOpHooks
	stale  bool
	mu     sync.Mutex
	cached context.Context
}

func (h *adoptStaleStream) OnModelEndStream(ctx context.Context, _ *hook.AgentInfo, sr specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cached == nil {
		h.cached = ctx
		return ctx, sr, nil
	}
	if h.stale {
		return h.cached, sr, nil
	}
	return ctx, sr, nil
}

// chunkCounter 数交给它的 chunk 事件（每个 Run 一个，同 agui 那种每连接一个的编排钩子）。
type chunkCounter struct {
	agent_v2.NoOpOrchestrationHooks
	chunks atomic.Int32
}

func (r *chunkCounter) OnNodeEvent(ctx context.Context, _ string, ev agent_v2.Event) context.Context {
	if ev.Type == "chunk" {
		r.chunks.Add(1)
	}
	return ctx
}

// TestHookCtx_ModelEndStreamStaleCtx_ChunksToOwnRun：OnModelEndStream 返回上一次 Run 存下的 ctx。那一处曾经
// 直接换成钩子的 ctx：第二个 Run 的流式片段交给了第一个 Run 的编排钩子（Run1 收到 4 个、Run2 收到 0 个）——
// 每连接一个钩子时，内容流到了另一个请求的连接上。
func TestHookCtx_ModelEndStreamStaleCtx_ChunksToOwnRun(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("OnModelEndStream返回上一次Run的ctx=%v", stale), func(t *testing.T) {
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("bot"), agent_v2.WithModel(streamABModel{}),
				agent_v2.WithHooks(&adoptStaleStream{stale: stale}))
			rec1, rec2 := &chunkCounter{}, &chunkCounter{}
			for i, rec := range []*chunkCounter{rec1, rec2} {
				if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage(fmt.Sprintf("Q%d", i+1)),
					agent_v2.WithOrchHooks(rec)); err != nil {
					t.Fatalf("Run %d: %v", i+1, err)
				}
			}
			if rec1.chunks.Load() != 2 || rec2.chunks.Load() != 2 {
				t.Errorf("每个 Run 的片段应只交给它自己的钩子（各 2 个）：Run1 %d、Run2 %d", rec1.chunks.Load(), rec2.chunks.Load())
			}
		})
	}
}
