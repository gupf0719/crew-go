package agent_v2

import (
	"context"
	"encoding/json"
	"testing"

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// routeStub 按序吐消息的假模型。
type routeStub struct {
	msgs []*schema.Message
	i    int
}

func (s *routeStub) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	i := s.i
	s.i++
	if i >= len(s.msgs) {
		i = len(s.msgs) - 1
	}
	m := *s.msgs[i]
	return &m, nil
}

func (s *routeStub) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	m, err := s.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(m)
	schema.CloseSend(sr)
	return sr, nil
}

func (s *routeStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return s, nil }

func routeCallMsg(id, target string) *schema.Message {
	args, _ := json.Marshal(map[string]any{"target": target})
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: id, Type: "function",
		Function: schema.FunctionCall{Name: RouteTool, Arguments: string(args)},
	}})
}

// TestRouteDest_NotInheritedByAgentAsTool：**被当工具用的 agent 不许替调用方决定路由**。
//
// 路由决定发给谁由 ctx 上的收件人决定，而 ctx 是天然下传的——被 AgentAsTool 包进来的 agent 继承的
// 正是同一个收件人，于是它调一次 route_to 做出的决定，跟 classifier 该做的那个**完全重合**：
// classifier 明明选了 A，最终却路由去了内层选的 B，且 err 全程为 nil。
//
// 转交那侧同款缺陷在这两个边界摘掉了收件人（[withoutHandoffDest]）；这条钉住路由这侧的对称
// 保护。哨兵 TestToolBoundaries_StripBothControlChannels 另外盯着「两个边界都摘、且两种信号都摘」
// 的机械对账。
func TestRouteDest_NotInheritedByAgentAsTool(t *testing.T) {
	const dest = "router@" // classifier 那一层的收件人

	// 内层 agent：被当工具用，它自己调 route_to 想把对话路由到 hijacked。
	innerTK := tool.NewToolkit()
	inner := NewBasicAgent(
		WithName("inner"),
		WithDescription("内层能力"),
		WithModel(&routeStub{msgs: []*schema.Message{
			routeCallMsg("tc_inner", "hijacked"),
			schema.AssistantMessage("inner done"),
		}}),
		WithToolkit(innerTK),
		WithRouteTool(), // 内层带着 route_to（业务把它装在别处，或 skill 里带了它）
		WithMaxRounds(3),
	)

	sess := NewRunSession()
	ctx := withRouteDest(WithSession(context.Background(), sess), dest) // classifier 这一层在等

	at := AgentAsTool(inner)
	sigs, err := compose_v2.CaptureSignals(ctx, func(c context.Context) error {
		_, err := at.Call(c, map[string]any{"input": "随便问点什么"})
		return err
	})
	if err != nil {
		t.Fatalf("AgentAsTool.Call: %v", err)
	}

	// 承重断言：发给 classifier 的决定里**不能**出现内层做出的目标。
	if got, _ := takeRouteTarget(sigs, dest); got != "" {
		t.Fatalf("被当工具用的 agent 不许替调用方决定路由：classifier 收到了 %q"+
			"（分类结果被内层劫持，且 err 全程为 nil）", got)
	}
}

// TestTakeRouteTarget_SequentialLaterWins：同一次分类里先后两步各定一次路由，后定的为准（分类器
// 是 Sequential / Loop，逐步细化）；发给别的 RoutingAgent 的原样留下。并发两路的冲突在合并点报，
// 见 compose_v2.JoinSignals。
//
// ⚠️ 曾经这里一律报冲突：Sequential 分类器先粗分（math）再细分（chat），整个 Run 失败——改动前的
// 实现是后者为准（审计实测）。
func TestTakeRouteTarget_SequentialLaterWins(t *testing.T) {
	emit := func(ctx context.Context, dest, origin, target string) {
		compose_v2.EmitSignal(ctx, routeSignal, compose_v2.Signal{Dest: dest, Value: target, Origin: origin})
	}
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		emit(ctx, "rt", "c0#0", "math")
		emit(ctx, "rt", "c1#1", "coder")
		emit(ctx, "outer", "c2#0", "chat")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got, rest := takeRouteTarget(sigs, "rt")
	if got != "coder" {
		t.Errorf("先后两步各定一次路由，应以后定的为准；实际 %q", got)
	}
	if rest.Len() != 1 {
		t.Errorf("发给别的 RoutingAgent 的那条应原样留下，实际剩 %d 条", rest.Len())
	}
}
