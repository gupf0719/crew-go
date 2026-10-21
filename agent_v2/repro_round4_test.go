package agent_v2_test

import (
	"context"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// TestRepro_R4_SendFromSameTargetFanoutCrosstalk 验证 audit Round 4 真 bug 1：
// SendFrom 同 Target N 次 → executeFrontier 并发启 N 个相同 nodeID 的 goroutine
// → 都拿同 visit → 同 InterruptID 串台。
func TestRepro_R4_SendFromSameTargetFanoutCrosstalk(t *testing.T) {
	tk := tool.NewToolkit()
	tk.Register(agent_v2.NewAskUserTool())

	// 收集每个 worker 抛的 InterruptID
	var mu sync.Mutex
	gotIDs := []string{}

	splitNode := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 同 Target=worker 投递 3 次 → frontier 含 [worker, worker, worker]
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{Target: "worker"},
			compose_v2.SendDirective{Target: "worker"},
			compose_v2.SendDirective{Target: "worker"},
		)
		return nil, nil
	}

	workerNode := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 每个 worker 调 ask_user 抛中断——按 visit 维度应该有不同 InterruptID
		_, err := tk.Call(ctx, agent_v2.AskUserToolName, map[string]any{"question": "Q"})
		if ie, ok := compose_v2.IsInterrupt(err); ok && ie.Query != nil {
			mu.Lock()
			gotIDs = append(gotIDs, ie.Query.InterruptID)
			mu.Unlock()
		}
		return nil, err
	}

	g, _ := compose_v2.NewBuilder().
		AddNode("split", splitNode).
		AddNode("worker", workerNode).
		AddEdge(compose_v2.StartNode, "split").
		AddEdge("split", compose_v2.EndNode).
		AddEdge("worker", compose_v2.EndNode).
		Compile()

	cm := compose_v2.NewChannelManager()
	_ = compose_v2.Run(context.Background(), g, cm)

	t.Logf("3 个 worker 并发抛的 InterruptIDs: %v", gotIDs)
	if len(gotIDs) != 3 {
		t.Fatalf("expected 3 worker interrupts, got %d: %v", len(gotIDs), gotIDs)
	}
	// 三个 worker 的 InterruptID 应该全不同
	idSet := map[string]bool{}
	for _, id := range gotIDs {
		idSet[id] = true
	}
	if len(idSet) != 3 {
		t.Fatalf("Bug 1 reproduced: 3 个并发 worker 的 InterruptID 不全不同（%d unique），串台:\n  %v\n"+
			"修复方向：fanout idx 编入 visit 维度",
			len(idSet), gotIDs)
	}
}

// TestRepro_R4_GraphValidateNodeIDForNS 验证 audit Round 4 真 bug 2 修复：
// GraphBuilder.validate() 必须调 ValidateNodeIDForNS 拒绝含 "/" 或 "#" 的 nodeID。
func TestRepro_R4_GraphValidateNodeIDForNS(t *testing.T) {
	t.Run("nodeID_with_slash_rejected", func(t *testing.T) {
		_, err := compose_v2.NewBuilder().
			AddNode("bad/name", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				return nil, nil
			}).
			AddEdge(compose_v2.StartNode, "bad/name").
			AddEdge("bad/name", compose_v2.EndNode).
			Compile()
		if err == nil {
			t.Fatal("expected validate error for nodeID with '/', got nil")
		}
	})

	t.Run("nodeID_with_hash_rejected", func(t *testing.T) {
		_, err := compose_v2.NewBuilder().
			AddNode("bad#name", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				return nil, nil
			}).
			AddEdge(compose_v2.StartNode, "bad#name").
			AddEdge("bad#name", compose_v2.EndNode).
			Compile()
		if err == nil {
			t.Fatal("expected validate error for nodeID with '#', got nil")
		}
	})

	t.Run("normal_nodeID_OK", func(t *testing.T) {
		_, err := compose_v2.NewBuilder().
			AddNode("normal_name", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				return nil, nil
			}).
			AddEdge(compose_v2.StartNode, "normal_name").
			AddEdge("normal_name", compose_v2.EndNode).
			Compile()
		if err != nil {
			t.Fatalf("normal nodeID should pass, got %v", err)
		}
	})
}

// TestRepro_R4_AgentAsToolValidateName 验证 AgentAsTool 拒绝 sub-agent 名字含 "/" / "#"。
func TestRepro_R4_AgentAsToolValidateName(t *testing.T) {
	// 用一个 stub agent 名字含 "/"
	stubBad := &nameOnlyAgent{name: "bad/name"}
	at := agent_v2.AgentAsTool(stubBad, agent_v2.WithAgentToolDescription("stub"))
	res, err := at.Call(context.Background(), map[string]any{"input": "x"})
	if err != nil {
		t.Fatalf("Call should not return error, got %v", err)
	}
	if res == nil || res.Success {
		t.Fatalf("expected tool.Failure with invalid name error, got %+v", res)
	}
	if res.Error == nil || res.Error.Code == "" {
		t.Fatalf("expected error code in result, got %+v", res)
	}
}

// nameOnlyAgent 是只用 Name() 的 stub agent for test
type nameOnlyAgent struct {
	name string
}

func (a *nameOnlyAgent) Name() string        { return a.name }
func (a *nameOnlyAgent) Description() string { return "" }
func (a *nameOnlyAgent) Run(_ context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *nameOnlyAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
