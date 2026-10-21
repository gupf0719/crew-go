package compose_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
)

// Phase 5.1.a Step 3：Checkpointer / sessionID / graphNS 通过 RunOption 注入
// 后节点函数可读 ctx 取出。

// TestCheckpointer_InjectionViaCtx 验证 RunOption 注入 Checkpointer + sessionID
// 后节点函数能从 ctx 取出。
func TestCheckpointer_InjectionViaCtx(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()

	var nodeSawCp compose_v2.Checkpointer
	var nodeSawSid string
	var nodeSawNS string

	g, _ := compose_v2.NewBuilder().
		AddNode("read_ctx", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			nodeSawCp = compose_v2.GetCheckpointer(ctx)
			nodeSawSid = compose_v2.GetSessionID(ctx)
			nodeSawNS = compose_v2.GetGraphNS(ctx)
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "read_ctx").
		AddEdge("read_ctx", compose_v2.EndNode).
		Compile()

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithCheckpointer(cp),
		compose_v2.WithSessionID("sid_xyz"),
		compose_v2.WithRunNS("parent/sub"),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if nodeSawCp != cp {
		t.Fatalf("节点应通过 ctx 拿到注入的 Checkpointer，实际 %v", nodeSawCp)
	}
	if nodeSawSid != "sid_xyz" {
		t.Fatalf("节点应通过 ctx 拿到 sessionID，实际 %q", nodeSawSid)
	}
	if nodeSawNS != "parent/sub" {
		t.Fatalf("节点应通过 ctx 拿到 graphNS，实际 %q", nodeSawNS)
	}
}

// TestCheckpointer_NotInjected_ReturnsNil 验证未注入时 ctx 取出 nil/空。
func TestCheckpointer_NotInjected_ReturnsNil(t *testing.T) {
	var nodeSawCp compose_v2.Checkpointer
	var nodeSawSid string

	g, _ := compose_v2.NewBuilder().
		AddNode("read_ctx", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			nodeSawCp = compose_v2.GetCheckpointer(ctx)
			nodeSawSid = compose_v2.GetSessionID(ctx)
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "read_ctx").
		AddEdge("read_ctx", compose_v2.EndNode).
		Compile()

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if nodeSawCp != nil {
		t.Fatalf("未注入时应返 nil，实际 %v", nodeSawCp)
	}
	if nodeSawSid != "" {
		t.Fatalf("未注入时应返空字符串，实际 %q", nodeSawSid)
	}
}

// TestCheckpointer_BusinessUseFromNode 验证业务节点可在 NodeFunc 内部直接用
// Checkpointer 持久化数据（demo 用法——Step 4 才把 subgraph 改用此协议）。
func TestCheckpointer_BusinessUseFromNode(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()

	// 业务在节点内主动写 cp 后**立即在节点内 LoadNS** 验证 SaveNS 接口可用——
	// 不依赖 Run 完成后的状态（F4 修复后 Run 成功会 DeleteSubtree(graphNS) 清
	// 子树，业务自定义 cp 在 graphNS 子树内会被一并清掉，这是预期 cleanup
	// 行为）。
	g, _ := compose_v2.NewBuilder().
		AddNode("custom_persist", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			cper := compose_v2.GetCheckpointer(ctx)
			sid := compose_v2.GetSessionID(ctx)
			ns := compose_v2.GetGraphNS(ctx)

			// 业务在节点内自定义 ns 路径写 cp
			myCp := &compose_v2.Checkpoint{
				Status:  compose_v2.CPStatusPending,
				GraphID: "g_business",
			}
			if err := cper.SaveNS(ctx, sid, ns+"/business_ns", myCp); err != nil {
				return nil, err
			}
			// 节点内立即 LoadNS 验证写入
			loaded, err := cper.LoadNS(ctx, sid, ns+"/business_ns")
			if err != nil {
				return nil, err
			}
			if loaded == nil {
				return nil, errors.New("cp 应已写入但 LoadNS 返 nil")
			}
			if loaded.GraphID != "g_business" {
				return nil, errors.New("LoadNS GraphID 错")
			}
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "custom_persist").
		AddEdge("custom_persist", compose_v2.EndNode).
		Compile()

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithCheckpointer(cp),
		compose_v2.WithSessionID("sid_biz"),
		compose_v2.WithRunNS("root"),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// **F4 行为**：Run 成功后 DeleteSubtree(sid_biz, root) 清根 cp 子树——
	// 业务自定义的 (sid_biz, root/business_ns) 也被一并清（在子树内）。
	got, _ := cp.LoadNS(context.Background(), "sid_biz", "root/business_ns")
	if got != nil {
		t.Fatalf("Run 成功 cleanup 应清掉子树内业务自定义 cp，实际仍存 %+v", got)
	}
}
