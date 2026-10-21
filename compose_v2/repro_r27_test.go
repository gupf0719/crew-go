package compose_v2_test

import (
	"context"
	"testing"

	"crew-go/compose_v2"
)

// TestRepro_R27a_MED1_SendDirectivePanicFrontier 是 R27a audit MED-1 回归——
// 验证 SendFrom dirWrites ApplyAll panic 时 cm.CurrentFrontier 应包含 SendFrom 的
// Target 让 Resume 能重跑（避免 Target 永久丢失）。
//
// **Bug 历史**：超步内节点 X 成功调 SendFrom(targets=[Y])，ApplyAll(dirWrites) 时
// 业务 reducer panic 触发 ApplyAll 失败 return → cm.frontierAtExit 仍是上次设的
// 本超步 frontier=[X]，cp.CurrentFrontier 不含 Y → Resume 重跑只跑老 [X]，SendFrom
// 投递的 Y 永远丢失。
//
// 业务场景：自定义 GraphAgent NodeFunc 内 SendFrom + 业务自定义 channel reducer
// 偶发 panic（如 type assert 失败）。生产 ParallelAgent split 节点不踩（split 简单
// 不会触发），但业务自定义路径会。
//
// 修法（graph_loop.go:489-491 加 cm.updateFrontier(nextFrontier)）让 Y 进 cp。
func TestRepro_R27a_MED1_SendDirectivePanicFrontier(t *testing.T) {
	// 业务自定义 reducer panic
	badCh := compose_v2.Custom("bad_r27", func(_, _ int) int {
		panic("biz reducer panic on dirWrites payload")
	}, 0)

	// X: 调 SendFrom([Y]) + payload to badCh
	splitFn := func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		compose_v2.SendFrom(ctx,
			compose_v2.SendDirective{
				Target:  "Y",
				Payload: []compose_v2.ChannelWrite{compose_v2.Write(badCh, 1)},
			},
		)
		return nil, nil
	}
	workerY := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	}

	b := compose_v2.NewBuilder()
	b.AddNode("X", splitFn)
	b.AddNode("Y", workerY)
	b.AddEdge(compose_v2.StartNode, "X")
	b.AddEdge("Y", compose_v2.EndNode)
	g, _ := b.Compile()

	cm := compose_v2.NewChannelManager()
	if err := compose_v2.RegisterChannel(cm, badCh); err != nil {
		t.Fatalf("register: %v", err)
	}
	runErr := compose_v2.Run(context.Background(), g, cm)
	if runErr == nil {
		t.Fatalf("Run 应因 dirWrites ApplyAll panic 失败")
	}

	// 核心验证：cm.CurrentFrontier 应含 Y（SendFrom Target）让 Resume 能重跑
	snap, err := cm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	gotY := false
	for _, m := range snap.CurrentFrontier {
		if m.NodeID == "Y" {
			gotY = true
			break
		}
	}
	if !gotY {
		t.Errorf("R27a MED-1 回归：dirWrites ApplyAll panic 时 SendFrom Target Y 应进 frontier；"+
			"实际 frontier=%v → Y 永久丢失，Resume 重跑只跑老 frontier\n"+
			"sanity-revert 删 graph_loop.go:489 的 cm.updateFrontier(nextFrontier) → 测试 fail",
			snap.CurrentFrontier)
	}
}
