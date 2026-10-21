package agent_v2_test

// 守「Resume 拿不到可续存档时，说得出是哪一种处境」。
//
// 哨兵仍是同一个（`ErrNoPendingCheckpoint`，业务的 errors.Is 不变——2026-08-11 定稿），
// 但「档在、只是没有可续内容」和「压根没有这回事」对调用方是两种动作。
//
// ⚠️ 2026-08-13 起「失败现场」**不再**是被拒的一档：引擎捡档按内容判
// （`HasResumableWork`），失败档 frontier 还指着失败分支 ⟹ 放行、Resume = 重试失败
// 分支。被拒的只剩「档里 frontier 与挂起都空」——典型是业务把一份跑完的快照存进了槽。

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

func TestResume_NoPendingCp_SaysWhich(t *testing.T) {
	mk := func() agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("diag"),
			agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("x")})))
	}

	t.Run("压根没有存档", func(t *testing.T) {
		cper := compose_v2.NewInMemoryCheckpointer()
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
		_, err := mk().Resume(ctx, "never_ran")
		if !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
			t.Fatalf("哨兵变了: %v", err)
		}
		if !strings.Contains(err.Error(), "没有存档") {
			t.Errorf("没说出是「压根没有」这一种: %v", err)
		}
	})

	t.Run("存档在但没有可续内容", func(t *testing.T) {
		cper := compose_v2.NewInMemoryCheckpointer()
		ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
		// 业务往续跑的槽里存了一份「跑完了」的快照（frontier 与挂起都空）——
		// 引擎按内容判它不是可续的活
		done := &compose_v2.Checkpoint{ID: "d", NS: "", GraphID: "g"}
		if err := cper.SaveNS(ctx, "stored_done", "", done); err != nil {
			t.Fatalf("seed: %v", err)
		}
		_, err := mk().Resume(ctx, "stored_done")
		if !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) {
			t.Fatalf("哨兵变了: %v", err)
		}
		if !strings.Contains(err.Error(), "没有可续的内容") {
			t.Errorf("没说出「档在、只是没有可续内容」——调用方分不出该开新会话还是自己存错了槽: %v", err)
		}
	})
}
