package agent_v2

import (
	"context"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// hangingAbandonCheckpointer 删被放弃的那个任务的存档时挂住，直到 ctx 结束；别的照常。
type hangingAbandonCheckpointer struct {
	compose_v2.Checkpointer
	sid string
}

func (h hangingAbandonCheckpointer) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	if sid != h.sid {
		return h.Checkpointer.DeleteSubtree(ctx, sid, ns)
	}
	<-ctx.Done()
	return 0, ctx.Err()
}

// 入口收尾被放弃任务的存储调用有上限（abandonEntryTimeout）：后端挂住时到点告警、这次 Run 照常跑完。审计实测：曾经直接用这次
// Run 的 ctx，调用方没设截止时间时（agui 的请求就是）整个 Run 卡在入口。
func TestAbandonTasksAtEntry_BoundedWhenBackendHangs(t *testing.T) {
	saved := abandonEntryTimeout
	abandonEntryTimeout = 100 * time.Millisecond
	defer func() { abandonEntryTimeout = saved }()

	cp := hangingAbandonCheckpointer{Checkpointer: compose_v2.NewInMemoryCheckpointer(), sid: "old-task"}
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	ag := NewBasicAgent(WithName("x"), WithModel(sameReplyModel{}), WithMaxRounds(2))
	done := make(chan error, 1)
	go func() {
		_, err := ag.Run(ctx, NewRunSession(), schema.UserMessage("hi"), WithAbandonedTasks("old-task"))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("删档到点只告警，这次 Run 应照常成功：%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("后端挂住时这次 Run 应在上限之后照常跑完，5 秒还没返回")
	}
}
