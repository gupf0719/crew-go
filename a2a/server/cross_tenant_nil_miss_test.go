package server_test

// 后端对「不是你的 / 没有」不返 ErrTaskNotFound，而是 Go 常见的 `(nil, nil)`（store 接口对 Get 只说 SHOULD 返
// ErrTaskNotFound），或者一个空壳 StoredTask：tasks/resubscribe 照样不能把别的租户在途的输出交出去。曾经归属检查把
// 「err 为 nil」当成存在、交给 belongsToCaller，而它对空结果返 true ⟹ 放行；SDK 的订阅遇到空快照直接跳过、接着推在途
// 事件（审计实测：B 收到 A 的 artifact 与状态消息）。

import (
	"context"
	"errors"
	"testing"

	"crew-go/a2a/server"
	"crew-go/compose_v2"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
)

// nilMissStore 按 owner 分区，但对没有 / 不是你的返 (nil, nil)（shell 为真时返空壳 &StoredTask{}）。
type nilMissStore struct {
	*taskstore.InMemory
	shell bool
}

func (s *nilMissStore) Get(ctx context.Context, id a2atypes.TaskID) (*taskstore.StoredTask, error) {
	st, err := s.InMemory.Get(ctx, id)
	if errors.Is(err, a2atypes.ErrTaskNotFound) {
		if s.shell {
			return &taskstore.StoredTask{}, nil
		}
		return nil, nil
	}
	return st, err
}

func newNilMissStore(shell bool) *nilMissStore {
	return &nilMissStore{
		InMemory: taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
			Authenticator: func(ctx context.Context) (string, error) { return "owner:" + compose_v2.GetOwner(ctx), nil },
		}),
		shell: shell,
	}
}

func TestCrossTenant_Resubscribe_NilNilMissStore(t *testing.T) {
	assertResubscribeDoesNotLeak(t, server.WithTaskStore(newNilMissStore(false)))
}

func TestCrossTenant_Resubscribe_EmptyShellMissStore(t *testing.T) {
	assertResubscribeDoesNotLeak(t, server.WithTaskStore(newNilMissStore(true)))
}
