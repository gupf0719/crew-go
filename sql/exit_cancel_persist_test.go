package sql_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
	agentSQL "crew-go/sql"
)

// TestSQLiteCheckpointer_CallerCancelExitPersists：调用方 ctx 取消导致的退出，按设计要存下可以续跑的档。
// 引擎出口写档曾用已经取消的 ctx，持久后端一律直接报错（「sql cp SaveNS graphid check: context
// canceled」），这类退出只在内存后端上续得上。用真 SQLite 钉住：出口写档不跟着调用方取消。
func TestSQLiteCheckpointer_CallerCancelExitPersists(t *testing.T) {
	cp := agentSQL.NewCheckpointer(newSQLiteDB(t), agentSQL.SQLite{})
	if err := cp.Setup(t.Context()); err != nil {
		t.Fatalf("cp Setup: %v", err)
	}
	started := make(chan struct{}, 1)
	b := compose_v2.NewBuilder().WithName("sqlite_cancel")
	b.AddNode("a", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddEdge("a", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
		compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID("sqlite_cancel"))
	if !errors.Is(err, context.Canceled) || errors.Is(err, compose_v2.ErrCheckpointNotPersisted) {
		t.Fatalf("调用方取消导致的退出应照常报 ctx 错、档已写成：%v", err)
	}
	exists, exErr := cp.ExistsNS(context.Background(), "sqlite_cancel", "")
	if exErr != nil || !exists {
		t.Fatalf("调用方取消之后档应已写进 SQLite：exists=%v err=%v", exists, exErr)
	}
}
