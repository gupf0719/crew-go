package redis_test

import (
	"crypto/rand"
	"errors"
	"testing"

	"crew-go/compose_v2"
	crewredis "crew-go/redis"

	goredis "github.com/redis/go-redis/v9"
)

func TestRedisCheckpointer_MigrateLegacyKeys(t *testing.T) {
	ctx := t.Context()
	client := goredis.NewClient(&goredis.Options{Addr: aguiRedisAddress(t)})
	t.Cleanup(func() { _ = client.Close() })
	prefix := "checkpoint-migration-" + rand.Text()
	cp := crewredis.NewCheckpointer(client, crewredis.WithCheckpointerPrefix(prefix))
	sid := "team:reports"
	const graph = "report"
	for _, ns := range []string{"", "planner/main", "planner#2", "planner-\xff\xfe"} {
		record := &compose_v2.Checkpoint{ID: sid, NS: ns, GraphID: graph, Status: compose_v2.CPStatusPending, CreatedAt: 12345}
		raw, err := compose_v2.MarshalCheckpoint(record)
		if err != nil {
			t.Fatal(err)
		}
		suffix := sid + ":" + ns
		_, err = client.Pipelined(ctx, func(pipe goredis.Pipeliner) error {
			pipe.Set(ctx, prefix+":cp:item:"+suffix, raw, 0)
			pipe.HSet(ctx, prefix+":cp:meta:"+suffix, "graph_id", graph, "status", "1", "created_at", "12345")
			pipe.SAdd(ctx, prefix+":cp:sids", sid)
			pipe.SAdd(ctx, prefix+":cp:nsidx:"+sid, ns)
			pipe.SAdd(ctx, prefix+":cp:pending:"+sid, ns)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// 误先启动新版时，写入和删除都应保留已有存档供迁移。
	if err := cp.SaveNS(ctx, sid, "", &compose_v2.Checkpoint{GraphID: "premature-write"}); err == nil {
		t.Fatal("expected write against unmigrated checkpoint to fail")
	}
	if n, err := cp.DeleteSubtree(ctx, sid, ""); err == nil || n != 0 {
		t.Fatalf("expected deletion to leave legacy index intact, deleted=%d err=%v", n, err)
	}
	if n, err := cp.MigrateLegacyKeys(ctx); err != nil || n != 4 {
		t.Fatalf("migrated %d checkpoints, want 4: %v", n, err)
	}
	if n, err := cp.MigrateLegacyKeys(ctx); err != nil || n != 0 {
		t.Fatalf("retry migrated %d checkpoints, want 0: %v", n, err)
	}
	root, err := cp.LoadNS(ctx, sid, "")
	if err != nil || root == nil || root.ID != sid || root.GraphID != graph || root.CreatedAt != 12345 {
		t.Fatalf("migrated root = %+v, err = %v", root, err)
	}
	pending, err := cp.ListPending(ctx, sid)
	if err != nil || len(pending) != 4 {
		t.Fatalf("expected 4 pending checkpoints, got %d: %v", len(pending), err)
	}
	if exists, err := cp.ExistsNS(ctx, sid, "planner-\xff\xfe"); err != nil || !exists {
		t.Fatalf("expected existing checkpoint at its original byte address, exists=%v err=%v", exists, err)
	}
	if err := cp.SaveNS(ctx, sid, "", &compose_v2.Checkpoint{GraphID: "different"}); !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Fatalf("expected graph conflict after migration, got %v", err)
	}
	if n, err := cp.DeleteSubtree(ctx, sid, "planner"); err != nil || n != 1 {
		t.Fatalf("expected only the planner descendant to be deleted, got %d: %v", n, err)
	}
	if exists, err := cp.ExistsNS(ctx, sid, "planner#2"); err != nil || !exists {
		t.Fatalf("expected sibling to remain, exists=%v err=%v", exists, err)
	}
	if n, err := cp.DeleteSubtree(ctx, sid, ""); err != nil || n != 3 {
		t.Fatalf("expected remaining three checkpoints to be deleted, got %d: %v", n, err)
	}
}

func TestRedisCheckpointer_MigrationRejectsMissingNamespaceIndex(t *testing.T) {
	ctx := t.Context()
	client := goredis.NewClient(&goredis.Options{Addr: aguiRedisAddress(t)})
	t.Cleanup(func() { _ = client.Close() })
	prefix := "checkpoint-migration-index-" + rand.Text()
	cp := crewredis.NewCheckpointer(client, crewredis.WithCheckpointerPrefix(prefix))
	const sid = "report-job"
	if err := cp.SaveNS(ctx, sid, "", &compose_v2.Checkpoint{GraphID: "report"}); err != nil {
		t.Fatal(err)
	}
	if err := client.Del(ctx, prefix+":cp:nsidx:"+sid).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := cp.MigrateLegacyKeys(ctx); err == nil || n != 0 {
		t.Fatalf("expected an error for a missing namespace index, got %d: %v", n, err)
	}
}

func TestRedisCheckpointer_MigrationPreservesExistingTarget(t *testing.T) {
	ctx := t.Context()
	client := goredis.NewClient(&goredis.Options{Addr: aguiRedisAddress(t)})
	t.Cleanup(func() { _ = client.Close() })
	prefix := "checkpoint-migration-conflict-" + rand.Text()
	cp := crewredis.NewCheckpointer(client, crewredis.WithCheckpointerPrefix(prefix))
	const sid = "report-job"
	if err := cp.SaveNS(ctx, sid, "", &compose_v2.Checkpoint{GraphID: "new"}); err != nil {
		t.Fatal(err)
	}
	raw, err := compose_v2.MarshalCheckpoint(&compose_v2.Checkpoint{ID: sid, GraphID: "legacy"})
	if err != nil {
		t.Fatal(err)
	}
	sourceItem := prefix + ":cp:item:" + sid + ":"
	sourceMeta := prefix + ":cp:meta:" + sid + ":"
	if err := client.Set(ctx, sourceItem, raw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, sourceMeta, "graph_id", "legacy", "status", "0", "created_at", "0").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := cp.MigrateLegacyKeys(ctx); err == nil {
		t.Fatal("expected migration to reject a pre-existing target")
	}
	got, err := cp.LoadNS(ctx, sid, "")
	if err != nil || got == nil || got.GraphID != "new" {
		t.Fatalf("current checkpoint changed: %+v %v", got, err)
	}
	if got, err := client.Get(ctx, sourceItem).Result(); err != nil || got != string(raw) {
		t.Fatalf("legacy checkpoint changed: %v", err)
	}
	if got, err := client.HGet(ctx, sourceMeta, "graph_id").Result(); err != nil || got != "legacy" {
		t.Fatalf("legacy metadata changed: %q %v", got, err)
	}
}
