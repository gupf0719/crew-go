package redis_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"os"
	"strings"
	"testing"
	"time"

	"crew-go/agui"
	"crew-go/compose_v2"
	crewredis "crew-go/redis"

	goredis "github.com/redis/go-redis/v9"
)

func TestRedisCheckpointer_DeleteWithoutExistsPermission(t *testing.T) {
	addr := os.Getenv("CREW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("需要支持 ACL 的隔离 Redis")
	}
	ctx := t.Context()
	admin := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = admin.Close() })
	prefix := "checkpoint-delete-acl:" + rand.Text()
	user := rand.Text()
	const password = "test-checkpoint-delete"
	setACL := func(rules ...any) {
		t.Helper()
		args := append([]any{"ACL", "SETUSER", user}, rules...)
		if err := admin.Do(ctx, args...).Err(); err != nil {
			t.Fatal(err)
		}
	}
	setACL("on", ">"+password, "~"+prefix+":*", "+@all", "-exists")
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := admin.Do(cleanup, "ACL", "DELUSER", user).Err(); err != nil {
			t.Error(err)
		}
		keys := admin.Scan(cleanup, 0, prefix+":*", 64).Iterator()
		for keys.Next(cleanup) {
			if err := admin.Del(cleanup, keys.Val()).Err(); err != nil {
				t.Error(err)
			}
		}
		if err := keys.Err(); err != nil {
			t.Error(err)
		}
	})
	client := goredis.NewClient(&goredis.Options{Addr: addr, Username: user, Password: password})
	t.Cleanup(func() { _ = client.Close() })
	store := crewredis.NewAGUISessionStore(crewredis.NewCheckpointer(client, crewredis.WithCheckpointerPrefix(prefix)))
	const sid, key = "old-task", "thread"
	ctx = compose_v2.WithCheckpointerCtx(ctx, store)
	ctx, thread, err := compose_v2.BeginTaskActivity(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer thread.Release()
	ctx, task, err := compose_v2.BeginTaskActivity(ctx, sid, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Release()
	for _, ns := range []string{"", "branch", "branch/child", "branch#1"} {
		cp := &compose_v2.Checkpoint{GraphID: "workflow", Status: compose_v2.CPStatusPending}
		if err := store.SaveNS(ctx, sid, ns, cp); err != nil {
			t.Fatalf("运行角色应可保存存档 %q: %v", ns, err)
		}
	}
	assertCheckpoint := func(ns string, want bool) {
		t.Helper()
		got, err := store.LoadNS(ctx, sid, ns)
		if err != nil || (got != nil) != want || (got != nil && got.GraphID != "workflow") {
			t.Fatalf("存档 %q 预期存在 %v，实际 %+v 错误 %v", ns, want, got, err)
		}
	}
	assertCheckpoint("branch/child", true)
	if err := client.Exists(ctx, prefix+":cp:sids").Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("测试角色的 EXISTS 应被实际拒绝，错误 %v", err)
	}
	if _, err := store.SaveSession(ctx, key, &agui.SessionRecord{TaskID: sid}, ""); err != nil {
		t.Fatal(err)
	}

	var check compose_v2.CheckpointDeleteCondition
	slots, err := store.LoadAllInSession(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	for _, slot := range slots {
		if compose_v2.IsNSInSubtree("branch", slot.NS) {
			check.Expected = append(check.Expected, slot)
		}
	}
	setACL("-srem")
	if n, err := store.DeleteSubtree(ctx, sid, "branch", check); err == nil || n != 0 {
		t.Fatalf("缺少索引写权限时条件删除同样应保留全树，删除 %d 错误 %v", n, err)
	}
	if n, err := store.SaveSession(ctx, key, nil, sid); err == nil || n != 0 {
		t.Fatalf("无索引写权限时应拒绝整次清理，删除 %d 错误 %v", n, err)
	}
	for _, ns := range []string{"", "branch", "branch/child", "branch#1"} {
		assertCheckpoint(ns, true)
	}
	if record, err := store.LoadSession(ctx, key); err != nil || record == nil || record.TaskID != sid {
		t.Fatalf("清理失败必须保留会话记录，实际 %+v 错误 %v", record, err)
	}
	setACL("+srem")

	if n, err := store.DeleteSubtree(ctx, sid, "branch", check); err != nil || n != 2 {
		t.Fatalf("运行角色应能清理自身和后代，删除 %d 错误 %v", n, err)
	}
	assertCheckpoint("branch", false)
	assertCheckpoint("branch/child", false)
	assertCheckpoint("branch#1", true)
	// 同一个角色也可通过选择器把 TYPE 限制在索引上，保留对 payload 的原有 EXISTS 权限。
	setACL("+exists", "-type", "(+type ~"+prefix+":cp:nsidx:* ~"+prefix+":cp:pending:* ~"+
		prefix+":cp:sids ~"+prefix+":agui:cancel)")
	payload := prefix + ":cp:item-v2:" + base64.RawURLEncoding.EncodeToString([]byte(sid)) + ":"
	if err := client.Type(ctx, payload).Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("选择器应禁止对 payload 使用 TYPE，错误 %v", err)
	}
	if n, err := store.SaveSession(ctx, key, nil, sid); err != nil || n != 2 {
		t.Fatalf("会话操作应原子清除剩余存档，删除 %d 错误 %v", n, err)
	}
	assertCheckpoint("", false)
	assertCheckpoint("branch#1", false)
	if record, err := store.LoadSession(ctx, key); err != nil || record != nil {
		t.Fatalf("会话应随旧存档删除，实际 %+v 错误 %v", record, err)
	}
	if pending, err := store.ListPending(ctx, sid); err != nil || len(pending) != 0 {
		t.Fatalf("清档后索引应为空，实际 %+v 错误 %v", pending, err)
	}
}
