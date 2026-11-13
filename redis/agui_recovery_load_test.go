//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"crew-go/agui"
	"crew-go/internal/aguiload"
	crewredis "crew-go/redis"
	"crew-go/schema"
	goredis "github.com/redis/go-redis/v9"
)

func TestAGUICancellationLoad(t *testing.T) {
	addr := os.Getenv("CREW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("需要隔离的 Redis")
	}
	for _, replicas := range []int{1, 4} {
		t.Run(fmt.Sprintf("replicas_%d", replicas), func(t *testing.T) {
			ctx := t.Context()
			client := goredis.NewClient(&goredis.Options{Addr: addr})
			defer client.Close()
			prefix := fmt.Sprintf("load_%d", time.Now().UnixNano())
			store := crewredis.NewAGUISessionStore(crewredis.NewCheckpointer(client, crewredis.WithCheckpointerPrefix(prefix)))
			payload, err := json.Marshal(&agui.SessionRecord{TaskID: "normal", Prior: []*schema.Message{schema.UserMessage(strings.Repeat("x", 4096))}})
			if err != nil {
				t.Fatal(err)
			}
			for from := 0; from < aguiload.Sessions; from += 500 {
				_, err := client.Pipelined(ctx, func(p goredis.Pipeliner) error {
					for i := from; i < min(from+500, aguiload.Sessions); i++ {
						p.Set(ctx, fmt.Sprintf("%s:agui:session:agui-thread:%064x", prefix, i), payload, 0)
						p.Set(ctx, fmt.Sprintf("%s:cp:item:normal-%d:", prefix, i), "{}", 0)
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			aguiload.Run(t, store, replicas, func() string {
				info, err := client.Info(ctx, "commandstats", "cpu").Result()
				if err != nil {
					t.Fatal(err)
				}
				var counters []string
				for _, line := range strings.Split(info, "\r\n") {
					if strings.HasPrefix(line, "cmdstat_scan:") || strings.HasPrefix(line, "cmdstat_get:") || strings.HasPrefix(line, "cmdstat_zrange") || strings.HasPrefix(line, "used_cpu_user:") || strings.HasPrefix(line, "used_cpu_sys:") {
						counters = append(counters, line)
					}
				}
				return strings.Join(counters, " ")
			})
		})
	}
}

// TestAGUICancellationMigrationRetriesAfterDeniedRead 验证权限恢复后仍能回填原取消意图。
func TestAGUICancellationMigrationRetriesAfterDeniedRead(t *testing.T) {
	addr := os.Getenv("CREW_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("需要支持 ACL 的隔离 Redis")
	}
	ctx := t.Context()
	admin := goredis.NewClient(&goredis.Options{Addr: addr})
	defer admin.Close()
	prefix := fmt.Sprintf("migration_%d", time.Now().UnixNano())
	key := aguiload.Key("legacy")
	if err := admin.Set(ctx, prefix+":agui:session:"+key, `{"task_id":"legacy","closing":true,"cancellation":{}}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	user := prefix
	if err := admin.Do(ctx, "ACL", "SETUSER", user, "on", ">fixture-only", "~"+prefix+"*", "+@all", "-get").Err(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := admin.Do(context.Background(), "ACL", "DELUSER", user).Err(); err != nil {
			t.Error(err)
		}
	}()
	client := goredis.NewClient(&goredis.Options{Addr: addr, Username: user, Password: "fixture-only"})
	defer client.Close()
	store := crewredis.NewAGUISessionStore(crewredis.NewCheckpointer(client, crewredis.WithCheckpointerPrefix(prefix)))
	list := func() ([]string, error) {
		var selected []string
		for cursor := ""; ; {
			keys, next, err := store.ListCancellations(ctx, cursor, 8)
			if err != nil {
				return nil, err
			}
			selected = append(selected, keys...)
			if next == "" {
				return selected, nil
			}
			cursor = next
		}
	}
	if _, err := list(); err == nil {
		t.Fatal("unreadable cancellation marked as migrated")
	}
	if n, err := admin.Exists(ctx, prefix+":agui:cancel:ready").Result(); err != nil || n != 0 {
		t.Fatalf("migration acknowledged: %d %v", n, err)
	}
	if err := admin.Do(ctx, "ACL", "SETUSER", user, "+get").Err(); err != nil {
		t.Fatal(err)
	}
	if keys, err := list(); err != nil || len(keys) != 1 || keys[0] != key {
		t.Fatalf("legacy cancellation missing: %v %v", keys, err)
	}
}
