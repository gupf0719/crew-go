package redis_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	crewredis "crew-go/redis"
	"crew-go/schema"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func aguiRedisAddress(t *testing.T) string {
	t.Helper()
	if addr := os.Getenv("CREW_TEST_REDIS_ADDR"); addr != "" {
		return addr
	}
	return miniredis.RunT(t).Addr()
}

func TestRedisAGUISessionStoreFencesBothTasks(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: aguiRedisAddress(t)})
	t.Cleanup(func() { _ = client.Close() })
	prefix := "agui-fences:" + rand.Text()
	s := crewredis.NewAGUISessionStore(crewredis.NewCheckpointer(client, crewredis.WithCheckpointerPrefix(prefix)))
	ctx := compose_v2.WithCheckpointerCtx(t.Context(), s)
	ctx, thread, err := compose_v2.BeginTaskActivity(ctx, "thread", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer thread.Release()
	ctx, tasks, err := compose_v2.BeginTaskActivity(ctx, "new", []string{"old"})
	if err != nil {
		t.Fatal(err)
	}
	defer tasks.Release()
	for _, ns := range []string{"", "child"} {
		if err := s.SaveNS(ctx, "old", ns, &compose_v2.Checkpoint{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SaveNS(ctx, "other", "", &compose_v2.Checkpoint{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SaveSession(ctx, "thread", &agui.SessionRecord{TaskID: "old"}, ""); err != nil {
		t.Fatal(err)
	}
	// 候选记录的任务缺少令牌时，即使被删任务有令牌也不能先删档。
	if _, err := s.SaveSession(ctx, "thread", &agui.SessionRecord{TaskID: "unleased"}, "old"); !errors.Is(err, compose_v2.ErrTaskLeaseLost) {
		t.Fatalf("missing record-task lease: %v", err)
	}
	for _, id := range []string{"thread", "old", "new"} {
		key := prefix + ":cp:lease:" + id
		token, err := client.Get(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Set(ctx, key, "different-holder", time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.SaveSession(ctx, "thread", &agui.SessionRecord{TaskID: "new"}, "old"); !errors.Is(err, compose_v2.ErrTaskLeaseLost) {
			t.Fatalf("lost %s lease: %v", id, err)
		}
		if err := client.Set(ctx, key, token, time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
	}
	// Redis 脚本运行时错误不会回滚，索引类型错误须发生在任何删除之前。
	pending := prefix + ":cp:pending:old"
	if err := client.Set(ctx, pending, "invalid-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SaveSession(ctx, "thread", &agui.SessionRecord{TaskID: "new"}, "old"); err == nil {
		t.Fatal("expected the corrupt pending index to reject replacement")
	}
	if cp, err := s.LoadAllInSession(ctx, "old"); err != nil || len(cp) != 2 {
		t.Fatalf("failed transaction changed old checkpoints: %d %v", len(cp), err)
	}
	if err := client.Del(ctx, pending).Err(); err != nil {
		t.Fatal(err)
	}
	if record, err := s.LoadSession(ctx, "thread"); err != nil || record == nil || record.TaskID != "old" {
		t.Fatalf("failed transaction replaced record: %+v %v", record, err)
	}
	n, err := s.SaveSession(ctx, "thread", &agui.SessionRecord{TaskID: "new"}, "old")
	if err != nil || n != 2 {
		t.Fatalf("want atomic replacement of old task, got %d %v", n, err)
	}
	if record, err := s.LoadSession(ctx, "thread"); err != nil || record == nil || record.TaskID != "new" {
		t.Fatalf("replacement missing: %+v %v", record, err)
	}
	if ok, err := s.ExistsNS(ctx, "other", ""); err != nil || !ok {
		t.Fatalf("replacement touched unrelated task: %v %v", ok, err)
	}
}

func TestRedisAGUIRecoveryIgnoresNestedStorePrefix(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: aguiRedisAddress(t)})
	t.Cleanup(func() { _ = client.Close() })
	prefix := "agui-prefix:" + rand.Text()
	outer := crewredis.NewAGUISessionStore(crewredis.NewCheckpointer(client, crewredis.WithCheckpointerPrefix(prefix)))
	inner := crewredis.NewAGUISessionStore(crewredis.NewCheckpointer(client, crewredis.WithCheckpointerPrefix(prefix+":agui:session:inner")))
	key := func(thread string) string {
		return fmt.Sprintf("agui-thread:%x", sha256.Sum256([]byte("6:tenant"+thread)))
	}
	seed := func(store *crewredis.AGUISessionStore, key, task string) {
		t.Helper()
		ctx := compose_v2.WithCheckpointerCtx(t.Context(), store)
		ctx, sessionLease, err := compose_v2.BeginTaskActivity(ctx, key, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer sessionLease.Release()
		ctx, taskLease, err := compose_v2.BeginTaskActivity(ctx, task, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer taskLease.Release()
		if err := store.SaveNS(ctx, task, "", &compose_v2.Checkpoint{}); err != nil {
			t.Fatal(err)
		}
		// 旧记录没有 thread ID，必须用存储键形状识别本实例的会话。
		if _, err := store.SaveSession(ctx, key, &agui.SessionRecord{TaskID: task, Closing: true,
			Cancellation: &agui.SessionCancellation{InterruptIDs: []string{"old"}}}, ""); err != nil {
			t.Fatal(err)
		}
	}
	seed(inner, key("inner"), "inner-task")
	seed(outer, key("outer"), "outer-task")
	graph, err := compose_v2.NewBuilder().WithName("prefix-isolation").AddNode("work", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, errors.New("background recovery must never run the graph")
	}).AddEdge(compose_v2.StartNode, "work").AddEdge("work", compose_v2.EndNode).Compile()
	if err != nil {
		t.Fatal(err)
	}
	var factories atomic.Int32
	candidate := "inner:agui:session:" + key("inner")
	h := agui.Handler(func(hook.Hooks) agent_v2.Agent {
		factories.Add(1)
		return agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName("prefix-isolation"),
			agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return schema.AssistantMessage("done") }))
	}, agui.WithSessionStore(firstCancellationCandidate{AGUISessionStore: outer, key: candidate}),
		agui.WithOwnerResolver(func(context.Context) string { return "tenant" }))
	defer h.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	waitRedisCancellation(t, ctx, outer, key("outer"))
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if rec, err := inner.LoadSession(t.Context(), key("inner")); err != nil || rec == nil || factories.Load() != 1 {
		t.Fatalf("outer recovery crossed the store prefix: record=%+v factory_calls=%d err=%v", rec, factories.Load(), err)
	}
	if ok, err := inner.ExistsNS(t.Context(), "inner-task", ""); err != nil || !ok {
		t.Fatalf("inner checkpoint was lost: %v %v", ok, err)
	}
}

type firstCancellationCandidate struct {
	*crewredis.AGUISessionStore
	key string
}

func (s firstCancellationCandidate) ListCancellations(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	keys, next, err := s.AGUISessionStore.ListCancellations(ctx, cursor, limit)
	if cursor == "" {
		// 固定 SCAN 无序候选的顺序，先让宿主碰到另一个存储的旧记录。
		keys = append([]string{s.key}, keys...)
	}
	return keys, next, err
}

func TestRedisAGUICancellationSurvivesProcessExit(t *testing.T) {
	addr := aguiRedisAddress(t)
	for _, restart := range []string{"restart", "idle"} {
		t.Run(restart, func(t *testing.T) {
			dir := t.TempDir()
			for _, phase := range []string{"cancel", restart} {
				ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRedisAGUICancelProcessWorker$")
				cmd.Env = append(os.Environ(), "CREW_TEST_REDIS_CANCEL_ADDR="+addr, "CREW_TEST_REDIS_CANCEL_DIR="+dir, "CREW_TEST_REDIS_CANCEL_PHASE="+phase)
				out, err := cmd.CombinedOutput()
				cancel()
				if err != nil || !strings.Contains(string(out), "verified-"+phase) {
					t.Fatalf("%s worker: %v\n%s", phase, err, out)
				}
			}
		})
	}
}

type failedRedisCancellation struct {
	*crewredis.AGUISessionStore
}

func (s failedRedisCancellation) SaveSession(ctx context.Context, key string, record *agui.SessionRecord, dropTask string) (int32, error) {
	if dropTask != "" {
		return 0, errors.New("injected checkpoint deletion failure")
	}
	return s.AGUISessionStore.SaveSession(ctx, key, record, dropTask)
}

func TestRedisAGUICancelProcessWorker(t *testing.T) {
	addr := os.Getenv("CREW_TEST_REDIS_CANCEL_ADDR")
	if addr == "" {
		t.Skip("由跨进程取消测试启动")
	}
	phase, dir := os.Getenv("CREW_TEST_REDIS_CANCEL_PHASE"), os.Getenv("CREW_TEST_REDIS_CANCEL_DIR")
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	cp := crewredis.NewCheckpointer(client, crewredis.WithCheckpointerPrefix(filepath.Base(dir)))
	s := crewredis.NewAGUISessionStore(cp)
	ref := filepath.Join(dir, "task.txt")
	key := fmt.Sprintf("agui-thread:%x", sha256.Sum256([]byte("6:tenantshared")))
	if phase == "idle" {
		before, err := s.LoadSession(t.Context(), key)
		if err != nil || before == nil || before.Cancellation == nil || before.Cancellation.CheckpointsDeleted {
			t.Fatalf("fixture has no uncompleted cancellation: %+v %v", before, err)
		}
		if ok, err := cp.ExistsNS(t.Context(), before.TaskID, ""); err != nil || !ok {
			t.Fatalf("fixture has no checkpoint: %v %v", ok, err)
		}
	}
	var store agui.SessionStore = s
	if phase == "cancel" {
		store = failedRedisCancellation{s}
	}
	var sid atomic.Value
	var executions atomic.Int32
	graph, err := compose_v2.NewBuilder().WithName("redis-cancel").
		AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			sid.Store(compose_v2.GetSessionID(ctx))
			_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "continue?"})
			if err == nil {
				executions.Add(1)
			}
			return nil, err
		}).AddEdge(compose_v2.StartNode, "ask").AddEdge("ask", compose_v2.EndNode).Compile()
	if err != nil {
		t.Fatal(err)
	}
	h := agui.Handler(func(hook.Hooks) agent_v2.Agent {
		return agent_v2.NewGraphAgent(graph, agent_v2.WithGraphAgentName("redis-cancel"),
			agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
			agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return schema.AssistantMessage("done") }))
	}, agui.WithOwnerResolver(func(context.Context) string { return "tenant" }), agui.WithSessionStore(store))
	t.Cleanup(func() { _ = h.Close() })
	if phase != "idle" {
		// 请求路径仍可在后台补偿停用时接手。
		_ = h.Close()
	}
	post := func(body string) string {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/agui", strings.NewReader(body)))
		return r.Body.String()
	}
	cancelBody := func(id, status string) string {
		return `{"threadId":"shared","resume":[{"interruptId":"` + id + `","status":"` + status + `","payload":"yes"}]}`
	}
	switch phase {
	case "cancel":
		post(`{"threadId":"shared","messages":[{"role":"user","content":"begin"}]}`)
		task, _ := sid.Load().(string)
		saved, err := cp.LoadNS(t.Context(), task, "")
		if err != nil || saved == nil || len(saved.PendingInterrupts) != 1 {
			t.Fatalf("fixture did not interrupt: %v %v", saved, err)
		}
		id := saved.PendingInterrupts[0].InterruptID
		if got := post(cancelBody(id, "cancelled")); !strings.Contains(got, "abort_failed") {
			t.Fatalf("expected deletion fault: %s", got)
		}
		if exists, err := cp.ExistsNS(t.Context(), task, ""); err != nil || !exists {
			t.Fatalf("checkpoint not retained on failure: %v %v", exists, err)
		}
		if err := os.WriteFile(ref, []byte(task+"\n"+id), 0600); err != nil {
			t.Fatal(err)
		}
		fmt.Println("verified-cancel")
		os.Exit(0)
	case "restart", "idle":
		raw, err := os.ReadFile(ref)
		if err != nil {
			t.Fatal(err)
		}
		task, id, ok := strings.Cut(string(raw), "\n")
		if !ok {
			t.Fatal("missing task reference")
		}
		if phase == "idle" {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			waitRedisCancellation(t, ctx, s, key)
		} else {
			if got := post(cancelBody(id, "resolved")); !strings.Contains(got, "cleanup_pending") {
				t.Fatalf("cancelled task remained executable: %s", got)
			}
			if got := post(cancelBody(id, "cancelled")); !strings.Contains(got, `"type":"cancelled"`) {
				t.Fatalf("restart failed to clean up: %s", got)
			}
		}
		if exists, err := cp.ExistsNS(t.Context(), task, ""); err != nil || exists || executions.Load() != 0 {
			t.Fatalf("cleanup failed: exists=%v executions=%d err=%v", exists, executions.Load(), err)
		}
		fmt.Println("verified-" + phase)
	default:
		t.Fatal("unknown worker phase")
	}
}

func waitRedisCancellation(t *testing.T, ctx context.Context, s *crewredis.AGUISessionStore, key string) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		record, err := s.LoadSession(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if record == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("idle handler did not clear durable cancellation")
		case <-ticker.C:
		}
	}
}
