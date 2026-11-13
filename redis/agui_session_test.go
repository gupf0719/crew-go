package redis_test

import (
	"context"
	"crypto/rand"
	"errors"
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

func TestRedisAGUICancellationSurvivesProcessExit(t *testing.T) {
	addr, dir := aguiRedisAddress(t), t.TempDir()
	for _, phase := range []string{"cancel", "restart"} {
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRedisAGUICancelProcessWorker$")
		cmd.Env = append(os.Environ(), "CREW_TEST_REDIS_CANCEL_ADDR="+addr, "CREW_TEST_REDIS_CANCEL_DIR="+dir, "CREW_TEST_REDIS_CANCEL_PHASE="+phase)
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("%s worker: %v\n%s", phase, err, out)
		}
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
	post := func(body string) string {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/agui", strings.NewReader(body)))
		return r.Body.String()
	}
	cancelBody := func(id, status string) string {
		return `{"threadId":"shared","resume":[{"interruptId":"` + id + `","status":"` + status + `","payload":"yes"}]}`
	}
	ref := filepath.Join(dir, "task.txt")
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
		os.Exit(0)
	case "restart":
		raw, err := os.ReadFile(ref)
		if err != nil {
			t.Fatal(err)
		}
		task, id, ok := strings.Cut(string(raw), "\n")
		if !ok {
			t.Fatal("missing task reference")
		}
		if got := post(cancelBody(id, "resolved")); !strings.Contains(got, "cleanup_pending") {
			t.Fatalf("cancelled task remained executable: %s", got)
		}
		if got := post(cancelBody(id, "cancelled")); !strings.Contains(got, `"type":"cancelled"`) {
			t.Fatalf("restart failed to clean up: %s", got)
		}
		if exists, err := cp.ExistsNS(t.Context(), task, ""); err != nil || exists || executions.Load() != 0 {
			t.Fatalf("cleanup failed: exists=%v executions=%d err=%v", exists, executions.Load(), err)
		}
	default:
		t.Fatal("unknown worker phase")
	}
}
