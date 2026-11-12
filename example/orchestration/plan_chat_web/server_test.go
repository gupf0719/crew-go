package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/observability"
	agentRedis "crew-go/redis"
	"crew-go/schema"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

type pauseModel struct {
	once    sync.Once
	started chan struct{}
	proceed chan struct{}
}

func (m *pauseModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *pauseModel) Generate(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.once.Do(func() { close(m.started) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.proceed:
		return schema.AssistantMessage("完成"), nil
	}
}

func (m *pauseModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func TestPlanWebSharedSessionLifecycle(t *testing.T) {
	redisURL := os.Getenv("PLAN_WEB_TEST_REDIS_URL")
	options := &goredis.Options{}
	if redisURL == "" {
		options.Addr = miniredis.RunT(t).Addr()
	} else {
		var err error
		options, err = goredis.ParseURL(redisURL)
		if err != nil {
			t.Fatal("PLAN_WEB_TEST_REDIS_URL 无效")
		}
	}
	m := &pauseModel{started: make(chan struct{}), proceed: make(chan struct{})}
	opened := false
	t.Cleanup(func() {
		if !opened {
			close(m.proceed)
		}
	})
	newReplica := func() *httptest.Server {
		rdb := goredis.NewClient(options)
		t.Cleanup(func() { _ = rdb.Close() })
		app := &chatServer{
			model: m, obs: observability.New(), sessions: &redisSessions{client: rdb, ttl: time.Minute},
			cp:             agentRedis.NewCheckpointer(rdb, agentRedis.WithCheckpointerPrefix("crew-plan-web")),
			requestTimeout: 5 * time.Second, durable: true,
		}
		server := httptest.NewServer(app.routes())
		t.Cleanup(server.Close)
		return server
	}
	a, b := newReplica(), newReplica()
	owner := rand.Text()
	client := &http.Client{Timeout: 8 * time.Second}
	type response struct {
		code int
		body string
		err  error
	}
	request := func(method, url, who string) response {
		req, err := http.NewRequestWithContext(t.Context(), method, url, nil)
		if err != nil {
			return response{err: err}
		}
		req.AddCookie(&http.Cookie{Name: ownerCookie, Value: who})
		resp, err := client.Do(req)
		if err != nil {
			return response{err: err}
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		return response{code: resp.StatusCode, body: string(body), err: err}
	}
	run := make(chan response, 1)
	go func() { run <- request(http.MethodGet, a.URL+"/api/chat?q=start", owner) }()
	select {
	case <-m.started:
	case resp := <-run:
		t.Fatalf("模型还没启动: %+v", resp)
	case <-time.After(5 * time.Second):
		t.Fatal("模型没有启动")
	}
	list := request(http.MethodGet, b.URL+"/api/sessions?status=running&limit=1", owner)
	var sessions []sessionRecord
	if list.err != nil || list.code != http.StatusOK || json.Unmarshal([]byte(list.body), &sessions) != nil || len(sessions) != 1 {
		t.Fatalf("另一个副本没有看到运行中的会话: %+v", list)
	}
	id := sessions[0].ID
	resumeURL := b.URL + "/api/chat?session_id=" + id
	sessionURL := b.URL + "/api/sessions/" + id
	t.Cleanup(func() {
		req, _ := http.NewRequest(http.MethodDelete, sessionURL, nil)
		req.AddCookie(&http.Cookie{Name: ownerCookie, Value: owner})
		if resp, err := client.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	})
	if got := request(http.MethodGet, sessionURL, rand.Text()); got.code != http.StatusNotFound {
		t.Fatalf("其他会话身份看到了任务: %+v", got)
	}
	if got := request(http.MethodGet, resumeURL, owner); got.code != http.StatusConflict {
		t.Fatalf("并发续跑没有被拒: %+v", got)
	}
	if got := request(http.MethodDelete, sessionURL, owner); got.code != http.StatusConflict {
		t.Fatalf("并发删除没有被拒: %+v", got)
	}
	if got := request(http.MethodPost, b.URL+"/api/interrupt?session_id="+id, owner); got.code != http.StatusOK {
		t.Fatalf("跨副本暂停失败: %+v", got)
	}
	close(m.proceed)
	opened = true
	if got := <-run; got.err != nil || !strings.Contains(got.body, `"status":"interrupted"`) {
		t.Fatalf("没有挂起: %+v", got)
	}
	if got := request(http.MethodGet, resumeURL, owner); got.err != nil || !strings.Contains(got.body, `"status":"success"`) {
		t.Fatalf("跨副本续跑失败: %+v", got)
	}
	if got := request(http.MethodDelete, sessionURL, owner); got.code != http.StatusNoContent {
		t.Fatalf("删除会话失败: %+v", got)
	}
	if got := request(http.MethodGet, sessionURL, owner); got.code != http.StatusNotFound {
		t.Fatalf("删除后会话仍存在: %+v", got)
	}
	if got := request(http.MethodGet, a.URL+"/api/chat?q=temporary&ephemeral=true", owner); got.err != nil || !strings.Contains(got.body, `"status":"success"`) {
		t.Fatalf("临时会话未完成: %+v", got)
	}
	if got := request(http.MethodGet, b.URL+"/api/sessions", owner); got.code != http.StatusOK || strings.TrimSpace(got.body) != "[]" {
		t.Fatalf("完成的临时会话仍然可见: %+v", got)
	}
}

func TestPlanWebInMemorySessionExpiry(t *testing.T) {
	store := newMemorySessions(time.Minute)
	key := sessionKey{owner: rand.Text(), id: "demo"}
	if err := store.update(t.Context(), key, func(*sessionRecord) (*sessionRecord, error) {
		return &sessionRecord{ID: key.id, Status: statusSuccess, LastActivity: time.Now().Add(-time.Hour)}, nil
	}); err != nil {
		t.Fatal(err)
	}
	app := &chatServer{sessions: store, cp: compose_v2.NewInMemoryCheckpointer()}
	_, err := app.load(t.Context(), key)
	if !errors.Is(err, errSessionMissing) {
		t.Fatalf("过期的终态会话应已清理: %v", err)
	}
}

type gatedSessionEdit struct {
	sessionStore
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *gatedSessionEdit) update(ctx context.Context, key sessionKey, edit func(*sessionRecord) (*sessionRecord, error)) error {
	return s.sessionStore.update(ctx, key, func(r *sessionRecord) (*sessionRecord, error) {
		s.once.Do(func() { close(s.reached) })
		<-s.release
		return edit(r)
	})
}

func TestPlanWebExpiredLeaseCannotCommitSession(t *testing.T) {
	mini := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	key := sessionKey{owner: rand.Text(), id: "fenced-transaction"}
	cp := agentRedis.NewCheckpointer(client, agentRedis.WithCheckpointerPrefix("crew-plan-web"))
	store := &redisSessions{client: client}
	if err := store.update(t.Context(), key, func(*sessionRecord) (*sessionRecord, error) {
		return &sessionRecord{ID: key.id, Status: statusRunning}, nil
	}); err != nil {
		t.Fatal(err)
	}
	gate := &gatedSessionEdit{sessionStore: store, reached: make(chan struct{}), release: make(chan struct{})}
	a := &chatServer{cp: cp, sessions: gate}
	ctx, release, err := a.acquire(a.taskContext(t.Context(), key.owner), key.id, false)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	finished := make(chan error, 1)
	go func() { finished <- a.finish(ctx, key, statusInterrupted, "old", 0) }()
	select {
	case <-gate.reached:
	case err := <-finished:
		t.Fatalf("didn't reach watched metadata edit: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out reaching watched metadata edit")
	}
	defer func() {
		select {
		case <-gate.release:
		default:
			close(gate.release)
		}
	}()
	mini.FastForward(time.Minute)
	leases := cp.CheckpointTaskLeases()
	if ok, err := leases.TryAcquireTaskLease(t.Context(), key.id, "other-process-token", time.Minute); err != nil || !ok {
		t.Fatalf("could not acquire successor lease: %t %v", ok, err)
	}
	close(gate.release)
	finishErr := <-finished
	rec, readErr := store.get(t.Context(), key)
	if !errors.Is(finishErr, compose_v2.ErrTaskLeaseLost) || readErr != nil || rec == nil || rec.Status != statusRunning {
		t.Fatalf("transaction succeeded with expired token: err=%v record=%+v read err=%v", finishErr, rec, readErr)
	}
}
