package agui_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
)

// alwaysFailSaveCP 的 SaveNS 一律报错（存储连不上）。
type alwaysFailSaveCP struct {
	*compose_v2.InMemoryCheckpointer
}

func (alwaysFailSaveCP) SaveNS(context.Context, string, string, *compose_v2.Checkpoint) error {
	return errors.New("dial tcp 10.0.0.7:5432: connection refused")
}

// TestHandler_CheckpointNotPersisted：中断时存档没写成，不把提问发给前端（答完续不上），给一个专属 code；
// 挂起状态清掉，续发得 no_session。曾经照发 RUN_FINISHED(outcome=interrupt)，用户答完续跑得
// no_pending_checkpoint、工具一次都没执行。
func TestHandler_CheckpointNotPersisted(t *testing.T) {
	h := ownerAwareHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("x"),
			agent_v2.WithModel(&askThenAnswer{}),
			agent_v2.WithAskUser(), agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(5))
	}, agui.WithCheckpointer(alwaysFailSaveCP{compose_v2.NewInMemoryCheckpointer()}))

	const tid = "thread-np"
	events := parseSSE(postAs(h, "tenant-A",
		`{"threadId":"`+tid+`","runId":"r1","messages":[{"role":"user","content":"hi"}]}`).Body.String())
	if id := findInterruptID(events); id != "" {
		t.Fatalf("存档没写成，不该把提问发给前端（答完续不上），interruptId=%s", id)
	}
	if code := findErrorCode(events); code != "checkpoint_save_failed" {
		t.Fatalf("错误码应为 checkpoint_save_failed，实际 %q", code)
	}
	for _, ev := range events {
		if ev["type"] != "RUN_ERROR" {
			continue
		}
		if msg, _ := ev["message"].(string); strings.Contains(msg, "connection refused") {
			t.Errorf("错误文案带出了后端原文：%q", msg)
		}
	}
	resume := `{"threadId":"` + tid + `","resume":[{"interruptId":"any","status":"resolved","payload":"a"}]}`
	if code := findErrorCode(parseSSE(postAs(h, "tenant-A", resume).Body.String())); code != "no_session" {
		t.Errorf("挂起状态应已清掉，续发应得 no_session，实际 %q", code)
	}
}

// ctxHonoringCP 包一层内存存档：ctx 已结束的读写直接报错（同持久后端），并记下写过的 sid。
type ctxHonoringCP struct {
	*compose_v2.InMemoryCheckpointer
	mu   sync.Mutex
	sids []string
}

func (c *ctxHonoringCP) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c.mu.Lock()
	c.sids = append(c.sids, sid)
	c.mu.Unlock()
	return c.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}

func (c *ctxHonoringCP) DeleteSubtree(ctx context.Context, sid, ns string) (int32, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	return c.InMemoryCheckpointer.DeleteSubtree(ctx, sid, ns)
}

// blockUntilCanceled 的模型调用一直等到请求 ctx 被取消。
type blockUntilCanceled struct {
	entered chan struct{}
	once    sync.Once
}

func (m *blockUntilCanceled) wait(ctx context.Context) error {
	m.once.Do(func() { close(m.entered) })
	<-ctx.Done()
	return ctx.Err()
}

func (m *blockUntilCanceled) Generate(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return nil, m.wait(ctx)
}

func (m *blockUntilCanceled) Stream(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, m.wait(ctx)
}

func (m *blockUntilCanceled) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHandler_DroppedSessionLeavesNoCheckpoint：浏览器断开（请求 ctx 取消）时引擎照常存下可以续跑的档（出口写档
// 不跟着调用方取消），而 agui 这边丢掉了会话句柄、没有续跑入口——档要一起清，不然持久后端上越积越多、一直
// 挂在 ListPending 里（审计实测）。
func TestHandler_DroppedSessionLeavesNoCheckpoint(t *testing.T) {
	cp := &ctxHonoringCP{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
	m := &blockUntilCanceled{entered: make(chan struct{})}
	h := ownerAwareHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("x"), agent_v2.WithModel(m),
			agent_v2.WithHooks(hooks), agent_v2.WithMaxRounds(3))
	}, agui.WithCheckpointer(cp))
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), testOwnerKey{}, "A"))
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/agui",
		strings.NewReader(`{"threadId":"t-dc","runId":"r1","messages":[{"role":"user","content":"hi"}]}`)).WithContext(ctx)
	go func() {
		<-m.entered
		cancel()
	}()
	h.ServeHTTP(httptest.NewRecorder(), req)
	cp.mu.Lock()
	sids := append([]string(nil), cp.sids...)
	cp.mu.Unlock()
	if len(sids) == 0 {
		t.Fatal("前置：调用方取消之后出口应写过档")
	}
	for _, sid := range sids {
		if pend, _ := cp.InMemoryCheckpointer.ListPending(context.Background(), sid); len(pend) != 0 {
			t.Fatalf("agui 丢掉了会话句柄，档却还在（sid=%s，%d 份挂起）", sid, len(pend))
		}
	}
}
