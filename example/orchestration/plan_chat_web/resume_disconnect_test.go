package main

import (
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/observability"
	agentRedis "crew-go/redis"
	"crew-go/schema"
)

type resumeDisconnectModel struct {
	calls                    atomic.Int32
	started, pause, resuming chan struct{}
}

func (m *resumeDisconnectModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *resumeDisconnectModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *resumeDisconnectModel) Generate(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	switch m.calls.Add(1) {
	case 1:
		close(m.started)
		select {
		case <-m.pause:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "write", Type: "function",
			Function: schema.FunctionCall{Name: "ask_writer", Arguments: `{"input":"test"}`},
		}}), nil
	case 2:
		if m.resuming != nil {
			close(m.resuming)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		fallthrough
	default:
		return schema.AssistantMessage("finished"), nil
	}
}

type disconnectClaimCheckpoint struct {
	*compose_v2.InMemoryCheckpointer
	entered     chan struct{}
	commit      bool
	failRead    bool
	claimed     atomic.Bool
	readFailure atomic.Bool
}

func (c *disconnectClaimCheckpoint) ClaimResume(ctx context.Context, sid, ns string, expected, source *compose_v2.Checkpoint) (*compose_v2.Checkpoint, error) {
	if ns != "" || !c.claimed.CompareAndSwap(false, true) {
		return c.InMemoryCheckpointer.ClaimResume(ctx, sid, ns, expected, source)
	}
	if c.commit {
		if _, err := c.InMemoryCheckpointer.ClaimResume(ctx, sid, ns, expected, source); err != nil {
			return nil, err
		}
	}
	close(c.entered)
	<-ctx.Done()
	c.readFailure.Store(c.failRead)
	return nil, ctx.Err()
}

func (c *disconnectClaimCheckpoint) LoadNS(ctx context.Context, sid, ns string) (*compose_v2.Checkpoint, error) {
	if ns == "" && ctx.Err() == nil && c.readFailure.CompareAndSwap(true, false) {
		return nil, errors.New("temporary checkpoint read failure")
	}
	return c.InMemoryCheckpointer.LoadNS(ctx, sid, ns)
}

type interruptedSettleFailure struct {
	sessionStore
	fail atomic.Bool
}

func (s *interruptedSettleFailure) update(ctx context.Context, key sessionKey, edit func(*sessionRecord) (*sessionRecord, error)) error {
	return s.sessionStore.update(ctx, key, func(current *sessionRecord) (*sessionRecord, error) {
		next, err := edit(current)
		if err == nil && next != nil && next.Status == statusInterrupted && s.fail.CompareAndSwap(true, false) {
			return nil, errors.New("temporary session commit failure")
		}
		return next, err
	})
}

func TestPlanWebResumeDisconnectKeepsPendingCheckpoint(t *testing.T) {
	for _, backend := range []string{"memory", "redis", "before-claim", "after-claim", "recovery-read-failure"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			m := &resumeDisconnectModel{started: make(chan struct{}), pause: make(chan struct{}), resuming: make(chan struct{})}
			app := &chatServer{model: m, obs: observability.New(), sessions: newMemorySessions(time.Minute),
				cp: compose_v2.NewInMemoryCheckpointer(), requestTimeout: 4 * time.Second}
			resuming := m.resuming
			var claim *disconnectClaimCheckpoint
			var settleFailure *interruptedSettleFailure
			if backend == "redis" {
				rd := goredis.NewClient(&goredis.Options{Addr: miniredis.RunT(t).Addr()})
				t.Cleanup(func() { _ = rd.Close() })
				app.cp = agentRedis.NewCheckpointer(rd, agentRedis.WithCheckpointerPrefix("crew-plan-web"))
				app.sessions = &redisSessions{client: rd, ttl: time.Minute}
				app.durable = true
			} else if backend != "memory" {
				claim = &disconnectClaimCheckpoint{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer(),
					entered: make(chan struct{}), commit: backend == "after-claim", failRead: backend == "recovery-read-failure"}
				app.cp, resuming, m.resuming = claim, claim.entered, nil
				if claim.failRead {
					settleFailure = &interruptedSettleFailure{sessionStore: app.sessions}
					app.sessions = settleFailure
				}
			}

			owner, mux := rand.Text(), app.routes()
			request := func(ctx context.Context, method, path string) *httptest.ResponseRecorder {
				rr := httptest.NewRecorder()
				req := httptest.NewRequestWithContext(ctx, method, path, nil)
				req.AddCookie(&http.Cookie{Name: ownerCookie, Value: owner})
				mux.ServeHTTP(rr, req)
				return rr
			}
			wait := func(started <-chan struct{}, phase string) {
				t.Helper()
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatalf("等待%s时模型实际调用 %d 次", phase, m.calls.Load())
				}
			}
			initial := make(chan *httptest.ResponseRecorder, 1)
			go func() { initial <- request(ctx, http.MethodGet, "/api/chat?q=start") }()
			wait(m.started, "首次调用")
			rows, err := app.sessions.list(ctx, owner)
			if err != nil || len(rows) != 1 {
				t.Fatalf("会话未创建: %v, %v", rows, err)
			}
			sid := rows[0].ID
			if response := request(ctx, http.MethodPost, "/api/interrupt?session_id="+sid); response.Code != http.StatusOK {
				t.Fatalf("暂停失败: %s", response.Body.String())
			}
			close(m.pause)
			if response := <-initial; !strings.Contains(response.Body.String(), `"status":"interrupted"`) {
				t.Fatalf("未挂起: %s", response.Body.String())
			}
			key := sessionKey{owner: owner, id: sid}
			saved, err := app.sessions.get(ctx, key)
			cp, loadErr := app.cp.LoadNS(ctx, sid, "")
			if err != nil || saved == nil || saved.InterruptID == "" || loadErr != nil || !cp.HasResumableWork() {
				t.Fatalf("未提交初始挂起，session=%+v，cp=%v，err=(%v,%v)", saved, cp != nil, err, loadErr)
			}

			resumeURL := "/api/chat?session_id=" + sid
			if claim != nil && claim.failRead {
				// 只让服务端执行 ctx 超时，SSE 客户端仍在线。
				app.requestTimeout = 300 * time.Millisecond
			}
			resumeCtx, disconnect := context.WithCancel(ctx)
			defer disconnect()
			resumed := make(chan *httptest.ResponseRecorder, 1)
			go func() { resumed <- request(resumeCtx, http.MethodGet, resumeURL) }()
			wait(resuming, "续跑进入指定阶段")
			if claim == nil || !claim.failRead {
				disconnect()
			}
			firstResume := <-resumed

			after, err := app.sessions.get(ctx, key)
			afterCP, loadErr := app.cp.LoadNS(ctx, sid, "")
			if claim != nil {
				if err != nil || loadErr != nil || after == nil || !afterCP.HasResumableWork() || m.calls.Load() != 1 {
					t.Fatalf("准入中断不应进入模型或移除快照，session=%+v，cp=%v，err=(%v,%v)，模型调用=%d",
						after, afterCP != nil, err, loadErr, m.calls.Load())
				}
				switch {
				case claim.commit:
					if after.Status != "resume_unconfirmed" || afterCP.Status != compose_v2.CPStatusResuming || after.InterruptID != "" {
						t.Fatalf("已取得准入的存档应封存，session=%+v，cp=%v", after, afterCP.Status)
					}
					got := request(ctx, http.MethodGet, resumeURL+"&text=old")
					if !strings.Contains(got.Body.String(), "resume_unconfirmed") || m.calls.Load() != 1 {
						t.Fatalf("未确认的执行被重跑: %s", got.Body.String())
					}
					got = request(ctx, http.MethodGet, resumeURL+"&action=abort")
					afterCP, loadErr = app.cp.LoadNS(ctx, sid, "")
					if !strings.Contains(got.Body.String(), `"status":"cancelled"`) || loadErr != nil || afterCP != nil {
						t.Fatalf("明确放弃未清档: %s, %v", got.Body.String(), loadErr)
					}
					return
				case claim.failRead:
					if after.Status != "resume_unconfirmed" || after.InterruptID != "" ||
						!strings.Contains(firstResume.Body.String(), `"type":"done"`) ||
						!strings.Contains(firstResume.Body.String(), `"status":"resume_unconfirmed"`) {
						t.Fatalf("读取暂不可用应结算可核对的会话: %+v，SSE=%s", after, firstResume.Body.String())
					}
					settleFailure.fail.Store(true)
					failed := request(ctx, http.MethodGet, resumeURL+"&text=old")
					if !strings.Contains(failed.Body.String(), "session state unavailable") || settleFailure.fail.Load() || m.calls.Load() != 1 {
						t.Fatalf("模拟核对提交失败未触发: %s", failed.Body.String())
					}
					verified := request(ctx, http.MethodGet, resumeURL+"&text=old")
					if !strings.Contains(verified.Body.String(), `"status":"interrupted"`) ||
						!strings.Contains(verified.Body.String(), saved.InterruptID) || m.calls.Load() != 1 {
						t.Fatalf("第一次重试应核实可续档: %s", verified.Body.String())
					}
				default:
					if after.Status != statusInterrupted || after.InterruptID != saved.InterruptID || afterCP.Epoch != cp.Epoch {
						t.Fatalf("准入前取消应保留原档，session=%+v，epoch=%q", after, afterCP.Epoch)
					}
				}
				if retry := request(ctx, http.MethodGet, resumeURL); !strings.Contains(retry.Body.String(), `"status":"success"`) {
					t.Fatalf("确认存档后无法续跑: %s", retry.Body.String())
				}
				return
			}
			retry := request(ctx, http.MethodGet, resumeURL)
			// 已经过了原提问，停在嵌套工具中，剩余工作以新存档为准。
			if err != nil || loadErr != nil || after == nil || after.Status != statusInterrupted ||
				after.InterruptID != "" || !afterCP.HasResumableWork() || len(afterCP.PendingInterrupts) != 0 ||
				retry.Code != http.StatusOK || !strings.Contains(retry.Body.String(), `"status":"success"`) {
				t.Fatalf("断线后 session=%+v, cp=%t, 重试 HTTP=%d, err=(%v,%v): %s",
					after, afterCP != nil, retry.Code, err, loadErr, retry.Body.String())
			}
		})
	}
}
