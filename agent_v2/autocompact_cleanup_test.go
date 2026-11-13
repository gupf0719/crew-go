package agent_v2_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"crew-go/agent_v2"
	"crew-go/schema"
)

func TestBasicAgent_CleanupSessionIsolatesBreakers(t *testing.T) {
	var calls int
	summary := &fnModel{fn: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
		if countUserContent(msgs, "old question") == 0 {
			t.Error("摘要模型未收到历史消息")
		}
		calls++
		return nil, errors.New("summary provider unavailable")
	}}
	cfg := compactionTestConfig(summary)
	cfg.MaxConsecutiveFailures = 1
	// 两个 Agent 都从已装配的配置模板读取压缩选项。
	template := applyWithAutoCompact(cfg)
	newAgent := func() *agent_v2.BasicAgent {
		return agent_v2.NewBasicAgent(
			func(c *agent_v2.Config) { c.AutoCompact = template },
			agent_v2.WithName("compact"), agent_v2.WithModel(&compactLoopModel{}))
	}
	first, second := newAgent(), newAgent()
	task, otherTask := compactionTestSession(), compactionTestSession()
	run := func(ag *agent_v2.BasicAgent, sess *agent_v2.RunSession, wantCalls int) {
		t.Helper()
		events, err := ag.Run(t.Context(), sess, schema.UserMessage("new question"))
		if err != nil || agent_v2.FinalMessage(events) == nil || calls != wantCalls {
			t.Fatalf("摘要调用 %d 次，预期 %d 次，主模型结果 %v，err=%v", calls, wantCalls, agent_v2.FinalMessage(events), err)
		}
	}
	run(first, task, 1)
	run(second, task, 2)
	run(first, otherTask, 3)
	run(first, task, 3)

	first.CleanupSession(task.RunID)
	first.CleanupSession(task.RunID)
	run(first, task, 4)
	run(second, task, 4)
	run(first, otherTask, 4)
}

func TestBasicAgent_CleanupSessionDrainsHalfOpenProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		probeStarted := make(chan struct{})
		finishProbe := make(chan struct{})
		var probeCompleted atomic.Bool
		summary := &fnModel{fn: func(ctx context.Context, msgs []*schema.Message) (*schema.Message, error) {
			if countUserContent(msgs, "old question") == 0 {
				t.Error("摘要模型未收到历史消息")
			}
			switch calls.Add(1) {
			case 1:
				return nil, errors.New("summary provider unavailable")
			case 2:
				close(probeStarted)
				select {
				case <-finishProbe:
					probeCompleted.Store(true)
					return nil, errors.New("half-open probe failed")
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			default:
				return schema.AssistantMessage("<summary>history compressed</summary>"), nil
			}
		}}
		main := &fnModel{fn: func(_ context.Context, msgs []*schema.Message) (*schema.Message, error) {
			switch {
			case countUserContent(msgs, "history compressed") == 1:
				return schema.AssistantMessage("compressed"), nil
			case countUserContent(msgs, "old question") > 0:
				return schema.AssistantMessage("original"), nil
			default:
				return nil, errors.New("missing history in main model input")
			}
		}}
		cfg := compactionTestConfig(summary)
		cfg.MaxConsecutiveFailures = 1
		ag := agent_v2.NewBasicAgent(agent_v2.WithName("compact"), agent_v2.WithModel(main), agent_v2.WithAutoCompact(cfg))
		sess := compactionTestSession()
		run := func(ctx context.Context, lane *agent_v2.RunSession) (string, error) {
			events, err := ag.Run(ctx, lane, schema.UserMessage("new question"))
			if final := agent_v2.FinalMessage(events); final != nil {
				return final.Content, err
			}
			return "", err
		}
		type result struct {
			output string
			err    error
		}
		start := func(ctx context.Context) <-chan result {
			done := make(chan result, 1)
			lane := sess.ForkLane()
			go func() {
				output, err := run(ctx, lane)
				done <- result{output, err}
			}()
			return done
		}
		assertWaiting := func(done <-chan result) {
			t.Helper()
			select {
			case got := <-done:
				t.Fatalf("探测尚未结算时，同任务摘要应继续等待，实际 %+v", got)
			default:
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("探测在途时只应有两次摘要调用，实际 %d 次", got)
			}
		}
		if got, err := run(t.Context(), sess); err != nil || got != "original" || calls.Load() != 1 {
			t.Fatalf("首次失败应触发冷却，主模型输出 %q，摘要调用 %d 次，err=%v", got, calls.Load(), err)
		}
		time.Sleep(5 * time.Minute)
		// 提前退出时取消等待中的摘要。
		probeCtx, cancel := context.WithCancel(t.Context())
		defer cancel()
		probe := start(probeCtx)
		synctest.Wait()
		select {
		case <-probeStarted:
		default:
			t.Fatal("冷却后应开始半开探测")
		}
		// 探测超过名额过期时间，下一次调用仍须通过同一调用槽。
		time.Sleep(5 * time.Minute)
		beforeCleanup := start(probeCtx)
		synctest.Wait()
		assertWaiting(beforeCleanup)
		ag.CleanupSession(sess.RunID)
		afterCleanup := start(probeCtx)
		synctest.Wait()
		assertWaiting(afterCleanup)

		waiterCtx, cancelWaiter := context.WithCancel(probeCtx)
		defer cancelWaiter()
		cancelled := start(waiterCtx)
		synctest.Wait()
		assertWaiting(cancelled)
		cancelWaiter()
		if got := <-cancelled; !errors.Is(got.err, context.Canceled) {
			t.Fatalf("排队调用应响应取消，实际 %+v", got)
		}

		close(finishProbe)
		for _, done := range []<-chan result{probe, beforeCleanup, afterCleanup} {
			if got := <-done; got.err != nil || got.output != "original" {
				t.Fatalf("探测失败后已有调用应沿用本次冷却，实际 %+v", got)
			}
		}
		if calls.Load() != 2 || !probeCompleted.Load() {
			t.Fatalf("半开探测应完成，且已有调用沿用冷却，实际 calls=%d probeCompleted=%t", calls.Load(), probeCompleted.Load())
		}
		if got, err := run(t.Context(), sess); err != nil || got != "compressed" || calls.Load() != 3 {
			t.Fatalf("清理完成后应重新摘要，主模型输出 %q，摘要调用 %d 次，err=%v", got, calls.Load(), err)
		}
	})
}
