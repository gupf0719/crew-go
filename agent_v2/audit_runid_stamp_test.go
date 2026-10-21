package agent_v2_test

import (
	"context"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/hook"
	"crew-go/model"
	specModel "crew-go/modules/model"
	"crew-go/schema"
)

// 回归测试：commit aa8cea9 两处 fix（generateWithRunID clone-before-stamp + streamOrGenerate
// 成功路径 nil ctx 守卫）。两个 fix 目标都是"业务实现违约/共享指针"这类潜伏路径，现有测试
// （每 branch 独立 model + 每次新 message / hook 都返有效 ctx）盖不到——revert 不 fail = 无
// 覆盖。本文件补上，sanity-revert 实测：revert 任一 fix 对应测试 fail 并指向 bug。

// sharedPtrModel 每次 Generate 返回同一个 *schema.Message 指针（模拟业务缓存/去重 model
// wrapper），Stream 返 model.ErrStreamUnsupported 强制走 Generate。
type sharedPtrModel struct{ msg *schema.Message }

func (m *sharedPtrModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return m.msg, nil // 同一指针
}
func (m *sharedPtrModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported // 强制降级 Generate
}
func (m *sharedPtrModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*sharedPtrModel)(nil)

// runIDCapture 记录每次 OnModelEnd 输出的 run_id。
type runIDCapture struct {
	hook.NoOpHooks
	mu     sync.Mutex
	runIDs map[string]bool
}

func (c *runIDCapture) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if out != nil && out.Message != nil {
		if rid, ok := out.Message.Annotations["run_id"].(string); ok && rid != "" {
			c.runIDs[rid] = true
		}
	}
	return ctx, nil
}

// TestAudit_GenerateFallback_SharedPtrModel 复现 clone fix 目标路径：单个 model 实例被
// ParallelAgent 两 branch 共享，并发 Generate 返回同一 *Message。就地 mutate（revert 版本）
// 让两 goroutine 同时写 out.Annotations → `-race` 报 concurrent map write / DATA RACE；
// clone-before-stamp 后各分支写自己 copy → 安全 + 两 branch run_id 独立。
// **必须 `go test -race` 跑才稳定捕获 revert**（就地写是数据竞争，非功能性断言）。
func TestAudit_GenerateFallback_SharedPtrModel(t *testing.T) {
	shared := &sharedPtrModel{msg: schema.AssistantMessage("shared")} // Annotations 初始 nil
	rc := &runIDCapture{runIDs: map[string]bool{}}
	mk := func(name string) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName(name),
			agent_v2.WithModel(shared), // 两 branch 共用同一实例 → 并发 Generate 同一 *msg
			agent_v2.WithHooks(rc),
		)
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("a", mk("A")).
		AddBranch("b", mk("B"))
	if _, err := par.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rc.mu.Lock()
	n := len(rc.runIDs)
	rc.mu.Unlock()
	if n < 2 {
		t.Errorf("两并发分支应各自独立 run_id（clone-before-stamp），got %d——为 1 说明就地 mutate 让别名共享覆盖", n)
	}
}

// nilCtxStreamHook 的 OnModelEndStream 违约返 nil ctx（但返有效 reader + 无 err）——触发
// streamOrGenerate 成功路径的 nil ctx。
type nilCtxStreamHook struct{ hook.NoOpHooks }

func (nilCtxStreamHook) OnModelEndStream(_ context.Context, _ *hook.AgentInfo, sr specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error) {
	return nil, sr, nil // nil ctx + 原样有效 reader + 无 err
}

// TestAudit_OnModelEndStream_NilCtxHook 验证业务 hook 违约返 nil ctx 时 streamOrGenerate 不
// deref：`if nctx1 != nil` 守卫保 pre-hook ctx。revert 回无条件 `ctx = nctx1` 后，下游
// GetEventSink(nil ctx) nil deref（被框架 recover 转 err）→ 本测试 err 断言 fail。
func TestAudit_OnModelEndStream_NilCtxHook(t *testing.T) {
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("s"),
		agent_v2.WithModel(&streamingModel{chunks: []string{"hi"}}), // 真流触发 OnModelEndStream
		agent_v2.WithHooks(nilCtxStreamHook{}),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("hook 返 nil ctx 时 Run 应正常（守卫保 pre-hook ctx），got err: %v", err)
	}
}
