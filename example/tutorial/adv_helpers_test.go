//go:build integration

// Package example_test 展示 crew-go SDK 进阶用法（v2 channel-based）：
//   - 9 种编排模式（贴近真实业务场景，覆盖更多 API 参数）
//   - Hooks 可观测性（timingHook / auditHook + ChainHooks）
//
// 运行（必须带 integration build tag，否则文件不会被编译）：
//
//	# 默认 qwen (跟 CLAUDE.md "测试用 LLM" 默认对齐, dashscope 兼容 OpenAI):
//	QWEN_API_KEY=<your-key> go test -tags=integration ./example/... -v -run TestAdv -timeout 600s
//
//	# 备选 DeepSeek (CLAUDE.md 仍可备选, observability_demo 等 -provider deepseek|qwen 两路兼容):
//	DEEPSEEK_API_KEY=<your-key> go test -tags=integration ./example/... -v -run TestAdv -timeout 600s
package tutorial_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/example/envloader"
	"crew-go/fallback"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/model/deepseek"
	"crew-go/model/qwen"
	"crew-go/schema"
)

// TestMain 在所有测试运行前自动加载项目根目录的 .env 文件。
func TestMain(m *testing.M) {
	envloader.Load()
	os.Exit(m.Run())
}

// requireAPIKey 检查测试 LLM API key 是否可用 — **默认走 qwen** (CLAUDE.md "测试用 LLM"
// 对齐, dashscope 兼容 OpenAI), **备选 DeepSeek** (observability_demo 等 -provider
// deepseek|qwen 两路保留). 返 provider 字符串供 `newLLM(provider)` 路由.
//
// 优先级: QWEN_API_KEY > DEEPSEEK_API_KEY; 都缺失时 t.Skip 整测试.
//
// R33 audit H-2 fix: 上版只读 DEEPSEEK_API_KEY 跟 CLAUDE.md / Makefile / testing-conventions
// "默认 qwen" 协议 drift, R33 改齐.
func requireAPIKey(t *testing.T) (provider string) {
	t.Helper()
	if os.Getenv("QWEN_API_KEY") != "" {
		return "qwen"
	}
	if os.Getenv("DEEPSEEK_API_KEY") != "" {
		return "deepseek"
	}
	t.Skip("QWEN_API_KEY / DEEPSEEK_API_KEY 都未设置，跳过真实调用测试")
	return ""
}

// ctx60s 返回 60 秒超时的 context。
func ctx60s(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// ctx300s 返回 300 秒超时的 context，适合多轮编排。
func ctx300s(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// newLLM 按 provider 创建 ChatModel — qwen (默认) 或 deepseek (备选,
// deepseek-v4-flash). qwen.NewTestClient 内部读 QWEN_API_KEY 环境变量,
// deepseek.NewClient 需显式传 apiKey.
//
// **qwen 走 fallback 链而非单个模型**：`NewTestClient("")` 的默认模型
// (qwen3.6-flash) 免费额度已耗尽 —— 整套 integration 测试曾因此全部 403 不可用。
// 用 `qwen.TestModelCandidates` 串成 fallback（它认得 dashscope 403
// AllocationQuota 并自动切下一个），单个模型额度用完不再拖垮全套。
func newLLM(provider string) model.ChatModel {
	switch provider {
	case "deepseek":
		return deepseek.NewClient(os.Getenv("DEEPSEEK_API_KEY"), "")
	default: // "qwen" 或空 — 默认走 qwen (跟 CLAUDE.md 对齐)
		models := make([]model.ChatModel, 0, len(qwen.TestModelCandidates))
		for _, name := range qwen.TestModelCandidates {
			models = append(models, qwen.NewTestClient(name))
		}
		return fallback.New(models)
	}
}

// newBasicAgentV2 快速创建只带 system prompt 的 v2 BasicAgent。
func newBasicAgentV2(provider, name, system string) agent_v2.Agent {
	return agent_v2.NewBasicAgent(
		agent_v2.WithName(name),
		agent_v2.WithSystemPrompt(system),
		agent_v2.WithModel(newLLM(provider)),
	)
}

// mustFinalMessageV2 从 v2 事件列表提取最终消息，失败则 t.Fatal。
func mustFinalMessageV2(t *testing.T, events []*agent_v2.AgentEvent) *schema.Message {
	t.Helper()
	out := agent_v2.FinalMessage(events)
	if out == nil {
		t.Fatal("FinalMessage 返回 nil")
	}
	return out
}

// minInt 返回两个整数中的较小值。
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ──────────────────────────────────────────────────────────────────
// timingHook / auditHook：Hook 是"框架回调"模式。
// 用户定义回调方法，框架在 LLM 调用、工具调用等生命周期节点自动触发。
// TestAdv_01 和 TestAdv_10 共用这两个 Hook 类型。
// ──────────────────────────────────────────────────────────────────

// timingHook 嵌入 hook.NoOpHooks，记录模型调用次数 / 工具调用次数 / 总 Token 消耗。
type timingHook struct {
	hook.NoOpHooks
	t              *testing.T
	modelCalls     int32
	toolStarts     int32
	totalInputTok  int32
	totalOutputTok int32
	mu             sync.Mutex
	startTimes     map[int32]time.Time // round → 开始时刻
}

func newTimingHook(t *testing.T) *timingHook {
	return &timingHook{t: t, startTimes: make(map[int32]time.Time)}
}

func (h *timingHook) OnModelStart(ctx context.Context, info *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	h.mu.Lock()
	h.startTimes[info.Round] = time.Now()
	h.mu.Unlock()
	return ctx, nil
}

func (h *timingHook) OnModelEnd(ctx context.Context, info *hook.AgentInfo, output *schema.ModelOutput) (context.Context, error) {
	h.mu.Lock()
	elapsed := time.Since(h.startTimes[info.Round])
	h.mu.Unlock()
	atomic.AddInt32(&h.modelCalls, 1)
	if output != nil && output.Usage != nil {
		atomic.AddInt32(&h.totalInputTok, output.Usage.InputTokens)
		atomic.AddInt32(&h.totalOutputTok, output.Usage.OutputTokens)
	}
	h.t.Logf("[hook] 第 %d 轮 LLM 调用耗时 %s", info.Round, elapsed.Round(time.Millisecond))
	return ctx, nil
}

func (h *timingHook) OnToolStart(ctx context.Context, _ *hook.AgentInfo, input *schema.ToolInput) (context.Context, error) {
	atomic.AddInt32(&h.toolStarts, 1)
	h.t.Logf("[hook] 工具调用开始: %s", input.Name)
	return ctx, nil
}

// auditHook 记录工具调用结果，用于操作审计。
type auditHook struct {
	hook.NoOpHooks
	mu      sync.Mutex
	entries []string
}

func (h *auditHook) OnToolEnd(ctx context.Context, info *hook.AgentInfo, output *schema.ToolOutput) (context.Context, error) {
	if output == nil || output.Result == nil {
		return ctx, nil
	}
	entry := fmt.Sprintf("[%s] → %s", info.Name, output.Result.Output)
	h.mu.Lock()
	h.entries = append(h.entries, entry)
	h.mu.Unlock()
	return ctx, nil
}

func (h *auditHook) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]string, len(h.entries))
	copy(cp, h.entries)
	return cp
}
