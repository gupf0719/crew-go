// durability_smoke 真 qwen 端到端验 Durability 经 ctx 进 agent_v2 BasicAgent 的完整流程。
//
// durability 是 compose_v2 级 RunOption，agent_v2 编排没暴露 WithDurability——业务经 ctx
// （WithDurabilityCtx，同 checkpointer 的 WithCheckpointerCtx 一致模式）透传进 BasicAgent
// 内部的 compose_v2.Run。本 smoke 验这条路在**真 LLM 多轮 ReAct**（qwen 调工具 → 多超步）下
// 工作：
//   - exit（基线）：durability=exit → 0 每超步写（成功只 DeleteSubtree）
//   - sync / async：每超步写 cp（SaveNS>0）+ 答案正确 + 成功即清
//   - 崩溃恢复：sync 跑捕获中途 cp → 注入 fresh checkpointer → 同 sid **显式 Resume** →
//     续跑完成 + 答案对 + Resume 比 fresh 少调 LLM（证真续跑非重跑）
//
// 跑法：QWEN_API_KEY 在项目根 .env 或环境变量。`go run ./example/harness/durability_smoke`。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

const secretCode = "ZephyrNine"

// recCp 包 InMemoryCheckpointer 数 SaveNS（证 durability 每超步写）+ 按序捕获每份 cp marshal
// （崩溃恢复注入用）。线程安全——async worker 从后台 goroutine 并发调 SaveNS，必须加锁。
type recCp struct {
	*compose_v2.InMemoryCheckpointer
	mu        sync.Mutex
	saveCount int
	allData   [][]byte
}

func (r *recCp) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	r.mu.Lock()
	r.saveCount++
	if data, err := compose_v2.MarshalCheckpoint(cp); err == nil {
		r.allData = append(r.allData, data)
	}
	r.mu.Unlock()
	return r.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp)
}

func (r *recCp) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.saveCount
}

func (r *recCp) dataCopy() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, len(r.allData))
	copy(out, r.allData)
	return out
}

// modelCounter 数 OnModelStart——崩溃恢复场景比 fresh vs resume 的 LLM 调用次数。
type modelCounter struct {
	hook.NoOpHooks
	mu sync.Mutex
	n  int
}

func (c *modelCounter) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
	return ctx, nil
}

func (c *modelCounter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// newAgent 建一个必须先调工具拿机密代号再作答的真 qwen BasicAgent（强制 ≥2 轮 ReAct）。
// smokeModel / smokeTracer 由 main 起始 llmtrace.NewQwenFallback 初始化，共享给所有场景的
// newAgent——累计跨场景 token + 记录服务模型（fallback 链，flash 优先）。
var (
	smokeModel  model.ChatModel
	smokeTracer *llmtrace.Tracer
)

func newAgent(hooks ...hook.Hooks) agent_v2.Agent {
	lookup := tool.NewFuncTool("lookup_secret", "查询机密代号。无参数，返回当前机密代号。",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success(secretCode), nil
		})
	tk := tool.NewToolkit()
	tk.Register(lookup)
	opts := []agent_v2.Option{
		agent_v2.WithName("secret_reporter"),
		agent_v2.WithSystemPrompt("你必须先调用 lookup_secret 工具拿到机密代号，然后把代号原样完整告诉用户。"),
		agent_v2.WithModel(smokeModel),
		agent_v2.WithToolkit(tk),
		// 追踪服务模型 + token：tracer.Hooks 累计 usage，与业务传入 hooks 串联
		agent_v2.WithHooks(hook.NewChainHooks(append([]hook.Hooks{smokeTracer.Hooks()}, hooks...)...)),
	}
	return agent_v2.NewBasicAgent(opts...)
}

// runScenario 跑一档 durability，返回 (每超步 SaveNS 次数, final 内容, 是否残留 cp, err)。
func runScenario(dur compose_v2.Durability) (int, string, bool, error) {
	ag := newAgent()
	cp := &recCp{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
	sess := agent_v2.NewRunSession()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	if dur != "" {
		ctx = compose_v2.WithDurabilityCtx(ctx, dur)
	}
	events, err := ag.Run(ctx, sess, schema.UserMessage("机密代号是什么？"))
	if err != nil {
		return cp.count(), "", false, err
	}
	content := ""
	if final := agent_v2.FinalMessage(events); final != nil {
		content = final.Content
	}
	residual, _ := cp.InMemoryCheckpointer.ExistsNS(context.Background(), sess.RunID, "")
	return cp.count(), content, residual, nil
}

// crashRecovery 验真 LLM 崩溃恢复：Run 1 捕获中途 cp，注入 fresh checkpointer 模拟"硬崩盘上
// 残留"，同 sid **显式 Resume** 续跑（顶层 Run 恒声明新任务、不捡存档）。
// 返回 (run1 LLM 次数, run2 LLM 次数, run2 答案, err)。
func crashRecovery() (int, int, string, error) {
	mc1 := &modelCounter{}
	ag1 := newAgent(mc1)
	cp := &recCp{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer()}
	sess := agent_v2.NewRunSession()
	sid := sess.RunID

	ctx1, cancel1 := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel1()
	ctx1 = compose_v2.WithCheckpointerCtx(ctx1, cp)
	ctx1 = compose_v2.WithDurabilityCtx(ctx1, compose_v2.DurabilitySync)
	if _, err := ag1.Run(ctx1, sess, schema.UserMessage("机密代号是什么？")); err != nil {
		return 0, 0, "", fmt.Errorf("run1: %w", err)
	}

	// 取一份中途 cp（模拟硬崩时盘上的状态，defer 没跑、成功 cleanup 也没清）。
	all := cp.dataCopy()
	if len(all) < 2 {
		return mc1.count(), 0, "", fmt.Errorf("捕获 cp 不足以演示续跑（%d 份）", len(all))
	}
	mid := all[len(all)/2] // 中途一份 → Resume 应只续跑剩余 round
	midCp, err := compose_v2.UnmarshalCheckpoint(mid)
	if err != nil {
		return mc1.count(), 0, "", fmt.Errorf("unmarshal mid cp: %w", err)
	}
	// 注入 fresh checkpointer（模拟崩溃重启后盘上只有这份残留 cp）。
	fresh := compose_v2.NewInMemoryCheckpointer()
	if err := fresh.SaveNS(context.Background(), sid, "", midCp); err != nil {
		return mc1.count(), 0, "", fmt.Errorf("inject mid cp: %w", err)
	}

	// Run 2：同 sid + 注入的 checkpointer → **走 Resume 续跑**。
	//
	// ⚠️ 这里曾经写的是 `ag2.Run(...)`，靠「同 sid 顶层 Run 会 auto-load 那份 Pending cp」。
	// 那个假设自 agent_v2 的新任务判定（顶层 Run 恒声明新任务）之后就不成立了——顶层 Run
	// 不再捡任何存档，于是整图从头跑、`Run2 == Run1`，本场景的断言（续跑该少调 LLM）
	// 从此恒红。**红的是示例不是引擎**：换成 Resume 后实测 LLM=1、节点序列
	// `[model postamble]`，崩溃恢复本身一直是对的。
	//
	// 崩溃恢复的正确姿势就是显式 Resume：进程重启后业务手里有的是 sid，
	// 「这个 sid 上还有没有没跑完的活」由框架按存档内容判（`HasResumableWork`）。
	mc2 := &modelCounter{}
	ag2 := newAgent(mc2)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel2()
	ctx2 = compose_v2.WithCheckpointerCtx(ctx2, fresh)
	ctx2 = compose_v2.WithDurabilityCtx(ctx2, compose_v2.DurabilitySync)
	events2, err := ag2.Resume(ctx2, sid)
	if err != nil {
		return mc1.count(), mc2.count(), "", fmt.Errorf("run2 resume: %w", err)
	}
	content := ""
	if final := agent_v2.FinalMessage(events2); final != nil {
		content = final.Content
	}
	return mc1.count(), mc2.count(), content, nil
}

func main() {
	envloader.Load()
	if os.Getenv("QWEN_API_KEY") == "" {
		log.Fatal("需要 QWEN_API_KEY（项目根 .env 或环境变量）")
	}
	smokeModel, smokeTracer = llmtrace.NewQwenFallback()

	pass := true
	check := func(cond bool, name, detail string) {
		status := "✓ PASS"
		if !cond {
			status, pass = "✗ FAIL", false
		}
		fmt.Printf("  %s %s%s\n", status, name, detail)
	}
	hasSecret := func(s string) bool { return strings.Contains(s, secretCode) }

	// 场景 exit（基线）：durability=exit 应 0 每超步写。
	fmt.Println("=== 场景 exit（基线）：每超步不写 ===")
	n, content, _, err := runScenario(compose_v2.DurabilityExit)
	if err != nil {
		log.Fatalf("exit run: %v", err)
	}
	fmt.Printf("  SaveNS=%d final=%q\n", n, content)
	check(n == 0, "exit 不每超步写", fmt.Sprintf("（实际 %d）", n))
	check(hasSecret(content), "真 LLM 调工具拿到代号", "")

	// 场景 sync：每超步同步写 + 成功即清。
	fmt.Println("=== 场景 sync：每超步同步写 + 成功即清 ===")
	n, content, residual, err := runScenario(compose_v2.DurabilitySync)
	if err != nil {
		log.Fatalf("sync run: %v", err)
	}
	fmt.Printf("  SaveNS=%d final=%q residual=%v\n", n, content, residual)
	check(n > 0, "sync 每超步写 cp（>0）", fmt.Sprintf("（实际 %d）", n))
	check(hasSecret(content), "真 LLM 调工具拿到代号", "")
	check(!residual, "成功即清（无残留 cp）", "")

	// 场景 async：单 worker 后台每超步写 + 成功即清。
	fmt.Println("=== 场景 async：后台每超步写 + 成功即清 ===")
	n, content, residual, err = runScenario(compose_v2.DurabilityAsync)
	if err != nil {
		log.Fatalf("async run: %v", err)
	}
	fmt.Printf("  SaveNS=%d final=%q residual=%v\n", n, content, residual)
	check(n > 0, "async 每超步写 cp（>0）", fmt.Sprintf("（实际 %d）", n))
	check(hasSecret(content), "真 LLM 调工具拿到代号", "")
	check(!residual, "成功即清（无残留 cp）", "")

	// 场景 崩溃恢复：sync 捕获中途 cp → 注入 → 同 sid 显式 Resume 续跑。
	fmt.Println("=== 场景 崩溃恢复：从中途 cp 显式 Resume 续跑 ===")
	run1Calls, run2Calls, rcContent, err := crashRecovery()
	if err != nil {
		log.Fatalf("crash recovery: %v", err)
	}
	fmt.Printf("  Run1 LLM=%d Run2(resume) LLM=%d final=%q\n", run1Calls, run2Calls, rcContent)
	check(hasSecret(rcContent), "Resume 续跑完成且答案正确", "")
	check(run2Calls < run1Calls, "Resume 比 fresh 少调 LLM（证真续跑非重跑）",
		fmt.Sprintf("（Run1=%d Run2=%d）", run1Calls, run2Calls))

	fmt.Println("\n" + smokeTracer.Report())
	if !pass {
		log.Fatal("\n✗ 有场景 FAIL")
	}
	fmt.Println("\n=== 全部 PASS：durability 经 ctx 进真 qwen BasicAgent 端到端正确（含崩溃恢复）===")
}
