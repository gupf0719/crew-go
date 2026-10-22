// background_smoke 用**真 qwen**验证长时间运行工具转入后台（WithBackgroundTasks）端到端，两段：
//
//  1. 同一个 Run 里：测试套件要跑几秒，超过 YieldAfter 转入后台——模型读懂说明、用 background_task 等到结果，
//     再回答哪几个测试失败了；
//  2. 跨 Run：让模型在后台启动构建、不用等。Run 在构建结束之前就结束；构建跑完之后，下一个 Run 里模型从
//     框架送进来的通知读到构建结果、答得上来。
//
// 用法：项目根 .env 配 QWEN_API_KEY，然后 go run ./example/harness/background_smoke
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// callRecorder 记下模型调了哪些工具、转入后台的有几次、模型输入里有没有出现过完成通知。
type callRecorder struct {
	hook.NoOpHooks
	mu      sync.Mutex
	names   []string
	handles int
	notes   int
}

func (r *callRecorder) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, msg := range in.Messages {
		if _, ok := msg.Annotations[agent_v2.BackgroundTaskNotificationAnnotation]; ok {
			r.notes++
			break
		}
	}
	return ctx, nil
}

func (r *callRecorder) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if out != nil && out.Result != nil && strings.Contains(fmt.Sprint(out.Result.Output), "已转入后台") {
		r.handles++
	}
	return ctx, nil
}

func (r *callRecorder) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	r.mu.Lock()
	r.names = append(r.names, in.Name)
	r.mu.Unlock()
	return ctx, nil
}

// take 取走到目前为止记下的，清零重记。
func (r *callRecorder) take() (names []string, handles, notes int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	names, handles, notes = r.names, r.handles, r.notes
	r.names, r.handles, r.notes = nil, 0, 0
	return names, handles, notes
}

func slowTool(name, desc, output string, d time.Duration, runs *atomic.Int32) tool.Tool {
	return tool.NewFuncTool(name, desc, map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			runs.Add(1)
			select {
			case <-time.After(d):
				return tool.Success(output), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
}

func answerOf(events []*agent_v2.AgentEvent) string {
	if final := agent_v2.FinalMessage(events); final != nil {
		return final.Content
	}
	return ""
}

func main() {
	envloader.Load()
	if os.Getenv("QWEN_API_KEY") == "" {
		fmt.Println("QWEN_API_KEY 未设置（.env 加 QWEN_API_KEY=xxx）——本 smoke 需真 qwen")
		os.Exit(1)
	}
	m, tr := llmtrace.NewQwenFallback()
	mgr := agent_v2.NewBackgroundTaskManager(nil)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mgr.Close(ctx)
	}()

	var testRuns, buildRuns atomic.Int32
	tk := tool.NewToolkit()
	tk.Register(slowTool("run_tests", "运行全部单元测试，返回测试报告（要跑几秒）",
		"共 128 个测试：126 个通过，2 个失败——TestRefundRounding、TestPayoutRetry", 4*time.Second, &testRuns))
	tk.Register(slowTool("build", "全量构建发布包，返回构建结果（要跑几秒）",
		"构建 BUILD-7731 成功，产物 app-2.4.0.tar.gz（38MB）", 3*time.Second, &buildRuns))
	rec := &callRecorder{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("devops"),
		agent_v2.WithModel(m),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHistory(agent_v2.NewInMemoryHistoryStore(), "smoke"),
		agent_v2.WithBackgroundTasks(&agent_v2.BackgroundTasksConfig{
			Manager:    mgr,
			Eligible:   func(t tool.Tool) bool { return t.Name() == "run_tests" || t.Name() == "build" },
			YieldAfter: time.Second,
		}),
		agent_v2.WithHooks(hook.NewChainHooks(rec, tr.Hooks())),
		agent_v2.WithMaxRounds(8),
		agent_v2.WithSystemPrompt("你是研发助手。需要数据时调用工具，不要编造结果。"),
	)
	ok := true

	// ── 第 1 段：同一个 Run 里等后台任务的结果 ──
	ctx1 := compose_v2.WithThreadIDCtx(context.Background(), "bg-smoke-tests")
	events, err := ag.Run(ctx1, agent_v2.NewRunSession(),
		schema.UserMessage("调用 run_tests 跑一遍单元测试，等它跑完，然后告诉我哪几个测试失败了。"))
	if err != nil {
		fmt.Println("FAIL 第 1 段 Run:", err)
		os.Exit(1)
	}
	a1 := answerOf(events)
	calls1, handles1, _ := rec.take()
	fmt.Printf("第 1 段调用过的工具：%v（转入后台 %d 次）\n第 1 段答复：%s\n", calls1, handles1, a1)
	if handles1 != 1 {
		fmt.Println("FAIL 第 1 段：run_tests 应转入后台一次")
		ok = false
	}
	waited := false
	for _, n := range calls1 {
		if n == agent_v2.BackgroundTaskToolName {
			waited = true
		}
	}
	if !waited {
		fmt.Println("FAIL 第 1 段：run_tests 转入后台之后模型没有用 background_task 等结果")
		ok = false
	}
	if !strings.Contains(a1, "TestRefundRounding") || !strings.Contains(a1, "TestPayoutRetry") {
		fmt.Println("FAIL 第 1 段：答复里没有两个失败的测试名")
		ok = false
	}
	if testRuns.Load() != 1 {
		fmt.Printf("FAIL 第 1 段：run_tests 执行了 %d 次\n", testRuns.Load())
		ok = false
	}

	// ── 第 2 段：跨 Run 的完成通知 ──
	ctx2 := compose_v2.WithThreadIDCtx(context.Background(), "bg-smoke-build")
	start := time.Now()
	events, err = ag.Run(ctx2, agent_v2.NewRunSession(),
		schema.UserMessage("调用 build 在后台启动一次全量构建。不要等它跑完，启动之后直接告诉我已经开始了就行。"))
	if err != nil {
		fmt.Println("FAIL 第 2 段 Run A:", err)
		os.Exit(1)
	}
	runA := time.Since(start)
	callsA, handlesA, _ := rec.take()
	fmt.Printf("第 2 段 Run A 用时 %s，调用过的工具：%v（转入后台 %d 次）\n第 2 段 Run A 答复：%s\n",
		runA.Round(time.Millisecond), callsA, handlesA, answerOf(events))
	if handlesA != 1 {
		fmt.Println("FAIL 第 2 段：build 应转入后台一次")
		ok = false
	}
	for _, n := range callsA {
		if n == agent_v2.BackgroundTaskToolName {
			fmt.Println("FAIL 第 2 段：Run A 里模型自己等了结果——跨 Run 通知这条路没走到（换个说法重跑）")
			ok = false
		}
	}
	if buildRuns.Load() != 1 {
		fmt.Printf("FAIL 第 2 段：build 执行了 %d 次\n", buildRuns.Load())
		ok = false
	}
	// 等构建在后台跑完（工具本身 3 秒）
	time.Sleep(4 * time.Second)
	events, err = ag.Run(ctx2, agent_v2.NewRunSession(), schema.UserMessage("构建结果怎么样？产物叫什么？"))
	if err != nil {
		fmt.Println("FAIL 第 2 段 Run B:", err)
		os.Exit(1)
	}
	aB := answerOf(events)
	callsB, _, notesB := rec.take()
	fmt.Printf("第 2 段 Run B 调用过的工具：%v（模型输入里带完成通知的调用 %d 次）\n第 2 段 Run B 答复：%s\n", callsB, notesB, aB)
	if notesB == 0 {
		fmt.Println("FAIL 第 2 段：Run B 的模型输入里没有完成通知")
		ok = false
	}
	if !strings.Contains(aB, "BUILD-7731") && !strings.Contains(aB, "app-2.4.0") {
		fmt.Println("FAIL 第 2 段：Run B 的答复里没有构建结果——通知没送到，或模型没读懂")
		ok = false
	}
	if buildRuns.Load() != 1 {
		fmt.Printf("FAIL 第 2 段：build 被重复执行（共 %d 次）\n", buildRuns.Load())
		ok = false
	}
	fmt.Println(tr.Report())
	if !ok {
		os.Exit(1)
	}
	fmt.Println("=== background_smoke PASS（慢工具转入后台 → 同一 Run 里 wait 拿结果；跨 Run 经完成通知拿结果）===")
}
