// a2a_orch_llm_smoke 用**真 qwen**补齐 A2A × 编排矩阵的剩余 6 格：Parallel / Loop /
// Routing / Reflect / Handoff / GraphAgent 各自嵌入 A2A RemoteAgent（agent 形态或 tool
// 形态），远程侧是真 qwen BasicAgent + 确定性 lookup_code 工具（"SECRET-7741"）。
// （Basic host / Sequential+HITL / Plan 四层 已由 a2a_llm_smoke / a2a_plan_smoke 覆盖。）
//
// 断言全用确定性锚（不赌 LLM 措辞）：远程 lookup_code 调用次数 delta（atomic——Parallel
// 两分支并发打远程，hook 计数跨请求并发）+ 终答含 "7741" + Routing 负分支零调用。
//
// 跑（项目根 .env 需 QWEN_API_KEY；6 部 ~20 次 qwen 调用，约 2-4 分钟）：
//
//	go run ./example/a2a/orch_llm_smoke
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"crew-go/a2a"
	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// atomicCounter 数某工具调用次数（atomic：served agent 会被并发请求并发跑，
// 两次 Run 的 OnToolStart 并发触发）。
type atomicCounter struct {
	hook.NoOpHooks
	name  string
	calls atomic.Int64
}

func (c *atomicCounter) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	if in.Name == c.name {
		c.calls.Add(1)
	}
	return ctx, nil
}

var (
	baseURL      string
	remoteCalls  *atomicCounter // 远程侧 lookup_code 计数
	weatherCalls *atomicCounter // Routing 负分支计数
)

// llm / llmTracer 是本 smoke 全部 agent 共用的 qwen 备选链（候选取 qwen.TestModelCandidates，
// 额度耗尽自动切）。在 main 里构造——NewTestClient 在构造期读 QWEN_API_KEY，包级 init 时 .env 还没加载。
var (
	llm       model.ChatModel
	llmTracer *llmtrace.Tracer
)

func main() {
	envloader.Load()
	if os.Getenv("QWEN_API_KEY") == "" {
		log.Fatal("QWEN_API_KEY 未设置（.env 加 QWEN_API_KEY=xxx）——本 smoke 需真 qwen")
	}
	llm, llmTracer = llmtrace.NewQwenFallback()
	ctx := context.Background()

	// ── 共享远程：真 qwen BasicAgent + 确定性 lookup_code，暴露成 A2A server ──
	lookupCode := tool.NewFuncTool("lookup_code", "查询内部代号。无参数，返回当前内部代号。",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success("SECRET-7741"), nil
		})
	remoteCalls = &atomicCounter{name: "lookup_code"}
	rtk := tool.NewToolkit()
	rtk.Register(lookupCode)
	remoteAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("code_service"),
		agent_v2.WithSystemPrompt("你是代号查询服务。收到任何请求都必须先调用 lookup_code 工具，然后把工具返回的代号原样完整地告诉对方（不要省略、不要改写）。"),
		agent_v2.WithModel(llm),
		agent_v2.WithToolkit(rtk),
		agent_v2.WithHooks(remoteCalls),
	)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	baseURL = "http://" + ln.Addr().String()
	go func() {
		_ = http.Serve(ln, server.Handler(remoteAgent, baseURL, server.WithOwnerResolver(func(context.Context) string { return "demo" })))
	}()
	defer func() { _ = ln.Close() }()
	fmt.Printf("✓ 远程 code_service（真 qwen + lookup_code）已暴露 %s\n\n", baseURL)

	partParallel(ctx)
	partLoop(ctx)
	partRouting(ctx)
	partReflect(ctx)
	partHandoff(ctx)
	partGraph(ctx)

	fmt.Println("\n" + llmTracer.Report())
	fmt.Println("\n=== A2A × 编排矩阵冒烟全 PASS（Parallel / Loop / Routing / Reflect / Handoff / GraphAgent × A2A × 真 qwen）===")
}

// newRemote 建一个 RemoteAgent 实例（agent 形态）。Parallel 各分支必须独立实例
// （CLAUDE.md：同实例多并发分支 taskKey 互覆约束）。
func newRemote(ctx context.Context, name string) *a2a.RemoteAgent {
	ra, err := a2a.NewRemoteAgent(ctx, baseURL,
		a2a.WithName(name),
		a2a.WithDescription("查询内部代号：把请求委派给远程代号服务，返回其答复。"))
	if err != nil {
		log.Fatalf("NewRemoteAgent(%s): %v", name, err)
	}
	return ra
}

// newWorker 建一个带 A2A tool 的本地 qwen worker（tool 形态嵌入用）。
func newWorker(ctx context.Context, workerName, remoteName string) agent_v2.Agent {
	ra := newRemote(ctx, remoteName)
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(ra))
	return agent_v2.NewBasicAgent(
		agent_v2.WithName(workerName),
		agent_v2.WithSystemPrompt(fmt.Sprintf("你负责查内部代号。必须调用 %s 工具完成查询（input 传查询请求），拿到答复后把其中的代号原样完整报告，不要改写。", remoteName)),
		agent_v2.WithModel(llm),
		agent_v2.WithToolkit(tk),
	)
}

func runCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 180*time.Second)
}

func mustFinal(events []*agent_v2.AgentEvent, label string) *schema.Message {
	final := agent_v2.FinalMessage(events)
	if final == nil || strings.TrimSpace(final.Content) == "" {
		log.Fatalf("✗ FAIL [%s]：无终答", label)
	}
	return final
}

func assertAnchor(label string, final *schema.Message, deltaWant int64, delta int64) {
	if delta < deltaWant {
		log.Fatalf("✗ FAIL [%s]：远程 lookup_code 调用 delta=%d（期望 ≥%d）——A2A 链路没真打到远程", label, delta, deltaWant)
	}
	if !strings.Contains(final.Content, "7741") {
		log.Fatalf("✗ FAIL [%s]：终答未含确定性代号 7741，got %q", label, final.Content)
	}
	fmt.Printf("✓ %s PASS（lookup_code delta=%d，代号传回）\n\n", label, delta)
}

// ── 1. ParallelAgent：两分支各嵌一个独立 RemoteAgent 实例（agent 形态），并发打远程 ──
func partParallel(ctx context.Context) {
	fmt.Println("───── Part 1: ParallelAgent × A2A（两分支独立实例并发）─────")
	before := remoteCalls.calls.Load()
	pa := agent_v2.NewParallelAgent(
		agent_v2.WithParallelName("code_fanout"),
		agent_v2.WithAggregator(func(_ context.Context, results map[string]string) (*schema.Message, error) {
			return schema.AssistantMessage(fmt.Sprintf("A=%s | B=%s", results["branch_a"], results["branch_b"])), nil
		}),
	).
		AddBranch("branch_a", newRemote(ctx, "code_agent_a")).
		AddBranch("branch_b", newRemote(ctx, "code_agent_b"))
	rc, cancel := runCtx(ctx)
	defer cancel()
	events, err := pa.Run(rc, agent_v2.NewRunSession(), schema.UserMessage("查一下内部代号，原样返回"))
	if err != nil {
		log.Fatalf("Parallel.Run: %v", err)
	}
	final := mustFinal(events, "Parallel")
	fmt.Printf("parallel> %.120s\n", final.Content)
	assertAnchor("Parallel×A2A", final, 2, remoteCalls.calls.Load()-before)
}

// ── 2. LoopAgent：body 是带 A2A tool 的 worker，强制跑 2 轮（每轮都打远程）──
func partLoop(ctx context.Context) {
	fmt.Println("───── Part 2: LoopAgent × A2A（body 每轮经 A2A tool 打远程）─────")
	before := remoteCalls.calls.Load()
	la := agent_v2.NewLoopAgent(
		agent_v2.WithLoopName("code_loop"),
		agent_v2.WithLoopMaxIterations(3),
		agent_v2.WithLoopExitCondition(func(_ context.Context, output *schema.Message, round int) (bool, error) {
			return round >= 2 && output != nil && strings.Contains(output.Content, "7741"), nil
		}),
	).WithBody(newWorker(ctx, "loop_worker", "code_agent"))
	rc, cancel := runCtx(ctx)
	defer cancel()
	events, err := la.Run(rc, agent_v2.NewRunSession(), schema.UserMessage("查一下内部代号，原样返回"))
	if err != nil {
		log.Fatalf("Loop.Run: %v", err)
	}
	final := mustFinal(events, "Loop")
	fmt.Printf("loop> %.120s\n", final.Content)
	assertAnchor("Loop×A2A", final, 2, remoteCalls.calls.Load()-before)
}

// ── 3. RoutingAgent：真 qwen classifier 在 A2A code 分支 vs 本地 weather 分支间路由 ──
func partRouting(ctx context.Context) {
	fmt.Println("───── Part 3: RoutingAgent × A2A（LLM classifier 选中 A2A 分支）─────")
	before := remoteCalls.calls.Load()

	weatherCalls = &atomicCounter{name: "get_weather"}
	wt := tool.NewFuncTool("get_weather", "查天气。", map[string]any{"type": "object", "properties": map[string]any{}},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) { return tool.Success("晴 25°C"), nil })
	wtk := tool.NewToolkit()
	wtk.Register(wt)
	weather := agent_v2.NewBasicAgent(
		agent_v2.WithName("weather_desk"),
		agent_v2.WithSystemPrompt("你是天气台。必须调 get_weather 工具后回答。"),
		agent_v2.WithModel(llm),
		agent_v2.WithToolkit(wtk),
		agent_v2.WithHooks(weatherCalls),
	)
	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("router_brain"),
		agent_v2.WithModel(llm),
	)
	ra := agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("code_or_weather"),
		agent_v2.WithRoutingClassifier(classifier),
		agent_v2.WithRoutingDescriptions(map[string]string{
			"code":    "查询内部代号 / 编号 / code 类请求",
			"weather": "天气、气温、下雨等气象问题",
		}),
	).
		AddBranch("code", newRemote(ctx, "code_route")).
		AddBranch("weather", weather)
	rc, cancel := runCtx(ctx)
	defer cancel()
	events, err := ra.Run(rc, agent_v2.NewRunSession(), schema.UserMessage("内部代号是什么？查出来原样告诉我"))
	if err != nil {
		log.Fatalf("Routing.Run: %v", err)
	}
	final := mustFinal(events, "Routing")
	fmt.Printf("routing> %.120s\n", final.Content)
	if weatherCalls.calls.Load() != 0 {
		log.Fatalf("✗ FAIL [Routing×A2A]：负分支 weather 不应被调，calls=%d", weatherCalls.calls.Load())
	}
	assertAnchor("Routing×A2A", final, 1, remoteCalls.calls.Load()-before)
}

// ── 4. ReflectAgent：actor 是带 A2A tool 的 worker，确定性 EvalFunc 锚 7741 ──
func partReflect(ctx context.Context) {
	fmt.Println("───── Part 4: ReflectAgent × A2A（actor 经 A2A tool 拿代号，EvalFunc 把关）─────")
	before := remoteCalls.calls.Load()
	ra := agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("code_reflect"),
		agent_v2.WithEvalFunc(agent_v2.EvalFunc(func(_ context.Context, output *schema.Message) (bool, error) {
			return output != nil && strings.Contains(output.Content, "7741"), nil
		})),
		agent_v2.WithReflectMaxRounds(2),
	).WithActor(newWorker(ctx, "reflect_actor", "code_agent_r"))
	rc, cancel := runCtx(ctx)
	defer cancel()
	events, err := ra.Run(rc, agent_v2.NewRunSession(), schema.UserMessage("查一下内部代号，原样返回"))
	if err != nil {
		log.Fatalf("Reflect.Run: %v", err)
	}
	final := mustFinal(events, "Reflect")
	fmt.Printf("reflect> %.120s\n", final.Content)
	assertAnchor("Reflect×A2A", final, 1, remoteCalls.calls.Load()-before)
}

// ── 5. HandoffAgent：前台 qwen 转交给带 A2A tool 的专员（模型 B transfer）──
func partHandoff(ctx context.Context) {
	fmt.Println("───── Part 5: HandoffAgent × A2A（前台 transfer → 专员经 A2A 拿代号）─────")
	before := remoteCalls.calls.Load()
	reception := agent_v2.NewBasicAgent(
		agent_v2.WithName("reception"),
		agent_v2.WithSystemPrompt("你是前台。用户的请求涉及内部代号/编号查询时，必须调用 transfer_to_agent 转交给 code_desk，不要自己回答。"),
		agent_v2.WithModel(llm),
		agent_v2.WithHandoffTransfer(),
	)
	codeDeskRA := newRemote(ctx, "code_agent_h")
	dtk := tool.NewToolkit()
	dtk.Register(agent_v2.AgentAsTool(codeDeskRA))
	codeDesk := agent_v2.NewBasicAgent(
		agent_v2.WithName("code_desk"),
		agent_v2.WithSystemPrompt("你是代号专员。必须调用 code_agent_h 工具查代号，把答复里的代号原样完整报告。"),
		agent_v2.WithModel(llm),
		agent_v2.WithToolkit(dtk),
		agent_v2.WithHandoffTransfer(),
	)
	ha := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("code_triage"),
		agent_v2.WithHandoffInitial("reception"),
		agent_v2.WithHandoffMaxHops(4),
	).
		AddAgent("reception", reception).
		AddAgent("code_desk", codeDesk)
	rc, cancel := runCtx(ctx)
	defer cancel()
	events, err := ha.Run(rc, agent_v2.NewRunSession(), schema.UserMessage("帮我查一下内部代号，原样告诉我"))
	if err != nil {
		log.Fatalf("Handoff.Run: %v", err)
	}
	final := mustFinal(events, "Handoff")
	fmt.Printf("handoff> %.120s\n", final.Content)
	assertAnchor("Handoff×A2A", final, 1, remoteCalls.calls.Load()-before)
}

// ── 6. GraphAgent：自定义 NodeFunc 内调 RemoteAgent.Run（agent 形态进图）──
func partGraph(ctx context.Context) {
	fmt.Println("───── Part 6: GraphAgent × A2A（NodeFunc 内调 RemoteAgent.Run）─────")
	before := remoteCalls.calls.Load()
	ra := newRemote(ctx, "code_agent_g")

	// 复用 a2a 包外的 compose_v2 最小图范式（见 agent_v2/graph_agent_test.go）
	g, ga := buildCodeGraph(ra)
	_ = g
	rc, cancel := runCtx(ctx)
	defer cancel()
	events, err := ga.Run(rc, agent_v2.NewRunSession(), schema.UserMessage("查一下内部代号，原样返回"))
	if err != nil {
		log.Fatalf("Graph.Run: %v", err)
	}
	final := mustFinal(events, "Graph")
	fmt.Printf("graph> %.120s\n", final.Content)
	assertAnchor("GraphAgent×A2A", final, 1, remoteCalls.calls.Load()-before)
}
