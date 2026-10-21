// a2a_plan_smoke 用**真 qwen（四层全真 LLM）**验证 plan-chat 拓扑调用 A2A 远程 agent
// （as tool）——完整复刻 plan_chat_web 的接线形态：
//
//	chat_entry BasicAgent(qwen)
//	  └─ AgentAsTool(PlanAgent)                ← plan_chat_web 同款包装
//	       └─ LLMPlanner(qwen) 派发 sub-agent
//	            └─ code_worker BasicAgent(qwen)
//	                 └─ A2A tool（NewRemoteAgentToolFromService 发现）   ← A2A as tool
//	                      └─（A2A wire）→ 远程 BasicAgent(qwen) + lookup_code 确定性工具
//
// 断言用确定性锚点（不依赖 LLM 措辞）：远程侧 lookup_code 真被调 + worker 侧 A2A 工具
// 真被调 + 终答含确定性代号 "SECRET-7741" 的数字部分。
//
// 跑（项目根 .env 需 QWEN_API_KEY；四层真 LLM 约 1-3 分钟）：
//
//	go run ./example/a2a/plan_smoke
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"crew-go/a2a"
	"crew-go/a2a/registry"
	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/hook"
	"crew-go/schema"
	"crew-go/tool"
)

// callCounter 数某个工具被调的次数（OnToolStart/End 串行触发，无需加锁）。
type callCounter struct {
	hook.NoOpHooks
	name  string
	calls int
}

func (c *callCounter) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	fmt.Printf("  [tool→] %s args=%s\n", in.Name, in.ArgumentsJSON)
	if in.Name == c.name {
		c.calls++
	}
	return ctx, nil
}

func main() {
	envloader.Load()
	if os.Getenv("QWEN_API_KEY") == "" {
		log.Fatal("QWEN_API_KEY 未设置（.env 加 QWEN_API_KEY=xxx）——本 smoke 需真 qwen")
	}
	// 全部 agent 共用一条 qwen 备选链（候选取 qwen.TestModelCandidates，额度耗尽自动切）。
	llm, llmTracer := llmtrace.NewQwenFallback()
	ctx := context.Background()

	// ── 1. "另外建的 agent"：真 qwen BasicAgent + 确定性 lookup_code 工具，暴露成 A2A server ──
	lookupCode := tool.NewFuncTool(
		"lookup_code",
		"查询内部代号。无参数，返回当前内部代号。",
		map[string]any{"type": "object", "properties": map[string]any{}},
		func(_ context.Context, _ map[string]any) (*tool.Result, error) {
			return tool.Success("SECRET-7741"), nil
		},
	)
	remoteTracer := &callCounter{name: "lookup_code"}
	remoteTK := tool.NewToolkit()
	remoteTK.Register(lookupCode)
	remoteAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("code_service"),
		agent_v2.WithSystemPrompt("你是代号查询服务。收到任何查询请求都必须先调用 lookup_code 工具，然后把工具返回的代号原样完整地告诉对方（不要省略、不要改写）。"),
		agent_v2.WithModel(llm),
		agent_v2.WithToolkit(remoteTK),
		agent_v2.WithHooks(remoteTracer),
	)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	baseURL := "http://" + ln.Addr().String()
	go func() {
		_ = http.Serve(ln, server.Handler(remoteAgent, baseURL, server.WithOwnerResolver(func(context.Context) string { return "demo" })))
	}()
	defer func() { _ = ln.Close() }()

	dir := registry.NewInMemory()
	if _, err := dir.Register(ctx, registry.DefaultNamespace, server.Instance("code-svc", "", "i1", baseURL)); err != nil {
		log.Fatalf("Register: %v", err)
	}
	fmt.Printf("✓ 远程 code_service（真 qwen + lookup_code 工具）已暴露 %s 并注册为 code-svc\n", baseURL)

	// ── 2. 服务发现 → A2A as tool ──
	codeTool, err := a2a.NewRemoteAgentToolFromService(ctx, dir, registry.DefaultNamespace, "code-svc", nil,
		a2a.WithName("code_agent"),
		a2a.WithDescription("查询内部代号：把查询请求委派给远程代号服务，返回其答复。"))
	if err != nil {
		log.Fatalf("NewRemoteAgentToolFromService: %v", err)
	}
	fmt.Printf("✓ 发现并包成 tool: %s\n", codeTool.Name())

	// ── 3. plan 执行层：code_worker BasicAgent(qwen)，A2A tool 在它的 toolkit ──
	workerTracer := &callCounter{name: "code_agent"}
	workerTK := tool.NewToolkit()
	workerTK.Register(codeTool)
	worker := agent_v2.NewBasicAgent(
		agent_v2.WithName("code_worker"),
		agent_v2.WithDescription("查询内部代号：调用远程代号服务（code_agent 工具）拿代号并原样返回。"),
		agent_v2.WithSystemPrompt("你负责查内部代号。必须调用 code_agent 工具完成查询（input 传查询请求），拿到答复后把其中的代号原样完整报告，不要改写。"),
		agent_v2.WithModel(llm),
		agent_v2.WithToolkit(workerTK),
		agent_v2.WithHooks(workerTracer),
	)

	// ── 4. plan 层：LLMPlanner(qwen) + PlanAgent（plan_chat_web 同款）──
	subAgents := map[string]agent_v2.Agent{"code_worker": worker}
	planner := agent_v2.NewLLMPlanner(llm, agent_v2.WithLLMPlannerSubAgents(subAgents))
	plan := agent_v2.NewPlanAgent(planner,
		agent_v2.PlanName("code_plan"),
		agent_v2.WithSubAgents(subAgents),
		agent_v2.WithPlanMaxSteps(3),
	)

	// ── 5. chat 入口层：BasicAgent(qwen) + AgentAsTool(PlanAgent)（plan_chat_web chat_entry 形态）──
	chatTK := tool.NewToolkit()
	chatTK.Register(agent_v2.AgentAsTool(plan, agent_v2.WithAgentToolDescription("处理需要查询内部代号的任务：规划并派发给代号查询 worker，返回查到的代号。")))
	chat := agent_v2.NewBasicAgent(
		agent_v2.WithName("chat_entry"),
		agent_v2.WithSystemPrompt("用户的请求涉及查询代号/编号时，必须调用 code_plan 工具完成（input 传用户的原始请求），把工具结果里的代号原样告诉用户。"),
		agent_v2.WithModel(llm),
		agent_v2.WithToolkit(chatTK),
	)

	// ── 6. 跑：chat → plan → worker → A2A → 远程 qwen → lookup_code ──
	runCtx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()
	fmt.Println("\n>>> 任务：帮我查一下现在的内部代号是什么，原样告诉我")
	events, err := chat.Run(runCtx, agent_v2.NewRunSession(), schema.UserMessage("帮我查一下现在的内部代号是什么，原样告诉我"))
	if err != nil {
		log.Fatalf("chat.Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		log.Fatal("无 final message")
	}
	fmt.Printf("\nchat> %s\n\n", final.Content)

	// ── 7. 确定性断言（三层锚点）──
	if remoteTracer.calls == 0 {
		log.Fatal("✗ FAIL：远程侧 lookup_code 未被调用——A2A 链路没真正打到远程工具")
	}
	if workerTracer.calls == 0 {
		log.Fatal("✗ FAIL：worker 侧 code_agent（A2A tool）未被调用——plan 派发没走到 A2A")
	}
	if !strings.Contains(final.Content, "7741") {
		log.Fatalf("✗ FAIL：终答未含确定性代号 7741（跨四层传回失败），got %q", final.Content)
	}
	fmt.Println(llmTracer.Report())
	fmt.Printf("=== A2A plan-chat 冒烟 PASS：chat→plan→worker→A2A→远程 qwen 四层真 LLM 闭环（lookup_code 调 %d 次 / A2A tool 调 %d 次 / 代号原样传回）===\n",
		remoteTracer.calls, workerTracer.calls)
}
