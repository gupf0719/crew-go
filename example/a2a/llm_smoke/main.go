// a2a_llm_smoke 用**真 LLM（qwen）**端到端验证 A2A 全链路，三部分：
//
//  1. 全互通环：crew-go agent 被 server 适配暴露成 A2A server + 注册进服务目录，
//     host BasicAgent（真 qwen）发现它、当工具，由 LLM 自主 tool-calling 调度远程 agent。
//  2. 流式：served 真 qwen BasicAgent 的 LLM chunk 经 server 桥接为 A2A artifact 事件，
//     裸 a2aclient 直接数 wire 上的 TaskArtifactUpdateEvent。
//  3. server 侧 HITL：served 真 qwen BasicAgent + ask_user 抛中断 → A2A input-required →
//     客户端 RemoteAgent(WithHumanGate) 中断 → Resume 答复 → 服务端按 taskID Resume → 终答。
//
// 这是 CLAUDE.md "build 通过≠能用 / 真 LLM 用例" 要求的真实 LLM 冒烟——单测用 mock model
// 验证机制，本 smoke 用真 qwen 验证真实链路。
//
// 跑（需项目根 .env 的 QWEN_API_KEY，envloader 自动加载）：
//
//	go run ./example/a2a/llm_smoke
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
	"crew-go/compose_v2"
	"crew-go/example/envloader"
	"crew-go/fallback"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/model/qwen"
	"crew-go/schema"
	"crew-go/tool"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

// shoutAgent 是被暴露的远程 agent——把文本变"响亮"（大写 + "!!!"）。确定性，不需 LLM，
// 让断言只依赖"工具是否被调 + 结果是否确定变换"，不依赖 host LLM 的措辞。
type shoutAgent struct{}

func (shoutAgent) Name() string { return "shout_agent" }
func (shoutAgent) Description() string {
	return "make text loud: returns the UPPERCASED text followed by !!!"
}

func (shoutAgent) Run(_ context.Context, sess *agent_v2.RunSession, input *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	out := schema.AssistantMessage(strings.ToUpper(input.Content) + "!!!")
	ev := agent_v2.NewExitEvent("shout_agent", out)
	sess.AddEvent(ev)
	return []*agent_v2.AgentEvent{ev}, nil
}

func (shoutAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, fmt.Errorf("shout: resume not supported")
}

// toolTracer 记录工具调用（OnToolStart/End 串行触发，无需加锁）。
type toolTracer struct {
	hook.NoOpHooks
	calls   int
	results []string
}

func (t *toolTracer) OnToolStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	fmt.Printf("  [tool→] %s args=%s\n", in.Name, in.ArgumentsJSON)
	return ctx, nil
}

func (t *toolTracer) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	t.calls++
	if out.Result != nil {
		res := fmt.Sprintf("%v", out.Result.Output)
		t.results = append(t.results, res)
		fmt.Printf("  [tool←] success=%v output=%q\n", out.Result.Success, res)
	}
	return ctx, nil
}

// serve 把 agent 起成 in-process A2A server，返回 baseURL（进程退出自动回收）。
func serve(ag agent_v2.Agent) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	baseURL := "http://" + ln.Addr().String()
	go func() {
		_ = http.Serve(ln, server.Handler(ag, baseURL, server.WithOwnerResolver(func(context.Context) string { return "demo" })))
	}()
	return baseURL
}

func main() {
	envloader.Load()
	if os.Getenv("QWEN_API_KEY") == "" {
		log.Fatal("QWEN_API_KEY 未设置（.env 加 QWEN_API_KEY=xxx）——本 smoke 需真 qwen")
	}
	ctx := context.Background()

	smokeInteropLoop(ctx)
	smokeStreaming(ctx)
	smokeServerHITL(ctx)

	fmt.Println("\n=== A2A 真 LLM 冒烟全 PASS（全互通环 + 流式 artifact + server 侧 HITL，全部真 qwen）===")
}

// ---- Part 1：全互通环（host qwen LLM 调度发现到的 A2A 工具）----

func smokeInteropLoop(ctx context.Context) {
	fmt.Println("───── Part 1: 全互通环（host qwen → 发现 → A2A 工具）─────")

	// 1. 把 shout_agent 暴露成 A2A server + 注册进服务目录。
	baseURL := serve(shoutAgent{})
	dir := registry.NewInMemory()
	if _, err := dir.Register(ctx, registry.DefaultNamespace, server.Instance("shout-svc", "", "i1", baseURL)); err != nil {
		log.Fatalf("Register: %v", err)
	}
	fmt.Printf("✓ shout_agent 已暴露为 A2A server (%s) 并注册为 shout-svc\n", baseURL)

	// 2. 从服务目录发现它，包成 host 可用的 tool。
	shoutTool, err := a2a.NewRemoteAgentToolFromService(ctx, dir, registry.DefaultNamespace, "shout-svc", nil)
	if err != nil {
		log.Fatalf("NewRemoteAgentToolFromService: %v", err)
	}
	fmt.Printf("✓ 发现并包装成 tool: %s\n", shoutTool.Name())

	// 3. host BasicAgent（真 qwen）+ 该 A2A 工具。
	tracer := &toolTracer{}
	tk := tool.NewToolkit()
	tk.Register(shoutTool)
	host := agent_v2.NewBasicAgent(
		agent_v2.WithName("host"),
		agent_v2.WithSystemPrompt(`你有一个名为 shout_agent 的工具，能把一段文本变"响亮"（转大写并加感叹号）。`+
			`当用户要求让某句话变响亮 / 喊出来 / 变大声时，必须调用 shout_agent 工具（input 传要处理的那句话），`+
			`拿到结果后把它告诉用户。`),
		agent_v2.WithModel(newModel()),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(tracer),
	)

	// 4. 给一个需要调用远程工具的任务。
	runCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	fmt.Println(">>> 任务：请把 hello a2a 这句话变得很响亮")
	events, err := host.Run(runCtx, agent_v2.NewRunSession(), schema.UserMessage("请把 hello a2a 这句话变得很响亮"))
	if err != nil {
		log.Fatalf("host.Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		log.Fatal("无 final message")
	}
	fmt.Printf("agent> %s\n", final.Content)

	// 5. 断言：真 LLM 确实调了 A2A 工具 + 远程确定性变换生效。
	if tracer.calls == 0 {
		log.Fatal("✗ FAIL：host LLM 没有调用任何工具（期望调 shout_agent A2A 工具）")
	}
	shouted := false
	for _, r := range tracer.results {
		if strings.Contains(r, "!!!") && r == strings.ToUpper(r) {
			shouted = true
		}
	}
	if !shouted {
		log.Fatalf("✗ FAIL：A2A 工具结果未呈现远程确定性变换（大写+!!!），results=%v", tracer.results)
	}
	fmt.Printf("✓ Part 1 PASS：qwen 自主调用了发现到的 A2A 工具 %d 次，远程返回变换正确\n\n", tracer.calls)
}

// ---- Part 2：流式（真 qwen chunk → A2A artifact，裸 client 数 wire 事件）----

func smokeStreaming(ctx context.Context) {
	fmt.Println("───── Part 2: 流式（真 qwen chunk → A2A artifact）─────")

	streamer := agent_v2.NewBasicAgent(
		agent_v2.WithName("qwen_streamer"),
		agent_v2.WithSystemPrompt("你是简洁的助手，用一两句话回答。"),
		agent_v2.WithModel(newModel()),
	)
	baseURL := serve(streamer)

	client, err := a2aclient.NewFromCard(ctx, server.Card(streamer, baseURL))
	if err != nil {
		log.Fatalf("NewFromCard: %v", err)
	}
	runCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	var (
		artifactEvents int
		artifactText   strings.Builder
		lastState      a2atypes.TaskState
	)
	msg := a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("用一句话介绍一下你自己"))
	for ev, e := range client.SendStreamingMessage(runCtx, &a2atypes.SendMessageRequest{Message: msg}) {
		if e != nil {
			log.Fatalf("stream: %v", e)
		}
		switch v := ev.(type) {
		case *a2atypes.TaskArtifactUpdateEvent:
			artifactEvents++
			if v.Artifact != nil {
				for _, p := range v.Artifact.Parts {
					if p != nil {
						artifactText.WriteString(p.Text())
					}
				}
			}
		case *a2atypes.TaskStatusUpdateEvent:
			lastState = v.Status.State
		case *a2atypes.Task:
			lastState = v.Status.State
		}
	}
	if artifactEvents < 1 {
		log.Fatalf("✗ FAIL：wire 上没有 artifact 事件（chunk→artifact 桥未生效），state=%s", lastState)
	}
	if lastState != a2atypes.TaskStateCompleted {
		log.Fatalf("✗ FAIL：终态应 completed，got %s", lastState)
	}
	if strings.TrimSpace(artifactText.String()) == "" {
		log.Fatal("✗ FAIL：artifact 聚合文本为空")
	}
	fmt.Printf("✓ Part 2 PASS：真 qwen 流式经 A2A 投递 %d 个 artifact 事件，聚合: %.60s...\n\n", artifactEvents, artifactText.String())
}

// ---- Part 3：server 侧 HITL（真 qwen ask_user 中断 → A2A input-required → Resume 闭环）----

func smokeServerHITL(ctx context.Context) {
	fmt.Println("───── Part 3: server 侧 HITL（真 qwen ask_user over A2A）─────")

	asker := agent_v2.NewBasicAgent(
		agent_v2.WithName("qwen_asker"),
		agent_v2.WithSystemPrompt(`无论用户说什么，你必须先调用一次 ask_user 工具问用户最喜欢什么颜色`+
			`（question 填"你最喜欢什么颜色？"）。拿到答案后用一句话回复，内容必须包含用户说的颜色。`+
			`没调用 ask_user 之前不要直接回答。`),
		agent_v2.WithModel(newModel()),
		agent_v2.WithAskUser(),
	)
	baseURL := serve(asker)

	ra, err := a2a.NewRemoteAgent(ctx, baseURL, a2a.WithHumanGate())
	if err != nil {
		log.Fatalf("NewRemoteAgent: %v", err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("hitl_seq")).AddStep("remote", ra)
	hctx := compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	runCtx, cancel := context.WithTimeout(hctx, 180*time.Second)
	defer cancel()
	sess := agent_v2.NewRunSession()

	fmt.Println(">>> 任务：给我点穿搭建议（期望 qwen 先 ask_user 问颜色 → 中断冒泡到客户端）")
	_, err = seq.Run(runCtx, sess, schema.UserMessage("给我点穿搭建议"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		log.Fatalf("✗ FAIL：期望 InterruptError（qwen 未触发 ask_user？），got %v", err)
	}
	if ie.Query == nil || strings.TrimSpace(ie.Query.Question) == "" {
		log.Fatalf("✗ FAIL：中断 Question 为空: %+v", ie)
	}
	fmt.Printf("✓ 中断冒泡到客户端: q=%q id=%q\n", ie.Query.Question, ie.Query.InterruptID)

	fmt.Println(">>> 人类答复：蓝色 → Resume（客户端续 taskID → 服务端按映射 Resume）")
	events, err := seq.Resume(runCtx, sess.RunID,
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("蓝色"),
		}))
	if err != nil {
		log.Fatalf("✗ FAIL：Resume: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || strings.TrimSpace(final.Content) == "" {
		log.Fatal("✗ FAIL：Resume 后无终答")
	}
	fmt.Printf("agent> %s\n", final.Content)
	if !strings.Contains(final.Content, "蓝") {
		log.Fatalf("✗ FAIL：终答未包含用户答复的颜色（蓝），got %q", final.Content)
	}
	fmt.Println("✓ Part 3 PASS：server 侧 HITL 闭环（中断 → input-required → 续发 → Resume → 终答含人类答复）")
}

// newModel 把 qwen.TestModelCandidates 串成 fallback 链——首位免费额度耗尽（dashscope
// 403 AllocationQuota.FreeTierOnly）时自动切下一个候选，冒烟不因单个模型的额度挂掉。
// ⚠️ 别写 NewTestClient("")：那只取候选首位**单个**模型，没有兜底。
func newModel() model.ChatModel {
	models := make([]model.ChatModel, 0, len(qwen.TestModelCandidates))
	for _, name := range qwen.TestModelCandidates {
		models = append(models, qwen.NewTestClient(name))
	}
	return fallback.New(models)
}
