//go:build integration

// a2a_zk_client 是纯 client 侧 A2A 服务发现 + 调用工具——连真 ZK 发现**别处注册**的
// agent（可能是别的进程 / 团队 / 语言写的）并调用，自己**不起 server、不注册**。这才是
// 真实拓扑：server 与 client 分属不同进程，client 只管发现 + 调用。
//
// 前提：zk 里已有对应 service 的 agent 注册（别人跑着 server）、且你知道它的 service name。
//
// 两种调用形态：
//   - 默认（agent 形态）：RemoteAgent 直接 ra.Run，无需 LLM，span 树 host → a2a client → a2a server。
//   - -as-tool（AgentAsTool 形态，RemoteAgent 主用法）：真 qwen host 把远程 agent 当 tool、由
//     host LLM 决定调用，span 树多 model 节点 + a2a client span 落 child ns（需 .env QWEN_API_KEY）。
//
// 跑（需可达 ZK）：
//
//	go run -tags integration ./example/a2a/zk_client -zk 127.0.0.1:2181 -service code-svc -q "帮我查代号"
//	go run -tags integration ./example/a2a/zk_client -zk 127.0.0.1:2181 -service code-svc -q "查代号" -as-tool
//
// 传 -obs-trace-endpoint 时把 trace 以 OTLP 发给该 collector：W3C trace context 写进 A2A metadata，
// 打印 trace ID 供在后端搜跨进程 trace。不传时不导出（调用照常）。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"crew-go/a2a"
	"crew-go/a2a/registry"
	"crew-go/a2a/zk"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/example/otelsetup"
	"crew-go/hook"
	"crew-go/observability"
	"crew-go/schema"
	"crew-go/tool"
)

func main() {
	var (
		zkEndpoint = flag.String("zk", "", "ZooKeeper 地址（逗号分隔多节点），如 127.0.0.1:2181")
		ns         = flag.String("ns", registry.DefaultNamespace, "namespace")
		service    = flag.String("service", "", "要发现 + 调用的 service name（别处注册的）")
		agentName  = flag.String("agent-name", "", "只发现指定 agentName 的实例（同一 service 注册多个 agent 时精确路由；空 = 该 service 下所有 agentName 聚合，selector 选一个）")
		query      = flag.String("q", "你好", "发给远程 agent 的消息")
		asTool     = flag.Bool("as-tool", false, "AgentAsTool 形态：真 qwen host 把远程 agent 当 tool、由 LLM 决定调用（RemoteAgent 主用法，需 .env QWEN_API_KEY）；默认 agent 形态直接 ra.Run")
		obsTrace   = flag.String("obs-trace-endpoint", "", "OTLP gRPC 地址（如 127.0.0.1:4317）；空 = 不导出 trace")
	)
	flag.Parse()
	if *zkEndpoint == "" || *service == "" {
		log.Fatal("必填：-zk <地址> -service <服务名>（纯 client：发现别处注册的 agent 并调用）")
	}
	servers := strings.Split(*zkEndpoint, ",")
	ctx := context.Background()

	// ── 1. 连 ZK（仅发现端，不注册、不起 server）──
	dir, err := zk.New(servers, 10*time.Second)
	if err != nil {
		log.Fatalf("zk.New: %v", err)
	}
	defer func() { _ = dir.Close() }()
	fmt.Printf("✓ 已连 ZK %s\n", *zkEndpoint)

	// ── 2. 发现 service 下的实例（别处 / 跨语言注册的）──
	insts, err := dir.Discover(ctx, *ns, *service)
	if err != nil {
		log.Fatalf("✗ Discover %s/%s: %v", *ns, *service, err)
	}
	if len(insts) == 0 {
		log.Fatalf("✗ %s/%s 下没发现任何实例——确认 zk 里有人注册了该 service（service name 拼对、注册进程活着、namespace 对）", *ns, *service)
	}
	fmt.Printf("✓ 发现 %d 个实例：\n", len(insts))
	for _, in := range insts {
		fmt.Printf("    id=%s agentName=%s baseURL=%s status=%s transport=%s cardURL=%s\n",
			in.InstanceID, in.AgentName, in.BaseURL, in.Status, in.Transport, in.AgentCardURL)
	}

	// ── 3. 接 observability：配了地址就装 OTel SDK 导出 trace；没配时 span 是 no-op，调用照常。──
	if *obsTrace != "" {
		shutdown, err := otelsetup.Setup(ctx, otelsetup.Config{ServiceName: "zk-client", TraceEndpoint: *obsTrace, Insecure: true})
		if err != nil {
			log.Printf("OTel SDK 装配失败（调用继续，不导出 trace）: %v", err)
		} else {
			defer func() { _ = shutdown(context.Background()) }()
		}
	}
	obs := observability.New(observability.WithContentRecording(true))

	opts := []a2a.RemoteOption{a2a.WithName("zk_remote")}
	if *agentName != "" {
		opts = append(opts, a2a.WithDiscoveryAgentName(*agentName))
	}
	opts = append(opts, a2a.WithObservability(obs))
	sel := registry.NewRoundRobinSelector()

	// ── 4. 调用——两种形态 ──
	var final *schema.Message
	if *asTool {
		// 4a. AgentAsTool 形态（RemoteAgent 主用法）：真 qwen host 把远程 agent 当 tool，由 host
		// 的 LLM 决定调用。span 树多 model 节点 + a2a client span 落在 child ns（host agent span 在
		// parent ns、经 RunSubAgent push child ns，验证 child ns 上溯找父 span）。需 .env QWEN_API_KEY。
		envloader.Load()
		remoteTool, err := a2a.NewRemoteAgentToolFromService(ctx, dir, *ns, *service, sel, opts...)
		if err != nil {
			log.Fatalf("✗ NewRemoteAgentToolFromService: %v", err)
		}
		tk := tool.NewToolkit()
		tk.Register(remoteTool)
		hostLLM, _ := llmtrace.NewQwenFallback() // 候选取 qwen.TestModelCandidates，额度耗尽自动切
		hostOpts := []agent_v2.Option{
			agent_v2.WithName("zk_client_host"),
			agent_v2.WithDescription("把远程 zk agent 当 tool 调用的 host"),
			// 引导 host LLM 调远程 tool（否则模糊的 tool 描述下 LLM 可能直接答、demo 不稳）
			agent_v2.WithSystemPrompt(fmt.Sprintf("你有一个远程 agent 工具（服务名 %s）。回答用户问题时优先调用它获取信息，再据此作答。", *service)),
			agent_v2.WithModel(hostLLM),
			agent_v2.WithToolkit(tk),
		}
		// ⚠️ 用 Install 一次接上**两条**链——这里曾只接 AgentHooks()，
		// 于是 compact/cp/fanout/node/skill 共 8 个 metric 全不 fire、node 与 graph
		// 的 span 也不建，而且**不报错**（漏装是静默的，见 Install 的 godoc）。
		runCtx, hostOpts := obs.Install(ctx, hostOpts...)
		host := agent_v2.NewBasicAgent(hostOpts...)
		sess := agent_v2.NewRunSession()
		runCtx = compose_v2.WithSessionIDCtx(runCtx, sess.RunID)
		// 开一个根 span 包住整次调用，拿它的 trace ID 去后端搜跨进程 trace。
		runCtx, root := obs.Tracer().Start(runCtx, "zk_client as_tool")
		defer root.End()
		if tid := observability.TraceIDFromContext(runCtx); tid != "" {
			fmt.Printf("✓ trace ID=%s（AgentAsTool 形态，在 trace 后端搜跨进程 trace）\n", tid)
		}
		callCtx, cancel := context.WithTimeout(runCtx, 120*time.Second)
		defer cancel()
		events, err := host.Run(callCtx, sess, schema.UserMessage(*query))
		if err != nil {
			log.Fatalf("✗ host.Run（AgentAsTool）: %v", err)
		}
		final = agent_v2.FinalMessage(events)
	} else {
		// 4b. agent 形态：RemoteAgent 直接 Run（无需 LLM）。手建顶层 host span，让 a2a client span
		// 有父可挂（input=query / output=应答，client 视角）。
		sess := agent_v2.NewRunSession()
		runCtx := compose_v2.WithSessionIDCtx(ctx, sess.RunID)
		ah := obs.AgentHooks()
		info := &hook.AgentInfo{Name: "zk_client_host", Type: "base"}
		runCtx, _ = ah.OnAgentStart(runCtx, info, &hook.AgentInput{Messages: []*schema.Message{schema.UserMessage(*query)}})
		endHost := func(out *schema.Message) {
			var outMsgs []*schema.Message
			if out != nil {
				outMsgs = []*schema.Message{out}
			}
			ah.OnAgentEnd(runCtx, info, &hook.AgentOutput{Messages: outMsgs})
		}
		if tid := observability.TraceIDFromContext(runCtx); tid != "" {
			fmt.Printf("✓ trace ID=%s（在 trace 后端搜此 ID 看跨进程 trace）\n", tid)
		}
		ra, err := a2a.NewRemoteAgentFromService(runCtx, dir, *ns, *service, sel, opts...)
		if err != nil {
			log.Fatalf("✗ NewRemoteAgentFromService: %v", err)
		}
		callCtx, cancel := context.WithTimeout(runCtx, 60*time.Second)
		defer cancel()
		events, runErr := ra.Run(callCtx, agent_v2.NewRunSession(), schema.UserMessage(*query))
		final = agent_v2.FinalMessage(events)
		endHost(final) // host span output = 远程应答（final nil 时 output 空，仍收尾）
		if runErr != nil {
			log.Fatalf("✗ RemoteAgent.Run: %v", runErr)
		}
	}

	if final == nil {
		log.Fatal("✗ 远程调用无 final message（A2A wire 链路问题）")
	}
	fmt.Printf("\n=== 远程应答 ===\n%s\n", final.Content)
}
