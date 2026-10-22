//go:build integration

// a2a_zk_server 起一个 agent 暴露为 A2A server + 注册到真 ZK + 常驻（保持 ephemeral 节点
// 存活），供 a2a_zk_client / 跨语言 client 发现 + 调用。这是真实拓扑的 server 侧——与 client
// 分属不同进程：一个终端跑本 server 注册，另一个终端跑 client 发现调用。
//
//	# 确定性 agent（默认，无外部依赖，先验证链路）
//	go run -tags integration ./example/a2a/zk_server -zk 127.0.0.1:2181 -service my-agent
//	# 真 qwen agent（需项目根 .env 的 QWEN_API_KEY）
//	go run -tags integration ./example/a2a/zk_server -zk 127.0.0.1:2181 -service my-agent -llm
//
// 注册的 baseURL / instanceId 用本机内网 IP（非 127.0.0.1），让别的进程 / 机器能连上调用。
// Ctrl+C 优雅退出：先 Deregister（ephemeral 节点立即消失）再关 server。传 -obs-trace-endpoint
// 时 trace 上报到该 collector（与 client 串成跨进程 trace）。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"crew-go/a2a/registry"
	"crew-go/a2a/server"
	"crew-go/a2a/zk"
	"crew-go/agent_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/example/otelsetup"
	"crew-go/model"
	"crew-go/observability"
	"crew-go/schema"
)

// fixedModel 确定性 ChatModel——恒返固定答案，不依赖外部 LLM（默认 model）。
type fixedModel struct{ answer string }

func (m *fixedModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(m.answer), nil
}

func (m *fixedModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return schema.NewSingleChunkReader(schema.AssistantMessage(m.answer)), nil
}

func (m *fixedModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func main() {
	var (
		zkEP     = flag.String("zk", "", "ZooKeeper 地址（逗号分隔多节点），如 127.0.0.1:2181")
		ns       = flag.String("ns", registry.DefaultNamespace, "namespace")
		service  = flag.String("service", "demo-agent", "注册的 service name")
		name     = flag.String("name", "", "agent 名（默认 = service）")
		port     = flag.Int("port", 0, "server 监听端口（0 = 随机）")
		useLLM   = flag.Bool("llm", false, "用真 qwen（需 .env QWEN_API_KEY）；默认确定性 model")
		obsTrace = flag.String("obs-trace-endpoint", "", "OTLP gRPC 地址（如 127.0.0.1:4317）；空 = 不导出 trace")
	)
	flag.Parse()
	if *zkEP == "" {
		log.Fatal("必填：-zk <地址>")
	}
	agentName := *name
	if agentName == "" {
		agentName = *service
	}
	ctx := context.Background()

	// ── 1. model（确定性 / 真 qwen）──
	var m model.ChatModel
	if *useLLM {
		envloader.Load()
		m, _ = llmtrace.NewQwenFallback()
		log.Println("使用真 qwen 备选链（候选取 qwen.TestModelCandidates，额度耗尽自动切；需 .env QWEN_API_KEY）")
	} else {
		m = &fixedModel{answer: fmt.Sprintf("我是 %s（确定性 demo agent），内部代号 SECRET-7741。", agentName)}
	}

	// ── 2. observability（先建——agent 与 executor 都要接）。配了地址就装 OTel SDK 导出 trace；
	// 打开内容采集，让 agent span 记真实输入 / 输出。──
	if *obsTrace != "" {
		shutdown, err := otelsetup.Setup(ctx, otelsetup.Config{ServiceName: "zk-server-" + *service, TraceEndpoint: *obsTrace, Insecure: true})
		if err != nil {
			log.Printf("OTel SDK 装配失败（server 照跑，不导出 trace）: %v", err)
		} else {
			defer func() { _ = shutdown(context.Background()) }()
		}
	}
	obs := observability.New(observability.WithContentRecording(true))

	// ── 3. 起 agent——接 obs.AgentHooks() 让 server 内部 ReAct agent 的 span 挂在 a2a server span 下
	// （带真实 input=query / output=应答）；只接 executor 的 obs 的话 a2a server span 下是空的。──
	agOpts := []agent_v2.Option{
		agent_v2.WithName(agentName),
		agent_v2.WithDescription(agentName + "（a2a_zk_server 注册的 agent）"),
		agent_v2.WithModel(m),
	}
	{
		// ⚠️ **这里是 Install 覆盖不了的那一档**：agent 建一次、ctx 由 a2a server 每个
		// 请求各自带，两个安装点结构上分开。所以两半各装各的——业务级在这儿，编排级
		// 走下面的 HTTP middleware。漏装任何一半都不报错，只是数据少一截。
		agOpts = append(agOpts, agent_v2.WithHooks(obs.AgentHooks()))
	}
	ag := agent_v2.NewBasicAgent(agOpts...)

	// ── 4. executor 接 obs（a2a server span）──
	execOpts := []server.ExecutorOption{server.WithExecutorObservability(obs)}

	// ── 3. 暴露 A2A server（监听本机内网 IP，让别处可达——非 127.0.0.1）──
	ip := localIP()
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, *port))
	if err != nil {
		log.Fatalf("listen %s:%d: %v", ip, *port, err)
	}
	baseURL := "http://" + ln.Addr().String()
	a2aHandler := server.Handler(ag, baseURL, append(execOpts, server.WithOwnerResolver(func(context.Context) string { return "demo" }))...)
	// ── 编排级 hook 的另一半：每个请求把它 push 进 ctx ──
	// 漏了这一层，节点 / 工作流 span 与压缩、存档、扇出等框架指标都不记——**而且不报错**。
	// executor 会自动把它与内部流式桥串起来（不互斥）。
	{
		inner := a2aHandler
		a2aHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			inner.ServeHTTP(w, r.WithContext(agent_v2.WithCtxOrchHooks(r.Context(), obs.OrchHooks())))
		})
	}
	httpSrv := &http.Server{Handler: a2aHandler}
	go func() { _ = httpSrv.Serve(ln) }()
	log.Printf("✓ A2A server 已起：%s（agent=%s）", baseURL, agentName)

	// ── 4. 注册到 zk（ephemeral：进程退出 / 断连即消失）──
	dir, err := zk.New(strings.Split(*zkEP, ","), 10*time.Second)
	if err != nil {
		log.Fatalf("zk.New: %v", err)
	}
	defer func() { _ = dir.Close() }()
	instanceID := ln.Addr().String() // 内网IP:port
	reg, err := dir.Register(ctx, *ns, server.Instance(*service, agentName, instanceID, baseURL))
	if err != nil {
		log.Fatalf("✗ 注册失败: %v", err)
	}
	log.Printf("✓ 已注册到 zk %s：%s/%s/%s/%s（ephemeral，4 层路径 service/agentName/instanceId）", *zkEP, *ns, *service, agentName, instanceID)
	log.Printf("  另开终端发现 + 调用：")
	log.Printf("    go run -tags integration ./example/a2a/zk_list -zk %s -ns %s", *zkEP, *ns)
	log.Printf("    go run -tags integration ./example/a2a/zk_client -zk %s -ns %s -service %s -q \"你好\"", *zkEP, *ns, *service)

	// ── 5. 常驻，Ctrl+C 优雅退出（Deregister + 关 server）──
	log.Println("server 运行中... Ctrl+C 退出（自动 Deregister）")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("收到退出信号，清理中...")
	if err := reg.Deregister(ctx); err != nil {
		log.Printf("Deregister 失败: %v", err)
	} else {
		log.Println("✓ 已 Deregister（zk 节点已删）")
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutCtx)
	log.Println("✓ server 已关闭，退出")
}

// localIP 取本机首个非 loopback IPv4（注册的 baseURL 要别处可达，不能用 127.0.0.1）。
func localIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, addr := range addrs {
		ipnet, ok := addr.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		if ipv4 := ipnet.IP.To4(); ipv4 != nil {
			return ipv4.String()
		}
	}
	return "127.0.0.1"
}
