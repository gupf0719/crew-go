//go:build integration

// a2a_zk_demo 用**真 ZooKeeper**验证 A2A 服务注册发现 + 跨进程调用闭环：
//
//	本地 agent → server.Handler 暴露 A2A server → zk.Register 注册到真 ZK
//	      ↓（独立 ZK 连接，模拟另一进程的发现端）
//	zk.Discover 发现实例 → a2a.NewRemoteAgentFromService 经 Selector 路由 + 拉 Card
//	      ↓
//	RemoteAgent.Run（A2A wire）→ 远程 agent 应答 → 验证拿到确定性代号
//
// 用确定性 model（不依赖 LLM/.env），聚焦 ZK 发现 + A2A wire；验证 4 个锚点：
// 注册成功 / 跨连接发现到实例 / 跨进程调用拿到远程答案 / ephemeral 反注册后消失。
//
// 跑（需可达 ZK）：
//
//	ZOOKEEPER_ENDPOINT=127.0.0.1:2181 go run -tags integration ./example/a2a/zk_demo
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
	"crew-go/a2a/zk"
	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
)

const codeAnswer = "查询完成：当前内部代号是 SECRET-7741。"

// fixedModel 是确定性 ChatModel——恒返固定答案（含代号），不依赖外部 LLM。
type fixedModel struct{ answer string }

func (m *fixedModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(m.answer), nil
}

func (m *fixedModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return schema.NewSingleChunkReader(schema.AssistantMessage(m.answer)), nil
}

func (m *fixedModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func main() {
	endpoint := os.Getenv("ZOOKEEPER_ENDPOINT")
	if endpoint == "" {
		log.Fatal("ZOOKEEPER_ENDPOINT 未设置——本 demo 需可达 ZooKeeper，如 ZOOKEEPER_ENDPOINT=127.0.0.1:2181")
	}
	servers := strings.Split(endpoint, ",")
	ctx := context.Background()
	const (
		ns          = registry.DefaultNamespace
		serviceName = "code-svc"
		instanceID  = "demo-inst-1"
	)

	// ── 1. 本地确定性 agent，暴露为 A2A server（随机端口）──
	remoteAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("code_service"),
		agent_v2.WithDescription("内部代号查询服务（确定性 demo agent）"),
		agent_v2.WithModel(&fixedModel{answer: codeAnswer}),
	)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	baseURL := "http://" + ln.Addr().String()
	srv := &http.Server{Handler: server.Handler(remoteAgent, baseURL, server.WithOwnerResolver(func(context.Context) string { return "demo" }))}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()
	fmt.Printf("✓ 本地 code_service 已暴露 A2A server: %s\n", baseURL)

	// ── 2. 注册端连接真 ZK，注册实例（ephemeral）──
	regDir, err := zk.New(servers, 10*time.Second)
	if err != nil {
		log.Fatalf("zk.New (注册端): %v", err)
	}
	defer func() { _ = regDir.Close() }()

	reg, err := regDir.Register(ctx, ns, server.Instance(serviceName, "", instanceID, baseURL))
	if err != nil {
		log.Fatalf("✗ 注册失败: %v", err)
	}
	fmt.Printf("✓ [锚点1] 注册成功 → 真 ZK %s 下 %s/%s/%s/%s（4 层，agentName 缺省=serviceName）\n", endpoint, ns, serviceName, serviceName, instanceID)

	// ── 3. 发现端：**独立 ZK 连接**模拟另一进程，发现刚注册的实例 ──
	discDir, err := zk.New(servers, 10*time.Second)
	if err != nil {
		log.Fatalf("zk.New (发现端): %v", err)
	}
	defer func() { _ = discDir.Close() }()

	insts, err := discDir.Discover(ctx, ns, serviceName)
	if err != nil {
		log.Fatalf("✗ Discover 失败: %v", err)
	}
	found := false
	for _, in := range insts {
		if in.InstanceID == instanceID {
			found = true
			fmt.Printf("✓ [锚点2] 跨连接发现到实例: id=%s baseURL=%s status=%s weight=%v cardURL=%s\n",
				in.InstanceID, in.BaseURL, in.Status, derefWeight(in.Weight), in.AgentCardURL)
		}
	}
	if !found {
		log.Fatalf("✗ 发现端没找到刚注册的实例 %q（共发现 %d 个）", instanceID, len(insts))
	}

	// ── 4. 经服务发现 + Selector 路由建 RemoteAgent → 跨进程调用 ──
	ra, err := a2a.NewRemoteAgentFromService(ctx, discDir, ns, serviceName, registry.NewRoundRobinSelector(),
		a2a.WithName("code_remote"))
	if err != nil {
		log.Fatalf("✗ NewRemoteAgentFromService: %v", err)
	}
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	events, err := ra.Run(runCtx, agent_v2.NewRunSession(), schema.UserMessage("帮我查内部代号"))
	if err != nil {
		log.Fatalf("✗ RemoteAgent.Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		log.Fatal("✗ 远程调用无 final message")
	}
	fmt.Printf("  远程应答: %q\n", final.Content)
	if !strings.Contains(final.Content, "SECRET-7741") {
		log.Fatalf("✗ [锚点3] 跨进程调用未拿到确定性代号 7741（A2A wire 链路问题），got %q", final.Content)
	}
	fmt.Println("✓ [锚点3] 跨进程调用拿到远程 agent 应答（含 SECRET-7741）")

	// ── 5. ephemeral 验证：反注册后发现端应查不到 ──
	if err := reg.Deregister(ctx); err != nil {
		log.Fatalf("✗ Deregister: %v", err)
	}
	time.Sleep(500 * time.Millisecond) // 等 ZK 节点删除传播
	insts2, err := discDir.Discover(ctx, ns, serviceName)
	if err != nil {
		log.Fatalf("✗ Deregister 后 Discover: %v", err)
	}
	for _, in := range insts2 {
		if in.InstanceID == instanceID {
			log.Fatalf("✗ [锚点4] 反注册后实例仍可发现（ephemeral 语义失效），共 %d 个", len(insts2))
		}
	}
	fmt.Printf("✓ [锚点4] 反注册后实例消失（ephemeral 语义，剩 %d 个）\n", len(insts2))

	fmt.Println("\n=== A2A × 真 ZooKeeper 服务发现 + 跨进程调用闭环 PASS ===")
}

// derefWeight 安全解 *int（nil → 显示默认 100 来源标记）。
func derefWeight(w *int) string {
	if w == nil {
		return "nil(默认100)"
	}
	return fmt.Sprintf("%d", *w)
}
