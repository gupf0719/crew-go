// v2_store_demo 演示 agent_v2.WithStore + WithStoreMemory 跨 Run 长期记忆最小闭环.
//
// **场景**: 单 user + 单 namespace, 用 InMemoryStore 验证两次 Run 的记忆传递:
//
//	Run 1: user "我喜欢 Go"  → postamble commitFinal 调 store.Put 保存
//	Run 2: user "我喜欢什么?" → preamble 自动 store.Search 注入历史 → LLM 看到记忆
//
// **关键 API**:
//   - `store.NewInMemoryStore()` — 单进程内存实现, 业务可替换 Redis/Postgres
//   - `agent_v2.WithStore(st)` — 把 store 挂到 agent (ctx 透传给 hook/tool/preamble)
//   - `agent_v2.WithStoreMemory(ns, &cfg)` — 启用 BasicAgent preamble 自动 Search +
//     postamble 自动 Put (无需业务自写持久化逻辑)
//   - `store.NS(parts...)` — 类型安全的层级 namespace 构造
//
// **不依赖真 LLM** — stubModel 按入参里"是否含历史 memory"判断返词.
//
// 跑法:
//
//	go run ./example/store/v2_store_demo
//
// 全场景 PASS + 退出码 0 表示通过.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/store"
)

// stubModel 按 LLM 收到的 messages 判路由——看是否含 "Go" 关键词
// 决定返"我已知"或"还不知道". 模拟 LLM 看到 store 注入的历史后才能答对.
//
// **关键**: BasicAgent commitFinal 把 assistant 最后输出存 store 不是 user input.
// 所以 Run 1 存的 store value = "你喜欢 Go 语言" (assistant), Run 2 preamble Search
// 注入这条 message → LLM input 含 "Go" 关键词.
type stubModel struct {
	captured []*schema.Message // 记最后一次收到的 messages 供 main 检查
}

func (m *stubModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.captured = msgs
	for _, msg := range msgs {
		if strings.Contains(msg.Content, "Go") {
			return schema.AssistantMessage("你喜欢 Go 语言"), nil
		}
	}
	return schema.AssistantMessage("我还不知道你喜欢什么"), nil
}

func (m *stubModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m *stubModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("v2_store_demo PASS")
}

func run() error {
	ctx := context.Background()

	// 1. 创建共享 store + 业务 namespace
	st := store.NewInMemoryStore()
	userID := "user_alice"
	ns := store.NS("users", userID, "memories")

	// 2. 创建 agent — WithStore 挂 store + WithStoreMemory 启自动 Search/Put
	model := &stubModel{}
	agent := agent_v2.NewBasicAgent(
		agent_v2.WithName("memory_demo"),
		agent_v2.WithModel(model),
		agent_v2.WithStore(st),
		agent_v2.WithStoreMemory(ns, nil), // nil = 默认 SearchLimit=5, 无 filter
	)

	// === Run 1: 告诉 agent "我喜欢 Go" ===
	sess1 := agent_v2.NewRunSession()
	if _, err := agent.Run(ctx, sess1, schema.UserMessage("我喜欢 Go")); err != nil {
		return fmt.Errorf("Run 1: %w", err)
	}
	fmt.Printf("Run 1 done — sess.RunID=%s\n", sess1.RunID)

	// 检查 store 确实保存了 (postamble commitFinal 应已调 store.Put)
	items, searchErr := st.Search(ctx, ns, store.WithQuery("Go"))
	if searchErr != nil {
		return fmt.Errorf("store.Search after Run 1: %w", searchErr)
	}
	if len(items) == 0 {
		return fmt.Errorf("expected store.Put 后有条目, got 0")
	}
	fmt.Printf("Store 已记录 %d 条 memory (仅 assistant output — commitFinal 不存 user input)\n", len(items))

	// === Run 2: 新 session, 同 user — 应能从 store 拿回记忆 ===
	sess2 := agent_v2.NewRunSession()
	if _, err := agent.Run(ctx, sess2, schema.UserMessage("我喜欢什么?")); err != nil {
		return fmt.Errorf("Run 2: %w", err)
	}
	fmt.Printf("Run 2 done — sess.RunID=%s\n", sess2.RunID)

	// 检查 LLM 真看到了 store 注入的历史 (preamble 应已 store.Search 注入)
	// store 内存的是 Run 1 assistant 输出 "你喜欢 Go 语言"
	sawMemory := false
	for _, msg := range model.captured {
		if strings.Contains(msg.Content, "Go") {
			sawMemory = true
			break
		}
	}
	if !sawMemory {
		return fmt.Errorf("expected Run 2 LLM input 含 'Go' memory (preamble Search 应注入), captured %d 条 messages 无命中", len(model.captured))
	}
	fmt.Println("Run 2 LLM 看到了 Run 1 保存的 memory — 跨 Run 长期记忆闭环 OK")

	return nil
}
