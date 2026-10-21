// a2a_smoke 冒烟验证 A2A RemoteAgent 端到端调用真实远程 A2A agent。
//
// 配套 a2a-go 官方 helloworld JSON-RPC server（返 "Hello, world!"）：
//
//	cd <你的 a2a-go checkout>
//	go run ./examples/helloworld/server/jsonrpc -port 19090 &
//	cd <crew-go 根目录>
//	go run ./example/a2a/smoke
//
// 验证：RemoteAgent 解析 AgentCard + 建 client + SendStreamingMessage + 聚合 Message
// 结果——拿到远程的 "Hello, world!"。
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/schema"
)

func main() {
	base := "http://127.0.0.1:19090"
	if len(os.Args) > 1 {
		base = os.Args[1]
	}
	ctx := context.Background()

	// 1. 解析远程 AgentCard + 建 RemoteAgent
	ra, err := a2a.NewRemoteAgent(ctx, base)
	if err != nil {
		log.Fatalf("NewRemoteAgent(%q): %v", base, err)
	}
	fmt.Printf("✓ 解析远程 agent: name=%q desc=%q\n", ra.Name(), ra.Description())

	// 2. 当 agent 直接 Run（委派任务给远程）
	events, err := ra.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if err != nil {
		log.Fatalf("RemoteAgent.Run: %v", err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		log.Fatal("RemoteAgent.Run 返回无 final message")
	}
	fmt.Printf("✓ 远程回复: %q\n", final.Content)
	if final.Content != "Hello, world!" {
		log.Fatalf("意外回复: %q（期望 %q）", final.Content, "Hello, world!")
	}

	// 3. 验证 agent → tool 形态（AgentAsTool 包装）也能调通
	tl, err := a2a.NewRemoteAgentTool(ctx, base)
	if err != nil {
		log.Fatalf("NewRemoteAgentTool: %v", err)
	}
	res, err := tl.Call(ctx, map[string]any{"input": "hi"})
	if err != nil {
		log.Fatalf("RemoteAgentTool.Call: %v", err)
	}
	fmt.Printf("✓ tool 形态结果: %v\n", res.Output)

	fmt.Println("\n=== A2A RemoteAgent 冒烟 PASS（agent 形态 + tool 形态都调通真实远程 A2A agent）===")
}
