// a2a_real_sut 把一个真 BasicAgent 暴露成 A2A server，用于对官方 a2a-tck 跑真实合规数。
//
// 区别 a2a_rich_server：后者是 mock executor（只冒烟 Task 状态机 / input-required /
// 流式路径），不是真 agent——拿它跑 conformance 会假性触发 artifact / message-vs-task /
// cancel / subscribe 等场景失败（mock 不实现这些）。本 SUT 走生产 a2a/server Executor +
// 真 BasicAgent，能把「mock 噪声」与「真 gap」分开。
//
// 跑（需 QWEN_API_KEY 环境变量，NewTestClient 读 os.Getenv）：
//
//	QWEN_API_KEY=xxx go run ./example/a2a/real_sut -port 9999
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"

	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/example/llmtrace"
	"crew-go/model"
	"crew-go/model/qwen"
)

func main() {
	port := flag.Int("port", 9999, "监听端口")
	modelName := flag.String("model", "", "qwen 模型名；留空 = 把 qwen.TestModelCandidates 串成备选链（额度耗尽自动切）")
	flag.Parse()

	var chatModel model.ChatModel
	if *modelName != "" {
		chatModel = qwen.NewTestClient(*modelName)
	} else {
		chatModel, _ = llmtrace.NewQwenFallback()
	}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("Real Test Agent"),
		agent_v2.WithDescription("a real BasicAgent exposed via a2a/server for TCK conformance"),
		agent_v2.WithModel(chatModel),
	)

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", *port)
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("bind port %d: %v", *port, err)
	}
	log.Printf("real agent A2A server on %s", baseURL)
	log.Fatal(http.Serve(ln, server.Handler(ag, baseURL, server.WithOwnerResolver(func(context.Context) string { return "demo" }))))
}
