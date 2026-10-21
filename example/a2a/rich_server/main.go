// a2a_rich_server 是一个更丰富的本地 A2A server，用于冒烟 RemoteAgent 的 Task 状态机 /
// input-required HITL / taskID 续跑 / 流式 artifact 路径——helloworld 只返 Message 盖不到
// 这些（CLAUDE.md："build 通过≠能用"，核心路径必须真跑）。
//
// 按入参文本分派 4 种行为：
//   - 续跑（StoredTask != nil，即客户端带已存 task 的 taskID 再发）→ completed "got it: <text>"
//   - 含 "ask"    → 新建 task + input-required（追问），task 留存等续跑
//   - 含 "stream" → working + 两个 artifact chunk（append 同一 artifact）+ completed
//   - 其他        → 新建 task + 直接 completed "task done: <text>"
//
// 配套 example/a2a/hitl_smoke 跑（后者会自动拉起本 server）。
package main

import (
	"context"
	"flag"
	"fmt"
	"iter"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// richExecutor 按入参文本走不同 Task 生命周期路径。
type richExecutor struct{}

var _ a2asrv.AgentExecutor = (*richExecutor)(nil)

func (*richExecutor) Execute(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		text := ""
		if execCtx.Message != nil && len(execCtx.Message.Parts) > 0 {
			text = execCtx.Message.Parts[0].Text()
		}

		// 续跑：客户端带已存 task 的 taskID 再发（input-required 后补输入）→ 直接完成
		if execCtx.StoredTask != nil {
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCompleted,
				a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("got it: "+text))), nil)
			return
		}

		// 新 task：先 submitted 注册（StoredTask==nil 时必须，否则 server 无 task 可挂状态）
		if !yield(a2a.NewSubmittedTask(execCtx, execCtx.Message), nil) {
			return
		}

		switch {
		case strings.Contains(text, "ask"):
			// 追问 → input-required；task 留存，等客户端带 taskID 续跑
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateInputRequired,
				a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("Which color do you prefer?"))), nil)
		case strings.Contains(text, "stream"):
			// 流式：working → 两个 artifact chunk（第二个 append 到第一个）→ completed
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateWorking, nil), nil)
			first := a2a.NewArtifactEvent(execCtx, a2a.NewTextPart("chunk-A "))
			if !yield(first, nil) {
				return
			}
			yield(a2a.NewArtifactUpdateEvent(execCtx, first.Artifact.ID, a2a.NewTextPart("chunk-B")), nil)
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCompleted, nil), nil)
		default:
			// 单轮直接完成
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCompleted,
				a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("task done: "+text))), nil)
		}
	}
}

func (*richExecutor) Cancel(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {}
}

var port = flag.Int("port", 19091, "port for the rich A2A server")

func main() {
	flag.Parse()
	addr := fmt.Sprintf("http://127.0.0.1:%d/invoke", *port)
	card := &a2a.AgentCard{
		Name:        "Rich Test Agent",
		Description: "A test agent exercising Task / input-required / streaming paths",
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(addr, a2a.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"text"},
		DefaultOutputModes: []string{"text"},
		Capabilities:       a2a.AgentCapabilities{Streaming: true},
		Skills:             []a2a.AgentSkill{}, // A2A 1.0 schema 要求 skills 为数组（非 null）
	}
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("bind port %d: %v", *port, err)
	}
	log.Printf("rich A2A server listening on 127.0.0.1:%d", *port)
	h := a2asrv.NewHandler(&richExecutor{})
	mux := http.NewServeMux()
	jsonrpcHandler := a2asrv.NewJSONRPCHandler(h)
	mux.Handle("/invoke", jsonrpcHandler)
	mux.Handle("/invoke/", jsonrpcHandler) // 容忍尾斜杠（官方 a2a-tck 带斜杠）
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	log.Printf("server stopped: %v", http.Serve(listener, mux))
}
