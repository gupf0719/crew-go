// execution_control_smoke 冒烟执行控制的四项能力：
//
//	① 声明式断点：GraphAgent 逐步执行（确定性，不调模型）
//	② 只读放行 + 审批方式写进提示词：真 qwen——只读查询直接执行，退款停下来等批准
//	③ 运行中插话：真 qwen——工具执行期间用户补一句「全部用英文回答」，最终答复是英文
//	④ 回合结束闸门：真 qwen——答复不以「【结论】」开头就打回，模型按反馈重写
//
// 跑（项目根 .env 需 QWEN_API_KEY；约 6~10 次模型调用、1 分钟左右）：
//
//	go run ./example/harness/execution_control_smoke
//
// 模型用 llmtrace.NewQwenFallback() 的 qwen 备选链（候选取 qwen.TestModelCandidates），结束时打印哪个模型真
// 服务了、用了多少 token。②~④ 的断言落在框架行为上（工具执行次数、有没有停下来审批、闸门问了几次）；
// 「答复是不是英文」「是不是以【结论】开头」两处看模型是否听从了插话 / 反馈，失败时打印答复便于判断。
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

var tracer *llmtrace.Tracer

func main() {
	envloader.Load()
	m, tr := llmtrace.NewQwenFallback()
	tracer = tr

	stepThroughBreakpoints()
	approvalAskWrites(m)
	interjection(m)
	turnGate(m)

	fmt.Println(tracer.Report())
	fmt.Println("\n=== 全部通过 ===")
}

// ① GraphAgent 配 AllNodes 断点：每个超步边界停一次，每次 Resume 恰好越过一个边界。
func stepThroughBreakpoints() {
	fmt.Println("=== ① 声明式断点：逐步执行 ===")
	inCh := compose_v2.LastValue[*schema.Message]("__ecs_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__ecs_out__")
	var trail []string
	bld := compose_v2.NewBuilder().WithName("ecs_steps")
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	for _, n := range []string{"fetch", "transform", "publish"} {
		name := n
		bld.AddNode(compose_v2.NodeID(name), func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			trail = append(trail, name)
			in, _ := compose_v2.Get(chs, inCh)
			return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(in.Content+" -> "+name))}, nil
		})
	}
	bld.AddEdge(compose_v2.StartNode, "fetch")
	bld.AddEdge("fetch", "transform")
	bld.AddEdge("transform", "publish")
	bld.AddEdge("publish", compose_v2.EndNode)
	g, err := bld.Compile()
	must(err)
	ga := agent_v2.NewGraphAgent(g,
		agent_v2.WithGraphAgentName("ecs_steps"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
		agent_v2.WithGraphInterruptBefore(compose_v2.AllNodes),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err = ga.Run(ctx, sess, schema.UserMessage("job"))
	for stops := 0; ; stops++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			must(err)
			break
		}
		q := ie.AllQueries()[0]
		if q.Breakpoint == nil {
			log.Fatalf("该是断点停下，实际 %+v", q)
		}
		fmt.Printf("  停在 %v 之前（已执行 %v）\n", q.Breakpoint.Before, trail)
		if len(trail) != stops {
			log.Fatalf("第 %d 次停下时该执行过 %d 个节点，实际 %v", stops+1, stops, trail)
		}
		_, err = ga.Resume(ctx, sess.RunID)
	}
	if strings.Join(trail, ",") != "fetch,transform,publish" {
		log.Fatalf("执行顺序不对：%v", trail)
	}
	fmt.Println("  PASS")
}

// ② 只读放行：query_order 声明只读、直接执行；refund_order 没声明，执行前停下来等批准。
func approvalAskWrites(m model.ChatModel) {
	fmt.Println("=== ② 只读放行 + 审批方式写进提示词 ===")
	var queries, refunds atomic.Int32
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("query_order", "查询订单详情（金额、状态）",
		map[string]any{"type": "object", "properties": map[string]any{"order_id": map[string]any{"type": "string"}}, "required": []string{"order_id"}},
		func(context.Context, map[string]any) (*tool.Result, error) {
			queries.Add(1)
			return tool.Success(map[string]any{"order_id": "A1001", "amount": 99, "status": "已签收"}), nil
		}, tool.WithReadOnly()))
	tk.Register(tool.NewFuncTool("refund_order", "给订单退款",
		map[string]any{"type": "object", "properties": map[string]any{
			"order_id": map[string]any{"type": "string"}, "amount": map[string]any{"type": "number"}}, "required": []string{"order_id", "amount"}},
		func(context.Context, map[string]any) (*tool.Result, error) {
			refunds.Add(1)
			return tool.Success("退款已提交"), nil
		}))
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ecs_support"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(6),
		agent_v2.WithSystemPrompt("你是电商客服助手，用工具办理用户的请求。"),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAskWrites),
		agent_v2.WithHooks(tracer.Hooks()),
	)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	events, err := ag.Run(ctx, sess, schema.UserMessage("先查一下订单 A1001，然后按订单金额给它全额退款。"))
	approvals := 0
	for {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			must(err)
			break
		}
		q := ie.AllQueries()[0]
		if q.Reason != agent_v2.ApprovalInterruptReason || q.ToolName != "refund_order" {
			log.Fatalf("只该为 refund_order 停下来审批，实际 %+v", q)
		}
		approvals++
		fmt.Printf("  停下来审批：%s（此时 query_order 已执行 %d 次、refund_order %d 次）\n", q.Question, queries.Load(), refunds.Load())
		if refunds.Load() != 0 {
			log.Fatal("批准之前 refund_order 不该执行")
		}
		events, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			q.InterruptID: schema.UserMessage(agent_v2.ApprovalOptionApprove)}))
	}
	if queries.Load() < 1 || approvals != 1 || refunds.Load() != 1 {
		log.Fatalf("query_order 至少执行 1 次（不审批）、refund_order 审批 1 次后执行 1 次，实际 query=%d approvals=%d refund=%d"+
			"（approvals=0 多半是模型在文字里征求同意而没调工具——审批提示词没起作用）",
			queries.Load(), approvals, refunds.Load())
	}
	fmt.Printf("  最终答复：%s\n  PASS\n", oneLine(agent_v2.FinalMessage(events)))
}

// ③ 运行中插话：research 工具跑到一半时用户补一句「全部用英文回答」。
func interjection(m model.ChatModel) {
	fmt.Println("=== ③ 运行中插话 ===")
	inbox := agent_v2.NewInbox()
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("research", "检索一个主题的资料",
		map[string]any{"type": "object", "properties": map[string]any{"topic": map[string]any{"type": "string"}}, "required": []string{"topic"}},
		func(context.Context, map[string]any) (*tool.Result, error) {
			_ = inbox.Push(schema.UserMessage("补充一句：最终答复请全部用英文写。")) // 用户在工具跑着的时候插话
			time.Sleep(200 * time.Millisecond)
			return tool.Success("咖啡起源于埃塞俄比亚，15 世纪在也门的苏菲派修道院流行，17 世纪传入欧洲。"), nil
		}))
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ecs_researcher"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
		agent_v2.WithSystemPrompt("你是研究助手：先用 research 工具查资料，再回答。"),
		agent_v2.WithHooks(tracer.Hooks()),
	)
	events, err := ag.Run(agent_v2.WithInboxCtx(context.Background(), inbox), agent_v2.NewRunSession(),
		schema.UserMessage("研究一下咖啡的历史，用两句话回答。"))
	must(err)
	final := agent_v2.FinalMessage(events)
	if inbox.Len() != 0 {
		log.Fatalf("插话该被取走，收件箱还剩 %d 条", inbox.Len())
	}
	if final == nil || hasCJK(final.Content) {
		log.Fatalf("最终答复该是英文（模型该看到了插话），实际：%s", oneLine(final))
	}
	fmt.Printf("  最终答复：%s\n  PASS\n", oneLine(final))
}

// ④ 回合结束闸门：答复必须以「【结论】」开头，不是就打回。
func turnGate(m model.ChatModel) {
	fmt.Println("=== ④ 回合结束闸门 ===")
	var gateCalls atomic.Int32
	gate := func(_ context.Context, in agent_v2.TurnGateInput) (string, error) {
		gateCalls.Add(1)
		if strings.HasPrefix(strings.TrimSpace(in.Final.Content), "【结论】") {
			return "", nil
		}
		return "答复必须以「【结论】」四个字开头，请按这个格式把刚才的答复重写一遍。", nil
	}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ecs_gated"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(4),
		agent_v2.WithSystemPrompt("你是助手。"),
		agent_v2.WithTurnGate(gate),
		agent_v2.WithHooks(tracer.Hooks()),
	)
	events, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("一句话介绍 Go 语言。"))
	must(err)
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.HasPrefix(strings.TrimSpace(final.Content), "【结论】") {
		log.Fatalf("闸门放行的答复该以【结论】开头，实际（闸门问了 %d 次）：%s", gateCalls.Load(), oneLine(final))
	}
	fmt.Printf("  闸门问了 %d 次，最终答复：%s\n  PASS\n", gateCalls.Load(), oneLine(final))
}

func hasCJK(s string) bool {
	for _, r := range s {
		if unicode.Is(unicode.Han, r) {
			return true
		}
	}
	return false
}

func oneLine(m *schema.Message) string {
	if m == nil {
		return "<nil>"
	}
	return strings.ReplaceAll(m.Content, "\n", " ")
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
