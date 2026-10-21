// handoff_chat 演示 HandoffAgent 多轮对话 + 跨 handoff 上下文保持（ISSUE-33 模式 3）。
//
// 模型 B（transfer_to_agent 工具自主转交）：L1 客服处理行政咨询，遇到具体技术故障调
// transfer_to_agent 转给 L2 技术专家。同一个 *RunSession 跨轮复用，框架自动把 session.History()
// 拼进每轮 LLM 输入——于是 transfer 之后 L2 能从历史读到此前 L1 轮的上下文（型号 / 现象），
// 体现"跨 handoff 上下文保持"。
//
// 运行（默认 qwen，需项目根 .env 的 QWEN_API_KEY）：
//
//	go run ./example/orchestration/handoff_chat
//	go run ./example/orchestration/handoff_chat -provider=deepseek
//
// 输入 /exit 或 Ctrl-D 退出。推荐脚本（体现跨 handoff 上下文）：
//  1. 你们几点上班？                                  → L1 直接答（行政咨询，不转交）
//  2. 我的 RX-3000 路由器一直断流，固件刷到最新还是不行   → L1 转交 L2（具体技术故障）
//  3. 刚才我说的那个型号你还记得吗？                    → L2 从历史读出"RX-3000"，证明跨 handoff 上下文保持
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/model/deepseek"
	"crew-go/schema"
)

func main() {
	provider := flag.String("provider", "qwen", "真 LLM provider: qwen (默认, dashscope, 需 QWEN_API_KEY) / deepseek (需 DEEPSEEK_API_KEY)")
	flag.Parse()
	envloader.Load()

	ag := buildHandoff(pickModel(*provider))
	sess := agent_v2.NewRunSession() // 跨轮复用的会话对象：values / events 一路攒着

	// **声明会话身份**——持久 active（「现在归 L2 专家接待」）是会话级状态，键取自
	// ThreadID；不声明的话它退化成按 RunID 存，而 RunID 每个任务一枚（下面 NextTask），
	// 于是每轮都退回 L1。见 doc/v2/handoff.md §8。
	convID := "handoff_chat:" + time.Now().Format("20060102T150405")

	fmt.Println("────────────────────────────────────────")
	fmt.Println(" HandoffAgent 多轮 demo（L1 客服 ↔ L2 技术专家）")
	fmt.Println(" 脚本: ①你们几点上班 ②我的 RX-3000 路由器一直断流刷了固件也没用 ③刚才那型号你还记得吗")
	fmt.Println(" /exit 或 Ctrl-D 退出")
	fmt.Println("────────────────────────────────────────")

	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	fmt.Print("\nyou> ")
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			fmt.Print("you> ")
			continue
		}
		if line == "/exit" || line == "/quit" {
			fmt.Println("bye")
			return
		}
		// 每一轮用户输入是一个独立任务：换一枚 RunID（values / events 不受影响）。
		// 不换的话，某轮被中断后用户改问别的，下一轮收尾会把上一轮的挂起存档删掉。
		sess.NextTask()
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		ctx = compose_v2.WithThreadIDCtx(ctx, convID)
		events, err := ag.Run(ctx, sess, schema.UserMessage(line))
		cancel()
		if err != nil {
			fmt.Printf("[error] %v\n", err)
			fmt.Print("\nyou> ")
			continue
		}
		if msg := agent_v2.FinalMessage(events); msg != nil {
			fmt.Printf("\nagent> %s\n", msg.Content)
		}
		fmt.Printf("[history size: %d]\n\nyou> ", len(sess.History()))
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("读取输入失败：%v", err)
	}
	fmt.Println()
}

// buildHandoff 构造 L1 客服 + L2 技术专家的 HandoffAgent（L1 配 transfer 工具能转交，L2 是终点）。
func buildHandoff(m model.ChatModel) agent_v2.Agent {
	l1 := agent_v2.NewBasicAgent(
		agent_v2.WithName("l1_support"),
		agent_v2.WithSystemPrompt(
			"你是 L1 客服。营业时间、地址、退换货政策等行政咨询直接简短回答。"+
				"一旦用户描述具体技术故障（设备型号 + 故障现象 / 报错 / 自助排查无效），"+
				"必须调用 transfer_to_agent 工具（参数 agent_name=\"l2_tech\"）转给 L2 技术专家，"+
				"不要自己编技术方案。",
		),
		agent_v2.WithModel(m),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithHooks(traceHook{}),
	)
	l2 := agent_v2.NewBasicAgent(
		agent_v2.WithName("l2_tech"),
		agent_v2.WithSystemPrompt(
			"你是 L2 技术专家。结合用户在之前对话里提到的设备型号和故障现象（从历史里读），"+
				"给出具体排查步骤。用户问及之前提过的型号时要能准确复述，体现上下文连续。",
		),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(traceHook{}),
	)
	return agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("support_desk"),
		agent_v2.WithHandoffInitial("l1_support"),
		agent_v2.WithHandoffMaxHops(4),
	).
		AddAgent("l1_support", l1).
		AddAgent("l2_tech", l2)
}

// pickModel 按 provider 选真 LLM（默认 qwen，对齐 CLAUDE.md "测试 LLM 默认 qwen"）。
func pickModel(provider string) model.ChatModel {
	switch provider {
	case "qwen":
		if os.Getenv("QWEN_API_KEY") == "" {
			log.Fatal("QWEN_API_KEY 未设置（.env 加 QWEN_API_KEY=xxx），或用 -provider=deepseek")
		}
		log.Printf("[handoff_chat] qwen 备选链模式 (dashscope; 候选取 qwen.TestModelCandidates, 额度耗尽自动切)")
		m, _ := llmtrace.NewQwenFallback()
		return m
	case "deepseek":
		if os.Getenv("DEEPSEEK_API_KEY") == "" {
			log.Fatal("DEEPSEEK_API_KEY 未设置")
		}
		log.Printf("[handoff_chat] deepseek 模式")
		return deepseek.NewClient(os.Getenv("DEEPSEEK_API_KEY"), "")
	default:
		log.Fatalf("未知 -provider %q（支持 qwen / deepseek）", provider)
		return nil
	}
}

// traceHook 打印每轮哪个 agent 在调 LLM / 调工具（含 transfer_to_agent 转交），让终端能看清控制权流转。
type traceHook struct{ hook.NoOpHooks }

func (traceHook) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	fmt.Printf("  [llm→] agent=%s round=%d msgs=%d\n", info.Name, info.Round, len(in.Messages))
	return ctx, nil
}

func (traceHook) OnToolStart(ctx context.Context, info *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	fmt.Printf("  [tool→] %s args=%s（%s 发起）\n", in.Name, in.ArgumentsJSON, info.Name)
	return ctx, nil
}
