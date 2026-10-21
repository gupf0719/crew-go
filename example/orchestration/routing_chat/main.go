// routing_chat 演示 RoutingAgent 多轮对话 + 跨轮路由感知（ISSUE-33 模式 5）。
//
// classifier 按用户问题路由到 math / code 专家。多轮：轮 1 路由 math、轮 2 路由 code、轮 3 用户
// 要"结合前两轮"——classifier 从 session 历史看到前两轮上下文做路由决策，命中的专家也从历史
// 读到前两轮内容综合作答，体现"跨轮路由感知"（不同轮路由到不同专家，后轮能引用前轮结果）。
//
// 运行（默认 qwen，需项目根 .env 的 QWEN_API_KEY）：
//
//	go run ./example/orchestration/routing_chat
//	go run ./example/orchestration/routing_chat -provider=deepseek
//
// 输入 /exit 或 Ctrl-D 退出。推荐脚本（体现跨轮路由）：
//  1. 100 以内有多少个质数？                        → 路由到 math 专家
//  2. 写个 Go 函数判断一个数是不是质数               → 路由到 code 专家
//  3. 把前面的结果和代码整理成一段给同事看的说明      → classifier 读历史路由，专家综合前两轮
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

	ag := buildRouting(pickModel(*provider))
	sess := agent_v2.NewRunSession()

	fmt.Println("────────────────────────────────────────")
	fmt.Println(" RoutingAgent 多轮 demo（classifier 跨轮路由 → math / code 专家）")
	fmt.Println(" 脚本: ①100以内有多少个质数 ②写个Go函数判断质数 ③把前面的结果和代码整理成一段说明")
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
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
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

// buildRouting 构造 classifier + math / code 专家 + general fallback 的 RoutingAgent。
func buildRouting(m model.ChatModel) agent_v2.Agent {
	classifier := agent_v2.NewBasicAgent(
		agent_v2.WithName("classifier"),
		agent_v2.WithSystemPrompt(
			"你是路由分类器。结合对话历史和当前问题，只输出一个目标分支名（math 或 code），不加任何其他内容。"+
				"用户说'结合前面'/'整理一下'等需要综合的，按其中最主要的产出形态选分支（要文字结论选 math，要代码选 code）。",
		),
		agent_v2.WithModel(m),
	)
	mathExpert := agent_v2.NewBasicAgent(
		agent_v2.WithName("math"),
		agent_v2.WithSystemPrompt("你是数学专家。给出计算过程和结果。结合历史里之前的问题/结论保持上下文连续。"),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(traceHook{}),
	)
	codeExpert := agent_v2.NewBasicAgent(
		agent_v2.WithName("code"),
		agent_v2.WithSystemPrompt("你是 Go 工程师。给出带注释的代码。结合历史里之前的问题/结果保持上下文连续。"),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(traceHook{}),
	)
	general := agent_v2.NewBasicAgent(
		agent_v2.WithName("general"),
		agent_v2.WithSystemPrompt("你是通用助手，综合对话历史给出整理性的回答。"),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(traceHook{}),
	)
	return agent_v2.NewRoutingAgent(
		agent_v2.WithRoutingName("expert_router"),
		agent_v2.WithRoutingClassifier(classifier),
		agent_v2.WithRoutingFallback(general),
		agent_v2.WithRoutingDescriptions(map[string]string{
			"math": "数学计算、数论、统计等问题",
			"code": "写代码、调试、重构等编程问题",
		}),
	).
		AddBranch("math", mathExpert).
		AddBranch("code", codeExpert)
}

// pickModel 按 provider 选真 LLM（默认 qwen，对齐 CLAUDE.md "测试 LLM 默认 qwen"）。
func pickModel(provider string) model.ChatModel {
	switch provider {
	case "qwen":
		if os.Getenv("QWEN_API_KEY") == "" {
			log.Fatal("QWEN_API_KEY 未设置（.env 加 QWEN_API_KEY=xxx），或用 -provider=deepseek")
		}
		log.Printf("[routing_chat] qwen 备选链模式 (dashscope; 候选取 qwen.TestModelCandidates, 额度耗尽自动切)")
		m, _ := llmtrace.NewQwenFallback()
		return m
	case "deepseek":
		if os.Getenv("DEEPSEEK_API_KEY") == "" {
			log.Fatal("DEEPSEEK_API_KEY 未设置")
		}
		log.Printf("[routing_chat] deepseek 模式")
		return deepseek.NewClient(os.Getenv("DEEPSEEK_API_KEY"), "")
	default:
		log.Fatalf("未知 -provider %q（支持 qwen / deepseek）", provider)
		return nil
	}
}

// traceHook 打印命中的专家分支 LLM 调用（classifier 不挂，避免噪声），看清每轮路由到谁。
type traceHook struct{ hook.NoOpHooks }

func (traceHook) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	fmt.Printf("  [llm→] 专家=%s round=%d msgs=%d\n", info.Name, info.Round, len(in.Messages))
	return ctx, nil
}
