// reflect_chat 演示 ReflectAgent 多轮对话 + 用户反馈驱动迭代（ISSUE-33 模式 4）。
//
// actor 撰写方案 → evaluator 评审（通过只输出 approved）→ 不过则 reflector 给改进意见、actor
// 重写（ReflectAgent 内部 maxRounds 循环）。多轮：用户下一轮反馈"太复杂"，actor 从 session
// 历史读到上一版方案 + 新反馈，重新生成简化版，evaluator 再评——体现"用户参与迭代"。
//
// 运行（默认 qwen，需项目根 .env 的 QWEN_API_KEY）：
//
//	go run ./example/orchestration/reflect_chat
//	go run ./example/orchestration/reflect_chat -provider=deepseek
//
// 输入 /exit 或 Ctrl-D 退出。推荐脚本（体现用户反馈迭代）：
//  1. 帮我设计一个秒杀系统的技术方案                          → actor 写方案，evaluator 评审
//  2. 太复杂了，我只是个日活几百的小程序，给我最简单能用的就行  → actor 读历史里上一版，重做简化版
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

	ag := buildReflect(pickModel(*provider))
	sess := agent_v2.NewRunSession()

	fmt.Println("────────────────────────────────────────")
	fmt.Println(" ReflectAgent 多轮 demo（actor 写方案 ↔ evaluator 评审 ↔ reflector 改进）")
	fmt.Println(" 脚本: ①帮我设计一个秒杀系统的技术方案 ②太复杂了我只是个日活几百的小程序给我最简单的就行")
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
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
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

// buildReflect 构造 actor（写方案）+ evaluator（评审）+ reflector（改进）三角的 ReflectAgent。
func buildReflect(m model.ChatModel) agent_v2.Agent {
	actor := agent_v2.NewBasicAgent(
		agent_v2.WithName("solution_writer"),
		agent_v2.WithSystemPrompt(
			"你是方案架构师。根据用户需求写技术方案，含技术选型 + 关键流程，篇幅适度。"+
				"收到改进意见或用户反馈（如'太复杂'）时，结合历史里的上一版方案针对性修改，不要从零重写。",
		),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(traceHook{}),
	)
	evaluator := agent_v2.NewBasicAgent(
		agent_v2.WithName("reviewer"),
		agent_v2.WithSystemPrompt(
			"你是方案评审员。方案合理且匹配用户诉求（含复杂度诉求）时，只输出一个词 approved（小写、无其他文字）；"+
				"否则列出 1-2 条具体不足，不要出现 approved 字样。",
		),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(traceHook{}),
	)
	reflector := agent_v2.NewBasicAgent(
		agent_v2.WithName("coach"),
		agent_v2.WithSystemPrompt(
			"你是改进教练。根据评审意见给 1-2 条具体可操作的修改方向，精准、不泛泛。",
		),
		agent_v2.WithModel(m),
		agent_v2.WithHooks(traceHook{}),
	)
	return agent_v2.NewReflectAgent(
		agent_v2.WithReflectName("solution_review"),
		agent_v2.WithEvaluator(evaluator),
		agent_v2.WithReflectorAgent(reflector),
		agent_v2.WithReflectMaxRounds(3),
	).WithActor(actor)
}

// pickModel 按 provider 选真 LLM（默认 qwen，对齐 CLAUDE.md "测试 LLM 默认 qwen"）。
func pickModel(provider string) model.ChatModel {
	switch provider {
	case "qwen":
		if os.Getenv("QWEN_API_KEY") == "" {
			log.Fatal("QWEN_API_KEY 未设置（.env 加 QWEN_API_KEY=xxx），或用 -provider=deepseek")
		}
		log.Printf("[reflect_chat] qwen 备选链模式 (dashscope; 候选取 qwen.TestModelCandidates, 额度耗尽自动切)")
		m, _ := llmtrace.NewQwenFallback()
		return m
	case "deepseek":
		if os.Getenv("DEEPSEEK_API_KEY") == "" {
			log.Fatal("DEEPSEEK_API_KEY 未设置")
		}
		log.Printf("[reflect_chat] deepseek 模式")
		return deepseek.NewClient(os.Getenv("DEEPSEEK_API_KEY"), "")
	default:
		log.Fatalf("未知 -provider %q（支持 qwen / deepseek）", provider)
		return nil
	}
}

// traceHook 打印 actor / evaluator / reflector 各自的 LLM 调用，看清反思迭代。
type traceHook struct{ hook.NoOpHooks }

func (traceHook) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	fmt.Printf("  [llm→] agent=%s round=%d msgs=%d\n", info.Name, info.Round, len(in.Messages))
	return ctx, nil
}
