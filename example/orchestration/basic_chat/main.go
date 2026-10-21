// basic_chat 演示 BasicAgent 的多轮对话能力（v2）：
// 同一个 *agent_v2.RunSession 被循环复用，框架自动在每轮把 session.History()
// 拼到 LLM 输入前面，于是 LLM 能看到此前所有对话，无需调用方手动维护消息数组。
//
// 运行 (默认 qwen, 跟 CLAUDE.md "测试 LLM 默认 qwen" 对齐):
//
//	QWEN_API_KEY=<your-key> go run ./example/orchestration/basic_chat
//	# 或切 deepseek
//	DEEPSEEK_API_KEY=<your-key> go run ./example/orchestration/basic_chat -provider=deepseek
//	# 或离线 mock 模式 (不调真 LLM, 用脚本化响应演示)
//	go run ./example/orchestration/basic_chat -mock
//
// 输入 /exit 或 Ctrl-D 退出。
//
// 推荐对话脚本（体现"上下文继承"）：
//  1. 北京天气如何？              → agent 调用 get_weather(city="北京")
//  2. 明天呢？                    → agent 从历史推断城市，直接 get_weather(city="北京", date="tomorrow")
//  3. 那上海呢？                  → agent 推断仍问"明天"，get_weather(city="上海", date="tomorrow")
//  4. 刚才第一个城市叫什么？      → agent 从 history 中读出"北京"并回答
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
	"crew-go/tool"
)

func main() {
	useMock := flag.Bool("mock", false, "离线 mock 模式（不调真 LLM）")
	provider := flag.String("provider", "qwen", "真 LLM provider: qwen (默认, dashscope, 需 QWEN_API_KEY) / deepseek (需 DEEPSEEK_API_KEY); 仅 -mock=false 生效")
	flag.Parse()

	// 自动加载项目根 .env，使得 `go run ./example/orchestration/basic_chat` 无需手动 export 环境变量。
	envloader.Load()

	var chatModel model.ChatModel
	if *useMock {
		chatModel = newMockWeatherModel()
		log.Printf("[basic_chat] mock 模式：不调真 LLM")
	} else {
		switch *provider {
		case "qwen":
			if os.Getenv("QWEN_API_KEY") == "" {
				log.Fatal("QWEN_API_KEY 未设置 (.env 加 QWEN_API_KEY=xxx); export 后再运行, 或 -mock / -provider=deepseek")
			}
			chatModel, _ = llmtrace.NewQwenFallback()
			log.Printf("[basic_chat] qwen 备选链模式 (dashscope; 候选取 qwen.TestModelCandidates, 额度耗尽自动切)")
		case "deepseek":
			apiKey := os.Getenv("DEEPSEEK_API_KEY")
			if apiKey == "" {
				log.Fatal("DEEPSEEK_API_KEY 未设置; export 后再运行, 或 -mock / -provider=qwen")
			}
			chatModel = deepseek.NewClient(apiKey, "")
			log.Printf("[basic_chat] deepseek 模式")
		default:
			log.Fatalf("未知 -provider %q (支持 qwen / deepseek)", *provider)
		}
	}

	ag := buildAgent(chatModel)
	sess := agent_v2.NewRunSession() // 跨轮复用的会话对象：values / events 一路攒着

	fmt.Println("────────────────────────────────────────")
	fmt.Println(" BasicAgent 多轮对话 demo（天气助手）")
	fmt.Println(" 输入 /exit 或 Ctrl-D 退出")
	fmt.Println("────────────────────────────────────────")

	sc := bufio.NewScanner(os.Stdin)
	// 允许长输入。
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

		// 每一轮用户输入是一个独立任务：换一枚 RunID（见 RunSession.RunID 契约）。
		// 复用对象是为了攒 history，换 ID 是为了让每个任务有自己的存档槽——两件事正交。
		sess.NextTask()
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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

// buildAgent 构造一个带 get_weather 工具的 BasicAgent。
func buildAgent(chatModel model.ChatModel) agent_v2.Agent {
	weather := tool.NewFuncTool(
		"get_weather",
		"查询指定城市的天气；date 可选，默认 today。",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"city": map[string]any{"type": "string", "description": "城市名，例如 北京"},
				"date": map[string]any{"type": "string", "description": "today / tomorrow，缺省 today"},
			},
			"required": []string{"city"},
		},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			city, _ := args["city"].(string)
			date, _ := args["date"].(string)
			if date == "" {
				date = "today"
			}
			// 假数据表，足够演示上下文继承。
			db := map[string]map[string]string{
				"北京": {"today": "晴 12°C", "tomorrow": "多云 10°C"},
				"上海": {"today": "小雨 18°C", "tomorrow": "阴 16°C"},
				"广州": {"today": "雷阵雨 26°C", "tomorrow": "多云 25°C"},
			}
			row, ok := db[city]
			if !ok {
				return tool.Failure("not_found", fmt.Sprintf("暂无 %s 的天气数据", city)), nil
			}
			val, ok := row[date]
			if !ok {
				return tool.Failure("not_found", fmt.Sprintf("%s 无 %s 数据", city, date)), nil
			}
			return tool.Success(fmt.Sprintf("%s %s：%s", city, date, val)), nil
		},
	)
	tk := tool.NewToolkit()
	tk.Register(weather)

	return agent_v2.NewBasicAgent(
		agent_v2.WithName("weather_bot"),
		agent_v2.WithSystemPrompt(`你是天气助手。当用户询问天气时必须调用 get_weather 工具获取真实数据，再组织语言回答。多轮对话中若用户使用"明天"/"后天"/省略城市等指代，应结合上下文推断参数。`),
		agent_v2.WithModel(chatModel),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHooks(traceHook{}),
	)
}

// traceHook 打印每轮 LLM 调用与工具调用的关键信息，便于直观感受 ReAct 循环。
type traceHook struct{ hook.NoOpHooks }

func (traceHook) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	// in.Tools 在 OnModelStart 框架 by design 恒 nil（工具绑定在 tools 节点、不经此 hook），
	// 打印它恒 0 会误导，故不打印。
	fmt.Printf("  [llm→] agent=%s round=%d msgs=%d\n",
		info.Name, info.Round, len(in.Messages))
	return ctx, nil
}

func (traceHook) OnToolStart(ctx context.Context, info *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	fmt.Printf("  [tool→] %s args=%s\n", in.Name, in.ArgumentsJSON)
	return ctx, nil
}

func (traceHook) OnToolEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out.Result == nil {
		return ctx, nil
	}
	if out.Result.Success {
		fmt.Printf("  [tool←] ok: %s\n", out.Result.Output)
	} else {
		fmt.Printf("  [tool←] fail(%s): %s\n", out.Result.ErrorCode, out.Result.Error)
	}
	return ctx, nil
}
