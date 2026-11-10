# crew-go 快速上手

> 5 分钟：从跑通 example 到写出第一个带工具的多轮 Agent。深入用法见文末"下一步"。
> 本文只讲最小路径；完整 API 速查见 [migration-history.md](v2/migration-history.md)。

## 环境

- 最低 Go 1.26.0，仓库声明工具链 Go 1.26.4。
- `-mock` 离线运行无需 API key。真模型默认读取 `QWEN_API_KEY`，示例也支持 `-provider=deepseek`，读取 `DEEPSEEK_API_KEY`。

## 0. 先跑通（0 配置感受效果）

```bash
go run ./example/orchestration/basic_chat -mock          # 离线 mock，不调真 LLM，看 ReAct 循环
# 已设置 QWEN_API_KEY 时可用真模型
go run ./example/orchestration/basic_chat
```

在 mock 模式输入「北京天气如何？」可以看到 Agent 调用 `get_weather`。接着问「刚才问的是哪个城市？」可以看到它从历史中找回北京。用真模型时可以试「明天呢？」这样的省略问法。

## 1. 第一个 Agent（最小，纯对话）

```go
package main

import (
	"context"
	"fmt"

	"crew-go/agent_v2"
	"crew-go/model/qwen"
	"crew-go/schema"
)

func main() {
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("assistant"),
		agent_v2.WithModel(qwen.NewTestClient("")), // 读 QWEN_API_KEY，取测试候选首位
	)

	events, err := ag.Run(context.Background(),
		agent_v2.NewRunSession(), schema.UserMessage("一句话介绍你自己"))
	if err != nil {
		panic(err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		panic("agent returned no final message")
	}
	fmt.Println(final.Content)
}
```

`BasicAgent` = ReAct 循环（LLM ↔ 工具）。没工具时就是纯对话；`Run` 返回 `[]*AgentEvent`，`FinalMessage` 取最终 assistant 回复。

## 2. 加工具（ReAct 自动调用）

```go
import "crew-go/tool"

weather := tool.NewFuncTool(
	"get_weather",
	"查询城市天气",
	map[string]any{
		"type": "object",
		"properties": map[string]any{
			"city": map[string]any{"type": "string", "description": "城市名"},
		},
		"required": []string{"city"},
	},
	func(_ context.Context, args map[string]any) (*tool.Result, error) {
		city, _ := args["city"].(string)
		return tool.Success(city + "：晴 25°C"), nil // 失败用 tool.Failure(code, msg)
	},
)

tk := tool.NewToolkit()
tk.Register(weather)

ag := agent_v2.NewBasicAgent(
	agent_v2.WithName("weather_bot"),
	agent_v2.WithSystemPrompt("查天气必须调用 get_weather 工具获取数据再回答"),
	agent_v2.WithModel(qwen.NewTestClient("")),
	agent_v2.WithToolkit(tk),
)
```

LLM 自己决定何时调工具，框架自动跑 ReAct 循环（调工具 → 喂结果回 LLM → 再问）直到出最终答复，业务无需手写循环。

## 3. 多轮对话（上下文自动继承）

`RunSession` 跨轮复用，框架自动把历史拼到每轮 LLM 输入前面：

```go
sess := agent_v2.NewRunSession() // 复用同一个 sess：历史 / 业务 KV 一路攒着
for _, q := range []string{"北京天气？", "明天呢？", "刚才问的是哪个城市？"} {
	sess.NextTask() // 每轮是一个独立任务：换一枚 RunID，history 不受影响
	events, err := ag.Run(context.Background(), sess, schema.UserMessage(q))
	if err != nil {
		panic(err)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil {
		panic("agent returned no final message")
	}
	fmt.Printf("Q: %s\nA: %s\n", q, final.Content)
	// 第 2/3 轮 LLM 看得到前面对话 → 能推断「明天」=北京明天、「刚才」=北京
}
```

> **为什么要 `NextTask()`**：`sess` 复用是为了攒历史，而 `sess.RunID` 是**存档主索引**，
> 契约是「一枚只服务一个任务」。不换的话——某轮 agent 问你要个确认、你没答直接问了别的，
> 下一轮跑完收尾会把上一轮那份挂起存档**静默删掉**（`err == nil`）。两件事正交：复用对象
> 攒状态，换 ID 给每个任务独立存档槽。详见 `RunSession.RunID` 的 godoc。

完整可跑版（含工具实现 + hook 打印 ReAct 过程）见 [`example/orchestration/basic_chat`](../example/orchestration/basic_chat/main.go)。

## 下一步

| 想做的 | 看 |
|---|---|
| **多 Agent 协作**（顺序/并行/循环/路由/规划/反思/状态机/自定义图）| [orchestration-patterns.md](v2/orchestration-patterns.md) — 9 编排选型决策树 + 每个完整最小例 |
| **各功能怎么写**（代码示例索引 → example + 文档）| [sdk-guide.md](sdk-guide.md) |
| **人在回路**（HITL 中断 + Resume）| [hitl-resume.md](v2/hitl-resume.md) |
| **长对话上下文压缩**（4 层）| [context-compaction.md](v2/context-compaction.md) |
| **跨 Run 长期记忆** | [store.md](v2/store.md) |
| **Skill 系统** | [orchestration-patterns.md §11](v2/orchestration-patterns.md) |
| **可观测**（OpenTelemetry trace / 指标）| [observability.md](v2/observability.md) |
| **生命周期 hook / 流式 SSE** | [hooks.md](v2/hooks.md) + [hooks-and-streaming.md](v2/hooks-and-streaming.md) |
| **底层图引擎**（compose_v2 channel-based）| [usage-guide.md](v2/usage-guide.md) + [channel-engine.md](v2/channel-engine.md) |
| **完整 API 速查** | [migration-history.md](v2/migration-history.md) |

可跑 example 全集见 [`example/`](../example/)：`basic_chat`（多轮 ReAct）/ `plan_chat`（Plan-Execute）/ `v2_skill_demo` / `v2_store_demo` / `v2_graphagent_demo` / `nested_smoke`（7 编排嵌套 HITL）/ `observability_demo` 等。
