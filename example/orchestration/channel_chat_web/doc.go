// channel_chat_web 演示 chat_entry 模式的 SSE web 版：单一 BasicAgent 入口 + 8 scenario
// PlanAgent 包成 AgentAsTool，LLM 按用户输入自动路由，事件经 SSE 实时推前端。
//
// chat_entry 模式（vs 旧的 dropdown 选 scenario）：
//
//   - 用户跟 chat_entry 自然对话，无需选 scenario
//   - 简单查询 chat_entry 直接回答；复杂任务自动调对应 scenario tool
//   - chat_entry → AgentAsTool(PlanAgent) → BasicAgent.ask_user → Resume HITL 全闭环
//   - cp ns 自动派生 (sid, "tools/<scenario_name>/...")
//
// SSE 实时推送的事件（演示 run_id 可观测性）：
//
//   - 图生命周期（OrchHooks.OnGraphStart/End/NodeStart/End）
//   - LLM 调用（hook.Hooks.OnModelStart/End）：带 run_id，可与 assistant 消息 Annotations 对齐
//   - 工具调用（hook.Hooks.OnToolStart/End）：含 chat_entry 调 scenario tool 与 scenario
//     内部 sub-agent 调真实 tool 两层
//   - 嵌套 cp 路径（AskQuery.InterruptID 按 5 维含 graphNS）：HITL 中断时显示完整 ns 路径
//
// 使用方法：
//
//	# 真 LLM（默认 qwen，需 QWEN_API_KEY；或 -provider=deepseek 需 DEEPSEEK_API_KEY）
//	go run ./example/orchestration/channel_chat_web -addr=:8080
//
//	# 离线 mock（外部服务不可用时也能演示）
//	go run ./example/orchestration/channel_chat_web -addr=:8080 -mock
//
// 打开 http://localhost:8080，在文本框输入任务后回车，右侧事件流实时更新。
//
// 本 demo 仅为观测性演示，未做生产级 SSE 细节（断线续传 / Last-Event-ID / 心跳 /
// backpressure 保护）。
package main
