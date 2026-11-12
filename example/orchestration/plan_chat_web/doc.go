// plan_chat_web 演示 ISSUE-71 接口合并后的 chat_entry 模式：单一 BasicAgent
// 入口 + 8 scenario PlanAgent 包成 AgentAsTool，LLM 按用户输入自动路由。
//
// 与 ISSUE-71 之前的 dropdown 选 scenario 模式相比：
//
//   - 用户跟 chat_entry 自然对话，无需选 scenario
//   - 简单查询 chat_entry 直接回答；复杂任务自动调对应 scenario tool
//   - chat_entry → AgentAsTool(PlanAgent) → BasicAgent.ask_user → Resume HITL 全闭环
//   - cp ns 自动派生 (sid, "tools/<scenario_name>/...")
//
// SSE 实时推送的事件包括（同时也演示 ISSUE-49 P0：run_id 可观测性）：
//
//   - 图生命周期（OrchHooks.OnGraphStart/End/NodeStart/End）
//   - LLM 调用（hook.Hooks.OnModelStart/End）：带 run_id，可与 assistant 消息 Annotations 对齐
//   - 工具调用（hook.Hooks.OnToolStart/End）：含 chat_entry 调 scenario tool 与
//     scenario 内部 sub-agent 调真实 tool 两层
//   - 嵌套 cp 路径（query.NS）：HITL 中断时显示完整 ns 路径
//
// 使用方法：
//
//	# 默认 qwen（需 QWEN_API_KEY）；或 -provider=deepseek（需 DEEPSEEK_API_KEY）
//	go run ./example/orchestration/plan_chat_web -addr=:8080
//
//	# 离线 mock（外部服务不可用时也能演示）
//	go run ./example/orchestration/plan_chat_web -addr=:8080 -mock
//
// 打开 http://localhost:8080，在文本框输入任务后回车，右侧事件流实时更新。
//
// # BasicAgent 级功能演示 flag（默认关，按需开一个或多个）
//
//   - -feat-tool-filter：ToolFilter——chat_entry 按 mode 裁工具（normal→ask_* 单步专家 /
//     plan→scenario 重型流程），框架工具恒豁免
//   - -feat-prompt-template：PromptTemplate——chat_entry sysPrompt 参数化模板（注入 mode + 工具数）
//   - -feat-tool-search：ToolSearch——scenario 重型工具 deferred，chat_entry 初始只看 ask_* +
//     tool_search，想用 scenario 先检索揭示（工具级渐进披露）
//   - -feat-approval：Approval——敏感工具 ask_coder 执行前强制审批（抛 InterruptError 走 ask_user
//     同款 cp 挂起 + Resume 链），其他工具默认放行
//
// # 多副本与会话查询
//
// 设置 PLAN_WEB_REDIS_URL 或 -redis-url，共享会话索引、暂停请求和 Checkpointer。
// 会话按浏览器 Cookie 隔离。GET /api/sessions 可查询状态，DELETE /api/sessions/{id}
// 可删除空闲任务。新建会话加 ephemeral=true 时在终态清理。
//
// SSE 每 15 秒发送心跳。状态恢复从最近的 Checkpoint 开始。部署与恢复边界见
// doc/issue/issue-90-plan-chat-web-lifecycle-prod.md。
package main
