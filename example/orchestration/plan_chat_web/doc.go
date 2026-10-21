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
// 本 demo 仅为观测性演示，未做生产级 SSE 细节（断线续传 / Last-Event-ID / 心跳 /
// backpressure 保护）。
//
// # 生产部署必知 (iter-5-N audit)
//
// 业务真生产部署 plan_chat_web 模式时必加:
//
//  1. **鉴权**: 当前 session_id = sess.RunID 暴露给前端, 业务前端拿到后可猜测 /
//     枚举其他 session 调 /api/interrupt 干扰别人对话. 必加:
//     - cookie / JWT 鉴权 user_id → session_id 绑定 (映射表存 Redis)
//     - /api/chat?session_id=xxx handler 验证 user_id 是 session owner 才放行
//     - /api/interrupt 同款验证
//
//  2. **cp backend**: 当前 cp 用 InMemoryCheckpointer (main.go:280) 进程重启所有
//     HITL Pending 对话**全断**. 生产改用 crew-go/sql.NewCheckpointer (PG+MySQL)
//     或 crew-go/redis.NewCheckpointer (Redis master-only). 详见 `redis/doc.go`
//     "Redis 持久化配置" 章节 (业务必配 appendonly yes + 双 backend hot/cold).
//
//  3. **HTTP timeout**: -request-timeout-seconds 默认 0 = 无 timeout (依赖 client
//     connection). 业务生产用 nginx / cloudflare 网关层管 timeout (业务 LLM
//     Plan-Execute 真长跑可超 10min, 服务端不要硬切).
//
//  4. **SSE backpressure**: eventSink 通道容量 256, 满时 silent drop (writeSSE
//     每 5s log warn `[plan_chat_web] SSE backpressure drop +N`). 业务生产监控
//     stderr grep 此 warn 排查 SSE 慢消费.
//
//  5. **TTL sweeper**: -session-ttl-minutes 默认 60 min — 业务长跑 thread (用户
//     conv 几天) 调大或设 0 永不过期 (但要监控 OOM, 单进程 demo 内存模式没扩展).
package main
