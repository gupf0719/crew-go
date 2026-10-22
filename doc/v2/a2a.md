# A2A 跨进程协作

> ✅ **已落地**——三类能力 + ZooKeeper binding 服务发现均已实现，冒烟 + 真 ZK 环境验证 PASS；server 端对官方 A2A 1.0 TCK 跑 64/73 MUST（见下方 §A2A 1.0 合规）。

crew-go 基于官方 [a2a-go](https://github.com/a2aproject/a2a-go) SDK 接入
[A2A 协议](https://a2aproject.github.io/A2A/)，把单进程编排扩展到跨进程 /
跨语言的 Agent 网络。

## 三类能力

| 能力 | 入口 | 说明 |
|---|---|---|
| **消费远程 agent** | `a2a.NewRemoteAgent(ctx, cardURL, opts...)` | 远程 A2A agent 当本地 `agent_v2.Agent` 嵌进任意编排 |
|  | `a2a.NewRemoteAgentTool(ctx, cardURL, opts...)` | 包成 `tool.Tool` 进 BasicAgent toolkit |
| **暴露本地 agent** | `a2a/server.Handler(agent, baseURL)` | 任意 `agent_v2.Agent` → 标准 A2A server（`http.Handler`）|
| **服务注册发现** | `a2a/registry.Directory` | 多语言服务目录（4 层 `{ns}/{serviceName}/{agentName}/{encodedInstanceId}`）：`NewInMemory` 参考实现 + ZooKeeper binding `a2a/zk.New`；`ServiceInstance.AgentName` 区分同 service 下多个 Agent Card（缺省=serviceName）|
|  | `a2a.NewRemoteAgentFromService(ctx, dir, ns, svc, sel, opts...)` | Discover（聚合 svc 下所有 agentName 实例）+ Selector（轮询 / 随机 / 加权，去重按 `InstanceIdentity`）→ RemoteAgent；`WithDiscoveryAgentName` 可精确路由到单个 agentName |

横切：Task 状态机 / taskID 续跑 / 流式 chunk→artifact / input-required HITL（默认追问
+ `WithHumanGate` 骑 host cp 真人 gate）全程跨进程透传。

## 现有 example / 冒烟

| 路径 | 覆盖 |
|---|---|
| `example/a2a/hitl_smoke` | Task 状态机 / input-required / 流式 / WithHumanGate HITL（自包含，无需真 LLM）|
| `example/a2a/llm_smoke` | 真 qwen：全互通环 / 流式 chunk→artifact / server 侧 HITL |
| `example/a2a/plan_smoke` | plan-chat × A2A as tool 四层真 LLM |
| `example/a2a/orch_llm_smoke` | 6 编排 × A2A × 真 LLM |
| `example/a2a/real_sut` | 真 BasicAgent 暴露为 A2A server，配官方 TCK 跑 1.0 conformance（`-model` 选未耗尽的模型）|

## 协议规范

- [A2A 服务注册发现 spec](../description/a2a-registry-discovery-spec.md)（ServiceInstance 模型 / Directory / ZooKeeper Binding v1 / Selector / failover）
- 冒烟清单见 [smoke-test-recipes §3](../description/smoke-test-recipes.md)
- 业务自实现 `registry.Directory`（Nacos/etcd/Consul 等）跑 `a2a/registry/conformance` 验收

## A2A 1.0 合规（官方 TCK 验证）

`server.Handler` 暴露的 server 对官方 [a2a-tck](https://github.com/a2aproject/a2a-tck)
（v1.0.0）JSON-RPC MUST 级跑 **64/73 pass**——核心协议（send / get / stream / task
生命周期 / JSON-RPC 错误信封与错误码）全过。server 端合规由 `server.Handler` 自动补齐，
业务无需额外配置：

| 合规点 | 机制 |
|---|---|
| 端点容忍尾斜杠 | `/invoke` 与 `/invoke/` 共用 handler（Go ServeMux 精确匹配，带尾斜杠 client 否则 404 + body 非 JSON 令对端解析失败）|
| AgentCard `skills` 为数组 | `Card` 补 `Skills: []`（1.0 schema 要求非 null）|
| ListTasks 可用 | `conformanceInterceptor` 注入默认已认证 User（SDK taskstore 按 User 隔离，无 User 返 unauthenticated）|
| A2A-Version 校验 | 同 interceptor 校验版本头，不符返 `VersionNotSupportedError`（-32009）|
| Content-Type 校验 | `contentTypeGuard` HTTP middleware：非 `application/json` 返 `ContentTypeNotSupportedError`（-32005，解析前拦）|

**剩余 9 个 MUST 失败均非框架可改**：
- **artifact / message 内容类（5）**：TCK 为 scenario-scripted SUT 设计（期望固定文本 `Generated text content` / file·data artifact），真 LLM agent 给真答案不照剧本——非合规缺陷。
- **timestamp 时区（1）**：SDK `NewStatusUpdateEvent` 用 `time.Now()`（本地时区非 UTC-Z）——server 进程设 `TZ=UTC` 绕过，或待上游修。
- **subscribe 无效/终态 task（3）**：SDK `SubscribeToTask` 已返 `-32001 task not found`（实测），但以 SSE 流内事件递送，TCK 期望即时错误响应——SDK 递送形式差异。

**自跑**：`example/a2a/real_sut` 把真 BasicAgent 暴露为 server，另端跑 TCK：

```bash
QWEN_API_KEY=xxx go run ./example/a2a/real_sut -port 9999
uv run python run_tck.py --sut-host http://127.0.0.1:9999 --transport jsonrpc --level must
```

## 三能力最小示例

### 消费远程 agent（agent 形态 / tool 形态）

```go
// agent 形态：远程 A2A agent 当本地 agent_v2.Agent 嵌进任意编排
ra, err := a2a.NewRemoteAgent(ctx, "http://10.0.0.12:8080/.well-known/agent-card.json")
events, err := ra.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("帮我查订单 123"))

// tool 形态：远程 agent 当 tool 进 host BasicAgent toolkit，host LLM 自主调度
remoteTool, err := a2a.NewRemoteAgentTool(ctx, cardURL, a2a.WithName("order_svc"))
tk := tool.NewToolkit(); tk.Register(remoteTool)
host := agent_v2.NewBasicAgent(agent_v2.WithModel(m), agent_v2.WithToolkit(tk))
```

### 暴露本地 agent 为 A2A server

```go
ag := agent_v2.NewBasicAgent(agent_v2.WithName("order_svc"), agent_v2.WithModel(m))
baseURL := "http://10.0.0.12:8080"
// Handler 自动挂 AgentCard(/.well-known/agent-card.json) + JSON-RPC(/invoke)
go http.ListenAndServe(":8080", server.Handler(ag, baseURL))
```

### 服务注册发现（跨进程 / 跨语言）

```go
dir, _ := zk.New([]string{"127.0.0.1:2181"}, 10*time.Second) // ZooKeeper binding
// server 侧注册（4 层 service/agentName/instance；agentName 传 "" → 缺省 = serviceName）
reg, _ := dir.Register(ctx, "default", server.Instance("order-svc", "", "10.0.0.12:8080", baseURL))
defer reg.Deregister(ctx)
// client 侧发现 + 调用（聚合 service 下所有 agentName 实例，selector 选一个）
ra, _ := a2a.NewRemoteAgentFromService(ctx, dir, "default", "order-svc", registry.NewRoundRobinSelector())
events, _ := ra.Run(ctx, sess, schema.UserMessage("..."))
// 同 service 多 Agent Card 时精确路由到单个：追加 a2a.WithDiscoveryAgentName("research")
```

## RemoteOption 速查

| Option | 作用 | 适用 |
|---|---|---|
| `WithName(name)` | override agent/tool 名（默认 AgentCard.Name 转合规名）| 消费 |
| `WithDescription(desc)` | override 描述（默认 AgentCard.Description）| tool 形态能力发现 |
| `WithClientOptions(opts...)` | 透传 a2aclient FactoryOption：auth header / 自定义 transport | 鉴权 |
| `WithHumanGate()` | 远程**普通追问**（input-required）抛 InterruptError 走真人 HITL（默认返追问让 host LLM 自动补）。**远端的审批请求不归它管、一律交给真人**：挂起清单标了审批的问题、任务状态 AUTH_REQUIRED，不管开没开都抛中断（宿主模型不能替人批准远端操作）；别家把审批写成纯文本追问时识别不了，对端不是 crew-go 时要真人把关就开它。AUTH_REQUIRED 的凭证走带外通道（规范 §7.6）：按审批问人，人没有拒绝就不发消息、重新挂上那个任务（先轮询到它离开 AUTH_REQUIRED，最多等 30 秒、到时仍停着就再问一次人；之后已结束取终态，还在跑的支持流式就订阅、不支持就接着轮询），拒绝时取消远端任务、这次调用报错。等人答复的那一问按调用位置记进 host 存档：没拿到答复不联系远端、原样再问，远端不再等真人（任务结束、或改问交给宿主模型的普通追问）就清掉、之后再跑发原始请求，几次调用 / 并行分支各续各的，续跑不依赖原 session；在节点执行里被调起时这次调用一律在它自己的相位里问，不占调用它的那段代码的提问序号（调用方按进度跳过它时后面的提问不挪位）；相位名在节点体、工具函数体里按远端 + 请求内容 + 第几次定——请求在续跑之间变了就是另一次调用，要么把拼好的请求记下来、要么经 RunSubAgent + WithCallKey 调；在别人给的相位里（框架交接、被交接的业务壳、钩子）按外层相位名 + 远端 + 第几次定、不看请求内容；引擎钩子 / 条件边里远端要人时报错 | 无 LLM 编排嵌远程 / 人类确认 |
| `WithHooks(h)` | agent 形态观测（fire OnAgentStart/End 对）| agent 形态嵌编排 |
| `WithObservability(obs)` | a2a 跨进程 trace：建 `invoke_agent` CLIENT span，把 W3C trace context（traceparent / tracestate / baggage）写进出站 `Message.metadata` | 跨进程 trace 串联 |
| `WithDiscoveryAgentName(name)` | **仅 FromService**：限发现指定 agentName（同 service 多 Agent Card 精确路由）| 服务发现 |

## failover 与 Selector 选型

**Selector（3 策略，`NewRemoteAgentFromService` 第 5 参；传 nil → 默认随机）**：

| 策略 | 行为 | 选型 |
|---|---|---|
| `registry.NewRoundRobinSelector()` | 轮询，跨调用持游标（须**复用同一 selector 实例**才轮询）| 均衡负载首选 |
| `registry.NewRandomSelector()` | 等概率随机，无状态 | 单次调用即分散 |
| `registry.NewWeightedSelector()` | 按 `ServiceInstance.Weight` 加权随机 | 异构实例按容量分配 |

**failover 契约（§8）**：只在「discovery 失败 / selector 选空 / Agent Card 拉取失败」阶段换下一实例重试（同次 failover 不重选已试过的——去重身份 `registry.InstanceIdentity` = agentName+instanceId，故同 instanceId 不同 agentName 视为不同实例）；**一旦 `message/send` 已发出或 `message/stream` 已建立，不自动切换实例**（避免远程重复建 task / 重复扣费 / 状态分叉）。请求级重试须由上层按幂等 key 显式实现。无可路由实例返 `ErrNoEligibleInstance`（`errors.Is` 区分"等注册"vs"全失败重试"）。

## 已补充（ZK 真环境验证 PASS）

- [x] ZooKeeper binding 服务发现端到端示例 → `example/a2a/zk_demo`（同进程）+ `a2a_zk_server`/`a2a_zk_client`/`a2a_zk_list`（真实拓扑分进程，client 纯发现+调用）（4 层 `service/agentName/instance`；2026-06 真 ZK 验证 PASS：zk conformance 15 子测试 + 自家 server↔client 4 层端到端 + a2a_hitl/llm 冒烟全 PASS）
- [x] 消费 / 暴露 / 发现 三能力的最小可跑示例代码 → 见上"三能力最小示例"
- [x] `RemoteOption` 逐项说明 → 见上"RemoteOption 速查"
- [x] failover 行为 + Selector 选型指南 → 见上"failover 与 Selector 选型"
