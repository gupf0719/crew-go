# 冒烟测试速查（Smoke Test Recipes）

CLAUDE.md "交付纪律" 提到的两个 binary 冒烟具体操作。改 cp / 嵌套 / Resume / ns 路径的 commit 前**必跑这两个**。

## 1. nested_smoke（约 30 秒）

验证 **7 编排** (BasicAgent + Sequential + Loop + Reflect + Handoff + Parallel + Routing；
含 BasicAgent 顶层) HITL Resume 闭环 + cleanup + ns 物理分离 + GraphID 防串台 + ThreadID
跨 run。**PlanAgent / GraphAgent 嵌套 HITL 走独立单测** (`agent_v2/plan_nested_resume_test.go`
+ `r30_graph_agent_hook_order_test.go` + `r31_graph_resume_test.go`)，本 smoke 不覆盖。

```bash
go run ./example/orchestration/nested_smoke
```

预期输出（**12 场景全过 + 退出码 0**）：

```
=== 场景: BasicAgent 顶层 ask_user (chat_entry / plan_chat_web 核心路径) === PASS
=== 场景: sequential 内嵌 BasicAgent ask_user ===                            PASS
=== 场景: loop body=BasicAgent ask_user ===                                  PASS
=== 场景: reflect actor=BasicAgent ask_user ===                              PASS
=== 场景: handoff initial=BasicAgent ask_user ===                            PASS
=== 场景: sequential 多次 Run 同 sessionID（验证 cleanup 不污染） ===        PASS
=== 场景: sequential Resume Abort（验证 cp.DeleteSubtree 清子树） ===        PASS
=== 场景: loop 多轮 HITL（验证 ns round_<N> 隔离不冲突） ===                 PASS
=== 场景: reflect actor + reflector 跨轮 HITL ===                            PASS
=== 场景: parallel 内嵌 BasicAgent ask_user (一分支中断) ===                 PASS
=== 场景: routing classifier→BasicAgent ask_user ===                         PASS
=== 场景: GraphID mismatch fail-fast (ISSUE-50 P0 防串台) ===                PASS

=== 总结：12/12 通过 ===
```

每个场景 stdout 还会打 `Run 1: 触发中断 interrupt_id="..." question="..." / Resume: final="..."` 调试行，便于定位 InterruptID 稳定性。

**12 场景覆盖矩阵**：
- 5 基础：BasicAgent / Sequential / Loop / Reflect / Handoff 各跑一次 Run + Resume
- 4 高级：cleanup 不污染 / Resume Abort / loop 多轮 ns 重入 / reflect 跨轮
- 2 补全：Parallel / Routing 嵌入路径（v2 接口合并后 7 编排全覆盖；Plan/Graph 走单测）
- 1 ISSUE-50：GraphID 防串台 fail-fast

## 2. plan_chat_web mock + HITL Resume（30 秒）

验证 BasicAgent + hook 主动 interrupt + Resume Continue + SSE 流连贯 + cleanupOnSuccess。

```bash
# 1. 启服务（mock 模式不依赖真 LLM）
go build -o /tmp/web ./example/orchestration/plan_chat_web
/tmp/web -mock -addr :19999 -compaction-window 1000 > /tmp/web.log 2>&1 &
SERVER_PID=$!
sleep 1

# 2. 启 chat 后台抓 sessionID（scenario 模式让 PlanAgent 多步比 chat_entry 慢）
rm -f /tmp/chat_intr.log
(curl -sN "http://localhost:19999/api/chat?q=plan+budget+report&mode=budget" --max-time 60 > /tmp/chat_intr.log 2>&1) &
INTR_PID=$!
sleep 0.5

# 3. grab SID
SID=$(grep -o 'session_id":"[^"]*' /tmp/chat_intr.log | head -1 | cut -d'"' -f3)
echo "SID=$SID"

# 4. 立即触发 hook interrupt
curl -s -w "interrupt HTTP=%{http_code}\n" -X POST "http://localhost:19999/api/interrupt?session_id=$SID" -o /dev/null

# 5. 等 chat 收 interrupted 事件后退出
wait $INTR_PID 2>/dev/null

# 6. 验证 interrupted 事件
grep -E '"type":"interrupted"' /tmp/chat_intr.log | head -1

# 7. Resume continue
rm -f /tmp/chat_resume.log
curl -sN "http://localhost:19999/api/chat?session_id=$SID&action=continue" --max-time 30 > /tmp/chat_resume.log 2>&1
grep -E '"type":"(final|done)"' /tmp/chat_resume.log | head -2

# 8. 验证 session 完成后清理
curl -s "http://localhost:19999/api/chat?session_id=$SID" | head -1
# 预期: "session already completed (status=success)"（HTTP 410）

# 9. 收尾
kill $SERVER_PID 2>/dev/null
```

**关键验证点**：
- ✅ `interrupted` 事件含 `query.interrupt_id="..."` + `query.question="已挂起，请点继续或换方向。"`（v2 严格契约 — InterruptError.Query 必须非 nil）
- ✅ cp 含 `frontier:["model"]` + `status:"pending"`
- ✅ Resume continue → 出 `final` + `done` 事件，history_size 累积
- ✅ 第二次同 sessionID 请求返 "session not found"（cleanupOnSuccess 工作）

## 3. a2a 冒烟（3a/3b 必跑；3c/3d 可选）

```bash
# 3a. a2a_hitl_smoke（~10 秒，自包含——自动拉起 example/a2a/rich_server 子进程）
go run ./example/a2a/hitl_smoke
# 预期尾行: "=== A2A HITL 冒烟全 PASS（Task 状态机 / input-required 默认 / taskID 续跑 / 流式 / WithHumanGate HITL 闭环）==="

# 3b. a2a_llm_smoke（~1-2 分钟，真 qwen——项目根 .env 需 QWEN_API_KEY）
go run ./example/a2a/llm_smoke
# 三部: Part1 全互通环（server 适配+注册+发现+qwen tool-calling）
#       Part2 真 LLM 流式（裸 a2aclient 数 wire 上 TaskArtifactUpdateEvent ≥1）
#       Part3 server 侧 HITL（qwen ask_user → input-required → 客户端 humanGate 中断 → Resume）
# 预期尾行: "=== A2A 真 LLM 冒烟全 PASS（全互通环 + 流式 artifact + server 侧 HITL，全部真 qwen）==="

# 3c.（可选）a2a_smoke——基础 Message 路径，需手动起 a2a-go 官方 helloworld server
#     cd <你的 a2a-go checkout> && go run ./examples/helloworld/server/jsonrpc -port 19090 &
go run ./example/a2a/smoke

# 3d.（有 Docker 或可达 ZK 时）ZK binding conformance（integration tag，默认 go test 不编译）
cd a2a/zk && go test -tags=integration ./...          # testcontainers 自动拉 zookeeper:3.9
# 或 ZK_ADDR=host:2181 go test -tags=integration ./... # 直连现成 ZK（节点建在 /a2a-conf-* 临时子树）
# 真 ZK 验证记录: ZK_ADDR=<zk-host>:2181 → 15 子测试全 PASS（含 MultipleAgentNames / SameInstanceDifferentAgentName，4 层 service/agentName/instance 路径，2026-06）

# 3d-bis.（有可达 ZK 时）a2a_zk_demo——A2A × 真 ZK 服务发现 + 跨进程调用端到端（确定性，不依赖 LLM）
ZOOKEEPER_ENDPOINT=host:2181 go run -tags integration ./example/a2a/zk_demo
# 4 锚点: ①注册 ②独立连接跨进程发现（weight 默认 100/AgentCardURL 拼对）③A2A wire 调用拿确定性代号 ④ephemeral 反注册消失
# 预期尾行: "=== A2A × 真 ZooKeeper 服务发现 + 跨进程调用闭环 PASS ==="

# 3d-ter.（有可达 ZK 时）真实拓扑分进程：server 常驻注册（4 层 service/agentName/instance）+ client 纯发现调用 + list 诊断
#   终端1（起 server 注册；-name 即 agentName，默认=service；加 -llm 用真 qwen）：
go run -tags integration ./example/a2a/zk_server -zk host:2181 -service my-go-agent -name research
#   终端2（纯 client：发现别处注册的 agent 并调；-agent-name 可精确路由到单个 agentName）：
go run -tags integration ./example/a2a/zk_client -zk host:2181 -service my-go-agent -q "你好"
#   诊断（列 zk 注册全貌；-tree raw dump 任意层级，排查跨语言对端真实结构）：
go run -tags integration ./example/a2a/zk_list -zk host:2181 -ns default -tree
# 预期: client "✓ 发现 N 个实例" + "=== 远程应答 ==="；server 日志显示 4 层路径 …/my-go-agent/research/…
```

```bash
# 3e.（可选，改 PlanAgent×A2A 集成时跑）a2a_plan_smoke——plan-chat 拓扑调 A2A as tool（四层全真 qwen）
#     chat_entry → AgentAsTool(PlanAgent) → LLMPlanner 派发 → worker BasicAgent → A2A tool → 远程 BasicAgent+确定性工具
go run ./example/a2a/plan_smoke
# 预期尾行含: "A2A plan-chat 冒烟 PASS：chat→plan→worker→A2A→远程 qwen 四层真 LLM 闭环"
# 注: 依赖 qwen.NewTestClient 默认关 thinking（dashscope thinking 与 LLMPlanner 的
#     tool_choice=required 互斥，见 model/qwen WithEnableThinking godoc）
```

```bash
# 3f.（可选，A2A×编排矩阵全验时跑）a2a_orch_llm_smoke——剩余 6 编排 × A2A × 真 qwen
#     Parallel(两独立实例并发) / Loop(每轮打远程) / Routing(LLM classifier 选 A2A 分支+负分支零调用) /
#     Reflect(actor 经 A2A) / Handoff(transfer→专员经 A2A) / GraphAgent(NodeFunc 内调 RemoteAgent.Run)
#     断言：远程 lookup_code 调用 delta（atomic 计数）+ 终答含确定性代号
go run ./example/a2a/orch_llm_smoke
# 预期尾行含: "A2A × 编排矩阵冒烟全 PASS"
# （Basic host / Sequential+HITL / Plan 四层由 3b + a2a_plan_smoke 覆盖——合计 9/9 编排 × A2A × 真 LLM）
```

**关键验证点**：
- ✅ 3a 场景 D：`WithHumanGate Run 抛中断` + Resume 后 `got it: blue`（taskID 续跑 + stableID 稳定 + GetResumeAnswer 三合一）
- ✅ 3b Part2：wire 上 artifact 事件数 ≥1（chunk→artifact 桥真生效，不是只发 completed）
- ✅ 3b Part3：终答包含人类答复的颜色（server 侧 taskID↔Resume 映射闭环）

## 4. durability_smoke（真 qwen，可选——改 durability / cp 写时机时跑）

```bash
# 真 qwen 端到端验 Durability 经 ctx 进 agent_v2 BasicAgent（项目根 .env 需 QWEN_API_KEY）
go run ./example/harness/durability_smoke
# 4 场景：exit 基线（SaveNS=0）/ sync / async（每超步写 cp + 成功即清）/ 崩溃恢复
#         （中途 cp 注入 → 同 sid **显式 Resume**，Run2 比 Run1 少调 LLM = 真续跑非重跑。
#          ⚠️ 不是 auto-load：顶层 Run 恒声明新任务、不捡存档，崩溃恢复必须显式 Resume）
# 预期尾行: "=== 全部 PASS：durability 经 ctx 进真 qwen BasicAgent 端到端正确（含崩溃恢复）==="
```

**关键验证点**：
- exit=0 vs sync/async>0 每超步写——证 durability 经 ctx 端到端 fire（agent_v2 编排没暴露 option，只能 ctx 透传）
- 成功即清对三档都成立（无残留 cp）
- 崩溃恢复 Run2 LLM 次数 < Run1——证 durability cp 真 Resume 续跑（非重头跑）

## 5. toolsearch_smoke（真 qwen，可选——改 toolsearch / 渐进披露时跑）

```bash
# 真 qwen 验工具级渐进披露端到端（项目根 .env 需 QWEN_API_KEY）
go run ./example/harness/toolsearch_smoke
# Part 1 渐进披露：deferred 工具隐藏 → qwen 自主调 tool_search 解锁 → get_weather 执行 → 答案落地
# Part 2 fail-closed：强提示 qwen 直调未揭示工具 → 越过 bind list emit get_weather →
#         框架返 [tool_not_revealed]（第一次直调被拦不执行）→ qwen 收到反馈后重走 tool_search
# 预期尾行: "=== 真 LLM toolsearch smoke 通过 ==="
# 模型走 fallback 链 flash→flash-2026-04-16→plus→plus37（flash 额度满/限流/欠费自动切）
```

**关键验证点**：
- Part 1 calledSearch + get_weather 执行 + 答案含天气——证披露闭环（隐藏→检索→揭示→调用）
- Part 2 越界 emit + [tool_not_revealed] 拦截（第一次直调被拦不执行）——证 fail-closed 真拦未揭示工具（非仅 mock）
- Part 2 若 qwen 守规矩走了 tool_search（⊘ 未触发）不算 FAIL；只有「越界 emit 但框架没拦截」才 FAIL

## 6. skill_action_smoke（真 qwen，可选——改 skill action / run_skill_action 时跑）

```bash
# 真 qwen 验 skill action 端到端（项目根 .env 需 QWEN_API_KEY）
go run ./example/harness/skill_action_smoke
# inventory skill 的 go-func action get_stock 确定性返一个 LLM 编不出的库存数 237。
# 真 qwen 被要求查库存 → 自主调 run_skill_action(inventory, get_stock, {sku}) → 框架调
# SkillBox.RunSkillAction → go-func 跑 → 结果回灌 → qwen 报出 237。
# 预期尾行: "=== skill action 真 LLM 端到端通过：action 执行 + 结果 237 回灌进答案 ==="
# 模型走 fallback 链 flash→flash-2026-04-16→plus→plus37（额度满/限流/欠费自动切）
```

**关键验证点**：
- run_skill_action 调用 + go-func action 执行 + 答案含 237——证「LLM→run_skill_action→go-func→回灌」整链通
- 237 是 LLM 编不出的数据，只有 action 跑了才拿得到（非 LLM 自己编）

## 7. approval_smoke（真 qwen，可选——改工具审批层时跑）

```bash
# 真 qwen 验工具审批层（harness P1）端到端（项目根 .env 需 QWEN_API_KEY）
go run ./example/harness/approval_smoke
# Part 1 approve：qwen 被要求删数据 → 调 delete_records → 框架审批 gate 拦截抛 tool_approval
#   中断（工具**未**执行）→ caller approve → Resume → 工具执行 → qwen 汇报"88 行"（编不出的标记）。
# Part 2 deny：同流程但 deny → 工具不执行 → [tool_denied] 回灌 → qwen 汇报被拒。
# 预期尾行: "=== 全部 PASS：工具审批层经真 qwen 端到端正确（approve 执行 / deny 拦截）==="
```

**关键验证点**：
- 中断时工具**未**执行（gate 拦在 Call 前）+ approve 后真执行 + qwen 答复含 88（回灌链通、非编造）
- deny 后工具**不**执行（被拒，工具级 `[tool_denied]` 非 abort 整 Run）

## 何时跑

按 CLAUDE.md "交付纪律"：

| 改动类型 | nested_smoke | plan_chat_web |
|---|---|---|
| cp 接口 / 协议 | ✅ | ✅ |
| 嵌套子图 / ns push 协议 | ✅ | ⚠️（间接验证）|
| Resume 路径 / cleanup | ✅ | ✅ |
| ns 寻址 / GraphNS 推进 | ✅ | ✅ |
| events 流 / OrchHooks | ⚠️ | ✅ |
| 4 层压缩改动 | N/A | ⚠️（mock 不触发 token 阈值）|
| 文档 / 注释 | N/A | N/A |
| 单纯测试改动 | N/A | N/A |

改 `a2a/`（client / registry / server / zk 任一）→ 必跑 §3 的 3a + 3b（CLAUDE.md 交付纪律）。
改 `compose_v2` durability / cp 写时机（graph_loop 写点 / durability.go）→ 跑 §4 durability_smoke（真 qwen，非 CI）验经 ctx 端到端 fire + 崩溃恢复。
改 `agent_v2` toolsearch（tool_search.go / makeModelFn 过滤 bind list / makeToolsFn fail-closed）→ 跑 §5 toolsearch_smoke（真 qwen，非 CI）验披露闭环 + fail-closed 拦截。
改 skill action（`skill/action.go` / `skillbox.go` RunSkillAction / `agent_v2/skill_action_tool.go` run_skill_action 注册）→ 跑 §6 skill_action_smoke（真 qwen，非 CI）验 LLM→run_skill_action→go-func→回灌整链。
改 `agent_v2` 工具审批层（`approval.go` checkToolApproval / `basic_agent.go` makeToolsFn 审批 gate）→ 跑 §7 approval_smoke（真 qwen，非 CI）验审批中断 + approve 执行 / deny 拦截。
改 `decision/`（请求校验、响应解析，或 `jev` / `bailian` 某一家的接入）→ 跑 §9 decision client_smoke，两家各一次（真服务端，非 CI）。
注意：上句"CI 友好"指上表 nested_smoke + plan_chat_web；a2a 冒烟里 **3a 无外部依赖可进 CI，3b 需 QWEN_API_KEY 不适合免 key CI**。

CI 友好——两个 binary 都不依赖外部 LLM API，可纳入 PR 必跑。

## §8 记账文案的重放率（真 LLM，integration 测试不是冒烟）

```bash
go test -tags=integration ./agent_v2/ -run TestRecordWording -v -timeout 20m
```

量 `describeUnresolvedCalls` 产出的记账文本会不会让模型把**用户放弃掉的副作用调用**
重新执行。**改那句文案前必跑。**

**为什么是测试不是冒烟**：其余冒烟验的是框架管道端到端、走公开 API；这个不建 agent
不跑图，就是拼 prompt 直接 Generate——本质是一次提示词 A/B 测量。写成测试还逼着把
pass/fail 语义想清楚（上一版写成 main 程序，退出码跟健康度**严格反相关**）。

**三条判据缺一不可**：
1. 生产文案必须 0 重放（要守的安全属性）
2. **至少一个反例档必须 > 0**——否则这轮没有测量能力，「生产档 0 重放」不成立
3. 阳性对照（根本没有记录）必须高——塌了说明场景已失效

**LLM 报错一律 `t.Fatal`**：上一版把 error 当「没重放」记，额度耗尽时全 403 却报
「✅ 全绿」退出 0。**fail-open 的量具比没有量具更糟。** 模型走 `fallback.New` 串
候选链，额度耗尽自动切。

预期尾行：`ok  crew-go/agent_v2`，日志里生产档 `重放 0/16`、至少一个反例档非 0、
对照档接近满分。

## §9 decision client_smoke（真服务端——改决策模型客户端时跑）

```bash
# 对真服务端跑一遍决策模型客户端（key 从项目根 .env 读）：
go run ./example/decision/client_smoke -provider jev                               # 需 TYPESAFE_API_KEY
go run ./example/decision/client_smoke -provider bailian -workspace <业务空间 ID>  # DASHSCOPE_API_KEY，没有就取 QWEN_API_KEY
# 三步：一个请求问三种题型 / 请求不存在的模型报 4xx 且不重试 / RoutingAgent 挂路由决策方路由三条输入
# 每步打印 ✓ 或 ✗，三步都 ✓ 时退出码 0
```

**关键验证点**：
- 响应按真服务端的形状解析成功。单测全是 httptest 桩，桩是照我们对协议的理解写的，理解偏了单测照样全绿
- 请求 ID 读到了（Jev 在 `x-typesafe-request-id` 响应头，百炼在 `x-request-id`），错误文本里也带着
- 4xx 只请求一次就返回，不重试
