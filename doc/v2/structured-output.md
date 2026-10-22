# 结构化输出设计：StructuredOutput

> **文档状态**：**P0 + 聚焦 P1 已实现（2026-07-06，见下方「实现状态」节）**——正文 §0-§10 是原始
> 设计草案（[设计]），保留作设计溯源；**实际落地范围 + 与草案的偏离以「实现状态」节 + 代码为准**
> （草案部分点如 A3 同轮裁决、多模式自动选、response_format strict 路径属 P2/P3 **非目标**，2026-07-13 定，见「实现状态」节）。
>
> **决策**：给 agent 加"最终答复结构化输出 + 校验失败自动回炉"。**业务只声明一次 schema**，框架按 provider 能力自动选机制(strict response_format / forced tool / json_object)+ **validate/retry 永远兜底**。capability 由 provider client **自报家门 + 用户可 override**，不查中央表、不运行时探测。
>
> **设计依据**：3 维深挖调研 + 优化 grep + **多角度审计(3 单视角 agent 找出 5 HIGH + 3 MED 洞)+ 挖 Pydantic AI 1.107/LangChain v1 1.3.9 源码逐洞求解**。审计发现+解法见 §0.5,**解法已折入 §4 正文**(§4 为权威最终设计,§0.5 为审计史+依据)。关键约束见 §0。

---

## 实现状态（2026-07-06）

> **正文 §0-§10 是原始设计草案**，与实际落地有偏离；本节为权威实现清单，正文仅作设计溯源。

**P0（model 层，已实现）**：
- `model.ResponseFormat` / `ResponseFormatJSONSchema` + `WithResponseFormat` / `WithJSONSchema(name, schema, strict)` / `WithJSONObject`；openai client wire 映射 + `Message.Refusal` 解析（Generate + Stream）。
- `model.NormalizeJSONSchema(s, profile)`（**导出**——草案 §4.3 叫 `normalizeStrictSchema`）+ `SchemaProfile` 两档 `ProfileStrict`（OpenAI/xAI）/ `ProfileInlineDefs`（qwen/deepseek）。结构感知遍历（不污染 enum/const 数据）。
- `model.StructuredCapability`（多字段能力画像）+ `StructuredCapable` 接口 + `StructuredMode`（ModeAuto/ModeTool/ModeResponseFormat/ModeJSONObject）；provider 自报（openai `DefaultOpenAICapability` 全 strict / qwen·deepseek `NonStrictToolCapability`）；`openai.WithStructuredCapability` 覆盖。

**聚焦 P1（agent_v2，仅 BasicAgent，已实现）**：
- `WithOutputSchema(name, schema, opts...)` + `WithMaxValidationRetries(n)`（默认 1）。**机制固定 ModeTool**（`final_output` return-direct 自校验工具，常量 `FinalOutputToolName`）。
- 校验（required 存在 + 顶层类型）通过 → return-direct 短路 + 结构化 JSON 作最终答复；失败 → error tool_result（`StructuredOutputInvalidCode`）→ **复用既有 ReAct 循环回炉**（`tools→model` 边白送，非另造重试引擎）。
- **A1**：`final_output` 校验通过后，tools 节点把它那条 tool_result（内容是工具 marshal 的**规范 JSON**，非模型 raw args）写进 `basicFinalCh` 作本轮产出；`structuredFinalOf` 只负责判「本轮收尾的是不是它」→ `FinalMessage().Content` 即结构化 JSON，业务直接 `json.Unmarshal`。
  ⚠️ 判据**看本轮产出不扫消息列表**——扫列表会捞到上一轮的结构化结果（含 prior，实测过：本轮零个 final_output 时静默返回上一个问题的答案）。
- **A4 预算**：tools 节点在 `OnToolEnd` 之后按派发层的章，把**本次调用**里框架那件 final_output 实际被调的次数记进 channel（`basicFinalOutputAttemptsCh`，随存档走、跨 Resume 累积；不扫消息列表——那个列表含 prior，开了会话历史时会把上几个回合的尝试算进来，实测），超 maxRetries 抛 `StructuredOutputError{SchemaName, Attempts, LastError}`；postamble 以「没有结构化答复」收尾时报的 `Attempts` 读同一个计数。
  「算不算一次失败」与 `LastError` 是同一个判据：final_output 这次调用有没有产出通过 schema 校验的答复——参数不是合法 JSON、schema 校验失败、校验通过却被钩子否决、被钩子从失败改成成功都算失败（回炉、同批业务 return-direct 工具让路）。答复有效与否归框架：钩子改成成功的那次不成为答复（同框架控制工具：钩子把失败改成成功不会凭空变出一个转交），`LastError` 报钩子之前那次失败。曾经参数非法哪处都不认：两次参数非法耗尽预算时 `LastError` 为空，跟业务 exit 工具同批时业务工具短路收尾；收成一处后又有一版把钩子改成成功当成不算失败，同样的症状从那条路回来（均实测）。
- 能力允许时 tool_choice=required 逼模型经工具收尾；框架那件 `final_output` 带「框架铸的」封闭标记（`agent_v2/framework_tool.go`），据它豁免审批 / ToolFilter / tool_search 渐进披露 bind+dispatch 两侧 / 中间件与超时 / 结果截断——按实例判、不按名字：没配 `WithOutputSchema` 时业务同名的 `final_output` 照常受这几道闸。
- **不写进业务的 toolkit**：`final_output`（连同 `WithAskUser` 的 `ask_user`）由框架每次 Run 拼工具表时注入（`withFrameworkTools`，绑定侧与派发侧各一次），跟 `WithToolkit` 的先后无关；业务 toolkit 里有同名工具时这份工具表里框架那件生效、构造期告警一次。曾经构造期直接注册进业务传进来的 toolkit：几个 agent 共用一个 toolkit 时，没配结构化输出的 agent 也拿到了这件工具，模型一调就拿别的 agent 的 schema JSON 当自己的最终答复收尾，`err == nil`；`WithToolkit` 排在后面时框架那件整个丢了，而构造期守卫按名字找、找到业务同名那件就放行（均实测，哨兵 `framework_tool_injection_test.go`）。
- **哪些结果是 final_output 的调用，按派发层盖的事实认，不按工具名认**：框架那件带封闭标记（`frameworkFinalOutput`），makeToolsFn 认出它就在结果上盖 `finalOutputAttemptAnnotation`（参数非法、校验失败、校验通过都盖），tools 节点收尾的三处判断与 `structuredFinalOf` 只读这个章。没配 `WithOutputSchema` 时 `final_output` 这个名字业务占得到，那件就是普通业务工具；按名字认的时候，它成功了也被记成一次失败的尝试，同批的业务 exit 工具不再短路、产出被丢、模型被多调一次，`err == nil`（审计实测；它失败、它自己是 return-direct 工具时同理）。
- **A3 组合 fail-fast**：`WithOutputSchema` + 转交 / 路由能力 → construct 期 panic（`validateOutputSchemaConfig`）。两条来路都判：`WithHandoffTransfer()` / `WithRouteTool()` 的 opt-in（工具由框架每次 Run 注入，toolkit 里看不到），以及业务 toolkit 里同名的 `transfer_to_agent` / `route_to`。
- 真 deepseek 冒烟 `example/harness/structured_smoke`（qwen3.6-flash 免费额度耗尽时 deepseek 备选）。

**与草案的关键偏离**（草案正文 stale，以此为准）：
- **A3**（§0.5/§4.4 草案写"采纳控制转移优先 + stub 裁决"）→ **实际未实现裁决，改为 construct fail-fast 拒绝组合**。
- **只做 ModeTool**：`WithStructuredMode` / ModeResponseFormat 自动选 / `WithRepairFunc` **未做**（P2/P3，非目标）。
- **预算口径**：草案 §4.5 说"数 error tool_result 条数"，实际按本次调用里框架那件 final_output 实际被调的次数计（按派发层的章，见上 A4）——等价（每次尝试一条）。
- `StructuredOutputError` 字段 `{SchemaName, Attempts, LastError}`，**无草案提的 `FinishReason`/`ActualMode`**（截断分流 / 多模式回吐属 P2，非目标）。

**非目标（WONTFIX，2026-07-13 定）**：response_format strict 路径 + 校验（refusal `RefusalError` / 截断 finish_reason=length 分流）/ ModeAuto 多模式自动降级 / `WithRepairFunc` 确定性修复 / A3 同轮裁决 / 嵌套跨 AgentAsTool 保对象。
> **理由**：主路径 forced tool（P0 + 聚焦 P1）已覆盖绝大多数结构化需求且真机冒烟过；上列均为 OpenAI strict 生态精细化 + 优化项，与「Go 原生最简 SDK」定位不匹配，不为脑补场景预留路径。
> **逃生舱**：将来真接 OpenAI strict 生产场景（refusal/截断需分流）再单开 issue 评估——model 层 P0 的 `WithJSONSchema` / `NormalizeJSONSchema` / `StructuredCapability` 底座已就绪，届时增量接 agent 层即可。

---

## 0. 一句话决策 + 关键约束

**要做**：BasicAgent 最终答复结构化(现在业务要 typed JSON 得自己 parse + 自己写重试)。

**核心设计(本轮讨论 + 优化 grep 收敛)**：
1. **业务只声明一次 output schema**(Go 原生 `map[string]any`，零依赖，**不引 jsonschema 反射库**)。
2. **框架按 provider 能力选机制**——strict-capable(openai 默认)→ `response_format json_schema strict` 或 strict tool(~100% 硬保证)；非 strict(qwen/deepseek 默认)→ **forced tool 非 strict + validate/retry**。
3. **capability 是 per-model 多字段 profile**，provider client 自报 + 用户 override(应对未来 qwen 支持 strict)，未知保守默认非 strict。
4. **validate + retry 永远开**：校验失败 → **路径 2(output tool)走既有 ReAct 循环的 error tool_result(引擎自动配对,零 422)/ 路径 1(response_format)追加 UserMessage**(见 §4.5)→ 重调,默认重试 1,耗尽抛 typed `StructuredOutputError`(绝不静默退化,但退化要**可见**)。可选确定性 `repairFunc` 前置省 LLM。
5. **schema 规范化器**把业务给的普通 schema 改成 strict 兼容(§4.3，落地必需，否则 400)。

**关键约束(决定可行性)**：
- **默认测试模型 qwen-flash 官方只支持 `json_object`，不支持 `json_schema`**；deepseek 同样只 `json_object` 且明确拒 `json_schema`。→ **"带 schema 约束"的跨 provider 主路径是 forced tool call**(qwen/deepseek/OpenAI 凡支持 function calling 都行)，**不赌 `response_format: json_schema`**。
- **某些 qwen 变体(如 qwen-3-coder)连 `tool_choice=required` 都不支持**(Pydantic AI `openai_supports_tool_choice_required=False`)——forced-tool 路径需 capability 兜底。
- **Claude 原生结构化走 Anthropic Messages API**(`/v1/messages`)，非 OpenAI 兼容端点，本设计 openai client 覆盖不到，单列(§7)。

---

## 0.5 设计审计发现 + 解法(2026-06 多角度审 → 挖框架源码求解)

3 个单视角 audit agent(并发/Resume · 嵌套编排/return-direct · 横切特性)对照**实际代码**审本设计,发现框架对齐没问题、但**把抽象机制落到 crew-go 真实 machinery(commitFinal/lastAssistant、return-direct OnceFlag、fallback 异构、checkpoint 预算 lifetime、AgentAsTool)时有 5 HIGH + 3 MED 洞**。**已逐洞挖 Pydantic AI/LangChain v1 源码求解、解法折入 §4 正文**。下方保留审计记录 + 解法摘要(依据):

> **注**:下列发现里的「修向」是**审计当时的初步方向**;**最终解法以下方「解决方案」节为准**,且已折入 §4 正文——部分初版修向(如 A2"取交集"、A4"路径1 提 channel")已被挖框架源码后的解法**推翻/精炼**,以解法为准。

### HIGH(阻塞实现)
- **A1 数据流断裂(命门,2 角度独立命中 + 代码亲验)**:路径2 结构化结果在 `final_output` 的 `tool_call.Arguments`,但 `commitFinal` 走 `lastAssistant`(`agent_v2/basic_agent.go:1468`)**显式跳过带 tool_call 的 assistant** → `FinalMessage().Content` 取到空/错误文本,结构化结果**取不到**。§4.1 vs §5(unmarshal 来源)、§4.4 vs §5(改不改 cond fn)还自相矛盾。**修向**:postamble 为结构化路径**合成**最终消息(`final_output.Arguments` → Content 或专门字段),不复用 lastAssistant;收敛 unmarshal 来源为单一。
- **A2 fallback × capability/mode**:`FallbackChatModel` 持异构候选(openai-strict + qwen-非strict)但 capability per-client——(a) 当时未实现 `StructuredCapable`（现已落地，见上方「实现状态」）;(b) 选 `ModeResponseFormat strict` 后 failover 到 qwen → 400 → `DefaultFailoverPolicy` 4xx 判不切 → **立即 error,failover 反害**(可恢复 429 变不可恢复 400)。**修向**:`FallbackChatModel.StructuredCapability` = 成员**交集**(全员支持才算,否则保守降 ModeTool)→ 异构组合自动落 forced tool。
- **A3 多 return-direct 同轮冲突**:`final_output`(return-direct)+ `transfer_to_agent`/`route_to`(也 return-direct)同轮 → OnceFlag 单 bit 无"谁赢"语义 → 转交 vs 结构化终态裁决未定义。**修向(需决策)**:structured-output agent 能否同时作 Handoff/Routing 成员?能→优先级裁决;不能→构造期 fail-fast。
- **A4 预算跨 interrupt 重置**:校验失败计数 channel 在 toolsNode 抛 Interrupt 时随 ChannelWrite 丢弃 + Resume 重跑归零 → maxRetries 被悄悄绕过,§0"耗尽抛 typed error 绝不静默退化"在 HITL 场景**不成立**;路径1 局部计数结构上不跨 interrupt;+ off-by-one(判定须用局部 `c+1` 不能回读 channel,因 read-after-write 跨超步才可见)。**修向**:计数写入避开 toolsNode 失败丢弃路径;路径1 计数提到 channel。
- **A5 AgentAsTool 嵌套拍平**:带结构化的 sub-agent 被 `AgentAsTool` 包 → `tool.Success(out.Content)`(`agent_tool.go:255`)拍平成文本(且因 A1 Content 还空)→ 结构化跨 agent 边界蒸发。**修向(需决策)**:文档划界"嵌套退化为 JSON 字符串" OR 给 `AgentEvent` 加结构化字段透传。

### MED
- **A6 required 破坏"该输出文本"的 sub-agent**:Reflect evaluator / Sequential 中间步配结构化 + `required` → 文本传值断裂。**修向**:文档声明组合约束(结构化只适合顶层终态/纯抽取,不适合 evaluator/中间步)。
- **A7 压缩丢回炉反馈**:error tool_result 无 protected marker → 被 AutoCompact/ReactiveCompact 压掉 → 模型可能重复同样格式错(语义降级,**非 422**——配对因 run_id 分组不破)。**修向**:文档声明 trade-off,或给 error tool_result 打 marker 加进 `isProtectedGroup`。
- **A8 LLMPlanner 关系未定义**:新 `WithOutputSchema` 取代还是并存 LLMPlanner 私有 tool-forcing?悬而未决 → 两套机制并存风险。**修向(需决策)**:P4 统一 or 并存+理由。

### 解决方案(挖 Pydantic AI 1.107 + LangChain v1 1.3.9 源码,2026-06)

逐洞看两家怎么解 → 推导 crew-go 落点(借鉴不照抄):

- **A1 → 采纳"专门 result slot"**:Pydantic `FinalResult.output`(独立 dataclass,`result.py:1048`)/ LangChain `structured_response`(独立 state 字段,`types.py:347`)——**都不靠 last-message,退出判 slot**,且 output tool_call **补配对 tool_result**。**crew-go**:加专门结构化结果 channel/slot,`FinalMessage` 优先读 slot(空才回退 lastAssistant 文本);`final_output` handler 产配对 tool_result——我们 ReAct 循环本就配对,**A1 + 422 一起解**。
- **A2 → 关键领悟:ModeTool 本身 fallback-safe**:Pydantic `FallbackModel` **拒绝聚合 profile**(`fallback.py:280` 抛 NotImplementedError),mode **per-member 在各成员 request 时解析**(auto schema 同携 tool+json 两形态各选兼容的);显式 pin 不支持 → 非 failover 错误上抛(`models/__init__.py:396` UserError 不在 fallback_on)。LangChain 通用 `with_fallbacks` 有真 bug(只按主成员判)、原生 middleware 重进 handler 碰巧 per-member。**crew-go**:**ModeTool(final_output 工具)是图层 tool 机制、不是 model option,任何支持 function calling 的成员都认 → 异构 fallback 下天然安全**;ModeResponseFormat 仅当"单 client 或全员 strict"才用,异构 fallback 默认落 ModeTool;显式 pin 不支持 → `ShouldFailover` 加一档"配置/能力类错误不切、上抛"。**别做"聚合 capability 交集"——学 Pydantic 不聚合。**
- **A4 → 采纳"隐式预算,无独立 counter"**:LangChain **数 checkpointed message 历史里 error ToolMessage 条数**当重试预算(`test_response_format.py:494` 实锤),**无独立计数器** → 天然跨 checkpoint;显式 limit 才用 `UntrackedValue` channel 显式声明不持久化。**crew-go**:路径2 预算 = **数对话历史里 `final_output` 的 error tool_result 条数**(已在 cp 的 messages 里,跨 Resume 累积不重置,**无可丢弃 counter → H1/H2 一起消失**);路径1 局部 per-tick(像 LangChain ModelRetryMiddleware,中途 interrupt 罕见、重置可接受,文档化)。**我原 `channel Sum counter` 方案被此替代(更干净)。** 另:compose_v2 可加 `UntrackedValue` 等价 channel 类型(把"不持久化"从隐式靠丢弃变显式声明)。
- **A3 → 采纳框架式优先级裁决(grep 实拿到,非"规避")**:LangChain `model_to_tools`(`factory.py:1854-1859`,源码坐实)**先执行 pending 业务/控制 tool、`structured_response` 退出排其后** → handoff(pending 控制 tool)执行并 `Command(goto)` 转移控制权、结构化退出被延后 → **de-facto"控制转移 > 结构化输出"**;Pydantic output tool 先处理、第一个有效赢、败者补 stub。**crew-go 采纳**:同轮 `final_output` + `transfer_to_agent`/`route_to` 时**控制转移优先(handoff/route 赢),`final_output` 补 paired stub tool_result(422-safe)**——对齐 LangChain。fail-fast 仅作更粗 V1 备选。**唯一无直接先例**:框架不把 handoff 建模成 return-direct tool(crew-go 独有),套其优先级原则即可;swarm `Command(goto)` vs structured 那段是失败 fetch 影响标"推断"的部分(不承重)。
- **A5 → 随 A1 解**:Pydantic 保结构化(通用 tool-result 序列化走 JSON 非 str(),`messages.py:1253`)/ LangChain 拍平 string(无解)。**crew-go**:A1 让结构化结果以 JSON 进 Content → AgentAsTool 传 JSON 上去;V1"嵌套=JSON 字符串、父需要再 parse"(文档化),跨边界保对象(AgentEvent 加结构化字段)留后续。
- **A8 → 采纳统一(框架答案明确)**:Pydantic/LangChain 都只一套结构化机制,无"独立 planner tool-forcing";planner 的"plan vs respond 二选一"用 **union output type(= 多 output tool,first-valid-wins)** 表达,仍走同一路径。**crew-go**:P4 把 LLMPlanner 收敛进 `WithOutputSchema`(plan/respond = 多 output tool union),**不并存两套**。

**净结果**:5 HIGH 全有框架背书的解 —— A1/A4 直接采纳两家 pattern(A4 解比原方案干净)、A2 因"ModeTool 天然 fallback-safe"大半消解、A5 随 A1 解、**A3 采纳"控制转移>结构化"优先级+stub、A8 采纳统一**。除非业务否决方向,无悬而未决决策。A6/A7 文档化(组合约束 / 压缩丢回炉反馈 trade-off)。

### 已澄清的伪担忧(框架已覆盖,不是洞)
- fanout 并发写计数 → 引擎 collect-after-wait + 串行 ApplyAll 覆盖;return-direct OnceFlag race → 单线程聚合段置位无 race;压缩拆 tool_call↔tool_result 配对 → run_id 分组保配对不破。

> **交叉验证**:A1 经 2 独立 agent + 我亲读代码确认(板上钉钉);其余引用具体 file:line;设计阶段无代码可 sanity-revert,实现时须配回归测试(尤其 A4 写"校验失败+同轮 ask_user→Resume→再失败"断言计数**累积不重置**)。

---

## 1. 背景与现状

### 1.1 唯一结构化路径是 NewLLMPlanner 的私有 tool-forcing
| 机制 | 位置 |
|---|---|
| forced tool_choice | `agent_v2/llm_planner.go:129`(`WithToolChoice(schema.ToolChoiceRequired)`) |
| 手写信号 tool schema | `llm_planner.go:227-305`(`*schema.ToolInfo{Params: map[string]any{...}}`) |
| 解析 | `llm_planner.go:307-345`(`GetToolCalls` + `json.Unmarshal`) |

注释明说"借 tool_call schema 让 provider 替我们强校验结构化"。**思路对、应推广，但锁在 PlanAgent 私有**，且它是**无业务 tool 的纯信号场景**——直接套到带 toolkit 的 BasicAgent 会出 §4.4 的问题。

### 1.2 缺口
BasicAgent 最终答复是自由文本。业务要结构化结果只能自己塞 prompt + 自己 parse + 自己重试。**没有框架级"声明 schema → 约束 → 校验 → 回炉"闭环**。

### 1.3 为什么值得做
结构化输出 + 校验回炉是 agent 框架**标配**(Pydantic AI 立身之本 / LangGraph response_format / OpenAI SDK output_type / Vercel generateObject / CrewAI output_pydantic)，3 个独立调研 agent 各查各的都点名它。生产高频痛点(产出要被程序稳定消费)。

---

## 2. Provider 可行性矩阵

| Provider | `json_schema` strict | `json_object` | forced tool | tool_choice=required | endpoint | 现 client 可达 |
|---|---|---|---|---|---|---|
| **OpenAI** | ✅ | ✅ | ✅(可 strict) | ✅ | `/chat/completions` | ✅(加 wire 后) |
| **qwen**(默认测试模型) | ⚠️ 官方未确认，flash 大概率无 | ✅(需 "json" 关键词 + 关 thinking) | ✅(非 strict) | ✅(**qwen-3-coder 变体不支持**) | `/compatible-mode/v1` | ✅ |
| **deepseek** | ❌ 明确拒 | ✅ | ✅(非 strict) | ✅ | `/chat/completions` | ✅ |
| **Claude** | ✅ GA | — | ✅ strict | ✅ | **`/v1/messages`(非兼容)** | ❌ |

**OpenAI strict 硬约束**(规范化器要处理，§4.3)：每 object `additionalProperties:false`；全字段 `required`(可选转 `["type","null"]`)；不支持 `minLength/maxLength/pattern/format/minimum/maximum/minItems` 等；根必 object；嵌套 ≤5。

**per-provider 差异主要在"能力开关"，不在"schema 规则"**(优化 grep 纠正)：xAI/Grok 用**和 OpenAI 完全相同**的 schema 规范化规则(同样全 required)，差别只在"不支持 strict *tool* definition"。所以**不需要 per-provider 写不同的 schema 规则**，只需 per-model 的能力开关(§4.2)。

**关键结论**：
1. **forced tool call 是带 schema 约束、覆盖最广的主路径**(qwen/deepseek/OpenAI 全支持 function calling)。Pydantic AI + LangChain **都默认走 tool 路径**(`default_structured_output_mode='tool'` / `method="function_calling"`)——强印证。
2. **降级链**：`json_schema` strict → forced tool → `json_object` + prompt + 客户端校验。
3. **流式不改管线**：只在 final 聚合后 parse(crew-go chunk 不写 session.Events，流式与最终结构化天然分离)。

---

## 3. 业界框架模式(选架构 2 内联)

- **架构 1（LangGraph v0，已 deprecated）**：ReAct 循环结束后**额外调一次**结构化专用 LLM(`generate_structured_response` 独立节点)，多一次调用、v0 本身零回炉。
- **架构 2（Pydantic AI / Vercel / OpenAI SDK / LangChain v1）**：output schema 内联主循环，无额外调用，校验失败同循环内追加错误回炉。

**选架构 2**：复用 crew-go 现有 model 节点 retry 骨架 + forced-tool 先例；不新增节点不改图。**LangChain 自己都从 v0(架构1)迁到 v1(架构2)**——反向印证。

校验回炉三流派：**A 错误当新消息回喂**(Pydantic RetryPromptPart / LangChain ToolMessage，主流，抄这个)/ **B client 端确定性修复**(Vercel repairText，可选前置省 LLM)/ **C 硬约束不回炉**(靠 provider strict，json_schema 路径属此但仍配 A 兜底)。

---

## 4. 推荐设计

### 4.1 业务 API([设计])

```go
// [设计] 业务只声明一次 schema；框架按 provider 能力自动选机制 + 永远兜底校验重试
func WithOutputSchema(name string, schema map[string]any, opts ...OutputOption) agent_v2.Option

// OutputOption
func WithStructuredMode(m StructuredMode) OutputOption   // 显式指定机制；默认 ModeAuto(按 capability 选)
func WithMaxValidationRetries(n int) OutputOption        // 默认 1(对齐 Pydantic AI)
func WithRepairFunc(fn func(raw string, verr error) (string, bool)) OutputOption // 抄 Vercel，确定性修复前置

// StructuredMode：对齐 Pydantic AI 4 模式
type StructuredMode int
const (
    ModeAuto       StructuredMode = iota // 按 provider capability 选(默认)
    ModeTool                             // forced output tool(最广，默认落点)
    ModeResponseFormat                   // 原生 json_schema(strict-capable provider)
    ModeJSONObject                       // json_object + prompt 注入 + 客户端校验(弱档兜底)
)
```

业务取结果：统一 `json.Unmarshal(final.Content, &target)`——**postamble 把 `final_output.Arguments`(路径2)或 native JSON(路径1)合成进最终消息的 Content**(A1 解,§4.5/§5),业务不用自己从 tool_call 里抠、来源单一。

### 4.2 capability profile(per-model 多字段，client 自报 + 用户 override)

对齐 Pydantic AI `ModelProfile`(不是单个 strict bool)：

```go
// [设计] model 包：provider client 持有，按 model 名解析
type StructuredCapability struct {
    SupportsJSONSchema        bool // 原生 response_format json_schema
    SupportsJSONObject        bool
    SupportsToolChoiceRequired bool // forced-tool 路径前提(qwen-3-coder 为 false)
    SupportsStrictTool        bool // forced tool 能否带 strict
    DefaultMode               StructuredMode // 默认 ModeTool(对齐 Pydantic/LangChain)
}
```

- **openai client 默认**：全 true，DefaultMode 可 ModeResponseFormat。
- **qwen/deepseek wrapper 默认**：`SupportsJSONSchema=false`，`SupportsJSONObject=true`，`SupportsToolChoiceRequired=true`(qwen-3-coder 变体设 false)，DefaultMode=ModeTool。
- **用户 override**：`qwen.NewClient(key, "qwen-plus", openai.WithStrictSchema(true))` 应对未来 qwen 支持(qwen 已走 openai 兼容 wire，**通道早通，翻 flag 即用，零框架改动**)。
- **未知 client 保守默认**：非 strict → ModeTool + retry(全 provider 可用)。

挂载机制(Go 细节 §5)：`model.ChatModel` 可选 interface `StructuredCapable interface { StructuredCapability(model string) StructuredCapability }`，框架类型断言；不实现=保守默认。

**× fallback(A2 解,学 Pydantic 不聚合)**：`FallbackChatModel` 持异构候选(openai-strict + qwen-非strict),**不实现聚合 capability、不取交集**(Pydantic `FallbackModel.profile` 直接抛 NotImplementedError,拒绝聚合)。**关键领悟**:**ModeTool(final_output 当工具)是图层 tool 机制、不依赖 provider 的 response_format 能力——任何支持 function calling 的成员都认,异构 fallback 下天然安全**。所以:① fallback model 默认走 **ModeTool**(安全);② `ModeResponseFormat` 仅当"单 client 或全员 strict"才用;③ 用户显式 pin 了某成员不支持的 mode → 抛**不参与 failover 的能力类错误**(`ShouldFailover` 加一档"配置/能力类不切、上抛",类比 Pydantic `UserError` 不在 `fallback_on`),直接上抛而非一路 failover——避免把可恢复的 429 变成不可恢复的 400。

### 4.3 schema 规范化器(落地必需，否则 strict 路径必 400)

业务给的普通 `map[string]any` schema 大概率不合 strict(缺 `additionalProperties:false`、required 不全、带 `minLength`)。**必须**一个纯函数把它改成 strict 兼容(抄 Pydantic AI `OpenAIJsonSchemaTransformer`)：

```go
// [设计] model 包：递归规范化；strictCompatible=false 表示有无法满足 strict 的结构
func normalizeStrictSchema(s map[string]any) (normalized map[string]any, strictCompatible bool)
```

规范化动作(逐条对齐 Pydantic AI `profiles/openai.py`)：
1. 每个 `type:object` 强制 `additionalProperties:false` + 把所有 `properties` key 塞进 `required`(可选字段上游用 `["type","null"]` 表达)。
2. **strip 掉 strict 不支持的关键词**(`minLength/maxLength/pattern/format/minimum/maximum/minItems/uniqueItems` 等)——但**把删掉的约束转写进该字段的 `description`**(不是无声丢，模型仍看得到语义提示)。
3. `oneOf` → `anyOf`；strip `title/$schema/discriminator`；strict 模式删 `default`。
4. 根必 object：根是 array 时 `strictCompatible=false`。
5. **2 档规范化器**(per-provider 差异在这)：strict 档(OpenAI/xAI 共用，上述全套)+ inline-defs 档(qwen/deepseek，只内联 `$defs` 不强制 required)。**第一版 2 档够，不做 Google 方言**(打不到 Gemini)。

**注意**：crew-go 用手写 `map[string]any`(不像 Python 有 Pydantic 帮生成合规结构)，所以**必须做完整规范化，不能学 LangChain 偷懒只设 `additionalProperties:false`**——这是手写 schema 路线的必然代价。规范化器做成纯函数 → forced-tool(strict tool)和 response_format 两路径复用 + 可单测(喂畸形 schema 断言输出)。

### 4.4 两场景 + output tool 复用 crew-go 现有 return-direct(不新增 cond 分支)

output tool 是 **end-tool 不是"强制"**：`tool_choice=ToolChoiceRequired`(逼模型调**某个** tool、**不 pin output tool 名**)。**H1 纠正(框架重核)**：Pydantic AI 默认也是这个 `required`/`"any"` 语义(`_tool_choice.py:76`)——上轮只对 LangChain 修了 end-tool,Pydantic 没同步,其实两家同机制。所以**别写"强制调 final_output"**,是"required + 检测 final_output 被调"。

**场景 A — 有业务 toolkit**：`final_output`(参数=output schema)**追加进**业务 tool 列表;模型多轮自由调业务 tool,想结束时调 `final_output`。
**场景 B — 纯抽取(无 toolkit)**：只有 `final_output`,`required` → 第一轮即产出(等价 NewLLMPlanner 先例)。

**关键(读 crew-go 代码后的落点)**：`final_output` 做成 crew-go **现有的 return-direct 工具**——工具返 `tool.ReturnDirectMetaKey=true`(走现有 Metadata→Annotation→`basicReturnDirectCh` OnceFlag 协议链,Handoff/Routing 同款;**`basicReturnDirectCh` 是 compose_v2 包私有,agent_v2 只能走 Metadata 信号**),**不新增 cond fn 分支、不改现有 `len(GetToolCalls)==0` 终止**。校验在 tools 节点 handler(见 §4.5)。

**A3 多 return-direct 同轮裁决(采纳框架优先级)**:`final_output` 与 `transfer_to_agent`/`route_to`(也 return-direct)同轮时——**控制转移优先(handoff/route 赢),`final_output` 补 paired stub tool_result**(对齐 LangChain `factory.py:1854`"pending 控制 tool 先执行、结构化退出排其后")。**注意**:crew-go fanout **不 early-cancel**(`wg.Wait` 等全跑完、每 tool 都产 result),所以**无需** Pydantic 那种"塞占位 tool_return"——每个 tool_call 本就有配对 result,422 天然不破。

### 4.5 校验 + 回炉(读代码后的核心修正：路径 2 复用 ReAct 循环，不另造 model 节点内循环)

**这是"不照抄"的核心。** Pydantic/LangChain 在 model turn 内追加 retry 消息重发;但 crew-go 的 **ReAct 循环(tool 失败 → tool_result → model 重生成)天生就是这个回炉**,且 tool_call↔tool_result 配对由引擎自动保证。框架重核发现的 **H4**(必须保留失败轮 + retry 消息配对原 tool_call_id,否则悬空 422——Pydantic `openai.py:1547`/LangChain `factory.py:1238` 都手动保证)**在我们架构里引擎免费满足、根本不存在**。

**路径 2(output tool 模式)——校验在 tools 节点的 `final_output` handler**：
- 校验 Arguments：**通过** → 产 `tool_result(ok)` + 置 return-direct → 短路 postamble,从该 tool_call 的 Arguments 取结构化结果;**失败** → 产 `schema.ToolResultMessage(failedCallID, 错误)`(role=tool,配对 `final_output` 的 tool_call_id)、**不**置 return-direct → tools→model 边 → model 重生成(看到错误 tool_result 再调 final_output)。
- **回炉 = 既有 ReAct 循环**:失败 assistant(含 tool_call)留 channel 历史 + tool_result 引擎自动配对 → **H4 零成本满足、零 422 风险**(`schema.ToolResultMessage(callID, content)` 是现成构造器,`schema/message.go:85`)。错误文案抄 LangChain 极简模板 `"Error: {error}\n Please fix your mistakes."`。
- **预算(A4 解,学 LangChain 隐式,不建独立 counter)**:重试预算 = **数对话历史里 `final_output` 的 error tool_result 条数**(已在 cp 的 messages 里 → 跨 Resume 累积不重置,**无可丢弃 counter → H1 跨超步可见性 + H2 toolsNode 失败丢 write 两个洞一起消失**)。≥ `WithMaxValidationRetries`(默认 1)→ 抛 `StructuredOutputError`。**H7 边界(显式定义)**:模型在 required 下调了**业务 tool 但没调 final_output** → 不算校验失败、不消耗预算(正常 ReAct 轮,只受 maxRounds);只有"调了 final_output 但 Arguments 不合格"才计入(数那条 error tool_result)。

**路径 1(response_format / json_object 模式)——校验在 model 节点**(无 tool_call):
- `callLLM` 成功后校验 `out.Content`:失败 → 追加 **`schema.UserMessage(错误)`**(**H6**:prompted/native 无 tool_call,**绝不能塞 tool_result** 否则悬空 422)→ model 节点内重发(可复用 §4.5 model 节点循环骨架,append 语义跟 ReactiveCompact 的"压缩替换"不同),默认重试 1,耗尽抛 `StructuredOutputError`。
- LangChain 对 native(ProviderStrategy)校验失败是 **fail-fast 不回炉**(`factory.py:1158`);**我们选回炉**(默认 qwen 无 strict 硬保证,对齐 Pydantic AI 对 native 也回炉)。
- 可选 `repairFunc` 前置:JSON 轻微坏用确定性代码修一次省 LLM(抄 Vercel,只对 path 1 的 content / path 2 的 Arguments 字符串适用)。

**H3 纠正(回炉次数哲学)**:默认 1 + 耗尽抛 typed error **只对齐 Pydantic AI**(`UnexpectedModelBehavior`,output 预算全局单轨);LangChain 默认近乎无限(recursion_limit≈9999、无 typed error)。我们选 Pydantic 那套(保守 + 显式失败),**别写"对齐 LangChain 重试次数"**。

### 4.6 refusal / 截断(typed error 分流)

- **refusal**(json_schema strict 模式模型拒答)：`modules/common/message.go:12` 已有 `Refusal` 字段，但 openai client `convertResponse` **不解析**——落地路径 1 必须补解析 `choices[0].message.refusal`。遇 refusal **抛独立 `RefusalError`**(不当 schema 失败重试，业务 `errors.As` 区分"拒答"vs"格式错")。
- **截断**(`finish_reason=length`)：JSON 半截 parse 失败，但根因是 length 不是格式——**应触发 4 层压缩，不是无脑重试结构化**(重试只会再截断)。`StructuredOutputError` 带 `FinishReason`，校验循环据此分流。

### 4.7 保证等级可见(strictCompatible 检查 + ActualMode 回吐)

对齐 Pydantic AI 的 `is_strict_compatible`(优化 grep 发现，价值高)：
- 用户 `WithStrictSchema(true)` 但 `normalizeStrictSchema` 返 `strictCompatible=false`(如根是 array)→ **fail-fast 报错**告诉业务"这份 schema 无法 strict，请改 schema 或用非 strict"，而不是盲发等 400。
- 自动模式(ModeAuto)：`strictCompatible` 参与决定开不开 strict。
- **实际用的机制回吐业务**：`StructuredOutputError.ActualMode` + 成功路径可查 → 对齐"绝不静默退化"：退化(strict→tool→json_object)可以，但**可见**。

### 4.8 跨 provider 降级链
`ModeAuto` 下框架按 capability：`json_schema strict`(SupportsJSONSchema)→ strict tool(SupportsStrictTool)→ 非 strict forced tool(SupportsToolChoiceRequired)→ `json_object` + prompt 注入。第一版可先只暴露显式 `WithStructuredMode`，自动降级编排作 §9 后续阶段。

---

## 5. 集成点(file:line)

**model 层 wire**：
| # | 文件:行 | 改动 |
|---|---|---|
| 1 | `model/option.go:6-14` | `Options` 加 `ResponseFormat *ResponseFormat`；`ParallelToolCalls *bool` **可选**(H2 纠正：LangChain **v1 不设**它，那是 v0 `with_structured_output` 的事——非标配；主要对纯抽取场景防并发多调，第一版可不做) |
| 2 | `model/option.go:52` 旁 | `WithResponseFormat` / `WithJSONSchema` / `WithJSONObject` / `WithParallelToolCalls` |
| 3 | `model/openai/client.go:513-524` | `openaiRequest` 加 `ResponseFormat` + `ParallelToolCalls` 字段 + 类型定义 |
| 4 | `model/openai/client.go:161-216`(buildRequest)+ `:277-317`(convertResponse) | 映射 ResponseFormat 进 req；回填 `Refusal` |
| 5 | `model/`(新)`schema_strict.go` | `normalizeStrictSchema`(§4.3 纯函数，2 档) |
| 6 | `model/`(新或 client 上) | `StructuredCapability` + 可选 interface(§4.2) |

**wire 限制(优化 grep 发现)**：`schema.ToolChoice` 是 string 枚举(`auto/none/required`，`schema/tool.go:34-40`)，**发不出"强制具体某 named tool"** 的对象形式。场景 A 用 `required`(不指定具体 tool)就够，**不需扩 wire**；named-tool tool_choice 列为后续。

**agent_v2 层**：
- **路径2 校验在 tools 节点 `final_output` handler**(复用 return-direct,**不改 cond fn**——A1/A3 纠正:此前"cond fn 改判"是错的,现有 `len(GetToolCalls)==0` 终止不动)。
- **路径1 校验在 `makeModelFn`**(`basic_agent.go:1068-1232`)callLLM 成功后(model 节点内循环 + UserMessage 回炉)。
- **postamble `commitFinal` 为结构化路径合成最终消息**:把 `final_output.Arguments`(路径2)/native JSON(路径1)塞进 Content,**不复用跳过 tool_call 的 `lastAssistant`**(`basic_agent.go:1468`——A1 命门解)。
- BasicAgent Config 加 `WithOutputSchema` option。

**时序(场景 A 有 toolkit,路径2)**：
```
preamble → model 节点(压缩 → callLLM[挂业务 tool + final_output tool, tool_choice=required])
         → cond(len(GetToolCalls)==0? 仍是现有终止判定,不改)
         → tools 节点(并发跑所有 tool_call)
            ├ final_output handler: parse + 校验 Arguments
            │    通过 → tool_result(ok) + 置 ReturnDirectMetaKey → 短路 postamble
            │    失败 → error tool_result(配对 callID) → tools→model 边 → model 重生成
            │           (ReAct 循环即回炉;历史里 error tool_result 条数 ≥ 预算 → 抛 StructuredOutputError)
            └ 业务 tool: 正常跑 → 回 model
         → postamble(commitFinal: 把 final_output.Arguments 合成进最终消息 Content) → End
→ FinalMessage(events).Content 即结构化 JSON;业务 json.Unmarshal(final.Content, &target)
```

---

## 6. 与现有机制关系

| 机制 | 关系 |
|---|---|
| **NewLLMPlanner** | 场景 B(纯抽取)就是推广它；场景 A(带 toolkit)是新模式，**先例不能无脑套** |
| **ReactiveCompact** | 路径2 **不在 model 节点循环**——走 tools 节点 handler + ReAct 循环回炉(§4.5);路径1 才在 model 节点循环(与 ReactiveCompact 同位但触发/append 语义不同) |
| **4 层压缩** | 截断(finish_reason=length)分流给压缩，不重试结构化(§4.6) |
| **OpenAI 422 不变量** | 路径 2 回炉是 ReAct 循环的 error tool_result，**引擎自动配对**失败 tool_call——H4 的"留失败轮+配对"零成本满足,无需像 Pydantic/LangChain 手动保证(§4.5) |
| **ISSUE-81 并发 fanout** | fanout 不 early-cancel(`wg.Wait` 等全跑完、每 tool 产 result);同轮 final_output + 控制 tool 按 A3 优先级裁决(控制转移 > 结构化)(§4.4) |
| **qwen thinking 互斥** | qwen3 thinking 不支持 forced tool_choice / response_format(`model/qwen/client.go:35-41` 默认已关 thinking) |

---

## 7. 待定问题 / 风险
1. **qwen `json_schema` 实测**——官方未确认，落地前真 API POST `{type:"json_schema"}` 验。不影响主设计(主走 forced tool / json_object)。
2. **Claude 单列**——走 Anthropic Messages API，openai client 覆盖不到，需单独 client / 网关。
3. **schema 反射 helper 是否做**——底座 `map[string]any` 手写；reflect 从 struct 生成留增量/可选。**决策：不做(LlamaIndex/MS AF/SK 调研印证)**。C# 系(SK / Agent Framework)把 type→schema 下沉成共享反射工具 `AIJsonUtilities.CreateJsonSchema`(底层 `System.Text.Json JsonSchemaExporter`),但那是因为它们**默认走 json_schema** 且 C# 有标准库反射基建。crew-go **主路径是 forced tool**(对 schema 精度要求低于 strict json_schema)+ Go 无等价标准库(引第三方 jsonschema 库违"零依赖")+ 规范化器(§4.3)反而绕开了 SK"无规范化器 → `DateTime`/`Uri` 直接 400"的坑。手写 `map[string]any` + 规范化器是更优解,不引 reflect helper。
4. **named-tool tool_choice**——`schema.ToolChoice` 扩成支持具体 named tool(更精确的"只强制 output tool")留后续；第一版用 `required`。
5. **流式结构化 partial**——不做(属前端流式范畴，已划界)。

---

## 8. 业界对照表

| 框架 | schema 声明 | 默认机制 | 校验回炉 | 默认重试 | schema 规范化 |
|---|---|---|---|---|---|
| **Pydantic AI** | output_type(Pydantic) | **tool** | ✅ RetryPromptPart 追加 | 1 | `OpenAIJsonSchemaTransformer`(完整) |
| **LangChain v1** | Schema/ToolStrategy | **tool**(AutoStrategy 选) | ✅ ToolMessage 追加 | 靠全局步数 | 只设 additionalProperties(弱) |
| **LangGraph v0** | response_format | 额外调一次 | ❌(v1 补) | — | — |
| **Vercel** | zod | 原生 | repairText 单发 | 0 | — |
| **CrewAI** | output_pydantic | prompt+parse | ✅ guardrail | 3 | — |
| **OpenAI SDK** | output_type | 原生 strict | ❌(抛 + guardrail tripwire) | — | 自动 |
| **LlamaIndex** | Pydantic | **forced tool**(tool_choice=required；strict 默认 off) | ❌ 默认无(RetryOutputParser/Reflection opt-in) | — | Pydantic 生成 |
| **微软 Agent Framework** | C# class / Pydantic | `response_format json_schema` | ❌ 无(防御判空 / decorator 额外调) | — | `AIJsonUtilities` 反射 |
| **Semantic Kernel** | `typeof(C# type)` | `response_format json_schema strict` | ❌ 无 | — | `KernelJsonSchemaBuilder` 反射 |
| **eino(Go)** | 无结构化抽象 | — | — | — | — |
| **本设计** | `map[string]any` | **tool**(ModeAuto 选；end-tool/required) | ✅ 路径2 复用 ReAct 循环(tool_result 引擎自动配对,零 422)/ 路径1 UserMessage | 1，耗尽抛 typed err | `normalizeStrictSchema`(2 档) + 可选 repair |

> **横向定位**：声明一次 schema → 选机制 → client 校验，七家同构。但**框架级默认 validate+retry 回炉**只有 Pydantic AI / LangChain v1 / 本设计有——LlamaIndex(opt-in)/ MS AF / SK / OpenAI SDK / LangGraph v0 都缺。**这是本设计相对多数框架的领先点。** SK 因无 schema 规范化器导致 `DateTime`→400，反向印证 §4.3 规范化器的必要。

---

## 9. 实施计划

> **P2 / P3 阶段（下表后两行）2026-07-13 定为非目标（WONTFIX），不实施**——理由见「实现状态」节。下表保留作设计溯源；P0 + 聚焦 P1 已落地。

| 阶段 | 内容 | 验收 |
|---|---|---|
| **P0 model wire + 规范化器** | `ResponseFormat`/`ParallelToolCalls` option + openai client 字段 + `refusal` 回填 + `normalizeStrictSchema`(2 档) + `StructuredCapability` | 单测：req 带正确 response_format；refusal 透出；规范化器喂畸形 schema 断言输出 + strictCompatible |
| **P1 forced tool + 校验回炉(场景 B 纯抽取)** | `WithOutputSchema` + ModeTool + `final_output` 做 return-direct 自校验工具(失败产 error tool_result 走 ReAct 循环回炉 + 预算=数历史 error tool_result 条数(不建 counter) + 耗尽抛 `StructuredOutputError{FinishReason,ActualMode}` + RefusalError) + postamble 合成最终消息(A1) | 单测：mock 返不合规 Arguments → 断言产 error tool_result + model 重生成 + `FinalMessage().Content` 是结构化 JSON；耗尽抛 typed error；sanity-revert |
| **P2 场景 A(带 toolkit)+ response_format + repair** | `final_output` 加进业务 tool 列表 + 返 `ReturnDirectMetaKey`(不改 cond fn) + A3 多 return-direct 优先级裁决 + ModeResponseFormat(路径1 model 节点 UserMessage 回炉) + `WithRepairFunc` | 单测：业务 tool 多轮 + final_output 收尾；A3 控制转移优先;repair 命中省 LLM；422 不变量(ReAct tool_result 引擎自动配对) |
| **P3 ModeAuto 自动降级 + 集成验证** | capability 驱动降级链 + 真 LLM 冒烟 | forced tool 在 qwen 跑通；json_schema 在 OpenAI 跑通；文档回填「已实现」 |

> 测试纪律：mock 读 input 断言(下游收到 response_format/ToolMessage)；sanity-revert 看 fail 精确指向 bug。

## 10. 测试策略(provider 矩阵驱动)
- **forced tool(ModeTool 主路径)**：qwen/deepseek/OpenAI 全支持 → **默认测试模型 qwen-flash 可验**(真 LLM 冒烟主力)。
- **`response_format json_schema strict`**：qwen-flash/deepseek **不支持** → 只能 OpenAI 兼容端验(CI 可选)。
- **`json_object`**：qwen/deepseek 支持(注意自动补 "json" 关键词 + 关 thinking)→ 默认模型可验。
- **校验回炉 / 规范化器 / 422 配对 / refusal / 截断分流**：mock + 单测保证，不依赖真 LLM。

→ 真 LLM 冒烟主验 **forced tool(qwen)** + **json_object(qwen)**；json_schema strict 放 OpenAI 端;回炉/规范化/边界用 mock 单测。
