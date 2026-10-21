# 工具可见性 / 裁剪三方对照

> 对照 **crew-go v2 / eino v0.8.0（+ eino-ext openai@v0.1.14）/ LangGraph 1.2.5**。
> 主题：运行时控制"这一轮 LLM 能看到 / 调用哪些工具"——按上下文裁到子集、白名单约束、渐进披露。
>
> 关联：**渐进披露（ToolSearch）** 的三方对照已在 [`eino-langgraph-upgrades.md`](eino-langgraph-upgrades.md) §1.2 + [`../../v2/toolsearch.md`](../../v2/toolsearch.md) 覆盖，本篇不重复；本篇聚焦**裁剪 / 白名单轴** + **框架工具豁免**决策。

## 触发场景

- 设计 / 改动"按上下文裁剪 LLM 可调工具子集"的能力
- 纠结裁剪放哪层（model option / 编排 hook / bind list）
- 框架内置工具（ask_user / activate_skill / ...）与业务工具裁剪的边界

## 一、三方机制对照

| 维度 | crew-go v2 | eino | LangGraph |
|---|---|---|---|
| 每轮裁业务工具子集 | `WithToolFilter(fn)`：modelFn bind 前裁 ToolInfos（`agent_v2/config.go:308`）| `ToolsNode.WithToolList(tools...)` 每轮换工具集（`compose/tool_node.go:56`）| model 工厂 `model(state)→bind_tools(subset)`（`chat_agent_executor.py:281`）|
| model 层白名单 | `WithAllowedTools(names...)`：client 裁 `req.Tools`（`model/option.go:63`）| `WithToolChoice(c, allowedNames...)`：设 OpenAI `tool_choice:allowed_tools`、**保全 req.Tools**（`components/model/option.go:52`）| `bind_tools(tools, tool_choice="name")`（langchain_core）|
| 白名单含 req.Tools 没有的名 | 静默忽略 | **fail-fast 报错**（`populateToolChoice`，eino-ext openai `chat_model.go:981`）| — |
| 框架工具 vs 业务工具区分 | **有**（实例上的封闭标记 `frameworkMintedTool`，`agent_v2/framework_tool.go`；2026-10-12 之前是 9 个名字的名单）| 无此概念（框架能力在 adk 中间件层）| 无（核心不内置框架工具）|
| 裁剪豁免框架工具 | **✅ 是**（ToolFilter 恒留框架工具）| N/A（中间件与裁剪正交、不同层）| N/A（无框架工具）|
| 渐进披露 | `WithToolSearch`（见 toolsearch.md）| `dynamictool/toolsearch` 中间件 | 核心无 |

## 二、各家详解

### crew-go：裁剪落在 modelFn bind list + 框架工具豁免

- **`WithToolFilter(fn)`**（业务工具裁剪）：每轮 model 调用前调 `fn(ctx, input) []string` 拿业务工具白名单，`filterToolsByName`（`agent_v2/tool_search.go:158`）在 `llm.WithTools` 前裁 `ToolInfos`（调用点 `agent_v2/basic_agent.go:1214`）。与 ToolSearch 的 `filterVisibleTools` 同位置正交叠加（先 ToolSearch 揭示、后 ToolFilter 裁）。`fn` 返 nil = 不裁、返子集 = 只留这些业务工具。
- **`WithAllowedTools(names...)`**（model 层白名单）：openai client（`model/openai/client.go:196`）构建 `req.Tools` 时按名过滤——**客户端裁 tools 列表**，provider-agnostic（qwen / deepseek 等都生效）。是给"直接用 model"或其他编排的底层原语；BasicAgent 走 ToolFilter，两者独立。
- **框架工具豁免**：`filterToolsByName` 查绑定那份工具表里这个名字下挂的实例带不带「框架铸的」封闭标记（`isFrameworkMintedIn`，`agent_v2/framework_tool.go`），框架工具恒保留——与审批层豁免同一判定。**按实例不按名字**（2026-10-12 改）：原先是 9 个名字的名单（含规划器的内部名 `plan` / `respond`），业务工具恰好叫这些名字就绕过白名单、审批、工具账（实测）。

### eino：model option 白名单（OpenAI allowed_tools）+ 中间件管框架工具

- **`AllowedToolNames`**（`components/model/option.go:52`）由 provider 消费：eino-ext openai `populateToolChoice`（`chat_model.go:981`）**不裁 req.Tools**，而设 OpenAI 原生 `tool_choice:{type:allowed_tools}`；白名单工具必须在 req.Tools 存在否则 fail-fast。依赖 provider 支持 `allowed_tools` 类型。
- 编排层 **`ToolsNode.WithToolList`**（`compose/tool_node.go:56`）每轮可换整个工具集。
- 框架能力（tool_search / skill）是 **adk 中间件**（`BeforeModelRewriteState` 改 `state.ToolInfos`），与 AllowedToolNames **正交、不同层**——故 eino 不存在"业务裁剪误伤框架工具"问题。

### LangGraph：业务 bind_tools 子集（不 bind 即裁）

- 核心无工具裁剪原语；`create_react_agent` 接受 model 工厂 `Callable[[State, Runtime], BaseChatModel]`（`chat_agent_executor.py:281`），业务在工厂里 `bind_tools(subset)`——**不 bind 即裁**（隐式负向）。
- `bind_tools(tools, tool_choice=...)` 的 tool_choice 支持 auto / none / required / 指定工具名（langchain_core）。
- 核心不内置框架工具（grep 确认无 skill / toolsearch / ask_user）——无"框架工具豁免"概念。`num_builtin` / `llm_builtin_tools`（`chat_agent_executor.py:554`）指 provider 端 server-side 工具，与本主题无关。

## 三、crew-go 取舍（具体可证伪）

- **优势**：`WithAllowedTools` 客户端裁 `req.Tools` 是 provider-agnostic（覆盖 qwen / deepseek / openai 全部），不像 eino 依赖 OpenAI `allowed_tools` 原生类型。工具裁剪与 ToolSearch 渐进披露正交叠加（不同机制可同时启用）。
- **代价**：每轮裁不同子集会改 `req.Tools` → 破坏 prompt cache（裁剪本就按上下文变，可接受）；不像 eino 保全 req.Tools 用 tool_choice 约束更 cache 友好但依赖 provider。

## 四、决策记录

1. **WithToolFilter 做成通用 hook、不绑 RoutingAgent**：裁剪是 model 层关注点，任何 BasicAgent 都能用；RoutingAgent classifier 分类后裁工具只是用法之一。对标 langgraph 动态 model（但返工具名子集、不返新 model 实例）。
2. **WithAllowedTools 客户端裁 req.Tools，不学 eino 的 `tool_choice.allowed_tools`**：eino 方式依赖 OpenAI 原生 allowed_tools 类型，qwen / deepseek 等兼容 provider 不一定支持；客户端裁 req.Tools provider-agnostic，覆盖我们所有测试 / 生产 provider。
3. **ToolFilter 豁免框架工具（footgun 修复，2026-06）**：我们把 skill / toolsearch / ask_user / approval 做成框架内置一等工具，它们与业务工具**同在 modelFn bind list**——业务 filter 漏带框架工具名会静默关掉搜索 / 技能 / 问用户 / 路由。eino（框架能力在中间件层、与裁剪正交）、LangGraph（核心无框架工具）都无此场景，**无现成可抄**；按我们审批层已有的框架工具豁免先例统一（当时那道豁免按名字判，2026-10-12 起两处一起改成按实例判）：ToolFilter 只裁业务工具，框架工具恒留。选"豁免"而非"全裁"是为与 approval 一致（全裁会让同一批框架工具上 approval 豁免、ToolFilter 能裁，态度分裂）。
4. **业务强制的 tool_choice=required 只管到模型第一次调工具（2026-10-13 定）**：同 OA 的 `reset_tool_choice` 与 MAF 的 `_reset_required_tool_choice`——调过之后还强制，模型永远不能以文本收尾。「调过工具」按**模型发起过调用**算、不看结果：失败、被拒、调了不存在的名字都算（MAF 处理完一批调用就复位，报错与未知工具名照样；OA 按 `ToolCallItem` 记、不看输出）。**框架的 tool_search 只在第一批（全是检索、且解锁了新工具）不算，之后的都算**。OA 把托管的 tool_search 整个排除（`tool_use_tracker.py` 的 `_TOOL_USE_RESET_TRACKING_ITEM_TYPES`），前提是它的检索在服务端、跟随后的调用发生在模型同一次响应里（runner 不支持客户端执行的 tool_search，`turn_resolution.py` 直接报错）；我们的 tool_search 是客户端工具、每次占一整轮，照搬整个排除时检索不到就每轮再检索、被逼到软降落轮（`df350bc0` 审计实测 6 次模型调用、改动前 2 次）。同日先定的是「每批解锁了新工具都不计」，审计实测工具库比轮次预算大时（8 个被延迟工具、预算 6 轮）照样被逼到软降落——MCP Hub 几十上百个工具、关键词检索几乎什么词都命中，正是渐进披露的主场景；改为只豁免第一批，多强制至多一轮，与工具库规模无关。代价是第一次检索解锁的不是要的工具时，第二次检索就用掉了额度（之后不再强制，模型仍可自己去调）。eino 不复位。
