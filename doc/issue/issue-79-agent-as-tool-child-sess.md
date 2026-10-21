# ISSUE-79 · AgentAsTool 子 sub-agent 在 Resume 路径 sess 不可续传 + 审计完全不可见

**优先级**：中（设计缺陷——影响 Skill listing / Memory / 审计完整性）
**状态**：✅ 已修（2026-05-15）— AgentAsTool 默认半共享 child sess（破坏性改动）
**发现轮次**：R31 audit 视角 B HIGH-2（2026-05-15）

> **API 变更说明**（PR 3.3 commit e6505f9）：本 issue 涉及的 Memory 接口已由 store 体系替代——
> `Memory.Read` → `store.Search` / `InMemoryMemory` → `InMemoryStore`。下文 "Memory 行为分裂" /
> "InMemoryMemory.Recall" 等叙述保留历史 API 名作档案；ISSUE-79 半共享 sess 协议在 store 体系下等价工作
> （业务 Store 实现可用 `sess.RootSession()` 上追看父 history）。

## 修复方案（已实施）

参考 LangGraph + eino adk 调研结果，方案 3「精细化共享」**直接默认化**（项目未上线
按 CLAUDE.md 约定不留 opt-in 兼容路径）：

**eino adk 设计**（`/Users/gupengfei/program/go/src/eino/adk/runctx.go:386-391`）作为主参考——
Values 指针共享 + Events 独立的"半共享"语义直击 ISSUE-79 三个痛点（Skill listing
跨 Run 重发 / 审计层断点 / Memory 跨 Run 失效），但 crew-go 保留**比 eino 更强**
的 NS 物理分离 cp（不照搬 eino bridgeStore 嵌入式 cp）。

### 实施

1. **`agent_v2/session.go`**:
   - `RunSession` 加 `parentSess *RunSession` 字段（反向指针）
   - 新公开 API `NewSharedChildSession(parent *RunSession) *RunSession`——
     构造半共享 child sess（values 指针共享 + Events 独立 + parentSess 指针）
   - 新公开方法 `ParentSession()` / `RootSession()`——业务 Memory.Read 沿父链上追

2. **`agent_v2/agent_tool.go`**（**破坏性改动**——但项目未上线无影响）:
   - `Call()` 默认构造半共享 child sess——eino adk 同款行为
   - 父 ctx 无 sess（直接裸调 tool.Call 不经 BasicAgent）时 fallback nil →
     sub-agent.Run 入口 NewRunSession 兜底
   - **签名已变** (#7 d085111 audit follow-up): 从 `AgentAsTool(a, description string)` 改为 `AgentAsTool(a Agent, opts ...AgentAsToolOption) tool.Tool`——description 走 `WithAgentToolDescription("...")` option (默认从 `a.Description()`, 空 panic), 新增 `WithAgentToolInputSchema` / `WithAgentToolFullHistoryInput` 选项 (eino 范式)

3. **回归测试** `agent_v2/r31_issue79_test.go`（4 个）:
   - `TestAgentAsTool_ISSUE79_SharedSession`：默认 values 共享 / events 独立 / RunID 沿用
   - `TestAgentAsTool_ISSUE79_RootSessionTraversal`：多层 sess 链 RootSession 追到根
   - `TestAgentAsTool_ISSUE79_NilParent_Fallback`：ctx 无父 sess 时 fallback
   - `TestAgentAsTool_ISSUE79_SkillListingAccumulation`：业务场景跨 sub-agent KV 累积

   sanity-revert: 把 `Call` 改回 nil sess，2 测试 (SharedSession +
   SkillListingAccumulation) 准确 FAIL；2 测试 (RootSessionTraversal /
   NilParent_Fallback) 测试独立 helper 路径不依赖 Call 改动仍 PASS。

### 业务用法

```go
// 直接调用——sub-agent 自动半共享父 sess（values 指针共享 + Events 独立 +
// RunID 沿用 + parentSess 反向指针）
tk.Register(agent_v2.AgentAsTool(researcher,
    agent_v2.WithAgentToolDescription("do research"))) // #7 d085111 后 opts 风格
```

业务 Memory 实现想看父对话历史时：

```go
func (m *MyMemory) Read(ctx context.Context) ([]*schema.Message, error) {
    sess := agent_v2.GetSession(ctx)
    if sess == nil { return nil, nil }
    // 沿父链上追到根 sess
    return sess.RootSession().HistoryFor(""), nil
}
```

### values 共享 namespace 约定

业务自管 key 前缀避冲突——多 sub-agent 共享同 values map，如两 sub-agent 都用
`"current_step"` 作 key 会互相覆盖。建议加「sub-agent 名」前缀，如
`"researcher.current_step"`。

框架自己往 values 里放东西时会自带 namespace 前缀，与业务 key 不冲突。
（曾以增量 skill listing 的 `SkillListingSessionKey` 为例，该功能 2026-07 已删。）

---

## 原始问题描述（保留备查）

## 问题

`agent_v2/agent_tool.go:55` AgentAsTool.Call 用 nil session：
```go
events, err := t.agent.Run(subCtx, nil, schema.UserMessage(inputStr))
```

子 BasicAgent.Run line 124-125 入口 fallback：
```go
if session == nil {
    session = NewRunSession()  // 全新 child sess（NewUUID RunID + 空 events + 空 values）
}
```

## 业务影响

### 1. 子 sess.values 全空

子 sub-agent 配 Skill 时通过 `sess.GetValue/SetValue(SkillListingSessionKey)` 跨 Run
跟踪 `sentSkillNames`——AgentAsTool 模式下子 sess 每次都是 NewRunSession 全空 →
**子 sub-agent 每次 Resume 都重发全 Skill listing**（增量 listing 协议失效）。

config.go:523-526 注释："必须 WithResumeSession 续传"——AgentAsTool 没续传，矛盾。

### 2. 子 sess.Events 永远空 → 审计层完全不可观测

父 BasicAgent 调 AgentAsTool 内含 ask_researcher → 子内部跑 5 轮 LLM + 3 个
search → 父 sess.Events 只见 1 条 `tool_result(content=最后 assistant 摘要)`，
**子内部 5 轮 LLM 调用 + 3 个 search 工具调用全部不可见**。

业务想做"子 tool failed 详情回溯"做不到（FinalMessage 只取最后一条 assistant）。
plan_chat_web 8 sub-agent 全走 AgentAsTool → 用户问"刚才你 search 的第二条结果是什么"
→ chat_entry LLM 自身历史无 → 答不出。

### 3. 子 Memory 行为分裂

子 BasicAgent 配 Memory 时：
- 子 sess 是临时 NewRunSession → Memory.Read 看不到本 conv 历史（sess.HistoryFor 永远空）
- 只有跨 Run Memory（持久层 InMemoryMemory.Recall）有效
- 业务期望"同 conv 内 sub-agent 累积"达不到

### 4. 子 ctx WithSession 静默覆盖父 sess

`agent_v2/basic_agent.go:127`：
```go
ctx = WithSession(ctx, session)  // 子 NewRunSession 覆盖父 sess
```

子节点函数（preambleFn / hooks）用 `GetSession(ctx)` 永远拿到子临时 sess——业务自定义
hook（SkillSelectionHook / HistoryCompactor / Memory.Read）想读父 sess 看跨 agent
视图（如 plan_chat_web 希望"sub-agent 看父 chat 历史用户问题"）拿不到父历史。

## 当前设计的合理性

子 sess 隔离避免父子 events cross-talk（子 LLM 不被父历史污染）——是合理设计选择。

## ~~修法（业务侧 opt-in 选项）~~ — 历史提案, 已被默认化方案替代

> ⚠️ **历史提案存档**: 此 section 原提议加 `WithSharedParentSession` / `WithMergeChildEventsToParent` opt-in 选项 — 实际落地走 **默认半共享** 方案 (header status 行 "破坏性改动按 CLAUDE.md 约定不留 opt-in 兼容路径"). 下面 example 用旧 `AgentAsTool(sub, "desc", ...)` 签名 + 已废弃的 opt-in option 名, 仅作历史档案保留, **业务不要直接拷**.

```go
// 历史提案 (不实施): 业务自选共享/独立
// agent_v2.AgentAsTool(sub, "desc", agent_v2.WithSharedParentSession())
// agent_v2.AgentAsTool(sub, "desc", agent_v2.WithMergeChildEventsToParent())
//
// 实际落地: 默认半共享 (#7 d085111 后 opts 签名)
//   tk.Register(agent_v2.AgentAsTool(sub, agent_v2.WithAgentToolDescription("desc")))
// values 指针共享 + Events 独立 + parentSess 反向指针 + RunID 沿用

## 当前防护

无——业务必须自管 ctx-value 通道传跨 agent 视图，或用 SequentialAgent / PlanAgent
嵌套（共享父 sess）替代 AgentAsTool。

## 关联 ISSUE

- ISSUE-78: AgentAsTool childNS 缺 tc.ID 维度（同 audit 角度）
