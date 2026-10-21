---
id: ISSUE-64
title: Subgraph 作头等节点（GraphBuilder.AddGraphNode）
status: ✅ 已实现（2026-05-03）
priority: 中
created: 2026-05-03
---

# ISSUE-64 · Subgraph 作头等节点

## 背景

当前 `compose.RunSubgraph` / `ResumeSubgraph` 是 helper，用户要把一个 `*Graph`
作为节点嵌入另一个图必须手写 NodeFunc 包装：

```go
b.AddNode("step", func(ctx context.Context, parentState *compose.GraphState) (*compose.GraphState, error) {
    // 1. Resume 路径检查 SubgraphCheckpoint
    if parentState.SubgraphCheckpoint("step") != nil {
        rOpts := compose.ResumeOptions{}
        if rc, ok := compose.GetResumeContext(ctx); ok {
            // 二选一转发 UserInput / UserInputs
        }
        result, err := compose.ResumeSubgraph(ctx, parentState, "step", subGraph, rOpts)
        if result != nil {
            parentState.LastOutput = result.LastOutput
        }
        return parentState, err
    }
    // 2. 首次执行：构造 inner state
    innerState := compose.NewGraphState(parentState.LastOutput)
    innerState.Session = parentState.Session
    runOpts := compose.GraphRunOptions{
        Checkpointer: compose.NewInMemoryCheckpointer(),
    }
    result, err := compose.RunSubgraph(ctx, parentState, "step", subGraph, innerState, runOpts)
    if result != nil {
        parentState.LastOutput = result.LastOutput
    }
    return parentState, err
})
```

20+ 行模板代码，且**容易漏关键细节**：
- 漏 `WithResumeUserInputs map` 转发 → 嵌套 ask_user 多分支配对失败
- 漏 OrchHooks 透传 → ISSUE-63 streaming chunk 跨子图边界丢失
- 漏 InMemoryCheckpointer → 内层 Resume 找不到 inner cp

eino 有 `AddGraphNode("sub", subgraph)`，LangGraph 有 `add_node(name, subgraph)`,
我们应该有一致的语法糖。

## 设计

### `GraphBuilder.AddGraphNode(id, sub, opts...)`

```go
b.AddGraphNode("step", subGraph)  // 默认契约
b.AddGraphNode("step", subGraph,
    compose.WithSubgraphInput(func(p *GraphState) *schema.Message { ... }),
    compose.WithSubgraphOutput(func(p, c *GraphState) { ... }),
    compose.WithIsolatedSession(),
    compose.WithoutHookPropagation(),
)
```

### 默认契约

| 维度 | 默认 | 自定义选项 |
|---|---|---|
| input | parent.LastOutput | `WithSubgraphInput(fn)` |
| output | parent.LastOutput = child.LastOutput | `WithSubgraphOutput(fn)` |
| Session | 共享父 session | `WithIsolatedSession()` |
| OrchHooks | 继承父（透传 streaming + observability） | `WithoutHookPropagation()` |
| Checkpointer | 内部 `NewInMemoryCheckpointer()`（嵌套 cp 协议） | 不可配（嵌套语义内部使用） |
| Resume | 自动 `SubgraphCheckpoint(id)` 检查 + UserInput/UserInputs 转发 | 不可配 |

### Hook 透传机制：ctx-based OrchHooks 注入

新增 `compose.WithOrchHooks(ctx, hooks)` / `GetOrchHooks(ctx)`。

`RunGraph` / `runGraphFrom` / `runGraphFromSuperstep` 在 opts.OrchHooks 非 nil 时
自动调 `WithOrchHooks(ctx, opts.OrchHooks)` 注入到 ctx。

`AddGraphNode` 生成的 NodeFunc 在子图 RunSubgraph 时调 `GetOrchHooks(ctx)` 拿到
父 hooks，传给子图的 GraphRunOptions.OrchHooks——**streaming chunk 跨子图边界
触达父 caller**，与 LangGraph callback 透传机制一致。

### CP 嵌套（复用 ISSUE-59 Phase 1 协议）

- 首次执行：`RunSubgraph` 在子图 InterruptError 时把内层 cp 嵌入
  `parentState.subgraphCheckpoints[id]`
- Resume：parent 重入本节点 → 检查 SubgraphCheckpoint(id) → 走 ResumeSubgraph
- 与 BasicAgent.RunNested 路径同协议，零冲突

## 改动文件

| 文件 | 说明 |
|---|---|
| `compose/subgraph_node.go`（新） | `AddGraphNode` + 4 个 SubgraphNodeOption |
| `compose/orch_hooks_ctx.go`（新） | `WithOrchHooks` / `GetOrchHooks` ctx 工具 |
| `compose/checkpoint.go` | `RunGraph` / `runGraphFrom` / `runGraphFromSuperstep` 三处入口注入 OrchHooks 到 ctx |
| `compose/subgraph_node_test.go`（新） | 5 个 e2e 测试 |

## 测试覆盖

- `TestAddGraphNode_BasicComposition`：默认 input/output 契约
- `TestAddGraphNode_HookPropagation`：父 OrchHooks 收到子图节点的 state.Send 事件 + OnNodeStart 触发内层节点
- `TestAddGraphNode_WithoutHookPropagation`：禁用透传后子图细节对父不可见
- `TestAddGraphNode_NestedInterruptResume`：子图 ask_user → 父 cp 嵌入子 cp →
  ResumeGraph(UserInput) 续跑（验证嵌套 cp + UserInput 转发）
- `TestAddGraphNode_CustomAdapters`：input/output 自定义适配器

## 与 RunSubgraph helper 的关系

`RunSubgraph` / `ResumeSubgraph` 不删——用作低层 API 供高级用户（如 BasicAgent
的 `executeReactGraph` nested 路径）。`AddGraphNode` 是它们之上的高层语法糖。

agent 层 BasicAgent.RunNested 仍走 RunSubgraph 直接调用——它有自己的 buildInitialMessages
等定制逻辑，不能用通用 AddGraphNode 包装。
