# ISSUE-91 · 外部 Agent 接入编排

**状态**　已实现。

通过 `agent_v2.NewAgentFunc(name, description, fn)` 把同步文本函数接入 Sequential、Parallel 或 Plan 等编排。
函数可以调用已有的 HTTP、gRPC 或其他框架的 Agent。输入是编排交给它的 `schema.Message.Content`，
返回字符串成为最终 assistant 消息。

```go
external := agent_v2.NewAgentFunc("external", "查询外部服务", func(ctx context.Context, input string) (string, error) {
    return service.Query(ctx, input)
})
workflow := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("workflow"))
workflow.AddStep("lookup", external)
```

适配器复用单节点 GraphAgent。配置 Checkpointer 后，续跑会重执行尚未成功的外部函数，
父编排已提交的步骤保持原产出。外部写入应使用幂等键，并响应调用方的取消。框架能管理该节点的
存档和上下游调度，外部系统自己的状态由业务适配。流式返回或外部 HITL 协议可通过自定义 Agent 接入。

[实现](../../agent_v2/agent_func.go) · [续跑回归](../../agent_v2/agent_func_test.go)
