## 改了什么

<!-- 一两句：问题 + 出路。不写变更日志（读的人绝大多数没见过旧行为）。 -->

## 检查单

- [ ] 新增 / 改动的 exported API 都有单测（公开行为黑盒 `package xxx_test`；内部分支白盒 `_internal_test.go`）
- [ ] `go test ./...` 通过；改了 sess / channel / sync / map 共享态的，`go test -race ./...` 也过
- [ ] 改了 cp / 嵌套 / Resume / ns：`go run ./example/orchestration/nested_smoke` + `plan_chat_web -mock` 两个冒烟跑过（[smoke-test-recipes](../../doc/description/smoke-test-recipes.md)）
- [ ] 改了 `a2a/`：`example/a2a/hitl_smoke` + `llm_smoke` 跑过
- [ ] 高风险 commit（session / checkpointer / graph_loop / `_agent.go` / sync / channel / interrupt）：独立 agent 单视角审 + 交叉验证，`bash .claude/scripts/mark-audit.sh status` 两个 marker 都在
- [ ] 改动的注释 / 活文档过了 commentaudit；教学面（godoc 示例 / 设计文档 / CLAUDE.md 速查表）同步改
- [ ] 破坏性改动：所有调用点同步改完，不留 deprecated / fallback 路径（项目未上线，不做兼容）

## 怎么验

<!-- 一条能跑的命令 / 一个测试名。backlog 条目也要带这个（doc/code/open-items-root-cause.md）。 -->
