# 下一版升级说明

## 部署前

- 根模块与三个子模块要求 Go 1.26.0 或更高版本。仓库工具链与 GitLab CI 使用 Go 1.26.4。
- 对共用同一 Redis 或 SQL 存档域的进程，先停旧副本，再启动新版。新版用任务租约协调执行、续跑和清档。 [任务租约](../v2/checkpoint.md#61-共享存档的任务租约)
- 恢复执行前会写入 `CPStatusResuming`。回退前让新版处理或显式放弃这类存档，旧版本可以按遗留 frontier 再次执行。自定义 Checkpointer 需要实现原子 `ClaimResume` 和带条件的 `DeleteSubtree`。 [持久准入](../v2/checkpoint.md#持久续跑准入)
- 新版恢复要求带版本的图存档身份。切换前让旧版处理仍在途的任务，或显式放弃，再统一切换读写版本。旧档可以读取和导出。回退前由新版处理新版产生的待答档。 [存档身份](../v2/checkpoint.md#43-graphid-防串台)
- 新建路由升级链时给每个 `WithRouteDecider` 档位使用稳定名称。 [路由决策](../v2/route-decider.md)
- 使用 `SendDirective.Input` 的部署应统一 Checkpoint 的读写版本。Schema 19 增加 `frontier_inputs` 和 `deferred_sends[].input`，缺少这两个字段的旧存档仍可读取。旧进程读取新存档会忽略这些字段，回退前先处理挂起的私有投递。 [存档结构](../v2/checkpoint.md)
- Schema 23 将 AutoCompact 审批绑定到本次摘要调用和实际送模视图。经过图身份校验后，未绑定的旧摘要审批会重新提问，业务提问沿用原 ID。 [摘要提问](../v2/context-compaction.md#压缩结果的复用单次-run-内的备忘与跨-run-的压缩边界)
- 旧工具账已登记展示但未记录展示方式时，AG-UI 续跑向前端发送空正文，模型与历史沿用存档中的定稿结果。 [AG-UI 事件映射](../v2/agui.md)
- A2A 使用 `a2a-go/v2 v2.7.0`。新版服务端要求请求携带 `A2A-Version: 1.0`。`server.Card` 默认声明 `text/plain` 和 `application/json`，按 `text` 匹配名片的客户端需更新。自装配服务端应按请求 owner 隔离任务读取、列表与重订阅。旧挂起档缺少答复投递状态时，续跑需能读取远端任务做对账。 [A2A 接入](../v2/a2a.md)
- MCP stdio 通过 `WithStdioEnvAllowlist` 选择继承变量，服务明确需要的凭据也可用 `WithStdioEnv` 提供。 [MCP](../v2/mcp.md)

## 迁移现有 SQL 表

以下语句使用默认表名 `crew_store`、`crew_checkpoint`。自定义表名按应用的配置替换。自定义 Checkpointer 表可用相同配置的 `Checkpointer.Setup` 建租约表，长表名的租约表名由框架派生。身份列按字节比较。先停写并备份，再执行迁移和读写验证。MySQL 旧排序规则已经合并的键只能从应用数据或备份核对。

使用 SQL 共享 AG-UI 会话的部署重跑 `AGUISessionStore.Setup`，创建独立的取消补偿轮询租约表。 [持久化取消补偿](../v2/agui.md#持久化取消补偿)

### PostgreSQL

```sql
ALTER TABLE crew_store
    ALTER COLUMN ns TYPE TEXT COLLATE "C",
    ALTER COLUMN data_key TYPE TEXT COLLATE "C";

ALTER TABLE crew_checkpoint
    ALTER COLUMN sid TYPE TEXT COLLATE "C",
    ALTER COLUMN ns TYPE TEXT COLLATE "C",
    ALTER COLUMN parent_ns TYPE TEXT COLLATE "C",
    ALTER COLUMN graph_id TYPE TEXT COLLATE "C";

CREATE TABLE IF NOT EXISTS crew_checkpoint_task_lease (
    sid TEXT COLLATE "C" NOT NULL PRIMARY KEY,
    owner_token TEXT COLLATE "C" NOT NULL,
    expires_at BIGINT NOT NULL
);
```

迁移后重建应用连接池，清除旧连接缓存的预编译语句。连接复用时第一次查询可能返回 SQLSTATE `0A000`。

### MySQL

```sql
ALTER TABLE crew_store
    MODIFY ns VARBINARY(2048) NOT NULL,
    MODIFY data_key VARBINARY(1020) NOT NULL;

ALTER TABLE crew_checkpoint
    MODIFY sid VARBINARY(1020) NOT NULL,
    MODIFY ns VARBINARY(2048) NOT NULL,
    MODIFY parent_ns VARBINARY(2048) NOT NULL DEFAULT '',
    MODIFY graph_id VARBINARY(1020) NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS crew_checkpoint_task_lease (
    sid VARBINARY(1020) NOT NULL PRIMARY KEY,
    owner_token VARBINARY(64) NOT NULL,
    expires_at BIGINT NOT NULL
) ENGINE=InnoDB;
```

已有索引的 MySQL 表直接执行上述迁移。新表可用 `Setup` 建表、索引和租约表。迁移后用只差大小写或尾随空格的键验证隔离。

### SQLite

已有 Checkpointer 数据库重跑 `Checkpointer.Setup`，创建任务租约表。存档表的身份列沿用 SQLite 默认的 `BINARY` 排序规则。

## 迁移现有 Redis 存档

Redis Checkpointer 将任务 ID 编为独立的 base64url 键段，namespace 及三个枚举索引沿用原值。
已有存档的部署先停止同一 Redis 前缀下的所有存档读写者并备份，再对每个使用中的前缀构造
`redis.NewCheckpointer` 并调用 `(*Checkpointer).MigrateLegacyKeys(ctx)`。迁移成功后启动新版，
查询已有挂起项并验证续跑。迁移中断后可重试，已迁移的记录会跳过。旧数据的记录归属或元数据不一致时，
按错误中的任务与 namespace 核对备份后再继续。旧格式已经覆盖的内容只能从备份或应用数据恢复。
需要回退时使用迁移前的备份。
会话记录、任务租约及 Store 数据沿用原键。 [Redis Checkpointer](../../redis/checkpointer_migration.go)

## 发布检查

确认 GitLab pipeline 中 `test-postgres` 和 `test-mysql` 的结果。两个 Job 分别使用 PostgreSQL 16.9、MySQL 8.0.35，覆盖 Store、Checkpointer、HistoryStore、租约和放弃与续跑竞争。 [GitLab CI](../../.gitlab-ci.yml)

真实模型的验证命令见 [冒烟测试速查](../description/smoke-test-recipes.md)。Qwen 使用 `QWEN_API_KEY`，Claude 使用 `ANTHROPIC_API_KEY`。Claude 的计费分类需要一次真实的计费拒绝才有观测结果。

## 应用接入

- Plan Web 可配置共享 Redis，支持跨副本续跑、会话状态查询、删除和完成即清理。 [Plan Web](../issue/issue-90-plan-chat-web-lifecycle-prod.md)
- 外部文本函数可用 `agent_v2.NewAgentFunc` 加入编排。配置存档后，失败或取消的函数可能重跑，外部副作用需保持幂等。 [外部 Agent](../issue/issue-91-external-agent-adapter.md)
- Skill 的核心流程留在 `SKILL.md`，详细资料放在参考文件，激活后按预算自动读取或由模型按需读取。 [Skill 参考文档](../v2/skill.md#按需读取参考文档)
- AG-UI 用户媒体按模型适配器支持的格式透传。配置共享 SQL 会话后，审批可跨实例续跑，重连可查询当前挂起项。 [AG-UI](../v2/agui.md#共享会话)
- A2A JSON-RPC 默认传输按 HTTP 状态识别 `GetTask` 的 401/403，首次答复可发出，已经开始投递的答复在无法对账时保留挂起。 [A2A](../v2/a2a.md)
