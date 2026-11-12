# ISSUE-90 · Plan Web 会话生命周期

**状态**　已实现会话查询、Redis 多副本续跑、删除和临时会话。

## 配置

`go run ./example/orchestration/plan_chat_web -mock` 使用模拟模型。Redis URL 为空时，会话存储使用进程内存。
部署多个副本时，各副本配置相同的 `PLAN_WEB_REDIS_URL`（也可用 `-redis-url`）。会话元数据和 Checkpointer 使用同一个 Redis，
执行、续跑和删除共用任务租约。Redis 模式在超步边界同步保存进度。生产配置应开启 Redis 持久化。
经 HTTPS 反向代理部署时，可加 `-secure-cookie`。 [Redis 接入](../../redis/doc.go)

## HTTP 接口

打开首页或开始第一场会话时服务端设置 `crew_plan_owner` Cookie。浏览器和 API 客户端
凭同一个 Cookie 查找会话，另一浏览器的 Cookie 对该会话得到 404。应用需要组织级账号时，
由部署网关管理身份和访问权限。

| 接口 | 行为 |
| --- | --- |
| `GET /api/chat?q=...` | 开启 SSE 会话，可加 `ephemeral=true` |
| `GET /api/chat?session_id=...&action=continue` | 从存档续跑，`action=abort` 清档并标为 cancelled |
| `POST /api/interrupt?session_id=...` | 标记暂停，执行该任务的副本在下一次图内钩子检查时处理 |
| `GET /api/sessions?status=interrupted&limit=20` | 查询本 Cookie 的会话索引，按创建时间倒序。limit 最大 100 |
| `GET /api/sessions/{id}` | 查询状态、模式、时间和历史计数 |
| `DELETE /api/sessions/{id}` | 删除会话和存档。任务仍持有执行许可时返回 409，成功返回 204 |

默认终态保留 60 分钟。Redis 使用键的过期时间，内存模式使用 sweeper。挂起任务保留供续跑。
临时会话进入终态后立即清理。终态会话仍在索引时，Resume 返回 410。

## 恢复边界

会话索引保存状态及时间，SSE 事件由当前连接消费。恢复从最近的 Checkpoint 开始。若进程在
图已完成清档、会话终态写入之前退出，索引会暂留 running，再次 Resume 会因存档缺失进入 error。
外部工具应按自己的幂等键处理重试。 [Plan Web 冒烟](../description/smoke-test-recipes.md#2-plan_chat_web-mock--hitl-resume30-秒)
