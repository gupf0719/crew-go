# ISSUE-90 · plan_chat_web 生产化 lifecycle (历史查询 API + Redis backend)

**优先级**：LOW (本 session 已对齐 LangGraph 6 状态 + TTL sweeper 模式, 缺业务诉求驱动的 production API)
**状态**：📋 跟踪中 (2026-05-17)
**触发**：plan_chat_web 多场景测试发现 dead 410 路径, 对照 LangGraph Platform
设计后改为"runs 不删 + 6 状态 + TTL sweeper", 但 LangGraph 还有 2 处生产能力
本 session 未实施 (业务诉求驱动加, YAGNI 不预先做).

## 已完成 (本 session)

参照 LangGraph Platform run status (`LANGGRAPH_PLATFORM.md:247,380`) 重设
plan_chat_web lifecycle:

| 改动 | 来源 |
|---|---|
| runSlot 加 `status string` 6 状态 (`running/interrupted/success/error/cancelled`) | LangGraph run.status |
| 删 `runs.Delete` 完成后 (slot 留 runs 让 history 可查) | LangGraph thread 默认持久化 |
| 加 `lastActivity time.Time` + `createdAt time.Time` | LangGraph thread.created_at |
| `-session-ttl-minutes` flag (默认 60min, 0=永不过期) | LangGraph Store `default_ttl` (`store/base/__init__.py:556`) |
| `-sweep-interval-minutes` flag (默认 5min) + 后台 sweeper goroutine | LangGraph `sweep_interval_minutes` (`__init__.py:566`) |
| sweeper Range runs.Map 清 `isFinished() && expired` slot, 不清 running/interrupted | opportunistic delete pattern |
| 410 错误 msg 含 `(status=xxx)` 让前端区分 success / cancelled / error | LangGraph thread.search status 字段语义 |
| done/interrupted/aborted SSE event 含 status 字段 | 前端可按 status 切 UI |

## 待实施 (业务诉求驱动)

### 1. 历史查询 API

LangGraph Platform 有 `client.threads.search(status, limit)` + `client.threads.delete(thread_id)`
(`LANGGRAPH_PLATFORM.md:214-220`). plan_chat_web 当前前端不查 history, 加 API
工作量 vs 价值不对称 (本 session 不加).

业务真要 (例如多用户 dashboard / 历史会话恢复) 时加:

```go
// GET /api/sessions?status=running&limit=20 → [{session_id, status, created_at, ...}]
// DELETE /api/sessions/{id} → 200 OK / 404
```

实现要点:
- `runs.Range` 收集所有 slot + filter status + sort by createdAt + limit
- 返 JSON array (不 stream)
- 估 ~80 行 handler + 测试 (lifecycle 基建已就位, 0 重构)

### 2. Redis / Postgres backend (单进程 → 多副本)

当前 `var runs sync.Map` 是单进程内存。生产多 replica 部署 (kubernetes deployment
2+ pods) 必须共享 backend 否则:
- session_id 路由不到 owning pod → 假 404
- Resume 跨 pod 失败

迁移路径 (跟 LangGraph Platform Postgres checkpointer 同款):

```go
// 抽 runStore interface, 现 sync.Map 是 InMemoryRunStore impl
type runStore interface {
    Load(sid string) (*runSlot, bool)
    Store(sid string, slot *runSlot)
    Delete(sid string)
    Range(fn func(string, *runSlot) bool)
}
// 加 RedisRunStore (slot 序列化 JSON + Redis SET/GET/DEL + SCAN)
// 加 PostgresRunStore (类 LangGraph PostgresCheckpointer schema)
```

实现要点:
- slot 内 sess / cp 也要分离持久化 (Redis Streams / Postgres rows)
- session_id 一致 hash 路由 (业务 LB + sticky session 不必要)
- cp 用 Postgres 子图嵌套 ns 物理分离 (已有 InMemoryCheckpointer 协议直接平移)
- TTL 用 Redis EXPIRE / Postgres scheduled job 替代当前 in-process sweeper
- 估 ~500 行 + 集成测试 + benchmarks

### 3. on_completion="delete" opt-in (per-session 控制)

LangGraph `runs.create(on_completion="delete")` 让业务声明 ephemeral session
(完成即删不留 history). plan_chat_web 当前所有 session 都留 history (TTL 清),
缺 per-session ephemeral 选项.

实现要点:
- `/api/chat?q=...&ephemeral=true` query param
- runSlot 加 `ephemeral bool` 字段
- runOrResume 完成时若 `ephemeral=true` 立即 `runs.Delete` 不留 history
- 估 ~20 行

## 决策依据

**YAGNI 不预实施**: 三项都是业务诉求驱动 (有人说"我要 history API" / "我要 Redis 多副本" / "我要 ephemeral session") 才加, 不预测式过设计.

**LangGraph 是 reference 不是 cargo cult**: 抄 6 状态 + TTL 设计模式 (核心 lifecycle), 不抄每个 API endpoint (那是 production service 边界).

## 重审时机

- 业务真上多副本生产 → 实施 Redis/Postgres backend
- 多用户 dashboard 需求 → 实施历史查询 API
- 业务有"完成即删不留 history" 诉求 → 实施 ephemeral 选项
