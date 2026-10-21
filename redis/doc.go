// Package redis 提供 crew-go/store.Store 和 compose_v2.Checkpointer 的 Redis
// backend 实现 — PoC 走纯 Redis 原语 (string/set/zset/Lua + EXPIRE), 不依赖
// RedisJSON / RediSearch modules. 任何 Redis 6.0+ 直接接.
//
// # 业务接入示例
//
//	import (
//	    "github.com/redis/go-redis/v9"
//	    agentRedis "crew-go/redis"
//	)
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	defer rdb.Close()
//
//	st := agentRedis.NewStore(rdb)              // store.Store
//	cp := agentRedis.NewCheckpointer(rdb)        // compose_v2.Checkpointer
//
// # 设计哲学
//
// 一种思路是用 RedisJSON + RediSearch 做内置语义搜索 + 复杂 filter, 但那必须
// Redis 8.0+ 或 redis-stack. crew-go 跟齐 sql backend "PoC 不内置语义搜索"
// 决策 — Search/ListNamespaces 走 SCAN + 客户端 filter, 业务大规模需求接 mem0
// 后端或自实现 RediSearch backend (跑 conformance 验证).
//
// # 部署门槛 + 业务限制
//
// **支持的 Redis 部署模式**:
//   - ✅ 单 instance Redis 6.0+
//   - ✅ Redis Sentinel **master-only** (业务 client 不可配 ReadOnly replica)
//   - ❌ **不支持 Redis Cluster** (跨 slot Lua 限制 — 见 "Cluster 限制" 章节)
//
// **业务 client 配置**:
//   - `*redis.Client` (单 instance) ✅
//   - `*redis.FailoverClient` 配 `RouteRandomly: false` + 默认 master ✅
//   - **不可** 配 `RouteRandomly: true` 或 `ReadOnly: true` → SaveNS-LoadNS
//     round-trip 走 replica 时 replica lag 导致拿到 nil cp, HITL Resume 断裂
//
// # Cluster 限制 (iter-5-J audit 视角 6 抓出)
//
// **本 backend 不支持 Redis Cluster**. 原因:
//
//   - SaveNS Lua 脚本含 5 KEYS (item / meta / nsidx / pending / sids), 跨 slot 必
//     `CROSSSLOT Keys in request don't hash to the same slot` 失败
//   - DeleteSubtree Lua 脚本同款跨 3+ KEYS 跨 slot
//   - Store.Put 跨 item key + 全局 nsidx key (slot 不同)
//   - SCAN 在 Cluster 模式只扫单节点不跨集群 — Search/ListNamespaces 漏数据
//
// `NewStore` / `NewCheckpointer` 入参类型收窄为 `*redis.Client` (不再接受
// `redis.UniversalClient`) — 业务接 `*redis.ClusterClient` 编译期 fail 而非生产
// outage. 业务真要 Cluster:
//
//   - **Phase 2 hash tag 设计**: 把 sid 维度 key 加 `{sid}` hash tag 让落同 slot,
//     sids 全局 key 拆单独操作 (放弃 atomic)
//   - **或自实现 backend**: 业务接 Cluster 自实现 `store.Store` / `compose_v2.
//     Checkpointer` 跑 conformance 验证
//
// 一种常见做法是 type 接 ClusterClient 但**实现仍有 CROSSSLOT 漏洞** (无 hash
// tag), Store 路径检测 `cluster_mode` 后逐 op 走 (放弃 pipeline atomic). crew-go
// 选**编译期 fail-fast 更诚实**.
//
// # Replica Lag 警告 (iter-5-J audit 视角 7 抓出)
//
// 业务接 Redis Sentinel / 主从分离时**不可配 ReadOnly replica**. 原因:
//
//   - SaveNS 写 master → 立即 LoadNS 业务期望读到刚写入 cp
//   - 若 LoadNS 走 replica → replica lag (100ms-数秒) 期间返 `(nil, nil)` → 业务
//     HITL Resume 拿不到 pending cp → fail-fast 错误 / 整闭环断裂
//
// 业务 client 配置范式:
//
//	// ✅ 正确: master-only
//	rdb := redis.NewClient(&redis.Options{Addr: master_addr})
//
//	// ❌ 错: 走 replica
//	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
//	    ReadOnly:      true,           // ← 不可!
//	    RouteByLatency: true,
//	})
//
// 同类 Redis backend 常有此问题 (默认走 master, 无 replica 处理) — crew-go
// 文档明示让业务 fail-fast 而非生产踩坑.
//
// # Key 命名规范
//
// 顶层 prefix 默认 "crew", 业务 `WithStorePrefix` / `WithCheckpointerPrefix`
// 自定义防多实例串台. ns 段用 `.` 拼接 (跟 sql backend nsSeparator 一致;
// `store.Namespace.Validate()` 已防段含 `.`):
//
//	# Store
//	{prefix}:store:item:{ns_joined}:{key}    → JSON value
//	{prefix}:store:nsidx                     → SET (所有 ns_joined, ListNamespaces 主路径)
//
//	# Checkpointer
//	{prefix}:cp:item:{sid}:{ns}              → JSON cp payload
//	{prefix}:cp:nsidx:{sid}                   → SET (sid 下所有 ns)
//	{prefix}:cp:pending:{sid}                 → SET (该 sid 下 pending 的 ns)
//	{prefix}:cp:sids                          → SET (所有 sid, Cleanup 用)
//
// # 原子性 (iter-5-K 修)
//
// 所有跨命令操作走 Lua 脚本服务端原子化 (Redis 单线程跑 Lua 期间无 race) + 用
// `redis.NewScript` 自动 SHA cache + NOSCRIPT 时 fallback Eval:
//
//   - **SaveNS**: `saveNSScript` — 内部 `HGET meta_key graph_id` 单字段查 +
//     冲突 check + HSET meta (graph_id/status/created_at) + SET payload +
//     SADD nsidx/sids + SADD/SREM pending
//   - **DeleteSubtree**: `deleteSubtreeScript` — 内部 SMEMBERS + Lua filter prefix +
//     对每 matched: DEL item + DEL meta + SREM nsidx/pending;
//     最后 SCARD nsidx 验空条件 SREM sids
//   - **Store.Put**: `putScript` — 内部 GET old + cjson.decode 拿 created_at + SET
//     new (拼新 JSON 保 CreatedAt 语义) + SADD nsidx
//
// **iter-5-K 解决 cjson decode silent fallback**: 用 Redis Hash 存 cp metadata
// (graph_id/status/created_at), Lua HGET 单字段查不依赖 cjson 解析整
// payload — 等价于 RedisJSON `JSON.GET $.graph_id` 单字段查思路 (但不依赖
// RedisJSON modules).
//
// # MGET chunk + ctx.Done 早退 (iter-5-L 修)
//
//   - **Search / loadCheckpoints MGET**: 大集合自动拆 1000 key 一批 (`mgetChunkSize`)
//     避 Redis server 单线程跑大命令 block 其他 client.
//   - **Cleanup / scanAll 主循环**: 每迭代检 `ctx.Err()`, 业务 deadline 时早退避免
//     无谓 RTT.
//
// # 业务大规模注意
//
// PoC SCAN 在大集合 (>10万 key) 慢; 业务大规模建议:
//   - 接 mem0 后端 (跨 backend 统一语义搜索)
//   - 自实现 RediSearch backend (跑 conformance 验证)
//   - 接 crew-go/sql + PG (jsonb 原生相等 + 类型守卫下推)
//
// # Redis 持久化配置 (业务必读!)
//
// **Redis 默认不持久化** — `appendonly no` + `save 3600 1` (1 小时存盘). 业务
// 用 crew-go/redis 当 cp backend 时 Redis crash/重启**最近 1 小时所有 HITL
// Pending cp 丢失** → 业务用户对话 session 全断.
//
// **业务必配** (`redis.conf` 或 `CONFIG SET`):
//
//	appendonly yes
//	appendfsync everysec    # 平衡持久化 + 性能 (P99 数据丢失 < 1s)
//	# 或更严: appendfsync always (每命令 fsync, 性能差 50%+)
//
// **冷备**: 定期 cron 跑 `BGSAVE` 出 RDB 文件外部备份.
//
// **双 backend 设计** (业务高可用推荐):
//   - hot redis cp (短期 HITL Resume 用, 性能优)
//   - cold sql cp (长期归档 + 跨进程 disaster recovery)
//   - 业务自实现 wrapper `Checkpointer` 同时写两 backend, LoadNS 优先 redis
//     fail 时 fallback sql — crew-go 当前不内置, 业务自管
//
// # Sentinel Failover 期间业务 handle (业务必读)
//
// Sentinel master 切换 5-30s 不可用期间, 业务调 SaveNS / LoadNS 都返 error.
// 业务 handle 范式:
//
//	// 推荐: 业务用 compose_v2.RunWithRetry 包一次 Run——判据先过框架的 IsRetryable
//	// (中断 / ctx 取消 / 确定性 HTTP 失败 / 终局失败不重试), 再问配置里的谓词;
//	// Sentinel failover 这类网络错才重跑
//	events, err := compose_v2.RunWithRetry(ctx,
//	    compose_v2.ExponentialBackoff(3, 500*time.Millisecond, 10*time.Second),
//	    func(ctx context.Context) ([]*agent_v2.AgentEvent, error) {
//	        return ag.Run(ctx, sess, input)
//	    })
//
//	// HITL Resume 用户答 ask_user 提交时:
//	// 业务 webserver 收用户答 → 调 Resume → SaveNS 失败 (Sentinel failover) →
//	// 业务 webserver 自己 retry (5-30s 后 master 恢复)
//
// 业务真高可用场景考虑双 backend (上面 "Redis 持久化配置" 章节).
//
// # 性能调优 (业务大规模)
//
//   - **SaveNS Lua**: 单 Lua 7 redis.call (HGET/HSET/SET/SADD ×3/SREM, 单次执行 6 个——pending 的 SADD/SREM 二选一)
//     ~1-3ms server-side. 1000 QPS × 2ms = 2000ms/s — 单 Redis instance 100%
//     CPU 阻塞其他 client. 业务 1000+ QPS 必须 Redis Cluster (本 backend 不支持,
//     业务自实现 Cluster backend).
//   - **大 cp payload** (Channels 含 messages 历史 MB 级): SET 10MB payload + Lua
//     跑期间 server block 几十 ms. 业务**必启** crew-go 4 层压缩
//     (`WithMicroCompact` + `WithAutoCompact` + `WithReactiveCompact` +
//     `WithStoreMemory`) 让 cp 小. 1000 QPS × 100KB payload = 100MB/s 千兆网卡
//     刚够.
//   - **Cleanup 1 万 sid**: 串行 DeleteSubtree 跑 50-100s. 业务 ctx timeout
//     5s 时只能删 ~500 sid — 推荐:
//     业务自实现 ZSCAN sids 分页 + 业务 cron 多轮跑, 或调小
//     [WithCleanupBatchSize] 分多轮 Cleanup.
//   - **EvalSha**: `redis.NewScript` 自动管 SHA1 cache + NOSCRIPT 时 fallback
//     Eval 全文. 高 QPS 节省网络带宽 (SHA1 40 字节 vs ~60 行 Lua ~3KB).
//
// # Observability metric 集成 (Phase 2 follow-up)
//
// 当前 redis backend 不内置 metric (跟 sql backend 同款). 业务自接 observability
// 建议:
//   - 业务用 `agent_v2.WithHooks` + `compose_v2.WithCtxOrchHooks` 接 framework
//     级 metric (SaveNS 跨 Run 走 OnAgentEnd hook)
//   - 业务 wrap Checkpointer interface 加 decorator emit SaveNS/LoadNS p99 +
//     ErrGraphIDConflict count
//   - Phase 2 可加 `compose_v2.CheckpointerWithMetrics` 标准 decorator
package redis
