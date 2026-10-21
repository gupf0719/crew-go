package redis

import (
	"context"
	"errors"
	"fmt"

	"crew-go/compose_v2"

	"github.com/redis/go-redis/v9"
)

// Checkpointer 是 compose_v2.Checkpointer 接口的 Redis 实现.
//
// **存储设计** (纯 Redis 原语 — 见 doc.go key 命名规范):
//   - `{prefix}:cp:item:{sid}:{ns}` → SET 含完整 cp JSON payload (LoadNS 读)
//   - `{prefix}:cp:meta:{sid}:{ns}` → HSET (graph_id / status / created_at)
//     metadata 冗余索引 (Lua 单字段查避 cjson 解析整 payload, iter-5-K 用 Redis
//     Hash 存单字段思路)
//   - `{prefix}:cp:nsidx:{sid}` → SET (sid 下所有 ns)
//   - `{prefix}:cp:pending:{sid}` → SET (该 sid 下 pending 的 ns)
//   - `{prefix}:cp:sids` → SET (所有 sid, Cleanup 用)
//
// **原子性** (iter-5-J/K 全 Lua + EvalSha): SaveNS / DeleteSubtree / Put 都用
// Lua 脚本 + `redis.NewScript` 自动管 SHA cache + NOSCRIPT 回退 — 减少高 QPS
// 业务网络带宽 (脚本只首次上传 SHA1).
//
// **业务限制** (见 doc.go "Cluster 不支持" 章节): 当前实现**仅支持单 instance
// Redis 或 Sentinel master**. 不支持 Redis Cluster + 不可配 ReadOnly replica.
type Checkpointer struct {
	rdb              *redis.Client
	prefix           string
	cleanupBatchSize int // 0 = 不限 (默认), 业务 WithCleanupBatchSize 设非 0 启用分批
}

// CheckpointerOption 配置 Checkpointer.
type CheckpointerOption func(*Checkpointer)

// WithCheckpointerPrefix 自定义 Redis key 前缀 (默认 `crew`).
func WithCheckpointerPrefix(prefix string) CheckpointerOption {
	return func(c *Checkpointer) { c.prefix = prefix }
}

// WithCleanupBatchSize 配 Cleanup 单次跑的 sid 上限. 业务 ctx timeout 短 + 1万
// sid 场景时设此 option 让 Cleanup 单轮快速结束, 业务多轮 cron 跑收敛.
//
// 默认 0 = 不限制 (跟原协议一致, 一次跑完). 业务真 1 万+ sid 推荐设 1000.
//
// **iter-5-M round audit 视角 3 HIGH-2 修**: 业务大 sid 数 ctx timeout 5s 时
// 跑不完无断点续传机制 — 加 option 让业务可配, 不破坏现有协议默认行为.
func WithCleanupBatchSize(n int) CheckpointerOption {
	return func(c *Checkpointer) { c.cleanupBatchSize = n }
}

// NewCheckpointer 创建 Redis Checkpointer.
//
// **rdb 类型 `*redis.Client`** (单 instance / Sentinel master-only).
//
// **nil 防御**: rdb == nil panic — CLAUDE.md 公开 API 防御纪律.
func NewCheckpointer(rdb *redis.Client, opts ...CheckpointerOption) *Checkpointer {
	if rdb == nil {
		panic("crew-go/redis NewCheckpointer: rdb must not be nil")
	}
	c := &Checkpointer{
		rdb:    rdb,
		prefix: DefaultPrefix,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// cpSidsKey 返所有 sid 的 SET key (Cleanup 用).
func (c *Checkpointer) cpSidsKey() string {
	return c.prefix + ":cp:sids"
}

// cpItemKeyPrefix 返 item (payload) key prefix for sid (DeleteSubtree Lua 拼用).
func (c *Checkpointer) cpItemKeyPrefix(sid string) string {
	return c.prefix + ":cp:item:" + sid + ":"
}

// saveNSScript 是 SaveNS 的 Lua 脚本 — **服务端**原子 HGET-check-HSET-SET.
//
// **iter-5-K 修** (verification audit HIGH-1 "cjson decode silent fallback"):
// 原 saveNSScript 用 `cjson.decode(existing_payload)` 拿 graph_id, decode 失败
// (业务自定义 channel value RegisterValueType 含特殊字符 / Redis 版本差异) 时
// silent fallback existing="" → GraphID conflict 防御静默失效. 改用 Redis Hash
// 存 metadata, 单字段 HGET 拿 graph_id — 不依赖 JSON 解析, 永不失败.
//
// 同时性能优化: 不再 GET 整 payload (可能 MB 级) 只为拿几十字节 graph_id, 直接
// HGET meta hash 单字段几十字节 RTT.
//
// **KEYS**:
//
//	[1] = item key  (payload)
//	[2] = meta key  (hash with graph_id/status/created_at)
//	[3] = nsidx key
//	[4] = pending key
//	[5] = sids key
//
// **ARGV**:
//
//	[1] = ns
//	[2] = payload (完整 cp JSON)
//	[3] = new graph_id
//	[4] = new status (int as string)
//	[5] = new created_at (int64 as string)
//	[6] = is_pending ("1" / "0")
//	[7] = sid
var saveNSScript = redis.NewScript(`
local item_key   = KEYS[1]
local meta_key   = KEYS[2]
local nsidx_key  = KEYS[3]
local pending_key = KEYS[4]
local sids_key   = KEYS[5]

local ns           = ARGV[1]
local payload      = ARGV[2]
local new_graph_id = ARGV[3]
local new_status   = ARGV[4]
local new_created_at = ARGV[5]
local is_pending   = ARGV[6]
local sid          = ARGV[7]

-- HGET meta graph_id (单字段, 不需 cjson 解析 — iter-5-K Redis Hash 思路)
local existing_graph_id = redis.call("HGET", meta_key, "graph_id")
if existing_graph_id == false then
    existing_graph_id = ""
end

-- GraphID 冲突检测 (跟 InMem + sql backend 同款 4 case 协议)
if existing_graph_id ~= "" and new_graph_id ~= "" and existing_graph_id ~= new_graph_id then
    return "GRAPH_ID_CONFLICT"
end

-- 原子写入 meta hash + payload + indices
redis.call("HSET", meta_key,
    "graph_id", new_graph_id,
    "status", new_status,
    "created_at", new_created_at)
redis.call("SET", item_key, payload)
redis.call("SADD", nsidx_key, ns)
redis.call("SADD", sids_key, sid)

if is_pending == "1" then
    redis.call("SADD", pending_key, ns)
else
    redis.call("SREM", pending_key, ns)
end

return "OK"
`)

// SaveNS 实现 compose_v2.Checkpointer.SaveNS.
func (c *Checkpointer) SaveNS(ctx context.Context, sessionID, ns string, cp *compose_v2.Checkpoint) error {
	if cp == nil {
		return errors.New("checkpoint is nil")
	}
	if sessionID == "" {
		return errors.New("sessionID is empty")
	}

	// 自动填 ID / NS / ParentNS
	cp.ID = sessionID
	cp.NS = ns
	if cp.ParentNS == "" && ns != "" {
		cp.ParentNS = compose_v2.ParentNS(ns)
	}

	payload, err := compose_v2.MarshalCheckpoint(cp)
	if err != nil {
		return fmt.Errorf("redis cp SaveNS marshal: %w", err)
	}

	isPending := "0"
	if cp.Status == compose_v2.CPStatusPending {
		isPending = "1"
	}

	keys := []string{
		cpItemKey(c.prefix, sessionID, ns),
		cpMetaKey(c.prefix, sessionID, ns),
		cpNSIndexKey(c.prefix, sessionID),
		cpPendingKey(c.prefix) + ":" + sessionID,
		c.cpSidsKey(),
	}
	args := []any{
		ns, payload, cp.GraphID,
		fmt.Sprintf("%d", int(cp.Status)),
		fmt.Sprintf("%d", cp.CreatedAt),
		isPending, sessionID,
	}

	result, err := saveNSScript.Run(ctx, c.rdb, keys, args...).Result()
	if err != nil {
		return fmt.Errorf("redis cp SaveNS eval: %w", err)
	}

	if resultStr, ok := result.(string); ok && resultStr == "GRAPH_ID_CONFLICT" {
		return fmt.Errorf("%w: sid=%q ns=%q new=%q",
			compose_v2.ErrGraphIDConflict, sessionID, ns, cp.GraphID)
	}
	return nil
}

// LoadNS 实现 compose_v2.Checkpointer.LoadNS. 不存在返 (nil, nil).
//
// **不变**: GET payload (完整 cp JSON) → UnmarshalCheckpoint → 返完整 cp. meta
// hash 是 Lua 内部冗余索引, LoadNS 路径不感知.
func (c *Checkpointer) LoadNS(ctx context.Context, sessionID, ns string) (*compose_v2.Checkpoint, error) {
	raw, err := c.rdb.Get(ctx, cpItemKey(c.prefix, sessionID, ns)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis cp LoadNS: %w", err)
	}
	cp, err := compose_v2.UnmarshalCheckpoint(raw)
	if err != nil {
		return nil, fmt.Errorf("redis cp LoadNS unmarshal: %w", err)
	}
	return cp, nil
}

// ExistsNS 实现 compose_v2.Checkpointer.ExistsNS.
func (c *Checkpointer) ExistsNS(ctx context.Context, sessionID, ns string) (bool, error) {
	n, err := c.rdb.Exists(ctx, cpItemKey(c.prefix, sessionID, ns)).Result()
	if err != nil {
		return false, fmt.Errorf("redis cp ExistsNS: %w", err)
	}
	return n > 0, nil
}

// 编译期 interface 实现检查.
var _ compose_v2.Checkpointer = (*Checkpointer)(nil)
