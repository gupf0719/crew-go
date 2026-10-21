package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"crew-go/store"

	"github.com/redis/go-redis/v9"
)

// DefaultPrefix 是 Redis key 默认前缀.
const DefaultPrefix = "crew"

// storeRecord 是 Redis 存的 item payload (含 value + 时间戳).
//
// **JSON 字段命名**: 跟 Lua 脚本内 cjson.decode 字段名严格对齐 (`value` /
// `created_at` / `updated_at`).
type storeRecord struct {
	Value     json.RawMessage `json:"value"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// Store 是 store.Store 接口的 Redis 实现.
//
// **TTLAware**: 支持 TTL (Redis 原生 EXPIRE). `SupportsTTL()` 返 true.
//
// **业务限制** (见 doc.go "Cluster 不支持" 章节): 当前实现**仅支持单 instance
// Redis 或 Sentinel master**. 不支持 Redis Cluster (跨 slot Lua / pipeline 限制).
type Store struct {
	rdb    *redis.Client
	prefix string
}

// StoreOption 配置 Store 行为.
type StoreOption func(*Store)

// WithStorePrefix 自定义 Redis key 前缀 (默认 `crew`).
func WithStorePrefix(prefix string) StoreOption {
	return func(s *Store) { s.prefix = prefix }
}

// NewStore 创建 Redis Store.
//
// **rdb 类型 `*redis.Client`** (单 instance / Sentinel master-only): iter-5-J 收窄
// 从 `redis.UniversalClient` 改为 `*redis.Client` — agent audit 视角 6 抓出业务
// 接 ClusterClient 时 Put pipeline 跨 slot 100% CROSSSLOT 失败. 收窄 type 让业务
// 编译期 fail-fast 而非生产 outage. Cluster 业务自实现 backend 跑 conformance.
//
// **nil 防御**: rdb == nil panic — CLAUDE.md 公开 API 防御纪律.
func NewStore(rdb *redis.Client, opts ...StoreOption) *Store {
	if rdb == nil {
		panic("crew-go/redis NewStore: rdb must not be nil")
	}
	s := &Store{
		rdb:    rdb,
		prefix: DefaultPrefix,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// ── store.Store interface 实现 ──

// Get 实现 store.Store.Get. 不存在返 (nil, nil) — 跟 InMemoryStore 协议一致.
func (s *Store) Get(ctx context.Context, namespace store.Namespace, key string) (*store.Item, error) {
	if err := namespace.Validate(); err != nil {
		return nil, err
	}
	if key == "" {
		return nil, fmt.Errorf("redis store get: key must not be empty")
	}

	raw, err := s.rdb.Get(ctx, storeItemKey(s.prefix, namespace, key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis store get: %w", err)
	}

	var rec storeRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, fmt.Errorf("redis store get: unmarshal: %w", err)
	}

	value, err := decodeValue(rec.Value)
	if err != nil {
		return nil, fmt.Errorf("redis store get: %w", err)
	}

	return &store.Item{
		Namespace: namespace,
		Key:       key,
		Value:     value,
		CreatedAt: rec.CreatedAt.UTC(),
		UpdatedAt: rec.UpdatedAt.UTC(),
	}, nil
}

// putScript 是 Put 的 Lua 脚本 — **服务端原子** GET-check-SET 保 CreatedAt 语义.
//
// **iter-5-J 修** (audit 视角 3 HIGH "Put CreatedAt race"): 原 client-side GET old
// + SET new 跨 2 RTT, 并发同 key Put 时 CreatedAt 漂移成 last write. 改 Lua 内部
// 原子 GET-parse-SET 保 "首次创建时间" 语义跟 InMem + sql backend 对齐.
//
// **KEYS**:
//
//	[1] = item key {prefix}:store:item:{ns_joined}:{key}
//	[2] = nsidx key {prefix}:store:nsidx
//
// **ARGV**:
//
//	[1] = ns_joined (写 nsidx)
//	[2] = value JSON (raw, Lua 不解析直接拼)
//	[3] = now (RFC3339Nano, 业务 time.Now().UTC().Format)
//	[4] = ttl_ms (毫秒, "0" 表示不设)
//
// **返**: "OK"
//
// **iter-5-K**: 用 `redis.NewScript` 自动管 SHA cache + NOSCRIPT 回退 — 减少高
// QPS 业务网络带宽 (脚本只首次上传 SHA1, 后续 EvalSha 仅 40 字节 SHA hash).
var putScript = redis.NewScript(`
local item_key  = KEYS[1]
local nsidx_key = KEYS[2]

local ns          = ARGV[1]
local value_json  = ARGV[2]
local now         = ARGV[3]
local ttl_ms      = tonumber(ARGV[4])

-- GET 拿 old created_at 保 "首次创建时间" 语义
local created_at = now
local old_payload = redis.call("GET", item_key)
if old_payload then
    local ok, decoded = pcall(cjson.decode, old_payload)
    if ok and type(decoded) == "table" and decoded.created_at then
        created_at = decoded.created_at
    end
end

-- 构造 new record JSON (Lua string concat — 避 cjson encode 字段序漂移; 时间戳
-- RFC3339Nano 不含 JSON 特殊字符直接 "..." 包)
local new_payload = '{"value":' .. value_json .. ',"created_at":"' .. created_at .. '","updated_at":"' .. now .. '"}'

if ttl_ms > 0 then
    redis.call("SET", item_key, new_payload, "PX", ttl_ms)
else
    redis.call("SET", item_key, new_payload)
end

redis.call("SADD", nsidx_key, ns)
return "OK"
`)

// Put 实现 store.Store.Put. value=nil 等价 Delete.
//
// **TTL**: 业务 `WithTTL(d)` → Redis SET EX, key 自动过期.
//
// **CreatedAt 保留语义** (iter-5-J 全 Lua 原子化): Put 覆盖时 CreatedAt 用旧值,
// 新建时 = UpdatedAt = now. Lua 服务端原子 GET-parse-SET 防 client-side race.
func (s *Store) Put(ctx context.Context, namespace store.Namespace, key string, value any, opts ...store.PutOption) error {
	if err := namespace.Validate(); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("redis store put: key must not be empty")
	}

	// value=nil → Delete
	if value == nil {
		return s.Delete(ctx, namespace, key)
	}

	cfg := store.ParsePutOptions(opts...)

	valueJSON, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("redis store put: marshal value: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)

	ttlMs := "0"
	if cfg.TTL > 0 {
		ttlMs = strconv.FormatInt(cfg.TTL.Milliseconds(), 10)
	}

	keys := []string{
		storeItemKey(s.prefix, namespace, key),
		storeNSIndexKey(s.prefix),
	}
	args := []any{
		joinNS(namespace),
		string(valueJSON),
		now,
		ttlMs,
	}

	if _, err := putScript.Run(ctx, s.rdb, keys, args...).Result(); err != nil {
		return fmt.Errorf("redis store put: %w", err)
	}
	return nil
}

// Delete 实现 store.Store.Delete. 不存在 key 不报错 (幂等).
//
// **ns 索引懒删**: 不删 `nsidx` 中的 ns (验证开销大). 业务调 Search 拿不到 item
// (空 ns) — 不影响正确性, 但 `ListNamespaces` 会继续列出它.
//
// ⚠️ **只有 redis 后端会这样**(2026-09-18 三后端差分实测): in-memory 与 sql 删掉最后
// 一条就不再列出该 ns(sql 压根没有 ns 索引, 它每次从表里 DISTINCT)。原注释写的
// 「跟 sql backend 设计一致」不成立。TTL 过期同理会残留。
func (s *Store) Delete(ctx context.Context, namespace store.Namespace, key string) error {
	if err := namespace.Validate(); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("redis store delete: key must not be empty")
	}

	if _, err := s.rdb.Del(ctx, storeItemKey(s.prefix, namespace, key)).Result(); err != nil {
		return fmt.Errorf("redis store delete: %w", err)
	}
	return nil
}

// 编译期 interface 实现检查.
var (
	_ store.Store    = (*Store)(nil)
	_ store.TTLAware = (*Store)(nil)
)

// SupportsTTL 实现 store.TTLAware. Redis 原生支持 EXPIRE, 返 true.
func (s *Store) SupportsTTL() bool { return true }

// decodeValue 把 json.RawMessage 反序列化为 any (用 json.Decoder + UseNumber()
// 保 int64 精度 — Snowflake ID 业务不能丢精度). 跟 sql.unmarshalValue 同款.
func decodeValue(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(jsonBytesReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("decode value: %w", err)
	}
	return v, nil
}
