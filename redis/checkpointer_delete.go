package redis

import (
	"context"
	"fmt"
	"strconv"

	"crew-go/compose_v2"

	"github.com/redis/go-redis/v9"
)

// deleteSubtreeScript 是 DeleteSubtree 的 Lua 脚本 — 服务端原子完成:
//   - SMEMBERS nsidx + Lua 内段级前缀匹配 (判据同 [compose_v2.IsNSInSubtree])
//   - 对每 matched ns: DEL item + DEL meta → SREM nsidx + SREM pending
//   - SCARD nsidx 验空 → 条件 SREM sids
//
// **KEYS**:
//
//	[1] = nsidx key {prefix}:cp:nsidx:{sid}
//	[2] = pending key {prefix}:cp:pending:{sid}
//	[3] = sids key {prefix}:cp:sids
//
// **ARGV**:
//
//	[1] = sid
//	[2] = ns_prefix (空 = 删整 session)
//	[3] = item_key_prefix
//	[4] = meta_key_prefix
//	[5] = 层级分隔符 (compose_v2.NSSubtreeSeparator(), 划定子树用)
var deleteSubtreeScript = redis.NewScript(`
local nsidx_key   = KEYS[1]
local pending_key = KEYS[2]
local sids_key    = KEYS[3]

local sid               = ARGV[1]
local ns_prefix         = ARGV[2]
local item_key_prefix   = ARGV[3]
local meta_key_prefix   = ARGV[4]

-- 判据的单一真相源是 compose_v2.IsNSInSubtree：前缀自身 + 经层级分隔符往下的后代；
-- 同一个节点的 fan-out 实例 / 重访实例段是兄弟执行，不删。此处是它的 Lua 复刻，两边
-- 是否仍一致由 compose_v2/checkpointer_conformance 的子树用例机械对账。
-- 层级分隔符由 Go 侧注入（ARGV[5]，来自 compose_v2.NSSubtreeSeparator()），不在这里
-- 手写一份。
-- ⚠️ 本文件的 Lua 写在 Go 反引号原始字符串里——注释里不能出现反引号。
local subtree_sep = ARGV[5]

local all_ns = redis.call("SMEMBERS", nsidx_key)
local matched = {}
local ns_prefix_len = #ns_prefix

for i, ns in ipairs(all_ns) do
    if ns_prefix == "" then
        matched[#matched + 1] = ns
    elseif ns == ns_prefix then
        matched[#matched + 1] = ns
    elseif #ns > ns_prefix_len then
        local sep = string.sub(ns, ns_prefix_len + 1, ns_prefix_len + 1)
        if sep == subtree_sep and string.sub(ns, 1, ns_prefix_len) == ns_prefix then
            matched[#matched + 1] = ns
        end
    end
end

if #matched == 0 then
    return 0
end

local deleted = 0
for i, ns in ipairs(matched) do
    local item_key = item_key_prefix .. ns
    local meta_key = meta_key_prefix .. ns

    redis.call("DEL", item_key)
    redis.call("DEL", meta_key)
    redis.call("SREM", nsidx_key, ns)
    redis.call("SREM", pending_key, ns)
    deleted = deleted + 1
end

local remaining = redis.call("SCARD", nsidx_key)
if remaining == 0 then
    redis.call("SREM", sids_key, sid)
end

return deleted
`)

// DeleteSubtree 实现 compose_v2.Checkpointer.DeleteSubtree.
//
// **协议** (跟 InMemoryCheckpointer.DeleteSubtree 一致):
//   - 删 sessionID 下 nsPrefix 这次执行的子树 (nsPrefix 自身 + 经 `/` 往下的后代;
//     同一个节点的 `#`/`@` 兄弟执行不删)
//   - 范围由 [compose_v2.IsNSInSubtree] 定义 — 段级匹配, 不是裸
//     字符前缀 (裸前缀会把 sibling `ns_ab` 当 `ns_a` 的后代删掉)
//   - prefix="" 删整 session
//
// **iter-5-K**: 全 Lua 原子, 不依赖 cjson 解析整 payload.
func (c *Checkpointer) DeleteSubtree(ctx context.Context, sessionID, nsPrefix string) (int32, error) {
	keys := []string{
		cpNSIndexKey(c.prefix, sessionID),
		cpPendingKey(c.prefix) + ":" + sessionID,
		c.cpSidsKey(),
	}
	args := []any{
		sessionID,
		nsPrefix,
		c.cpItemKeyPrefix(sessionID),
		cpMetaKeyPrefix(c.prefix, sessionID),
	}
	// ARGV[5] = 划定子树的层级分隔符，来自唯一真相源（见脚本里 subtree_sep）。
	args = append(args, compose_v2.NSSubtreeSeparator())

	result, err := deleteSubtreeScript.Run(ctx, c.rdb, keys, args...).Result()
	if err != nil {
		return 0, fmt.Errorf("redis cp DeleteSubtree eval: %w", err)
	}

	n, ok := result.(int64)
	if !ok {
		if s, ok2 := result.(string); ok2 {
			parsed, err := strconv.ParseInt(s, 10, 32)
			if err != nil {
				return 0, fmt.Errorf("redis cp DeleteSubtree parse result: %w", err)
			}
			n = parsed
		} else {
			return 0, fmt.Errorf("redis cp DeleteSubtree: unexpected result type %T", result)
		}
	}
	return int32(n), nil
}

// 编译期 IsNSInSubtree 协议引用.
var _ = compose_v2.IsNSInSubtree
