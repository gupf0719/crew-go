package redis

import (
	"strings"

	"crew-go/store"
)

// nsSeparator 是 ns 段拼接符 (跟 sql backend 一致). `store.Namespace.Validate()`
// 已防段含 `.` (`store/store.go:68`), 不会跟段内容冲突.
const nsSeparator = "."

// joinNS 把 namespace slice 拼为单段 Redis key 部分.
func joinNS(ns store.Namespace) string {
	return strings.Join(ns, nsSeparator)
}

// splitNS 把 Redis key ns 部分解析回 namespace slice.
//
// **跟 sql backend Option A 一致** (iter-5-I): 写入端
// `store.Namespace.Validate()` 已 fail-fast 防非法 ns 入 Redis, 读出端信任 Redis
// 数据完整性, 不做兜底验证.
func splitNS(joined string) store.Namespace {
	if joined == "" {
		return nil
	}
	return store.Namespace(strings.Split(joined, nsSeparator))
}

// storeItemKey 返 store item 的 Redis key.
//
//	{prefix}:store:item:{ns_joined}:{key}
func storeItemKey(prefix string, ns store.Namespace, key string) string {
	return prefix + ":store:item:" + joinNS(ns) + ":" + key
}

// storeItemKeyByJoined 用已 join 的 ns string 拼 Redis key (Search 内部用).
func storeItemKeyByJoined(prefix, nsJoined, key string) string {
	return prefix + ":store:item:" + nsJoined + ":" + key
}

// storeItemPatternForNS 返扫 ns 自身 + 后代下所有 item 的 SCAN MATCH 模式.
//
// 业务 Search(NS("users", "u1")) 应匹配 `users.u1` (自身) + `users.u1.*` (后代).
// 用两个 SCAN: `...:item:users.u1:*` + `...:item:users.u1.*` (后者 ns_joined 含 `.`
// 后的 key, 上层会按 ns_joined prefix 二次筛).
//
// 跟 sql.nsExactOrDescendantSQL 同款语义.
func storeItemPatternForNS(prefix, nsJoined string) (selfPattern, descendantPattern string) {
	// **ns 必须转义**：SCAN MATCH 是 glob，`*` / `?` / `[` 在里面是元字符，而
	// `store.Namespace.Validate` 只拒 `.` 与 NUL——`NS("a*")` 是合法 namespace。
	// 不转义时它的 Search 模式变成 `...:item:a*:*`，**读到 `alpha` 的数据**（实测），
	// 而这一层没有任何二次筛兜底（解出的 ns 直接进结果）。namespace 是多租户隔离
	// 边界，这是越权读不是多返几条。见 escapeGlobPattern。
	esc := escapeGlobPattern(nsJoined)
	selfPattern = prefix + ":store:item:" + esc + ":*"
	descendantPattern = prefix + ":store:item:" + esc + nsSeparator + "*"
	return
}

// escapeGlobPattern 转义 Redis glob 元字符，让一段字面量在 SCAN MATCH 里只匹配自己。
//
// Redis 的模式匹配（`stringmatchlen`）认 `*`（任意长）、`?`（单字符）、
// `[...]`（字符类）三种元字符，并用 `\` 转义。反斜杠自身必须最先替换，
// 否则后续插入的反斜杠会被二次转义。
//
// `]` 不必转义（Redis 只在 `[` 开启的字符类里才对它赋予含义），但一并转掉更省心，
// 代价只是模式串长一点。
func escapeGlobPattern(s string) string {
	repl := strings.NewReplacer(
		`\`, `\\`,
		"*", `\*`,
		"?", `\?`,
		"[", `\[`,
		"]", `\]`,
	)
	return repl.Replace(s)
}

// storeNSIndexKey 返 Store ns 索引 SET 的 Redis key.
//
//	{prefix}:store:nsidx → SET (所有 ns_joined)
func storeNSIndexKey(prefix string) string {
	return prefix + ":store:nsidx"
}

// parseStoreItemKey 从 Redis item key 反向解析 (ns, key). 失败返 ("", "", false).
//
// **格式**: `{prefix}:store:item:{ns_joined}:{key}`
// **解析**: 从 prefix+":store:item:" 之后剪掉, 最后一个 `:` 之后是 key, 之前是 ns_joined.
func parseStoreItemKey(prefix, redisKey string) (ns store.Namespace, key string, ok bool) {
	itemPrefix := prefix + ":store:item:"
	if !strings.HasPrefix(redisKey, itemPrefix) {
		return nil, "", false
	}
	remaining := redisKey[len(itemPrefix):]
	// 最后一个 `:` 之后是 key
	lastColon := strings.LastIndex(remaining, ":")
	if lastColon < 0 {
		return nil, "", false
	}
	nsJoined := remaining[:lastColon]
	key = remaining[lastColon+1:]
	if nsJoined == "" || key == "" {
		return nil, "", false
	}
	return splitNS(nsJoined), key, true
}

// cpItemKey 返 cp item (完整 payload) 的 Redis key.
//
//	{prefix}:cp:item:{sid}:{ns}    → SET 含 MarshalCheckpoint(cp) 完整 JSON
func cpItemKey(prefix, sid, ns string) string {
	return prefix + ":cp:item:" + sid + ":" + ns
}

// cpMetaKey 返 cp metadata hash 的 Redis key.
//
// **iter-5-K 加 (Redis Hash 单字段查思路)**: 用 Redis Hash
// 存 cp metadata (graph_id / status / created_at), 让 Lua 脚本 HGET
// 单字段查不依赖 cjson 解析整 payload — 修 verification audit HIGH-1/HIGH-2
// (cjson decode silent fallback) + 顺带优化性能 (不解析 MB 级 payload 拿几十字节
// metadata).
//
//	{prefix}:cp:meta:{sid}:{ns}    → HSET graph_id <gid> status <s> created_at <ts>
//
// payload (`cpItemKey`) 仍存完整 cp JSON — LoadNS 路径不变, meta 是冗余索引仅
// Lua 单字段查用.
func cpMetaKey(prefix, sid, ns string) string {
	return prefix + ":cp:meta:" + sid + ":" + ns
}

// cpMetaKeyPrefix 返 meta hash key prefix for sid (DeleteSubtree Lua 拼 meta_key 用).
func cpMetaKeyPrefix(prefix, sid string) string {
	return prefix + ":cp:meta:" + sid + ":"
}

// cpNSIndexKey 返 sid 下所有 ns 的 SET key.
//
//	{prefix}:cp:nsidx:{sid} → SET (所有该 sid 下的 ns)
func cpNSIndexKey(prefix, sid string) string {
	return prefix + ":cp:nsidx:" + sid
}

// cpPendingKey 返 pending cp 全局索引 SET key (实际用作 prefix, 业务调用拼
// `:` + sid 得 per-sid key).
//
// **跨 sid 隔离协议** (audit iter-3 H4): per-sid SET (`{prefix}:cp:pending:{sid}`)
// 让 ListPending 只查指定 sid 不跨 sid 扫描.
//
// **note**: 业务真存的 pending SET member 是**纯 ns** (不是 "sid|ns"), 因为
// per-sid 隔离不需要 sid 在 member 里冗余.
func cpPendingKey(prefix string) string {
	return prefix + ":cp:pending"
}
