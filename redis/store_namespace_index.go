package redis

import (
	"context"

	"crew-go/store"

	"github.com/redis/go-redis/v9"
)

const namespaceIndexBatch = 32

// namespaceIndexLua 在所有索引操作中用同一台 Redis 的时钟记录过期点。永久条目记 +inf。
const namespaceIndexLua = `
if redis.replicate_commands then redis.replicate_commands() end
local namespace_header = '{"namespace_hex":"'

local function index_clock()
    local now = redis.call("TIME")
    return tonumber(now[1])*1000 + math.floor(tonumber(now[2])/1000)
end

local function prune_expired_items(items, now)
    local expired = redis.call("ZRANGEBYSCORE", items, "-inf", now, "LIMIT", 0, 16)
    if #expired > 0 then
        redis.call("ZREM", items, unpack(expired))
    end
end

local function expire_index(items)
    local last = redis.call("ZREVRANGE", items, 0, 0, "WITHSCORES")
    if #last > 0 then
        local deadline = tonumber(last[2])
        if deadline and deadline < math.huge then
            redis.call("PEXPIREAT", items, last[2])
        else
            redis.call("PERSIST", items)
        end
    end
end
`

var deleteStoreItemScript = redis.NewScript(namespaceIndexLua + `
redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], KEYS[1])
expire_index(KEYS[2])
return 0
`)

func (s *Store) namespaceNames(ctx context.Context) ([]string, error) {
	indexKey, readyKey := storeNSIndexKey(s.prefix), storeNSIndexReadyKey(s.prefix)
	pipe := s.rdb.Pipeline()
	ready := pipe.Exists(ctx, readyKey)
	names := pipe.SMembers(ctx, indexKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	if ready.Val() == 1 {
		return names.Val(), nil
	}
	var maxLength int
	for _, name := range names.Val() {
		maxLength = max(maxLength, len(name))
	}
	if maxLength > 0 {
		if err := s.indexLegacyNamespaceItems(ctx, indexKey, maxLength); err != nil {
			return nil, err
		}
	}
	if err := s.rdb.Set(ctx, readyKey, "1", 0).Err(); err != nil {
		return nil, err
	}
	return s.rdb.SMembers(ctx, indexKey).Result()
}

// indexLegacyNamespaceItems 首次枚举补建成员索引，只扫描当前 Store 前缀下的条目。
func (s *Store) indexLegacyNamespaceItems(ctx context.Context, indexKey string, maxLength int) error {
	prefix := s.prefix + ":store:item:"
	pattern := escapeGlobPattern(prefix) + "*"
	var cursor uint64
	for {
		items, next, err := s.rdb.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return err
		}
		for len(items) > 0 {
			n := min(namespaceIndexBatch, len(items))
			keys := make([]string, 1, n+1)
			keys[0] = indexKey
			keys = append(keys, items[:n]...)
			if err := indexLegacyItemsScript.Run(ctx, s.rdb, keys, len(prefix), maxLength, storeNSItemsPrefix(s.prefix)).Err(); err != nil {
				return err
			}
			items = items[n:]
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

// 旧键的分隔符有歧义，只回填已登记的候选。扫描后新写入带精确归属，跳过回填。
var indexLegacyItemsScript = redis.NewScript(namespaceIndexLua + `
local clock_ms = index_clock()
local prefix_len = tonumber(ARGV[1])
local last_separator = prefix_len + tonumber(ARGV[2]) + 1
for i = 2, #KEYS do
    local item = KEYS[i]
    local header = redis.call("GETRANGE", item, 0, #namespace_header-1)
    if header ~= "" and header ~= namespace_header then
        local ttl = redis.call("PTTL", item)
        if ttl ~= -2 then
            local expiry = ttl < 0 and "+inf" or clock_ms + ttl
            local split = string.find(item, ":", prefix_len+2, true)
            while split and split < #item and split <= last_separator do
                local ns = string.sub(item, prefix_len+1, split-1)
                if redis.call("SISMEMBER", KEYS[1], ns) == 1 then
                    local owner = string.gsub(ns, ".", function(byte)
                        return string.format("%02x", string.byte(byte))
                    end)
                    local items = ARGV[3] .. owner
                    redis.call("ZADD", items, "NX", expiry, item)
                    expire_index(items)
                end
                split = string.find(item, ":", split+1, true)
            end
        end
    end
end
return 0
`)

// 清理只在确认没有存活条目时移除名称。每批只查有限个候选和成员。
var checkNamespaceItemsScript = redis.NewScript(namespaceIndexLua + `
local clock_ms = index_clock()
local start = "(" .. string.format("%.0f", clock_ms)
local result = {}
for i = 1, #KEYS-1 do
    local items = KEYS[i+1]
    local ns = ARGV[2*i-1]
    local owner = namespace_header .. ARGV[2*i] .. '"'
    prune_expired_items(items, clock_ms)
    local live = false
    for _, item in ipairs(redis.call("ZREVRANGEBYSCORE", items, "+inf", start, "LIMIT", 0, 16)) do
        local header = redis.call("GETRANGE", item, 0, #owner-1)
        if header ~= "" and (header == owner or string.sub(header, 1, #namespace_header) ~= namespace_header) then
            live = true
            break
        end
        redis.call("ZREM", items, item)
    end
    if live then
        expire_index(items)
        result[i] = 1
    elseif redis.call("ZCOUNT", items, start, "+inf") == 0 then
        redis.call("UNLINK", items)
        redis.call("SREM", KEYS[1], ns)
        result[i] = 0
    else
        result[i] = -1
    end
end
return result
`)

func (s *Store) liveNamespaces(ctx context.Context, pending []store.Namespace) ([]store.Namespace, error) {
	found := make([]store.Namespace, 0, len(pending))
	prefix := storeNSItemsPrefix(s.prefix)
	for len(pending) > 0 {
		n := min(len(pending), namespaceIndexBatch)
		batch := pending[:n]
		pending = pending[n:]
		keys := make([]string, 1, n+1)
		keys[0] = storeNSIndexKey(s.prefix)
		args := make([]any, 0, 2*n)
		for _, ns := range batch {
			joined := joinNS(ns)
			owner := storeNSIndexOwner(joined)
			keys = append(keys, prefix+owner)
			args = append(args, joined, owner)
		}
		status, err := checkNamespaceItemsScript.Run(ctx, s.rdb, keys, args...).Int64Slice()
		if err != nil {
			return nil, err
		}
		for i, v := range status {
			switch v {
			case 1:
				found = append(found, batch[i])
			case -1:
				pending = append(pending, batch[i])
			}
		}
	}
	return found, nil
}
