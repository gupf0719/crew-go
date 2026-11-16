package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// MigrateLegacyKeys 迁移旧格式的 Redis 存档键，并返回本次迁移的存档数。
// 调用前备份并停止同一前缀下的所有存档读写者。迁移逐份提交，中断后可重试。
// 旧 payload 的任务和 namespace 与索引不符，或缺少存档时，返回错误并保留该条数据，供应用核对备份。
func (c *Checkpointer) MigrateLegacyKeys(ctx context.Context) (int, error) {
	const batchSize = 128
	sids := c.rdb.SScan(ctx, c.cpSidsKey(), 0, "", batchSize).Iterator()
	migrated := 0
	for sids.Next(ctx) {
		sid := sids.Val()
		namespaces := c.rdb.SScan(ctx, cpNSIndexKey(c.prefix, sid), 0, "", batchSize).Iterator()
		hasNamespace := false
		for namespaces.Next(ctx) {
			hasNamespace = true
			ns := namespaces.Val()
			changed, err := c.migrateLegacyKey(ctx, sid, ns)
			if err != nil {
				return migrated, fmt.Errorf("redis checkpoint migration task %q namespace %q: %w", sid, ns, err)
			}
			if changed {
				migrated++
			}
		}
		if err := namespaces.Err(); err != nil {
			return migrated, fmt.Errorf("redis checkpoint migration list namespaces for %q: %w", sid, err)
		}
		if !hasNamespace {
			return migrated, fmt.Errorf("redis checkpoint migration task %q has no indexed namespace", sid)
		}
	}
	if err := sids.Err(); err != nil {
		return migrated, fmt.Errorf("redis checkpoint migration list tasks: %w", err)
	}
	return migrated, nil
}

// 脚本先校验源和目标。已经迁移的记录必须同时具有 payload 和 metadata。
var migrateLegacyCheckpoint = goredis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
    return "source changed"
end
if redis.call("TYPE", KEYS[2]).ok ~= "hash" then
    return "legacy metadata is missing or invalid"
end
local metadata = redis.call("HMGET", KEYS[2], "graph_id", "status", "created_at")
if metadata[1] ~= ARGV[2] or metadata[2] ~= ARGV[3] or metadata[3] ~= ARGV[4] then
    return "legacy metadata does not match the checkpoint"
end
if redis.call("EXISTS", KEYS[3], KEYS[4]) ~= 0 then
    return "target already exists"
end
if redis.acl_check_cmd and (not redis.acl_check_cmd("RENAME", KEYS[1], KEYS[3]) or
    not redis.acl_check_cmd("RENAME", KEYS[2], KEYS[4])) then
    return "rename is not permitted"
end
redis.call("RENAME", KEYS[2], KEYS[4])
redis.call("RENAME", KEYS[1], KEYS[3])
return ""
`)

func (c *Checkpointer) migrateLegacyKey(ctx context.Context, sid, ns string) (bool, error) {
	suffix := sid + ":" + ns
	oldItem := cpLegacyItemPrefix(c.prefix, sid) + ns
	oldMeta := c.prefix + ":cp:meta:" + suffix
	newItem := cpItemKey(c.prefix, sid, ns)
	newMeta := cpMetaKey(c.prefix, sid, ns)
	raw, err := c.rdb.Get(ctx, oldItem).Bytes()
	if errors.Is(err, goredis.Nil) {
		itemType, err := c.rdb.Type(ctx, newItem).Result()
		if err != nil {
			return false, err
		}
		metaType, err := c.rdb.Type(ctx, newMeta).Result()
		if err != nil {
			return false, err
		}
		if itemType == "string" && metaType == "hash" {
			return false, nil
		}
		return false, errors.New("indexed checkpoint is missing or incomplete")
	}
	if err != nil {
		return false, err
	}
	var identity struct {
		ID        string `json:"id"`
		NS        string `json:"ns"`
		GraphID   string `json:"graph_id"`
		Status    int    `json:"status"`
		CreatedAt int64  `json:"created_at"`
	}
	if err := json.Unmarshal(raw, &identity); err != nil {
		return false, fmt.Errorf("decode legacy checkpoint identity: %w", err)
	}
	if identity.ID != checkpointJSONIdentity(sid) || identity.NS != checkpointJSONIdentity(ns) {
		return false, errors.New("legacy checkpoint identity does not match the index")
	}
	result, err := migrateLegacyCheckpoint.Run(ctx, c.rdb, []string{oldItem, oldMeta, newItem, newMeta},
		raw, identity.GraphID, identity.Status, identity.CreatedAt).Text()
	if err != nil {
		return false, err
	}
	if result != "" {
		return false, fmt.Errorf("cannot move checkpoint: %s", result)
	}
	return true, nil
}

// 旧 payload 的 JSON 将每个非法 UTF-8 字节替换为 U+FFFD，Redis 索引保留原字节。
func checkpointJSONIdentity(value string) string {
	return string([]rune(value))
}
