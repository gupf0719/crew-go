package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"crew-go/agui"
	"crew-go/compose_v2"

	"github.com/redis/go-redis/v9"
)

// AGUISessionStore 在 Checkpointer 的 Redis 实例和前缀内保存 AG-UI 共享会话。
type AGUISessionStore struct {
	*Checkpointer
}

var _ agui.SessionStore = (*AGUISessionStore)(nil)

// NewAGUISessionStore 从 cp 建立共享会话存储。多个 Handler 使用相同 Redis 和 Checkpointer 前缀。
func NewAGUISessionStore(cp *Checkpointer) *AGUISessionStore {
	if cp == nil {
		panic("redis: NewAGUISessionStore requires a checkpointer")
	}
	return &AGUISessionStore{Checkpointer: cp}
}

func (s *AGUISessionStore) sessionKey(key string) string {
	return s.prefix + ":agui:session:" + key
}

// LoadSession 读取共享会话。无记录时返回 nil。
func (s *AGUISessionStore) LoadSession(ctx context.Context, key string) (*agui.SessionRecord, error) {
	raw, err := s.rdb.Get(ctx, s.sessionKey(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis agui session load: %w", err)
	}
	var record agui.SessionRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, fmt.Errorf("redis agui session decode: %w", err)
	}
	return &record, nil
}

// 前四个键和前六个参数与 deleteSubtreeLua 共用。会话与所有相关任务的租约均在变更前验证。
var saveAGUISessionScript = redis.NewScript(deleteSubtreeLua + `
if ARGV[7] == "" or redis.call("GET", KEYS[5]) ~= ARGV[7] then
    return -1
end
if ARGV[1] ~= "" and (ARGV[6] == "" or redis.call("GET", KEYS[4]) ~= ARGV[6]) then
    return -1
end
if ARGV[8] ~= "" and (ARGV[9] == "" or redis.call("GET", KEYS[6]) ~= ARGV[9]) then
    return -1
end

local deleted = 0
if ARGV[10] == "" then
    check_command("DEL", KEYS[7])
else
    check_command("SET", KEYS[7], ARGV[10])
end
if ARGV[1] ~= "" then
    deleted = delete_subtree()
end
if ARGV[10] == "" then
    redis.call("DEL", KEYS[7])
else
    redis.call("SET", KEYS[7], ARGV[10])
end
return deleted
`)

// SaveSession 在同一脚本内核对会话与任务的租约、删除 dropTask 的存档并更新会话。
func (s *AGUISessionStore) SaveSession(ctx context.Context, key string, record *agui.SessionRecord, dropTask string) (int32, error) {
	leases := s.CheckpointTaskLeases()
	threadToken := compose_v2.CheckpointTaskLeaseToken(ctx, leases, key)
	if threadToken == "" {
		return 0, compose_v2.ErrTaskLeaseLost
	}
	var recordTask string
	var payload []byte
	if record != nil {
		recordTask = record.TaskID
		if recordTask == "" {
			return 0, errors.New("redis agui session requires a task id")
		}
		var err error
		payload, err = json.Marshal(record)
		if err != nil {
			return 0, fmt.Errorf("redis agui session encode: %w", err)
		}
	}
	keys, args := s.subtreeDeleteArgs(ctx, dropTask, "")
	keys = append(keys, taskLeaseKey(s.prefix, key), taskLeaseKey(s.prefix, recordTask), s.sessionKey(key))
	args = append(args, threadToken, recordTask, compose_v2.CheckpointTaskLeaseToken(ctx, leases, recordTask), string(payload))
	deleted, err := saveAGUISessionScript.Run(ctx, s.rdb, keys, args...).Int64()
	if err != nil {
		return 0, fmt.Errorf("redis agui session save: %w", err)
	}
	if deleted == -1 {
		return 0, compose_v2.ErrTaskLeaseLost
	}
	return int32(deleted), nil
}
