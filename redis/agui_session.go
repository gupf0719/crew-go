package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

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

var sessionScanPattern = strings.NewReplacer(`\`, `\\`, `*`, `\*`, `?`, `\?`, `[`, `\[`, `]`, `\]`)

// 每页在同一脚本里核对当前记录并登记旧取消，避免覆盖并发清理的结果。
var migrateAGUICancellationsScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) ~= 0 then
    return "0"
end
local page = redis.call("SCAN", ARGV[1], "MATCH", ARGV[2], "COUNT", 128)
for _, key in ipairs(page[2]) do
    local id = string.sub(key, tonumber(ARGV[3]) + 1)
    if #id == 76 and string.match(id, "^agui%-thread:%x+$") then
        local kind = redis.call("TYPE", key).ok
        local payload = false
        if kind == "string" then
            payload = redis.call("GET", key)
        end
        if type(payload) == "string" then
            local ok, record = pcall(cjson.decode, payload)
            if ok and type(record) == "table" and type(record.cancellation) == "table" then
                redis.call("ZADD", KEYS[1], 0, id)
            end
        end
    end
end
if page[1] == "0" then
    redis.call("SET", KEYS[2], "1")
end
return page[1]
`)

func (s *AGUISessionStore) cancellationIndex() string { return s.prefix + ":agui:cancel" }

func (s *AGUISessionStore) migrateCancellations(ctx context.Context, cursor string) (string, error) {
	prefix := s.sessionKey("")
	pattern := sessionScanPattern.Replace(prefix) + "*"
	return migrateAGUICancellationsScript.Run(ctx, s.rdb, []string{s.cancellationIndex(), s.cancellationIndex() + ":ready"}, cursor, pattern, strconv.Itoa(len(prefix))).Text()
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

// ListCancellations 从待清理索引按键分页，首次使用分批导入旧记录。
func (s *AGUISessionStore) ListCancellations(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	if limit <= 0 {
		return nil, "", errors.New("redis agui cancellation page size must be positive")
	}
	const migrationPrefix = "migration:"
	if cursor == "" || strings.HasPrefix(cursor, migrationPrefix) {
		pos := strings.TrimPrefix(cursor, migrationPrefix)
		if pos == "" {
			pos = "0"
		}
		next, err := s.migrateCancellations(ctx, pos)
		if err != nil {
			return nil, "", fmt.Errorf("redis agui migrate cancellations: %w", err)
		}
		if next != "0" {
			return nil, migrationPrefix + next, nil
		}
		cursor = ""
	}
	lower := "-"
	if cursor != "" {
		lower = "(" + cursor
	}
	keys, err := s.rdb.ZRangeByLex(ctx, s.cancellationIndex(), &redis.ZRangeBy{Min: lower, Max: "+", Count: int64(limit)}).Result()
	if err != nil {
		return nil, "", fmt.Errorf("redis agui list cancellations: %w", err)
	}
	if len(keys) == limit {
		return keys, keys[len(keys)-1], nil
	}
	return keys, "", nil
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

local index_type = redis.call("TYPE", KEYS[8]).ok
if index_type ~= "none" and index_type ~= "zset" then
    error("agui cancellation index must be a sorted set")
end
if ARGV[12] == "1" then
    check_command("ZADD", KEYS[8], "0", ARGV[11])
else
    check_command("ZREM", KEYS[8], ARGV[11])
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
if ARGV[12] == "1" then
    redis.call("ZADD", KEYS[8], 0, ARGV[11])
else
    redis.call("ZREM", KEYS[8], ARGV[11])
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
	keys = append(keys, taskLeaseKey(s.prefix, key), taskLeaseKey(s.prefix, recordTask), s.sessionKey(key), s.cancellationIndex())
	pending := "0"
	if record != nil && record.Cancellation != nil {
		pending = "1"
	}
	args = append(args, threadToken, recordTask, compose_v2.CheckpointTaskLeaseToken(ctx, leases, recordTask), string(payload), key, pending)
	deleted, err := saveAGUISessionScript.Run(ctx, s.rdb, keys, args...).Int64()
	if err != nil {
		return 0, fmt.Errorf("redis agui session save: %w", err)
	}
	if deleted == -1 {
		return 0, compose_v2.ErrTaskLeaseLost
	}
	return int32(deleted), nil
}
