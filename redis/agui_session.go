package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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

var listAGUICancellationsScript = redis.NewScript(`
local page = redis.call("SCAN", ARGV[1], "MATCH", ARGV[2], "COUNT", ARGV[3])
local pending, invalid = {}, {}
for _, key in ipairs(page[2]) do
    local payload = redis.pcall("GET", key)
    if type(payload) == "string" then
        local ok, record = pcall(cjson.decode, payload)
        if not ok or type(record) ~= "table" then
            invalid[#invalid + 1] = key
        elseif type(record.cancellation) == "table" then
            pending[#pending + 1] = key
        end
    elseif payload then
        invalid[#invalid + 1] = key
    end
end
return {page[1], pending, invalid}
`)

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

// ListCancellations 使用 Redis SCAN 分页查看会话键，仅读取会话记录中的取消标记。
// limit 是每次 SCAN 的 COUNT 提示，游标可能在空页后继续，重读时允许出现重复键。
func (s *AGUISessionStore) ListCancellations(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	if limit <= 0 {
		return nil, "", errors.New("redis agui cancellation page size must be positive")
	}
	if cursor == "" {
		cursor = "0"
	}
	if _, err := strconv.ParseUint(cursor, 10, 64); err != nil {
		return nil, "", fmt.Errorf("redis agui cancellation cursor: %w", err)
	}
	prefix := s.sessionKey("")
	pattern := sessionScanPattern.Replace(prefix) + "*"
	page, err := listAGUICancellationsScript.Run(ctx, s.rdb, nil, cursor, pattern, limit).Slice()
	if err != nil {
		return nil, "", fmt.Errorf("redis agui scan cancellations: %w", err)
	}
	if len(page) != 3 {
		return nil, "", errors.New("redis agui scan returned an invalid cancellation page")
	}
	next, hasCursor := page[0].(string)
	keys, hasKeys := page[1].([]any)
	invalid, hasInvalid := page[2].([]any)
	if !hasCursor || !hasKeys || !hasInvalid {
		return nil, "", errors.New("redis agui scan returned an invalid cancellation page")
	}
	if len(invalid) > 0 {
		slog.WarnContext(ctx, "redis: 无法识别待补偿会话的记录", "keys", invalid)
	}
	selected := make([]string, 0, len(keys))
	for _, entry := range keys {
		if key, ok := entry.(string); ok && strings.HasPrefix(key, prefix) && key != prefix {
			selected = append(selected, key[len(prefix):])
		}
	}
	if next == "0" {
		next = ""
	}
	return selected, next, nil
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
