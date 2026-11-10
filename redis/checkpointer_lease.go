package redis

import (
	"context"
	"errors"
	"time"

	"crew-go/compose_v2"

	"github.com/redis/go-redis/v9"
)

// CheckpointTaskLeases 返回 Redis 存档的跨进程任务租约存储。
func (c *Checkpointer) CheckpointTaskLeases() compose_v2.TaskLeaseStore {
	return taskLeaseStore{rdb: c.rdb, prefix: c.prefix}
}

type taskLeaseStore struct {
	rdb    *redis.Client
	prefix string
}

func taskLeaseKey(prefix, sid string) string {
	return prefix + ":cp:lease:" + sid
}

var acquireTaskLeaseScript = redis.NewScript(`
local owner = redis.call("GET", KEYS[1])
if owner and owner ~= ARGV[1] then
    return 0
end
redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
return 1
`)

var renewTaskLeaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
    return 0
end
return redis.call("PEXPIRE", KEYS[1], ARGV[2])
`)

var releaseTaskLeaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
`)

// TryAcquireTaskLease 尝试为任务设置有效期内唯一的持有者。
func (c taskLeaseStore) TryAcquireTaskLease(ctx context.Context, sid, token string, ttl time.Duration) (bool, error) {
	return c.updateTaskLease(ctx, acquireTaskLeaseScript, sid, token, ttl)
}

// RenewTaskLease 延长当前仍有效且属于 token 的租约。
func (c taskLeaseStore) RenewTaskLease(ctx context.Context, sid, token string, ttl time.Duration) (bool, error) {
	return c.updateTaskLease(ctx, renewTaskLeaseScript, sid, token, ttl)
}

func (c taskLeaseStore) updateTaskLease(ctx context.Context, script *redis.Script, sid, token string, ttl time.Duration) (bool, error) {
	if sid == "" || token == "" || ttl.Milliseconds() <= 0 {
		return false, errors.New("redis cp: invalid task lease request")
	}
	n, err := script.Run(ctx, c.rdb, []string{taskLeaseKey(c.prefix, sid)}, token, ttl.Milliseconds()).Int()
	return n == 1, err
}

// ReleaseTaskLease 只移除属于 token 的租约。
func (c taskLeaseStore) ReleaseTaskLease(ctx context.Context, sid, token string) error {
	return releaseTaskLeaseScript.Run(ctx, c.rdb, []string{taskLeaseKey(c.prefix, sid)}, token).Err()
}
