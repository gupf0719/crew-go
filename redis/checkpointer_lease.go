package redis

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"crew-go/compose_v2"

	"github.com/redis/go-redis/v9"
)

// CheckpointTaskLeases 返回 Redis 存档的跨进程任务租约存储。
func (c *Checkpointer) CheckpointTaskLeases() compose_v2.TaskLeaseStore {
	return taskLeaseStore{rdb: c.rdb, prefix: c.prefix}
}

// WatchTaskLease 在监视任务租约和业务键的 Redis 事务内校验当前持有者。
// fn 须经传入的 Tx 使用 TxPipelined 提交。租约在读取后变化时，提交返回 redis.TxFailedErr，
// 调用方可以重新调用本方法，并据 ErrTaskLeaseLost 结束过期操作。
func (c *Checkpointer) WatchTaskLease(ctx context.Context, sid string, fn func(*redis.Tx) error, keys ...string) error {
	token := compose_v2.CheckpointTaskLeaseToken(ctx, c.CheckpointTaskLeases(), sid)
	if token == "" {
		return fmt.Errorf("redis task %q: %w", sid, compose_v2.ErrTaskLeaseLost)
	}
	leaseKey := taskLeaseKey(c.prefix, sid)
	return c.rdb.Watch(ctx, func(tx *redis.Tx) error {
		current, err := tx.Get(ctx, leaseKey).Result()
		if errors.Is(err, redis.Nil) || (err == nil && current != token) {
			return fmt.Errorf("redis task %q: %w", sid, compose_v2.ErrTaskLeaseLost)
		}
		if err != nil {
			return err
		}
		return fn(tx)
	}, append(slices.Clone(keys), leaseKey)...)
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
