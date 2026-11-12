package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"crew-go/compose_v2"
	agentRedis "crew-go/redis"

	goredis "github.com/redis/go-redis/v9"
)

const (
	statusRunning     = "running"
	statusInterrupted = "interrupted"
	statusSuccess     = "success"
	statusError       = "error"
	statusCancelled   = "cancelled"
)

type sessionRecord struct {
	ID             string    `json:"session_id"`
	Status         string    `json:"status"`
	Mode           string    `json:"mode"`
	CreatedAt      time.Time `json:"created_at"`
	LastActivity   time.Time `json:"last_activity"`
	InterruptID    string    `json:"interrupt_id,omitempty"`
	PauseRequested bool      `json:"pause_requested,omitempty"`
	Ephemeral      bool      `json:"ephemeral,omitempty"`
	HistorySize    int       `json:"history_size"`
}

func (s sessionRecord) terminal() bool {
	return s.Status == statusSuccess || s.Status == statusError || s.Status == statusCancelled
}

type sessionKey struct{ owner, id string }

// sessionStore 保存可跨连接重建的元数据。update 的回调可重试，输入或输出为 nil 分别表示记录缺失或删除。
type sessionStore interface {
	get(context.Context, sessionKey) (*sessionRecord, error)
	update(context.Context, sessionKey, func(*sessionRecord) (*sessionRecord, error)) error
	list(context.Context, string) ([]sessionRecord, error)
}

type memorySessions struct {
	mu      sync.Mutex
	records map[sessionKey]sessionRecord
	ttl     time.Duration
}

func newMemorySessions(ttl time.Duration) *memorySessions {
	return &memorySessions{records: make(map[sessionKey]sessionRecord), ttl: ttl}
}

func (s *memorySessions) expired(record sessionRecord, now time.Time) bool {
	return s.ttl > 0 && record.terminal() && !now.Before(record.LastActivity.Add(s.ttl))
}

func (s *memorySessions) getLocked(key sessionKey) *sessionRecord {
	record, ok := s.records[key]
	if !ok {
		return nil
	}
	if s.expired(record, time.Now()) {
		delete(s.records, key)
		return nil
	}
	return &record
}

func (s *memorySessions) get(_ context.Context, key sessionKey) (*sessionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getLocked(key), nil
}

func (s *memorySessions) update(ctx context.Context, key sessionKey, edit func(*sessionRecord) (*sessionRecord, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	next, err := edit(s.getLocked(key))
	if err != nil {
		return err
	}
	if next == nil {
		delete(s.records, key)
	} else {
		s.records[key] = *next
	}
	return nil
}

func (s *memorySessions) list(_ context.Context, owner string) ([]sessionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]sessionRecord, 0)
	now := time.Now()
	for key, record := range s.records {
		if key.owner == owner && !s.expired(record, now) {
			out = append(out, record)
		}
	}
	return out, nil
}

func (s *memorySessions) startSweeper(interval time.Duration) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				s.mu.Lock()
				for key, record := range s.records {
					if s.expired(record, now) {
						delete(s.records, key)
					}
				}
				s.mu.Unlock()
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

type redisSessions struct {
	client *goredis.Client
	ttl    time.Duration
}

func redisSessionPrefix(owner string) string { return "crew-plan-web:session:" + owner + ":" }

func decodeSession(cmd *goredis.StringCmd) (*sessionRecord, error) {
	data, err := cmd.Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var record sessionRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *redisSessions) get(ctx context.Context, key sessionKey) (*sessionRecord, error) {
	return decodeSession(s.client.Get(ctx, redisSessionPrefix(key.owner)+key.id))
}

func (s *redisSessions) update(ctx context.Context, key sessionKey, edit func(*sessionRecord) (*sessionRecord, error)) error {
	name := redisSessionPrefix(key.owner) + key.id
	watch := s.client.Watch
	if cp, ok := compose_v2.GetCheckpointer(ctx).(*agentRedis.Checkpointer); ok {
		watch = func(ctx context.Context, fn func(*goredis.Tx) error, keys ...string) error {
			return cp.WatchTaskLease(ctx, key.id, fn, keys...)
		}
	}
	for {
		err := watch(ctx, func(tx *goredis.Tx) error {
			record, err := decodeSession(tx.Get(ctx, name))
			if err != nil {
				return err
			}
			next, err := edit(record)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(p goredis.Pipeliner) error {
				if next == nil {
					p.Del(ctx, name)
					return nil
				}
				data, err := json.Marshal(next)
				if err != nil {
					return err
				}
				var ttl time.Duration
				if next.terminal() {
					ttl = s.ttl
				}
				p.Set(ctx, name, data, ttl)
				return nil
			})
			return err
		}, name)
		if !errors.Is(err, goredis.TxFailedErr) {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

func (s *redisSessions) list(ctx context.Context, owner string) ([]sessionRecord, error) {
	out := make([]sessionRecord, 0)
	seen := make(map[string]bool)
	var cursor uint64
	for {
		keys, next, err := s.client.Scan(ctx, cursor, redisSessionPrefix(owner)+"*", 128).Result()
		if err != nil {
			return nil, err
		}
		if len(keys) > 0 {
			values, err := s.client.MGet(ctx, keys...).Result()
			if err != nil {
				return nil, err
			}
			for i, value := range values {
				raw, ok := value.(string)
				if !ok || seen[keys[i]] {
					continue // 扫描期间到期或已访问过的键。
				}
				var record sessionRecord
				if err := json.Unmarshal([]byte(raw), &record); err != nil {
					return nil, err
				}
				seen[keys[i]] = true
				out = append(out, record)
			}
		}
		if next == 0 {
			return out, nil
		}
		cursor = next
	}
}
