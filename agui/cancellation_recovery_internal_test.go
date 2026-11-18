package agui

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/internal/panicerr"
)

type panickingCancellationPollStore struct{}

func (panickingCancellationPollStore) TryAcquireCancellationPoll(context.Context, string, time.Duration) (bool, error) {
	panic("acquire cancellation poll")
}

func (panickingCancellationPollStore) ReleaseCancellationPoll(context.Context, string) error {
	panic("release cancellation poll")
}

func TestCancellationPollStorePanicsAreIsolated(t *testing.T) {
	ctx := t.Context()
	store := panickingCancellationPollStore{}
	h := &handler{}
	token, err := h.scanCancellations(ctx, store, &cancellationRecovery{})
	var panicRecord *panicerr.Error
	if token == "" || !errors.As(err, &panicRecord) || panicRecord.Value != "acquire cancellation poll" {
		t.Fatalf("acquire should retain the token for a conditional release, token=%q err=%v", token, err)
	}
	err = releaseCancellationPoll(ctx, store, token)
	if !errors.As(err, &panicRecord) || panicRecord.Value != "release cancellation poll" {
		t.Fatalf("release panic should be reported as an error, got %v", err)
	}
}

type slowCancellationPages struct {
	SessionStore
	mu        sync.Mutex
	cp        *compose_v2.InMemoryCheckpointer
	keys      []string
	cursors   []string
	records   map[string]*SessionRecord
	token     string
	expiresAt time.Time
}

func (s *slowCancellationPages) TryAcquireCancellationPoll(_ context.Context, token string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if now.Before(s.expiresAt) {
		return false, nil
	}
	s.token, s.expiresAt = token, now.Add(ttl)
	return true, nil
}

func (s *slowCancellationPages) ReleaseCancellationPoll(_ context.Context, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if token == s.token {
		s.token, s.expiresAt = "", time.Time{}
	}
	return nil
}

func (s *slowCancellationPages) ListCancellations(ctx context.Context, cursor string, _ int) ([]string, string, error) {
	s.mu.Lock()
	s.cursors = append(s.cursors, cursor)
	s.mu.Unlock()
	timer := time.NewTimer(6 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, "", ctx.Err()
	case <-timer.C:
	}
	if cursor == "" {
		return s.keys[:1], "page-2", nil
	}
	return s.keys[1:], "", nil
}

func (s *slowCancellationPages) LoadSession(_ context.Context, key string) (*SessionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.records[key], nil
}

func (s *slowCancellationPages) SaveSession(ctx context.Context, key string, record *SessionRecord, dropTask string) (int32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old := s.records[key]; old == nil || dropTask != old.TaskID {
		return 0, errors.New("补偿选中了错误任务")
	}
	n, err := s.cp.DeleteSubtree(ctx, dropTask, "")
	if err == nil {
		s.records[key] = record
	}
	return n, err
}

func (s *slowCancellationPages) scanState() (cursors []string, token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.cursors), s.token
}

func TestCancellationRecoveryDispatchesSlowPagesAfterPollExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &slowCancellationPages{
			cp:      compose_v2.NewInMemoryCheckpointer(),
			records: make(map[string]*SessionRecord),
		}
		for _, thread := range []string{"first", "second"} {
			key := sharedSessionID(sessionKey{threadID: thread})
			store.keys = append(store.keys, key)
			store.records[key] = &SessionRecord{TaskID: thread, Closing: true, Cancellation: &SessionCancellation{ThreadID: thread}}
			if err := store.cp.SaveNS(t.Context(), thread, "", &compose_v2.Checkpoint{Status: compose_v2.CPStatusPending}); err != nil {
				t.Fatal(err)
			}
		}
		h := &handler{cp: store.cp, sessionStore: store, build: func(hook.Hooks) agent_v2.Agent {
			return agent_v2.NewBasicAgent(agent_v2.WithName("recovery"))
		}}
		server := newServer(h)
		defer server.Close()
		time.Sleep(5500 * time.Millisecond)
		synctest.Wait()
		if acquired, err := store.TryAcquireCancellationPoll(t.Context(), "follower", 5*time.Second); err != nil || !acquired {
			t.Fatalf("补偿扫描租约到期后其他副本应能取得租约：acquired=%v err=%v", acquired, err)
		}

		time.Sleep(time.Second)
		synctest.Wait()
		first, _ := store.LoadSession(t.Context(), store.keys[0])
		if cp, err := store.cp.LoadNS(t.Context(), "first", ""); err != nil || cp != nil || first != nil {
			t.Fatalf("迟于轮询租约的第一页应完成真实清档和会话收尾：cp=%v err=%v", cp, err)
		}
		if cursors, token := store.scanState(); !slices.Equal(cursors, []string{""}) || token != "follower" {
			t.Fatalf("过期轮次应把第二页留给后续轮次，其他副本仍持有新租约：cursors=%v token=%q", cursors, token)
		}

		time.Sleep(11 * time.Second)
		synctest.Wait()
		second, _ := store.LoadSession(t.Context(), store.keys[1])
		if cp, err := store.cp.LoadNS(t.Context(), "second", ""); err != nil || cp != nil || second != nil {
			t.Fatalf("下一轮应从上次游标继续清理第二页：cp=%v err=%v", cp, err)
		}
		if cursors, _ := store.scanState(); !slices.Equal(cursors, []string{"", "page-2"}) {
			t.Fatalf("分页游标没有跨轮推进：%v", cursors)
		}
	})
}
