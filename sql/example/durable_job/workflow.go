package main

import (
	"context"
	dbsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
	storage "crew-go/sql"
)

const (
	waitReason = "demo:external_job"
	sidPrefix  = "demo-job:"
	completed  = "completed"
	cancelled  = "cancelled"
	timedOut   = "timed_out"
)

type externalJob struct {
	handle, result string
	completed      bool
}

// jobService 是外部作业系统的边界。重试用同一个 key。lookup 在作业不存在时返回 (nil, nil)。
type jobService interface {
	submit(ctx context.Context, key, input string) (string, error)
	lookup(ctx context.Context, key string) (*externalJob, error)
	cancel(ctx context.Context, handle string) error
}

type job struct {
	id, input, handle, decision, result string
	deadline                            int64
	delivered                           bool
}

type runner struct {
	db      *dbsql.DB
	cp      *storage.Checkpointer
	service jobService
	graph   *compose_v2.Graph
}

func newRunner(ctx context.Context, db *dbsql.DB, service jobService) (*runner, error) {
	cp := storage.NewCheckpointer(db, storage.SQLite{})
	if err := cp.Setup(ctx); err != nil {
		return nil, err
	}
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS job_wait (
		id TEXT PRIMARY KEY, input TEXT NOT NULL, deadline INTEGER NOT NULL, handle TEXT NOT NULL DEFAULT '',
		decision TEXT NOT NULL DEFAULT '', result TEXT NOT NULL DEFAULT '', delivered INTEGER NOT NULL DEFAULT 0,
		cleaned INTEGER NOT NULL DEFAULT 0
	)`)
	if err != nil {
		return nil, err
	}
	r := &runner{db: db, cp: cp, service: service}
	r.graph, err = compose_v2.NewBuilder().
		AddNode("wait_for_job", r.waitForJob).
		AddEdge(compose_v2.StartNode, "wait_for_job").
		AddEdge("wait_for_job", compose_v2.EndNode).
		Compile()
	return r, err
}

func (r *runner) create(ctx context.Context, id, input string, deadline time.Time) error {
	// 先记任务身份与幂等键，提交作业在图内进行。
	_, err := r.db.ExecContext(ctx, "INSERT INTO job_wait (id, input, deadline) VALUES (?, ?, ?)",
		id, input, deadline.UnixMilli())
	return err
}

func (r *runner) load(ctx context.Context, id string) (job, error) {
	j := job{id: id}
	err := r.db.QueryRowContext(ctx,
		"SELECT input, deadline, handle, decision, result, delivered FROM job_wait WHERE id = ?", id).
		Scan(&j.input, &j.deadline, &j.handle, &j.decision, &j.result, &j.delivered)
	return j, err
}

func (r *runner) waitForJob(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
	id := strings.TrimPrefix(compose_v2.GetSessionID(ctx), sidPrefix)
	j, err := r.load(ctx, id)
	if err != nil {
		return nil, err
	}
	if j.handle == "" {
		j.handle, err = r.recoverHandle(ctx, j)
		if err != nil {
			return nil, err
		}
		if j.handle != "" {
			if _, err := r.db.ExecContext(ctx, "UPDATE job_wait SET handle = ? WHERE id = ?", j.handle, id); err != nil {
				return nil, err
			}
		}
	}
	answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "等待构建结果", Reason: waitReason})
	if err != nil {
		return nil, err
	}
	if j.decision == "" || answer.Content != j.decision {
		return nil, fmt.Errorf("external job %q: answer does not match the stored outcome", id)
	}
	if j.handle != "" && (j.decision == timedOut || j.decision == cancelled) {
		if err := r.service.cancel(ctx, j.handle); err != nil {
			return nil, err
		}
	}
	// 先持久化业务交付，再让图成功清档。图重放时这条 UPDATE 仍只交付一次。
	_, err = r.db.ExecContext(ctx, "UPDATE job_wait SET delivered = 1 WHERE id = ? AND decision = ? AND delivered = 0",
		id, j.decision)
	return nil, err
}

func (r *runner) recoverHandle(ctx context.Context, j job) (string, error) {
	if j.decision == timedOut || j.decision == cancelled {
		// 已结束的任务只找回可能已经创建的作业，避免为取消动作新建作业。
		remote, err := r.service.lookup(ctx, j.id)
		if err != nil || remote == nil {
			return "", err
		}
		return remote.handle, nil
	}
	handle, err := r.service.submit(ctx, j.id, j.input)
	if err == nil && handle == "" {
		err = errors.New("external job returned an empty handle")
	}
	return handle, err
}

func (r *runner) choose(ctx context.Context, id, decision, result string) error {
	// 完成回调、取消与超时在同一槽竞争，先落库的一方决定终态。
	_, err := r.db.ExecContext(ctx, "UPDATE job_wait SET decision = ?, result = ? WHERE id = ? AND decision = ''",
		decision, result, id)
	return err
}

// drive 同时用于初始运行、回调和恢复扫描。取得真实任务租约后才检查待答点并执行。
func (r *runner) drive(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ctx = compose_v2.WithCheckpointerCtx(ctx, r.cp)
	sid := sidPrefix + id
	ctx, activity, err := compose_v2.BeginTaskActivity(ctx, sid, nil)
	if err != nil {
		return err
	}
	defer activity.Release()

	for range 2 {
		j, err := r.load(ctx, id)
		if err != nil {
			return err
		}
		if j.delivered {
			return r.clean(ctx, id, sid)
		}
		cp, err := r.cp.LoadNS(ctx, sid, "")
		if err != nil {
			return err
		}
		opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(r.cp), compose_v2.WithSessionID(sid)}
		if cp != nil && len(cp.PendingInterrupts) > 0 {
			if j.decision == "" {
				return nil
			}
			if len(cp.PendingInterrupts) != 1 || cp.PendingInterrupts[0].Reason != waitReason {
				return fmt.Errorf("external job %q: unexpected pending point", id)
			}
			opts = append(opts, compose_v2.WithResumeAnswers(map[string]*schema.Message{
				cp.PendingInterrupts[0].InterruptID: schema.UserMessage(j.decision),
			}))
		}
		err = compose_v2.Run(ctx, r.graph, compose_v2.NewChannelManager(), opts...)
		if err == nil {
			return r.clean(ctx, id, sid)
		}
		if _, interrupted := compose_v2.IsInterrupt(err); !interrupted {
			return err
		}
		// 回调可能早于挂起落盘。存档提交后在同一任务租约下再检查一次。
	}
	return nil
}

func (r *runner) clean(ctx context.Context, id, sid string) error {
	if _, err := r.cp.DeleteSubtree(ctx, sid, ""); err != nil {
		return err
	}
	_, err := r.db.ExecContext(ctx, "UPDATE job_wait SET cleaned = 1 WHERE id = ? AND delivered = 1", id)
	return err
}

func (r *runner) reconcile(ctx context.Context, now time.Time) error {
	rows, err := r.db.QueryContext(ctx, "SELECT id FROM job_wait WHERE cleaned = 0")
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close() // 保留扫描错误。
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var combined error
	for _, id := range ids {
		j, err := r.load(ctx, id)
		if err == nil && j.decision == "" {
			err = r.reconcileDecision(ctx, j, now)
		}
		if err == nil {
			err = r.drive(ctx, id)
		}
		if err != nil && !errors.Is(err, compose_v2.ErrTaskBusy) {
			combined = errors.Join(combined, fmt.Errorf("drive %q: %w", id, err))
		}
	}
	return combined
}

func (r *runner) reconcileDecision(ctx context.Context, j job, now time.Time) error {
	remote, err := r.service.lookup(ctx, j.id)
	if err != nil {
		return err
	}
	if remote != nil && remote.completed {
		return r.choose(ctx, j.id, completed, remote.result)
	}
	if j.deadline <= now.UnixMilli() {
		return r.choose(ctx, j.id, timedOut, "")
	}
	return nil
}

// callback 接收作业完成通知，再从作业系统读取结果。通知本身不提供任务归属或结果。
func (r *runner) callback(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, 4096)).Decode(&body); err != nil || body.ID == "" {
		http.Error(w, "invalid callback", http.StatusBadRequest)
		return
	}
	ctx := req.Context()
	j, err := r.load(ctx, body.ID)
	if err != nil {
		if errors.Is(err, dbsql.ErrNoRows) {
			http.Error(w, "unknown job", http.StatusNotFound)
		} else {
			http.Error(w, "job store unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	if j.decision == "" {
		remote, err := r.service.lookup(ctx, body.ID)
		if err != nil || remote == nil || !remote.completed {
			http.Error(w, "job result unavailable", http.StatusServiceUnavailable)
			return
		}
		if err := r.choose(ctx, body.ID, completed, remote.result); err != nil {
			http.Error(w, "job result could not be saved", http.StatusServiceUnavailable)
			return
		}
	}
	if err := r.drive(ctx, body.ID); err != nil && !errors.Is(err, compose_v2.ErrTaskBusy) {
		http.Error(w, "job reconciliation failed", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
