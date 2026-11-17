package compose_v2

import (
	"context"
	"errors"
	"fmt"
)

type resumeAdmissionKey struct{}

func (resumeAdmissionKey) ExecutionState() {}

type resumeAdmission struct {
	sid, ns, taskID string
}

func withResumeAdmission(ctx context.Context, sid, ns, taskID string) context.Context {
	return context.WithValue(ctx, resumeAdmissionKey{}, resumeAdmission{sid: sid, ns: ns, taskID: taskID})
}

func hasResumeAdmission(ctx context.Context, sid, ns, taskID string) bool {
	admitted, ok := ctx.Value(resumeAdmissionKey{}).(resumeAdmission)
	return ok && admitted.sid == sid &&
		(admitted.taskID == "" || admitted.taskID == taskID) && IsNSInSubtree(admitted.ns, ns)
}

// ErrCheckpointResumeUnconfirmed 表示此前的续跑已获准，尚无已落盘的后续结果。
// 保留存档供诊断或显式放弃。它同时匹配 [ErrCheckpointNotResumable]。
var ErrCheckpointResumeUnconfirmed = fmt.Errorf("%w: previous resume outcome is unconfirmed", ErrCheckpointNotResumable)

// PrepareResumeCheckpoint 核对当前档和准入预期，并为目标槽生成新世代。
// 提交时仍须确保当前档未被改变。空预期允许在空槽中恢复导出的快照。
func PrepareResumeCheckpoint(sid, ns string, current, expected, source *Checkpoint) (*Checkpoint, error) {
	if sid == "" {
		return nil, errors.New("sessionID is empty")
	}
	if current != nil && current.Status == CPStatusResuming || source != nil && source.Status == CPStatusResuming {
		return nil, ErrCheckpointResumeUnconfirmed
	}
	if !sameStoredCheckpoint(current, expected) {
		return nil, ErrCheckpointStale
	}
	if !source.HasResumableWork() || source.Status == CPStatusAborted || current != nil && current.Status == CPStatusAborted {
		return nil, ErrNoPendingCheckpoint
	}
	if current != nil && current.GraphID != "" && source.GraphID != "" && current.GraphID != source.GraphID {
		return nil, ErrGraphIDConflict
	}
	if current.HasResumableWork() && !sameStoredCheckpoint(current, source) {
		return nil, ErrCheckpointStale
	}
	claimed := *source
	claimed.ID, claimed.NS = sid, ns
	if source.ID != sid || source.NS != ns || claimed.ParentNS == "" {
		claimed.ParentNS = ParentNS(ns)
	}
	claimed.Status = CPStatusResuming
	claimed.Epoch = newEpoch()
	return &claimed, nil
}

// ClaimResume 在同一锁内核对当前档和记录持有的续跑世代。
func (c *InMemoryCheckpointer) ClaimResume(ctx context.Context, sid, ns string, expected, source *Checkpoint) (*Checkpoint, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	claimed, err := PrepareResumeCheckpoint(sid, ns, c.data[sid][ns], expected, source)
	if err != nil {
		return nil, err
	}
	if c.data[sid] == nil {
		c.data[sid] = make(map[string]*Checkpoint)
	}
	c.data[sid][ns] = claimed
	return claimed, nil
}
