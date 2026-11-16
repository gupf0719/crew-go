package compose_v2

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
)

type checkpointRevisionCtxKey struct{}

func (checkpointRevisionCtxKey) ExecutionState() {}

// WithCheckpointRevisionCtx 声明本次执行树的业务版本。节点、工具、模型或条件闭包
// 的恢复语义变化时更新该值。Run 和 Resume 使用相同的版本才能共享存档。
// 版本与编译图自身的版本同时参与校验。
func WithCheckpointRevisionCtx(ctx context.Context, revision string) context.Context {
	return context.WithValue(ctx, checkpointRevisionCtxKey{}, revision)
}

// CheckpointRevisionFromContext 读取调用方声明的执行树版本。
func CheckpointRevisionFromContext(ctx context.Context) string {
	revision, _ := ctx.Value(checkpointRevisionCtxKey{}).(string)
	return revision
}

type resumeCheckpointTargetKey struct{}

func (resumeCheckpointTargetKey) ExecutionState() {}

type resumeCheckpointTarget struct {
	sid, ns, owner string
	checkpoint     *Checkpoint
}

// BindResumeCheckpointTarget 将宿主核对过的目标档绑定到即将恢复的图。
func BindResumeCheckpointTarget(ctx context.Context, sid, ns string, cp *Checkpoint) context.Context {
	return context.WithValue(ctx, resumeCheckpointTargetKey{}, resumeCheckpointTarget{
		sid: sid, ns: ns, owner: GetOwner(ctx), checkpoint: cp,
	})
}

// CanAutoResumeCheckpoint 判断这一层是否应载入已读出的存档。它检查调用意图和任务
// 所有权。图身份由 Graph.CheckCheckpointIdentity 校验，冲突由调用方返回给上层。
func CanAutoResumeCheckpoint(ctx context.Context, cp *Checkpoint) bool {
	return !IsFreshRunCtx(ctx) && cp.HasResumableWork() && cp.BelongsToTask(getRunTaskID(ctx))
}

// checkForeignCheckpointSubtree 保护续跑时临时选用的后端。当前任务只可清理
// 已接管的子树，要检查完整命名空间，包括只有后代存档的场景。
func checkForeignCheckpointSubtree(ctx context.Context, cp Checkpointer, sid, ns string) error {
	taskID := getRunTaskID(ctx)
	if cp == nil || sid == "" || taskID == "" || IsFreshRunCtx(ctx) || sameCheckpointPartition(ctx, cp, sid) {
		return nil
	}
	slots, err := cp.LoadAllInSession(ctx, sid)
	if leaseErr := CheckCheckpointTaskLease(ctx); leaseErr != nil {
		return leaseErr
	}
	if err != nil {
		return fmt.Errorf("check checkpoint subtree %q for task %q: %w", ns, sid, err)
	}
	for _, slot := range slots {
		if IsNSInSubtree(ns, slot.NS) && slot.Checkpoint.HasResumableWork() && !slot.Checkpoint.BelongsToTask(taskID) {
			return RejectCheckpointIdentity(slot.Checkpoint, fmt.Sprintf("checkpoint subtree %q contains another task at %q", ns, slot.NS))
		}
	}
	return nil
}

func sameCheckpointPartition(ctx context.Context, cp Checkpointer, sid string) bool {
	base := TaskCommitContext(ctx)
	if GetOwner(base) != GetOwner(ctx) || GetSessionID(base) != sid {
		return false
	}
	root := GetCheckpointer(base)
	if reflect.ValueOf(cp).Comparable() && cp == root {
		return true
	}
	parent, parentLeased := root.(TaskLeaseProvider)
	child, childLeased := cp.(TaskLeaseProvider)
	if !parentLeased || !childLeased {
		return false
	}
	parentToken := CheckpointTaskLeaseToken(ctx, parent.CheckpointTaskLeases(), sid)
	return parentToken != "" && parentToken == CheckpointTaskLeaseToken(ctx, child.CheckpointTaskLeases(), sid)
}

// CheckCheckpointIdentity 确认存档可以交给这张编译图。旧档仍能解码，续跑要求
// 明确且相同的存档身份。cp 为 nil 时没有需要校验的身份。
func (g *Graph) CheckCheckpointIdentity(cp *Checkpoint) error {
	if cp == nil || (cp.GraphID != "" && cp.GraphID == g.CheckpointIdentity()) {
		return nil
	}
	return RejectCheckpointIdentity(cp, fmt.Sprintf("graph %q has checkpoint identity %q", g.ID(), g.CheckpointIdentity()))
}

// RejectCheckpointIdentity 拒绝恢复这份档。问题 ID 随错误交给祖先图，以保留该分支原有的待答项。
func RejectCheckpointIdentity(cp *Checkpoint, reason string) error {
	conflict := &checkpointIdentityRejection{reason: reason}
	if cp != nil {
		conflict.savedIdentity = cp.GraphID
		conflict.pending = cloneCheckpointQuestions(cp.PendingInterrupts)
	}
	return conflict
}

type checkpointIdentityRejection struct {
	reason        string
	savedIdentity string
	pending       []*AskQuery
	visible       []*AskQuery
	cause         error
	unknownTarget bool
}

func (e *checkpointIdentityRejection) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return fmt.Sprintf("%s: saved checkpoint identity %q: %s", ErrGraphIDConflict, e.savedIdentity, e.reason)
}

func (e *checkpointIdentityRejection) Unwrap() error {
	if e.cause != nil {
		return e.cause
	}
	return ErrGraphIDConflict
}

// projectCheckpointRejection 让上一层得到本图实际还待答的问题，即使它们来自冲突节点的兄弟。
func projectCheckpointRejection(err error, pending []*AskQuery) error {
	return &checkpointIdentityRejection{cause: err, visible: cloneCheckpointQuestions(pending)}
}

func checkpointTargetRejection(err error, cp *Checkpoint) error {
	rejection := &checkpointIdentityRejection{cause: err}
	if cp != nil {
		rejection.pending = cloneCheckpointQuestions(cp.PendingInterrupts)
	}
	return rejection
}

func cloneCheckpointQuestions(pending []*AskQuery) []*AskQuery {
	var out []*AskQuery
	for _, q := range pending {
		if q != nil && q.InterruptID != "" {
			out = append(out, q.clone())
		}
	}
	return out
}

type checkpointRestoreOrigin struct {
	graphID, taskID, epoch, runID, namespace string
	snapshot                                 [sha256.Size]byte
}

func checkpointRestoreOriginOf(cp *Checkpoint) (checkpointRestoreOrigin, error) {
	origin := checkpointRestoreOrigin{
		graphID: cp.GraphID, taskID: cp.TaskID, epoch: cp.Epoch, runID: cp.ID, namespace: cp.NS,
	}
	// 手工导出的快照可能没有后端写入世代，同一任务可导出多份内容。
	if cp.Epoch == "" {
		data, err := MarshalCheckpoint(cp)
		if err != nil {
			return checkpointRestoreOrigin{}, err
		}
		origin.snapshot = sha256.Sum256(data)
	}
	return origin, nil
}

func sameCheckpointSnapshot(before, after *Checkpoint) bool {
	if before == nil || after == nil {
		return before == after
	}
	first, err := checkpointRestoreOriginOf(before)
	if err != nil {
		return false
	}
	second, err := checkpointRestoreOriginOf(after)
	return err == nil && first == second
}

func (cm *ChannelManager) checkRestoreOrigin(cp *Checkpoint) error {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if !cm.hasRestored {
		return nil
	}
	origin, err := checkpointRestoreOriginOf(cp)
	if err != nil {
		return RejectCheckpointIdentity(cp, "cannot verify channel manager restore origin")
	}
	if cm.mixedRestores || cm.restoreOrigin != origin {
		return RejectCheckpointIdentity(cp, "channel manager already contains state restored from a different checkpoint")
	}
	return nil
}

// appendCheckpointIdentityRejections 只收图身份拒绝，避免其他分支的中断或终局信号改写冲突语义。
func appendCheckpointIdentityRejections(out []*checkpointIdentityRejection, err error) []*checkpointIdentityRejection {
	switch e := err.(type) {
	case *checkpointIdentityRejection:
		return append(out, e)
	case interface{ Unwrap() []error }:
		for _, child := range e.Unwrap() {
			out = appendCheckpointIdentityRejections(out, child)
		}
	case interface{ Unwrap() error }:
		return appendCheckpointIdentityRejections(out, e.Unwrap())
	}
	return out
}

// preserveConflictedQuestions 恢复冲突分支原来公开过的问题。其他分支按本次执行结果
// 更新问题，已经兑付、重问或不再等待的问题不会混回来。
func preserveConflictedQuestions(current, previous []*AskQuery, conflicts []*checkpointIdentityRejection, consumed []string) []*AskQuery {
	prior := make(map[string]*AskQuery, len(previous))
	for _, q := range previous {
		if q != nil {
			prior[q.InterruptID] = q
		}
	}
	recorded := make(map[string]bool, len(current)+len(consumed))
	for _, q := range current {
		recorded[q.InterruptID] = true
	}
	for _, id := range consumed {
		recorded[id] = true
	}
	add := func(q *AskQuery) {
		id := q.InterruptID
		if recorded[id] {
			return
		}
		if old := prior[id]; old != nil {
			q = old
		}
		current = append(current, q)
		recorded[id] = true
	}
	for _, conflict := range conflicts {
		for _, q := range conflict.pending {
			if prior[q.InterruptID] != nil {
				add(q)
			}
		}
		for _, q := range conflict.visible {
			add(q)
		}
	}
	return current
}

func checkpointIdentityRejectionError(conflicts []*checkpointIdentityRejection) error {
	if len(conflicts) == 0 {
		return nil
	}
	if len(conflicts) == 1 {
		return conflicts[0]
	}
	errs := make([]error, 0, len(conflicts))
	for _, conflict := range conflicts {
		errs = append(errs, conflict)
	}
	return errors.Join(errs...)
}

// fanoutCheckpointIdentityError 保留有身份冲突的分支及有效兄弟的中断。
func fanoutCheckpointIdentityError(errs []error) error {
	var kept []error
	var hasConflict bool
	for _, err := range errs {
		conflicts := appendCheckpointIdentityRejections(nil, err)
		if len(conflicts) != 0 {
			hasConflict = true
			if errors.Is(err, ErrRunAborted) {
				err = checkpointIdentityRejectionError(conflicts)
			}
			kept = append(kept, err)
		} else if _, pending := pendingInterrupt(err); pending {
			kept = append(kept, err)
		}
	}
	if !hasConflict {
		return nil
	}
	return errors.Join(kept...)
}

func withInterruptValidationError(original, invalid error) error {
	conflicts := appendCheckpointIdentityRejections(nil, original)
	if conflict := checkpointIdentityRejectionError(conflicts); conflict != nil {
		return fmt.Errorf("%w, invalid sibling interruption: %v", conflict, invalid)
	}
	return invalid
}

func checkpointTargetUnknown(conflicts []*checkpointIdentityRejection) bool {
	for _, conflict := range conflicts {
		if conflict.unknownTarget {
			return true
		}
	}
	return false
}
