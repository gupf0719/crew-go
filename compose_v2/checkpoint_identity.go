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

type checkpointRestartKey struct{}

func (checkpointRestartKey) ExecutionState() {}

type checkpointRestartScope struct {
	base         context.Context
	checkpointer Checkpointer
	sid, ns      string
}

func withCheckpointRestartScope(ctx, base context.Context, cfg *runOptions) context.Context {
	return context.WithValue(ctx, checkpointRestartKey{}, checkpointRestartScope{
		base: base, checkpointer: cfg.checkpointer, sid: cfg.sessionID, ns: cfg.graphNS,
	})
}

// restartOwnsCheckpoint 允许低阶空根槽重开时回收原分区的旧子档。候选档必须能在
// 最终根存储上下文中读回同一份快照，仅有相同的后端句柄和 namespace 并不足够。
func restartOwnsCheckpoint(ctx context.Context, cp Checkpointer, sid string, slot NSCheckpoint) (bool, error) {
	root, ok := ctx.Value(checkpointRestartKey{}).(checkpointRestartScope)
	if !ok || root.sid != sid || GetOwner(root.base) != GetOwner(ctx) ||
		!reflect.ValueOf(cp).Comparable() || cp != root.checkpointer ||
		root.ns == slot.NS || !IsNSInSubtree(root.ns, slot.NS) ||
		slot.Checkpoint.TaskID == "" || slot.Checkpoint.Status == CPStatusAborted {
		return false, nil
	}
	lookupCtx, cancel := context.WithCancel(root.base)
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	original, err := cp.LoadNS(lookupCtx, sid, slot.NS)
	return err == nil && sameCheckpointSnapshot(original, slot.Checkpoint), err
}

// verifyGraphCheckpointScope 核对最终存储上下文。自定义 Checkpointer 可以按私有 ctx 值
// 路由；共用一个句柄或精确槽为空都不足以证明其后代存档属于当前任务。
func verifyGraphCheckpointScope(ctx context.Context, cfg *runOptions, rootRun bool, exact *Checkpoint) error {
	if IsFreshRunCtx(ctx) {
		return checkFreshTaskCheckpoints(ctx, cfg.checkpointer, cfg.sessionID)
	}
	taskID := getRunTaskID(ctx)
	var authorizedExact *Checkpoint
	if rootRun && taskID == "" {
		if cfg.resumeFrom != nil {
			taskID = cfg.resumeFrom.TaskID
		} else if exact.HasResumableWork() {
			taskID = exact.TaskID
		}
		if taskID == "" {
			// 顶层刚校验过的精确槽可按旧任务兼容规则载入，不能据此接管身份不明的后代。
			authorizedExact = exact
		}
	}
	err := verifyCheckpointSubtree(ctx, cfg.checkpointer, cfg.sessionID, cfg.graphNS, taskID, authorizedExact)
	if err != nil && !errors.Is(err, ErrGraphIDConflict) && !errors.Is(err, ErrTaskLeaseLost) {
		// 无法验证所有权时，外层协议同样不能将它视作普通失败并清档。
		return checkpointTargetRejection(errors.Join(ErrGraphIDConflict, err), exact)
	}
	return err
}

func verifyCheckpointSubtree(ctx context.Context, cp Checkpointer, sid, ns, taskID string, authorizedExact *Checkpoint) error {
	slots, err := cp.LoadAllInSession(ctx, sid)
	if leaseErr := CheckCheckpointTaskLease(ctx); leaseErr != nil {
		return leaseErr
	}
	if err != nil {
		return fmt.Errorf("check checkpoint subtree %q for task %q: %w", ns, sid, err)
	}
	for _, slot := range slots {
		if !IsNSInSubtree(ns, slot.NS) || !checkpointRequiresTaskOwnership(slot.Checkpoint) {
			continue
		}
		if taskID != "" && slot.Checkpoint.TaskID == taskID {
			continue
		}
		if slot.NS == ns && authorizedExact != nil && sameCheckpointSnapshot(authorizedExact, slot.Checkpoint) {
			continue
		}
		owned, err := restartOwnsCheckpoint(ctx, cp, sid, slot)
		if err != nil {
			return fmt.Errorf("check restart checkpoint %q for task %q: %w", slot.NS, sid, err)
		}
		if owned {
			continue
		}
		return RejectCheckpointIdentity(slot.Checkpoint, fmt.Sprintf("checkpoint subtree %q has unverified task ownership at %q", ns, slot.NS))
	}
	return nil
}

// checkpointRequiresTaskOwnership 区分仍有任务意义的存档与普通完成快照。
// 终局标记虽然不可续，仍负责阻止旧快照重放。
func checkpointRequiresTaskOwnership(cp *Checkpoint) bool {
	return cp != nil && (cp.Status == CPStatusAborted || cp.HasResumableWork())
}

func sameCheckpointPartition(ctx context.Context, cp Checkpointer, sid string) bool {
	base := TaskCommitContext(ctx)
	if GetOwner(base) != GetOwner(ctx) || GetSessionID(base) != sid {
		return false
	}
	root := GetCheckpointer(base)
	if fixed, ok := cp.(*InMemoryCheckpointer); ok && root == fixed {
		return true
	}
	// 共用有效租约的前提是两个客户端确实访问同一存储域，见 TaskLeaseProvider。
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

// terminalCheckpointConflictError 保留终局与冲突的原错误链，不向调用方投影冲突链上的旧问题。
type terminalCheckpointConflictError struct{ error }

func (e *terminalCheckpointConflictError) Unwrap() error { return e.error }

// terminalCheckpointConflict 保留终局原链和保护子档所需的身份冲突。
func terminalCheckpointConflict(terminal, conflict error) error {
	if errors.Is(terminal, ErrGraphIDConflict) {
		if _, ok := terminal.(*terminalCheckpointConflictError); ok {
			return terminal
		}
		return &terminalCheckpointConflictError{error: terminal}
	}
	if conflict == nil {
		return terminal
	}
	return &terminalCheckpointConflictError{error: errors.Join(terminal, conflict)}
}

func checkpointConflictForAbort(conflicts []*checkpointIdentityRejection, graphIDConflict bool) error {
	if conflict := checkpointIdentityRejectionError(conflicts); conflict != nil {
		return conflict
	}
	if graphIDConflict {
		return ErrGraphIDConflict
	}
	return nil
}

// pickFanoutTerminalOrIdentityError 先确定终局，保留同时发现的身份冲突。
// 只有这一批仍可续跑时才向外提交有效兄弟的中断。
func pickFanoutTerminalOrIdentityError(errs []error) error {
	var kept []error
	var conflicts []*checkpointIdentityRejection
	var terminal error
	var graphIDConflict bool
	for _, err := range errs {
		if terminal == nil && errors.Is(err, ErrRunAborted) {
			terminal = err
		}
		graphIDConflict = graphIDConflict || errors.Is(err, ErrGraphIDConflict)
		before := len(conflicts)
		conflicts = appendCheckpointIdentityRejections(conflicts, err)
		if len(conflicts) > before {
			kept = append(kept, err)
		} else if _, pending := pendingInterrupt(err); pending {
			kept = append(kept, err)
		}
	}
	if terminal != nil {
		return terminalCheckpointConflict(terminal, checkpointConflictForAbort(conflicts, graphIDConflict))
	}
	if len(conflicts) == 0 {
		return nil
	}
	return errors.Join(kept...)
}

func withInterruptValidationError(original, invalid error) error {
	conflicts := appendCheckpointIdentityRejections(nil, original)
	return terminalCheckpointConflict(invalid, checkpointConflictForAbort(conflicts, errors.Is(original, ErrGraphIDConflict)))
}

func checkpointTargetUnknown(conflicts []*checkpointIdentityRejection) bool {
	for _, conflict := range conflicts {
		if conflict.unknownTarget {
			return true
		}
	}
	return false
}
