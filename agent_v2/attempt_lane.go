package agent_v2

import (
	"context"

	"crew-go/compose_v2"
)

// runDiscarded 报一次调用以错误 err 结束之后，它的进度是不是整个作废了：没有 checkpointer（一点没存下，
// 下一次只能从头再来），或者终局失败（[compose_v2.ErrRunAborted]，存档已清）。成功、或者留下了可续的存档
// （下一次从存档续）时不算作废。cper 是这次调用交给引擎的 checkpointer。
//
// 用在两处：取走的插话要不要放回收件箱（settleInterjections）；编排成员的一次失败尝试被下一次尝试取代时，
// 下一次是从头再跑还是从存档续（attemptLanes）。
func runDiscarded(cper compose_v2.Checkpointer, err error) bool {
	return err != nil && (cper == nil || !compose_v2.LeavesResumableCheckpoint(err))
}

// attemptLanes 让编排成员每次尝试写进会话的事件先落在 sess 的一条暂存 lane 上（[RunSession.ForkLane]），按这次
// 尝试有没有被取代决定去留：
//
//   - 成功的当场提交给 sess（[CommitLaneEvents]）。
//   - 失败的先留着，到下一次尝试开始时再定：下一次从存档续（进度存下了，见 runDiscarded）就先交上去再跑——
//     那些步骤不重跑，事件属于留下来的进度；下一次从头再跑就丢掉——它被取代了。
//   - 重试结束时（finish）最后那次失败尝试的事件照常提交：没有下一次尝试取代它，它就是调用方该看到的结局
//     （软降落的成员带着收尾答复返回 MaxIterationsError，丢掉的话调用方拿不到那条答复，审计实测）。
//   - panic 的当场提交：panic 越过重试往上走（RunWithRetry 不兜 panic），同样不会再有下一次尝试。
//
// 为什么要分：成员共享编排的会话，没有存档时重试是从头再跑——失败那次尝试写下的事件（BasicAgent 在 preamble
// 记下的那条输入、成员是编排时它里面已经成功的几步的问答）留在会话里的话，重来那次把它们当上文读进去，模型
// 看到同一个问题两次、还看到作废那次的答复（实测）。
//
// lane 看到的上文是这次尝试开始那一刻 sess 的全部事件（快照，见 [RunSession.ForkLane]），成员的上文不受影响；
// 尝试进行中，它写的事件只在它拿到的那个会话（lane）里看得到，调用方手里的 sess 要等这次尝试有了结局才看得到
// （同 ParallelAgent 的分支）。尝试一次接一次地跑，不并发。
type attemptLanes struct {
	sess    *RunSession
	cper    compose_v2.Checkpointer
	pending *RunSession // 上一次失败尝试的 lane，等下一次尝试或 finish 来定去留
	pendErr error
}

func newAttemptLanes(ctx context.Context, sess *RunSession) *attemptLanes {
	return &attemptLanes{sess: sess, cper: compose_v2.GetCheckpointer(ctx)}
}

// run 跑一次尝试：先处置上一次失败尝试留下的 lane（这一次就是取代它的那次），再在新 lane 上跑 fn。
func (a *attemptLanes) run(fn func(*RunSession) error) error {
	if a.sess == nil {
		return fn(nil)
	}
	a.settle(false)
	lane := a.sess.ForkLane()
	// fn 没正常返回（panic）时当场提交。不 recover：兜住 panic、留下 panic 现场的栈归引擎的节点兜底，在这里
	// recover 再 panic 会把栈换成这一行。
	returned := false
	defer func() {
		if !returned {
			CommitLaneEvents(a.sess, lane)
		}
	}()
	err := fn(lane)
	returned = true
	if err == nil {
		CommitLaneEvents(a.sess, lane)
		return nil
	}
	a.pending, a.pendErr = lane, err
	return err
}

// finish 在重试结束之后调：最后那次失败尝试的事件照常提交。
func (a *attemptLanes) finish() { a.settle(true) }

// settle 处置上一次失败尝试的 lane：final（不会再有下一次尝试）或下一次从存档续时提交，下一次从头再跑时丢掉。
func (a *attemptLanes) settle(final bool) {
	if a.pending == nil {
		return
	}
	if final || !runDiscarded(a.cper, a.pendErr) {
		CommitLaneEvents(a.sess, a.pending)
	}
	a.pending, a.pendErr = nil, nil
}
