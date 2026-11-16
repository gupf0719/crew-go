package compose_v2

import "context"

type restoredResume struct {
	version int
	pending map[string]struct{}
}

func withRestoredResume(ctx context.Context, cp *Checkpoint) context.Context {
	delivery := resumeDeliveryFrom(ctx)
	if cp == nil && delivery.restored == nil {
		return ctx
	}
	delivery.restored = nil
	if cp != nil {
		pending := make(map[string]struct{}, len(cp.PendingInterrupts))
		for _, question := range cp.PendingInterrupts {
			if question != nil && question.Breakpoint == nil && question.InterruptID != "" {
				pending[question.InterruptID] = struct{}{}
			}
		}
		delivery.restored = &restoredResume{version: cp.SchemaVer, pending: pending}
	}
	return context.WithValue(ctx, resumeCtxKey{}, delivery)
}

// RestoredCheckpointVersion 返回本图已恢复的存档版本。没有恢复过存档则返回零值。
func RestoredCheckpointVersion(ctx context.Context) int {
	if restored := resumeDeliveryFrom(ctx).restored; restored != nil {
		return restored.version
	}
	return 0
}

// RestoredPhaseQuestion 按当前相位和执行位置核对本图恢复的挂起问题。
// 从第一问起，沿调用方提供的连续答复寻找待答位置。没有对应的存档问题时返回空 ID 和 nil。
// 返回匹配的问题 ID 和一个报告该问题是否已在本次执行中收到答复的函数。
// 本函数不交出答复、不消耗提问序号，实际答复仍由 [Ask] 或 [NextAnswerSlot] 读取。
func RestoredPhaseQuestion(ctx context.Context) (id string, answered func() bool) {
	delivery := resumeDeliveryFrom(ctx)
	if delivery.restored == nil || len(delivery.restored.pending) == 0 {
		return "", nil
	}
	site := interruptSiteFromCtx(ctx)
	if site.NodeID == "" {
		return "", nil
	}
	for ; ; site.Idx++ {
		id := site.InterruptID()
		if answerAlreadyConsumed(ctx, id) {
			return "", nil
		}
		if _, pending := delivery.restored.pending[id]; pending {
			return id, func() bool { return answerAlreadyConsumed(ctx, id) }
		}
		if delivery.context == nil || delivery.context.UserInputs[id] == nil {
			return "", nil
		}
	}
}
