package compose_v2

import (
	"context"
	"log/slog"

	"crew-go/internal/panicerr"
)

// ────────────────────────────────────────────────────────────
// cp.save / cp.load / cp.cleanup EventSink event (Phase 2 D2 Sub-3)
//
// 设计来源:
//   - framework 在 cp 操作层面 (save / load / cleanup) 主动 fire event, 不只是
//     serde 类型/字段层面
//   - 走 compose_v2.EventSink.Send (跟现有 chunk / reset_stream / compaction /
//     store / history.compact 同协议)
//
// 设计核心:
//   - **opt-in** RunOption WithEmitCpEvent (cp 是持久化观测非压缩, 保留独立开关
//     业务订阅时显式开; 区别于压缩各层已改 sink-gated 无独立开关)
//   - **走 compose_v2.EventSink.Send**: 业务订阅 OrchestrationHooks.OnNodeEvent
//     按 event.Type 分发处理 *CpAction
//   - **零开销 sink nil / cfg.emitCpEvent=false**: 业务不订阅时不付任何代价
//
// observability/hooks.go OnNodeEvent 消费侧 D2 真接入时 case event.Type
// 反序列化加 CS-OSS Monitor 自定义 attr.
// ────────────────────────────────────────────────────────────

// CpEventType 是 compose_v2.Event.Type 字符串.
const CpEventType = "cp"

// CpOperationType 区分 cp 操作类型.
type CpOperationType string

const (
	// CpOperationSave 对应 Checkpointer.SaveNS 调用.
	CpOperationSave CpOperationType = "save"
	// CpOperationLoad 对应 Checkpointer.LoadNS 调用 (Resume 路径).
	CpOperationLoad CpOperationType = "load"
	// CpOperationCleanup 对应 Checkpointer.DeleteSubtree 调用 (清 cp 子树).
	//
	// **两条路径都 fire, 别只当成功信号**: Run 成功收尾 + **终局失败**(ErrRunAborted,
	// 如 cond 报错走默认 CondFailAbort 档 — graph_loop.go Run 出口 defer 主动 purge).
	// 两者语义一致: "这个 sid 的活干完了、cp 不该留", 只是结论不同。业务要区分看
	// CpCleanupAction 所在 event 的上下文 (Run 返的 err 是否 errors.Is(ErrRunAborted)).
	CpOperationCleanup CpOperationType = "cleanup"
)

// CpAction 是 compose_v2.Event.Metadata["action"] 内的结构化字段——业务订阅
// OnNodeEvent 按 Type 分发处理 Save/Load/Cleanup 字段.
type CpAction struct {
	// Type 操作类型.
	Type CpOperationType `json:"type"`

	// Save 仅 Type=save 时非 nil.
	Save *CpSaveAction `json:"save,omitempty"`
	// Load 仅 Type=load 时非 nil.
	Load *CpLoadAction `json:"load,omitempty"`
	// Cleanup 仅 Type=cleanup 时非 nil.
	Cleanup *CpCleanupAction `json:"cleanup,omitempty"`
}

// CpSaveAction 对应 SaveNS 调用.
//
// **注**: 不含 Status 字段 — framework 路径 SaveNS 时 Status 总是 Pending
// (graph_loop.go:300 hardcode 显式赋值, 生产代码无其他 CPStatus 写入). 业务判
// HITL 中断 vs 业务错用 PendingInterruptsCount>0 (audit HIGH-3 fix: 之前 Status
// 字段死字段无信息量误导业务).
type CpSaveAction struct {
	// SessionID cp save 的 sessionID.
	SessionID string `json:"session_id"`
	// NS cp save 的 graphNS (空字符串 = 根 graph).
	NS string `json:"ns"`
	// ChannelsCount cp.Channels 字段数 (估算 cp 体量).
	ChannelsCount int `json:"channels_count"`
	// PendingInterruptsCount cp.PendingInterrupts 长度. 业务用此字段区分:
	//   - >0: HITL 中断 (用户答 ask_user 暂停)
	//   - ==0: 业务错 / panic / ctx.cancel 等其他原因保 cp
	PendingInterruptsCount int `json:"pending_interrupts_count"`
	// DurationMs SaveNS 调用耗时 (毫秒).
	DurationMs int64 `json:"duration_ms"`
	// Ok 调用是否成功.
	Ok bool `json:"ok"`
	// Err 失败原因 (err.Error()), 成功时空字符串.
	Err string `json:"err,omitempty"`
}

// CpLoadAction 对应 LoadNS 调用 / 显式 WithResumeFrom 路径.
//
// **fire 覆盖路径** (audit follow-up):
//   - auto-load (cper+sid+无 WithResumeFrom+无 freshRun): LoadNS 调用前后 fire
//   - 显式 WithResumeFrom (业务直接传 cp / subgraph subNodeFunc 取子 cp): Run 入口 fire
//   - subgraph.go LoadNS (业务嵌套子图 Resume 路径): 调用前后 fire
type CpLoadAction struct {
	// SessionID cp load 的 sessionID.
	SessionID string `json:"session_id"`
	// NS cp load 的 graphNS.
	NS string `json:"ns"`
	// Found cp 是否存在 (auto-load LoadNS 返 nil 表示无 cp; 显式路径恒 true).
	Found bool `json:"found"`
	// Status 若 Found=true, cp.Status (Pending / Success / Unset / Failed).
	Status string `json:"status,omitempty"`
	// WouldResume framework 是否真用此 cp 走 Resume 路径. 业务区分 "看到 cp / 真用 cp":
	//   - true:  Found && Status==Pending && GraphID 不冲突 → framework 真 Resume
	//   - false: cp 存在但 framework fresh Run (Status≠Pending / GraphID 冲突)
	// 业务统计 "Resume 次数" 时用此字段过滤.
	WouldResume bool `json:"would_resume"`
	// Source cp 来源:
	//   - "auto_load": framework 自动 LoadNS (graph_loop.go Run 入口)
	//   - "explicit":  业务显式 WithResumeFrom 传入 cp (含 subgraph subNodeFunc 取子 cp)
	//   - "subgraph":  subgraph.go LoadNS 嵌套子图 Resume 路径
	Source string `json:"source,omitempty"`
	// DurationMs LoadNS 调用耗时 (显式路径为 0 — 无 LoadNS 调用).
	DurationMs int64 `json:"duration_ms"`
	// Ok 调用是否成功.
	Ok bool `json:"ok"`
	// Err 失败原因.
	Err string `json:"err,omitempty"`
}

// CpCleanupAction 对应 DeleteSubtree 调用 (清根 cp 子树).
// **成功路径与终局失败(ErrRunAborted)路径都 fire** — 见 CpOperationCleanup godoc.
type CpCleanupAction struct {
	// SessionID 清的 sessionID.
	SessionID string `json:"session_id"`
	// NS 清的 graphNS prefix.
	NS string `json:"ns"`
	// Reason 清理动因: "success" (Run 干净跑完) / "abort" (终局失败, ErrRunAborted).
	// 两者语义一致 ("这个 sid 的活干完了、cp 不该留"), 只是结论不同 — 业务订阅可据此区分.
	//
	// **是类型化字段不是 Metadata map key**: Event.Metadata 的 key 只有 graph_ns 有
	// sink 层兜底自动补 (hooks.go), reason 没有 — 做成 map key 就只有部分 fire 路径带得上,
	// 业务读不到时分不清"是 success 还是这条路径没填". 跟 action 走才能所有路径都有.
	Reason string `json:"reason,omitempty"`
	// DeletedCount DeleteSubtree 返的删除 cp 数.
	DeletedCount int32 `json:"deleted_count"`
	// DurationMs DeleteSubtree 调用耗时.
	DurationMs int64 `json:"duration_ms"`
	// Ok 调用是否成功.
	Ok bool `json:"ok"`
	// Err 失败原因.
	Err string `json:"err,omitempty"`
}

// emitCpEventCtxKey 是 ctx 内 emitCpEvent flag 的 key. graph_loop.go Run 入口
// 注入 (if cfg.emitCpEvent), subgraph.go subNodeFunc 内 EmitCpEvent helper 取此
// flag 决定是否 fire. 这样 subgraph 不需要拿 caller runOptions, 通过 ctx 透传.
type emitCpEventCtxKey struct{}

// WithEmitCpEventCtx 注入 emitCpEvent flag 到 ctx, 启用 cp.{save,load,cleanup}
// EventSink event fire (默认 false opt-in).
//
// **业务两条启用路径**:
//   - (a) **BasicAgent 单独用**: agent_v2.WithEmitCpEvent(true) Option (内部 push 同款 ctx flag).
//   - (b) **跨嵌套 9 编排用**: 业务直接调本 helper WithEmitCpEventCtx(ctx, true) 注入 ctx
//     后传给 Run —— 推荐, 且是**唯一对非-BasicAgent 编排生效的方式**: 其余 8 编排
//     (Sequential/Parallel/Loop/Routing/Reflect/Handoff/Plan/Graph) 没有 WithEmitCpEvent
//     Option, 只能靠本 helper 注入 ctx flag, 由 9 编排 wrapper 透传到嵌套子 graph,
//     子图 subNodeFunc / 业务自定义 NodeFunc 内 EmitCpEvent 取此 flag 决定是否 fire.
//
// graph_loop.go Run 入口 `cfg.emitCpEvent || isEmitCpEventCtx(ctx)` 双路径任一开都 fire.
func WithEmitCpEventCtx(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, emitCpEventCtxKey{}, enabled)
}

// isEmitCpEventCtx 检 ctx 内 emitCpEvent flag (默认 false).
func isEmitCpEventCtx(ctx context.Context) bool {
	v, _ := ctx.Value(emitCpEventCtxKey{}).(bool)
	return v
}

// EmitCpCleanupHelper 在 cper.DeleteSubtree 前后 wrap fire cp.cleanup event —
// 凡是**主动删 cp** 的地方都该经它，否则那次清理在观测面上不存在。
//
// ⚠️ 这段 godoc 曾写着「共 11 处调用, 漏 fire 占 91%」——实测早已只剩 2 个生产
// 调用者（9 编排的 abort 路径已收敛进 commonResumePrep 一处）。计数是最会漂的那种
// 散文，所以这里不写数：调用点集合由 TestDeleteSubtree_CallSitesAccountedFor 机械
// 对账，那张表钉的是 DeleteSubtree 本身。
//
// reason 填 "success" / "abort" — caller 知道自己在哪条路径上, 见 CpCleanupAction.Reason.
//
// **不守 opt-in**: caller 责任先判. 走 EmitCpEvent 内 ctx 双守 (flag + sink).
//
//	delCount, delErr := cper.DeleteSubtree(ctx, sid, ns)
//	compose_v2.EmitCpCleanupHelper(ctx, sid, ns, "abort", delCount, time.Since(start), delErr)
func EmitCpCleanupHelper(ctx context.Context, sessionID, ns, reason string, deletedCount int32, durationMs int64, err error) {
	action := &CpCleanupAction{
		SessionID:    sessionID,
		NS:           ns,
		Reason:       reason,
		DeletedCount: deletedCount,
		DurationMs:   durationMs,
		Ok:           err == nil,
	}
	if err != nil {
		action.Err = err.Error()
	}
	EmitCpEvent(ctx, &CpAction{Type: CpOperationCleanup, Cleanup: action})
}

// EmitCpEvent 投递 cp event 到 compose_v2.EventSink — 业务自定义 NodeFunc 内调
// cper.SaveNS/LoadNS/DeleteSubtree 时用 (subgraph.go subNodeFunc 也用此 helper).
//
// **opt-in 双重守**: ctx 内 emitCpEvent flag (来源二选一: BasicAgent 的
// agent_v2.WithEmitCpEvent RunOption 经 Run 入口注入, 或业务直接 WithEmitCpEventCtx
// 注入) + EventSink 在 ctx (NodeFunc scope 内 framework 自动注 nodeEventSink).
// 业务侧 ctx 无 flag / 无 sink 时 short-circuit return.
//
// **graph_loop.go Run defer 内 fire 不走本 helper**——走 cfg.hooks.OnNodeEvent 直接
// emit（R33 后 Run-level sink 全程在 ctx，但 defer 直调 hooks 等价且不绕一层）.
// nodeID="" 标"非节点 framework 级 event"
// (业务订阅 OnNodeEvent 时按 nodeID=="" + Event.Type==CpEventType 识别).
//
// cp 事件只作观测：钩子 panic 在这里兜住、记一条带栈的日志，不往上冒（同 deliverCpEvent）。
func EmitCpEvent(ctx context.Context, action *CpAction) {
	if action == nil {
		return
	}
	if !isEmitCpEventCtx(ctx) {
		return
	}
	sink := GetEventSink(ctx)
	if sink == nil {
		return
	}
	defer recoverObserver(ctx, "compose_v2: cp 事件的钩子 panic，已兜住、不影响这次执行")
	sink.Send(Event{
		Type:    CpEventType,
		Content: string(action.Type),
		Metadata: map[string]any{
			"action": action,
		},
	})
}

// deliverCpEvent 把框架发的 cp 事件交给编排钩子（nodeID 为 ""）。cp 事件只作观测：钩子 panic 在这里兜住、记一条
// 带栈的日志，不往上冒——这次 Run 返回的错误、档写没写成、清没清都跟钩子没 panic 时一样（见 exit_checkpoint.go
// 不变量 1）。成功收尾时发 cp.cleanup 的那一刻档已经清掉，让 Run 因此报错只会引来从头重跑、副作用重放。
func deliverCpEvent(ctx context.Context, cfg *runOptions, ev Event) {
	defer recoverObserver(ctx, "compose_v2: cp 事件的钩子 panic，已兜住、不影响这次 Run")
	cfg.hooks.OnNodeEvent(eventHookCtx(ctx), "", ev)
}

// recoverObserver 兜住只作观测的代码（cp 事件的钩子、Run 出口结论定下之后的日志）里的 panic：记一条带栈的日志，
// 到此为止。必须直接 defer 调用（recover 只在被 defer 的那个函数里生效）。
func recoverObserver(ctx context.Context, msg string) {
	if r := recover(); r != nil {
		panicerr.Log(ctx, slog.LevelError, msg, panicerr.New(r))
	}
}
