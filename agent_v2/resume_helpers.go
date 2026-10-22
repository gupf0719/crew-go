package agent_v2

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"crew-go/compose_v2"
	"crew-go/hook"
)

// resumePrep 是 commonResumePrep 的返回, 各编排 Resume 共享 boilerplate 的输出.
//
// **AbortDone 协议**: 为 true 时 caller 直接 `return nil, nil` (helper 已处理
// abort 路径含 DeleteSubtree + cp.cleanup event + slog.Warn). caller 不再做
// 后续 Run / hook fire 等业务逻辑.
type resumePrep struct {
	Cper      compose_v2.Checkpointer // 已从 ctx 取出, caller build Run opts 时复用
	Rcfg      resumeConfig            // 已解析的 ResumeOption
	Session   *RunSession             // 已初始化 (nil 时 helper 新建 RunID = sessionID)
	AbortDone bool                    // ResumeActionAbort 已处理 (caller 直接 return nil, nil)
}

// commonResumePrep 抽取 9 编排 Resume 共享 boilerplate (audit #5 阶段 1 补审 MED-2 fix).
//
// **处理范围**:
//  0. Resume 只在顶层调：在一个正在执行的任务里调、或脱离之后续跑脱离出来之前所在的任务 → 拒
//     （[rejectCall]，排在一切之前——Abort 分支会删存档）
//  1. cper := compose_v2.GetCheckpointer(ctx) + nil check + fail-fast
//  2. rcfg := applyResumeOptions(opts)；WithResumeSession 的 session.RunID 为空或 ≠ sessionID → 拒
//  3. ResumeActionAbort 分支: forward graphNS + DeleteSubtree + EmitCpCleanupHelper +
//     slog.Warn (cper backend 失败时业务可订阅) + 标 AbortDone=true 返
//  4. GA-M2 fail-fast: (sid, graphNS) 没有存档、或存档里没有可续的内容（LoadNS +
//     HasResumableWork，与引擎 auto-load 同一个谓词）→ 返 wrap 的 compose_v2.ErrNoPendingCheckpoint
//     （Abort 豁免——步骤 3 已返回）
//  5. session := rcfg.session；没给时建 `&RunSession{RunID: sessionID}`
//  6. ctx = WithSession(ctx, session)
//
// **不处理**:
//   - ResumeActionReplan 路径——各编排自定 (PlanAgent/BasicAgent/GraphAgent 接受,
//     其他 6 编排 fail-fast 返 "<name> agent %q: ResumeActionReplan not supported")
//   - BasicAgent 特定的 `pushStoreCtx(ctx, a.config.Store)` 透传——store 是
//     BasicAgent 私有字段, caller 在 Resume 内自调 (helper 后 + ResumeActionReplan
//     分支之后)
//   - defer outer recover——panic 在 caller 函数内只能 caller defer 注册
//
// **返回语义**:
//   - err != nil: caller 直接 `return nil, err`
//   - res.AbortDone=true: caller 直接 `return nil, nil`
//   - 否则 caller 用 res.Cper / res.Rcfg / res.Session + newCtx 继续
//
// **历史教训** (CLAUDE.md "协议变更四复证"): commit `dc142c3` isAgentNil 7 编排
// 漏 SetExecutor + WithLLMPlannerSubAgents 2 处, 协议落地不完整. 抽 helper 后
// 9 编排走同一 code path, 未来协议改 (e.g., R29 视角 4/5 forward graphNS to abort)
// 一次改 helper 覆盖 9 编排, 不再漏修.
//
// **P0 observability fix** (crew.resumes counter): commonResumePrep 内 push
// WithResumePhase(ctx, rcfg.action) 让 observability 的 OnAgentStart 通过
// GetResumePhase 识别 Resume 路径 + action label fire counter. Abort 路径额外
// fire 一对 OnAgentStart / OnAgentEnd 让 observability 完整记录 abort 事件
// (含 action=abort label + interrupt→resume duration if 之前有 InterruptError).
// 不 fire 这对 hook 时 Abort 路径 observability 完全感知不到 → resume 计数漏 abort.
//
// `info` + `hooks` 参数仅供 Abort 路径 fire 用; Continue / Replan 路径不调
// (各编排 Resume 自己在 commonResumePrep 之后 fire OnAgentStart / OnAgentEnd).
// hooks 为 nil 或 NoOpHooks 时 Abort 路径 skip fire — 业务未注入 Observability 时
// 零开销.
// ⚠️ **subs 是「历史配置从谁那儿借」**——Abort 分支要在删 cp 之前给历史补上悬空 tool_call
// 的结果，而那时写入器还没起（`startHistory` 在本函数返回之后）。编排传自己的
// `historySubs()`，BasicAgent 传 `[]Agent{a}`（它自己就实现了 historyConfigProvider）。
// 传 nil 表示这条路不接历史。
func commonResumePrep(ctx context.Context, agentTypeName, agentName, sessionID string, opts []ResumeOption, info *hook.AgentInfo, hooks hook.Hooks, subs []Agent) (*resumePrep, context.Context, error) {
	// 0. Resume 只在顶层调：在一个正在执行的任务里，续跑的要么是这个任务本身（它正在跑，用不着
	// 续），要么是另一个任务——那得先 DetachTask 拿一个跟当前执行无关的 ctx，否则当前执行留在 ctx
	// 上的位置、答案、新开信号全被带进那个任务（见 enclosingTask）。排在一切之前：Abort 分支会删存档。
	if task, ok := enclosingTask(ctx); ok {
		where := fmt.Sprintf("在正在执行的任务 %q 里", task)
		if task == "" {
			where = "在一个正在执行的任务里（那次 Run 没配 session ID）"
		}
		return nil, ctx, rejectCall(ctx, "%s %q (ns=%q): %s调 Resume(sid=%q)；续跑另一个任务先 DetachTask(ctx)",
			agentTypeName, agentName, compose_v2.GetGraphNS(ctx), where, sessionID)
	}
	// 脱离之后续跑的正是脱离出来之前所在的、此刻仍在执行的任务（脱离几层都算，见 compose_v2.DetachedFrom）：
	// 它就在执行着，再续一遍会把它正在跑的节点重跑一遍（实测）。
	if detachedFromRunning(ctx, sessionID) {
		return nil, ctx, rejectCall(ctx, "%s %q: 脱离之后 Resume 的 sessionID %q 是脱离出来之前所在的任务——"+
			"它此刻正在执行，不能再续跑一次", agentTypeName, agentName, sessionID)
	}

	// 1. cper nil check (跟 9 编排 8330d6e 后 message 格式对齐:
	//    "<agentTypeName> %q: resume requires compose_v2.Checkpointer in ctx (use compose_v2.WithCheckpointer)")
	cper := compose_v2.GetCheckpointer(ctx)
	if cper == nil {
		return nil, ctx, fmt.Errorf("%s %q: resume requires compose_v2.Checkpointer in ctx (use compose_v2.WithCheckpointer)", agentTypeName, agentName)
	}

	// 2. parse opts
	rcfg := applyResumeOptions(opts)

	// sessionID 与 WithResumeSession 传入的 session.RunID 是同一个任务身份的两份声明：对不上就是
	// 两个互斥的意图，拒（排在 Abort 分支之前——它会删存档）。放行的话存档按 sessionID 续、嵌套层
	// 按 session.RunID 找自己的存档：一个任务被劈成两半，嵌套层找不到自己的档、从头重放副作用，
	// 跑成功后按 session.RunID 清子树，把另一个任务挂着的存档删掉（实测 err=nil）；审批规则也跟着
	// session.RunID 走，另一个任务里答的 approve_always 让这个任务挂着的转账静默执行。
	if s := rcfg.session; s != nil && s.RunID != sessionID {
		if s.RunID == "" {
			return nil, ctx, rejectCall(ctx, "%s %q: WithResumeSession 传入的 session 的 RunID 为空；续跑用的 session 的 RunID 要等于 sessionID %q",
				agentTypeName, agentName, sessionID)
		}
		return nil, ctx, rejectCall(ctx, "%s %q: Resume 的 sessionID %q 与 WithResumeSession 传入的 session.RunID %q 不一致；两者是同一个任务的身份，传同一个值",
			agentTypeName, agentName, sessionID, s.RunID)
	}

	// P0 observability: push Resume action 到 ctx — observability OnAgentStart
	// 通过 GetResumePhase 识别 + 记 crew.resumes{crew.resume.action=continue/replan/abort}.
	ctx = WithResumePhase(ctx, rcfg.action)

	// 3. ResumeActionAbort: helper 处理整路径
	if rcfg.action == ResumeActionAbort {
		// review-v6 H-03 fix: forward 父 graphNS 让 Abort 只删本 sub-agent 子树
		// 不误删父 cp / sibling sub-agent / 其他业务在同 sid 下的 cp.
		ns := compose_v2.GetGraphNS(ctx)
		// review-v6 M-08 fix: cper backend (Redis/PG/S3) 失败时 silent → 业务以为
		// abort 成功 cp 清掉但实际 persist → 多租户场景泄漏 sensitive context.
		// slog.Warn 让业务订阅 handler 转告警 (非 fail-fast 因业务可能批量 abort
		// 容忍部分失败).
		// **先补历史再删 cp。** 顺序承重：补记只依赖历史层，但放在删之后的话，
		// DeleteSubtree 失败提前 return 时补记就跟着丢了。
		closeUnansweredToolCallsOnAbort(ctx, resolveHistoryConfigForAbort(ctx, subs), agentName, sessionID)

		delStart := time.Now()
		delCount, dErr := cper.DeleteSubtree(ctx, sessionID, ns)
		// reason="resume_abort"：业务主动弃单（WithResumeAction(Abort)）——跟引擎的
		// "abort"（ErrRunAborted 终局失败，graph_loop Run 出口 purge）不同源，别混成一个值
		compose_v2.EmitCpCleanupHelper(ctx, sessionID, ns, "resume_abort", delCount, time.Since(delStart).Milliseconds(), dErr)
		if dErr != nil {
			slog.WarnContext(ctx, "agent resume abort: cper.DeleteSubtree failed (cp 可能未清)",
				"agent", agentName, "sessionID", sessionID, "ns", ns, "err", dErr)
		}

		// **audit MED fix**：补 push sid——Abort 分支在下方 WithSession（step 6）之前就
		// return，不在此 push 则下面 OnAgentStart/End 的 ctx 无 sid → observability 的 span
		// 缺 crew.run.id（无法 join 回原 run），按 (sid, ns, agent) 记的挂起记录也取不到（等待时长
		// 不记、记录留到过期）。用 WithSessionIDCtx 而非 WithSession：此处
		// session 尚未初始化（step 5）可能 nil。全 9 编排共用本 helper，单一根因。
		ctx = compose_v2.WithSessionIDCtx(ctx, sessionID)

		// P0 observability: Abort 路径 fire OnAgentStart + OnAgentEnd 让
		// observability 完整记录 (含 action=abort counter + 之前 InterruptError 的
		// resume duration). 业务未注入 Observability hook (NoOpHooks) 时 fire 为 no-op,
		// 零开销.
		if info != nil && !isFrameworkNil(hooks) {
			// 钩子报错不拦放弃（放弃是幂等清理），只是不采纳它的 ctx。
			endCtx, _ := compose_v2.CallHook(ctx, func(c context.Context) (context.Context, error) {
				return hooks.OnAgentStart(c, info, &hook.AgentInput{})
			})
			// Abort 是用户主动操作非 error 路径 — 不 push WithEndError, GetEndError 返 nil.
			fireAgentEnd(endCtx, hooks, info, &hook.AgentOutput{}, nil) // 放弃是成功的收尾，钩子 panic 不把它变成错误
		}
		return &resumePrep{Cper: cper, Rcfg: rcfg, AbortDone: true}, ctx, nil
	}

	// 4. GA-M2（四编排 review 衍生协议修复）：显式 Resume 意图 + cp 不存在 →
	// fail-fast 返哨兵。显式 Resume 找不到 cp 不应静默跑业务逻辑——修复前静默 fresh-run：
	// GraphAgent 空 channels 跑出垃圾 final 且 err=nil；BasicAgent 报误导性
	// "PreambleInput nil" 框架内部错；拼错 sid 还会经 defer SaveNS 写垃圾 Pending
	// cp 污染该 sid。校验放 Abort 分支**之后**：Abort 是幂等清理（DeleteSubtree
	// 对不存在 ns 也合法），a2a/server Cancel 协调依赖"框架编排 abort 恒返 nil"。
	// **判据与引擎 auto-load 同一个谓词**（`HasResumableWork`，按内容判）——这道闸门
	// 的唯一职责是让「引擎会拒的」在这里提前变成可读的哨兵错，所以它必须和引擎判
	// **同一件事**。两处判据分叉的实测后果：这边放行、那边不捡 ⟹ 没有初始写入的
	// fresh run，业务收到 "PreambleInput nil" 这种框架内部错，跟真实原因毫无关系。
	//
	// 失败档（frontier 还指着失败分支）从此**放行**：`Resume(sid)` 不带答案 = 重试
	// 失败分支（成功兄弟不重放）；带答案 = 答问题，两者可叠加（混合档一次调用把两件
	// 事都办了）。「重跑必然一样」的终局失败不在此列——那类档在 Run 出口就被
	// `ErrRunAborted` 主动清掉了，走不到这儿。
	ns := compose_v2.GetGraphNS(ctx)
	savedCp, exErr := cper.LoadNS(ctx, sessionID, ns)
	if exErr != nil {
		return nil, ctx, fmt.Errorf("%s %q: resume check checkpoint (sid=%q ns=%q): %w", agentTypeName, agentName, sessionID, ns, exErr)
	}
	if savedCp == nil {
		return nil, ctx, fmt.Errorf("%s %q: resume sid=%q ns=%q: 没有存档（sid 拼错 / 从未跑过 / 上次正常跑完已清）: %w",
			agentTypeName, agentName, sessionID, ns, compose_v2.ErrNoPendingCheckpoint)
	}
	if !savedCp.HasResumableWork() {
		// **手上有存档就把它的样子说出来**。哨兵仍是同一个（业务的 errors.Is 不变——
		// 2026-08-11 定稿过「哨兵不变、消息区分」，别拆），但「档里没有可续的活」和
		// 「压根没有存档」对调用方是两种处境。走到这一格的档 frontier 和挂起都是空
		// ——典型是业务自己往这个槽 SaveNS 了一份「跑完了」的快照。
		return nil, ctx, fmt.Errorf("%s %q: resume sid=%q ns=%q: 存档在（status=%s）但没有可续的内容"+
			"（frontier 与挂起清单都是空——像是一份跑完了的快照被存进了续跑的槽）: %w",
			agentTypeName, agentName, sessionID, ns, savedCp.Status, compose_v2.ErrNoPendingCheckpoint)
	}

	// 5. session init（与 sessionID 一致与否已在步骤 2 之后核对过）
	session := rcfg.session
	if session == nil {
		session = &RunSession{RunID: sessionID}
	}

	// 6. WithSession push
	newCtx := WithSession(ctx, session)

	return &resumePrep{Cper: cper, Rcfg: rcfg, Session: session}, newCtx, nil
}
