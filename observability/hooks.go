package observability

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"time"
	"unicode/utf8"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/fallback"
	"crew-go/hook"
	specModel "crew-go/modules/model"
	"crew-go/schema"
)

// redactString 跑 RedactPatterns 列表替换为 [REDACTED].
//
// audit² B-HIGH-B1 fix: 之前 redact 只在 renderContent (RecordMessageContent 路径) 跑,
// OnError lastError.message → endSpan/sweepInFlight 写 error.message attr 跳过
// redact → 业务 LLM error 含 PII (e.g., "Unauthorized API key sk-xxx" / "user_id=12345
// mobile=13800138000") 明文上 trace UI. 抽 helper 让 OnError 路径也走 redact.
//
// 业务未设 RedactPatterns 时直接返原 string (零开销).
func (o *Observability) redactString(s string) string {
	if o == nil || len(o.cfg.RedactPatterns) == 0 {
		return s
	}
	for _, re := range o.cfg.RedactPatterns {
		if re == nil {
			continue
		}
		s = re.ReplaceAllString(s, "[REDACTED]")
	}
	return s
}

// renderContent 把业务消息 / args / result 序列化 + redact + 截断 1MB 作为
// `gen_ai_content` event attribute 值.
//
// review P0-P2 整体 audit² 视角 B HIGH (RecordMessageContent + RedactPatterns +
// MaxInlineEventBytes 三联 dead) fix: 业务 opt-in RecordMessageContent=true
// 后真采 messages 内容, 配 RedactPatterns 真做 PII filter, 超 MaxInlineEventBytes
// 真截断.
//
// caller 必先判 `cfg.RecordMessageContent` 才调本函数.
func (o *Observability) renderContent(v any) string {
	if v == nil {
		return ""
	}
	var s string
	switch x := v.(type) {
	case string:
		s = x
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		s = string(b)
	}
	s = o.redactString(s)
	// 1MB 截断 (spec line 141 gen_ai_content 上限)
	// P2.6 audit LOW-2 fix: 之前 s[:max-3] 按 byte 切, 中文 (3 byte) / emoji (4 byte)
	// 在边界处被切成无效 UTF-8 — spec 不报错但前端展示崩. 回退到 rune 边界.
	if max := o.cfg.MaxInlineEventBytes; max > 0 && len(s) > max {
		cut := max - 3
		// 若 cut 落在多字节字符中间, 回退到上一 rune 完整边界.
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		s = s[:cut] + "..."
	}
	return s
}

// AgentHooks 返业务 hook (hook.Hooks 接口 8 method, ISSUE-82 后 9→8 删 OnAgentInit) — 业务用
// `agent_v2.WithHooks(obs.AgentHooks())` 注入.
//
// 实现按 impl-guide §1 trace 埋点表:
//   - OnAgentStart → 创 gen_ai.agent.{name} span (operation.name=agent) + RunsInProgress.Inc（整个调用 1 次）
//   - OnAgentEnd → 结 agent span + fire MetricAgentRunTotal/Duration/Rounds + RunsInProgress.Dec
//   - OnModelStart → 创 gen_ai.chat.{name}_model_call span (operation.name=chat)
//   - OnModelEnd → 结 model span + token attrs + fire MetricLLMCallDuration/TokensTotal
//   - OnModelEndStream → 同 OnModelEnd + chunk event (wrap reader 监听 EOF)
//   - OnToolStart → 创 gen_ai.tool.{name} span (operation.name=tool) + 识别 ActivateSkillToolName fire MetricSkillActivateTotal
//   - OnToolEnd → 结 tool span + fire MetricToolCallDuration + ToolErrorTotal(Success=false)
//   - OnError → 当前 span attr 更新 status (实际由 *End hook 内的 status 决定)
//
// **手动挂进钩子链时 obs 必须排第一**：`NewChainHooks(obs.AgentHooks(), business)`（`obs.Install`
// 已经这么装）。排在业务钩子后面有两个坑：
//   - 业务 hook 在 obs 前 panic 会冒泡到 framework outer recover, 跳过 obs.OnAgentEnd →
//     MetricAgentRunTotal Counter 漏 fire (Sub-5 Round 4 audit L1)；
//   - 链内部不按来源采纳 ctx：前面的业务钩子返回存下来的 ctx 时，obs 收到的是上一次 Run 的 sid，
//     按它重建出一个之后没人清的会话（实测：两个 Run 各自 CleanupSession 之后还剩 1 个）。框架自己
//     接住链的返回值时是按来源采纳的（compose_v2.CallHook），只有链内部不是。
//
// 跟 OTel SDK 共存提示: obs 自己创 OTel-compatible span (gen_ai.* attrs), 业务接
// 自家 OTel/LangSmith hook 时**不要**再创 span — trace tree 重复且 hierarchy 错位.
// 业务想集成第三方 trace backend 应在 OTLP collector 端转发, 而非业务 hook 内重复创 span.
func (o *Observability) AgentHooks() hook.Hooks {
	if o == nil {
		return hook.NoOpHooks{}
	}
	return &agentHooks{o: o}
}

// OrchHooks 返编排 hook (agent_v2.OrchestrationHooks 5 method 业务级抽象) —
// 业务用 `agent.Run(ctx, sess, input, agent_v2.WithOrchHooks(obs.OrchHooks()))`
// 注入. framework 内部 adaptHooks 自动转 compose_v2.OrchestrationHooks
// (带 Channels 的底层接口).
//
// **必接** (Sub-5 Round 4 audit M2 + Round 5 audit P5 fix): 漏接 OrchHooks 时下列
// 8 个 metric 全漏 fire (Round 5 数字订正: 之前 godoc 写"5 类" 错, 实际 8 个):
//   - MetricCompactTotal / Duration / Ratio (4 层 compact 共 3 个)
//   - MetricCpSaveTotal / SizeBytes (cp 操作共 2 个)
//   - MetricFanoutBranches (toolsFn 并发 fanout)
//   - MetricNodeExecutionTotal (节点执行次数, OnNodeStart 路径不在 OnNodeEvent 内
//     但 OrchHooks 全 5 method 都需注入才有 OnNodeStart)
//   - MetricSkillActivateTotal (虽 OnToolStart 触发, 但 SkillBox 通过 OrchHooks
//     EventSink 协议传递 activation 上下文)
//
// 业务想要完整 metric 覆盖必须**同时**接 AgentHooks + OrchHooks.
//
// 手动挂进编排钩子链时同样要排第一（理由同 AgentHooks：链内部不按来源采纳 ctx）。
//
// review P0-P2 整体 audit² 视角 A HIGH-A1 fix: 之前返 agentOrchAdapter 实现的是
// compose_v2.OrchestrationHooks (5 method 带 Channels), 跟 agent_v2.WithOrchHooks
// 期望的 agent_v2.OrchestrationHooks (5 method ctx + string nodeID + Event) 签名不
// 匹配 → 业务编译失败. 改返 agentOrchAdapter 实现 agent_v2.OrchestrationHooks.
//
// 实现:
//   - OnGraphStart → 创 gen_ai.workflow.{graph_id} root span (operation.name=workflow)
//   - OnGraphEnd → 结 graph span
//   - OnNodeStart → 创 gen_ai.workflow.{graph_id}.{node_id} child span
//   - OnNodeEnd → 结 node span
//   - OnNodeEvent → 当前 graph/node span 加 event (e.g., chunk / compact)
func (o *Observability) OrchHooks() agent_v2.OrchestrationHooks {
	if o == nil {
		return agent_v2.NoOpOrchestrationHooks{}
	}
	return &agentOrchAdapter{o: o}
}

// ─────────────── agentHooks: hook.Hooks 8 method 实现 (ISSUE-82 后 9→8) ───────────────

type agentHooks struct {
	o *Observability
}

// warnIfHalfInstalled 在「业务级 hook 接上了、编排级没接」时喊一次。
//
// **为什么这个检测有必要**：两条链的安装点不在同一处（业务级是构造期
// `agent_v2.WithHooks(...)`、编排级是 `WithCtxOrchHooks(ctx, ...)` 或 `WithOrchHooks`
// RunOption），漏一条**不报错也不崩**，只是 8 个 metric 静默不 fire。实测我们自己的
// example 里 4 个有 2 个就漏了。
//
// **判据是「obs 自己那半跑过没有」而不是「ctx 里有没有」**：编排 hook 可以走 ctx
// 也可以走 RunOption（`extractOrchHooksOrCtx` 两条都认），只查 ctx 会把用 RunOption 的
// 业务误报成漏装。
//
// **为什么在 OnAgentEnd 而不是 OnAgentStart**：`OnAgentStart` 在图执行之前 fire，
// 那时编排 hook 还没轮到，一查一个准的误报。到 OnAgentEnd 时图已经跑完了。
//
// 每个 Observability 实例只喊一次——漏装是个配置问题，每轮刷屏没有额外信息。
func (h *agentHooks) warnIfHalfInstalled(ctx context.Context) {
	if h == nil || h.o == nil || h.o.orchFired.Load() {
		return
	}
	if h.o.halfInstallWarned.Swap(true) {
		return
	}
	slog.WarnContext(ctx, "observability: 只接了业务级 hook，编排级 hook 没接上——"+
		"compact(3) / cp(2) / fanout / node / skill 共 8 个 metric 不会 fire，"+
		"node 与 graph 的 span 也不会建。"+
		"用 obs.Install(ctx, agentOpts...) 一次接上两条；"+
		"agent 与 ctx 在不同地方装配时（如 a2a/server），"+
		"记得 agent_v2.WithCtxOrchHooks(ctx, obs.OrchHooks()) 那一半也要装")
}

// resumeInputCount 数「这次续跑调用方给了几条输入」——**两条通道都算**。
//
// 寻址型（`UserInputs`，按 InterruptID 投递给某个提问）+ 非寻址型（`UserText`，
// 「用户这轮说了什么」）。只数前者的话，只走非寻址通道的续跑（Replan 的推荐形态）在
// trace 上显示「0 输入」，而那恰恰是最需要看见用户说了什么的场景。
func resumeInputCount(rc *compose_v2.ResumeContext) int {
	n := len(rc.UserInputs)
	if rc.UserText != "" {
		n++
	}
	return n
}

// resumeInputContent 拼「这次续跑的输入内容」给 content event——同样两条通道都带。
//
// 只有寻址型时保持原样返 map（不改既有 wire 形状）；有非寻址型时才包一层，把它作为
// 一个独立条目带上，让排查的人看得见用户那句话。
func resumeInputContent(rc *compose_v2.ResumeContext) any {
	if rc.UserText == "" {
		return rc.UserInputs
	}
	out := make(map[string]any, len(rc.UserInputs)+1)
	for k, v := range rc.UserInputs {
		out[k] = v
	}
	out["__user_text__"] = rc.UserText
	return out
}

func (h *agentHooks) OnAgentStart(ctx context.Context, info *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	// 8-method 协议：OnAgentStart 整个调用 1 次（由 agent_v2.{编排}.Run/Resume top-level
	// fire，原 OnAgentInit 逻辑迁入）——创 agent span + runs.inprogress++ + 暂存 startNanos。
	// 父 = graph span（业务 BasicAgent.Run 在 OnGraphStart 之后立即 fire OnAgentStart）。
	sess := h.o.session(ctx)
	graphNS := compose_v2.GetGraphNS(ctx)

	// 找父 span (agent 作 trace root 树):
	//   - 顶层 (graphNS=""): parentSpanID="" → startSpan 走 agent/graph 顶层分支用
	//     sess.rootSpanID 作 trace root.
	//   - 嵌套 sub-agent (graphNS!=""): 父在某个祖先 ns 层. ParentNS 一次只剥一层
	//     (fan-out ns 剥 #N 后缀, 普通 ns 剥末段), 但 active agent 可能在多层之上 —
	//     例: leaf ns="worker/skill#abc" → ParentNS="worker/skill" (剥 #abc), 但
	//     active agent 在 "worker" 层. 需循环 ParentNS 到根直到找到 in-flight agent.
	//     优先级:
	//     1. 任何祖先 ns 层的 in-flight agent (沿 ParentNS 链上溯)
	//     2. 都没找到 → parent="" 走顶层 case (业务异常路径, 极罕见)
	parentSpanID := ""
	if graphNS != "" {
		// 从父 ns 起上溯（排除自身——本 agent span 即将建）找祖先层 in-flight agent。
		parentSpanID, _ = sess.findActiveAgentSpanFrom(compose_v2.ParentNS(graphNS))
	}

	attrs := []*commonpb.KeyValue{}
	if info.Type != "" {
		attrs = append(attrs, strKV("gen_ai.agent.kind", info.Type))
	}
	// agent span operation.name 恒 "agent" (对齐 OTel invoke_agent) — **不**被 component
	// kind 覆盖. component kind (retrieval/embedding/guardrail) 只标在 tool span
	// (routeToolSpanKind) 与 node span (OnNodeStart 读 GetComponentKind); agent 是 invoke
	// 不是组件动作. 原 evaluator 是唯一在 agent 级标 component 的用例, 已退化删除 (CS-OSS/
	// OTel operation.name 无 evaluator, LLM-as-judge sub-agent invocation 即 invoke_agent).
	operationName := "agent"
	spanName := "gen_ai.agent." + info.Name
	h.o.startSpan(ctx, "agent", operationName,
		spanName, info.Name, 0, "",
		parentSpanID, graphNS, attrs)
	// runs.inprogress gauge ++ (Run/Resume 入口) — agent.OnAgentEnd 内 -- 配对.
	if g := h.o.Gauge(MetricRunsInProgress, nil); g != nil {
		g.Inc()
	}
	// 暂存 startTime 让 OnAgentEnd 算 duration（agent 轮次走 MetricAgentRounds histogram,
	// 不在 span attr 重复——8-method 下 OnAgentStart 整个调用 1 次, round 恒 0 无意义）.
	if v, ok := sess.spans.Load(registryKey("agent", graphNS, info.Name, 0, "", "")); ok {
		entry := v.(*spanEntry)
		entry.mu.Lock()
		entry.runStartNanos = time.Now().UnixNano()
		entry.mu.Unlock()
	}

	// P0 fix: Resume 路径检测 + fire MetricResumesTotal{action} + MetricInterruptResumeDuration.
	//
	// agent_v2.commonResumePrep 在 Resume 入口 push WithResumePhase(ctx, action) —
	// GetResumePhase 返 (action, true) 表示当前是 Resume; (Continue, false) 表示 Run 路径.
	// Run 路径跳过 resume 计数 (Run 不是 resume).
	if action, isResume := agent_v2.GetResumePhase(ctx); isResume {
		// MetricResumesTotal Counter: label{agent_name, agent_kind, action=continue/replan/abort}
		if c := h.o.Counter(MetricResumesTotal, map[string]string{
			LabelAgentName:    info.Name,
			LabelAgentKind:    info.Type,
			LabelResumeAction: action.String(),
		}); c != nil {
			c.Inc()
		}
		// MetricInterruptResumeDuration Histogram: 中断→Resume 时长 (秒). 仅当 OnAgentEnd
		// 之前记录了 interrupt nanos (= sess.interruptStartNanos != 0) 时算; nanos=0
		// 说明此 Resume 不是接 InterruptError (e.g., 业务直接调 Resume 而无 Run 中断 — 少见).
		startNanos := sess.interruptStartNanos.Swap(0) // read + clear 防多次 Resume 重复算
		var resumeDurationSec float64
		if startNanos > 0 {
			resumeDurationSec = float64(time.Now().UnixNano()-startNanos) / 1e9
			if hg := h.o.Histogram(MetricInterruptResumeDuration, BucketsLLMDuration,
				map[string]string{
					LabelAgentName: info.Name,
					LabelAgentKind: info.Type,
				}); hg != nil {
				hg.Observe(resumeDurationSec)
			}
		}
		// P1 fix: 加 `resume` span event 标 Resume 入口精确时刻 — 业务在 trace timeline
		// 看到"这里从 cp 恢复继续". 含 action + user_inputs_count + interrupt→resume 时长.
		resumeEvAttrs := []*commonpb.KeyValue{
			strKV("crew_resume_action", action.String()),
		}
		if rc, ok := compose_v2.GetResumeContext(ctx); ok && rc != nil {
			// **两条通道都要数**：寻址型（按 InterruptID 投递的答案）+ 非寻址型
			// （「用户这轮说了什么」）。只数前者的话，走非寻址通道的 Replan 在 trace 上
			// 显示「用户 0 输入」——而 Replan 恰恰是最需要看见用户说了什么的那种续跑。
			resumeEvAttrs = append(resumeEvAttrs, intAttr("user_inputs_count", int64(resumeInputCount(rc))))
		}
		if resumeDurationSec > 0 {
			resumeEvAttrs = append(resumeEvAttrs, floatAttr("interrupt_to_resume_seconds", resumeDurationSec))
		}
		h.o.addEvent(ctx, "agent", info.Name, 0, "", "resume", resumeEvAttrs)
	}

	// AI_TRACE_SPEC.md §Events: agent span 加 `agent_input` event 让前端看到
	// **本次 agent 调用的真 input** (不是 system prompt / 历史 / 内部 messages).
	//
	// 语义 (业务期望对齐监控 UI 视角):
	//   - Run 路径: in.Messages 末条 user message (caller 传给 agent.Run 的真 user 输入)
	//   - Resume 路径: ResumeContext.UserInputs (Resume 答案 map by InterruptID)
	//     framework 在 Resume 入口给 hook 喂空 in.Messages (走 cp 重建无新 input).
	//     observability 从 ctx 读 ResumeContext 显示业务真"输入"(answer to interrupt).
	//
	// 默认仅 messages_count + crew_resume_indicator 元数据;
	// 业务 opt-in `cfg.RecordMessageContent=true` 后填 gen_ai_content (真 JSON 内容,
	// 1MB 截断 + RedactPatterns PII filter).
	evAttrs := []*commonpb.KeyValue{}
	var inputContent any
	if action, isResume := agent_v2.GetResumePhase(ctx); isResume {
		// Resume 路径: 显示 ResumeContext.UserInputs (业务真"输入"= Resume 答案)
		evAttrs = append(evAttrs, strKV("crew_resume_action", action.String()))
		if rc, ok := compose_v2.GetResumeContext(ctx); ok && rc != nil && resumeInputCount(rc) > 0 {
			evAttrs = append(evAttrs, intAttr("messages_count", int64(resumeInputCount(rc))))
			inputContent = resumeInputContent(rc)
		} else {
			evAttrs = append(evAttrs, intAttr("messages_count", 0))
		}
	} else if in != nil {
		// Run 路径: 取末条 user message (caller 传给 agent.Run 的真 user 输入)
		userMsg := lastUserMessage(in.Messages)
		if userMsg != nil {
			evAttrs = append(evAttrs, intAttr("messages_count", 1))
			inputContent = userMsg
		} else {
			evAttrs = append(evAttrs, intAttr("messages_count", int64(len(in.Messages))))
		}
	}
	if h.o.cfg.RecordMessageContent && inputContent != nil {
		evAttrs = append(evAttrs, strKV(attrGenAIContent, h.o.renderContent(inputContent)))
	}
	h.o.addEvent(ctx, "agent", info.Name, 0, "", "agent_input", evAttrs)
	return ctx, nil
}

func (h *agentHooks) OnAgentEnd(ctx context.Context, info *hook.AgentInfo, out *hook.AgentOutput) context.Context {
	h.warnIfHalfInstalled(ctx)
	// review P2 audit² HIGH-A3 fix: sweep 本 agent 下面所有 in-flight model/tool span
	// (compose_v2 error 路径 OnModelEnd/OnToolEnd 不 fire → 永留泄漏).
	// agent span 自己最后 finalize.
	graphNS := compose_v2.GetGraphNS(ctx)
	// OnAgentEnd sweep: outer 用 "error" 默认 (in-flight 没 *End fire 通常是 error 路径)
	// 个体 lastError 优先 (OnError 路径已写)
	h.o.sweepInFlight(ctx, "model|"+graphNS+"|"+info.Name+"|", "error", "", "")
	// 方案 C: tool 类 span registry key 已归一为 "tool|graphNS|callID" (registryKey 对
	// isToolKind 始终归一, 含空 callID) — 只 sweep "tool|" 前缀即覆盖 tool/skill/retrieval/
	// embedding/guardrail 全部. tool sweep 按 graphNS 前缀不带 info.Name (跨
	// agent 共享 toolkit 设计).
	//
	// **不变量依赖**: 此 sweep 假设同一 graphNS 下不会有两个 BasicAgent 并发在飞 — 否则先
	// 结束的 agent A 的 OnAgentEnd 会把 agent B 仍在飞的 tool span 提前 sweep 成 error.
	// 当前 Sequential/Parallel/Handoff 都经 JoinNS(parentNS, name) 给每 sub-agent 独立
	// childNS ({sequential,parallel,handoff}_agent.go), graphNS 天然隔离无触发路径; 业务
	// 自定义编排若让多 agent 共享同 graphNS 需自负此风险.
	h.o.sweepInFlight(ctx, "tool|"+graphNS+"|", "error", "", "")
	// Phase 2 Sub-5b: agent.run.duration histogram observe (OnAgentStart 设
	// startNanos, 此处算 now - start). 在 endSpan 之前算因为 endSpan 会清 sess.spans entry.
	sess := h.o.session(ctx)
	if v, ok := sess.spans.Load(registryKey("agent", graphNS, info.Name, 0, "", "")); ok {
		entry := v.(*spanEntry)
		entry.mu.Lock()
		startNanos := entry.runStartNanos
		entry.mu.Unlock()
		if startNanos > 0 {
			durationSec := float64(time.Now().UnixNano()-startNanos) / 1e9
			// Round 5 Q1 fix: label 对齐 OTel — gen_ai.agent.name / gen_ai.agent.kind.
			if hg := h.o.Histogram(MetricAgentRunDuration, BucketsLLMDuration,
				map[string]string{
					LabelAgentName: info.Name,
					LabelAgentKind: info.Type,
				}); hg != nil {
				hg.Observe(durationSec)
			}
		}
	}
	// runs.inprogress gauge -- (OnAgentStart ++ 配对)
	if g := h.o.Gauge(MetricRunsInProgress, nil); g != nil {
		g.Dec()
	}
	// Phase 2 Sub-5c: gen_ai.agent.rounds histogram (本 Run 跑了几轮 ReAct).
	// info.Round 是从 0 起的 round index, OnAgentEnd 时是最后一轮的 round number.
	// 业务用此 metric 看 P50/P95 轮次分布判断 prompt 复杂度 / max_rounds 配置合理性.
	// Round 5 Q1 fix: label 对齐 OTel.
	if hg := h.o.Histogram(MetricAgentRounds, BucketsCount,
		map[string]string{
			LabelAgentName: info.Name,
			LabelAgentKind: info.Type,
		}); hg != nil {
		hg.Observe(float64(info.Round))
	}
	// Phase 2 Sub-5a + audit HIGH-1 fix: agent.run.total counter inc.
	status := "success"
	if ctx != nil && ctx.Err() != nil {
		if ctx.Err() == context.DeadlineExceeded {
			status = "timeout"
		} else {
			status = "cancelled"
		}
	}
	// P0 fix: 检 agent_v2.GetEndError 拿 Run/Resume 真 retErr — InterruptError 时
	// status="blocked" + fire MetricInterruptTotal + 记 sess.interruptStartNanos 让
	// 下次 Resume 入口算 duration. retErr 非 nil 但非 InterruptError 时 status="error".
	// agent_v2 Run/Resume 在 defer OnAgentEnd 前调 WithEndError(ctx, retErr) push.
	var interruptInfo *compose_v2.InterruptError
	// 顶层 retErr 兜底 (传 endSpan 作 fallback errMsg): 7 编排非通信主体错误 (MaxIterations
	// 等) 不 fire OnError → lastError 空 → 之前 agent span status=error 但 error.message 空.
	var fbErrType, fbErrMsg string
	// 提到外层：metrics_mapping 段（下方）要用它给 gen_ai_stale_writes 建反查路径。
	// 空串 = 本次不是 stale 路径。
	var staleOp string
	// stale 的分类走 endSpan 的**强制位**而非 fallback 位（下方 forceErrType）：走
	// model 节点时 OnError 会抢先按表层症状写 error.type="model_call_error"，业务按
	// error.type="checkpoint_stale" 筛并发冲突就会漏掉这一整类。OnError 只看得见
	// "model 节点报错了"，只有 Run 出口这里才知道根因是同一会话被两个副本处理。
	var forceErrType, forceErrMsg string
	if retErr := agent_v2.GetEndError(ctx); retErr != nil {
		// **陈旧写回要先于 IsInterrupt 判**——引擎出口写档撞上它时返 ErrCheckpointNotPersisted、
		// 中断不在链上，但自研 / 远程 agent 仍可能把它跟中断绑在一条链上交过来；那种照
		// IsInterrupt-first 的顺序会落进 blocked 分支，而那个分支不设 errType/
		// errMsg ⟹ 导出的是一个 status=blocked、OTel Status=OK、零错误信息的**绿 span**，
		// 与一次正常 HITL 提问逐字节相同，运维完全看不出发生了并发冲突。
		//
		// 而且那个中断**永远不会被 Resume**（a2a/agui 都已明确不留 entry 等续发），
		// 计进 interrupt metric 会让业务的 interrupt→resume 漏斗静默产生缺口。
		//
		// 顺序与另外两个消费方对齐：`a2a/server/executor.go` 的 keepPending 判据、
		// `agui/handler.go` 的 handleOutcome 都把 stale 提到了 IsInterrupt 之前。
		if errors.Is(retErr, compose_v2.ErrCheckpointStale) {
			status = "error"
			// message 送**完整**错误（含 op / ns / 两代 epoch）。这里是内部上报面，跟
			// agui / a2a 那两处对前端的脱敏不是一回事——排查并发冲突时"撞的是哪个 ns、
			// 基线是哪一代"正是唯一有用的信息，而 sid 在 trace 里本就以
			// gen_ai.request.id 存在、epoch 是随机串，都不是敏感数据。
			//
			// 更要紧的是**一致**：走 model 节点的 stale 由 OnError 抢先写完整 message，
			// 这个 fallback 只在编排层直接返 stale（没进 model）时才生效。两边给不同
			// 形状，业务按 error.message 建的告警规则就会漏掉一半。
			forceErrType, forceErrMsg = "checkpoint_stale", retErr.Error()
			staleOp = "unknown"
			var sw *compose_v2.StaleWriteError
			if errors.As(retErr, &sw) {
				// Op 是低基数维度（save / purge-success / purge-abort），可安全当 label。
				// SessionID / Baseline / Current 是高基数，**绝不能进 label**——进 message
				// 无妨（不建索引），进 label 就是 cardinality 爆炸。
				staleOp = sw.Op
			}
			// framework auto-fire：业务拿不到这个信号（埋在框架 Run 出口的错误链里，
			// 且判定要穿透双 %w），不打这个 metric 运维就只能翻日志
			if c := h.o.Counter(MetricStaleWriteTotal, map[string]string{
				LabelStaleOp: staleOp,
			}); c != nil {
				c.Inc()
			}
		} else if ie, isInt := compose_v2.IsInterrupt(retErr); isInt && !errors.Is(retErr, compose_v2.ErrCheckpointNotPersisted) {
			// 档没写成（ErrCheckpointNotPersisted）跟中断绑在一条链上时（引擎出口不产出这种形状，防自研
			// agent 这样交过来）不算挂起：那个中断续不上，记成 blocked 会计进 interrupt 指标、导出绿 span。
			status = "blocked"
			interruptInfo = ie
			// fire MetricInterruptTotal{agent_name, agent_kind}
			if c := h.o.Counter(MetricInterruptTotal, map[string]string{
				LabelAgentName: info.Name,
				LabelAgentKind: info.Type,
			}); c != nil {
				c.Inc()
			}
			// 记 interrupt nanos 让下次 Resume 入口 OnAgentStart 算 interrupt→resume duration.
			// Store 语义: 多次 InterruptError 嵌套时最新覆盖历史 (HITL Run→Resume→Run→
			// Resume 循环按最新中断时间算, 历史中断不累计). 单 SID 设计上串行不会并发.
			sess.interruptStartNanos.Store(time.Now().UnixNano())
		} else {
			// 非 interrupt 真错误: ctx.Err 已设 status 时不覆盖 (timeout/cancelled 优先),
			// 仅默认 "success" 时覆盖为 error. 无论哪种, retErr 经 mapErrToStatus 分类后作
			// endSpan fallback (status 由现有逻辑定, 这里只取 errType/errMsg) — 修 7 编排
			// 非通信主体错误 (MaxIterations 等) error.message 落空.
			if status == "success" {
				status = "error"
			}
			var mapped string
			mapped, fbErrType, fbErrMsg = mapErrToStatus(ctx, retErr)
			// **取消档要接住**：上面那句无条件把 status 打成 error 了，而
			// mapErrToStatus 认得"这不是失败是喊停"。目前唯一走到这里的是软取消
			// （ctx 取消在更外层就已设好 status，不经这条路），它**不动 ctx** 所以
			// 那些基于 ctx.Err() 的判断一律看不到它。
			//
			// 不接的后果：用户每点一次"停止"，监控平台上多一条 status=error——错误率被
			// 高频的正常操作污染，排查的人根本想不到 error 里混着停止按钮。
			if mapped == "cancelled" || mapped == "timeout" {
				status = mapped
			}
		}
	}
	agentLabels := map[string]string{
		LabelAgentName: info.Name,
		LabelAgentKind: info.Type,
		LabelStatus:    status,
	}
	if c := h.o.Counter(MetricAgentRunTotal, agentLabels); c != nil {
		c.Inc()
	}

	// **iter-5-Q Phase 2** metrics_mapping span attribute (spec AI_METRICS_SPEC.md
	// line 36-53): 把本 span 关联的 metric name+labels 写到 span 让监控前端从
	// metric 图反查 trace.
	durationLabels := map[string]string{
		LabelAgentName: info.Name,
		LabelAgentKind: info.Type,
	}
	mapping := map[string]map[string]string{
		MetricAgentRunTotal:    agentLabels,
		MetricAgentRunDuration: durationLabels,
		MetricAgentRounds:      durationLabels,
	}
	// HITL 业务核心: InterruptError 路径补 gen_ai_interrupts mapping 让业务从
	// interrupt counter 图点 → 反查到具体 blocked agent span (排查"哪个 agent 高频
	// 中断 / 中断耗时分布"). spec maxNum=5, 当前 3 base + 1 interrupt = 4/5, 留 1
	// 空位 (cost / TTFT 等业务真痛再加).
	//
	// gen_ai_resumes 不加 — 业务可从 trace 列表按 status=success+resume_action 过滤
	// 直接找到 Resume span, 不需 metric 反查路径.
	if interruptInfo != nil {
		mapping[MetricInterruptTotal] = durationLabels
	}
	// 并发冲突同理: 业务在监控平台看到 gen_ai_stale_writes 尖峰后要能点进具体 span,
	// 否则只知道"有并发"却定位不到是哪个 agent / 哪次会话。这个信号的价值恰恰在
	// 于反查——它意味着同一会话正被两个副本处理、副作用可能已重复发生。
	//
	// label 必须与实际 fire 的那次**完全一致**（只有 crew_stale_op 一个 key），
	// 否则前端按 mapping 反查会落空。stale 与 interrupt 互斥（同一个 if-else 链），
	// 故 mapping 最多 3 base + 1 = 4/5，不超 spec maxNum。
	if staleOp != "" {
		mapping[MetricStaleWriteTotal] = map[string]string{LabelStaleOp: staleOp}
	}
	mappingJSON := buildMetricsMapping(mapping)
	var extraEndAttrs []*commonpb.KeyValue
	if mappingJSON != "" {
		extraEndAttrs = append(extraEndAttrs, strKV(MetricsMappingAttrKey, mappingJSON))
	}

	// P1 fix: 加 `interrupt` span event 标 InterruptError 精确时刻 — 业务在 trace
	// timeline 看到"这里 cp 被存档暂停". 含 reason + question + interrupt_id +
	// queries_count (多 query fanout 场景).
	if interruptInfo != nil {
		queries := interruptInfo.AllQueries()
		intEvAttrs := []*commonpb.KeyValue{
			strKV("reason", interruptInfo.Reason),
			intAttr("queries_count", int64(len(queries))),
		}
		if len(queries) > 0 && queries[0] != nil {
			intEvAttrs = append(intEvAttrs,
				strKV("interrupt_id", queries[0].InterruptID),
				strKV("question", queries[0].Question),
			)
		}
		h.o.addEvent(ctx, "agent", info.Name, 0, "", "interrupt", intEvAttrs)
	}

	// AI_TRACE_SPEC.md §Events: agent span 加 `agent_output` event 让前端看到
	// **本次 agent 调用的真 final 答复** (不是整个 messages history 含 system + 历史).
	//
	// 语义: out.Messages 是 BasicAgent ReAct 循环累积的完整 messages history (system +
	// 历史 + user + assistant rounds + tool results). 业务期望 agent_output 是
	// "agent 这次给我的回答" — 取**末条 assistant message** (不含 tool_calls 中间状态
	// 的 final answer).
	//
	// 边界:
	//   - InterruptError 路径: 可能无 final assistant (中断时 LLM 还没回完). 仅 emit
	//     status + messages_count + interrupt 标记, 不填 gen_ai_content (无意义 dup
	//     在 prompt event 内).
	//   - 业务 opt-in RecordMessageContent 后填 final message JSON (1MB 截断 +
	//     RedactPatterns PII filter).
	outEvAttrs := []*commonpb.KeyValue{
		strKV("status", status),
	}
	if out != nil {
		outEvAttrs = append(outEvAttrs, intAttr("messages_count", int64(len(out.Messages))))
		if h.o.cfg.RecordMessageContent {
			finalMsg := lastAssistantMessage(out.Messages)
			if finalMsg != nil {
				outEvAttrs = append(outEvAttrs, strKV(attrGenAIContent, h.o.renderContent(finalMsg)))
			}
		}
	}
	h.o.addEvent(ctx, "agent", info.Name, 0, "", "agent_output", outEvAttrs)

	// review P2 audit² HIGH-A2: status 由 endSpan 内部读 lastError 覆盖
	h.o.endSpan(ctx, "agent", info.Name, 0, "", status,
		forceErrType, forceErrMsg, fbErrType, fbErrMsg, extraEndAttrs)
	return ctx
}

func (h *agentHooks) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	sess := h.o.session(ctx)
	graphNS := compose_v2.GetGraphNS(ctx)
	parentSpanID := sess.findParentSpanID(graphNS, "agent", info.Name, 0)

	attrs := []*commonpb.KeyValue{}
	if in != nil && in.Config != nil {
		if in.Config.ModelName != "" {
			attrs = append(attrs, strKV("gen_ai.model", in.Config.ModelName))
		}
		if in.Config.Temperature > 0 {
			attrs = append(attrs, floatAttr("gen_ai.request.temperature", float64(in.Config.Temperature)))
		}
		if in.Config.MaxTokens > 0 {
			attrs = append(attrs, intAttr("gen_ai.request.max_tokens", int64(in.Config.MaxTokens)))
		}
	}
	// **这次请求由哪些消息构成**——`ModelInput` 把 agent 前言（system prompt / 模板渲染
	// 结果 / skill 正文）拆成了跟 `Messages` 并列的 `Preamble`，两半合起来才是一次请求。
	//
	// ⚠️ **算一次、span attr 与 prompt event 共用**：曾经只把 span attr 那处补上，
	// 结果同一个 span 上 attr 报 2、event 报 1、`gen_ai_content` 里干脆没有 system prompt
	// ——自相矛盾的上报比一致地少报更难排查。
	var promptMsgs []*schema.Message
	if in != nil {
		promptMsgs = make([]*schema.Message, 0, len(in.Preamble)+len(in.Messages))
		promptMsgs = append(promptMsgs, in.Preamble...)
		promptMsgs = append(promptMsgs, in.Messages...)
		attrs = append(attrs, intAttr("gen_ai.request.messages_count", int64(len(promptMsgs))))
	}
	// audit² C-HIGH-2 fix: chat span node.id 嵌入 agent 下 (graphNS/agent/{name}/model/r{round})
	agentNodeID := graphNS + "/agent/" + info.Name
	h.o.startSpan(ctx, "model", "chat",
		"gen_ai.chat."+info.Name+"_model_call", info.Name, info.Round, "",
		parentSpanID, agentNodeID, attrs)
	// Phase 2 Sub-5c: model span startNanos 暂存让 OnModelEnd 算 gen_ai.llm.call.duration.
	// 跟 OnAgentStart / runStartNanos 同款 pattern — 重新 Load 防 nil (startSpan 返 nil
	// 当 sampled=false / exporter nil).
	// 注: model name label 不在此写 (in.Config framework 不设一直 nil) — 改由 OnModelEnd
	// 从 out.Message.Metadata.Model (response model) 读, 见 OnModelEnd 注释.
	if v, ok := sess.spans.Load(registryKey("model", graphNS, info.Name, info.Round, "", "")); ok {
		entry := v.(*spanEntry)
		entry.mu.Lock()
		entry.runStartNanos = time.Now().UnixNano()
		entry.mu.Unlock()
	}

	// prompt event — 默认不塞 content (PII), 仅元数据
	if in != nil {
		eventAttrs := []*commonpb.KeyValue{
			intAttr("messages_count", int64(len(promptMsgs))),
		}
		// review P0-P2 整体 audit² 视角 B HIGH fix: opt-in 后真采 messages 内容
		//
		// 采的是 promptMsgs（前言 + 对话）不是 in.Messages：`gen_ai_content` 是**承载
		// prompt 正文的唯一 key**（有独立 1MB 预算），只采后半等于 agent 的 system prompt /
		// few-shot / skill 正文一个字都不上报。
		if h.o.cfg.RecordMessageContent {
			eventAttrs = append(eventAttrs, strKV(attrGenAIContent, h.o.renderContent(promptMsgs)))
		}
		h.o.addEvent(ctx, "model", info.Name, info.Round, "", "prompt", eventAttrs)
	}
	return ctx, nil
}

func (h *agentHooks) OnModelEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	// OnModelEnd 从 out.Message.Metadata 取 AI_TRACE_SPEC §模型调用 字段:
	// response.finish_reasons / request.model + response.model (gap 1) / system (gap 2).
	// gen_ai.system (provider) 经 model client 的 openai.WithSystem 透传到 Metadata.System
	// (Generate + Stream 两路), 不再走 ctx-pushed provider 协议.
	attrs := []*commonpb.KeyValue{}
	if out != nil && out.Usage != nil {
		attrs = append(attrs,
			intAttr("gen_ai.usage.input_tokens", int64(out.Usage.InputTokens)),
			intAttr("gen_ai.usage.output_tokens", int64(out.Usage.OutputTokens)),
			intAttr("gen_ai.usage.total_tokens", int64(out.Usage.TotalTokens)),
		)
		// 属性名对齐 OTel GenAI semconv (2026 Q2): reasoning.output_tokens / cache_read.input_tokens /
		// cache_creation.input_tokens (旧名 reasoning_tokens / cached_input_tokens). AI_TRACE_SPEC 未定义
		// 这几个 (crew-go 扩展), 加性不破坏 CS-OSS Monitor.
		if out.Usage.ThinkTokens > 0 {
			attrs = append(attrs, intAttr("gen_ai.usage.reasoning.output_tokens", int64(out.Usage.ThinkTokens)))
		}
		// cache_read.input_tokens — prompt cache 命中数, 业务降本核心 (provider 折价计费).
		// openai / qwen / deepseek / anthropic 都报. Cache 未命中为 0, 不 emit 避免噪音.
		if out.Usage.CachedInputTokens > 0 {
			attrs = append(attrs, intAttr("gen_ai.usage.cache_read.input_tokens", int64(out.Usage.CachedInputTokens)))
		}
		// cache_creation.input_tokens — 本次写入 prompt cache 的 token (按写入价计费, 比普通输入贵).
		// 目前只有 model/anthropic 报; 只报 cache 读的 provider 为 0, 不 emit.
		if out.Usage.CacheCreationInputTokens > 0 {
			attrs = append(attrs, intAttr("gen_ai.usage.cache_creation.input_tokens", int64(out.Usage.CacheCreationInputTokens)))
		}
	}
	if out != nil && out.Message != nil && out.Message.Metadata != nil {
		if out.Message.Metadata.FinishReason != "" {
			attrs = append(attrs, strKV("gen_ai.response.finish_reasons", out.Message.Metadata.FinishReason))
		}
		if out.Message.Metadata.Model != "" {
			// gen_ai.request.model: 保留 — metric label LabelRequestModel 同源 + 请求侧占位
			// (framework modelFn in.Config 多为 nil 拿不到请求 model, 用 response model 代理).
			attrs = append(attrs, strKV("gen_ai.model", out.Message.Metadata.Model))
			// gen_ai.response.model: AI_TRACE_SPEC §模型调用 — 实际响应模型名 (gap 1 补).
			attrs = append(attrs, strKV("gen_ai.response.model", out.Message.Metadata.Model))
		}
		// gen_ai.system: AI_TRACE_SPEC §模型调用 — provider (gap 2 补, model client
		// 经 WithSystem 透传到 Metadata.System). 空则不 emit.
		if out.Message.Metadata.System != "" {
			attrs = append(attrs, strKV("gen_ai.system", out.Message.Metadata.System))
		}
	}
	// response event — 默认不塞 content
	eventAttrs := []*commonpb.KeyValue{}
	if out != nil && out.Message != nil {
		eventAttrs = append(eventAttrs, intAttr("output_chars", int64(len(out.Message.Content))))
		if h.o.cfg.RecordMessageContent {
			eventAttrs = append(eventAttrs, strKV(attrGenAIContent, h.o.renderContent(out.Message)))
		}
	}
	h.o.addEvent(ctx, "model", info.Name, info.Round, "", "response", eventAttrs)

	// Phase 2 Sub-5c: 算 gen_ai.llm.call.duration histogram + gen_ai.llm.tokens.total counter.
	// 在 endSpan 之前算因为 endSpan 会清 sess.spans entry. duration 计算跟 OnAgentEnd
	// 同款 pattern (Load entry 拿 runStartNanos, 算 now-start).
	sess := h.o.session(ctx)
	graphNS := compose_v2.GetGraphNS(ctx)
	// gen_ai.request.model 来源 (跨 provider 集成验证 fix): 真 LLM (openai/deepseek/qwen)
	// 把 response model name 填在 out.Message.Metadata.Model (model client 知道自己的
	// model). 从此读 — 不依赖 OnModelStart 的 ModelInput.Config (framework modelFn 闭包
	// 不暴露 model name, in.Config 一直 nil 导致 label 空, 之前 spanEntry.modelName 链路废).
	var modelName string
	if out != nil && out.Message != nil && out.Message.Metadata != nil {
		modelName = out.Message.Metadata.Model
	}
	if v, ok := sess.spans.Load(registryKey("model", graphNS, info.Name, info.Round, "", "")); ok {
		entry := v.(*spanEntry)
		entry.mu.Lock()
		startNanos := entry.runStartNanos
		entry.mu.Unlock()
		if startNanos > 0 {
			durationSec := float64(time.Now().UnixNano()-startNanos) / 1e9
			// Round 5 Q1+Q2 fix: label 对齐 OTel GenAI semconv — gen_ai.agent.name +
			// gen_ai.request.model 让业务跨 trace+metric join.
			if hg := h.o.Histogram(MetricLLMCallDuration, BucketsLLMDuration,
				map[string]string{
					LabelAgentName:    info.Name,
					LabelRequestModel: modelName,
				}); hg != nil {
				hg.Observe(durationSec)
			}
		}
	}
	// gen_ai.llm.tokens.total counter — 按 token type 分 series (input/output/think/total/cached/
	// cache_creation, 值 <= 0 的不 emit).
	// Round 5 Q1+Q2 fix: label 对齐 OTel — gen_ai.agent.name + gen_ai.request.model +
	// gen_ai.usage.type (OTel 推荐 "token type" 字段名).
	if out != nil && out.Usage != nil {
		emitToken := func(tokenType string, n int32) {
			if n <= 0 {
				return
			}
			if c := h.o.Counter(MetricLLMTokensTotal, map[string]string{
				LabelAgentName:    info.Name,
				LabelRequestModel: modelName,
				LabelUsageType:    tokenType,
			}); c != nil {
				c.Add(int64(n))
			}
		}
		emitToken("input", out.Usage.InputTokens)
		emitToken("output", out.Usage.OutputTokens)
		emitToken("think", out.Usage.ThinkTokens)
		emitToken("total", out.Usage.TotalTokens)
		// HIGH-2.2 fix: cached 单独 1 series 让业务算 cache 命中率 =
		// cached / input (PromQL 直接 join 算). 业务 dashboard 看降本进度核心.
		emitToken("cached", out.Usage.CachedInputTokens)
		// cache_creation: 写入 cache 的 token (Anthropic 按写入价计费), 跟 cached 一起才算得出缓存的净收益.
		emitToken("cache_creation", out.Usage.CacheCreationInputTokens)
	}

	// **iter-5-Q Phase 2** metrics_mapping span attribute — 关联本 model span 到
	// gen_ai_model_calls / gen_ai_tokens metric, 让监控前端从 token 图反查 trace.
	modelLabels := map[string]string{
		LabelAgentName:    info.Name,
		LabelRequestModel: modelName,
	}
	mapping := map[string]map[string]string{
		MetricLLMCallDuration: modelLabels,
	}
	if out != nil && out.Usage != nil && out.Usage.TotalTokens > 0 {
		tokenLabels := map[string]string{
			LabelAgentName:    info.Name,
			LabelRequestModel: modelName,
			LabelUsageType:    "total",
		}
		mapping[MetricLLMTokensTotal] = tokenLabels
	}
	if mappingJSON := buildMetricsMapping(mapping); mappingJSON != "" {
		attrs = append(attrs, strKV(MetricsMappingAttrKey, mappingJSON))
	}

	h.o.endSpan(ctx, "model", info.Name, info.Round, "", "success", "", "", "", "", attrs)
	return ctx, nil
}

func (h *agentHooks) OnModelEndStream(ctx context.Context, info *hook.AgentInfo, sr specModel.MessageStreamReader) (context.Context, specModel.MessageStreamReader, error) {
	// stream 路径: 简化实现 — 不 wrap reader, 仅在 stream 入口时把 span 标 streaming
	// (业务真要拿每 chunk 数据可 future 加 wrap; 现阶段 P2 简化只确保 model span 关)
	// 实际 stream EOF 是业务侧 reader 消费完后, OnModelEnd 不会 fire (stream 路径
	// 走自己 finalize). 但 BasicAgent ISSUE-74 后 model 节点内部聚合后 stream 完成
	// 仍 fire OnModelEnd — 所以这里直接复用 OnModelEnd 逻辑.
	h.o.addEvent(ctx, "model", info.Name, info.Round, "", "chunk", nil)
	// 不结 span — 等 OnModelEnd 结
	return ctx, sr, nil
}

func (h *agentHooks) OnToolStart(ctx context.Context, info *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	if in == nil {
		return ctx, nil
	}
	sess := h.o.session(ctx)
	graphNS := compose_v2.GetGraphNS(ctx)
	parentSpanID := sess.findParentSpanID(graphNS, "agent", info.Name, 0)

	// Phase 1 fix: 路由 spanKind 决策 — 优先级:
	//   1. activate_skill 工具 (framework 内置 skill 系统) → kind=skill, displayName=args.skill
	//   2. business tool 实现 Categorizer (透 Extra["category"]) → kind=retrieval/embedding/
	//      guardrail (仅这 3 个合法值, 其他 fallback tool)
	//   3. 默认 → kind=tool
	//
	// activate_skill 优先级最高: 即使业务 typo 给它加 Extra["category"]="retrieval"
	// 也走 skill (framework 行为不被 business hint 覆盖, 保 skill metric 准确性).
	spanKind, operationName, displayName := h.routeToolSpanKind(in)

	attrs := []*commonpb.KeyValue{
		strKV("gen_ai.tool.call_id", in.CallId),
		strKV("gen_ai.tool.type", "function"),
	}
	// crew.tool.category attr 让监控 UI 过滤 / metric 聚合用 — 即使监控后端
	// 不识别 kind=retrieval 也能按 attr 区分.
	if spanKind != "tool" {
		attrs = append(attrs, strKV("crew.tool.category", spanKind))
	}

	// audit² C-HIGH-2 fix: tool span node.id 嵌入 agent 下 (graphNS/agent/{agent_name}/tool/{tool_name}/{callID})
	agentNodeID := graphNS + "/agent/" + info.Name
	entry := h.o.startSpan(ctx, spanKind, operationName,
		"gen_ai."+operationName+"."+displayName, displayName, 0, in.CallId,
		parentSpanID, agentNodeID, attrs)
	// tool/skill/retrieval/... span startNanos 让 OnToolEnd 算 duration.
	// 方案 C: 直接用 startSpan 返回的 entry 设 (entry 已含 kind/name), 不再 Load 反查.
	if entry != nil {
		entry.mu.Lock()
		entry.runStartNanos = time.Now().UnixNano()
		entry.mu.Unlock()
	}
	// Phase 2 Sub-5f: gen_ai.skill.activate.total Counter — 仅 spanKind=="skill" 时 fire
	// (跟 span kind 决策一致, 避免 metric/trace 数据偏差).
	//
	// **audit fix eaa3fdf HIGH-2**: 之前 `if in.Name == ActivateSkillToolName` 无条件
	// fire counter, 但 routeToolSpanKind 内 args 解析失败 (JSON malformed / skill 字段
	// 缺失) 时 fallback "tool" kind → counter 多记一笔. 业务监控 dashboard 按
	// `kind=skill` 过滤 span 验证 counter 时找不到对应 span 出现 "counter 增量但无
	// trace" 假象. 修法: 跟 spanKind 决策绑定, spanKind=="skill" 才 fire.
	if spanKind == "skill" {
		if c := h.o.Counter(MetricSkillActivateTotal, map[string]string{
			LabelAgentName: info.Name,
		}); c != nil {
			c.Inc()
		}
	}

	// tool_input event — 默认仅 args size
	eventAttrs := []*commonpb.KeyValue{
		intAttr("args_size_bytes", int64(len(in.ArgumentsJSON))),
	}
	if h.o.cfg.RecordMessageContent {
		eventAttrs = append(eventAttrs, strKV(attrGenAIContent, h.o.renderContent(in.ArgumentsJSON)))
	}
	h.o.addEvent(ctx, spanKind, displayName, 0, in.CallId, "tool_input", eventAttrs)
	return ctx, nil
}

// routeToolSpanKind 路由 tool ToolInput → (spanKind, operationName, displayName).
// 优先级见 OnToolStart godoc.
//
// 返参语义:
//   - spanKind: startSpan 内 switch 派生 nodeID 用 (tool/skill/retrieval/...)
//   - operationName: gen_ai.operation.name attr 值 (跟 spanKind 同步)
//   - displayName: gen_ai.node.name attr 值 + spanKey 内 name (skill 用 args.skill 字段,
//     其他 kind 用 in.Name)
func (h *agentHooks) routeToolSpanKind(in *schema.ToolInput) (kind, operation, display string) {
	// 路径 1: activate_skill 优先级最高
	// **operation.name="skill" 是监控前端故意区分**（skill 本质是 activate_skill 工具
	// 调用，监控前端为在执行树标记 skill 激活做了独立 skill 节点类型）——**非** spec 8 枚举
	// (agent/workflow/chat/embedding/tool/retrieval/rerank/guardrail) 的遗漏，审计勿报 drift。
	if in.Name == agent_v2.ActivateSkillToolName {
		if skillName := extractSkillNameFromArgs(in.ArgumentsJSON); skillName != "" {
			return "skill", "skill", skillName
		}
		// args 解析失败时 fallback tool kind (理论不该发生, framework 强制 args 格式)
		return "tool", "tool", in.Name
	}
	// 路径 2: business Categorizer (透 Extra["category"])
	if in.Extra != nil {
		if cat, ok := in.Extra["category"].(string); ok {
			switch cat {
			case "retrieval", "embedding", "guardrail":
				return cat, cat, in.Name
			}
			// 未知 category (业务 typo 等) fallback tool kind 不报错, 保兼容
		}
	}
	// 路径 3: 默认 tool kind
	return "tool", "tool", in.Name
}

// extractSkillNameFromArgs 从 activate_skill ArgumentsJSON 取 skill 字段.
//
// 格式协议 (agent_v2 内置 activate_skill 工具):
//
//	{"skill": "<skill_name>", "query": "<user query>"}
//
// 解析失败 / 字段缺失返空字符串, caller fallback tool kind.
func extractSkillNameFromArgs(argsJSON string) string {
	type skillArgs struct {
		Skill string `json:"skill"`
	}
	var a skillArgs
	if err := json.Unmarshal([]byte(argsJSON), &a); err != nil {
		return ""
	}
	return a.Skill
}

func (h *agentHooks) OnToolEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out == nil {
		return ctx, nil
	}
	status := "success"
	var errMsg string
	if out.Result != nil && !out.Result.Success {
		status = "error"
		errMsg = out.Result.Error
	}
	// tool_output event
	eventAttrs := []*commonpb.KeyValue{}
	if out.Result != nil {
		eventAttrs = append(eventAttrs,
			intAttr("output_size_bytes", int64(len(out.Result.Output))),
			strKV("ok", boolStr(out.Result.Success)),
		)
		if h.o.cfg.RecordMessageContent {
			eventAttrs = append(eventAttrs, strKV(attrGenAIContent, h.o.renderContent(out.Result.Output)))
		}
	}
	// 方案 C: callID 是 tool span 全局唯一锚点 (registryKey 归一到 "tool|graphNS|callID"),
	// 直接 Load 拿 entry, 从 entry 读真实 kind / name — 消除之前遍历 6 kind + 解析 key
	// 反查 name 的脆弱代码 (eaa3fdf HIGH-1 根因: 反查到 foundKind 后 endSpan 却硬编码
	// "tool" 导致 skill/retrieval 等 span 失配不被正常 end, 被 OnGraphEnd sweep 兜底假标 error).
	sess := h.o.session(ctx)
	graphNS := compose_v2.GetGraphNS(ctx)
	v, ok := sess.spans.Load(registryKey("tool", graphNS, "", 0, out.CallId, ""))
	if !ok {
		return ctx, nil // span 没创 (业务直接调测试场景 / 未采样)
	}
	entry := v.(*spanEntry)
	foundKind := entry.kind
	foundName := entry.name

	h.o.addEvent(ctx, foundKind, foundName, 0, out.CallId, "tool_output", eventAttrs)
	// Phase 2 Sub-5c: 算 gen_ai.tool.call.duration histogram + gen_ai.tool.error.total counter.
	// 在 endSpan 之前算 (endSpan 会 LoadAndDelete 清 entry). entry 已 Load 直接用.
	entry.mu.Lock()
	startNanos := entry.runStartNanos
	entry.mu.Unlock()
	if startNanos > 0 {
		durationSec := float64(time.Now().UnixNano()-startNanos) / 1e9
		// Round 5 Q1 fix: label 对齐 OTel — gen_ai.agent.name / gen_ai.tool.name.
		if hg := h.o.Histogram(MetricToolCallDuration, BucketsSmallDuration,
			map[string]string{
				LabelAgentName: info.Name,
				LabelToolName:  foundName,
			}); hg != nil {
			hg.Observe(durationSec)
		}
	}
	toolLabels := map[string]string{
		LabelAgentName: info.Name,
		LabelToolName:  foundName,
	}
	if status == "error" {
		if c := h.o.Counter(MetricToolErrorTotal, toolLabels); c != nil {
			c.Inc()
		}
	}

	// **iter-5-Q Phase 2** metrics_mapping span attribute — 关联 tool span 到
	// gen_ai_tool_calls / gen_ai_tool_errors metric.
	mapping := map[string]map[string]string{
		MetricToolCallDuration: toolLabels,
	}
	if status == "error" {
		mapping[MetricToolErrorTotal] = toolLabels
	}
	var extraToolAttrs []*commonpb.KeyValue
	if mappingJSON := buildMetricsMapping(mapping); mappingJSON != "" {
		extraToolAttrs = append(extraToolAttrs, strKV(MetricsMappingAttrKey, mappingJSON))
	}

	// 方案 C: 传真实 foundKind (从 entry 读). endSpan 内 registryKey 因 callID 非空归一到
	// "tool|graphNS|callID", 不管 kind 都配对成功 — 根除 eaa3fdf HIGH-1 硬编码 "tool" 失配.
	h.o.endSpan(ctx, foundKind, foundName, 0, out.CallId, status, "", errMsg, "", "", extraToolAttrs)
	return ctx, nil
}

func (h *agentHooks) OnError(ctx context.Context, info *hook.AgentInfo, err *schema.CommError) context.Context {
	// review P2 audit MED-1 + audit² HIGH-A2 + P2.5 audit² HIGH-D2 fix:
	// 直接对 in-flight span 写 error attrs + status (跳过 lastError 协议),
	// 避免之前 OnError 写 callID="" 跟 OnToolEnd 真 callID 不匹配 lastError leak.
	//
	// 协议: OnError fire 时**所有 in-flight span 都有可能是出错主体** (agent/model
	// 内 tool 不知道哪个). 直接遍历 sessions.spans 找当前 graphNS 下 in-flight 的
	// agent/model/tool span (按 info.Name 过滤) 写 lastError 标记 — 随后 *End fire
	// 时按 spanKey 真匹配 LoadAndDelete (含真实 callID), 漏取的 OnGraphEnd
	// sweepInFlight 兜底清.
	if err == nil {
		return ctx
	}
	slog.WarnContext(ctx, "observability: OnError fire",
		"agent", info.Name, "type", info.Type, "round", info.Round,
		"code", err.Code, "message", err.Message)

	if h.o == nil {
		return ctx
	}
	sess := h.o.session(ctx)
	graphNS := compose_v2.GetGraphNS(ctx)
	// 遍历 in-flight span 找当前 agent 下的 spans, 用真实 spanKey 写 lastError.
	// model/agent 跟 info.Name 直接绑, tool 用 in-flight 真 callID 绑.
	sess.spans.Range(func(k, _ any) bool {
		key := k.(string)
		// 简化: 只写 agent + model + tool 三类, 同 graphNS + 同 info.Name
		// (tool 含 callID 后缀也会按 prefix 匹配)
		// 注: spanKey 协议 round=0 不加 "|", callID="" 不加 "|" — prefix 末尾不带
		// `|` (e.g., agent span key="agent|graphNS|name" 3 段没 trailing pipe).
		// audit² B-HIGH-B1 fix: err.Message 走 RedactPatterns redact 防 PII
		// (LLM 报错可能含 API key / 手机号 / 用户 ID)
		redactedMsg := h.o.redactString(err.Message)
		// audit² D-HIGH-1 fix: 之前 prefix `agent|ns|chat` 会误命中
		// `agent|ns|chat_v2` (name 子串包含). spanKey 协议: round=0 callID="" 不加
		// trailing pipe — 必须 exact match round=0 路径 + prefix+"|" round>0 路径.
		// tool 暂保 graphNS sweep (跨 agent 共享 toolkit 设计, 留待 D-HIGH-2 单独修).
		agentKey := "agent|" + graphNS + "|" + info.Name
		modelExact := "model|" + graphNS + "|" + info.Name        // round=0 key
		modelPrefix := "model|" + graphNS + "|" + info.Name + "|" // round>0 key
		// 方案 C: tool 类 span key 归一为 "tool|graphNS|callID" — 单 "tool|" 前缀即覆盖
		// tool/skill/retrieval/embedding/guardrail 全部 (callID 唯一锚点不掺 kind).
		matched := key == agentKey || key == modelExact ||
			startsWith(key, modelPrefix) || startsWith(key, "tool|"+graphNS+"|")
		if matched {
			sess.lastErrors.Store(key, &commonError{code: err.Code, message: redactedMsg})
		}
		return true
	})
	return ctx
}

// ─────────────── agentOrchAdapter: agent_v2.OrchestrationHooks 5 method ───────────────
//
// review P0-P2 整体 audit² 视角 A HIGH-A1 fix: 签名跟 agent_v2.OrchestrationHooks
// 完全对齐 (ctx + string nodeID + Event), 不带 compose_v2.Channels. framework
// 内部用 adaptHooks 自动转 compose_v2 视角.

// compile-time 断言 — 防 signature 跟 agent_v2.OrchestrationHooks 漂移
var _ agent_v2.OrchestrationHooks = (*agentOrchAdapter)(nil)

type agentOrchAdapter struct {
	o *Observability
}

func (a *agentOrchAdapter) OnGraphStart(ctx context.Context) (context.Context, error) {
	if a == nil || a.o == nil {
		return ctx, nil
	}
	// 记「编排这半确实接上了」——给 agentHooks.OnAgentEnd 的「只装了一半」检测用，
	// 见 Observability.orchFired 的说明。
	a.o.orchFired.Store(true)
	sess := a.o.session(ctx)
	graphNS := compose_v2.GetGraphNS(ctx)

	// 设计: agent 作 trace root, graph 是 framework 实现细节不暴露.
	//
	// 跳过创 graph span 条件: **当前 graphNS 下已有 in-flight agent span**
	// (agent_v2.Run 路径, OnAgentStart 早于 OnGraphStart fire) → agent 兼任 root,
	// graph 跳过避免重复 + 修 child > parent bug (之前 agent ⊃ graph 时间跨度,
	// graph 作 root 时监控界面显示"相对耗时 >100%" + 同 spanID 冲突 dedup 丢一条).
	//
	// 创 graph span 条件: 无 in-flight agent (业务直接调 compose_v2.Run 不走
	// agent_v2 wrapper). 此场景 graph 是 trace 的唯一表达, 不论顶层/嵌套都创 span:
	//   - 顶层 (graphNS=""): graph 作 trace root, sid=rootSpanID
	//   - 嵌套 (graphNS!=""): 父 graph 在 ParentNS 层 (业务自定义 NodeFunc 内嵌套
	//     compose_v2.Run 子图), graph span 挂父 graph 下作 child.
	if _, agentNodeID := sess.findActiveAgentSpan(graphNS); agentNodeID != "" {
		return ctx, nil
	}

	// 无 agent — graph 作 trace 表达, 创 span.
	graphID := graphNS
	if graphID == "" {
		graphID = "root"
	}
	parentSpanID := ""
	parentNS := compose_v2.ParentNS(graphNS)
	if parentNS != graphNS { // ParentNS 顶层返自身, 真有父 layer 时 != self
		parentSpanID = sess.findParentSpanID(parentNS, "graph", parentNS, 0)
	}
	a.o.startSpan(ctx, "graph", "workflow",
		"gen_ai.workflow."+graphID, graphNS, 0, "",
		parentSpanID, "", nil)
	return ctx, nil
}

func (a *agentOrchAdapter) OnGraphEnd(ctx context.Context, err error) context.Context {
	if a == nil || a.o == nil {
		return ctx
	}
	graphNS := compose_v2.GetGraphNS(ctx)
	// audit² C-HIGH-1 fix: 不 sweep "agent|" (OnAgentEnd 自己 finalize, 详见 sweep 注释)
	// audit² D-HIGH-6 fix: 先算 outer status (graph err), 传给 sweep — InterruptError
	// 走 "blocked" 不被 sweep 硬标 "error".
	status := "success"
	var errType, errMsg string
	if err != nil {
		status, errType, errMsg = mapErrToStatus(ctx, err)
	}
	// sweep outer 默认 "error" (in-flight 没 *End fire 不可能是 success). 仅 graph err
	// 为 InterruptError/cancelled/timeout 时 outer 用 mapErrToStatus 算出来的非 success
	// 值 (blocked/cancelled/timeout) 同步给 in-flight 视为同款状态.
	sweepStatus := status
	if sweepStatus == "success" {
		sweepStatus = "error"
	}
	// 方案 C: tool 类 span 归一为 "tool|" 前缀 (callID 唯一锚点不掺具体 kind), 只需
	// node/model/tool 三前缀即覆盖全部 in-flight span.
	for _, kindPrefix := range []string{"node|", "model|", "tool|"} {
		a.o.sweepInFlight(ctx, kindPrefix+graphNS+"|", sweepStatus, errType, errMsg)
	}
	a.o.endSpan(ctx, "graph", graphNS, 0, "", status, errType, errMsg, "", "", nil)
	return ctx
}

func (a *agentOrchAdapter) OnNodeStart(ctx context.Context, nodeID string) (context.Context, error) {
	if a == nil || a.o == nil {
		return ctx, nil
	}
	sess := a.o.session(ctx)
	graphNS := compose_v2.GetGraphNS(ctx)

	// P2 fix: workflow node 优先挂当前 graphNS 下 in-flight agent span 作 parent.
	// 之前 node 直接挂 graph 跟 agent span 平级, trace 树 preamble/model/tools 跟
	// agent 同级 — 业务困惑 (model 既出现 workflow node "model" 又出现 chat span
	// "agent/chat_entry/model/r0", 看着像 2 次调用). 改后挂 agent 下层级清晰:
	//   graph
	//   └── agent (chat_entry)
	//       ├── workflow node (preamble/model/tools/postamble)
	//       └── chat span (model_call)
	//
	// fallback: graphNS 下没 in-flight agent (e.g., 业务直接调 compose_v2.Run 不走
	// agent_v2 wrapper) 时挂 graph 兜底.
	parentSpanID, parentNodeID := sess.findActiveAgentSpan(graphNS)
	if parentSpanID == "" {
		parentSpanID = sess.findParentSpanID(graphNS, "graph", graphNS, 0)
		parentNodeID = graphNS
	}

	// Phase 2: node 声明组件类型时 (GraphAgent 显式编排路径), operation.name 标组件 kind
	// (retrieval/embedding/guardrail) 而非通用 workflow — 让 trace 区分显式编排
	// 的组件调用. **registry 配对锚点仍用 "node" + nodeID** (复用方案 C "配对锚点 vs 展示
	// kind 解耦"): node span 无 callID, nodeID 在 graph 内唯一作配对锚点; sweep "node|" 前缀
	// 已覆盖不泄漏; 不碰 registryKey/isToolKind, 跟 tool span callID 路径互不干扰.
	operationName := "workflow"
	spanName := "gen_ai.workflow.node." + nodeID
	// isToolKind 校验: 仅合法 component (retrieval/embedding/guardrail/tool/skill)
	// 才覆盖 operation.name + spanName, 业务 typo (如 "retrievalr") 自动 fallback workflow —
	// 跟 tool.Categorizer 宽松语义一致, observability 是权威合法值定义 (compose_v2 只透传 string).
	// spanName 跟 operation.name 同步反映 component (对齐 tool span "gen_ai.{op}.{name}" 格式),
	// 否则 trace 树 span 名显 "workflow.node" 而 operation 显 retrieval — 展示矛盾.
	if comp := compose_v2.GetComponentKind(ctx); isToolKind(comp) {
		operationName = comp
		spanName = "gen_ai." + comp + "." + nodeID
	}
	a.o.startSpan(ctx, "node", operationName,
		spanName, nodeID, 0, "",
		parentSpanID, parentNodeID, nil)
	// Phase 2 Sub-5c: gen_ai.node.execution.total counter (label: node = nodeID).
	// 业务用此 metric 看哪个 node 高频执行 / hot path.
	// Round 5 Q1 fix: crew 自定义 label (无 OTel 对应) 加 prefix `crew.` 区分.
	if c := a.o.Counter(MetricNodeExecutionTotal, map[string]string{LabelNodeID: nodeID}); c != nil {
		c.Inc()
	}
	return ctx, nil
}

func (a *agentOrchAdapter) OnNodeEnd(ctx context.Context, nodeID string, err error) context.Context {
	if a == nil || a.o == nil {
		return ctx
	}
	status := "success"
	var errType, errMsg string
	if err != nil {
		status, errType, errMsg = mapErrToStatus(ctx, err)
	}

	// P3+P4 fix: flush chunk buffer 成 streaming_response event (B 方案: 拼接 content
	// 让业务 partial response 一键可见). InterruptError 路径同样 flush — partial
	// assistant content (LLM 中断前已 stream 内容) 自动在 trace 可见, 不需业务自拼.
	sess := a.o.session(ctx)
	graphNS := compose_v2.GetGraphNS(ctx)
	bufKey := chunkBufferKey(graphNS, nodeID, compose_v2.GetCurrentInstanceID(ctx))
	if bufV, loaded := sess.chunkBuffers.LoadAndDelete(bufKey); loaded {
		buf := bufV.(*chunkBuffer)
		content, count, firstNanos, lastNanos := buf.snapshot()
		if count > 0 {
			streamAttrs := []*commonpb.KeyValue{
				intAttr("chunk_count", int64(count)),
			}
			if lastNanos > firstNanos {
				streamAttrs = append(streamAttrs, floatAttr("streaming_seconds", float64(lastNanos-firstNanos)/1e9))
			}
			// HIGH-2.1 fix: TTFT (time-to-first-token) = first chunk 时间 - 当前 node
			// span start 时间. node 是 framework 的 graph node (e.g. "model"), 内含 LLM
			// 调用 — node start 约等于 LLM 调用开始时刻. 业务 SLO 必备指标 (流式响应
			// 体感延迟).
			//
			// 从 sess.spans Load 当前 node entry 拿 StartTimeUnixNano. node span 已存在
			// (OnNodeStart 创, OnNodeEnd 还没调 endSpan 删) — Load 成功.
			if nodeV, ok := sess.spans.Load(registryKey("node", graphNS, nodeID, 0, "", compose_v2.GetCurrentInstanceID(ctx))); ok {
				nodeEntry := nodeV.(*spanEntry)
				nodeEntry.mu.Lock()
				nodeStart := nodeEntry.span.StartTimeUnixNano
				nodeEntry.mu.Unlock()
				if nodeStart > 0 && uint64(firstNanos) > nodeStart {
					ttftSec := float64(uint64(firstNanos)-nodeStart) / 1e9
					streamAttrs = append(streamAttrs, floatAttr("gen_ai.response.first_token_time", ttftSec))
				}
			}
			// content 走 spec 1MB gen_ai_content 路径 (在 addEvent 内 applyAttrLimit 时
			// gen_ai_content 不走 MaxAttributeValueChars 默认 1MB 上限).
			if a.o.cfg.RecordMessageContent {
				streamAttrs = append(streamAttrs, strKV(attrGenAIContent, a.o.renderContent(content)))
			}
			a.o.addEvent(ctx, "node", nodeID, 0, "", "streaming_response", streamAttrs)
		}
	}

	a.o.endSpan(ctx, "node", nodeID, 0, "", status, errType, errMsg, "", "", nil)
	return ctx
}

func (a *agentOrchAdapter) OnNodeEvent(ctx context.Context, nodeID string, ev agent_v2.Event) context.Context {
	if a == nil || a.o == nil {
		return ctx
	}

	// P3 fix: chunk events 不每条 addEvent (避免 trace UI 128 个 chunk 淹没), 改聚到
	// sess.chunkBuffers, OnNodeEnd flush 成单个 streaming_response event 含拼接 content.
	// 业务 partial response 在 trace 上一键可见, InterruptError 路径也自动有 partial
	// (因 OnNodeEnd 在 error 路径也 fire). 业务实时 streaming 走 SSE 不依赖 trace.
	if ev.Type == "chunk" {
		sess := a.o.session(ctx)
		graphNS := compose_v2.GetGraphNS(ctx)
		bufKey := chunkBufferKey(graphNS, nodeID, compose_v2.GetCurrentInstanceID(ctx))
		bufV, _ := sess.chunkBuffers.LoadOrStore(bufKey, &chunkBuffer{})
		buf := bufV.(*chunkBuffer)
		buf.appendChunk(ev.Content, time.Now().UnixNano())
		// 不 addEvent + 不路由 metric (chunk 无 metric 映射)
		return ctx
	}

	// 加 event 到当前 node span (如果 in-flight)
	attrs := []*commonpb.KeyValue{}
	if ev.Content != "" {
		attrs = append(attrs, strKV("content", truncString(ev.Content, a.o.cfg.MaxAttributeValueChars)))
	}
	// compaction event: 结构化展开 *CompactionAction 成独立 span attr——否则默认
	// toString(action) 吃成空字符串, span 上看不到"压缩了什么"(dropped/kept/token/耗时).
	compactionExpanded := false
	if action, ok := ev.Metadata["action"].(*agent_v2.CompactionAction); ok {
		attrs = append(attrs, compactionSpanAttrs(action)...)
		compactionExpanded = true
	}
	for k, v := range ev.Metadata {
		if k == "action" && compactionExpanded {
			continue // 已按 phase 结构化展开, 不再 toString 塞空 "action"
		}
		attrs = append(attrs, strKV(k, truncString(toString(v), a.o.cfg.MaxAttributeValueChars)))
	}
	a.o.addEvent(ctx, "node", nodeID, 0, "", ev.Type, attrs)
	// Phase 2 Sub-5d + Sub-5e: 按 event.Type 路由 metric.
	// 压缩各层 sink-gated 恒发 event 后此路径自动 fire 对应 metric (零侵入).
	a.routeEventToMetric(ctx, ev)
	return ctx
}

// routeEventToMetric 按 ev.Type 路由到对应 metric fire 函数.
// 压缩各层接了 observability 后 sink-gated 恒发 event, 此路径自动跑.
func (a *agentOrchAdapter) routeEventToMetric(ctx context.Context, ev agent_v2.Event) {
	switch ev.Type {
	case agent_v2.CompactionEventType:
		a.fireCompactionMetric(ctx, ev)
	case compose_v2.CpEventType:
		a.fireCpMetric(ctx, ev)
	case agent_v2.FanoutEventType:
		a.fireFanoutMetric(ctx, ev)
	case agent_v2.PlanReplanEventType:
		a.firePlanReplanMetric(ctx, ev)
	case agent_v2.RouteDecisionEventType:
		a.fireRouteDecisionMetric(ctx, ev)
	case fallback.FailoverEventType:
		a.fireFailoverMetric(ctx, ev)
	case fallback.MemberTrippedEventType:
		a.fireMemberTrippedMetric(ctx, ev)
	}
}

// firePlanReplanMetric P1 fix: 反序列化 PlanReplanAction → fire MetricPlanReplansTotal.
//
// fire 路径:
//   - agent_v2/plan_agent.go::planNode 每次调 planner 时 fire (含 initial + N 次 replan)
//   - observability.OnNodeEvent 识别 → firePlanReplanMetric → Counter.Inc
//
// label 协议: crew_plan_phase (initial vs replan) — 业务用 phase 过滤算
// "真正 replan 次数 = total - initial 次数 = total - run_count".
func (a *agentOrchAdapter) firePlanReplanMetric(ctx context.Context, ev agent_v2.Event) {
	action, ok := ev.Metadata["action"].(*agent_v2.PlanReplanAction)
	if !ok || action == nil {
		return
	}
	if c := a.o.Counter(MetricPlanReplansTotal, map[string]string{
		LabelPlanPhase: string(action.Phase),
	}); c != nil {
		c.Inc()
	}
}

// fireRouteDecisionMetric P1 fix: 反序列化 RouteDecisionAction → fire MetricRouteDecisionsTotal.
//
// fire 路径:
//   - agent_v2 RoutingAgent 的升级链（route_decider.go::decisionRouter）定出目标后 fire，
//     不管最终由决策方、classifier 还是兜底分支决定
//   - observability.OnNodeEvent 识别 → fireRouteDecisionMetric → Counter.Inc
//
// label 协议: crew_route_target (选中的 branch 名 / "__fallback__" / 业务自己发空目标时的 "__none__").
func (a *agentOrchAdapter) fireRouteDecisionMetric(ctx context.Context, ev agent_v2.Event) {
	action, ok := agent_v2.RouteDecisionOf(ev)
	if !ok {
		return
	}
	target := action.Target
	if target == "" {
		target = "__none__" // 框架自己不发空目标，只可能来自业务直接发的事件——区分于空 string label
	}
	if c := a.o.Counter(MetricRouteDecisionsTotal, map[string]string{
		LabelAgentName:   action.AgentName,
		LabelRouteTarget: target,
	}); c != nil {
		c.Inc()
	}
}

// fireFanoutMetric Sub-5f: 反序列化 FanoutAction → fire MetricFanoutBranches.
//
// fire 路径:
//   - basic_agent.go::toolsFn N>=2 tool_call (ISSUE-81 goroutine 并发) → EmitFanoutEvent
//   - observability.OnNodeEvent 识别 → fireFanoutMetric → Histogram.Observe(Branches)
//
// label kind 区分来源 (当前只 tool_calls; 未来 ParallelAgent 等可同款 fire 不同 kind).
func (a *agentOrchAdapter) fireFanoutMetric(ctx context.Context, ev agent_v2.Event) {
	action, ok := ev.Metadata["action"].(*agent_v2.FanoutAction)
	if !ok || action == nil {
		return
	}
	// Round 5 Q1 fix: crew 自定义 label 加 prefix.
	if hg := a.o.Histogram(MetricFanoutBranches, BucketsCount,
		map[string]string{LabelFanoutKind: string(action.Kind)}); hg != nil {
		hg.Observe(float64(action.Branches))
	}
}

// fireCompactionMetric Sub-5d: 反序列化 CompactionAction → fire metric.
//
// **OTel self-contained 设计** (audit fix): After event 自带
// InputTokens / OutputTokens / DurationMs 字段——caller (microcompact / autocompact
// / reactive_compact) 在自身 scope 内有完整数据, 直接填到 After.* 让 observability
// 端 single-event 取完整压缩数据算 metric.
//
// **不再 sess cache**: 之前用 sess.compactStartNanos 缓存 Before event 数据让 After
// 取出关联 — 增 state lifecycle 管理 + cache miss 兜底 + 跨 observability 实例无法
// 复用. 现 caller 自填 + observability 端零 state, 跟 cp event caller-fills-DurationMs
// 设计一致. compaction Action 协议同款 self-contained, framework 内自动 fire metric.
//
// fire 路径:
//   - Before event: 不 fire metric (只是通知 observability "压缩开始了"). Trace 路径
//     由 OnNodeEvent 默认 addEvent 处理.
//   - After event: 一次性 fire 3 metric:
//     · CompactDuration Histogram (秒, 从 After.DurationMs 换算)
//     · CompactTotal Counter (label{layer, ok})
//     · CompactRatio Histogram (After.OutputTokens / After.InputTokens, 仅 InputTokens>0
//     时算 — MicroCompact 不计 token 时 InputTokens=0 跳过 ratio)
//   - Generate / Retry event: 不 fire metric (phase 内细节, 业务订阅 event 直接看;
//     metric 只关心整体压缩动作).
func (a *agentOrchAdapter) fireCompactionMetric(ctx context.Context, ev agent_v2.Event) {
	action, ok := ev.Metadata["action"].(*agent_v2.CompactionAction)
	if !ok || action == nil {
		return
	}
	if action.Type != agent_v2.CompactionActionAfter || action.After == nil {
		return
	}
	layer := string(action.Layer)
	after := action.After
	// Round 5 Q1 fix: crew 自定义 label 加 prefix `crew.compact.`.
	if after.DurationMs > 0 {
		if hg := a.o.Histogram(MetricCompactDuration, BucketsSmallDuration,
			map[string]string{LabelCompactLayer: layer}); hg != nil {
			hg.Observe(float64(after.DurationMs) / 1000.0)
		}
	}
	if after.InputTokens > 0 {
		ratio := float64(after.OutputTokens) / float64(after.InputTokens)
		if hg := a.o.Histogram(MetricCompactRatio,
			[]float64{0.1, 0.3, 0.5, 0.7, 0.9, 0.95, 1.0},
			map[string]string{LabelCompactLayer: layer}); hg != nil {
			hg.Observe(ratio)
		}
	}
	okStr := "true"
	if after.Err != "" {
		okStr = "false"
	}
	if c := a.o.Counter(MetricCompactTotal, map[string]string{
		LabelCompactLayer: layer,
		LabelOutcome:      okStr,
	}); c != nil {
		c.Inc()
	}
}

// fireFailoverMetric 反序列化 fallback.FailoverAction → fire MetricFailoverTotal Counter.
//
// sink-gated auto-fire：fallback 切换时 emit failover event（fallback/event.go），业务接了
// observability 就自动跑到这里 fire metric，没接则 fallback 侧零开销。跟 fireCompactionMetric
// 同款单向消费——observability import fallback，fallback 不知道 observability 存在。
// label{reason（低基数 err 分类）, from（故障候选下标）}——业务按 reason 聚合看哪类故障最多。
func (a *agentOrchAdapter) fireFailoverMetric(ctx context.Context, ev agent_v2.Event) {
	action, ok := ev.Metadata["action"].(*fallback.FailoverAction)
	if !ok || action == nil {
		return
	}
	if c := a.o.Counter(MetricFailoverTotal, map[string]string{
		LabelFailoverReason: action.Reason,
		LabelFailoverFrom:   strconv.Itoa(action.FromIdx),
	}); c != nil {
		c.Inc()
	}
}

// fireMemberTrippedMetric 反序列化 fallback.MemberTrippedAction → fire MetricMemberTrippedTotal
// Counter。sink-gated auto-fire 同 fireFailoverMetric——断路器 closed→open 边缘时 fallback emit
// member_tripped event，业务接 observability 就 fire。label{member（被熔断候选下标）,
// reason（低基数 err 分类）}——业务按 member 聚合看哪个候选最不健康（长期熔断）。
func (a *agentOrchAdapter) fireMemberTrippedMetric(ctx context.Context, ev agent_v2.Event) {
	action, ok := ev.Metadata["action"].(*fallback.MemberTrippedAction)
	if !ok || action == nil {
		return
	}
	if c := a.o.Counter(MetricMemberTrippedTotal, map[string]string{
		LabelTrippedMember: strconv.Itoa(action.MemberIdx),
		LabelTrippedReason: action.Reason,
	}); c != nil {
		c.Inc()
	}
}

// fireCpMetric Sub-5e: 反序列化 CpAction → fire metric.
//
// 设计:
//   - cp event 是单点 fire (不像 compact 有 before/after 配对) — caller (graph_loop
//     SaveNS / LoadNS / DeleteSubtree) 自己测 duration 写入 Action.DurationMs.
//     我们直接读 Action.DurationMs Histogram.Observe.
//   - MetricCpSaveTotal Counter label{op, ok}: 所有 op 共享一个 metric name (业务 grouping by op)
//   - MetricCpSizeBytes Histogram: 仅 save op 算 channels_count * 估算 (我们用 ChannelsCount * 1024
//     作粗略估算 — 真实 size 需序列化, 太重. 业务想精确尺寸可自己测).
func (a *agentOrchAdapter) fireCpMetric(ctx context.Context, ev agent_v2.Event) {
	action, ok := ev.Metadata["action"].(*compose_v2.CpAction)
	if !ok || action == nil {
		return
	}
	op := string(action.Type)
	var okStr string
	var durationMs int64
	var channelsCount int
	switch action.Type {
	case compose_v2.CpOperationSave:
		if action.Save == nil {
			return
		}
		if action.Save.Ok {
			okStr = "true"
		} else {
			okStr = "false"
		}
		durationMs = action.Save.DurationMs
		channelsCount = action.Save.ChannelsCount
	case compose_v2.CpOperationLoad:
		if action.Load == nil {
			return
		}
		if action.Load.Ok {
			okStr = "true"
		} else {
			okStr = "false"
		}
		durationMs = action.Load.DurationMs
	case compose_v2.CpOperationCleanup:
		if action.Cleanup == nil {
			return
		}
		if action.Cleanup.Ok {
			okStr = "true"
		} else {
			okStr = "false"
		}
		durationMs = action.Cleanup.DurationMs
	default:
		return
	}
	// cp.save.total Counter (注: 跟 metric name 命名一致, 含所有 op — label 区分)
	// Round 5 Q1 fix: crew 自定义 label 加 prefix.
	if c := a.o.Counter(MetricCpSaveTotal, map[string]string{
		LabelCpOp:    op,
		LabelOutcome: okStr,
	}); c != nil {
		c.Inc()
	}
	// duration Histogram — 复用 CpSizeBytes 之前一个 metric 名... 实际上 metric.go 25
	// metric 表内 cp 只有 SaveTotal + SizeBytes, 没专门 duration. 把 durationMs 走 trace
	// span (Sub-3 已实现), 不重复 metric. SizeBytes 仅 save 路径估算.
	_ = durationMs
	if action.Type == compose_v2.CpOperationSave && channelsCount > 0 {
		// 粗估 cp size: ChannelsCount 字段数 × 1KB (业务真实尺寸需自己序列化测)
		estBytes := float64(channelsCount) * 1024
		if hg := a.o.Histogram(MetricCpSizeBytes, BucketsSize, nil); hg != nil {
			hg.Observe(estBytes)
		}
	}
}

// ─────────────── helper ───────────────

// mapErrToStatus 把 framework error 映射 5 态 status (跟 impl-guide §1 OnError 表对齐).
func mapErrToStatus(ctx context.Context, err error) (status, errType, errMsg string) {
	errMsg = err.Error()
	if ctx != nil && ctx.Err() != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "timeout", "DeadlineExceeded", errMsg
		}
		return "cancelled", "Canceled", errMsg
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		return "blocked", "InterruptError", errMsg
	}
	// 软取消跟上面 ctx 取消是**同一件事**——业务主动喊停，只是优雅版（当前超步跑完
	// 再停 + 存档）。区别在于它**不动 ctx**，所以上面那个 ctx.Err() 分支永远看不到它。
	//
	// 不单列的后果：用户每点一次"停止"，监控平台上就多一条 status=error——错误率指标被
	// 用户的正常操作污染，而"停止"恰恰是高频操作。
	if errors.Is(err, compose_v2.ErrSoftCanceled) {
		return "cancelled", "SoftCanceled", errMsg
	}
	return "error", "BusinessError", errMsg
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// lastUserMessage 倒序找 *schema.Message slice 末条 user message.
//
// agent_input event 用 — 业务期望显示"本次 agent 调用的真 user 输入"(末条 user
// message), 不是完整 messages history (含 system / 历史 / tool_result 等).
// 没 user 返 nil — caller 退化只 emit messages_count 不填 gen_ai_content.
func lastUserMessage(msgs []*schema.Message) *schema.Message {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i] != nil && msgs[i].Role == schema.RoleUser {
			return msgs[i]
		}
	}
	return nil
}

// lastAssistantMessage 倒序找 *schema.Message slice 末条 assistant message.
//
// agent_output event 用 — 业务期望显示"本次 agent 调用的 final 答复"(末条 assistant
// message), 不是完整 messages history (含 system + history + tool_result + 中间 round
// assistant).
//
// **不过滤 tool_calls 中间状态**: ReAct 循环最后一轮 assistant 一定是 final answer
// (无 tool_calls 才退出循环), 倒序遍历找到的 assistant 就是 final. 业务 hook 可
// 包多 round 的不在此函数管 (调用方决定).
//
// 没可用 assistant 返 nil（两个来源：一条 assistant 都没有；或有但全是框架合成的记账文本） — caller 退化不填 gen_ai_content (Interrupt 路径常见).
func lastAssistantMessage(msgs []*schema.Message) *schema.Message {
	for i := len(msgs) - 1; i >= 0; i-- {
		// 跳过框架合成的记账文本。这个 event 报的是「本次 agent 调用的真 final 答复」，
		// 而记账文本是「assistant + 零 tool_call」、正好命中朴素的倒扫判据——出事的还
		// 恰好是 error 路径（模型没产出就退出时它就是最后一条 assistant），也就是有人
		// 正在看 trace 排障的时候。见 agent_v2.IsUnresolvedCallsRecord 的 godoc。
		if msgs[i] != nil && msgs[i].Role == schema.RoleAssistant &&
			!agent_v2.IsUnresolvedCallsRecord(msgs[i]) {
			return msgs[i]
		}
	}
	return nil
}

// compactionSpanAttrs 把 *CompactionAction 按 phase 结构化展开成 span attr(点命名
// crew.compact.*, 对齐现有 span attr 惯例如 crew.tool.category)——补上
// toString 默认吃空的压缩细节, 让 trace 单条 compaction event 直接可见"压缩了什么":
// 哪层 / 触发原因 / 删了多少 / token before→after / 耗时. 按 Type 只展开对应 phase
// 的字段:dropped/kept 恒展开(含 0——"这次没删"也是信息); token/耗时等可选字段
// 非零才展开避免噪声.
func compactionSpanAttrs(action *agent_v2.CompactionAction) []*commonpb.KeyValue {
	if action == nil {
		return nil
	}
	attrs := []*commonpb.KeyValue{strKV("crew.compact.layer", string(action.Layer))}
	switch action.Type {
	case agent_v2.CompactionActionBefore:
		if b := action.Before; b != nil {
			if b.Trigger != "" {
				attrs = append(attrs, strKV("crew.compact.trigger", b.Trigger))
			}
			if b.InputTokens > 0 {
				attrs = append(attrs, intAttr("crew.compact.input_tokens", int64(b.InputTokens)))
			}
		}
	case agent_v2.CompactionActionAfter:
		if af := action.After; af != nil {
			attrs = append(attrs,
				intAttr("crew.compact.dropped", int64(af.Dropped)),
				intAttr("crew.compact.kept", int64(af.Kept)))
			if af.InputTokens > 0 {
				attrs = append(attrs, intAttr("crew.compact.input_tokens", int64(af.InputTokens)))
			}
			if af.OutputTokens > 0 {
				attrs = append(attrs, intAttr("crew.compact.output_tokens", int64(af.OutputTokens)))
			}
			if af.DurationMs > 0 {
				attrs = append(attrs, intAttr("crew.compact.duration_ms", af.DurationMs))
			}
			if af.Err != "" {
				attrs = append(attrs, strKV("crew.compact.err", af.Err))
			}
		}
	case agent_v2.CompactionActionGenerate:
		if g := action.Generate; g != nil {
			attrs = append(attrs, intAttr("crew.compact.attempt", int64(g.Attempt)))
			if g.Phase != "" {
				attrs = append(attrs, strKV("crew.compact.gen_phase", string(g.Phase)))
			}
			if g.Err != "" {
				attrs = append(attrs, strKV("crew.compact.err", g.Err))
			}
		}
	case agent_v2.CompactionActionRetry:
		if r := action.Retry; r != nil {
			attrs = append(attrs,
				intAttr("crew.compact.attempt", int64(r.Attempt)),
				intAttr("crew.compact.max_attempts", int64(r.MaxAttempts)),
				intAttr("crew.compact.orig_msgs", int64(r.OrigMsgs)),
				intAttr("crew.compact.compacted_msgs", int64(r.CompactedMsgs)))
			if r.Reason != "" {
				attrs = append(attrs, strKV("crew.compact.reason", r.Reason))
			}
		}
	}
	return attrs
}

func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case error:
		return x.Error()
	default:
		return ""
	}
}

// audit² A-MED-1 / D-HIGH-3 fix: 之前 hooks.go 定义独立 `truncate` 函数 byte 切
// UTF-8 unsafe — OnNodeEvent 走 truncate, P2.6 audit LOW-2 修了 renderContent 但
// 漏改这个同模式 helper. 改用 observability.go truncString (UTF-8 safe 头尾切),
// 删本地 truncate.

// startsWith — 内部 registry key prefix 匹配 helper (OnError sweep 路径用).
func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// Install 一次接上**两条** hook 链，返回已 push 编排 hook 的 ctx 和已追加业务 hook 的
// agent 构造 option 列表。
//
//	ctx, opts := obs.Install(ctx,
//	    agent_v2.WithName("assistant"),
//	    agent_v2.WithModel(m),
//	)
//	ag := agent_v2.NewBasicAgent(opts...)
//	events, err := ag.Run(ctx, sess, input)   // ← 必须用返回的这个 ctx
//
// # 为什么要有这个入口
//
// 两条链**必须同时接**：漏掉编排那条，compact(3) / cp(2) / fanout / node / skill 共 8 个
// metric 全不 fire，node 与 graph 的 span 也不会建。而它们的安装点**不在同一个地方**
// （业务级是构造期 option、编排级是 ctx），于是「只装一半」不报错、不崩，只是数据少一截
// ——实测我们自己的 example 里 4 个有 2 个漏了编排那条。
//
// 这个入口把那个选择去掉了：**拿不到 opts 就拿不到 ctx，反之亦然**。真要只装一半得写
// `_, opts := obs.Install(...)` 显式丢掉——那是「想过了不要」，不是「忘了」。
//
// # 它不覆盖的那一档
//
// **agent 在一处构造、ctx 在每个请求 push** 的拓扑（典型是 a2a/server：agent 建一次，
// HTTP middleware 每请求 push ctx）结构上就是两个安装点，收不进一次调用。那一档仍用
// [Observability.AgentHooks] + [Observability.OrchHooks] 两半各装各的，漏装由
// warnIfHalfInstalled 在运行时喊。
//
// # 业务自己也有 hook 时
//
// 照常把 `agent_v2.WithHooks(business)` 放进 opts，Install 会把 obs 链在**最前**
// （`hook.NewChainHooks(obs, business)`）——理由见 [Observability.AgentHooks]：obs 在前
// 才能最早 fire start、最晚 fire end，且业务 hook panic 不会顺带吃掉 obs 的 end。
func (o *Observability) Install(ctx context.Context, opts ...agent_v2.Option) (context.Context, []agent_v2.Option) {
	if o == nil {
		return ctx, opts
	}
	ctx = agent_v2.WithCtxOrchHooks(ctx, o.OrchHooks())

	// 业务已经放了 WithHooks 时不能直接再追加一个——后者会覆盖前者（option 按序应用、
	// 后写赢），业务的 hook 会被静默丢掉。这里显式合成链，obs 在前。
	merged := make([]agent_v2.Option, 0, len(opts)+1)
	merged = append(merged, opts...)
	merged = append(merged, agent_v2.WithHooksChained(o.AgentHooks()))
	return ctx, merged
}
