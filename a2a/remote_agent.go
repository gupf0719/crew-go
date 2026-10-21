package a2a

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"

	"log/slog"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/internal/panicerr"
	"crew-go/observability"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
)

// RemoteAgent 把一个远程 A2A 协议 agent 包成 agent_v2.Agent——既能直接嵌入编排
// （Sequential/Parallel/Loop 等收 Agent），也能经 agent_v2.AgentAsTool 转成 tool 注册进
// host BasicAgent 的 toolkit（设计说明见包 godoc remote_tool.go）。
//
// HITL：RemoteAgent 无自己的 cp，嵌入运行时**骑 host（编排 / BasicAgent toolsNode）的
// cp**——远程返 input-required 时（WithHumanGate 启用）抛 InterruptError 从 host 节点冒泡、
// host cp 存挂起态；等人答复的那一问（远端 taskID、会话线程、第几问、问题）按调用位置记进 host 节点的
// 私有态、随 host cp 走（见 gatePending）。Resume 时 host 重跑节点 → RemoteAgent.Run 再调 → 读回那条
// 记录，按身份（gateQuestionID）取回人的答复续那个远端任务。引擎钩子与条件边里不能问人、不在任何节点
// 执行里时句柄记在 session 上（见 WithHumanGate）。
//
// **并发约束（默认模式下 ParallelAgent 多分支别复用同一 RemoteAgent 实例）**：默认模式（不开
// WithHumanGate）的远程 taskID 按
// `a2a:<name>:task_id:<sess.RunID>` 存共享 session values map。同一 RemoteAgent 实例放进 ParallelAgent
// 多个并发分支、且远程返**可续 task**（input-required / working）时，多分支写同一 key
// → last-writer-wins 互覆 → Resume 续错 task（跟 FinalMessageByAgent "分支用 distinct
// 名" 同款约束）。修法本可按位置维度 scope key，但那会破坏默认模式 ReAct 跨轮续跑
// （AgentAsTool 路径 graphNS 含调用身份的哈希、每个 tool_call 都不同 → key 每轮变 → 续跑 miss），
// 得不偿失。
// **正确用法：每分支用独立 RemoteAgent 实例**（各自 client/card，taskID 天然不撞）。
// 单实例顺序复用（同 session 多次 Run / BasicAgent 单 tool 跨轮）不受影响。gate 模式在节点执行里不受这条
// 约束：等人答复的那一问按调用位置记，并行分支、同一批几个 tool_call、节点里先后几次调用各是各的。
type RemoteAgent struct {
	client    *a2aclient.Client
	card      *a2atypes.AgentCard
	name      string
	desc      string
	humanGate bool
	hooks     hook.Hooks                   // WithHooks 注入；nil 时 NoOp（agent 形态观测见 WithHooks godoc）
	obs       *observability.Observability // WithObservability 注入；nil 时无 a2a 跨语言 trace
}

var _ agent_v2.Agent = (*RemoteAgent)(nil)

// NewRemoteAgent 解析 cardURL 处的 AgentCard + 建 A2A client，返回包装的 RemoteAgent。
//
// cardURL 是远程 agent 的基地址（解析器自动取 /.well-known/agent-card.json）。
func NewRemoteAgent(ctx context.Context, cardURL string, opts ...RemoteOption) (*RemoteAgent, error) {
	// cardURL 是 base 地址——解析器自动追加 /.well-known/agent-card.json。
	card, err := agentcard.DefaultResolver.Resolve(ctx, cardURL)
	if err != nil {
		return nil, fmt.Errorf("a2a: resolve agent card from %q: %w", cardURL, err)
	}
	return NewRemoteAgentFromCard(ctx, card, opts...)
}

// NewRemoteAgentFromCard 从**已解析的** AgentCard 建 RemoteAgent——业务已自行拉到 card、
// 或发现层按完整 agentCardUrl 解析后用（见 NewRemoteAgentFromService）。建 client + 定名/描述。
func NewRemoteAgentFromCard(ctx context.Context, card *a2atypes.AgentCard, opts ...RemoteOption) (*RemoteAgent, error) {
	if card == nil {
		return nil, fmt.Errorf("a2a: NewRemoteAgentFromCard requires a non-nil AgentCard")
	}
	cfg := &remoteConfig{}
	for _, o := range opts {
		o(cfg)
	}
	client, err := a2aclient.NewFromCard(ctx, card, cfg.clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("a2a: create client for agent %q: %w", card.Name, err)
	}
	name := cfg.name
	if name == "" {
		name = sanitizeName(card.Name)
	}
	desc := firstNonEmpty(cfg.description, card.Description)
	desc = firstNonEmpty(desc, "delegate a task to the remote A2A agent "+card.Name)
	return &RemoteAgent{client: client, card: card, name: name, desc: desc, humanGate: cfg.humanGate, hooks: cfg.hooks, obs: cfg.obs}, nil
}

// Name 返回 agent/tool 名（OpenAI 合规）。
func (a *RemoteAgent) Name() string { return a.name }

// Description 返回职责描述（默认远程 AgentCard.Description——供 host LLM 能力发现）。
func (a *RemoteAgent) Description() string { return a.desc }

// Run 把 input 委派给远程 A2A agent，返回本次事件增量。
func (a *RemoteAgent) Run(ctx context.Context, session *agent_v2.RunSession, input *schema.Message, _ ...agent_v2.RunOption) (events []*agent_v2.AgentEvent, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			// error 类 panic 保 unwrap 链（对齐 recoverToErr）
			pe := panicerr.New(r)
			retErr = panicerr.Wrap("a2a.RemoteAgent.Run panic recovered", pe)
			panicerr.Log(ctx, slog.LevelError, "a2a: RemoteAgent 兜住 panic，已转成错误", pe, panicerr.KeyError, retErr.Error())
		}
	}()
	if input == nil {
		input = schema.UserMessage("")
	}
	// 同 9 个编排的入口（agent_v2.EnterRun）：在一个正在执行的任务里 session 必须属于它（远端 task
	// 句柄按 RunID 存——随手给新 session 的话父节点每次重跑都是一个新的远端 task，用户的回答被当成
	// 新 task 的第一条消息发出去，原来那个一直挂着，实测）；nil 时补一个属于所在任务的。同时盖任务章、
	// 遮住调用方的节点授予（钩子跑在图外，拿不到调用方的转交 / 路由能力）。
	ctx, session, err := agent_v2.EnterRun(ctx, session, a.name)
	if err != nil {
		return nil, err
	}
	ctx = agent_v2.WithSession(ctx, session)
	// 不 emit input 的 OutputEvent（区别于编排 wrapper / BasicAgent）：RemoteAgent 是
	// 无自己 cp 的 leaf，HITL 经 host 重跑 RemoteAgent.Run 续跑——编排靠
	// shouldEmitInputEvent（查自己 cp ns 是否存在）防 Resume 重跑 dup，但 RemoteAgent
	// 无 cp ns 该 guard 不适用，重跑会把 input event 重复写进共享父 session（泄漏进下游
	// 读 history 的 LLM 输入，同 ISSUE-74 input-dup class）。input 由 host 层记录，
	// RemoteAgent 只 emit 远程输出的 ExitEvent。
	startOffset := len(session.Events())

	// 业务级 hook 对（对齐 8 编排范式，audit obs-#3）：OnAgentStart top-level fire（input
	// 改写 [消费]）+ OnAgentEnd defer 兜底全部退出路径（含 InterruptError / panic，
	// WithEndError 协议让 observability 识别中断 fire interrupts metric）。
	hooks := a.hooks
	if hooks == nil {
		hooks = hook.NoOpHooks{}
	}
	info := &hook.AgentInfo{Name: a.name, Type: "a2a_remote", Round: 0}
	var finalMessagesRef []*schema.Message
	agentIn := &hook.AgentInput{Messages: []*schema.Message{input}}
	// 放标记、调、采纳收在 compose_v2.CallHook 一处（同 9 编排）：钩子返回别处缓存的 ctx 时，另一次执行的状态、
	// 另一个请求的环境与取消都不会被带进来。
	var hErr error
	ctx, hErr = compose_v2.CallHook(ctx, func(c context.Context) (context.Context, error) {
		return hooks.OnAgentStart(c, info, agentIn)
	})
	if hErr != nil {
		if _, isInt := compose_v2.IsInterrupt(hErr); !isInt && ctx.Err() == nil {
			_ = hooks.OnError(ctx, info, &schema.CommError{Code: "agent_start_error", Message: hErr.Error()})
		}
		// 与 9 编排同一入口（agent_v2.WrapAgentStartErr）：顶层（无父节点路径接住）的
		// hook 中断在这儿被响亮拒绝——原先 `%w` 透传让假暂停原样存活（IsInterrupt=true
		// 但没有任何档可续，用户答完 Resume 撞 ErrNoPendingCheckpoint）。RemoteAgent 是
		// 第 10 个 Agent 实现，这条规矩曾漏接。嵌套（被编排当成员调）照旧放行。
		return eventsSince(session, startOffset), agent_v2.WrapAgentStartErr(ctx, hErr)
	}
	if len(agentIn.Messages) > 0 && agentIn.Messages[len(agentIn.Messages)-1] != nil {
		input = agentIn.Messages[len(agentIn.Messages)-1]
	}
	defer func() {
		// panic 兜底在 OnAgentEnd defer 内 recover（对齐 Sequential 块 + recoverPanicToErr
		// 语义：error 类 panic 用 %w 保 unwrap 链，label 标 recover 点区分外层）——让
		// OnAgentEnd 看到 panic 转的 retErr；外层 recover defer 仍是最终 backstop。
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			retErr = panicerr.Wrap("a2a.RemoteAgent.OnAgentEnd panic recovered", pe)
			panicerr.Log(ctx, slog.LevelError, "a2a: RemoteAgent 收尾兜住 panic，已转成错误", pe, panicerr.KeyError, retErr.Error())
		}
		ctxOut := agent_v2.WithEndError(ctx, retErr)
		_ = hooks.OnAgentEnd(ctxOut, info, &hook.AgentOutput{Messages: finalMessagesRef})
	}()

	final, err := a.consume(ctx, session, input)
	if err != nil {
		// InterruptError / ctx 错 / 业务错统一 propagate——InterruptError 让 host cp 存挂起态
		return eventsSince(session, startOffset), err
	}
	if final != nil {
		finalMessagesRef = []*schema.Message{final}
		session.AddEvent(agent_v2.NewExitEvent(a.name, final))
	}
	return eventsSince(session, startOffset), nil
}

// Resume 标准 Agent 接口要求。RemoteAgent 无自己的 cp——**嵌入运行时由 host 编排 /
// BasicAgent 的 Resume 重跑节点再调 Run 完成续跑**（RunSubAgent 调 sub.Run 非 sub.Resume），
// 故 RemoteAgent.Resume 仅在业务**裸顶层**调 RemoteAgent.Run 后单独 Resume 时被调，这种
// 无 host cp 的场景不支持。
func (a *RemoteAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, fmt.Errorf("a2a.RemoteAgent %q: standalone Resume not supported — embed RemoteAgent in a host (AgentAsTool in BasicAgent, or an orchestration step) so the host's Resume reruns the remote call (HITL needs WithHumanGate)", a.name)
}

// consume 是核心 A2A 消费逻辑：发消息 → 迭代事件流 → 按 Task 终态返结果 / 抛中断 / 返错误。
func (a *RemoteAgent) consume(ctx context.Context, session *agent_v2.RunSession, input *schema.Message) (final *schema.Message, retErr error) {
	taskID, contextID := a.loadIDs(session)
	sendMsg := input // 首发用完整 input（含多模态 ContentBlocks）；续一个在等人答复的远端任务时换成人的答复

	// human gate：这次调用在等人答复的那一问记在这个调用位置上（gatePending，随宿主的存档走）。位置在这里取
	// 一次（占一个提问序号，跟有没有在等人的那一问无关，同一相位里后面的提问才不挪位）。
	//
	// 待续的远端任务只认这条记录，不认 session 上按 RunID 存的那一个槽——那个槽整个任务共用：节点里先后调两次
	// 时第一次调用续掉了第二次调用挂着的远端任务，同一批两个 tool_call 抢同一个任务（实测）。
	//
	// 按调用发生在哪里分三类（见 WithHumanGate）：
	//
	//   - 节点执行里、有提问位置（节点体、工具、工具钩子、编排成员）：按位置记（byPosition）。
	//   - 节点执行里、没有提问位置（引擎的节点钩子、图级钩子、条件边）：那里抛的中断接不住（被拒或被丢掉），
	//     不续任何任务、也不碰 session 上的句柄，远端要人时报错。用 session 句柄的话，下一次调用会把上一次在等
	//     的那一问当成已答、远端照着执行（两个节点的 OnNodeEnd 各调一次，实测 `err == nil`）。
	//   - 不在任何节点执行里（顶层直接调、脱离出来的独立任务）：没有存档接得住中断（中断的 ID 为空），业务拿
	//     同一个 session 带上人的答复再 Run 一次就续刚才那个任务——沿用 session 上的句柄，同默认模式。只按位置
	//     记的话顶层再 Run 一次会新开任务、把人的答复当成请求（复审实测）。
	pos := ""
	var pend gatePending
	if a.humanGate {
		pos = compose_v2.CurrentInterruptSite(ctx).InterruptID()
	}
	inNode := a.humanGate && compose_v2.WithinNodeScope(ctx)
	byPosition := inNode && pos != ""
	var sendParts a2atypes.ContentParts // 非 nil 时直接发它（按 interruptId 的答复），不再由 sendMsg 转
	if inNode {
		taskID = ""
	} else if taskID != "" {
		// 续一个远端挂着多问的任务：调用方（host LLM / 业务）按约定给的是 interruptId → 答复的 JSON 对象时，
		// 换成每问一个 DataPart。远端挂多问时只接受按 interruptId 的答复，一条文本会被请重答。
		if ids := a.loadPendingIDs(session); len(ids) > 1 {
			sendParts = answerPartsFromJSON(input, ids)
		}
	}
	if byPosition {
		if p, ok := compose_v2.GetStepResult[gatePending](ctx, gateStepKey(pos)); ok && p.TaskID != "" {
			pend = p
			if len(p.Items) > 1 {
				// 远端挂着多问：每一问各是宿主这边的一个提问，答了几问就按 interruptId 发几问（远端没答到的
				// 继续挂着，下一轮再来要人）。一问都没答：不联系远端，原样再问一次。
				parts := gateItemAnswers(ctx, pos, p)
				if len(parts) == 0 {
					return nil, gateItemsInterrupt(pos, p.TaskID, p.Round, p.Items)
				}
				taskID, sendParts = p.TaskID, parts
			} else {
				id := gateQuestionID(pos, p.TaskID, p.Round)
				ans, ok := compose_v2.GetResumeAnswer(ctx, id)
				if !ok || ans == nil {
					// 那一问还没答（没给、或给的是 nil）：不联系远端，原样再问一次。曾经把这次调用的原始输入当成
					// 答复发出去，远端照样续上、做完，`err == nil`（实测）
					return nil, gateInterrupt(id, p.Question, p.Items)
				}
				taskID, sendMsg = p.TaskID, ans
			}
			contextID = firstNonEmpty(p.ContextID, contextID)
		}
	}

	var (
		result       strings.Builder
		blocks       []*schema.ContentBlock // 远程应答的非文本 part（raw/url/data → File/Data block）
		lastState    a2atypes.TaskState
		newTaskID    string
		newContextID string
		statusMsg    string
		statusParts  a2atypes.ContentParts // 最后一条状态消息的 parts（input-required 时从里面取挂起清单）
	)
	// 完整多模态发送：sendMsg 的 Content + ContentBlocks → A2A parts（text/raw/url/data）
	parts := sendParts
	if parts == nil {
		parts = MessageToA2AParts(sendMsg)
	}
	msg := buildMessageFromParts(parts, taskID, contextID)

	// a2a 跨语言 trace（WithObservability 启用）：建 client span（协议——仅当有活跃父
	// agent span 时才建，无父返 no-op 不注入避免孤立 span）+ 把 CS-OSS trace context
	// 注入 outbound Message.metadata（走 message.metadata 非 HTTP header，跨语言串联）。
	// span 生命周期覆盖整个 stream 消费 + 终态（defer End 按 retErr 标 status）。
	if a.obs != nil {
		// span remoteName 用远程 AgentCard.Name（远程声明的真实 agent 名），而非 a.name（host 端
		// WithName 可能设的本地占位名，如 example 里的 "zk_remote"）——让 trace 一眼看出实际调的
		// 哪个远程 agent（对齐跨语言 trace 规范 gen_ai.a2a_client.<remoteName>）。a.card 在
		// NewRemoteAgentFromCard 时必非 nil（拉到 card 才建得了 client）。
		cliSpan := a.obs.StartA2AClientSpan(ctx, a.card.Name, "message_stream", "jsonrpc", contextID, taskID)
		if tc := cliSpan.TraceContext(); tc.IsValid() {
			if msg.Metadata == nil {
				msg.Metadata = map[string]any{}
			}
			observability.InjectToMetadata(msg.Metadata, tc.TraceID, tc.SpanID, tc.AppName, tc.NextIdx, tc.Sampled)
		}
		defer func() {
			status, errType := "success", ""
			if retErr != nil {
				if _, isInt := compose_v2.IsInterrupt(retErr); isInt {
					status = "input_required" // 远程 input-required + humanGate 抛中断，非错误
				} else {
					status, errType = "error", "a2a_remote_error"
				}
			}
			cliSpan.End(status, errType, "") // errMsg 留空——协议安全边界：不记可能含敏感的远程 error detail
		}()
	}

	for ev, e := range a.client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg}) {
		if e != nil {
			return nil, fmt.Errorf("a2a: send to remote agent %q: %w", a.card.Name, e)
		}
		switch v := ev.(type) {
		case *a2atypes.Message:
			t, bs := PartsToContent(v.Parts)
			result.WriteString(t)
			blocks = append(blocks, bs...)
			emitChunk(ctx, a.name, t)
			if v.TaskID != "" {
				newTaskID = string(v.TaskID)
			}
			if v.ContextID != "" {
				newContextID = v.ContextID
			}
		case *a2atypes.Task:
			newTaskID = string(v.ID)
			newContextID = v.ContextID
			lastState = v.Status.State
			if v.Status.Message != nil {
				statusMsg = a2aPartsText(v.Status.Message.Parts)
				statusParts = v.Status.Message.Parts
			}
			for _, art := range v.Artifacts {
				if art != nil {
					t, bs := PartsToContent(art.Parts)
					result.WriteString(t)
					blocks = append(blocks, bs...)
				}
			}
		case *a2atypes.TaskStatusUpdateEvent:
			if v.TaskID != "" {
				newTaskID = string(v.TaskID)
			}
			if v.ContextID != "" {
				newContextID = v.ContextID
			}
			lastState = v.Status.State
			if v.Status.Message != nil {
				statusMsg = a2aPartsText(v.Status.Message.Parts)
				statusParts = v.Status.Message.Parts
			}
		case *a2atypes.TaskArtifactUpdateEvent:
			if v.Artifact != nil {
				t, bs := PartsToContent(v.Artifact.Parts)
				result.WriteString(t)
				blocks = append(blocks, bs...)
				emitChunk(ctx, a.name, t)
			}
		}
	}

	q := firstNonEmpty(statusMsg, "the remote agent needs more information to proceed")
	if inNode {
		// 远端给了答复（这一问不再等人）之后这一位上就没有在等人的提问了：清掉记录，之后再跑发原始请求。
		// 只在收到远端的结局之后清——发送在网络层失败时（上面早退）记录留着，续跑拿回同一个答案续同一个任务
		if byPosition && lastState != a2atypes.TaskStateInputRequired && pend.TaskID != "" {
			if err := compose_v2.SaveStepResult(ctx, gateStepKey(pos), gatePending{}); err != nil {
				return nil, fmt.Errorf("a2a: remote agent %q: clear the answered question: %w", a.card.Name, err)
			}
		}
		// 会话线程标识跨任务存活（见 clearIDs）；远端任务的句柄在 gate 模式下只记在调用位置上
		a.storeIDs(session, "", newContextID)
	}

	switch lastState {
	case a2atypes.TaskStateInputRequired:
		items := PendingQuestionsFromParts(statusParts)
		if a.humanGate {
			// 这一问属于哪个远端任务：本次响应里给的，没给就是刚才续的那个
			askTask := firstNonEmpty(newTaskID, taskID)
			if askTask == "" {
				// 不合协议（input-required 是任务的状态，必带 taskID）。抛中断的话人答了也续不上：续跑时没有
				// 待续的任务，原始请求再发一遍、远端再要人，同一个问题一遍遍重问。
				return nil, fmt.Errorf("a2a: remote agent %q asked for input without a task id, the answer could not be routed back to it", a.card.Name)
			}
			if !inNode {
				// 不在节点执行里：句柄记在 session 上，中断没有 ID（没有位置可派生）。挂多问时业务用同一个
				// session 再 Run 一次、输入给 interruptId → 答复的 JSON 对象（见 answerPartsFromJSON）。
				a.storeIDs(session, askTask, newContextID)
				a.storePendingIDs(session, items)
				if len(items) > 1 {
					q += "\n" + jsonAnswerHint
				}
				return nil, gateInterrupt("", q, items)
			}
			if !byPosition {
				// 引擎钩子、条件边里：没有提问位置，抛中断也接不住。调用放错了位置，重跑必然一样：终局失败（不重试、
				// 清掉存档）。普通错误会被外面的重试一遍遍重跑，每次多开一个停在要人上、没人去续的远端任务（实测）
				return nil, fmt.Errorf("%w: a2a: remote agent %q asked for input inside an engine hook or edge condition (ns=%q), where no interrupt can be caught; call it from a node body, a tool or an orchestration member instead",
					compose_v2.ErrRunAborted, a.card.Name, compose_v2.GetGraphNS(ctx))
			}
			// 同一个远端任务里的第几问：续着的那个任务又来要人就是下一问，新任务从第 1 问数起
			round := 1
			if pend.TaskID == askTask {
				round = pend.Round + 1
			}
			// 记的是已经发生的事：远端这个任务在等人答这一问
			rec := gatePending{TaskID: askTask, ContextID: firstNonEmpty(newContextID, contextID), Round: round, Question: q, Items: items}
			if err := compose_v2.SaveStepResult(ctx, gateStepKey(pos), rec); err != nil {
				return nil, fmt.Errorf("a2a: remote agent %q: record the pending question: %w", a.card.Name, err)
			}
			// 真人 HITL：抛 InterruptError 骑 host cp（续跑时按同一个身份取回人的答复、续这个远端任务）。远端挂多问时
			// 每一问各抛一个提问，人分别作答。
			if len(items) > 1 {
				return nil, gateItemsInterrupt(pos, askTask, round, items)
			}
			return nil, gateInterrupt(gateQuestionID(pos, askTask, round), q, items)
		}
		// 远程要更多输入——存 taskID 供续跑同一 task
		a.storeIDs(session, newTaskID, newContextID)
		a.storePendingIDs(session, items)
		// 默认：把追问当输出返回——host BasicAgent 的 LLM 看到后补信息再调一次（续同一 task）
		if len(items) > 1 {
			return schema.AssistantMessage(fmt.Sprintf(
				"[input_required] remote agent %q is waiting for answers to several questions:\n%s\n(%s)",
				a.card.Name, q, jsonAnswerHint)), nil
		}
		return schema.AssistantMessage(fmt.Sprintf(
			"[input_required] remote agent %q needs more information: %s\n(Provide it by calling this tool again with the additional info.)",
			a.card.Name, q)), nil
	case a2atypes.TaskStateCompleted, a2atypes.TaskStateUnspecified:
		if !inNode {
			a.clearIDs(session)
		}
		return assistantWithBlocks(firstNonEmpty(result.String(), statusMsg), blocks), nil
	case a2atypes.TaskStateFailed, a2atypes.TaskStateRejected, a2atypes.TaskStateCanceled:
		if !inNode {
			a.clearIDs(session)
		}
		return nil, fmt.Errorf("a2a: remote agent %q task ended %s: %s", a.card.Name, lastState, firstNonEmpty(statusMsg, "(no detail)"))
	default:
		// working/submitted/auth_required 但流意外结束——存 ID 供续跑，返已聚合内容。
		// 注：A2A 规范下 AuthRequired/Working/Submitted 都非终态（Terminal() 不含），
		// 合规 server 不会以它们结束流——仅连接异常断开才落这里。best-effort 返已收内容，
		// 但部分内容伪装完整答案对业务是静默错——warn 给观测信号（"什么算成功"防御）。
		// 节点执行里的 gate 不留句柄：这个任务没在等人，下一次调用发它自己的请求。
		slog.WarnContext(ctx, "a2a: remote stream ended on non-terminal state (partial content returned)",
			"agent", a.card.Name, "state", string(lastState))
		if !inNode {
			a.storeIDs(session, newTaskID, newContextID)
		}
		return assistantWithBlocks(result.String(), blocks), nil
	}
}

// gateInterrupt 是 human gate 抛给宿主的那一问。items 是远端给的挂起清单：只有一问且是工具审批时，宿主这边的
// 提问也标成审批（带上选项），宿主的前端适配层按审批展示、校验答复。
func gateInterrupt(id, question string, items []PendingQuestion) *compose_v2.InterruptError {
	var item *PendingQuestion
	if len(items) == 1 {
		item = &items[0]
	}
	q := gateQuery(id, question, item)
	return &compose_v2.InterruptError{
		Reason: q.Reason,
		// Reason 也填在 query 上：单个中断走「原样透出」不经 MergeInterrupts，
		// 而拷 ie.Reason 到 query 只发生在 Merge 里——只填 ie.Reason 的话 cp 里
		// 读不出这条挂起是什么，且单/多路径行为分叉（同 approval.go / ask_user_tool.go）。
		Query: q,
	}
}

// gateQuery 构造宿主这边的一问：远端那一问是工具审批时标成审批（不带远端的 toolCallId——那不是宿主的工具调用）。
func gateQuery(id, question string, item *PendingQuestion) *compose_v2.AskQuery {
	q := &compose_v2.AskQuery{Question: question, InterruptID: id, Reason: "a2a_input_required"}
	if item != nil && item.Approval {
		q.Reason = agent_v2.ApprovalInterruptReason
		q.Options = item.Options
	}
	return q
}

// gateItemsInterrupt 是远端挂多问时 human gate 抛给宿主的中断：每一问一个提问，各有自己的身份（gateItemID）。
func gateItemsInterrupt(pos, taskID string, round int, items []PendingQuestion) *compose_v2.InterruptError {
	qs := make([]*compose_v2.AskQuery, 0, len(items))
	for i := range items {
		qs = append(qs, gateQuery(gateItemID(pos, taskID, round, items[i].InterruptID), items[i].Question, &items[i]))
	}
	return &compose_v2.InterruptError{Reason: "a2a_input_required", Queries: qs}
}

// gateItemAnswers 取宿主这边对远端每一问的答复，转成按 interruptId 的 DataPart；没答到的跳过。
func gateItemAnswers(ctx context.Context, pos string, p gatePending) a2atypes.ContentParts {
	var parts a2atypes.ContentParts
	for _, it := range p.Items {
		ans, ok := compose_v2.GetResumeAnswer(ctx, gateItemID(pos, p.TaskID, p.Round, it.InterruptID))
		if !ok || ans == nil {
			continue
		}
		parts = append(parts, AnswerPart(it.InterruptID, nil, ans.Content))
	}
	return parts
}

// jsonAnswerHint 告诉调用方远端挂多问时怎么作答。
const jsonAnswerHint = `answer by calling again with a JSON object that maps each interruptId to its answer, e.g. {"<interruptId>": "<answer>"}; tool approvals take true / false or one of "approve", "approve_always", "approve_always_tool", "deny"`

// answerPartsFromJSON 把调用方给的 interruptId → 答复 JSON 对象转成按 interruptId 的 DataPart：键必须都是远端
// 挂着的提问，值是字符串或布尔。不是这个形状时返回 nil（照常作为文本发出，远端会请重答）。
func answerPartsFromJSON(input *schema.Message, pendingIDs []string) a2atypes.ContentParts {
	if input == nil {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(input.Content)), &m); err != nil || len(m) == 0 {
		return nil
	}
	known := make(map[string]bool, len(pendingIDs))
	for _, id := range pendingIDs {
		known[id] = true
	}
	ids := make([]string, 0, len(m))
	for id := range m {
		if !known[id] {
			return nil
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	parts := make(a2atypes.ContentParts, 0, len(ids))
	for _, id := range ids {
		switch v := m[id].(type) {
		case string:
			parts = append(parts, AnswerPart(id, nil, v))
		case bool:
			parts = append(parts, AnswerPart(id, &v, ""))
		default:
			return nil
		}
	}
	return parts
}

// ---- 远程 task 句柄的存取（session 半共享 values，跨 Resume 在进程内存活）----
//
// **两个键的生命周期不同，键形态跟着分开**：
//
//   - taskID 是**任务级**的（A2A 的 Task 一次办一件事）⟹ 键里编入 `sess.RunID`。
//     `RunSession.RunID` 的契约是「一枚只服务一个任务」：同一任务内的续跑（Resume 带
//     `WithResumeSession`、或 input-required 后同 sess 再 Run 一次）RunID 不变 ⟹ 读得到；
//     业务 `NextTask()` 开新任务换了 RunID ⟹ 旧 taskID 自然不可达。
//     不编的话（曾经就是裸名字键）：任务 A 的远端停在 input-required、A 被放弃、新任务 B
//     调同一个远程 agent ⟹ B 读到 A 的 taskID ⟹ **B 的无关内容被远端当成 A 那个危险
//     确认的回答**（实测 wire 上 B 带着 A 的 taskID），全程 err=nil。
//     旧任务残留的 entry 键带旧 RunID、永不再被读，随 session 生命周期回收。
//   - contextID 是**会话级**的（A2A 的会话线程标识，跨多 task 存活）⟹ 键不带任务维，
//     跨 NextTask 保留是**有意的**——见 clearIDs 里那段。
func (a *RemoteAgent) taskKey(s *agent_v2.RunSession) string {
	return "a2a:" + a.name + ":task_id:" + s.RunID
}
func (a *RemoteAgent) ctxKey() string { return "a2a:" + a.name + ":context_id" }

func (a *RemoteAgent) loadIDs(s *agent_v2.RunSession) (taskID, contextID string) {
	if s == nil {
		return "", ""
	}
	// 断言 ok 显式丢弃：values 是业务共享 KV，理论上他人可能在同 key 存非 string——
	// 那种污染场景取零值 ""（buildMessage 当无 task 处理）是安全降级，无需 fail。
	if v, ok := s.GetValue(a.taskKey(s)); ok {
		taskID, _ = v.(string)
	}
	if v, ok := s.GetValue(a.ctxKey()); ok {
		contextID, _ = v.(string)
	}
	return taskID, contextID
}

func (a *RemoteAgent) storeIDs(s *agent_v2.RunSession, taskID, contextID string) {
	if s == nil {
		return
	}
	if taskID != "" {
		s.SetValue(a.taskKey(s), taskID)
	}
	if contextID != "" {
		s.SetValue(a.ctxKey(), contextID)
	}
}

// pendingKey 是远端挂多问时那几问的 interruptId 在 session 上的键（任务级，同 taskKey）。
func (a *RemoteAgent) pendingKey(s *agent_v2.RunSession) string {
	return "a2a:" + a.name + ":pending:" + s.RunID
}

// storePendingIDs 记下远端挂着的几问的 interruptId（只有一问或没有清单时清空）：续这个任务时据此认出调用方给的
// 按 interruptId 的 JSON 答复。
func (a *RemoteAgent) storePendingIDs(s *agent_v2.RunSession, items []PendingQuestion) {
	if s == nil {
		return
	}
	if len(items) <= 1 {
		s.SetValue(a.pendingKey(s), []string(nil))
		return
	}
	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.InterruptID)
	}
	s.SetValue(a.pendingKey(s), ids)
}

func (a *RemoteAgent) loadPendingIDs(s *agent_v2.RunSession) []string {
	if s == nil {
		return nil
	}
	v, _ := s.GetValue(a.pendingKey(s))
	ids, _ := v.([]string)
	return ids
}

func (a *RemoteAgent) clearIDs(s *agent_v2.RunSession) {
	if s == nil {
		return
	}
	s.SetValue(a.pendingKey(s), []string(nil))
	// **只清 taskID，contextID 留着**。A2A 语义里 contextID 是会话线程标识、跨多 task
	// 存活；taskID 才是单次任务的。
	//
	// 原先两个一起清，理由是「MVP 单 task，清掉不丢可达状态」——那个前提**已经不成立**：
	// 被服务方按 ContextID 认会话（server 侧把它 push 成 ThreadID，会话历史与持久转交都
	// 挂在上面）。清掉之后下一次请求不带 contextId，对端 SDK 会**现生成一个随机的**，于是
	// 同一场对话被拆成互不相识的多条：远端第二轮读不到第一轮的历史，而且全程 err=nil、
	// 无 warn。典型触发是 NewRemoteAgentTool 挂进 host 的 toolkit，host 在两轮 ReAct 里
	// 各调一次远程。
	//
	// 留着它不会让状态过期：contextID 存在 RunSession 的 KV 里，随 session 生命周期结束。
	s.SetValue(a.taskKey(s), "")
}

// gatePending 是 human gate 在一个调用位置上等人答复的那一问：远端哪个任务、这个任务里的第几问、问的什么。
// 远端要人时记下、远端给了别的结局时清掉（TaskID 为空即没有）。
//
// 记在宿主节点这次执行的私有态里（compose_v2.SaveStepResult，键带上调用位置）：按节点体 / tool_call / 交接
// 出去的那一段分开，节点里先后调两次、同一批几个 tool_call、几个并行分支各是各的；随宿主的存档走，续跑不带
// 原来的 session 也读得回。
//
// 会话线程（ContextID）也跟任务一起记：并发的几个调用各自新开任务时，远端可能给出不同的线程，续哪个任务
// 就要带哪个任务的线程——session 上那一个槽是最后写的那个，续别的任务时会被远端拒掉（实测）。
type gatePending struct {
	TaskID    string `json:"task_id"`
	ContextID string `json:"context_id"`
	Round     int    `json:"round"`
	Question  string `json:"question"`
	// Items 是远端给的挂起清单（远端不是 crew-go、没给清单时为空）。多于一问时每一问各是宿主这边的一个
	// 提问；只有一问且是工具审批时，宿主这边的那一问标成审批。
	Items []PendingQuestion `json:"items,omitempty"`
}

func init() {
	compose_v2.RegisterValueType[gatePending]("a2a.gatePending")
}

// gateStepKey 是 gatePending 在节点私有态里的键：同一个分域里先后几次调用按各自的位置分开。
func gateStepKey(pos string) string {
	return "a2a.gate\x00" + pos
}

// gateQuestionID 是 human gate 那一问的身份：这次调用的位置（pos，按执行位置派生，见
// compose_v2.CurrentInterruptSite）、它问的那个远端任务，以及这个任务里的第几问。
//
// 只用位置的话，同一个位置上先后两个远端任务的追问是同一个 ID：前一个任务失败之后再跑、新任务又来要人时，
// 调用方累积传的旧答案（答的是前一个任务的那一问）就成了新任务那一问的答案。带上 taskID 之后，新任务的追问
// 是新问题，换一个 ID 问人；带上第几问，同一个任务答完一问又来要人也是新问题。
//
// 不在节点执行里（pos 为空）时返回空串：那里没有位置可派生。
func gateQuestionID(pos, taskID string, round int) string {
	if pos == "" {
		return ""
	}
	h := fnv.New64a()
	// hash.Hash 的 Write 按约定不返回错误
	_, _ = h.Write([]byte(pos + "\x00" + taskID + "\x00" + strconv.Itoa(round)))
	return fmt.Sprintf("%016x", h.Sum64())
}

// gateItemID 是远端挂多问时每一问在宿主这边的身份：gateQuestionID 的那几维，再加上远端那一问的 interruptId。
func gateItemID(pos, taskID string, round int, remoteID string) string {
	if pos == "" {
		return ""
	}
	h := fnv.New64a()
	// hash.Hash 的 Write 按约定不返回错误
	_, _ = h.Write([]byte(pos + "\x00" + taskID + "\x00" + strconv.Itoa(round) + "\x00" + remoteID))
	return fmt.Sprintf("%016x", h.Sum64())
}

// eventsSince 返回 session 中 startOffset 之后产生的事件增量（拷贝，不别名 session 内部 slice）。
func eventsSince(s *agent_v2.RunSession, startOffset int) []*agent_v2.AgentEvent {
	all := s.Events()
	if startOffset >= len(all) {
		return nil
	}
	return append([]*agent_v2.AgentEvent(nil), all[startOffset:]...)
}
