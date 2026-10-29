package a2a

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"iter"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

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
// cp**——远端要人时（审批请求与 AUTH_REQUIRED 一律如此，普通追问要开 WithHumanGate）抛 InterruptError 从
// host 节点冒泡、host cp 存挂起态；等人答复的那一问（远端 taskID、会话线程、第几问、问题）按调用位置记进
// host 节点的私有态、随 host cp 走（见 gatePending）。Resume 时 host 重跑节点 → RemoteAgent.Run 再调 → 读回
// 那条记录，按身份（gateQuestionID）取回人的答复续那个远端任务；AUTH_REQUIRED 的凭证走带外通道，人答「已经
// 授权」时不发消息、重新挂上那个任务（见 reattach）。在节点执行里被调起时这次调用在它自己的相位里问，不占调用它
// 的那段代码的提问序号（见 callPhase）。引擎钩子与条件边里不能问人、不在任何节点执行里时句柄记在 session 上（见
// WithHumanGate）。续一个远端任务之前先查它此刻的状态（见 reconcileTask）：上一次续它时响应在路上丢了、远端其实已经
// 办完或换了一问的，不再去续一个已结束的任务、不把旧答复再发一次。
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
	// 在节点执行里：这次调用连同它的 agent 级钩子放进它自己的相位（见 callPhase）。远端要人时提问不占调用它的那段代码
	// 的序号——调用方在节点体、工具体、交接相位还是钩子里都一样，它按进度跳过这次调用时后面的提问不挪位。
	if compose_v2.WithinNodeScope(ctx) {
		ctx = compose_v2.EnterPhase(ctx, a.callPhase(ctx, input))
	}
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
		// OnAgentEnd 是观测钩子，它 panic 不改变这次调用的结局——就地兜住、带栈记一条 ERROR（同 agent_v2 的编排）。成功时转成
		// 错误的话，编排续跑 / 重试时远端请求再发一遍、远端的副作用重放；失败时交给外层兜底的话，原来的错误（远端的追问中断、
		// 终局失败）被整个换掉。
		defer func() {
			if r := recover(); r != nil {
				panicerr.Log(ctx, slog.LevelError, "a2a: RemoteAgent 的 OnAgentEnd 钩子 panic（这次调用的结局不变）", panicerr.New(r),
					"call_failed", retErr != nil)
			}
		}()
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
	return nil, fmt.Errorf("a2a.RemoteAgent %q: standalone Resume not supported — embed RemoteAgent in a host (AgentAsTool in BasicAgent, or an orchestration step) so the host's Resume reruns the remote call", a.name)
}

// consume 是核心 A2A 消费逻辑：发消息（或重新挂上一个在等带外授权的任务）→ 迭代事件流 → 按 Task 终态返结果 /
// 抛中断 / 返错误。
func (a *RemoteAgent) consume(ctx context.Context, session *agent_v2.RunSession, input *schema.Message) (final *schema.Message, retErr error) {
	taskID, contextID := a.loadIDs(session)
	sendMsg := input // 首发用完整 input（含多模态 ContentBlocks）；续一个在等人答复的远端任务时换成人的答复
	// reattach：续的是一个停在 AUTH_REQUIRED 上的远端任务、人没有拒绝——不发消息，重新挂上那个任务接着读（见
	// reattach）。授权的凭证按规范走带外通道，远端拿到凭证就接着办、不需要客户端再发消息（规范 §7.6）。
	reattach := false

	// human gate：这次调用在等人答复的那一问记在这个调用位置上（gatePending，随宿主的存档走）。位置在这里取
	// 一次：在节点执行里时 ctx 已经在这次调用自己的相位里（见 callPhase），不占调用方的提问序号。
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
	//
	// 远端的审批请求（挂起清单里标了审批的问题、任务状态 AUTH_REQUIRED）不管开没开 WithHumanGate 都交给真人，所以节点
	// 里的提问位置一律要取。WithHumanGate 只决定普通追问交给谁：没开时普通追问照旧交给宿主模型、靠 session 上的句柄
	// 续同一个远端任务。
	inNode := compose_v2.WithinNodeScope(ctx)
	pos := ""
	if inNode {
		pos = compose_v2.CurrentInterruptSite(ctx).InterruptID()
	}
	var pend gatePending
	byPosition := inNode && pos != ""
	sessionHandles := !(a.humanGate && inNode) // gate 模式在节点里不用 session 上的句柄（见上）
	var sendParts a2atypes.ContentParts        // 非 nil 时直接发它（按 interruptId 的答复），不再由 sendMsg 转
	// handleWaiting：session 上的句柄是在远端等人（input-required）时留下的——这次的输入是答复，续之前要对账；不是（上一次
	// 的流停在远端还在办的状态上就断了）时这次是新的请求（见下面对账那一段）。seenArtifacts：问人那一刻任务里各 artifact 的状态。
	handleWaiting := false
	var seenArtifacts []seenArtifact
	if !sessionHandles {
		taskID = ""
	} else if taskID != "" {
		handleWaiting = a.loadHandleWaiting(session)
		seenArtifacts = a.loadSeenArtifacts(session)
		if a.loadAuthWait(session) {
			// 上一次停在远端要授权上、句柄在 session 上（不在任何节点执行里）：这次的输入是人对那一问的答复。标记在
			// 收到远端回应之后才清（见下）：重新挂上在网络层失败、调用方超时时它留着，下一次 Run 照样重新挂上
			if authRefused(input) {
				a.cancelRemoteTask(ctx, taskID)
				a.clearIDs(session)
				return nil, a.authRefusedErr()
			}
			reattach = true
		} else if ids := a.loadPendingIDs(session); len(ids) > 1 {
			// 续一个远端挂着多问的任务：调用方（host LLM / 业务）按约定给的是 interruptId → 答复的 JSON 对象时，
			// 换成每问一个 DataPart。远端挂多问时只接受按 interruptId 的答复，一条文本会被请重答。
			sendParts = answerPartsFromJSON(input, ids)
		}
	}
	if byPosition {
		if p, ok := compose_v2.GetStepResult[gatePending](ctx, gateStepKey(pos)); ok && p.TaskID != "" {
			pend = p
			seenArtifacts = p.SeenArtifacts
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
					if p.Auth {
						return nil, authInterrupt(id, p.Question)
					}
					return nil, gateInterrupt(id, p.Question, p.Items)
				}
				taskID, sendMsg = p.TaskID, ans
				if p.Auth {
					if authRefused(ans) {
						a.cancelRemoteTask(ctx, p.TaskID)
						if err := compose_v2.SaveStepResult(ctx, gateStepKey(pos), gatePending{}); err != nil {
							return nil, fmt.Errorf("a2a: remote agent %q: clear the refused authorization: %w", a.card.Name, err)
						}
						return nil, a.authRefusedErr()
					}
					reattach = true
				}
			}
			contextID = firstNonEmpty(p.ContextID, contextID)
		}
	}

	var (
		result       = newRemoteOutput(a.name) // 远程应答的文本与非文本 part（raw/url/data → File/Data block）
		lastState    a2atypes.TaskState
		sawState     bool // 流里给过任务状态（Task / 状态更新事件），区分「状态认不出」与「远端只回了消息」
		newTaskID    string
		newContextID string
		statusMsg    string
		statusMsgID  string                // 最后一条状态消息的 ID（记进在等的那一问，对账时据它认出远端回应了没有）
		statusParts  a2atypes.ContentParts // 最后一条状态消息的 parts（input-required 时从里面取挂起清单）
	)
	// 事件从哪来：续一个在等带外授权的任务、人说已经授权 ⟹ 重新挂上它；否则发一条消息（完整多模态：sendMsg 的
	// Content + ContentBlocks → A2A parts（text/raw/url/data））。
	var (
		msg    *a2atypes.Message
		source iter.Seq2[a2atypes.Event, error]
		method = "message_stream"
	)
	// recovered：事件来自对账取回的快照、或重新挂上的任务，而不是这次发出的消息的回应。快照里的 artifact 是任务此刻的全文，
	// 正常续任务的流只收到这一轮的：问人之前已有的内容不算这一轮的产出，按 seenArtifacts 只取之后多出来的（见
	// remoteOutput.taskArtifactSince）。重新挂上之后陆续到达的增量本来就只含新内容，照常收。
	// a2a 跨进程 trace（WithObservability 启用）：建 invoke_agent CLIENT span，覆盖这次调用跟远端的全部交互——续任务前的
	// 对账（取任务、轮询）、发消息、读流到终态（defer 按 retErr 收尾；远端要更多输入不算失败）。开在对账之前：对账失败提前
	// 返回时这次远端调用也要在 trace 上（曾经开在对账之后，对账出错时整个调用不见了，审计实测）。方法等交互方式定下再补。
	// span 用远程 AgentCard.Name（远程声明的真实 agent 名），而非 a.name（host 端 WithName 可能设的本地占位名）——trace 上
	// 一眼看出实际调的是哪个远程 agent。a.card 在 NewRemoteAgentFromCard 时必非 nil。
	var cliSpan *observability.A2AClientSpan
	if a.obs != nil {
		ctx, cliSpan = a.obs.StartA2AClientSpan(ctx, a.card.Name, method, cardTransport(a.card), contextID, taskID)
		defer func() { cliSpan.End(ctx, retErr) }()
	}
	recovered := false
	if reattach {
		source, method, recovered = a.reattach(ctx, taskID), "subscribe_task", true
	} else {
		parts := sendParts
		if parts == nil {
			parts = MessageToA2AParts(sendMsg)
		}
		msg = buildMessageFromParts(parts, taskID, contextID)
		answering := byPosition && pend.TaskID != "" && pend.TaskID == taskID
		switch {
		case answering || handleWaiting:
			// 续一个在等人的远端任务：先查它此刻的状态再决定发不发（见 reconcileTask）。答复人那一问的消息 ID 按那一问派生，
			// 重发同一个答复是同一个 ID，对账时据它认出远端收没收到。
			sentID, questionID := "", ""
			if answering {
				sentID, questionID = gateMessageID(pos, pend.TaskID, pend.Round), pend.QuestionMsgID
				msg.ID = sentID
			}
			snap, how, rerr := a.reconcileTask(ctx, taskID, sentID, questionID)
			switch how {
			case reconcileSnapshot:
				msg, method, recovered = nil, "get_task", true
				source = func(yield func(a2atypes.Event, error) bool) { yield(snap, nil) }
			case reconcileResubscribe:
				msg, recovered = nil, true
				source, method = a.reattach(ctx, taskID), "subscribe_task"
			case reconcileGone:
				// 远端没有这个任务了：清掉句柄与在等的那一问，之后再跑发原始请求（同远端任务失败）
				if answering {
					if err := compose_v2.SaveStepResult(ctx, gateStepKey(pos), gatePending{}); err != nil {
						return nil, fmt.Errorf("a2a: remote agent %q: clear the question of a vanished task: %w", a.card.Name, err)
					}
				}
				if sessionHandles {
					a.clearIDs(session)
				}
				return nil, fmt.Errorf("a2a: remote agent %q no longer has task %s; the request starts a new task when run again", a.card.Name, taskID)
			case reconcileInFlight:
				// 不重发（远端收下了）、不当成新的一问；记录留着，再跑时接着对账。等的时候调用方停下了就交出它的错
				if cerr := ctx.Err(); cerr != nil {
					return nil, fmt.Errorf("a2a: remote agent %q: waiting for task %s to respond: %w", a.card.Name, taskID, context.Cause(ctx))
				}
				return nil, fmt.Errorf("a2a: remote agent %q received the answer for task %s but has not responded yet; run again to pick up its response", a.card.Name, taskID)
			case reconcileUnknown:
				// 查不到远端此刻的状态，又是在答人那一问：不盲发（远端可能已经收下、改问了别的，旧答复会被当成新那一问的
				// 答复），记录留着，再跑时接着对账
				return nil, fmt.Errorf("a2a: remote agent %q: check the state of task %s before answering it: %w", a.card.Name, taskID, rerr)
			}
		case sessionHandles && taskID != "":
			// session 上的句柄不是在远端等人时留下的（上一次的流停在远端还在办的状态上就断了）：这次是新的请求，不是答复。
			// 远端那个任务已经结束就当新任务发、只留会话线程；没结束照旧发给它（规范允许远端在任务没结束时收追加的消息）。
			// 曾经一律按答复对账：远端已经办完时这次的请求一个字没发出去，交出的是上一个请求的结果（err == nil，审计实测）。
			if t, err := a.client.GetTask(ctx, &a2atypes.GetTaskRequest{ID: a2atypes.TaskID(taskID)}); (err == nil && t != nil && t.Status.State.Terminal()) ||
				errors.Is(err, a2atypes.ErrTaskNotFound) {
				a.clearIDs(session)
				taskID = ""
				msg = buildMessageFromParts(parts, "", contextID)
			}
		}
	}

	// 定下了怎么跟远端交互（发消息 / 取任务快照 / 重新挂上）：方法补进 span；要发消息时把 span 的 trace context
	// （W3C traceparent / tracestate / baggage）写进出站 Message.metadata，远端的 span 树挂在它下面。重新挂上、取快照时
	// 没有外发的消息，不注入。
	if a.obs != nil {
		cliSpan.SetMethod(method)
		if msg != nil {
			if msg.Metadata == nil {
				msg.Metadata = map[string]any{}
			}
			a.obs.InjectToMetadata(ctx, msg.Metadata)
		}
	}
	if msg != nil {
		source = a.client.SendStreamingMessage(ctx, &a2atypes.SendMessageRequest{Message: msg})
	}

	// 任务停在等人的状态上（input-required / auth-required）就不再读流：有的服务端只在 input-required 上关流（a2a-go 的
	// a2asrv 到了 auth-required 不关、等着带外的凭证），等它关流会一直等到超时；auth-required 的后续经 reattach 取。
stream:
	for ev, e := range source {
		if e != nil {
			return nil, fmt.Errorf("a2a: send to remote agent %q: %w", a.card.Name, e)
		}
		switch v := ev.(type) {
		case *a2atypes.Message:
			result.add(ctx, v.Parts, true)
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
			sawState = true
			if v.Status.Message != nil {
				statusMsg = a2aPartsText(v.Status.Message.Parts)
				statusMsgID = v.Status.Message.ID
				statusParts = v.Status.Message.Parts
			}
			for _, art := range v.Artifacts {
				if art == nil {
					continue
				}
				if recovered {
					result.taskArtifactSince(ctx, art.ID, art.Parts, findSeen(seenArtifacts, string(art.ID)))
					continue
				}
				result.taskArtifact(ctx, art.ID, art.Parts)
			}
			if waitsForInput(lastState) {
				break stream
			}
		case *a2atypes.TaskStatusUpdateEvent:
			if v.TaskID != "" {
				newTaskID = string(v.TaskID)
			}
			if v.ContextID != "" {
				newContextID = v.ContextID
			}
			lastState = v.Status.State
			sawState = true
			if v.Status.Message != nil {
				statusMsg = a2aPartsText(v.Status.Message.Parts)
				statusMsgID = v.Status.Message.ID
				statusParts = v.Status.Message.Parts
			}
			if waitsForInput(lastState) {
				break stream
			}
		case *a2atypes.TaskArtifactUpdateEvent:
			if v.Artifact != nil {
				result.artifactUpdate(ctx, v.Artifact.ID, v.Artifact.Parts, v.Append)
			}
		}
	}

	if lastState == a2atypes.TaskStateUnspecified && sawState {
		// 流里给过任务状态、SDK 却认不出（规范后来加的、厂商扩展的状态值都映射成 Unspecified）：按「流停在中间状态上」处理
		// （下面 default 那一档），不跟「远端只回了一条消息、没有任务」混成已完成。
		lastState = unrecognizedTaskState
	}
	auth := lastState == a2atypes.TaskStateAuthRequired
	if sessionHandles && !auth {
		// 远端回应了、不再停在授权上：session 上的授权等待标记清掉。曾经在重新挂上之前就清，一次超时之后人的答复被当成
		// 消息发给远端任务，执行器还在跑时回「任务还在执行」、跑完了回 invalid params，之后每一次都这样（审计实测）
		a.storeAuthWait(session, false)
	}
	items := PendingQuestionsFromParts(statusParts)
	if auth {
		// 授权请求问的是「去带外完成授权、然后接着办」，不是远端挂着的哪几问
		items = nil
	}
	// 这一问交给真人：审批请求（AUTH_REQUIRED，或挂起清单里标了审批的问题）一律如此——宿主模型不能替人批准远端的
	// 操作；普通追问看 WithHumanGate
	toHuman := waitsForInput(lastState) && (a.humanGate || auth || anyApproval(items))
	q := firstNonEmpty(statusMsg, "the remote agent needs more information to proceed")
	if auth {
		q = firstNonEmpty(statusMsg, "the remote agent needs authorization to proceed") + "\n(" + authAnswerHint + ")"
	}
	// 记录只描述「这一位上远端有一问在等真人答」。这次没有再交给真人（远端给了结局、转成交给宿主模型的普通追问、
	// 流意外结束）就清掉，之后再跑发原始请求；交给真人时下面按新的一问重记。只在收到远端的回应之后清——发送在网络层
	// 失败时（上面早退）记录留着，续跑拿回同一个答案续同一个任务。曾经只在「远端不再等人」时清：批准之后远端改问
	// 一个交给宿主模型的普通追问，记录还在，节点再跑时把人的批准又发给了远端、被当成那个追问的答复（实测）
	if byPosition && pend.TaskID != "" && !toHuman {
		if err := compose_v2.SaveStepResult(ctx, gateStepKey(pos), gatePending{}); err != nil {
			return nil, fmt.Errorf("a2a: remote agent %q: clear the answered question: %w", a.card.Name, err)
		}
	}
	if !sessionHandles {
		// 会话线程标识跨任务存活（见 clearIDs）；远端任务的句柄在 gate 模式下只记在调用位置上
		a.storeIDs(session, "", newContextID)
	}

	switch lastState {
	case a2atypes.TaskStateInputRequired, a2atypes.TaskStateAuthRequired:
		if toHuman {
			// 这一问属于哪个远端任务：本次响应里给的，没给就是刚才续的那个
			askTask := firstNonEmpty(newTaskID, taskID)
			if askTask == "" {
				// 不合协议（input-required 是任务的状态，必带 taskID）。抛中断的话人答了也续不上：续跑时没有
				// 待续的任务，原始请求再发一遍、远端再要人，同一个问题一遍遍重问。
				return nil, fmt.Errorf("a2a: remote agent %q asked for input without a task id, the answer could not be routed back to it", a.card.Name)
			}
			if !inNode {
				// 不在节点执行里：句柄记在 session 上，中断没有 ID（没有位置可派生）。挂多问时业务用同一个
				// session 再 Run 一次、输入给 interruptId → 答复的 JSON 对象（见 answerPartsFromJSON）；要授权时输入
				// 人的答复（deny 取消远端任务，别的重新挂上它，见 authRefused）。
				a.storeIDs(session, askTask, newContextID)
				a.storePendingIDs(session, items)
				a.storeAuthWait(session, auth)
				a.storeHandleWaiting(session, true, a.artifactStates(ctx, askTask))
				if auth {
					return nil, authInterrupt("", q)
				}
				if len(items) > 1 {
					q += "\n" + jsonAnswerHint
				}
				return nil, gateInterrupt("", q, items)
			}
			if !byPosition {
				// 引擎钩子、条件边里：没有提问位置，抛中断也接不住。调用放错了位置，重跑必然一样：终局失败（不重试、
				// 清掉存档）。普通错误会被外面的重试一遍遍重跑，每次多开一个停在要人上、没人去续的远端任务（实测）。
				// 远端那个在等人的任务没人会去续，尽力取消掉
				a.cancelRemoteTask(ctx, askTask)
				return nil, fmt.Errorf("%w: a2a: remote agent %q asked for input or approval inside an engine hook or edge condition (ns=%q), where no interrupt can be caught; call it from a node body, a tool or an orchestration member instead",
					compose_v2.ErrRunAborted, a.card.Name, compose_v2.GetGraphNS(ctx))
			}
			// 同一个远端任务里的第几问：续着的那个任务又来要人就是下一问，新任务从第 1 问数起
			round := 1
			if pend.TaskID == askTask {
				round = pend.Round + 1
			}
			// 记的是已经发生的事：远端这个任务在等人答这一问
			rec := gatePending{TaskID: askTask, ContextID: firstNonEmpty(newContextID, contextID), Round: round, Question: q, Items: items, Auth: auth,
				QuestionMsgID: statusMsgID, SeenArtifacts: a.artifactStates(ctx, askTask)}
			if err := compose_v2.SaveStepResult(ctx, gateStepKey(pos), rec); err != nil {
				return nil, fmt.Errorf("a2a: remote agent %q: record the pending question: %w", a.card.Name, err)
			}
			// 真人 HITL：抛 InterruptError 骑 host cp（续跑时按同一个身份取回人的答复、续这个远端任务）。远端挂多问时
			// 每一问各抛一个提问，人分别作答。
			id := gateQuestionID(pos, askTask, round)
			if auth {
				return nil, authInterrupt(id, q)
			}
			if len(items) > 1 {
				return nil, gateItemsInterrupt(pos, askTask, round, items)
			}
			return nil, gateInterrupt(id, q, items)
		}
		// 远程要更多输入——存 taskID 供续跑同一 task
		a.storeIDs(session, newTaskID, newContextID)
		a.storePendingIDs(session, items)
		a.storeHandleWaiting(session, true, a.artifactStates(ctx, firstNonEmpty(newTaskID, taskID)))
		// 默认：把追问当输出返回——host BasicAgent 的 LLM 看到后补信息再调一次（续同一 task）
		if len(items) > 1 {
			return schema.AssistantMessage(fmt.Sprintf(
				"[input_required] remote agent %q is waiting for answers to several questions:\n%s\n(%s)",
				a.card.Name, q, jsonAnswerHint)), nil
		}
		return schema.AssistantMessage(fmt.Sprintf(
			"[input_required] remote agent %q needs more information: %s\n(Provide it by calling this tool again with the additional info.)",
			a.card.Name, q)), nil
	case a2atypes.TaskStateCompleted, a2atypes.TaskStateUnspecified: // Unspecified：远端只回了消息、没有任务
		if sessionHandles {
			a.clearIDs(session)
		}
		return assistantWithBlocks(firstNonEmpty(result.String(), statusMsg), result.Blocks()), nil
	case a2atypes.TaskStateFailed, a2atypes.TaskStateRejected, a2atypes.TaskStateCanceled:
		if sessionHandles {
			a.clearIDs(session)
		}
		return nil, fmt.Errorf("a2a: remote agent %q task ended %s: %s", a.card.Name, lastState, firstNonEmpty(statusMsg, "(no detail)"))
	default:
		// working/submitted 但流意外结束——存 ID 供续跑，返已聚合内容。合规 server 不会以它们结束流——仅连接异常断开
		// 才落这里（AUTH_REQUIRED 是挂起态，上面按审批请求处理）。best-effort 返已收内容，
		// 但部分内容伪装完整答案对业务是静默错——warn 给观测信号（"什么算成功"防御）。
		// 节点执行里的 gate 不留句柄：这个任务没在等人，下一次调用发它自己的请求。
		slog.WarnContext(ctx, "a2a: remote stream ended on non-terminal state (partial content returned)",
			"agent", a.card.Name, "state", string(lastState))
		if sessionHandles {
			a.storeIDs(session, newTaskID, newContextID)
			a.storeHandleWaiting(session, false, nil)
		}
		return assistantWithBlocks(result.String(), result.Blocks()), nil
	}
}

// reconcileOutcome 是续远端任务之前对账的结论。
type reconcileOutcome int

const (
	reconcileSend        reconcileOutcome = iota // 照常发
	reconcileSnapshot                            // 不发：按查到的这份快照收尾
	reconcileResubscribe                         // 不发：远端已经在办，重新挂上读结果
	reconcileGone                                // 远端没有这个任务了
	reconcileInFlight                            // 不发：远端收下了这次的答复、等了一阵还没回应
	reconcileUnknown                             // 不发：查不到远端此刻的状态，又是在答人那一问
)

// reconcileTask 在续一个在等人的远端任务之前查它此刻的状态（A2A GetTask）。上一次续它时远端收下答复、办完了、只是响应在路上
// 丢了，句柄与在等的那一问按设计留着（网络失败要能续同一个任务），不对账的话之后每次都去续一个已结束的任务、远端每次都拒；
// 远端收下答复又问了新的一问时，重发的旧答复会被当成新那一问的答复（均审计实测）。
//
//   - 已结束：不发，按快照收尾——完成就取回结果，失败、取消、拒绝同直接收到那个结局。
//   - 在办（submitted / working）：它在等人时不会自己动起来，只能是收下了答复：不重发，重新挂上读结果。
//   - 在等人、sentID 非空（答复人那一问，ID 按那一问派生）、任务历史里有它：远端收下了答复，重发的话会被当成下一问的答复。
//     远端回应过（历史里它后面还有东西——服务端更新状态时把旧的状态消息移进历史；或状态消息已不是我们答的那一问 questionID）
//     ⟹ 在问新的，按快照收尾、问人新的那一问；没回应过（执行器发出第一个状态之前任务一直停在原来那一问上）：轮询等它回应，
//     最多 authReattachGrace，回应了按上面几条处置，到时仍没回应报 reconcileInFlight——不重发：远端可能只是慢，在不去重的
//     服务端上重发会让它办两遍。历史里没有它（没收到，或远端不留历史）照常发。
//   - 远端说没有这个任务：reconcileGone。
//   - 查不到状态：sentID 非空时报 reconcileUnknown（不盲发，理由同上）；远端不支持 GetTask，或这次续的是 session 上的句柄
//     （sentID 为空、答复认不出是哪一条）时照常发，跟没有这一步时一样。
func (a *RemoteAgent) reconcileTask(ctx context.Context, taskID, sentID, questionID string) (*a2atypes.Task, reconcileOutcome, error) {
	id := a2atypes.TaskID(taskID)
	task, err := a.client.GetTask(ctx, &a2atypes.GetTaskRequest{ID: id})
	if err != nil {
		switch {
		case errors.Is(err, a2atypes.ErrTaskNotFound):
			return nil, reconcileGone, nil
		case sentID != "" && !deterministicTaskQueryErr(err):
			return nil, reconcileUnknown, err
		}
		return nil, reconcileSend, nil
	}
	if task == nil {
		return nil, reconcileSend, nil
	}
	at := slices.IndexFunc(task.History, func(m *a2atypes.Message) bool { return m != nil && m.ID == sentID })
	if !waitsForInput(task.Status.State) || sentID == "" || at < 0 {
		return task, settledOutcome(task), nil
	}
	responded := func(t *a2atypes.Task) bool {
		if questionID != "" && statusMessageID(t) != questionID {
			return true
		}
		i := slices.IndexFunc(t.History, func(m *a2atypes.Message) bool { return m != nil && m.ID == sentID })
		return i >= 0 && i < len(t.History)-1
	}
	if responded(task) {
		return task, reconcileSnapshot, nil
	}
	task, err = a.pollTask(ctx, id, func(t *a2atypes.Task) bool {
		return !waitsForInput(t.Status.State) || responded(t)
	}, authReattachGrace)
	if err != nil || (waitsForInput(task.Status.State) && !responded(task)) {
		return nil, reconcileInFlight, nil
	}
	if waitsForInput(task.Status.State) {
		return task, reconcileSnapshot, nil
	}
	return task, settledOutcome(task), nil
}

// deterministicTaskQueryErr 判 GetTask 的错误是不是「再查也一样」：远端不支持这个操作、请求本身被拒（参数、鉴权、版本）。
// 这类错误下等也查不到，照常发（跟没有对账这一步时一样）；传输错误、服务端内部错误这类瞬时的才不盲发、等下一次再查——
// 否则 GetTask 一直被拒的远端上人的答复永远发不出去（审计推断）。
func deterministicTaskQueryErr(err error) bool {
	// ErrParseError 不算：它只说明「这次的响应解不开」，分不出是不是每次都一样——JSON-RPC 下是服务端回 -32700（我们发的是良构的，
	// 多半是传输途中坏了），REST 下是 A2A 形状的错误体后面带着多余的数据。拿不准时不盲发：规范只说远端「可以」按 messageId
	// 去重，Go / Python / Java 三家参考服务端执行都不去重（本仓依赖的 a2a-go 只是不把同一个 ID 的消息重复记进历史、执行器照跑；
	// 2026-06 的 a2a-python / a2a-java），照发就是远端把同一个答复再处理一遍；不发是看得见的错误、有上界。
	//
	// 鉴权层、网关给的不是 A2A 形状的拒绝：REST 下客户端按 HTTP 状态码映射（a2a-go v2.7.0 起 401 → ErrUnauthenticated、
	// 403 → ErrUnauthorized、404 / 405 / 501 → ErrMethodNotFound），照常发；JSON-RPC 下非 200 只给一句文本、拿不到状态码，
	// 按瞬时处理、一直卡住（见 doc/code/todo.md「a2a 对账：拒绝 GetTask 的确定性错误」）。
	for _, target := range []error{a2atypes.ErrMethodNotFound, a2atypes.ErrUnsupportedOperation,
		a2atypes.ErrInvalidRequest, a2atypes.ErrInvalidParams, a2atypes.ErrVersionNotSupported, a2atypes.ErrExtensionSupportRequired,
		a2atypes.ErrUnauthenticated, a2atypes.ErrUnauthorized} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// settledOutcome 是不涉及「这次的答复送到没有」时按任务状态的处置：已结束按快照收尾，在办重新挂上，其余照常发。
func settledOutcome(task *a2atypes.Task) reconcileOutcome {
	switch st := task.Status.State; {
	case st.Terminal():
		return reconcileSnapshot
	case st == a2atypes.TaskStateSubmitted || st == a2atypes.TaskStateWorking:
		return reconcileResubscribe
	}
	return reconcileSend
}

// statusMessageID 取任务当前状态消息的 ID（没有状态消息时为空）。
func statusMessageID(t *a2atypes.Task) string {
	if t.Status.Message == nil {
		return ""
	}
	return t.Status.Message.ID
}

// gateMessageID 是答复人那一问（位置 pos、远端任务 taskID、第几问 round，见 gateQuestionID）的那条消息的 ID：重发同一个
// 答复时 ID 不变，对账时据它认出远端收没收到（见 reconcileTask）。
func gateMessageID(pos, taskID string, round int) string {
	return "crew-gate-" + gateQuestionID(pos, taskID, round)
}

// waitsForInput 判断远端任务停在等人的状态上（input-required / auth-required）。
func waitsForInput(st a2atypes.TaskState) bool {
	return st == a2atypes.TaskStateInputRequired || st == a2atypes.TaskStateAuthRequired
}

// anyApproval 判断挂起清单里有没有工具审批。
func anyApproval(items []PendingQuestion) bool {
	for _, it := range items {
		if it.Approval {
			return true
		}
	}
	return false
}

// callPhase 是 RemoteAgent 在节点执行里被调起时这次调用自己的相位名。远端要人时提问在这个相位里问、不占调用它的那段
// 代码的序号：调用方按进度跳过这次已完成的调用时，后面的提问不挪位（共用序号时，跳过它会让后面那一问挪到它的位置上、
// 拿到别的问题的答案——节点体与被交接的业务壳里各实测过一次）。
//
// 名字决定续跑时认不认得出「同一次调用」，分两种：
//   - 节点体、工具函数体里（外层相位名为空）：哪个远端 + 发出去的请求内容 + 同样的请求在这次执行里第几次
//     （compose_v2.NextSubCallIndex）。跳过别的调用不影响这一次找回自己挂着的那一问；内容完全相同的几次只能按次序
//     区分，别按进度单独跳过其中一次（同 agent_v2.RunSubAgent）。⚠️ 请求在续跑之间变了（带请求号、时间戳、节点里现调
//     模型拼的）就是另一次调用：原始请求作为新任务重发、远端要人时再问一次——把拼好的请求记下来
//     （compose_v2.SaveStepResult），或经 agent_v2.RunSubAgent + WithCallKey 调。
//   - 别人给的相位里（框架交接给它或包着它的业务壳、图与工具的钩子）：外层相位名 + 哪个远端 + 在这个相位里第几次，不看
//     请求内容——交过来的输入在续跑之间可以合法地变（AgentAsTool 的全历史输入、RunSubAgent 带 call key），按内容分就找
//     不回挂着的那一问（实测：业务壳被交接时永不收敛）。带上外层相位名，几个钩子相位里各调一次时名字不撞。同一个相位
//     里对同一个远端的几次调用按次序区分，别按进度单独跳过其中一次。
//
// 两种名字里都有远端名（a.name）：续跑之间它要不变（配 WithName；没配时取 card 名，服务发现挑到 card 名不同的实例时
// 就变了）。变了的话挂着的那一问找不回来，原始请求作为新任务重发、远端要人时再问一次人，不会拿错答案。
func (a *RemoteAgent) callPhase(ctx context.Context, input *schema.Message) string {
	if outer := compose_v2.SubCallScope(ctx); outer != "" {
		key := outer + "\x00a2a\x00" + a.name
		return key + "#" + strconv.Itoa(compose_v2.NextSubCallIndex(ctx, key))
	}
	content, err := json.Marshal(MessageToA2AParts(input))
	if err != nil {
		// 数据块里有编码不了的值（NaN 之类）：退到只按文本区分，仍然稳定
		content = []byte(input.Content)
	}
	sum := sha256.Sum256(content)
	key := "a2a\x00" + a.name + "\x00" + hex.EncodeToString(sum[:16])
	return key + "#" + strconv.Itoa(compose_v2.NextSubCallIndex(ctx, key))
}

// authReattachGrace 是人说已经授权之后、远端仍停在 AUTH_REQUIRED 上时最多等多久：过了还没离开就交出任务、再问一次人
// （授权可能没有成），不一直挂到调用方超时。远端离开 AUTH_REQUIRED 之后不再计这个时间，等它办完。
var authReattachGrace = 30 * time.Second

// authPollInterval 是等远端离开 AUTH_REQUIRED、以及远端不支持流式时轮询任务状态的间隔。
var authPollInterval = 500 * time.Millisecond

// unrecognizedTaskState 标记流里给过、但 SDK 认不出的任务状态（consume 按流停在中间状态上处理）。
const unrecognizedTaskState a2atypes.TaskState = "unrecognized"

// stalledSubscribeGrace 是订阅在任务还没结束、也不在等人时就关了流之后，再轮询多久看它有没有动。
var stalledSubscribeGrace = 30 * time.Second

// reattach 重新挂上一个在等带外授权的远端任务，事件交给 consume 的读流循环（规范 §7.6.2：客户端订阅或轮询 GetTask
// 取后续，不需要再发消息）：
//
//  1. 轮询到任务离开 AUTH_REQUIRED，最多等 authReattachGrace；到时仍停在那儿就交出它（consume 据此再问一次人）。
//  2. 已经结束、或停在别的等人状态上：交出它。
//  3. 还在跑：远端支持流式就订阅、从它当时的快照接着读（规范 §3.1.6）；不支持就接着轮询，到结束或停在等人状态上为止。
//  4. 订阅在任务没结束、也不在等人时关了流：远端没有执行在跑这个任务（重启之后持久 store 里留着的 working 任务——
//     a2a-go 自 v2.7.0、a2a-js 的服务端这时交出快照就正常关流）。再轮询 stalledSubscribeGrace，动了就交出它，
//     没动就报错、句柄留着。不当成结束：consume 会把已聚合的内容（往往是空的）当成功答复交出去（审计实测）。
//
// 先轮询再订阅：订阅的第一个事件是任务当时的快照，订阅时它还停在 AUTH_REQUIRED 上的话分不清是旧的还是新的一次要授权。
func (a *RemoteAgent) reattach(ctx context.Context, taskID string) iter.Seq2[a2atypes.Event, error] {
	return func(yield func(a2atypes.Event, error) bool) {
		id := a2atypes.TaskID(taskID)
		task, err := a.pollTask(ctx, id, func(t *a2atypes.Task) bool {
			return t.Status.State != a2atypes.TaskStateAuthRequired
		}, authReattachGrace)
		if err != nil {
			yield(nil, err)
			return
		}
		st := task.Status.State
		if st.Terminal() || waitsForInput(st) {
			yield(task, nil)
			return
		}
		if !a.card.Capabilities.Streaming {
			task, err = a.pollTask(ctx, id, func(t *a2atypes.Task) bool {
				return t.Status.State.Terminal() || waitsForInput(t.Status.State)
			}, 0)
			if err != nil {
				yield(nil, err)
				return
			}
			yield(task, nil)
			return
		}
		got := false
		last := st
		for ev, e := range a.client.SubscribeToTask(ctx, &a2atypes.SubscribeToTaskRequest{ID: id}) {
			if e != nil {
				if !got {
					// 取状态之后、订阅之前任务结束了：订阅对已结束的任务报错，再取一次终态
					if t, gerr := a.client.GetTask(ctx, &a2atypes.GetTaskRequest{ID: id}); gerr == nil && t.Status.State.Terminal() {
						yield(t, nil)
						return
					}
				}
				yield(nil, e)
				return
			}
			got = true
			switch v := ev.(type) {
			case *a2atypes.Task:
				last = v.Status.State
			case *a2atypes.TaskStatusUpdateEvent:
				last = v.Status.State
			}
			if !yield(ev, nil) {
				return
			}
		}
		if last.Terminal() || waitsForInput(last) {
			return
		}
		// 第 4 条：流关了、任务还停在中间状态上
		task, err = a.pollTask(ctx, id, func(t *a2atypes.Task) bool {
			return t.Status.State.Terminal() || waitsForInput(t.Status.State)
		}, stalledSubscribeGrace)
		if err != nil {
			yield(nil, err)
			return
		}
		if st := task.Status.State; st.Terminal() || waitsForInput(st) {
			yield(task, nil)
			return
		}
		yield(nil, fmt.Errorf("remote task %s is %s but the remote is not working on it: the subscription closed without a final state and the task did not change within %s",
			taskID, task.Status.State, stalledSubscribeGrace))
	}
}

// pollTask 每隔 authPollInterval 取一次远端任务（第一次立刻取），直到 done 对它为真、或过了 limit（0 不限，只受
// ctx 约束），返回最后一次取到的任务。
func (a *RemoteAgent) pollTask(ctx context.Context, id a2atypes.TaskID, done func(*a2atypes.Task) bool, limit time.Duration) (*a2atypes.Task, error) {
	var deadline time.Time
	if limit > 0 {
		deadline = time.Now().Add(limit)
	}
	for {
		task, err := a.client.GetTask(ctx, &a2atypes.GetTaskRequest{ID: id})
		if err != nil {
			return nil, err
		}
		if done(task) || (limit > 0 && !time.Now().Before(deadline)) {
			return task, nil
		}
		timer := time.NewTimer(authPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// authAnswerHint 告诉人怎么答一次授权请求。
const authAnswerHint = `complete the authorization the remote agent asked for, then reply to continue; reply "deny" to refuse it, which cancels the remote task`

// authRefused 判断人对一次授权请求的答复是不是明确的拒绝（agent_v2.NormalizeApprovalAnswer 认作 deny）。
//
// 其余答复——批准词、「已授权」「好了」这类认不出的话、空串——一律当作「已经在带外完成授权、接着办」：重新挂上远端
// 任务只读不写，答错了也只是接着等。拒绝时取消远端任务，不把拒绝作为消息发过去：规范允许在 AUTH_REQUIRED 期间发
// 消息协商、更正或拒绝（§7.6.2），但执行器在这期间一直在跑的服务端不收（a2a-go 的 a2asrv 回「任务还在执行」，实测），
// 取消是各家都支持的停法。
func authRefused(ans *schema.Message) bool {
	if ans == nil {
		return false
	}
	opt, ok := agent_v2.NormalizeApprovalAnswer(ans.Content)
	return ok && opt == agent_v2.ApprovalOptionDeny
}

// authRefusedErr 是人拒绝了远端要的授权时这次调用的错误：远端任务已尽力取消。不是终局失败——在工具里调用时它变成
// 工具的失败结果、宿主模型可以换个办法；重试这次调用会新开一个远端任务、再问一次授权。
func (a *RemoteAgent) authRefusedErr() error {
	return fmt.Errorf("a2a: remote agent %q: the authorization it asked for was refused, the remote task was canceled", a.card.Name)
}

// authInterrupt 是远端要授权（AUTH_REQUIRED）时抛给宿主的那一问：标成审批，选项是批准（已经完成授权、接着办）与拒绝
// （取消远端任务），前端按审批展示。
func authInterrupt(id, question string) *compose_v2.InterruptError {
	q := &compose_v2.AskQuery{
		InterruptID: id,
		Question:    question,
		Reason:      agent_v2.ApprovalInterruptReason,
		Options:     []string{agent_v2.ApprovalOptionApprove, agent_v2.ApprovalOptionDeny},
	}
	return &compose_v2.InterruptError{Reason: q.Reason, Query: q}
}

// remoteCancelTimeout 是取消一个没人会去续的远端任务时最多等多久。
const remoteCancelTimeout = 10 * time.Second

// cancelRemoteTask 尽力取消一个在等人、但这里接不住中断的远端任务：不取消的话它一直挂着。取消失败只记日志——调用本身
// 已经要以终局错误结束，取消只是收尾。
func (a *RemoteAgent) cancelRemoteTask(ctx context.Context, taskID string) {
	if taskID == "" {
		return
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), remoteCancelTimeout)
	defer cancel()
	if _, err := a.client.CancelTask(cctx, &a2atypes.CancelTaskRequest{ID: a2atypes.TaskID(taskID)}); err != nil {
		slog.WarnContext(ctx, "a2a: cancel remote task that waits for a human failed",
			"agent", a.card.Name, "task", taskID, "err", err)
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

// handleKey 是「session 上的句柄是在远端等人时留下的」在 session 上的键（任务级，同 taskKey）；seenKey 是那时任务里已有的
// artifact（见 gatePending.SeenArtifacts）。
func (a *RemoteAgent) handleKey(s *agent_v2.RunSession) string {
	return "a2a:" + a.name + ":handle_waiting:" + s.RunID
}

func (a *RemoteAgent) seenKey(s *agent_v2.RunSession) string {
	return "a2a:" + a.name + ":seen_artifacts:" + s.RunID
}

func (a *RemoteAgent) storeHandleWaiting(s *agent_v2.RunSession, waiting bool, seen []seenArtifact) {
	if s == nil {
		return
	}
	s.SetValue(a.handleKey(s), waiting)
	s.SetValue(a.seenKey(s), seen)
}

func (a *RemoteAgent) loadHandleWaiting(s *agent_v2.RunSession) bool {
	if s == nil {
		return false
	}
	v, _ := s.GetValue(a.handleKey(s))
	waiting, _ := v.(bool) // 别人在同 key 存了别的类型：当作不在等人，按新请求发
	return waiting
}

func (a *RemoteAgent) loadSeenArtifacts(s *agent_v2.RunSession) []seenArtifact {
	if s == nil {
		return nil
	}
	v, _ := s.GetValue(a.seenKey(s))
	seen, _ := v.([]seenArtifact) // 同上：取不到就按没见过收全文
	return seen
}

// seenArtifact 是问人那一刻远端任务里一个 artifact 的状态：正文的字节数与哈希、非文本块的个数与哈希。取回快照时据此判断这一轮
// 对它做了什么——正文与块都仍以当时的内容开头就是往后追加了，只收多出来的部分；有一样对不上就是整段替换了，收全文（同正常续
// 任务时收到 append=false 的更新）。只记 ID 的话，远端跨轮往同一个 artifact 里追加时这一轮的内容整段被跳过（答复变空）；块
// 只比个数的话，正文没变、只换了数据块的替换被当成没动（同样变空，均审计实测）。
//
// 局限：远端整段替换、而新内容恰好以旧内容开头时，快照里看不出它是替换还是追加，按追加只收后缀（正常续任务时收到的是全文）。
// 快照只有最终全文，客户端消不掉这个歧义。
type seenArtifact struct {
	ID         string `json:"id"`
	TextLen    int    `json:"text_len"`
	TextHash   string `json:"text_hash"`
	Blocks     int    `json:"blocks"`
	BlocksHash string `json:"blocks_hash"`
}

// artifactStatesTimeout 是问人那一刻取 artifact 状态最多等多久。不设上限的话，卡住的远端让问人这一步挂到调用方的期限（调用方
// 没设期限就一直挂）；设得太短又会在慢但正常的远端上取不到——取不到不是无害的：之后按快照收尾、授权之后重新挂上（正常流程，
// 不需要响应丢失）时都不扣除，答复里静默带上问人之前的内容。所以跟等远端状态的另一处上限（authReattachGrace）同一量级，并且
// 只取 artifact、不取历史（HistoryLength 0）。曾经是 5 秒，审计实测慢 6 秒的远端在授权流程里就把授权前的内容收进了答复。
var artifactStatesTimeout = 30 * time.Second

// artifactStates 向远端取任务此刻各 artifact 的状态（问人那一刻调）。取不到时返回 nil：之后取回快照或授权后重新挂上时不扣除，
// 跟没有这一步一样把已有内容也收进答复（记一条告警；远端不支持 GetTask、调用方已经放弃时不记——前者本来就没有可扣的依据，
// 后者不是远端的问题）。累积不出来：每次调用的输出只有这一次的增量，不知道远端此前对同一个 artifact 是追加还是替换。
func (a *RemoteAgent) artifactStates(ctx context.Context, taskID string) []seenArtifact {
	if taskID == "" {
		return nil
	}
	qctx, cancel := context.WithTimeout(ctx, artifactStatesTimeout)
	defer cancel()
	noHistory := 0
	task, err := a.client.GetTask(qctx, &a2atypes.GetTaskRequest{ID: a2atypes.TaskID(taskID), HistoryLength: &noHistory})
	if err != nil || task == nil {
		if ctx.Err() == nil && !errors.Is(err, a2atypes.ErrMethodNotFound) && !errors.Is(err, a2atypes.ErrUnsupportedOperation) {
			slog.WarnContext(ctx, "a2a: 问人时没取到远端任务的 artifact 状态，之后按快照收尾或授权后重新挂上时，答复可能带上问人之前的内容",
				"agent", a.card.Name, "task", taskID, "err", err)
		}
		return nil
	}
	seen := make([]seenArtifact, 0, len(task.Artifacts))
	for _, art := range task.Artifacts {
		if art == nil || art.ID == "" {
			continue
		}
		t, bs := PartsToContent(art.Parts)
		seen = append(seen, seenArtifact{ID: string(art.ID), TextLen: len(t), TextHash: textHash(t), Blocks: len(bs), BlocksHash: blocksHash(bs)})
	}
	return seen
}

// findSeen 取 id 那个 artifact 问人那一刻的状态，没有时返回 nil。
func findSeen(seen []seenArtifact, id string) *seenArtifact {
	for i := range seen {
		if seen[i].ID == id {
			return &seen[i]
		}
	}
	return nil
}

// textHash 是正文的哈希（截短，只用来判断前缀是不是同一段内容）。
func textHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:16])
}

// blocksHash 是一组非文本块的哈希（按 JSON 形状，map 键已排序）。编不成 JSON 时返回空串：对不上，按整段替换收全文。
// 没有块时 nil 与空切片同一个哈希——问人时没有块记下的是 nil，取回时截出来的前缀是空切片，编出来一个是 null、一个是 []，
// 曾因此把「往没有块的 artifact 里追加了块」当成整段替换、旧正文重复收进答复（审计实测）。
func blocksHash(bs []*schema.ContentBlock) string {
	if len(bs) == 0 {
		bs = nil
	}
	raw, err := json.Marshal(bs)
	if err != nil {
		return ""
	}
	return textHash(string(raw))
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

// authKey 是「这个任务停在远端要授权上」在 session 上的键（任务级，同 taskKey）。只在不在任何节点执行里、句柄记在
// session 上时用：下一次 Run 的输入是人对那一问的答复（见 consume）。
func (a *RemoteAgent) authKey(s *agent_v2.RunSession) string {
	return "a2a:" + a.name + ":auth:" + s.RunID
}

func (a *RemoteAgent) storeAuthWait(s *agent_v2.RunSession, waiting bool) {
	if s == nil {
		return
	}
	s.SetValue(a.authKey(s), waiting)
}

func (a *RemoteAgent) loadAuthWait(s *agent_v2.RunSession) bool {
	if s == nil {
		return false
	}
	v, _ := s.GetValue(a.authKey(s))
	waiting, _ := v.(bool)
	return waiting
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
	s.SetValue(a.authKey(s), false)
	s.SetValue(a.handleKey(s), false)
	s.SetValue(a.seenKey(s), []string(nil))
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
	// Auth：远端停在 AUTH_REQUIRED 上要授权。凭证走带外通道：人拒绝时取消这个任务，其余答复都重新挂上它（见
	// reattach），不发消息。
	Auth bool `json:"auth,omitempty"`
	// QuestionMsgID 是远端问这一问的那条状态消息的 ID：续任务之前对账时，远端的状态消息还是它就是还没回应这次的答复
	// （见 reconcileTask）。
	QuestionMsgID string `json:"question_msg_id,omitempty"`
	// SeenArtifacts 是问这一问的那一刻，这个远端任务里各 artifact 的状态：对账取回快照、或重新挂上任务时，那时已有的内容不算
	// 回应这一问的产出（正常续任务的流里只有这一轮的，见 seenArtifact）。
	SeenArtifacts []seenArtifact `json:"seen_artifacts,omitempty"`
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

// cardTransport 是记在 client span 上的传输方式：名片上声明的接口只有一种协议时取它（小写，如 jsonrpc、http+json、grpc），
// 声明了多种时返回空串（不记）——客户端最终选了哪一种 a2a-go 不对外交出，按名片猜会记错。
func cardTransport(card *a2atypes.AgentCard) string {
	if card == nil {
		return ""
	}
	var binding a2atypes.TransportProtocol
	for _, ifc := range card.SupportedInterfaces {
		if ifc == nil || ifc.ProtocolBinding == "" {
			continue
		}
		if binding != "" && ifc.ProtocolBinding != binding {
			return ""
		}
		binding = ifc.ProtocolBinding
	}
	return strings.ToLower(string(binding))
}
