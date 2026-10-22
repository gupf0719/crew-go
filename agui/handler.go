package agui

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/internal/panicerr"
	"crew-go/schema"
)

// RunAgentInput 是 AG-UI client（如 CopilotKit）POST 给 server 的运行请求。
// 只解析 server 端需要的字段——state / tools / context 暂不消费。
//
// messages 按 AG-UI 1.0 视作这场对话的完整历史：最后一条 user 是这一轮的输入，它之前的 user / assistant 文本作为
// 调用方提供的前文交给 agent（agent_v2.WithCallerHistoryCtx——一次运行只认一个来源：agent 配了历史层、由它拥有这场
// 对话时以服务端为准，服务端第一次见到这场对话时把前文写进去一次；否则每个读历史的 agent 都看到这份前文）。续跑推 Run
// 时那一份（存在会话条目里），不按续跑请求的 messages 重算。
// 客户端的 system / developer 不采信（它们会成为模型的指令，而客户端能随意伪造），剥掉并告警；工具调用与结果、
// reasoning、activity 不取；最后一条 user 之后的消息不取。
type RunAgentInput struct {
	ThreadID string        `json:"threadId"`
	RunID    string        `json:"runId"`
	Messages []AGUIMessage `json:"messages"`
	// Resume 非空 = 回答上一次 HITL 中断、续跑（走 Resume 而非新 Run）。
	Resume []ResumeEntry `json:"resume,omitempty"`
}

// ResumeEntry 是一个 HITL 中断的答复——**对齐官方 AG-UI ResumeEntry 协议**
// （interruptId + status + payload），CopilotKit 等标准前端按此格式发送
// （见 ag-ui SDK types.go / TS ResumeEntrySchema）。
type ResumeEntry struct {
	// InterruptID 对应上次 RunFinished(interrupt) 里 interrupt.id。
	InterruptID string `json:"interruptId"`
	// Status 是解决状态："resolved"（已答，payload 是答案）/ "cancelled"（用户放弃）。
	// 全 cancelled → 终止 run（ResumeActionAbort）；混合 resolved/cancelled → resolved
	// 续跑、cancelled 注入取消标记。
	Status string `json:"status"`
	// Payload 是用户答复（resolved 时的答案，可能是字符串或结构化对象）。
	Payload any `json:"payload,omitempty"`
}

// AG-UI ResumeEntry.Status 取值。
const (
	ResumeStatusResolved  = "resolved"
	ResumeStatusCancelled = "cancelled"
)

// DefaultMaxSessions 是挂起会话上限默认值——超上限时淘汰一个空闲挂起会话（+ 清其 cp）
// 防长跑 OOM。淘汰按 owner 分摊、上限是软的，见 [WithMaxSessions]。
const DefaultMaxSessions = 10000

// AGUIMessage 是 AG-UI 请求里的一条消息（简化：只取 role + content 的文本）。
//
// content 可以是字符串，也可以是 ContentPart 数组（AG-UI 1.0 的多模态消息）：数组里的 text 部分按顺序以换行拼成
// Content，媒体部分（image / audio / video / document）不取——模型层还不把多模态块送给模型。activity 消息的
// content 是对象，不是对话内容，Content 留空；别的角色的 content 是对象时解码报错。
type AGUIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	// droppedParts 是 content 数组里没取的媒体部分的类型（按出现顺序），解码时填。
	droppedParts []string
}

// UnmarshalJSON 解 role 与 content：content 接受字符串、ContentPart 数组、对象与 null（见类型说明）。
func (m *AGUIMessage) UnmarshalJSON(b []byte) error {
	var raw struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	m.Role, m.Content, m.droppedParts = raw.Role, "", nil
	c := bytes.TrimSpace(raw.Content)
	switch {
	case len(c) == 0 || bytes.Equal(c, []byte("null")):
		return nil
	case c[0] == '{':
		// 只有 activity 的 content 是对象（结构化进度，不是对话内容）；别的角色是对象就是非法消息，照跑的话 user 的输入
		// 成了空串
		if raw.Role != "activity" {
			return fmt.Errorf("message content: a %s message's content must be a string or an array of content parts", raw.Role)
		}
		return nil
	case c[0] == '[':
		var parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(c, &parts); err != nil {
			return fmt.Errorf("message content: %w", err)
		}
		texts := make([]string, 0, len(parts))
		for _, p := range parts {
			if p.Type == "text" {
				texts = append(texts, p.Text)
				continue
			}
			m.droppedParts = append(m.droppedParts, p.Type)
		}
		m.Content = strings.Join(texts, "\n")
		return nil
	default:
		return json.Unmarshal(c, &m.Content)
	}
}

// AgentFactory 每个 AG-UI 请求调一次，返回处理该请求的 agent。传入的 hooks 是 AG-UI
// adapter——**必须注入 agent**（`agent_v2.WithHooks(hooks)` 或 chain 进业务 hooks），
// 否则拿不到 LLM 输出 / tool 调用、AG-UI 事件流只剩纯文本。chain 时 adapter 排在会在 OnModelEnd
// 里问人的业务钩子前面：续跑沿用那次答复时 adapter 按 hook.ModelResponseReusedKey 不再发（上一次 run
// 已经交给前端），排在后面、模型又不流式时那次没发出去、这一遍也不发。**必须返回非 nil agent**
// （返 nil 时 handler fail-fast 发 RunError(code=nil_agent)）。
//
// 为什么是工厂而非直接传 agent：AG-UI 是 per-connection SSE，每个请求的 adapter 绑定
// 各自的 HTTP response；而 crew-go 的 hook.Hooks 是构造期注入（RunOption 无 per-Run
// hook.Hooks），故每请求构造挂独立 adapter 的 agent。BasicAgent 等构造是轻量 builder，
// 每请求构造开销可接受；业务想复用重资源（model / toolkit / skillbox）在工厂闭包外
// 预建、闭包内只 new agent 引用它们即可。Resume 也走工厂新建 agent——crew-go Resume
// 靠 cp + sid 重建状态，与 agent 实例无关。
type AgentFactory func(hooks hook.Hooks) agent_v2.Agent

// Option 配置 Handler。
type Option func(*handler)

// WithCheckpointer 给 Handler 配 checkpointer——**HITL Resume 必需**（Run 中断存档、
// 续发时按 sid 重建）。不配则中断只发 RunFinished(interrupt)、前端答复无法续跑。
func WithCheckpointer(cp compose_v2.Checkpointer) Option {
	return func(h *handler) { h.cp = cp }
}

// WithSessionTTL 设挂起会话的**最长闲置时间**：超过 d 没有活动的空闲挂起会话被回收
// （连同它的 cp）。`<=0`（默认）= 不按时间回收。
//
// # 为什么需要它，以及为什么默认关
//
// [WithMaxSessions] 把挂起会话按**数量**兜住了，但没按**时间**——一个被放弃的会话
// （用户关了页面）会一直占着自己的 cp，直到表攒够上限才被挤掉。低流量部署下那可能是
// 几个月，而 cp 里装的是整条消息历史，后端持久化时那是实打实的存储。
//
// 业务自己也解决不了：会话表在 handler 内部，外面够不着。
//
// # 两段语义，分开的
//
//  1. **过期即不可用**（精确）：命中一条已超期的会话时当「不存在」处理——新请求开新会话、
//     续发返「找不到」。这一档不依赖回收什么时候真跑，所以**语义上界是准的**。
//  2. **真删是惰性的**（有延迟）：请求进来时顺手扫一遍表，最多每 TTL/4 扫一次。低流量
//     部署下回收会拖后——没请求就不扫。可以接受，因为没请求也就不新增；而且第 1 段已经
//     保证了「过期的绝不会被用上」。
//
// 拆开是有意的：把「什么时候真删」和「什么时候算过期」绑在一起的话，回收延迟就变成了
// 语义延迟——一条早该过期的会话在下次扫描之前仍然续得起来。
//
// # 为什么是惰性而不是后台 sweeper
//
// **选择**，不是被逼的：后台 goroutine 的退出点是能解的（`runtime.SetFinalizer` 挂在外层
// wrapper 上是成熟做法，也可以让 [Handler] 返一个带 `Close()` 的具体类型）。选惰性是因为
// 它不引入生命周期管理负担、也不要求调用方记得关，而这里回收精度本来就不重要。
//
// # 为什么默认关
//
// **框架判不出「多久算放弃」**：HITL 审批跨一个周末是正常的，而一个定错的 TTL 会连人带
// cp 一起清掉——那比多占一阵存储严重。数量上限已经挡住了 OOM，时间维交给知道自己业务
// 节奏的人开。
//
// 在途（busy）的会话永不回收——它正被一个请求驱动着。
//
// ⚠️ TTL 别设得太小：扫一遍是 O(n) 且**持写锁**，而 n **不以 [DefaultMaxSessions] 为界**
// ——那个上限是软的（见 [WithMaxSessions]），实际约 `max(maxSessions, 有挂起会话的租户数)
// + 在途请求数`。实测 `maxSessions=5` 下 100 个租户各挂 1 条 ⟹ 表里 100 条，20 倍。
// 多租户 + 大量被放弃的会话正是这个特性针对的形态，也正是 n 远超 maxSessions 的形态：
// 10 万租户各留一条 ⟹ TTL=1 分钟意味着每 15 秒一次**十万次**比较持锁。分钟级以下没有
// 意义——被放弃的会话不差那几分钟。
func WithSessionTTL(d time.Duration) Option {
	return func(h *handler) { h.sessionTTL = d }
}

// WithMaxSessions 设挂起会话上限（默认 DefaultMaxSessions）。超上限时淘汰一个空闲挂起
// 会话 + 清其 cp，防前端放弃中断（关页面 / 断网）导致 sessions map 长跑 OOM。
// <=0 表示不限（不推荐生产用——AG-UI 无 a2a Cancel 那样的显式释放入口）。
//
// **淘汰谁：份额最大的那个 owner 出，且到达者不赢平局。** 单租户部署（resolver 返固定
// 值）下只有一个分区，这就是全局 LRU——最旧的空闲会话被淘汰，跟没有 owner 维时一样，
// **可用会话数不打折**。多租户下，一个租户狂开会话挤掉的恒是它自己的；开个新会话就想
// 挤掉别人挂起的 HITL 会话（连 cp 一起清）这条路是堵死的。
//
// ⚠️ **这个上限因此是软的**，两种情况下表会暂时超出它：
//
//   - 该出账的那个分区没有空闲会话可淘汰（典型：到达者只有刚插入的那条在途 entry）；
//   - 要淘汰的会话正在途（busy）——在途的动不得，它正被一个请求驱动着。
//
// 换算下来实际占用约是 `max(maxSessions, 有挂起会话的租户数) + 在途请求数`。硬上限
// 要靠「满了就拒绝新请求」来换，而那等于把一个安静的跨租户 DoS 换成一个吵闹的：任何
// 租户填满表就能让其他租户开不出新会话。在途那部分本来就受服务端并发数约束（每条在途
// entry 背后有一个 HTTP handler goroutine），真正会无限攒的是**被放弃的挂起会话**，
// 而那部分仍按 owner 分摊受限。
func WithMaxSessions(n int) Option {
	return func(h *handler) { h.maxSessions = n }
}

// WithOwnerResolver 设「这次请求属于谁」的解析函数——**必配**，构造期缺失 panic。
//
// AG-UI 的 threadId 由**客户端**生成、任何人都能递：它是续接标识，不是授权凭据。
// 不带 owner 维时，谁猜中（或抓包看到）threadId 谁就能续别人的会话——读到挂起的
// 提问、替对方回答、继承已转交的控制权。owner 由服务端从**已认证的凭据**里解析
// （认证中间件放进 r.Context() 的主体），进会话键与下游存储键，不是一道闸：
// 同 owner 同 threadId 照常续（前端刷新重连不受影响），异 owner 读到空、走新会话。
//
// 单租户 / 内网可信环境：返一个固定值即可——
//
//	agui.WithOwnerResolver(func(context.Context) string { return "default" })
//
// 多租户：从认证中间件放进 ctx 的主体取。**不要**从客户端可控的字段（header /
// body / query）取，那等于没做。resolver 返空串的请求全落同一个匿名分区。
func WithOwnerResolver(fn compose_v2.OwnerResolver) Option {
	return func(h *handler) { h.ownerResolver = fn }
}

// Handler 把一个 agent 暴露成 AG-UI server 的 http.Handler（POST + SSE）。
//
// 请求：POST，body 是 RunAgentInput。resume 字段非空走 HITL 续跑，否则新 Run。
// 响应：text/event-stream，AG-UI 事件流（RunStarted → TextMessage* / ToolCall* … →
// RunFinished；中断时 RunFinished.outcome=interrupt、带 interrupts）。
//
//	cp := compose_v2.NewInMemoryCheckpointer()
//	http.Handle("/agui", agui.Handler(func(h hook.Hooks) agent_v2.Agent {
//	    return agent_v2.NewBasicAgentBuilder(...).WithHooks(h).Build()
//	}, agui.WithCheckpointer(cp)))
//
// HITL 闭环：中断→前端收 RunFinished(interrupt)→用户答→带同 threadId + resume=[{interruptId,
// status, payload}]（对齐官方 ResumeEntry）续发→server 走 Resume。session 映射 in-process
// 持有（同 a2a/server 限制：server 重启丢挂起会话、跨进程 Resume 不支持）。
//
// 并发：同一 threadId 至多一个在途 Run/Resume（busy 保护）——第二个并发请求返
// RunError(code=busy)，避免同一挂起 sess 被两个 Resume 并发驱动 / cp 双写。
func Handler(build AgentFactory, opts ...Option) http.Handler {
	if build == nil {
		panic("agui: AgentFactory must be non-nil")
	}
	h := &handler{
		build:       build,
		sessions:    map[sessionKey]*sessionEntry{},
		maxSessions: DefaultMaxSessions,
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.ownerResolver == nil {
		panic("agui: WithOwnerResolver 必配且无默认值——" +
			"threadId 由客户端生成，单靠它谁猜中谁就能续别人的会话；" +
			"单租户返一个固定值即可，" +
			"多租户从认证中间件放进 ctx 的主体取（会话历史层的 WithHistoryOwnerFunc 是同一条规矩）")
	}
	return h
}

// sessionKey 是会话表的键：**owner 与 threadId 各占一维，不拼串**。
//
// 两维都是身份的一部分：threadId 由客户端生成（续接标识），owner 由服务端从已认证凭据
// 解析（谁在续）。合成一个字符串（`owner + NUL + threadId`）要求「分隔符不出现在任一段
// 里」，而 owner 来自 JWT claim / DB 字段时这条保不住——`{acme\x00evil, t}` 跟
// `{acme, evil\x00t}` 拼出同一个键 ⟹ 两个租户共用一个会话槽。Go 的 map 键可以是可比较
// 的 struct，两维直接各占一个字段，那个前提就不需要了。
//
// 顺带：owner 是**淘汰时要用的一等事实**，在插入这一刻就在手上；键本身带着它，就不必
// 事后拆键反推（拆出来的那个在 owner 含 NUL 时是错的——实测让受害者被算进攻击者的份额）。
type sessionKey struct {
	owner    string // 服务端从已认证凭据解析（客户端填不了）；空 = 匿名分区
	threadID string // 客户端给的续接标识
}

// sessionEntry 是一个会话（owner + threadId）的挂起状态。
type sessionEntry struct {
	sess      *agent_v2.RunSession // 中断挂起的 sess（待续发 Resume 续用）；busy 壳期间可能 nil
	busy      bool                 // 有在途 Run/Resume——防并发续发 + 覆盖竞态
	touchedAt int64                // 最后活动 Unix nano，LRU 淘汰用
	// open 是挂起的那个 run 交给前端的全部中断（InterruptID → 是不是工具审批）。续跑的 resume 列表必须恰好覆盖它们
	// （AG-UI：不支持只答一部分，也不能答不在其中的）；审批的答复先校验再开跑，认不出的整批拒绝、这些问题保持
	// 挂起（交给审批层的话认不出就按拒绝处理，`{"approved":true}`、`Yes.` 曾都被当成拒绝）。
	open map[string]bool
	// prior 是 Run 时交给 agent 的调用方前文（agent_v2.WithCallerHistoryCtx），续跑时原样再推。
	prior []*schema.Message
	// answers 是这个任务里已经采纳过的全部答复（按 InterruptID），每次续跑都整份交给 Resume。没进工具账的提问
	// （ask_user、节点里直接 Ask 的）续跑时会重问，框架要 Resume 的调用方把答过的答案带上；只交这一轮的，一个
	// 节点先后问两问时就会来回重问、永远答不完（实测）。新开任务时 acquireForRun 换上新的 entry，跟着清空。
	answers map[string]*schema.Message
}

type handler struct {
	build         AgentFactory
	cp            compose_v2.Checkpointer // HITL Resume 用（nil = 不支持 Resume）
	maxSessions   int                     // 挂起会话上限（<=0 不限）
	sessionTTL    time.Duration           // 挂起会话最长闲置（<=0 不按时间回收），见 WithSessionTTL
	ownerResolver compose_v2.OwnerResolver

	// 客户端内容没采信 / 没取时各告警一次（见 warnClientMessages）
	warnedInstructions atomic.Bool
	warnedMedia        atomic.Bool

	mu        sync.Mutex
	sessions  map[sessionKey]*sessionEntry // (owner, threadId) → 挂起会话状态
	lastSweep int64                        // 上次按 TTL 扫过的 Unix nano（摊掉每请求 O(n)）
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in RunAgentInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "agui: bad request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	sse, ok := NewSSEWriter(w)
	if !ok {
		http.Error(w, "agui: streaming unsupported", http.StatusInternalServerError)
		return
	}
	// panic 兜底：handler goroutine 上（serveRun/serveResume/handleOutcome/factory 构造
	// 等 agent.Run 之外）的 panic 若不兜底，net/http 掐连接会留半截 SSE 流——RunStarted
	// 已 flush 却无 RUN_FINISHED/RUN_ERROR 终态，违反 AG-UI 生命周期契约、前端等终态挂死。
	// 此 recover 补发 RUN_ERROR 收尾（agent.Run 内部 panic 已由 agent_v2 recoverToErr 转
	// err 走正常 RunError 路径；此兜底只覆盖 Run 外的 handler 层 panic）。
	defer func() {
		if rec := recover(); rec != nil {
			// 栈只进服务端日志，不发给前端
			panicerr.Log(r.Context(), slog.LevelError, "agui: handler 兜住 panic，已给前端发 RUN_ERROR", panicerr.New(rec))
			_ = sse.WriteEvent(NewRunError(fmt.Sprintf("agui: internal panic: %v", rec), "panic"))
		}
	}()

	// **没给 threadId 就当场铸一个**（仅新一轮运行；续跑见下）。
	//
	// AG-UI 协议把 `threadId` 定义成必填，且要求 agent 按 thread 维护未决中断——
	// threadId 为空时那个查询无定义。而这一层**无条件**给 ctx 挂 checkpointer，所以匿名
	// 请求一旦中断照样落存档，回收却全部挂在「threadID 非空」上：占用/覆盖清档、挂起
	// 登记、LRU 淘汰，一条都不认匿名 ⟹ 存档没人回收，持久后端下无界增长。而前端拿到的
	// `interrupts[].id` 也用不上——续跑必须带 threadId。
	//
	// **铸而不是拒**：拒会打断「我就问一句话」这种合法用法，还顺带剥夺那次 run 的可续跑性；
	// 而 `RUN_STARTED` 事件本来就要回传 threadId，铸一个成本为零。铸出来的**必须回传**（下面
	// `NewRunStarted(in.ThreadID, ...)` 用的就是它），否则前端仍拿不到可续跑的 id。
	//
	// **续跑不铸**：那时 threadId 是客户端手上已有的续跑句柄，缺了就是客户端的错——
	// 铸一个只会静默开出一个空会话，所以**就在下面这几行里拒**（`no_thread_id`）。
	// 拒也放在这儿而不是 `serveResume`，是为了让「threadID 恒非空」对两条路都成立。
	//
	// 铸完之后「threadID 恒非空」成为不变量，下游那一串判空随之消失——**判据只留一处**。
	// 那串判空正是这个坑的成因：同一个判断散在多处，其中一处（给 ctx 挂 checkpointer）
	// 当初就漏了，而漏的方式恰恰是**没写**。
	if in.ThreadID == "" {
		if len(in.Resume) > 0 {
			_ = sse.WriteEvent(NewRunError("resume requires threadId to locate suspended session", "no_thread_id"))
			return
		}
		in.ThreadID = newThreadID()
	}

	// threadId 作会话聚类 ID push（观测跨 run 聚类，对齐 crew-go ThreadID）。
	// **到这儿 threadID 恒非空**（上面要么铸了、要么已拒），下游不再判空。
	ctx := compose_v2.WithThreadIDCtx(r.Context(), in.ThreadID)
	// owner 从已认证凭据解析（认证中间件放进 r.Context() 的主体），push 给下游
	// （HandoffAgent 持久转交等按 owner 分区的存储都读它），并织进本层会话键。
	// threadId 是续接标识不是授权凭据——键里没有 owner 维时，任何知道 threadId 的人
	// 都能续别人的挂起会话：读到里面的提问、替对方回答。
	owner := h.ownerResolver(r.Context())
	ctx = compose_v2.WithOwner(ctx, owner)
	// 内部会话键 = (owner, threadId) 两维（见 sessionKey）。owner 为空（匿名）时全部
	// 匿名请求落进同一个分区——那是「没有身份」的诚实语义，不是漏洞被修好了的假象。
	key := sessionKey{owner: owner, threadID: in.ThreadID}
	// cp 配了就 push——Run 中断存档 + Resume 重建都需要。
	if h.cp != nil {
		ctx = compose_v2.WithCheckpointerCtx(ctx, h.cp)
	}

	if len(in.Resume) > 0 {
		h.serveResume(ctx, sse, &in, key)
		return
	}
	h.serveRun(ctx, sse, &in, key)
}

// serveRun 处理新一轮运行。
func (h *handler) serveRun(ctx context.Context, sse *SSEWriter, in *RunAgentInput, key sessionKey) {
	prior, inputMsg, rep := splitClientMessages(in.Messages)
	if inputMsg == nil {
		_ = sse.WriteEvent(NewRunError("no user message in request", "bad_request"))
		return
	}
	if inputMsg.Content == "" && rep.inputDropped {
		// 只有媒体、没有文字：照跑的话模型收到一个空问题，还以为自己看过了用户发的东西
		_ = sse.WriteEvent(NewRunError("the last user message has no text part; image / audio / video / document parts are not supported", "unsupported_content"))
		return
	}
	h.warnClientMessages(ctx, rep)
	ctx = agent_v2.WithCallerHistoryCtx(ctx, prior)

	// 占用 threadId（并发保护 + 处理已有挂起会话）。**threadId 到这儿恒非空**——
	// ServeHTTP 没给就铸一个，见那里的说明。
	// key 含 owner 维——map 键一律用它，wire 事件仍用裸 ThreadID（前端只认自己给的那个）。
	stale, evicted, ok := h.acquireForRun(key)
	// 过期回收出来的 cp 要清，**这条早退路径上也要**——acquireForRun 已经把那些 entry
	// 从表里删了，这儿不清就没有第二次机会。
	for _, sid := range evicted {
		h.dropCP(ctx, sid)
	}
	if !ok {
		_ = sse.WriteEvent(NewRunError("run already in progress for this thread", "busy"))
		return
	}
	defer h.releaseBusy(key) // 确保 busy 清（含 panic 路径）
	// 已有挂起 sess（用户开新对话、放弃旧中断）→ 清旧 cp 防孤儿泄漏。
	if stale != nil {
		h.dropCP(ctx, stale.RunID)
	}

	adapter := newSSEAdapter(sse, in.RunID)
	ag := h.build(adapter)
	if ag == nil {
		h.removeEntry(key) // 清 acquireForRun 建的空壳，避免泄漏
		_ = sse.WriteEvent(NewRunError("agui: AgentFactory returned nil agent", "nil_agent"))
		return
	}
	// ctx 版 OrchHooks 让嵌套 sub-agent 也继承 → sub-agent 的 chunk 也真流式，而非只靠
	// OnModelEnd 一次性补发（RunOption WithOrchHooks 只对顶层生效、不向 sub-agent 传播）。
	ctx = agent_v2.WithCtxOrchHooks(ctx, adapter)

	sess := agent_v2.NewRunSession()
	runID := in.RunID
	if runID == "" {
		runID = sess.RunID
	}
	adapter.runID = runID

	// RunStarted 写失败 = client 已断开——短路不跑 agent，省 LLM/工具开销。（agent 跑
	// 期间断开由 r.Context() canceled 触发框架超步边界 ctx.Err() 停 + handleOutcome 清 sess。）
	if err := sse.WriteEvent(NewRunStarted(in.ThreadID, runID)); err != nil {
		h.removeEntry(key) // 清 acquireForRun 建的空壳，避免泄漏
		return
	}
	// **收尾必须挂在 defer 上，不能写成 ag.Run 之后的普通语句。**
	//
	// `runtime.Goexit`（业务 hook 里调了 testify 的 require.* / zap.WithFatalHook 之类）
	// 会跳过所有后续普通语句，而 `ServeHTTP` 的 panic 兜底用的是 `recover()`——**Goexit
	// 下它返 nil**，那道兜底整个不触发。实测后果：事件序列停在 `RUN_STARTED`、客户端拿到
	// `unexpected EOF`（违反 AG-UI 生命周期契约：RUN_STARTED 已 flush 却无终态事件），
	// 而且每次漏一份**没有回收路径**的 cp——`storeInterrupt` 没跑 ⟹ 空壳 entry 的
	// `sess == nil` ⟹ LRU 淘汰它时取不到 sid、连 cp 一起清不掉。
	//
	// ⚠️ **别以为「跑在另一条 goroutine 上」就免疫**——a2a 那侧的 SSE 生产者确实是独立
	// goroutine + `defer recover()`，但 Goexit 下 `recover()` 返 nil，它走的是 `else`
	// 分支（关闭 channel），结果是**消费者干净退出、一个终态事件都没产出**。那侧真正
	// 的免疫来自它自己的 `sent` 旗（见 a2a/server/executor.go），跟 goroutine 形状无关。
	//
	// 换句话说：这道 defer 不是「补 agui 独有的洞」，是这一类收尾协议的通用要求——
	// 凡是「欠外部对端一个终态事件」的地方，都得有一个旗 + defer，而旗必须由**调用
	// 之后的普通语句**置位（defer 里置位没用，Goexit 时那句同样跑不到）。
	settled := false
	defer func() {
		if settled {
			return
		}
		// 走到这儿 = ag.Run 没正常返回（Goexit）。补一个终态事件让前端的生命周期闭合，
		// 并清掉空壳 entry + 它的 cp（那份档谁也够不着：Status=failed 列不出来、
		// sess==nil 让 LRU 也清不了）。
		h.dropCP(ctx, sess.RunID)
		h.removeEntry(key)
		_ = sse.WriteEvent(NewRunError("run terminated abnormally (goroutine exited without returning)", "agent_error"))
	}()

	_, err := ag.Run(ctx, sess, inputMsg)
	settled = true
	adapter.finish()

	h.handleOutcome(ctx, sse, key, runID, sess, prior, err)
}

// serveResume 处理 HITL 续跑——用上次中断挂起的 sess + cp 重建，把答案按 InterruptID
// 配对注入。
func (h *handler) serveResume(ctx context.Context, sse *SSEWriter, in *RunAgentInput, key sessionKey) {
	if h.cp == nil {
		_ = sse.WriteEvent(NewRunError("resume requires checkpointer (agui.WithCheckpointer)", "no_checkpointer"))
		return
	}
	// 校验 resume entry 合法性（对齐官方 ResumeEntry：status 是 resolved/cancelled 枚举、
	// interruptId 非空）——非法值 fail-fast 发 RunError，不静默当 resolved（官方 wire 层是
	// enum 拒非法；内联协议层显式拒暴露客户端 bug，而非把未知状态当有效答案注入续跑）。
	for _, e := range in.Resume {
		if e.Status != ResumeStatusResolved && e.Status != ResumeStatusCancelled {
			_ = sse.WriteEvent(NewRunError(fmt.Sprintf("invalid resume status %q (want %q or %q)", e.Status, ResumeStatusResolved, ResumeStatusCancelled), "invalid_resume"))
			return
		}
		if e.InterruptID == "" {
			_ = sse.WriteEvent(NewRunError("resume entry missing interruptId", "invalid_resume"))
			return
		}
	}

	// 占用挂起会话（并发保护：已 busy = 另一 Resume/Run 在途，拒；不存在 = 会话已结束/过期）。
	sess, open, prior, ok, busy := h.acquireForResume(key)
	if busy {
		_ = sse.WriteEvent(NewRunError("resume already in progress for this thread", "busy"))
		return
	}
	if !ok {
		_ = sse.WriteEvent(NewRunError("no suspended session for thread (interrupt expired or wrong threadId)", "no_session"))
		return
	}
	defer h.releaseBusy(key) // 确保 busy 清（含 panic 路径）
	// 续跑里才开始跑的成员（Sequential 的后一步之类）同样要看到前文，而且是 Run 时那一份：跟同一个任务里先跑的成员看到
	// 的一样。不按续跑请求的 messages 重算——前端可能把这一问的答复当成一条 user 追加进去，那样 Run 时的输入会被算进
	// 前文、续跑才跑的成员看到它两遍。
	ctx = agent_v2.WithCallerHistoryCtx(ctx, prior)

	// **resume 列表恰好覆盖挂起的那些中断**（AG-UI：每一条答一个挂起的中断，全部都要答到或者明确放弃，不支持只答
	// 一部分）。规范把这条交给前端检查，这里仍然查：只答一部分的话，没答的那几问照样挂着，而同一批里没进工具账的
	// 提问续跑时会重问；同一个 ID 出现两次时后一条覆盖前一条，审批的批准与拒绝取决于数组顺序（审计实测）。
	if msg := coverageProblem(in.Resume, open); msg != "" {
		_ = sse.WriteEvent(NewRunError(msg, "invalid_resume"))
		return
	}

	// **审批答复先校验、再开跑**：交给审批层的话，认不出的答复按拒绝处理（fail-closed）——前端发
	// `{"approved":true}` 或 `Yes.`，工具就被当成用户拒绝、一次都不执行（实测）。认不出就报错，这一问
	// 保持挂起（会话、存档原样），前端可以重答；一批里只要有一条认不出，整批都不采纳。
	approvalAnswers := make(map[string]string, len(open))
	for _, e := range in.Resume {
		if e.Status != ResumeStatusResolved || !open[e.InterruptID] {
			continue
		}
		option, ok := approvalAnswerFromPayload(e.Payload)
		if !ok {
			_ = sse.WriteEvent(NewRunError(fmt.Sprintf("interrupt %q is a tool approval; answer with true / false, "+
				`{"approved": true|false} (no other fields: editing the tool arguments is not supported), `+
				`or one of "approve", "approve_always", "approve_always_tool", "deny"`, e.InterruptID),
				"invalid_resume_payload"))
			return
		}
		approvalAnswers[e.InterruptID] = option
	}

	// **wire 上的 runId 与我们内部的存档键是两个东西，别共用一个载体。**
	//
	//   - `sess.RunID` 是 crew-go 的「一个任务」身份：一次 Run + N 次 Resume 共用一枚，
	//     它是 checkpoint 的主索引，`ag.Resume` 必须拿它去找存档。
	//   - AG-UI 的 `runId` 是「**一次** run」身份。规范原文：「`runId` identifies one run.
	//     It MUST NOT be reused for another run on the same thread」，而且
	//     「continuing means a new run」——续跑在 wire 上就是一个新 run。
	//
	// 这里曾经两处都用 `sess.RunID`（同一文件的 serveRun 反而是对的，优先取 `in.RunID`）：
	// **N=1（没中断过）正确，N≥2（中断续跑）静默违规**——重播了被中断那个 run 的 id。
	// 叠加 adapter 把 runID 当 messageId 用，续跑的工具结果还会被前端**追加进中断前
	// 那条消息**（规范：「Re-streaming a message the consumer already has appends to it」）。
	//
	// `RunAgentInput.runId` 在 1.0 里是 required，前端必给；留兜底只为不让一个缺字段的
	// 请求崩掉整条流。
	wireRunID := in.RunID
	if wireRunID == "" {
		wireRunID = sess.RunID
	}

	adapter := newSSEAdapter(sse, wireRunID)
	ag := h.build(adapter)
	if ag == nil {
		_ = sse.WriteEvent(NewRunError("agui: AgentFactory returned nil agent", "nil_agent"))
		return
	}
	ctx = agent_v2.WithCtxOrchHooks(ctx, adapter) // 同 serveRun，让 sub-agent 也真流式

	// 分类 resume entry：resolved 收集答案（按 InterruptID 配对），cancelled 记账。
	answers := make(map[string]*schema.Message, len(in.Resume))
	hasResolved, hasCancelled := false, false
	for _, e := range in.Resume {
		if e.Status == ResumeStatusCancelled {
			hasCancelled = true
			continue
		}
		hasResolved = true
		if option, isApproval := approvalAnswers[e.InterruptID]; isApproval {
			answers[e.InterruptID] = schema.UserMessage(option)
			continue
		}
		answers[e.InterruptID] = schema.UserMessage(payloadToString(e.Payload))
	}

	// RunStarted 写失败 = client 已断开——短路不跑 Resume。
	if err := sse.WriteEvent(NewRunStarted(in.ThreadID, wireRunID)); err != nil {
		return
	}

	// 同 serveRun：收尾挂 defer，防 runtime.Goexit 跳过后续普通语句留下没有终态事件的
	// 半截流。这里**不清 cp**——挂起的存档是用户答完还要用的，Resume 中途 Goexit 时
	// 清掉等于把人家答到一半的会话删了。只补终态事件 + 放掉 busy 让下次续发进得来。
	settled := false
	defer func() {
		if settled {
			return
		}
		_ = sse.WriteEvent(NewRunError("resume terminated abnormally (goroutine exited without returning)", "agent_error"))
	}()

	// 全 cancelled（用户放弃、无任何有效答复）→ 终止整个 run，对齐官方 AG-UI cancelled
	// 语义 "user abandoned"。ResumeActionAbort 清 cp + 退出（返 nil error）。
	if hasCancelled && !hasResolved {
		_, err := ag.Resume(ctx, sess.RunID,
			agent_v2.WithResumeSession(sess),
			agent_v2.WithResumeAction(agent_v2.ResumeActionAbort))
		settled = true
		adapter.finish()
		h.removeEntry(key) // run 终止，清挂起 entry
		if err != nil {
			_ = sse.WriteEvent(NewRunError(err.Error(), "abort_failed"))
			return
		}
		// cancelled 走**正式的 outcome**（1.0 定稿后有这个分支了，见 NewRunCancelled 的
		// godoc——此前发的「省掉 outcome」那一版会被 1.0 前端读成一个成功完成的 run）。
		// Custom("cancelled") 保留：它早于 outcome 存在，前端可能已依赖；但它不再是
		// 唯一信号。
		_ = sse.WriteEvent(NewCustom("cancelled", map[string]any{"runId": wireRunID}))
		_ = sse.WriteEvent(NewRunCancelled(in.ThreadID, wireRunID))
		return
	}

	// 有 resolved（可能混合 cancelled）→ 续跑。混合时 cancelled 的 interrupt 仍需答案
	// 才能推进，注入明确取消标记（非模糊 [cancelled]）让业务工具 / LLM 感知用户取消了该项。
	for _, e := range in.Resume {
		if e.Status == ResumeStatusCancelled {
			answers[e.InterruptID] = schema.UserMessage("[用户取消了此项]")
		}
	}
	_, err := ag.Resume(ctx, sess.RunID,
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(h.recordAnswers(key, answers)))
	settled = true
	adapter.finish()

	h.handleOutcome(ctx, sse, key, wireRunID, sess, prior, err)
}

// handleOutcome 按 Run/Resume 结果发终态 + 更新挂起 entry——中断则 entry 留 sess 与 Run 时的前文（prior）待续发
// （多轮 HITL），成功 / 错误则清 entry（会话结束）。
// ⚠️ **发回前端的只能是 key.threadID 那一维**——owner 是服务端解析出来的身份，前端只认
// 它自己给的那个 threadId，把整个键的内容发出去它就认不出来了（顺带把身份泄给了浏览器）。
func (h *handler) handleOutcome(ctx context.Context, sse *SSEWriter, key sessionKey, runID string, sess *agent_v2.RunSession, prior []*schema.Message, err error) {
	// **陈旧写回**（`WithStaleWriteGuard` 开启时才可能出现）：这次的结果没落盘，槽里是另一个执行者
	// 的状态，留 entry 等续发是徒劳——前端答完发回来，框架载入的是别人的状态，于是重新问一遍已经
	// 答过的问题，来回不收敛（实测：问 q1 → 答 → 问 q2 → 答 → 又问 q1）。引擎出口写档撞上它时返
	// `ErrCheckpointNotPersisted`、中断不在链上；判在 IsInterrupt 之前，是防自研 agent 把它跟中断绑在
	// 一条链上交过来。
	//
	// 正确处置：不留 entry（续发没有意义），给前端一个专属 code 让它知道是并发冲突、
	// 而不是"又一次正常提问"。
	// **存档自相矛盾也要专属处置**，理由跟上面那条同源：落进通用兜底的话前端拿到的 code 是
	// `agent_error`，而那一档的定义是「可能是瞬时的 ⟹ 原样重来是合理选项」——**框架自己在
	// `IsRetryable` 里明确排掉了它**（重跑必然拿到同一份档）。同一个错，对内说别重试、对外
	// 说可以重来，前端照做只会拿到一条与真实原因无关的 `no_session`。
	//
	// 正文同样脱敏：`ErrCheckpointNotResumable` 的错误信息带 sid / ns / graphID / 各字段
	// 计数，那是给服务端日志排查用的，不该推给浏览器。
	if errors.Is(err, compose_v2.ErrCheckpointNotResumable) {
		// **句柄留着，不能 removeEntry**——这是这条分支跟上面那条 stale 分支最大的不同，
		// 也是我第一版照抄 stale 形状踩的坑。
		//
		// stale 那条删 entry 是无损的：**它那种情况下存档根本没落盘**（见那条的注释），
		// 删的是个已经没有指向的壳。而这条的整个卖点是**存档被引擎特意保住了**——保住是为了
		// 排查，可保住之后总得有人能把它清掉。
		//
		// **清它的唯一一条路是「放弃」，而放弃跟「回答」是同一个端点**（AG-UI 协议如此：
		// 全 `cancelled` 的 resume ⟹ `ResumeActionAbort`）。删了 entry，下一次请求在
		// `acquireForResume` 就返 `no_session`，**回答和放弃一起没了** ⟹ 那份档变成谁也
		// 够不着的孤儿，而且它 Status 是挂起，会永远留在业务的待处理会话列表里。
		//
		// 留着 entry 之后：放弃这条路**不经过坏掉的那个东西**——`agent_v2.Resume` 的 Abort
		// 分支在载入存档之前就 `DeleteSubtree` 返回了（实测过），所以引擎那道守卫根本不会
		// 再开火。而「用户再也不回来」由 `WithMaxSessions` 的淘汰兜底（淘汰时连 cp 一起清），
		// 那本来就是这一层为「放弃的挂起」准备的机制。⚠️ 淘汰按 owner 分摊之后这道兜底
		// **只由这个 owner 自己的下一个会话触发**——它再也不来的话这条 entry 就一直留着
		// （每个 owner 至多一条，那正是 WithMaxSessions 说的软上限）。
		//
		// ⚠️ 前端照 code 再发一次**回答**会拿到同样的错——那是对的：确定性的错就该确定性地
		// 报，而不是换一种说法。文案里点明另一条出路。
		h.keepSuspended(key, sess) // 挂起的还是之前那几问：记下的中断与答复都留着
		_ = sse.WriteEvent(NewRunError("the saved state for this session is inconsistent and cannot be resumed; "+
			"send all interrupts as cancelled to discard it, or start a new session",
			"checkpoint_not_resumable"))
		return
	}
	if errors.Is(err, compose_v2.ErrCheckpointStale) {
		h.removeEntry(key)
		// 不回传 err.Error()——里面有 session id / ns / epoch token 等内部信息
		_ = sse.WriteEvent(NewRunError("this session was concurrently advanced by another run; result was not persisted",
			"checkpoint_stale"))
		return
	}
	// **身份冲突**：这个会话的存档属于另一张图（同一个 threadId 先后接了拓扑不同的 agent——接线错误）。
	// 跟上面两条同理要专属 code：它重跑必然撞同一个槽（框架在 IsRetryable 里排掉了它），落进通用兜底
	// 的 `agent_error` 会让前端原样重来。文案不回传 err.Error()——里面有 sid / ns / 两张图的 GraphID。
	if errors.Is(err, compose_v2.ErrGraphIDConflict) {
		h.removeEntry(key)
		_ = sse.WriteEvent(NewRunError("this session's saved state belongs to a different agent; start a new session",
			"graph_id_conflict"))
		return
	}
	// **档没写成**（存储出错）：这次没有可以续的档，引擎已经尽力清掉槽里更早的那份。不留 entry——留着
	// 的话前端续发要么找不到档，要么续进旧档（重问已经答过的问题）。文案不回传 err.Error()——里面有
	// 后端的错误原文与 sid。
	if errors.Is(err, compose_v2.ErrCheckpointNotPersisted) {
		h.removeEntry(key)
		_ = sse.WriteEvent(NewRunError("this run's state could not be saved, so it cannot be resumed; start a new session",
			"checkpoint_save_failed"))
		return
	}
	if ie, isInterrupt := compose_v2.IsInterrupt(err); isInterrupt {
		queries := ie.AllQueries()
		interrupts := make([]RunInterrupt, 0, len(queries))
		open := make(map[string]bool, len(queries))
		for _, q := range queries {
			// 取每条自己的 reason：同批多个中断经 MergeInterrupts 合成一个时，ie.Reason 是一句合并说明，各自的原因
			// （如 tool_approval）在 q.Reason 上。单个中断原样透出时 q.Reason 可能没填，回落 ie.Reason。
			reason := q.Reason
			if reason == "" {
				reason = ie.Reason
			}
			if reason == agent_v2.ApprovalInterruptReason {
				open[q.InterruptID] = true
				interrupts = append(interrupts, approvalInterrupt(q))
				continue
			}
			open[q.InterruptID] = false
			interrupts = append(interrupts, RunInterrupt{ID: q.InterruptID, Reason: reason, Message: q.Question})
		}
		h.storeInterrupt(key, sess, open, prior)
		_ = sse.WriteEvent(NewRunInterrupted(key.threadID, runID, interrupts))
		return
	}
	h.removeEntry(key)
	// **句柄丢了，档一起清**：这次退出留下的档（调用方取消导致的退出、业务报错留下的失败现场）在 agui 这边
	// 没有续跑入口——放弃得到 no_session，LRU、TTL 都够不着它——留着只会在持久后端上越积越多、一直挂在
	// ListPending 里（审计实测：浏览器断开之后留下一份 pending 档）。
	if sess != nil && compose_v2.LeavesResumableCheckpoint(err) {
		h.dropCP(ctx, sess.RunID)
	}
	if err != nil {
		// **给 code**：这是最常走的一条错误分支，而它原先是这里唯一不填 code 的
		// （其余每一处 `NewRunError` 都带一个字面量 code——**别在这儿抄一份清单**，
		// 那份清单已经漂过两次；要全集就 grep `NewRunError(`）。前端拿到一个没有
		// 机器可读代号的通用错误，就没法在**三条相反的动作**之间选：会话已经结束了该
		// 开新的、这次是终局失败别再试、还是可以原样重来。协议侧 `code` 是可选字段，
		// 但填上才谈得上编程处理。
		_ = sse.WriteEvent(NewRunError(err.Error(), agentErrorCode(err)))
		return
	}
	_ = sse.WriteEvent(NewRunFinished(key.threadID, runID))
}

// ── sessions map 并发 + 生命周期 helpers ──

// acquireForRun 为 serveRun 占用会话键（标 busy）。已 busy（在途 Run/Resume）返
// ok=false。返旧挂起 sess（stale，非 nil 时 caller 清其 cp）+ 因上限淘汰的旧 sid 列表
// （caller 清其 cp）。
func (h *handler) acquireForRun(key sessionKey) (stale *agent_v2.RunSession, evicted []string, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// **expired 必须无条件带出去**，包括下面 busy 早退那条：sweep 已经把 entry 从表里
	// 删了，RunID 掉在地上就是一堆没人清的 cp 孤儿。
	expired := h.sweepExpiredLocked()
	if e := h.sessions[key]; e != nil {
		if e.busy {
			return nil, expired, false
		}
		stale = e.sess // 旧挂起（覆盖前 caller 清其 cp）
	}
	h.sessions[key] = &sessionEntry{busy: true, touchedAt: time.Now().UnixNano()}
	// 淘汰要知道「这一次是谁把表撑过上限的」——刚插进去的这条就是它，账记在它头上。
	evicted = append(expired, h.evictIfNeededLocked(key.owner)...)
	return stale, evicted, true
}

// sweepExpiredLocked 回收闲置超过 sessionTTL 的空闲挂起会话，返回它们的 RunID 供 caller
// 清 cp。调用方必须持锁。
//
// **惰性 + 限频**：只在请求进来时顺手扫，且最多每 TTL/4 扫一次——扫一次是 O(n)，每个请求
// 都扫会让 10000 条的表变成每请求一万次比较。限频的代价是回收最晚迟到 TTL/4，无所谓。
//
// 在途（busy）的跳过：它正被一个请求驱动着，删了那个请求的 releaseBusy / storeInterrupt
// 就写到一个已经不在表里的 entry 上（那两处对 nil 都是 no-op，不会 panic，但会静默丢挂起）。

// expiredLocked 判一条会话是不是已经闲置超过 sessionTTL。TTL<=0（默认）恒 false。
// 调用方必须持锁。
func (h *handler) expiredLocked(e *sessionEntry) bool {
	if h.sessionTTL <= 0 || e == nil {
		return false
	}
	return time.Now().UnixNano()-e.touchedAt > int64(h.sessionTTL)
}

func (h *handler) sweepExpiredLocked() []string {
	if h.sessionTTL <= 0 {
		return nil
	}
	now := time.Now().UnixNano()
	if h.lastSweep != 0 && now-h.lastSweep < int64(h.sessionTTL/4) {
		return nil
	}
	h.lastSweep = now
	var sids []string
	for k, e := range h.sessions {
		if e.busy || !h.expiredLocked(e) {
			continue
		}
		delete(h.sessions, k)
		if e.sess != nil {
			sids = append(sids, e.sess.RunID)
		}
	}
	return sids
}

// acquireForResume 为 serveResume 占用挂起会话（标 busy），返回挂起的中断（InterruptID → 是不是工具审批）与 Run 时的
// 前文。不存在（含 busy 壳无 sess）返 ok=false；已 busy 返 busy=true。
func (h *handler) acquireForResume(key sessionKey) (sess *agent_v2.RunSession, open map[string]bool, prior []*schema.Message, ok bool, busy bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e := h.sessions[key]
	if e == nil || e.sess == nil {
		return nil, nil, nil, false, false
	}
	if e.busy {
		return nil, nil, nil, false, true
	}
	// **过期即不可用**，跟惰性回收什么时候真跑无关——见 WithSessionTTL 的两段语义。
	// 不在这儿判的话，回收延迟就变成语义延迟：一条早该过期的会话在下次扫描之前仍然续
	// 得起来，而低流量部署下「下次扫描」可能是几天以后。
	if h.expiredLocked(e) {
		return nil, nil, nil, false, false
	}
	e.busy = true
	e.touchedAt = time.Now().UnixNano()
	// 交出去的是拷贝：锁外读，store 在别的请求里可能换掉它
	open = make(map[string]bool, len(e.open))
	for id, approval := range e.open {
		open[id] = approval
	}
	return e.sess, open, e.prior, true, false
}

// recordAnswers 把这一轮采纳的答复并进这个任务累积的答复，返回整份（拷贝）交给 Resume。entry 不在了（被淘汰）时
// 只返回这一轮的。
func (h *handler) recordAnswers(key sessionKey, answers map[string]*schema.Message) map[string]*schema.Message {
	h.mu.Lock()
	defer h.mu.Unlock()
	e := h.sessions[key]
	if e == nil {
		return answers
	}
	if e.answers == nil {
		e.answers = make(map[string]*schema.Message, len(answers))
	}
	for id, ans := range answers {
		e.answers[id] = ans
	}
	all := make(map[string]*schema.Message, len(e.answers))
	for id, ans := range e.answers {
		all[id] = ans
	}
	return all
}

// releaseBusy 清该会话键的 busy 标记（defer 调，含 panic 路径）。entry 已被 remove
// 则 no-op。
func (h *handler) releaseBusy(key sessionKey) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := h.sessions[key]; e != nil {
		e.busy = false
	}
}

// storeInterrupt 中断挂起时把 sess、这次交给前端的中断（InterruptID → 是不是工具审批）与 Run 时的前文存入 entry，换掉
// 之前记下的中断；累积的答复留着（busy 保持——releaseBusy defer 会清）。
func (h *handler) storeInterrupt(key sessionKey, sess *agent_v2.RunSession, open map[string]bool, prior []*schema.Message) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e := h.sessions[key]
	if e == nil {
		e = &sessionEntry{}
		h.sessions[key] = e
	}
	e.sess = sess
	e.open = open
	e.prior = prior
	e.touchedAt = time.Now().UnixNano()
}

// keepSuspended 让会话照原样挂着：sess 存回 entry，记下的中断与答复都不动（续跑没开成、挂起的还是之前那几问）。
func (h *handler) keepSuspended(key sessionKey, sess *agent_v2.RunSession) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e := h.sessions[key]
	if e == nil {
		e = &sessionEntry{}
		h.sessions[key] = e
	}
	e.sess = sess
	e.touchedAt = time.Now().UnixNano()
}

// coverageProblem 检查 resume 列表是否恰好覆盖挂起的中断：每一条都是挂起的中断、没有重复、每个挂起的中断都有一条。
// 不满足时返回给前端的说明。
func coverageProblem(entries []ResumeEntry, open map[string]bool) string {
	seen := make(map[string]bool, len(entries))
	for _, e := range entries {
		if _, ok := open[e.InterruptID]; !ok {
			return fmt.Sprintf("interrupt %q is not open on this thread", e.InterruptID)
		}
		if seen[e.InterruptID] {
			return fmt.Sprintf("interrupt %q appears more than once in resume", e.InterruptID)
		}
		seen[e.InterruptID] = true
	}
	if len(seen) == len(open) {
		return ""
	}
	missing := make([]string, 0, len(open)-len(seen))
	for id := range open {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	sort.Strings(missing)
	return fmt.Sprintf("resume must cover every open interrupt (answer it, or send it as cancelled); missing %s", strings.Join(missing, ", "))
}

// removeEntry 删该会话键的挂起 entry（会话结束：成功 / 错误 / abort）。
func (h *handler) removeEntry(key sessionKey) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.sessions, key)
}

// evictCandidate 是一条「可以拿来抵账」的空闲 entry（连同它那个 owner 当前占的份额）。
type evictCandidate struct {
	key   sessionKey
	entry *sessionEntry
	share int // 该 owner 当前占的 entry 数（含在途的）
}

// beats 定序：份额大的先付账 → 同份额里更旧的先 → 再同就比键。
// 最后那一档不是摆设：map 迭代无序，没有它同一个场景每次淘汰的会话都不同
// （UnixNano 在粗时钟平台上撞得到）。
func (c evictCandidate) beats(o evictCandidate) bool {
	switch {
	case c.share != o.share:
		return c.share > o.share
	case c.entry.touchedAt != o.entry.touchedAt:
		return c.entry.touchedAt < o.entry.touchedAt
	case c.key.owner != o.key.owner:
		return c.key.owner < o.key.owner
	default:
		return c.key.threadID < o.key.threadID
	}
}

// evictIfNeededLocked 超上限时淘汰一条空闲（非 busy）entry，返其 sid 供 caller 清 cp。
// inserter 是刚把表撑过上限的那个 owner。调用方须持 h.mu。
//
// 两条规矩，缺一条就漏一种跨租户 DoS：
//
//  1. **份额最大的分区付账**。一个租户狂开会话，挤掉的恒是它自己的（它就是最大那个）；
//     单租户部署下只有一个分区，行为跟全局 LRU 一模一样。
//  2. **到达者不赢平局**——只有份额**严格大于**到达者的分区才轮得到别人付账。
//
// 第 2 条是「跑完即走」那类攻击的解药，也是这段代码原先唯一缺的东西：攻击者发一个
// 立刻跑完的请求，它的 entry 转眼就被 removeEntry 掉，份额恒是 1，跟受害者持平 ⟹
// 老代码在平局上回落全局最旧 ⟹ 受害者被淘汰（连 cp 一起清，续跑拿到 no_session）。
// 而刚插进来的那条 entry 恒是 busy、永远选不中，于是**新来的恒胜过在位的**。实测
// A/V 各挂 1 个填满表、攻击者发一个立刻跑完的请求，20 轮全部淘汰 A。
//
// 平局时到达者自己付账，可它手上只有刚插入的那条 busy entry ⟹ **没有可淘汰的**，
// 这时**不淘汰**（返 nil，表暂时超出上限），不回落去找别人。代价见 WithMaxSessions
// 的 godoc：上限因此是软的。
func (h *handler) evictIfNeededLocked(inserter string) []string {
	if h.maxSessions <= 0 || len(h.sessions) <= h.maxSessions {
		return nil
	}
	// 份额**含在途（busy）的 entry**：一条在途 entry 一样占着表里一个位子，更要紧的是
	// ——刚插入的那条算进到达者头上，它才会跟受害者「平局」而不是「份额 0」。不算的话
	// 到达者恒是最小的那个，第 2 条规矩就形同虚设（实测：不算就退回原来的 DoS）。
	shares := make(map[string]int, len(h.sessions))
	for k := range h.sessions {
		shares[k.owner]++
	}
	mine := shares[inserter]

	var best evictCandidate
	for k, e := range h.sessions {
		if e.busy {
			continue // 在途，动不得
		}
		s := shares[k.owner]
		if k.owner != inserter && s <= mine {
			continue // 到达者不赢平局
		}
		c := evictCandidate{key: k, entry: e, share: s}
		if best.entry == nil || c.beats(best) {
			best = c
		}
	}
	if best.entry == nil {
		return nil // 无可淘汰（到达者自己没有空闲会话 / 别人都在途）——表暂时超出上限
	}
	delete(h.sessions, best.key)
	if best.entry.sess == nil {
		return nil // 空壳（还没挂起过），没有 cp 要清
	}
	return []string{best.entry.sess.RunID}
}

// dropCP 清一个 sid 的 cp 子树（覆盖旧挂起 / 淘汰时防 cp 孤儿泄漏）。cp 未配则 no-op。
// 用 DeleteSubtree 直删而非 Resume(Abort)——淘汰场景不需 agent 层 emit，省 build 开销。
func (h *handler) dropCP(ctx context.Context, sid string) {
	if h.cp == nil || sid == "" {
		return
	}
	// detached ctx——cp 清理不受 request ctx 取消影响（client 断开时仍要清孤儿 cp；honor-ctx
	// 的 backend 如 PG/Redis 尤其需要，否则 ctx 取消让 DeleteSubtree 失败反而漏清）。对齐
	// a2a executor 的 context.WithoutCancel。另加上限：后端挂住时 handler 不一直卡着（同引擎出口的宽限期）。
	// best-effort：失败不阻断主流程（cp 泄漏优于崩）。
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dropCPTimeout)
	defer cancel()
	_, _ = h.cp.DeleteSubtree(dctx, sid, "")
}

// dropCPTimeout 是 dropCP 最多等多久。
const dropCPTimeout = 10 * time.Second

// approvalResponseSchema 是工具审批中断期望的答复形状（AG-UI 概念文档推荐的工具审批 schema，不含 editedArgs：
// 不支持带修改的批准；不收别的字段，带了的答复按不合 schema 拒绝）。
func approvalResponseSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"approved": map[string]any{"type": "boolean"}},
		"required":             []any{"approved"},
		"additionalProperties": false,
	}
}

// approvalInterrupt 把一条工具审批提问转成 AG-UI 的中断：reason 用规范的核心值 tool_call（必须带 toolCallId），
// 通用前端据此渲染审批界面；provider 没给工具调用 ID 时用 confirmation（不绑定工具调用的是 / 否确认）。
// 四个选项与工具名放进 metadata，给认得它们的前端用。
func approvalInterrupt(q *compose_v2.AskQuery) RunInterrupt {
	reason := "tool_call"
	if q.ToolCallID == "" {
		reason = "confirmation"
	}
	meta := map[string]any{"options": q.Options}
	if q.ToolName != "" {
		meta["toolName"] = q.ToolName
	}
	return RunInterrupt{
		ID:             q.InterruptID,
		Reason:         reason,
		Message:        q.Question,
		ToolCallID:     q.ToolCallID,
		ResponseSchema: approvalResponseSchema(),
		Metadata:       meta,
	}
}

// approvalAnswerFromPayload 把一条审批答复归一成四个选项之一：true / false、`{"approved": bool}`，或选项字符串
// （词表见 agent_v2.NormalizeApprovalAnswer）。认不出返回 ok=false。对象只认 approved 一个字段：带了别的字段（典型
// 是 editedArgs，带修改的批准）时拒绝——只取 approved 的话，用户改过的参数被丢掉、工具按原参数执行（审计实测）。
func approvalAnswerFromPayload(p any) (string, bool) {
	switch v := p.(type) {
	case bool:
		return approvalOptionFromBool(v), true
	case map[string]any:
		approved, ok := v["approved"].(bool)
		if !ok || len(v) != 1 {
			return "", false
		}
		return approvalOptionFromBool(approved), true
	case string:
		return agent_v2.NormalizeApprovalAnswer(v)
	default:
		return "", false
	}
}

func approvalOptionFromBool(approved bool) string {
	if approved {
		return agent_v2.ApprovalOptionApprove
	}
	return agent_v2.ApprovalOptionDeny
}

// payloadToString 把 AG-UI resume payload（any——字符串或结构化对象）转成注入用文本。
func payloadToString(p any) string {
	if p == nil {
		return ""
	}
	if s, ok := p.(string); ok {
		return s
	}
	b, err := json.Marshal(p)
	if err != nil {
		return fmt.Sprintf("%v", p)
	}
	return string(b)
}

// clientMessageReport 记下拆 messages 时没取的东西，给告警用。
type clientMessageReport struct {
	// instructions 是剥掉的 system / developer 条数。
	instructions int
	// droppedParts 是没取的媒体部分条数（前文与输入合计）。
	droppedParts int
	// inputDropped 报这一轮的输入里有没取的媒体部分。
	inputDropped bool
}

// splitClientMessages 把请求里的 messages 拆成这一轮的输入（最后一条 user）与此前的轮次（交给
// agent_v2.WithCallerHistoryCtx 的调用方前文）。前文只取 user / assistant 的文本：system / developer 剥掉（计进
// report），tool、reasoning、activity 与没有文字的消息不取；最后一条 user 之后的消息不取。没有 user 消息时 input
// 为 nil。
func splitClientMessages(msgs []AGUIMessage) (prior []*schema.Message, input *schema.Message, rep clientMessageReport) {
	last := -1
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == "user" {
			last = i
			break
		}
	}
	if last < 0 {
		return nil, nil, rep
	}
	input = schema.UserMessage(msgs[last].Content)
	rep.inputDropped = len(msgs[last].droppedParts) > 0
	rep.droppedParts = len(msgs[last].droppedParts)
	prior = make([]*schema.Message, 0, last)
	for _, m := range msgs[:last] {
		rep.droppedParts += len(m.droppedParts)
		switch m.Role {
		case "system", "developer":
			rep.instructions++
		case "user":
			if m.Content != "" {
				prior = append(prior, schema.UserMessage(m.Content))
			}
		case "assistant":
			if m.Content != "" {
				prior = append(prior, schema.AssistantMessage(m.Content))
			}
		}
	}
	return prior, input, rep
}

// warnClientMessages 对没采信 / 没取的客户端内容各告警一次（每个 handler 一次，每个请求都告警会刷屏）。
func (h *handler) warnClientMessages(ctx context.Context, rep clientMessageReport) {
	if rep.instructions > 0 && h.warnedInstructions.CompareAndSwap(false, true) {
		slog.WarnContext(ctx, "agui: 请求里的 system / developer 消息不采信、已剥掉——客户端能伪造它们，指令只认 agent 自己的前言"+
			"（本条每个 handler 只记一次）", "count", rep.instructions)
	}
	if rep.droppedParts > 0 && h.warnedMedia.CompareAndSwap(false, true) {
		slog.WarnContext(ctx, "agui: 消息里的 image / audio / video / document 部分不支持、已丢弃，只取文本"+
			"（本条每个 handler 只记一次）", "count", rep.droppedParts)
	}
}

// agentErrorCode 把 agent 返回的错误映射成前端可编程处理的 code。
//
// 只认框架承诺过 `errors.Is` 友好的那几个哨兵，其余归一到 `agent_error`——**不按错误
// 文本猜**：文本是给人读的，会随措辞变，拿它当判据等于建一份会漂的拷贝。
//
// 每个 code 对应前端一条**互不相同**的动作：
//   - `checkpoint_not_resumable`：存档自相矛盾、当下续不了（典型是自研 backend 漏了
//     字段）⟹ 别重发同一个续跑；出路是把全部中断标 cancelled 丢弃它，或开新会话；
//   - `no_pending_checkpoint`：这个会话没有在等你的东西了（正常跑完 / 从未跑过 /
//     上次带着错退出）⟹ 该开新会话，别重发续跑；
//   - `graph_id_conflict`：这个会话的存档属于另一张图（接线错误）⟹ 别重试，开新会话；
//   - `checkpoint_save_failed`：这次的档没写成（存储出错），没有可以续的东西 ⟹ 开新会话；
//   - `run_aborted`：终局失败，重跑必然一样（业务违反协议、参数非法）⟹ 该停，别重试；
//   - `agent_error`：其余，可能是瞬时的 ⟹ 原样重来是合理选项。
//
// 顺序跟 handleOutcome 的专属分支一致：档没写成多数时候同时带着 ErrRunAborted，因写档撞上身份冲突
// 时同时带着 ErrGraphIDConflict，更具体的先判。
func agentErrorCode(err error) string {
	switch {
	case errors.Is(err, compose_v2.ErrCheckpointNotResumable):
		// 兜底里也留一路：这个错在 handleOutcome 有专属分支先接走，但 agentErrorCode
		// 是纯函数、别处也可能调，判据得自洽。
		return "checkpoint_not_resumable"
	case errors.Is(err, compose_v2.ErrNoPendingCheckpoint):
		return "no_pending_checkpoint"
	case errors.Is(err, compose_v2.ErrGraphIDConflict):
		return "graph_id_conflict"
	case errors.Is(err, compose_v2.ErrCheckpointNotPersisted):
		return "checkpoint_save_failed"
	case errors.Is(err, compose_v2.ErrRunAborted):
		return "run_aborted"
	default:
		return "agent_error"
	}
}
