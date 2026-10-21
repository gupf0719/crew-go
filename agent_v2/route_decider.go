package agent_v2

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"strings"

	"crew-go/compose_v2"
	"crew-go/internal/panicerr"
	"crew-go/schema"
)

// RouteDecider 是带置信度的路由决策方：给一次路由请求，返回选中的分支和对这次选择
// 有多确定。
//
// 用 [WithRouteDecider] 挂到 RoutingAgent 上，每个决策方配一个门槛。框架拿
// [RouteDecision.Confidence] 跟门槛比，够就采纳，不够就交给下一档。升级链的顺序是：
// 按挂载顺序逐个决策方 → [WithRoutingClassifier] 的 classifier →
// [WithRoutingFallback] 的兜底分支。
//
// 下面几种情况都按「这一档没决定」处理，交给下一档：
//   - Confidence 低于门槛
//   - Target 为空，表示决策方弃权
//   - Target 不是候选分支之一
//   - Confidence 不在 [0, 1] 内或是 NaN
//   - 返回了普通 error，比如决策服务超时或限流
//
// 后面没有档可交时，Run 返回 [*RouteUndecidedError]。只记录、永不采纳的影子档见 [WithShadow]。
//
// 例外是必须原样往上抛的错误，判据是 compose_v2.MustPropagate：中断（含在决策方里调
// compose_v2.Ask 问用户）、调用方的 ctx 已结束、软取消、陈旧写回、存档不可续与
// compose_v2.ErrRunAborted。它看的是调用方 ctx 的状态，不看错误链上有没有
// context.DeadlineExceeded：决策服务自己的请求超时正是该升级的情形。
//
// 决策方在 RoutingAgent 的 classify 节点里执行，所以可以调 compose_v2.Ask 做 HITL。
// 每一档有自己的提问身份：不同档的提问不会共用一个答案位，哪怕 Resume 时前面某一档
// 走了别的路。Resume 时 classify 节点整个重跑，但已经跑完、没被采纳的档不会被再调用：
// 它们的结果记在节点私有状态里，原样重放。重新执行的只有抛出中断的那一档和它后面的档；
// 报错的档这时同样原样重放，因必须上抛的错误（比如调用方取消）退出后也一样。以
// RouteUndecidedError 收尾后，才按失败的原因决定从哪一档重判，见 [RouteUndecidedError]。
// 决策方自己要记进度可以调 compose_v2.SaveNodeState：它落在这一档自己的那一格（按下面说的调用身份
// 分域），碰不到升级链的进度记录；以失败收尾后重新调用时身份变了，是新的一格。
//
// 这个身份借的是 tool_call 的那几个维度：决策方里 compose_v2.GetCurrentToolCallID 返回
// "route_decider_<档位下标>"（下标不算影子档；影子档是 "route_shadow_<序号>"），以失败收尾后重新调用时
// 带上 "_retry<代数>"。它跨任务、跨
// 会话都可能相同，不是某次 LLM 调用的 tool_call ID，别拿它当幂等键或缓存键。
//
// ⚠️ **同一个输入要问同一个问题**：中断续跑时这一档会被重新执行，身份不变，用户的回答
// 按身份兑现。这次改口问别的，拿到的就是上一个问题的答案。问题内容取决于前面各档的结果
// 时没关系——前面的档在这条路上是原样重放的。
//
// 框架调决策方时已经套了 [WithoutHistoryCtx]：它是内部决策，产出不进会话历史。
// 决策方里要跑子 agent 时用 [RunSubAgent]：它按这一档的身份给子 agent 派生独立的存档
// 命名空间，不会与 classifier 那一档或别的档撞在一起。
//
// 决策方拿到的 [RouteRequest] 是拷贝，改了不影响分支收到的输入，也不影响后面的档：
// Input 的顶层字段、Annotations、Metadata、ContentBlocks 及其中每个内容块都拷了一层，
// 候选清单与前面各档的结果连同分数表是深拷贝。更深的结构（Annotations 里嵌套的 map 与
// 切片，内容块指向的文本、图片等，Metadata 里的 Usage 与 Logprobs）仍与分支共享，别改。
type RouteDecider func(ctx context.Context, req RouteRequest) (RouteDecision, error)

// RouteRequest 是交给 [RouteDecider] 的一次路由请求。
type RouteRequest struct {
	// Input 是 RoutingAgent 收到的原始输入。
	Input *schema.Message
	// Candidates 是全部候选分支，按 AddBranch 的注册顺序。Description 来自
	// [WithRoutingDescriptions]，没配的是空串。兜底分支不在其中。
	Candidates []RoutingChoice
	// Escalations 是这次路由里排在前面、没被采纳的各档结果，按执行顺序，不含影子档的记录
	// （见 [WithShadow]）。第一档收到的恒为空。后面的档可以拿它做文章，比如只在前一档的
	// 前几名里问用户。
	Escalations []RouteEscalation
}

// RouteUndecidedError 表示升级链里每一档都没给出可采纳的决定，后面也没有兜底分支可交。
// 两种来源：配了 classifier 时，是 classifier 报错、没给出目标或给的不是候选分支
// （原因在 ClassifierFailure）；没配 classifier 时，是最后一个非影子档报错、弃权或给了非法
// 结果（构造期检查保证它的门槛是 0，不会是低置信；排在前面的档可以是低置信）。影子档
// （见 [WithShadow]）不影响这里的任何判断：它的记录留在 Escalations 里供诊断，但重判起点与
// LastErr 都不看它。
//
// 之后 Resume 或重试时从哪一档重判，按失败出在哪里分两种，两种都满足「重新调用的档之前
// 的档原样重放，用户在那些档里答过的问题不会再问」：
//   - 失败出在 classifier（它报错，或没给出候选分支）：决策方原样重放，只
//     重新调用 classifier。决策方里有报错的，改从第一个报错的决策方开始重新调用——临时
//     故障可能已经恢复，它这次若拿定了主意就不必再走到 classifier。
//   - 失败出在决策方链（没配 classifier）：有档报错时从第一个报错的档开始重新调用；没有
//     档报错（都是弃权或非法结果，排在前面的档可以是低置信）时从头判，每一档都重新调用，
//     输出不完全确定的决策方（比如 LLM）这次可能就有把握了。
//
// 被重新调用的档带着新的身份（见 [RouteDecider]），它再提问时是新的问题、新的提问 ID：
// 上一次的答案不会被套到这次的问题上（前一档的结果变了，这一档的问题可能也跟着变了），
// 用户会被重新问到。它用 RunSubAgent 跑的子 agent 也换了命名空间，从头跑，不会从上一次
// 的失败现场带着旧输入续跑；代价是那个子 agent 上一次已经跑完的工具会重做一遍，有外部
// 副作用的工具要自己保幂等。旧命名空间下的子 agent 存档成了孤儿，任务成功时随子树一起
// 清掉。它们是失败现场（Status=failed），所以 ListPending 查不到（那个只返在等答案的），
// 要看得用 LoadAllInSession 按 sid 枚举。
//
// 它是任务层面的失败，不是终局失败：存档留着，外层把它当普通的失败处理，比如包成工具时
// 交还给模型换条路。
//
// ⚠️ 存档留着也意味着：同一个位置（同一个 RunSubAgent nodeID、同一个工具调用身份）的下一次
// 调用，只要调用时的 ctx 里没有「新开」信号（compose_v2.IsFreshRunCtx 为 false），就会续上
// 这次失败、沿用存档里的旧输入，这次传入的新输入不生效，且可能以 err == nil 按旧输入路由。
// 带着这个信号的只有 agent_v2 编排顶层 Run 的第一次尝试，以及显式传了
// compose_v2.WithFreshRun 的 Run；Resume 之后、编排重试的第二次及以后的尝试、顶层直接调
// compose_v2.Run 而没传 WithFreshRun 时都没有。要带着新输入重新路由，换一个位置。这是嵌套
// 调用在失败后续跑的通用问题，见中心 backlog。
type RouteUndecidedError struct {
	// AgentName 是 RoutingAgent 名。
	AgentName string
	// Escalations 是每一档决策方没被采纳的原因，按执行顺序，含影子档的记录（Shadow 为 true）。
	// 没配决策方时为空。
	Escalations []RouteEscalation
	// ClassifierFailure 是 classifier 没给出可用目标的原因：它返回的错误，它没给出目标，
	// 或它给的目标不是候选分支，截断到 300 个字符。失败不出在 classifier 时为空。
	ClassifierFailure string
	// LastErr 是最后一个报错的档（非影子的决策方或 classifier）返回的原始错误，没有档报错
	// 时为 nil；Resume 时重放出来的决策方只剩错误文本。要取原因时对它本身做 errors.As。
	// ClassifierFailure 以 "error: " 开头时它来自 classifier，否则来自最后一个报错的决策方。
	//
	// 它**不经 Unwrap 暴露**：决策服务自己的请求超时会带着 context.DeadlineExceeded，
	// 漏到错误链上，按错误链判断的地方就会当成调用方的 ctx 到期了，比如存档会被标成
	// 挂起而不是失败。
	LastErr error
}

func (e *RouteUndecidedError) Error() string {
	parts := make([]string, 0, len(e.Escalations)+1)
	for _, esc := range e.Escalations {
		// 影子档不进错误文本：它对路由不可见，而这段文本可能交给模型（包成工具时成为 tool_result）。
		// 它的记录照样留在 Escalations 字段里。
		if esc.Shadow {
			continue
		}
		if esc.Err != "" {
			parts = append(parts, fmt.Sprintf("decider %d: %s: %s", esc.DeciderIndex, esc.Reason, esc.Err))
			continue
		}
		parts = append(parts, fmt.Sprintf("decider %d: %s", esc.DeciderIndex, esc.Reason))
	}
	if e.ClassifierFailure != "" {
		parts = append(parts, "classifier: "+e.ClassifierFailure)
		return fmt.Sprintf("routing agent %q: the classifier produced no usable route and there is no fallback to escalate to (%s)",
			e.AgentName, strings.Join(parts, "; "))
	}
	return fmt.Sprintf("routing agent %q: no decider produced an acceptable decision and there is no classifier or fallback to escalate to (%s)",
		e.AgentName, strings.Join(parts, "; "))
}

// NonRetryable 实现 compose_v2.NonRetryable：外层编排按默认判据 compose_v2.IsRetryable
// 不会整体重试。
//
// 瞬时故障该由出错的那一档自己重试，在外层重试等于把那一档的次数相乘。decision.Client 自带
// 重试；classifier 默认**不**重试，它的模型一次 503 就会让这次路由以本错误收尾——要重试就
// 给它的模型套 fallback.New 并配 fallback.WithPerModelRetry（只有一个候选时不配就不重试）。
//
// 没有档报错时重试也不一定有用：失败出在决策方链时要重新调用每一档、问过用户的还会再问；
// 失败出在 classifier 时只重新调用 classifier，它对同一输入稳定地给不出分支时，重试只会以
// 同一个错误收尾。值不值得由业务决定，框架不自动做。外层自己给了 ShouldRetry 时，要在里面
// 排除 compose_v2.NonRetryable，比如先调一次 compose_v2.IsRetryable。
func (e *RouteUndecidedError) NonRetryable() {}

// RouteDecision 是决策方给出的一次判断。
type RouteDecision struct {
	// Target 是选中的分支名，必须是候选之一。空串表示弃权，交给下一档。
	Target string
	// Confidence 是决策方对这次选择的确定程度，取值 [0, 1]。它怎么算由决策方定义，
	// 框架只拿它跟门槛比。想按前两名的差距判，就在决策方里把差距算进这个数。
	Confidence float64
	// Scores 是可选的各候选分数，比如概率分布。框架不据此决策，只透传给后面的档位
	// 和路由决策事件。
	Scores map[string]float64
	// Source 是可选的来源标识，比如实际服务的模型版本，只用于观测。
	Source string
}

// RouteEscalation 记录升级链里一档没被采纳的原因。
type RouteEscalation struct {
	// DeciderIndex 是这个决策方在 [WithRouteDecider] 挂载顺序里的下标，从 0 起，不算影子档；Shadow 为
	// true 时是它在影子档里的序号（同样按挂载顺序、从 0 起）。
	DeciderIndex int `json:"decider_index"`
	// Reason 是没被采纳的原因，取值见 RouteReason 开头的常量。
	Reason string `json:"reason"`
	// Target、Confidence、Scores、Source 是这一档给出的判断；Reason 为
	// RouteReasonError 时都为零值。Confidence 不是有限数时记为 0，Scores 里不是有限数
	// 的项被丢掉：这条记录要序列化进存档与事件，JSON 表示不了 NaN 与无穷大。
	Target     string             `json:"target,omitempty"`
	Confidence float64            `json:"confidence"`
	Scores     map[string]float64 `json:"scores,omitempty"`
	Source     string             `json:"source,omitempty"`
	// Err 在 Reason 为 RouteReasonError 时是决策方返回的错误文本，为
	// RouteReasonInvalidConfidence 时说明非法的取值，其余情况为空。
	Err string `json:"err,omitempty"`
	// Shadow 表示这条记录来自影子档（见 [WithShadow]）：它永不采纳，判断够得上门槛时 Reason 为
	// RouteReasonShadow，否则为平常的原因。
	Shadow bool `json:"shadow,omitempty"`
}

// 一档决策没被采纳的原因。
const (
	// RouteReasonLowConfidence 表示置信度低于这一档的门槛。
	RouteReasonLowConfidence = "low_confidence"
	// RouteReasonAbstain 表示决策方返回了空的 Target。
	RouteReasonAbstain = "abstain"
	// RouteReasonUnknownTarget 表示 Target 不是候选分支之一。
	RouteReasonUnknownTarget = "unknown_target"
	// RouteReasonInvalidConfidence 表示 Confidence 不在 [0, 1] 内或是 NaN。
	RouteReasonInvalidConfidence = "invalid_confidence"
	// RouteReasonError 表示决策方返回了错误。
	RouteReasonError = "error"
	// RouteReasonShadow 表示这是影子档（见 [WithShadow]），它的判断够得上这一档的门槛，按配置不采纳。
	RouteReasonShadow = "shadow"
)

// 做出最终路由决定的是升级链的哪一档，见 [RouteDecisionAction.Stage]。
const (
	RouteStageDecider    = "decider"
	RouteStageClassifier = "classifier"
	RouteStageFallback   = "fallback"
)

// routeDeciderStage 是升级链里的一档决策方。
type routeDeciderStage struct {
	decide        RouteDecider
	minConfidence float64
	// branchMin 是按分支单独设的门槛，覆盖 minConfidence。
	branchMin map[string]float64
	// shadow 为 true 时这一档是影子档：照常调用、如实记录，永不采纳，见 WithShadow。
	shadow bool
}

// threshold 返回选中某个分支时这一档要求的置信度。
func (st routeDeciderStage) threshold(target string) float64 {
	if v, ok := st.branchMin[target]; ok {
		return v
	}
	return st.minConfidence
}

// canEscalate 报这一档会不会因置信度不够而升级。
func (st routeDeciderStage) canEscalate() bool {
	if st.minConfidence > 0 {
		return true
	}
	for _, v := range st.branchMin {
		if v > 0 {
			return true
		}
	}
	return false
}

// RouteDeciderOption 配置 [WithRouteDecider] 挂的那一档。
type RouteDeciderOption func(*routeDeciderStage)

// WithBranchMinConfidence 给某个分支单独设门槛，覆盖这一档的默认门槛。
//
// 门槛该按分支的风险定：选错代价大的分支（转账、删除、改配置）要求更高的把握，
// 闲聊类分支可以放低。取值 [0, 1]；同一档里同一个分支设两次、或者分支名没有注册，
// 都是配置错误：前者构造期 panic，后者 Run 时报错。
func WithBranchMinConfidence(branch string, minConfidence float64) RouteDeciderOption {
	if branch == "" {
		panic("WithBranchMinConfidence: empty branch name")
	}
	if math.IsNaN(minConfidence) || minConfidence < 0 || minConfidence > 1 {
		panic(fmt.Sprintf("WithBranchMinConfidence(%q): minConfidence must be within [0, 1], got %v", branch, minConfidence))
	}
	return func(st *routeDeciderStage) {
		if _, dup := st.branchMin[branch]; dup {
			panic(fmt.Sprintf("WithBranchMinConfidence: branch %q is set twice for the same decider", branch))
		}
		if st.branchMin == nil {
			st.branchMin = make(map[string]float64)
		}
		st.branchMin[branch] = minConfidence
	}
}

// WithShadow 把这一档设成影子档：照常调用、如实记下它的判断，但永远不采纳。用来在线上拿一个新的
// 决策方和现有路由并跑：攒够样本、定好门槛之后，去掉这个选项让它接管。
//
// 影子档对路由不可见：选中哪个分支、后面各档收到的请求（[RouteRequest.Escalations] 里没有影子
// 记录）、Resume 与失败重判时哪些档重放哪些重新调用（影子档报错不算「有档报错」，也不进
// [RouteUndecidedError.LastErr]）、跨档的约束（「最后一档」与「有没有决策来源」只看非影子档）、
// RouteUndecidedError 的错误文本，都与把它拿掉时相同。影子档不占真实档的下标（真实档的
// DeciderIndex、调用身份与续跑记录都不算它），所以在途的任务挂上或摘掉影子档，续跑也不错位。
// 它的产出只有路由决策事件与 RouteUndecidedError.Escalations 里它自己的那条 [RouteEscalation]：
// Shadow 为 true，DeciderIndex 是它在影子档里的序号；判断够得上这一档的门槛时 Reason 记
// [RouteReasonShadow]，否则记平常的原因。门槛与 [WithBranchMinConfidence] 照常生效，只决定记哪个
// 原因；分支门槛引用了未注册的分支时照样在 Run 时报错（配置写错了，跟普通档一样）。
//
// 几点要知道：
//   - 它同步执行，路由的延迟里会多出它这一次调用。它的结果不影响路由，给它包一层短超时更合适
//     （写法见 doc/v2/route-decider.md §4.1），超时记为 error。
//   - 想让每次路由都有影子记录，就把它挂在第一档：排在后面时，只有前面的档都没定下来才轮得到它。
//   - 它报错、panic 都只记为 error，不影响路由。影子档里不该提问：compose_v2.Ask 的中断与其它必须上抛的
//     错误照常上抛，路由会停下来。
//   - 不能和 [WithRouter] 同时用：Router 总会自己做决定，影子档永远不会被调用，构造期 panic。
func WithShadow() RouteDeciderOption {
	return func(st *routeDeciderStage) { st.shadow = true }
}

// WithRouteDecider 给 RoutingAgent 加一档带置信度的决策方。
//
// minConfidence 取 [0, 1]：置信度不低于它才采纳，否则交给下一档。设 0 表示不因置信度
// 升级，这时置信度只用于观测；弃权、非法结果与报错照样升级。个别分支要不同的门槛时用
// [WithBranchMinConfidence]。可以多次调用，按调用顺序排成升级链，排在 classifier 和兜底
// 分支前面。
//
// 置信度只和同一档自己的门槛比，不同决策方的分数不需要可比。
//
// 约束，违反时构造期 panic：
//   - 不能和 [WithRouter] 同时用：Router 函数总会给出决定，排在它后面的档位永远轮不到。
//   - 最后一个非影子档可能因置信度不够而升级（默认门槛或任一分支门槛大于 0）时，必须配
//     [WithRoutingClassifier] 或 [WithRoutingFallback]，否则没有地方可升级。
//
// 典型用法，快而便宜的决策模型先判，拿不准再交给 LLM：
//
//	router := agent_v2.NewRoutingAgent(
//	    agent_v2.WithRoutingName("support"),
//	    agent_v2.WithRouteDecider(decision.NewRouteDecider(client), 0.85),
//	    agent_v2.WithRoutingClassifier(llmClassifier),
//	    agent_v2.WithRoutingDescriptions(descriptions),
//	)
func WithRouteDecider(d RouteDecider, minConfidence float64, opts ...RouteDeciderOption) RoutingOption {
	if d == nil {
		panic("WithRouteDecider: nil decider")
	}
	if math.IsNaN(minConfidence) || minConfidence < 0 || minConfidence > 1 {
		panic(fmt.Sprintf("WithRouteDecider: minConfidence must be within [0, 1], got %v", minConfidence))
	}
	st := routeDeciderStage{decide: d, minConfidence: minConfidence}
	for _, opt := range opts {
		if opt == nil {
			panic("WithRouteDecider: nil RouteDeciderOption")
		}
		opt(&st)
	}
	return func(r *RoutingAgent) {
		r.deciders = append(r.deciders, st)
	}
}

// validateDecisionSources 在构造期检查决策来源的组合。只看调用方自己给的配置。
func (r *RoutingAgent) validateDecisionSources() {
	if r.router != nil && (len(r.deciders) > 0 || r.classifier != nil || r.fallback != nil) {
		panic(fmt.Sprintf("NewRoutingAgent %q: WithRouter 不能和 WithRouteDecider、WithRoutingClassifier、"+
			"WithRoutingFallback 同时用——Router 函数总会给出决定，其余几项永远不会被调用", r.name))
	}
	// 只看最后一个非影子档：影子档永不采纳，有没有它，升级链能不能走完都一样。
	if last, ok := r.lastDecidingStage(); ok && r.classifier == nil && r.fallback == nil && last.canEscalate() {
		panic(fmt.Sprintf("NewRoutingAgent %q: 最后一档 WithRouteDecider 设了大于 0 的门槛，置信度不够时没有地方可升级——"+
			"加 WithRoutingClassifier 或 WithRoutingFallback，或者把最后一档的门槛都设成 0", r.name))
	}
}

// lastDecidingStage 返回最后一个非影子档；没有决策方或全是影子档时 ok 为 false。
func (r *RoutingAgent) lastDecidingStage() (routeDeciderStage, bool) {
	for i := len(r.deciders) - 1; i >= 0; i-- {
		if !r.deciders[i].shadow {
			return r.deciders[i], true
		}
	}
	return routeDeciderStage{}, false
}

// hasDecidingStage 报有没有能做决定的决策方。影子档永不采纳，不算决策来源。
func (r *RoutingAgent) hasDecidingStage() bool {
	_, ok := r.lastDecidingStage()
	return ok
}

// validateBranchThresholds 检查 WithBranchMinConfidence 引用的分支都注册过。分支在构造
// 之后才 AddBranch，所以这项检查放在建图时做。
func (r *RoutingAgent) validateBranchThresholds() error {
	registered := make(map[string]bool, len(r.branches))
	for _, br := range r.branches {
		registered[br.Name] = true
	}
	ci, si := 0, 0 // 下标口径同 decisionRouter：真实档与影子档各自编号
	for _, st := range r.deciders {
		kind, idx := "decider", ci
		if st.shadow {
			kind, idx = "shadow decider", si
			si++
		} else {
			ci++
		}
		for branch := range st.branchMin {
			if !registered[branch] {
				return fmt.Errorf("routing agent %q: %s %d has WithBranchMinConfidence for unknown branch %q", r.name, kind, idx, branch)
			}
		}
	}
	return nil
}

// routingChoices 按注册顺序列出全部候选分支，描述取自 WithRoutingDescriptions。
func (r *RoutingAgent) routingChoices() []RoutingChoice {
	out := make([]RoutingChoice, 0, len(r.branches))
	for _, br := range r.branches {
		out = append(out, RoutingChoice{Name: br.Name, Description: r.descriptions[br.Name]})
	}
	return out
}

// routeStageMemo 记下升级链里已经跑完、没被采纳的各档，存在 classify 节点的私有状态里，
// 随存档持久化。
//
// Resume 时 classify 节点整个重跑。没有它，排在前面的决策方会被重新调用，而决策模型
// 的输出不完全确定：上一轮拿不准、已经交给后面去问用户的请求，重跑时前一档可能刚好
// 越过门槛被直接采纳，用户的回答就被悄悄丢掉；后面若是 classifier 在问用户，它挂着的
// 提问也一样被丢掉。有了它，已跑完的档原样重放。
//
// 按 compose_v2.SaveNodeState 的规约，只记已经返回的结果（含报错的档）。节点成功后 visit 推进，这份
// 记录自然不可达。以失败收尾时，按失败的原因整理这份记录，决定重试时哪些档重放、哪些
// 重新调用，见 [RouteUndecidedError] 与 progressBeforeFirstError。
type routeStageMemo struct {
	Escalations []RouteEscalation `json:"escalations,omitempty"`
	// Shadow 是已经跑完的影子档，按影子档序号对齐，与 Escalations 分开存：影子档不占真实档的下标，挂上或
	// 摘掉影子档都不让真实档的重放错位（见 WithShadow）。以失败收尾时不保留，重试时影子档重新调用——
	// 它不影响路由，重新调用只多花一次调用。
	Shadow []RouteEscalation `json:"shadow,omitempty"`
	// Retry 是以失败收尾的次数，每个失败出口加一。重新调用的档把它带进调用身份：上一次
	// 失败之前问过的问题，答案从没兑付过，如果身份不变，调用方带着的旧答案会被套到这次
	// 内容可能已经变了的问题上。中断续跑不加，提问 ID 保持稳定。
	Retry int `json:"retry,omitempty"`
}

func init() {
	compose_v2.RegisterValueType[*routeStageMemo]("*agent_v2.routeStageMemo")
}

// finiteOrZero 把非有限数换成 0。
func finiteOrZero(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

// finiteScores 返回丢掉非有限数之后的分数拷贝；没有可保留的项时返回 nil。
func finiteScores(scores map[string]float64) map[string]float64 {
	if len(scores) == 0 {
		return nil
	}
	out := make(map[string]float64, len(scores))
	for k, v := range scores {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// escalationFor 把一档没被采纳的判断整理成可序列化的记录。
func escalationFor(i int, reason string, dec RouteDecision) RouteEscalation {
	e := RouteEscalation{
		DeciderIndex: i, Reason: reason,
		Target: dec.Target, Confidence: finiteOrZero(dec.Confidence), Scores: finiteScores(dec.Scores), Source: dec.Source,
	}
	if reason == RouteReasonInvalidConfidence {
		e.Err = fmt.Sprintf("confidence %v is outside [0, 1]", dec.Confidence)
	}
	return e
}

// judgeRouteDecision 返回一档决策不被采纳的原因，采纳时返回空串。
func judgeRouteDecision(dec RouteDecision, branchSet map[string]bool, st routeDeciderStage) string {
	switch {
	case dec.Target == "":
		return RouteReasonAbstain
	case !branchSet[dec.Target]:
		return RouteReasonUnknownTarget
	case math.IsNaN(dec.Confidence) || dec.Confidence < 0 || dec.Confidence > 1:
		return RouteReasonInvalidConfidence
	case dec.Confidence < st.threshold(dec.Target):
		return RouteReasonLowConfidence
	default:
		return ""
	}
}

// decisionRouter 把决策方、classifier、兜底分支串成升级链，包成 classify 节点调用的
// Router。路由决策事件只在这里发，每次路由恰好一条。
func (r *RoutingAgent) decisionRouter(parentSession *RunSession) RoutingAgentRouter {
	agentName := r.Name()
	candidates := r.routingChoices()
	branchSet := make(map[string]bool, len(candidates))
	for _, c := range candidates {
		branchSet[c.Name] = true
	}
	deciders := append([]routeDeciderStage(nil), r.deciders...)
	var classify classifierStageFunc
	if r.classifier != nil {
		classify = r.classifierStage(parentSession, branchSet)
	}
	hasFallback := r.fallback != nil

	return func(ctx context.Context, input *schema.Message) (compose_v2.NodeID, error) {
		// 三份记录：chain 是真实档的（按真实档下标对齐，交给后面的档、决定重判起点），shadows 是影子档的
		// （按影子档序号对齐），escalations 是两者按执行顺序合起来的（给事件与 RouteUndecidedError）。影子档
		// 不占真实档的下标：挂上或摘掉影子档，真实档的重放对齐与调用身份都不变（见 WithShadow）。
		chain := make([]RouteEscalation, 0, len(deciders))
		var shadows []RouteEscalation
		escalations := make([]RouteEscalation, 0, len(deciders))
		var lastErr error
		// 重跑时读回上一次已经跑完的档，见 routeStageMemo。
		var replay, shadowReplay []RouteEscalation
		retry := 0
		if memo, ok := compose_v2.GetNodeState[*routeStageMemo](ctx); ok && memo != nil {
			replay, shadowReplay, retry = memo.Escalations, memo.Shadow, memo.Retry
		}
		save := func() {
			if err := compose_v2.SaveNodeState(ctx, &routeStageMemo{Escalations: cloneEscalations(chain), Shadow: cloneEscalations(shadows), Retry: retry}); err != nil {
				// 记不下来只影响 Resume 时这一档会被重新调用，不影响本次路由，所以不让它失败。
				slog.WarnContext(ctx, "routing: failed to save decider progress", "agent", agentName, "err", err)
			}
		}
		record := func(e RouteEscalation) {
			chain = append(chain, e)
			escalations = append(escalations, e)
			save()
		}
		ci, si := 0, 0 // 真实档下标、影子档序号
		for _, st := range deciders {
			if st.shadow {
				j := si
				si++
				if j < len(shadowReplay) && shadowReplay[j].DeciderIndex == j {
					shadows = append(shadows, shadowReplay[j])
					escalations = append(escalations, shadowReplay[j])
					continue
				}
				e, err := runShadowStage(ctx, st, j, retry, input, candidates, chain, branchSet, agentName)
				if err != nil {
					return "", err
				}
				shadows = append(shadows, e)
				escalations = append(escalations, e)
				save()
				continue
			}
			i := ci
			ci++
			if i < len(replay) && replay[i].DeciderIndex == i {
				chain = append(chain, replay[i])
				escalations = append(escalations, replay[i])
				if replay[i].Reason == RouteReasonError {
					lastErr = errors.New(replay[i].Err)
				}
				continue
			}
			// 输入、候选与前面各档的结果每次给一份拷贝：决策方是业务代码，改了它们不该
			// 影响分支收到的输入，也不该影响下一档。前面各档的结果只含真实档：影子档对路由不可见。
			req := RouteRequest{
				Input:       copyInput(input),
				Candidates:  append([]RoutingChoice(nil), candidates...),
				Escalations: cloneEscalations(chain),
			}
			// 每一档一个独立的调用身份：提问 ID 与 RunSubAgent 的子命名空间都按它派生，
			// 不同档的提问不共用答案位，子 agent 也不会与 classifier 那一档撞命名空间。
			// 以失败收尾后重新调用时身份带上重试代数，见 routeStageMemo.Retry。身份里不必带
			// 节点的访问序号：子命名空间、提问 ID、私有态键各自已经含它。
			dctx := compose_v2.WithCurrentToolCallContext(WithoutHistoryCtx(ctx), deciderCallID(i, retry), i)
			dec, err := st.decide(dctx, req)
			if err != nil {
				if compose_v2.MustPropagate(ctx, err) {
					return "", err
				}
				slog.WarnContext(ctx, "routing decider failed, escalating",
					"agent", agentName, "decider", i, "err", err)
				lastErr = err
				record(RouteEscalation{DeciderIndex: i, Reason: RouteReasonError, Err: err.Error()})
				continue
			}
			if reason := judgeRouteDecision(dec, branchSet, st); reason != "" {
				record(escalationFor(i, reason, dec))
				continue
			}
			conf := dec.Confidence
			EmitRouteDecisionEvent(ctx, &RouteDecisionAction{
				AgentName: agentName, Target: dec.Target,
				Stage: RouteStageDecider, DeciderIndex: i, Confidence: &conf, Scores: finiteScores(dec.Scores), Source: dec.Source,
				Escalations: escalations,
			})
			return compose_v2.NodeID(dec.Target), nil
		}

		if classify != nil {
			out, err := classify(ctx, input, retry)
			if err != nil {
				// 必须往上抛的错误（典型是 classifier 在问用户）不动进度：Resume 时要原样
				// 重放前面各档，才走得回 classifier 去兑现那个提问。classifier 里几路都定了
				// 路由（SignalConflictError）也在其中：它带着 ErrRunAborted，是终局失败，
				// 存档随之清掉，谈不上从哪一档重判；同一批里的几个 route_to 不走到这里，
				// 排在前面的生效。不在其中的只剩框架自己的不变量被破坏（classify 不在节点
				// 执行里、铸不出收件人）：按失败出在 classifier 处理，决策方的结果留着重放；
				// 有档报错时从那一档重新调用。
				if !compose_v2.MustPropagate(ctx, err) {
					saveRetryProgress(ctx, agentName, progressBeforeFirstError(chain, chain), retry)
				}
				return "", err
			}
			if out.target == "" {
				// classifier 没给出可用目标，又没有兜底分支可交：和决策方无处升级是同一种失败，
				// 报错而不是静默结束。重试时决策方的结果留着重放、只重判 classifier，有档报错时
				// 从那一档起。
				saveRetryProgress(ctx, agentName, progressBeforeFirstError(chain, chain), retry)
				if out.err != nil {
					lastErr = out.err
				}
				return "", &RouteUndecidedError{AgentName: agentName, Escalations: escalations,
					ClassifierFailure: out.failure, LastErr: lastErr}
			}
			EmitRouteDecisionEvent(ctx, &RouteDecisionAction{
				AgentName: agentName, Target: out.target, Stage: out.stage,
				ClassifierFailure: out.failure, Escalations: escalations,
			})
			return compose_v2.NodeID(out.target), nil
		}
		if hasFallback {
			EmitRouteDecisionEvent(ctx, &RouteDecisionAction{
				AgentName: agentName, Target: fallbackBranchName, Stage: RouteStageFallback, Escalations: escalations,
			})
			return fallbackBranchName, nil
		}
		// 失败的原因是整条链没拿定主意：有档报错时从第一个报错的档重新判，没有时从头判。
		saveRetryProgress(ctx, agentName, progressBeforeFirstError(chain, nil), retry)
		return "", &RouteUndecidedError{AgentName: agentName, Escalations: escalations, LastErr: lastErr}
	}
}

// progressBeforeFirstError 返回重试时要重放的档：截到第一个报错的档之前；没有档报错时
// 返回 whenNoError。
//
// 报错的档要重新调用，不然这次的报错会被当成已完成来重放，一次临时故障就成了永久失败；
// 它后面的档看到的前序结果会变，也要重新调用。之前的档照旧重放，用户在那些档里答过的
// 问题不会再问（答案不进存档，只随那一次 Resume 传入）。
func progressBeforeFirstError(escalations, whenNoError []RouteEscalation) []RouteEscalation {
	if i := firstErroredStage(escalations); i >= 0 {
		return escalations[:i]
	}
	return whenNoError
}

// saveRetryProgress 在 classify 节点以失败收尾前写下重试时要重放的档，并把失败重试代数
// 在本次代数 retry 的基础上加一，被重新调用的档据此换新身份（见 routeStageMemo.Retry）。
//
// 加一只在这里做，三个失败出口都经过它：各自写 retry+1 时，任何一处写错（比如写成常数，
// 把代数当成「重试过没有」的旗标）都只有连续失败两次以上才会暴露。
func saveRetryProgress(ctx context.Context, agentName string, keep []RouteEscalation, retry int) {
	if err := compose_v2.SaveNodeState(ctx, &routeStageMemo{Escalations: cloneEscalations(keep), Retry: retry + 1}); err != nil {
		slog.WarnContext(ctx, "routing: failed to save decider progress for retry", "agent", agentName, "err", err)
	}
}

// deciderCallID 是第 i 档决策方的调用身份。重试代数为 0 时是 "route_decider_<i>"，之后
// 带上 "_retry<代数>"。
func deciderCallID(i, retry int) string {
	if retry == 0 {
		return fmt.Sprintf("route_decider_%d", i)
	}
	return fmt.Sprintf("route_decider_%d_retry%d", i, retry)
}

// firstErroredStage 返回第一个报错的档在 escalations 里的下标，没有时返回 -1。传进来的是真实档的记录：
// 影子档报错不挪动重判的起点（它的记录另存，见 routeStageMemo.Shadow）。
func firstErroredStage(escalations []RouteEscalation) int {
	for i, e := range escalations {
		if e.Reason == RouteReasonError {
			return i
		}
	}
	return -1
}

// runShadowStage 调第 j 个影子档，返回它的记录。它永不采纳：够得上门槛的记 shadow，其余记平常的原因；
// 报错与 panic 都记为 error。只有必须上抛的错误（含它里面提问的中断）原样返回。
func runShadowStage(ctx context.Context, st routeDeciderStage, j, retry int, input *schema.Message,
	candidates []RoutingChoice, chain []RouteEscalation, branchSet map[string]bool, agentName string) (RouteEscalation, error) {
	req := RouteRequest{
		Input:       copyInput(input),
		Candidates:  append([]RoutingChoice(nil), candidates...),
		Escalations: cloneEscalations(chain),
	}
	sctx := compose_v2.WithCurrentToolCallContext(WithoutHistoryCtx(ctx), shadowCallID(j, retry), j)
	dec, err := callShadowDecider(sctx, st.decide, req)
	if err != nil {
		if compose_v2.MustPropagate(ctx, err) {
			return RouteEscalation{}, err
		}
		slog.WarnContext(ctx, "routing shadow decider failed", "agent", agentName, "shadow", j, "err", err)
		return RouteEscalation{DeciderIndex: j, Reason: RouteReasonError, Err: err.Error(), Shadow: true}, nil
	}
	reason := judgeRouteDecision(dec, branchSet, st)
	if reason == "" {
		reason = RouteReasonShadow
	}
	e := escalationFor(j, reason, dec)
	e.Shadow = true
	return e, nil
}

// callShadowDecider 调影子档。它 panic 时记为这一档报错，不让整次路由失败：影子档对路由不可见，而它的用途
// 正是在线上并跑一个还没验证过的决策方（见 WithShadow）。普通档的 panic 照旧交给节点兜底、整次路由失败。
func callShadowDecider(ctx context.Context, d RouteDecider, req RouteRequest) (dec RouteDecision, err error) {
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			panicerr.Log(ctx, slog.LevelError, "routing: shadow decider panicked", pe)
			err = panicerr.WrapOpaque("shadow decider panicked", pe)
		}
	}()
	return d(ctx, req)
}

// shadowCallID 是第 j 个影子档的调用身份，与真实档的（deciderCallID）分开编号。
func shadowCallID(j, retry int) string {
	if retry == 0 {
		return fmt.Sprintf("route_shadow_%d", j)
	}
	return fmt.Sprintf("route_shadow_%d_retry%d", j, retry)
}

// copyInput 给决策方一份输入的拷贝：结构体、Annotations、Metadata、ContentBlocks 及其
// 中每个内容块各拷一层。nil 原样返回。
//
// Annotations 必须拷：并行编排里兄弟分支拿到的是同一个输入指针，决策方写这个 map 的同时
// 兄弟分支在读，是并发读写同一个 map，进程会直接崩溃。
func copyInput(m *schema.Message) *schema.Message {
	if m == nil {
		return nil
	}
	c := *m
	c.Annotations = maps.Clone(m.Annotations)
	if m.Metadata != nil {
		md := *m.Metadata
		c.Metadata = &md
	}
	if m.ContentBlocks != nil {
		c.ContentBlocks = make([]*schema.ContentBlock, len(m.ContentBlocks))
		for i, b := range m.ContentBlocks {
			if b == nil {
				continue
			}
			bc := *b
			c.ContentBlocks[i] = &bc
		}
	}
	return &c
}

// cloneEscalations 深拷贝升级记录，连同每条的 Scores。
func cloneEscalations(in []RouteEscalation) []RouteEscalation {
	if len(in) == 0 {
		return nil
	}
	out := make([]RouteEscalation, len(in))
	for i, e := range in {
		e.Scores = maps.Clone(e.Scores)
		out[i] = e
	}
	return out
}
