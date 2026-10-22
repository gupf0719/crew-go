// Package agent_v2 — approval.go：工具执行审批/权限层（harness P1）。
//
// LLM emit tool_call 后、框架执行业务工具前插一道审批 gate：按 ApprovalMode + 业务启发式
// + don't-ask-again 规则决定 放行 / 拒绝 / 抛 caller 审批。Ask 决策复用 ask_user 同款
// InterruptError + 稳定 ID + GetResumeAnswer 范式（骑现有 cp 挂起 / Resume 机制，零新并发原语）。
// 拦截点在 basic_agent.go makeToolsFn 的 runOne 内（inline + fanout 都过），与 ISSUE-81
// fanout 的 MergeInterrupts 自动协同（多工具同时需审批一次问完）。
//
// 对齐 harness scoping 决策（harness-survey.md §四）：框架只给机制 + 数据模型 + 启发式 seam，
// **不内置** shell allowlist 等具体安全策略（通用编排框架越界，规则内容是业务策略）。
package agent_v2

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"crew-go/compose_v2"
	"crew-go/internal/panicerr"
	"crew-go/schema"
)

// ApprovalMode 工具执行审批模式。
type ApprovalMode int

const (
	// ApprovalAuto 默认——还没形成审批结论的工具调用直接放行（零开销）；先前形成过结论的调用续跑时照样按结论走
	// （见 approvalDecisionOf）。
	ApprovalAuto ApprovalMode = iota
	// ApprovalAsk 每个业务工具执行前需用户审批（除非启发式放行或已 approve_always）。
	ApprovalAsk
	// ApprovalAskWrites 是「只读放行」档：声明只读的工具（tool.IsReadOnly，见 tool.ReadOnlyDeclarer）直接执行，
	// 其余的执行前问人——没声明只读的一律按有副作用处理（拿不准就问）。业务一个工具都没声明时等同
	// ApprovalAsk，不会意外放行。
	//
	// 启发式照旧可以改判任何一次调用：返 Ask / Deny 的连只读工具也问 / 拒，返 Default 时才落到只读判定。
	// approve_always 照常生效——那是用户明说的「这件事以后别问」，档位只决定默认。
	ApprovalAskWrites
)

// ApprovalDecision 单个工具调用的审批决策（ApprovalHeuristic 返回）。
type ApprovalDecision int

const (
	// ApprovalDecisionDefault 交回 ApprovalMode 默认（Auto→放行 / Ask→询问）。
	ApprovalDecisionDefault ApprovalDecision = iota
	// ApprovalDecisionAllow 直接放行（启发式判定安全，跳过询问）。
	ApprovalDecisionAllow
	// ApprovalDecisionAsk 强制询问（即使 mode=Auto）。
	ApprovalDecisionAsk
	// ApprovalDecisionDeny 直接拒绝执行。
	ApprovalDecisionDeny
)

// ApprovalHeuristic 是业务可插拔的审批启发式 seam——对每个工具调用返回决策，返回
// ApprovalDecisionDefault 时交回 ApprovalMode。框架**不内置** shell allowlist 等具体安全策略
// （通用编排框架越界，对齐 harness scoping 决策）——业务按自己的安全模型实现。
//
// # 每次工具调用只判一次，判的就是执行的
//
// 一次工具调用的内容（`OnToolStart` 改写之后的工具名与参数）在这一批第一次交给工具执行时定下，之后的每次
// 执行都按这份内容（见 fixToolCallContent）；审批结论在第一次形成时随这次调用的审批记录进存档，中断续跑、
// 失败续跑时原样沿用、不再调启发式（需要配 checkpointer；没配时重试是从头跑的新调用）。所以：
//   - 启发式判过的、用户在审批问题里看到并批过的，就是实际执行的那一份——续跑时 `OnToolStart` 再改写
//     （注入时间戳、请求号，或改了工具名）对这次调用不生效；
//   - 实现不必每次给出相同结论——调模型判断、按时间或额度判断都可以——用户的答复不会被一次重新判定推翻；
//   - 审批配置在两次执行之间变了（滚动发布时加上或去掉启发式、换 ApprovalMode）也一样：已经形成结论的调用
//     沿用它，新配置只管还没形成结论的调用。
//
// args 是这次调用参数的一份拷贝，只供判断：就地改它不影响执行（要改参数用 OnToolStart）。
//
// 判断本身失败（模型超时、返回非法值）时返回 ApprovalDecisionAsk 交给人，别返回 Allow；panic 不形成结论（报成审批启发式
// 自己的错，不算到工具头上），续跑时会再调一次。结论不会过期：额度用完、开关关掉，这次调用先前的 Allow 在续跑时照样作数——它是对
// 这次调用的准入，不是每次执行前的检查。启发式跑在自己的相位里：它里面的 compose_v2.Ask、SaveNodeState、
// RunSubAgent 用自己的计数与私有态，碰不到这次工具调用的；但签名交不出中断，在里面提问问不到人。
//
// **可重入**：fanout（ISSUE-81 N>=2 tool_call）下框架可能并发对多个工具调用（不同 goroutine），
// 实现读写共享状态需 thread-safe。
type ApprovalHeuristic func(ctx context.Context, toolName string, args map[string]any) ApprovalDecision

// 审批 InterruptError 的 Query 选项 + Resume 答案约定（业务展示 Options 给用户、按答案 Resume）。
const (
	// ApprovalOptionApprove 本次放行。
	ApprovalOptionApprove = "approve"
	// ApprovalOptionApproveAlways 本任务（RunID）内**这个 agent 的同一个工具、同样的参数**不再询问
	// （don't-ask-again；跨 Resume 持久需 WithResumeSession）。规则按 RunID 分：NextTask 换了 RunID 就是新的一套；
	// 复用同一个 RunID 开新任务时由编排入口清掉。**只对答应它的那个 agent 实例生效**：别的 agent（编排的其他成员、
	// 嵌套调用的被调方或调用方）同名的工具是另一个工具，照样要问；agent 被重建（每个请求经工厂重建的接入）就再问一次
	// （见 approvedToolsKey）。整个工具都不再问用 ApprovalOptionApproveAlwaysTool。
	ApprovalOptionApproveAlways = "approve_always"
	// ApprovalOptionApproveAlwaysTool 是**工具级**的 don't-ask-again：这个工具的**任何**
	// 调用都不再问。
	//
	// 跟 ApprovalOptionApproveAlways 分成两个答案而不是一个，是因为用户心里的「一直允许」
	// 通常指**这件事**（"读这个项目的文件"），而不是**这个能力**（"读任何文件，永远"）。
	// 两者混成一个答案时，框架只能猜——而猜宽了就是把用户没打算给的权限给出去。
	// 拆开之后「我批的是这件事」和「我批的是这个工具」在协议层就分开了。
	ApprovalOptionApproveAlwaysTool = "approve_always_tool"
	// ApprovalOptionDeny 拒绝执行（返工具级 error 给 LLM，不 abort 整 Run）。
	ApprovalOptionDeny = "deny"
)

// ApprovalInterruptReason 是工具审批中断的 Reason（InterruptError 与它的 AskQuery 上都填）。前端适配层
// （agui、a2a server）据它认出「这一问是审批」，按审批的答复格式校验与展示。
const ApprovalInterruptReason = "tool_approval"

// NormalizeApprovalAnswer 把一条审批答复文本归一成四个选项之一（ApprovalOptionApprove /
// ApprovalOptionApproveAlways / ApprovalOptionApproveAlwaysTool / ApprovalOptionDeny）。不分大小写、去掉首尾
// 空白；认得的词：approve、yes、y、allow → approve；approve_always、always → approve_always；
// approve_always_tool；deny、no、n → deny。其余返回 ok=false。
//
// 词表只有这一份：审批层续跑时用它（认不出的按拒绝处理，fail-closed），前端适配层用它校验答复（认不出的
// 不交给审批层、请对方重答）。"Yes."、"好的" 这类自然语言认不出——词表一放宽，认错一次就执行了用户没
// 批准的工具。
func NormalizeApprovalAnswer(text string) (option string, ok bool) {
	switch strings.ToLower(strings.TrimSpace(text)) {
	case ApprovalOptionApprove, "yes", "y", "allow":
		return ApprovalOptionApprove, true
	case ApprovalOptionApproveAlways, "always":
		return ApprovalOptionApproveAlways, true
	case ApprovalOptionApproveAlwaysTool:
		return ApprovalOptionApproveAlwaysTool, true
	case ApprovalOptionDeny, "no", "n":
		return ApprovalOptionDeny, true
	default:
		return "", false
	}
}

// approvedToolsKey 是 don't-ask-again 规则集在 RunSession KV 里的 key 前缀（不走 cp）。规则只在
// 一个任务里有效，实际的 key 带上任务的身份，见 [approvalRulesKey]。
//
// 规则的 key 是 **`agent 实例身份 + "\x00" + 工具名 + "\x00" + 参数指纹`**，不是裸工具名。
//
// **为什么带 agent 实例身份**：工具名只在一个 agent 的工具表里唯一，别的 agent 同名的可能是另一个工具（沙箱里的
// delete 与生产的 delete）。规则集按任务存、而嵌套调用的被调方与编排成员跟父共用一个任务——不带身份时，用户在
// 沙箱子 agent 里对 delete 答的「一直允许」，父 agent 自己那个生产 delete 同样参数时静默执行（实测）；Sequential
// 里 a 答的「一直允许」，b 的同名工具不经询问就执行（实测）。身份是构造时铸的（[Config] 的 approvalOwner），不用
// agent 名（框架不保证唯一）也不用编排位置（那是历史归属借来的量，内部决策 LLM 没有它）。代价：agent 被重建就是
// 新的身份，规则跟着丢、再问一次（方向安全）——每个请求经工厂重建 agent 的接入（agui）在同一个任务的下一次中断
// 之后会再问一遍；同一个实例挂在两个位置时两处共用规则（工具表相同，能力也相同）。
//
// **为什么必须带参数维度**：一条授权的有效期不该长于「用户还能预期到这个动作会发生」的
// 范围，而这个范围由**时间**（多久失效）和**空间**（覆盖多大动作集）两个维度共同决定，
// 风险大致是两者之积。我们的时间维度是「整个任务」（同一个 RunID 的一次 Run 连同它的全部续跑，
// 任务不结束就不失效）——那么空间维度就必须收紧到「这次要做的这件事」，否则一次
// `transfer_money(张三, 5000)` 的放行会顺带批准之后**任意金额转给任意人**。
//
// 工具级（覆盖该工具全部调用）保留为**显式的第二档**，见 ApprovalOptionApproveAlwaysTool。
const approvedToolsKey = "crew.approved_tools"

// approvalRulesKey 是 session s 当前任务的规则集在 RunSession KV 里的 key。
//
// **按任务分，任务的身份是 RunID**（见 RunSession.RunID）。values 表活得比任务长（`NextTask`
// 换 RunID、保留 values），也可能被几个 session 共用（半共享子 session、ForkLane）；规则集只按
// session 放一份的话，它的边界就得靠「新任务开始时清一下」来维持——而那一刻不一定经过框架
// （业务换了 RunID 自己接着跑），经过了又会把共用这张表的另一个任务的规则一起清掉。按 RunID
// 分之后换任务自然是一套新的规则，别的任务的规则也碰不到。旧任务那份留在 values 表里，每个用过
// approve_always 的任务一小份。
func approvalRulesKey(s *RunSession) string { return approvedToolsKey + "\x00" + s.RunID }

// approvalRuleKey 把「工具 + 这次的参数」拼成规则 key。
//
// 参数指纹默认是**参数原文精确匹配**。「哪些参数算同一件事」是**业务策略**不是框架能定的
// ——`bash` 可能只该比前几个词、文件工具可能只该比目录前缀、而转账必须整串都比。所以
// 框架只给机制（`WithApprovalScope` 让业务插自己的归一化），默认取最保守的那档。
// 这跟本文件开头写的分工一致：框架给机制，安全策略业务自负。
func approvalRuleKey(cfg *Config, tc schema.ToolCall) string {
	fp := tc.Function.Arguments
	if cfg != nil && cfg.ApprovalScope != nil {
		fp = cfg.ApprovalScope(tc)
	}
	return approvalToolKey(cfg, tc.Function.Name) + "\x00" + fp
}

// approvalToolKey 是「这个 agent 的这个工具」：规则 key 的前半，工具级规则在它后面接通配（见 approvedToolsKey）。
func approvalToolKey(cfg *Config, name string) string {
	owner := ""
	if cfg != nil {
		owner = cfg.approvalOwner
	}
	return owner + "\x00" + name
}

// approvalToolWildcard 是工具级规则的指纹占位——`approvalToolKey(这个 agent, 工具名) + "\x00" + 这个` 表示「这个 agent 的
// 该工具全放行」。
// 用控制字符是为了不可能跟真实参数指纹相撞（合法 JSON 参数里不会出现裸 \x01）。
const approvalToolWildcard = "\x01*"

// approvalMu 守护 approve_always 规则集的 read-modify-write——fanout 下多 goroutine 可能并发
// markToolApproved（同 Resume 多工具 approve_always），SetValue/GetValue 各自线程安全但
// get-modify-put 序列需整体串行。
var approvalMu sync.Mutex

// isToolApproved 查这次调用有没有被 don't-ask-again 规则覆盖。
//
// 两档都查：**这件事**（工具+参数指纹）和**这个工具**（通配）。
func isToolApproved(s *RunSession, cfg *Config, tc schema.ToolCall) bool {
	if s == nil {
		return false
	}
	approvalMu.Lock()
	defer approvalMu.Unlock()
	v, _ := s.GetValue(approvalRulesKey(s))
	m, _ := v.(map[string]bool)
	if m == nil {
		return false
	}
	return m[approvalRuleKey(cfg, tc)] || m[approvalToolKey(cfg, tc.Function.Name)+"\x00"+approvalToolWildcard]
}

// markToolApproved 记一条 don't-ask-again 规则。toolWide=true 时覆盖该工具全部调用。
func markToolApproved(s *RunSession, cfg *Config, tc schema.ToolCall, toolWide bool) {
	if s == nil {
		return
	}
	approvalMu.Lock()
	defer approvalMu.Unlock()
	key := approvalRulesKey(s)
	v, _ := s.GetValue(key)
	m, _ := v.(map[string]bool)
	if m == nil {
		m = map[string]bool{}
	}
	if toolWide {
		m[approvalToolKey(cfg, tc.Function.Name)+"\x00"+approvalToolWildcard] = true
	} else {
		m[approvalRuleKey(cfg, tc)] = true
	}
	s.SetValue(key, m)
}

// clearApprovalRulesForNewTask 在**新任务开始**那一刻丢弃这个任务名下的 don't-ask-again 规则。
//
// **它只管规则集，而且是产品决策不是正确性补丁**——规则的 key 是「agent 实例身份 + 工具名 + 参数指纹」
// （跨任务不会误命中），它该活多久是个取舍：对照下来各家从「一次 run」到「跨进程落盘」都有，我们取最保守
// 的那档：一个任务。
//
// 规则集本来就按 RunID 分（[approvalRulesKey]），换了 RunID 的新任务碰不到旧的；这里管的是
// **复用同一个 RunID 开新任务**的写法（顶层 Run 被判成新任务，而 session 没换 RunID）。
//
// 「这次调用批过没有」那份**不在这里**：它已经搬进 tools 节点的私有中断态
// （`toolCallMemo.Approved`），新任务是新 cp、天然为空，**隔离不依赖这个函数**
// （有专门的用例把清除点删掉仍绿）。它原先在这里，而那时这个函数是正确性补丁
// ——谁忘了调、或判据跟别处走岔，跨任务泄漏立刻静默复现。
//
// **为什么要清**：授权的时间维度不该长于「用户还能预期到这个动作会发生」的范围。用户在
// 任务 A 里说「以后别再问这个工具了」，语境是任务 A（"读这个项目的文件"）；他放弃 A、
// 转头发起任务 B 时，那句话不该继续生效。实测过：任务 A 里答的 approve_always，在任务 B
// 里让同一个工具**静默执行、不再询问**。
//
// **本函数不自带判据**，调用点只有两处：
//
//   - `enterRunTask` 判定这次 Run 是新任务时（判据是「上面有没有这个 RunID 所属的任务」，见
//     inTask，不是 ns 字符串）：那一刻同时宣告忽略旧存档，授权跟着清是同一语义的补完。不清
//     就是**存档认为是新任务、授权认为是老任务**——那正是这个缺陷的形状；
//   - [ClearApprovalRules]：业务自己宣告语境变了。
//
// **嵌套不清**（inTask 为真的那条路）：子 agent 的 Run 是父任务的一部分，清了等于每个子
// agent 各问一遍。
func clearApprovalRulesForNewTask(s *RunSession) {
	if s == nil {
		return
	}
	approvalMu.Lock()
	defer approvalMu.Unlock()
	key := approvalRulesKey(s)
	// 这个 RunID 名下没有规则就不写：每个新任务都写一个空键的话，长期复用一个 session、每条消息 NextTask 的
	// 聊天循环里会话 KV 随消息数线性增长（审计实测），而那些键什么都不装。
	if _, ok := s.GetValue(key); !ok {
		return
	}
	s.SetValue(key, nil)
}

// ClearApprovalRules 让业务主动丢弃 session 当前任务（它的 RunID）名下的 don't-ask-again 规则。
//
// 规则本来只在一个任务里有效（换 RunID 就是一套新的，见 approvalRulesKey），框架判定新任务时
// 也会清（见 clearApprovalRulesForNewTask）。这个导出函数是给「语境变了但任务没换」的业务用
// 的——比如一个长 Run 里跨了多个用户意图，业务自己知道。
func ClearApprovalRules(s *RunSession) { clearApprovalRulesForNewTask(s) }

// callApprovalFns 是「这次调用的审批记录」的读写回调——由调用方（tools 节点的 runOne）注入。
// 两样记录：审批结论（`decided` / `decide`，见 [approvalDecisionOf]）与「用户批过没有」
// （`is` / `mark`）。
//
// **为什么不再自己读写 session KV**：「批过没有」原先按位置 ID 存在 session 上，而位置 ID
// 跨任务重复、session 又活得比任务长 ⟹ 跨任务泄漏。现在两样都在 tools 节点的
// **节点私有中断态**里（`toolCallMemo.Approved` / `Decided`），键跟 tool memo 同一把——那个容器
// 新任务就是空的，泄漏结构上不可能。详见 `toolCallMemo.Approved` 的注释。
//
// 这里做成回调而不是直接引用 memo，是为了让 `checkToolApproval` 保持只依赖入参、
// 不反向依赖 tools 节点的内部结构。
type callApprovalFns struct {
	is      func() bool
	mark    func()
	decided func() (ApprovalDecision, bool)
	decide  func(ApprovalDecision)
	// heuristicPhase 是调启发式时开的相位名（见 approvalDecisionOf），askPhase 是审批问题占提问序号
	// 时开的相位名（见 checkToolApproval），都要在这次节点执行里唯一；空 = 不开相位。
	heuristicPhase string
	askPhase       string
}

// approvalActive 报当前配置下审批层对业务工具生效没有：ApprovalAsk，或配了启发式。不生效时（ApprovalAuto 且没有
// 启发式）还没形成结论的调用直接放行，审批层不写任何状态。
func approvalActive(cfg *Config) bool {
	return cfg.ApprovalMode != ApprovalAuto || cfg.ApprovalHeuristic != nil
}

type callApprovalKey struct{}

// withCallApproval 注入「这次调用的审批记录」的读写回调。
func withCallApproval(ctx context.Context, fns callApprovalFns) context.Context {
	return context.WithValue(ctx, callApprovalKey{}, fns)
}

func isCallApproved(ctx context.Context) bool {
	fns, _ := ctx.Value(callApprovalKey{}).(callApprovalFns)
	return fns.is != nil && fns.is()
}

func markCallApproved(ctx context.Context) {
	if fns, _ := ctx.Value(callApprovalKey{}).(callApprovalFns); fns.mark != nil {
		fns.mark()
	}
}

// approvalDecisionOf 返回这次调用的审批结论；第二个返回值 false 表示审批对它不生效、直接放行。
//
// 结论只形成一次：这次调用有记下的结论就重放它（中断续跑、失败续跑），不看现在的配置；没有时按当前配置形成——审批
// 不生效时不形成（也不记），生效时取 ApprovalMode 的默认、配了启发式再由它覆盖，记进这次调用的审批记录。没注入
// 记录回调时（不经 tools 节点的形态）每次都现算。
//
// **为什么只形成一次**：结论在两次执行之间可能变——启发式的判断不一定每次相同（模型判断在拿不准的区间里会翻转，
// 按时间、额度判断的规则隔一阵再跑也会翻），配置也可能变（滚动发布时加上或去掉启发式、换 ApprovalMode）。原先每次
// 执行都现算、而且只在配了启发式时才记，结论一变就出这几种事，全都 `err == nil`（实测）：
//   - 用户对审批问题的答复被推翻：答了 deny，续跑判成 Allow（启发式翻转，或续跑时才加上判 Allow 的启发式、关掉了
//     审批），工具照样执行；答了 approve，续跑判成 Deny，工具被拒。
//   - 已经放行的调用被重新拦下：启发式判 Allow、工具自己提问，续跑时去掉了启发式（换成 ApprovalAsk），这次调用改走
//     审批，用户对工具问题的回答被当成审批答复。
//
// 结论记下之后不再看配置，这几件一起消失。RoutingAgent 的升级链是同一个做法（`routeStageMemo`）。
//
// 重放的前提是这次调用的内容没变：内容在这一批第一次交给工具执行时定下（fixToolCallContent），续跑时按定下的
// 内容执行，不按 `OnToolStart` 这一次的改写。不固定的话，两种做法各有一个反例（均审计实测）：只按调用重放，
// 续跑时被改写成 1000 的参数沿用了启发式对 100 的放行；按内容重判，钩子每次注入新值时每次续跑都重判，
// 启发式一改口，用户的拒绝就被推翻。
//
// readOnly 是这次派发的那件工具有没有声明只读（tool.IsReadOnly），只在 ApprovalAskWrites 档下用到；它跟
// 结论一起只判一次——结论记下之后工具换了声明也不推翻。
func approvalDecisionOf(ctx context.Context, cfg *Config, name string, args map[string]any, readOnly bool) (ApprovalDecision, bool, error) {
	fns, _ := ctx.Value(callApprovalKey{}).(callApprovalFns)
	if fns.decided != nil {
		if d, ok := fns.decided(); ok {
			return d, true, nil
		}
	}
	if !approvalActive(cfg) {
		return ApprovalDecisionDefault, false, nil
	}
	decision := ApprovalDecisionAsk
	switch cfg.ApprovalMode {
	case ApprovalAuto:
		decision = ApprovalDecisionAllow
	case ApprovalAskWrites:
		if readOnly {
			decision = ApprovalDecisionAllow
		}
	}
	if cfg.ApprovalHeuristic != nil {
		// 启发式跑在自己的相位里（同工具钩子，见 compose_v2.EnterPhase）：它要是调了 compose_v2.Ask、
		// SaveNodeState 或 RunSubAgent，用的是自己的提问与嵌套调用计数和自己那一格私有态，碰不到这次工具
		// 调用的。共用的话，续跑时它不再被调（结论重放），工具调用的提问序号就挪位：审批问题换 ID 重问，
		// 工具自己的问题拿到审批的答复（审计实测）。
		hctx := ctx
		if fns.heuristicPhase != "" {
			hctx = compose_v2.EnterPhase(ctx, fns.heuristicPhase)
		}
		// 给它一份拷贝：启发式只做判断，就地改了 args 也碰不到执行的那份。同一个 map 交下去的话，它的改动只在
		// 首跑生效（续跑时结论重放、不再调它），判的、问的、执行的就不是同一份（审计实测：用户确认退 500，续跑退了
		// 1000）。要改参数用 OnToolStart。
		d, err := runHeuristic(hctx, cfg.ApprovalHeuristic, name, cloneAnyMap(args))
		if err != nil {
			return ApprovalDecisionDefault, true, err // 不形成结论：续跑时再调一次
		}
		if d != ApprovalDecisionDefault {
			decision = d
		}
	}
	if fns.decide != nil {
		fns.decide(decision)
	}
	return decision, true, nil
}

// runHeuristic 调启发式并兜住它的 panic，报成启发式自己的错。不兜的话 panic 被工具调用那层的 recover 接住，报成
// 「tool "refund" panic recovered: …」，排查时以为是工具体崩了（审计实测）。
func runHeuristic(ctx context.Context, h ApprovalHeuristic, name string, args map[string]any) (d ApprovalDecision, err error) {
	defer func() {
		if r := recover(); r != nil {
			pe := panicerr.New(r)
			err = panicerr.Wrap(fmt.Sprintf("approval heuristic for tool %q panicked", name), pe)
			panicerr.Log(ctx, slog.LevelError, "agent_v2: 审批启发式 panic，已转成错误", pe, panicerr.KeyError, err.Error())
		}
	}()
	return h(ctx, name, args), nil
}

// approvalResult 是 checkToolApproval 的决策结果。
type approvalResult struct {
	proceed bool            // true → 继续执行工具
	denyMsg *schema.Message // proceed=false 且 err=nil → 拒绝的 tool_result（回 LLM）
	// err 非 nil → 这次调用停在审批这一步：*InterruptError（需 caller / 用户审批），或审批启发式 panic。
	err error
}

// checkToolApproval 在工具执行前查审批决策链（仅业务工具；minted 为真——这次派发的是框架铸的那件，
// 见 frameworkMintedTool——直接放行，问人、交出转交决定这类事审批无意义）：
//
//	这次调用记下的结论 / ApprovalMode 默认 → 启发式覆盖（每次调用只形成一次，见 approvalDecisionOf）
//	→（若 Ask）这次调用已准入过 → 本次 Resume 答案 → don't-ask-again 规则 fast-path → 首次抛中断
//
// **准入只发生一次**：一次调用经任何一条路被准入（结论是 Allow、用户批准、approve_always 规则放行）之后，续跑都
// 不再问、不再被推翻——调用方续跑时对同一个审批 ID 改答 deny 不推翻，不带原 session 续跑、规则丢了也不再问；
// 结论是 Deny 或用户拒绝时，拒绝结果记进工具账，同样不再变。判的、问的、执行的是同一份内容（一批调用的内容在
// 第一次执行时定下，见 fixToolCallContent；启发式拿到的是参数的拷贝）。
//
// Ask 决策且未 Resume → 抛 InterruptError（审批问题在自己的相位里取提问序号，见 NextAnswerSlot），骑现有 cp
// 挂起；Resume 后按答案 approve / approve_always（记规则）/ deny（拒绝，工具级 error 不 abort）。
// **三样状态、两个载体**：「这次调用的审批结论」与「这次调用已批过」记在 tools 节点的私有中断态里
// （`toolCallMemo.Decided` / `Approved`，与 tool memo 同一把键、随 cp 走）——跨 Resume 自动带着。
// `approve_always` / `approve_always_tool` 的**规则集**存 RunSession KV、按任务（RunID）分
// （不走 cp），规则只对答应它的那个 agent 实例生效（键带构造时铸的身份，见 approvedToolsKey），跨 Resume 持久需
// caller 传 `WithResumeSession` 复用同一 session（默认 Resume 新建空 session，规则丢失会重新询问）。
func checkToolApproval(ctx context.Context, cfg *Config, tc schema.ToolCall, args map[string]any, minted, readOnly bool) approvalResult {
	if minted {
		return approvalResult{proceed: true}
	}
	name := tc.Function.Name
	// 默认路径（Auto 模式、无启发式、这次调用也没有记下的结论）直接放行：只查一次记录，不写任何状态
	// （绝大多数 BasicAgent 走这）。
	decision, active, err := approvalDecisionOf(ctx, cfg, name, args, readOnly)
	if err != nil {
		return approvalResult{err: err}
	}
	if !active {
		return approvalResult{proceed: true}
	}
	switch decision {
	case ApprovalDecisionAllow:
		return approvalResult{proceed: true}
	case ApprovalDecisionDeny:
		return approvalResult{denyMsg: markToolDenied(schema.ToolResultMessage(tc.ID,
			fmt.Sprintf("[tool_denied] 工具 %q 被审批策略拒绝执行。不要重试同样的调用，也不要换别的工具绕开它；"+
				"换一个更稳妥的做法，或者向用户说明情况。", name)))}
	}

	// decision == Ask
	session := GetSession(ctx)
	// 审批问题在自己的相位里占提问序号，不占这次工具调用的：工具自己的提问从它自己的 0 号数起，审批问不问都不挪位。
	// 共用一个计数时，审批问不问（结论是不是 Ask）决定工具自己的问题落在第几号——这次调用在续跑之间改走审批或不再
	// 走审批，审批的答复就交给工具自己的问题、工具问题的答复被当成审批放行（实测）。
	slotCtx := ctx
	if fns, _ := ctx.Value(callApprovalKey{}).(callApprovalFns); fns.askPhase != "" {
		slotCtx = compose_v2.EnterPhase(ctx, fns.askPhase)
	}
	stableID, ansMsg, hasAns := compose_v2.NextAnswerSlot(slotCtx)
	// 0) **这次调用已经准入过就直接放行，排在本次答复之前**：准入只发生一次。批准之后工具可能已经开始执行（产生了
	//    副作用、问了自己的问题），续跑时调用方对同一个审批 ID 改答 deny 的话，工具只执行了一半（审计实测）；拒绝记进
	//    工具账之后同样改不回来，两边对称。
	if isCallApproved(ctx) {
		return approvalResult{proceed: true}
	}
	// 1) **本次 Resume 的显式答案优先于 don't-ask-again 规则**——防同名工具（同轮同名 tc ≥2）
	//    的 deny 被 sibling 的 approve_always 规则抢跑：per-call 显式答案永远先于规则 fast-path。
	if hasAns && ansMsg != nil {
		option, _ := NormalizeApprovalAnswer(ansMsg.Content) // 认不出时 option 为空，落进下面的拒绝
		switch option {
		case ApprovalOptionApprove:
			markCallApproved(ctx) // 记本调用已批：工具自身再抛中断时重跑不重复要审批
			return approvalResult{proceed: true}
		case ApprovalOptionApproveAlways:
			// 「这件事」——同工具同参数下次不再问，换了参数照样问
			markToolApproved(session, cfg, tc, false)
			markCallApproved(ctx)
			return approvalResult{proceed: true}
		case ApprovalOptionApproveAlwaysTool:
			// 「这个工具」——它的任何调用都不再问。用户得显式选这一档才拿得到。
			markToolApproved(session, cfg, tc, true)
			markCallApproved(ctx)
			return approvalResult{proceed: true}
		default: // deny / 空 / 未识别 → 拒绝（保守 fail-closed）
			return approvalResult{denyMsg: markToolDenied(schema.ToolResultMessage(tc.ID,
				fmt.Sprintf("[tool_denied] 用户拒绝执行工具 %q。不要用同样的参数再试；换一个更稳妥的做法，"+
					"或者向用户说明情况、问清楚再继续。", name)))}
		}
	}
	// 2) 无本次答案 → don't-ask-again fast-path：approve_always 规则命中。**规则存 RunSession KV——跨 Resume 持久需
	//    caller 传 WithResumeSession 复用同一 session 对象**（默认 Resume 新建空 session，规则会丢、重新询问）。
	//    经规则放行同样记下「这次调用已准入」：工具之后自己中断、续跑没带原 session 时规则跟着丢了，不记的话这次调用
	//    被再问一次审批，用户答 deny 就只执行了一半（审计实测）。
	if isToolApproved(session, cfg, tc) {
		markCallApproved(ctx)
		return approvalResult{proceed: true}
	}
	// 3) 首次 → 抛 InterruptError（Query.InterruptID 填稳定 ID 让 caller 配对答案）
	return approvalResult{err: &compose_v2.InterruptError{
		Reason: ApprovalInterruptReason,
		Query: &compose_v2.AskQuery{
			Question: fmt.Sprintf("是否允许执行工具 %q？参数：%s", name, tc.Function.Arguments),
			Options: []string{ApprovalOptionApprove, ApprovalOptionApproveAlways,
				ApprovalOptionApproveAlwaysTool, ApprovalOptionDeny},
			InterruptID: stableID,
			// **Reason 直接填在 query 上，别只填 InterruptError.Reason**：单个中断
			// 走的是「原样透出」（`basic_agent.go` 的 `interruptErrs[0]` /
			// `graph_loop.go` 的 `interruptRaws[0]`），不经 `MergeInterrupts`——
			// 而拷 ie.Reason 到 query 的动作只发生在 Merge 里。只填 ie.Reason 的话，
			// 单工具审批时 cp 里连「这是审批挂起」都读不出来，多工具时反而读得出，
			// 同一语义两条路径行为分叉。
			Reason: ApprovalInterruptReason,
			// 指向这次审批卡住的那个工具调用，给调用方 / 前端定位用：InterruptID 是哈希，反查不出是哪一个调用。
			// 框架自己不读它（放弃时按会话历史补结果，见 closeUnansweredToolCallsOnAbort）。
			ToolCallID: tc.ID,
			ToolName:   name,
		},
	}}
}

// WithApprovalMode 设置工具执行审批模式。三档：
//
//   - ApprovalAuto（默认）——全放行、零开销
//   - ApprovalAsk——每个业务工具执行前抛 InterruptError 等调用方审批（骑 HITL Resume 机制，用
//     WithResumeUserInputs 回注 approve / approve_always / approve_always_tool / deny）
//   - ApprovalAskWrites——只读放行：声明只读的工具直接执行，其余同 ApprovalAsk
//
// 框架工具（ask_user / activate_skill / final_output 等，按实例认）三档下都豁免。审批生效时（非 Auto，或配了
// 启发式）框架还会在这个 agent 的系统提示里写一段当前审批方式（见 [WithApprovalPrompt]），让模型事先知道
// 哪些调用会停下来等人、被拒了该怎么办，而不是靠试错。
//
// **只对设置它的这个 BasicAgent 生效**：编排里的每个 BasicAgent 各自设置；PlanAgent 现造的执行体
// （TmpSubAgent）经 [WithPlanTmpSubAgentOptions] 设置。
func WithApprovalMode(mode ApprovalMode) Option {
	return func(c *Config) { c.ApprovalMode = mode }
}

// WithApprovalPrompt 替换框架写进系统提示的那段「当前审批方式」说明（审批生效时才写，默认文案见
// approvalPrompt）。传空串 = 不写这一段（业务自己的系统提示里已经讲清楚时）。
func WithApprovalPrompt(text string) Option {
	return func(c *Config) { c.ApprovalPrompt = &text }
}

// approvalPrompt 是写进系统提示的「当前审批方式」说明，审批不生效时为空。
//
// 讲的是「会发生什么、该怎么做」，不讲内部机制：哪些调用会停下来等人；需要批准的就直接调用（系统会去问），
// 别先在回复里征求许可、别换工具绕开；被拒了别原样重试。不列工具名：哪些调用要批准是每次调用判一次的
// （启发式、approve_always 规则都参与），而工具清单可能随渐进披露变化，静态清单会跟实际对不上。
//
// 位置在 agent 的前言里（跟系统提示一起，每次模型调用都在、不进会话历史、不被压缩）；一个任务里审批配置
// 不变，前言是稳定的前缀。
func approvalPrompt(cfg *Config) string {
	if !approvalActive(cfg) {
		return ""
	}
	if cfg.ApprovalPrompt != nil {
		return *cfg.ApprovalPrompt
	}
	var head string
	switch cfg.ApprovalMode {
	case ApprovalAsk:
		head = "本次任务里，你调用的工具在执行前都要经过用户批准（少数框架内置工具除外）。"
	case ApprovalAskWrites:
		head = "本次任务里，只读的工具（只查询、不改动任何状态）直接执行，其余工具在执行前要经过用户批准。"
	default:
		head = "本次任务里，部分工具调用在执行前可能需要用户批准。"
	}
	return "【工具审批】" + head + "调用前不必先在回复里征求同意：直接调用，需要批准时系统会去问用户；也不要换别的工具" +
		"绕开审批。被拒绝时你会收到以 [tool_denied] 开头的结果——不要用同样的参数再试，换一个更稳妥的做法，" +
		"或者向用户说明情况、问清楚再继续。"
}

// ApprovalScope 把一次工具调用归一化成「这算哪件事」的指纹——决定 `approve_always`
// 这条规则覆盖多大范围。
//
// **为什么这是业务的事**：「哪些参数算同一件事」没有通用答案。`bash` 可能只该比命令的
// 前几个词（`git status` 放行不等于 `git push` 放行），文件工具可能只该比目录前缀，
// 而转账必须整串都比（金额和收款人各差一个字都是另一件事）。框架给机制、业务给策略，
// 跟 ApprovalHeuristic 同一条分工。
//
// 默认（不设）= 参数原文精确匹配，最保守的那档。
//
// 返回空串会让所有调用归成同一件事——等于退回工具级，慎用。
type ApprovalScope func(tc schema.ToolCall) string

// WithApprovalScope 设参数指纹归一化函数。nil 忽略（保持默认精确匹配）。
//
//	// bash 只按命令的前两个词判「同一件事」
//	agent_v2.WithApprovalScope(func(tc schema.ToolCall) string {
//	    if tc.Function.Name != "bash" { return tc.Function.Arguments }
//	    var a struct{ Cmd string `json:"command"` }
//	    _ = json.Unmarshal([]byte(tc.Function.Arguments), &a)
//	    f := strings.Fields(a.Cmd)
//	    if len(f) > 2 { f = f[:2] }
//	    return strings.Join(f, " ")
//	})
func WithApprovalScope(fn ApprovalScope) Option {
	return func(c *Config) {
		if fn != nil {
			c.ApprovalScope = fn
		}
	}
}

// WithApprovalHeuristic 注入业务审批启发式 seam（可选）——对每个工具调用返回
// Allow/Ask/Deny/Default。框架不内置具体策略（如 shell allowlist），业务按自己的安全模型实现。
// 与 ApprovalMode 协作：启发式返 Default 时交回 mode 默认。
func WithApprovalHeuristic(fn ApprovalHeuristic) Option {
	return func(c *Config) {
		if fn != nil {
			c.ApprovalHeuristic = fn
		}
	}
}
