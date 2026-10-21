package agent_v2

import (
	"context"
)

// routeGrant 是一次「路由能力授予」——**由 RoutingAgent 亲手交给它的分类器**，收到的人才有
// route_to 工具。规矩跟 [handoffGrant] 是同一套：
//
//   - 载体是签名（RunOption）不是环境（ctx）：只到达被调用的那一个对象，不会自己往下渗；
//   - 受让方按对象身份判（[sameEntity]：本人，或嵌入外壳 / `Unwrap()` 链上的那一个）；
//   - 往下走要**显式**再交一次：编排经 runMember 转授给自己的成员（ParallelAgent 的分支是并发的，
//     不转授），业务壳调 [RegrantRouteTo]，跑在被授予的那个 agent 自己图里的代码（GraphAgent 的节点、
//     BasicAgent 的工具与钩子）调 [RegrantFromNode]。
//
// # 为什么路由也要授予
//
// 曾经 RoutingAgent 把收件人放进分类器整段执行的 ctx，`WithRouteTool` 把 route_to 静态注册进
// toolkit，route_to 只看 ctx 里有没有收件人——资格是**环境**。于是分类器（业务壳、工具、钩子）在
// 同一段执行里**请教**另一个带 route_to 的 agent 时，那个 agent 的路由就成了分类器的路由：壳请教
// 一个 helper（它定 math）、自己用 annotation 定 chat ⟹ 走 math，`err == nil`（审计实测；上行
// 信号优先于最终消息解析）。转交那侧没有这个问题：transfer 工具要授予。
//
// 收件人仍在 ctx 里——它是**地址**（决定交给谁），不是**资格**（谁能做决定）。资格由授予定：没被
// 授予的 agent 连 route_to 都没有，产生不出路由信号。
type routeGrant struct {
	// grantee 是这份授予的受让方——RoutingAgent 调分类器时放进去的**那个 Agent 接口值本身**
	// （存成 any 的理由同 handoffGrant.grantee）。
	grantee any
}

// withRouteGrant 把路由授予交给**这一次**调用（RunOption）。传 nil 时不写——「上层没给」与「上层
// 给了一个空的」是同一件事。
func withRouteGrant(g *routeGrant) RunOption {
	return func(c *runConfig) {
		if g == nil {
			return
		}
		c.routeGrant = g
	}
}

// grantFor 校验这份授予是不是发给 self 的——是则返回它，不是（或压根没有）返 nil。每个消费它的
// 对象都必须自查，理由同 [handoffGrant.grantFor]。
func (g *routeGrant) grantFor(self any) *routeGrant {
	if g == nil || self == nil {
		return nil
	}
	if !sameEntity(g.grantee, self) {
		return nil
	}
	return g
}

// regrant 铸一份发给 child 的新授予。只有已经自查过的授予才该调它；nil 接收者返 nil。
func (g *routeGrant) regrant(child Agent) *routeGrant {
	if g == nil || isAgentNil(child) {
		return nil
	}
	return &routeGrant{grantee: child}
}

// RegrantRouteTo 把「发给 self 的路由授予」重新发一份给 to，返回可直接喂给 `to.Run` /
// [RunSubAgent] 的 opts。用法、self 为什么必须传、忘了调的下场，都跟 [RegrantHandoffTo] 相同：
//
//	func (w *myClassifier) Run(ctx context.Context, s *agent_v2.RunSession,
//	    in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
//	    return w.inner.Run(ctx, s, in, agent_v2.RegrantRouteTo(opts, w, w.inner)...)
//	}
//
// 把 opts 原样转发（`inner.Run(ctx, s, in, opts...)`）什么也不会发生——受让方仍是壳本人。忘了调
// 的话 to 那侧没有 route_to，模型只能用文字作答，RoutingAgent 从最终消息里解析目标（解析不出走
// fallback），**不报错**：「这一层不该替我定路由」是绝大多数调用的正确答案，框架分不出「你忘了」
// 和「你就是不想给」。
func RegrantRouteTo(opts []RunOption, self Agent, to Agent) []RunOption {
	if isAgentNil(self) || isAgentNil(to) {
		return opts
	}
	g := routeGrantFromOpts(opts).grantFor(self)
	if g == nil {
		return opts
	}
	out := make([]RunOption, 0, len(opts)+1)
	out = append(out, opts...)
	return append(out, withRouteGrant(g.regrant(to)))
}

// routeGrantFromOpts 把 opts 应用到一个弃用的 runConfig 上，读出里面的路由授予（只读）。
func routeGrantFromOpts(opts []RunOption) *routeGrant {
	var c runConfig
	for _, o := range opts {
		if o == nil {
			continue
		}
		o(&c)
	}
	return c.routeGrant
}

// ── 能力授予的打包与转授 ────────────────────────────────────────────────────

// memberGrants 是一次调用收到、**已自查过**（发给自己）的全部能力授予：转交、路由各一份，都可以
// 没有。编排经 runMember 把它们原样转授给成员——成员是编排的组成部分。ParallelAgent 例外：分支是
// 并发的，而一个收件人同一超步里只能有一个决定方，所以分支拿空授予（见 ParallelAgent 的 godoc）。
//
// 协调者另外给替它做决定的那个 agent **新铸**一份自己那一类的授予（HandoffAgent 给候选铸转交、
// RoutingAgent 给分类器铸路由）：地址是它自己的收件人，转发下来的同类授予在那一层没有意义。
type memberGrants struct {
	handoff *handoffGrant
	route   *routeGrant
}

// grantsFor 读出 c 里发给 self 的那几份授予——不是发给 self 的读出来是空。
func (c *runConfig) grantsFor(self any) memberGrants {
	return memberGrants{handoff: c.handoffGrant.grantFor(self), route: c.routeGrant.grantFor(self)}
}

// regrantOpts 把这几份授予转授给 child，返回喂给 child.Run 的 opts（没有的那几份不写）。
func (g memberGrants) regrantOpts(child Agent) []RunOption {
	return []RunOption{withHandoffGrant(g.handoff.regrant(child)), withRouteGrant(g.route.regrant(child))}
}

// ── 从被授予的 agent 自己的图里显式转授 ──────────────────────────────────────

// nodeGrantsKey 是 ctx-value key：被授予的 agent（BasicAgent / GraphAgent）把自己收到的授予放进去，
// [RegrantFromNode] 读最近的那一份。
type nodeGrantsKey struct{}

// withNodeGrants 放进 agent 自己收到的授予，**没有时也放**（空的那份）：遮住外层放的那份。
func withNodeGrants(ctx context.Context, g memberGrants) context.Context {
	return context.WithValue(ctx, nodeGrantsKey{}, &g)
}

// shadowNodeGrants 给框架即将调起的另一个 agent 放一份空的：被调方的代码（不管它是框架编排还是业务
// 自研的 agent）从这里往下拿不到调用方那份。四个入口各调一次——编排的成员（runMember）、内部决策
// LLM（runDecisionAgent）、嵌套调用（runNestedCall：RunSubAgent / AgentAsTool / skill fork）、
// HandoffAgent 的候选。被调方要能力得走签名：调用方经 RunOption 显式转授。
func shadowNodeGrants(ctx context.Context) context.Context {
	return withNodeGrants(ctx, memberGrants{})
}

// RegrantFromNode 让跑在**被授予的那个 agent 自己图里**的代码，把这个 agent 收到的转交 / 路由授予
// 显式转授给它调起的 agent to，返回喂给 [RunSubAgent] / `to.Run` 的 opts：
//
//	// GraphAgent 的节点里（这个 GraphAgent 是 RoutingAgent 的分类器）
//	_, err := agent_v2.RunSubAgent(ctx, "router", router, in, agent_v2.RegrantFromNode(ctx, router)...)
//
// 「自己图里的代码」指 GraphAgent 的节点函数（含 AddSubgraphNode 子图里的节点、节点里直接
// compose_v2.Run 的嵌套图）、BasicAgent 的工具函数与图里的钩子（OnModelStart / OnToolStart /
// OnToolEnd 等）——它们拿不到 agent 的 Run 收到的 opts，所以没法用 [RegrantHandoffTo] /
// [RegrantRouteTo]。agent 级的 OnAgentStart / OnAgentEnd 跑在图外，拿不到。经 ctx 装下来、跑在这张图上的外层钩子（如顶层装
// 的 OrchHooks）也算：它是显式调用，跟业务把那段代码写进节点里是同一件事。
//
// 判据是**最近放进来的那一份**：BasicAgent / GraphAgent 在 Run 入口放自己收到的；框架把另一个 agent
// 调起来的每个入口（编排的成员与决策 LLM、RunSubAgent / AgentAsTool / skill fork、HandoffAgent 的
// 候选）都给被调方放一份空的——**孙辈拿到的是空的**：只有「被授予的那一层」能决定把能力交给谁，交
// 出去之后再往下要再显式交一次。曾经按 ns 判（放进来时的 ns 与当前相同才算），子图节点跑在子 ns 里、
// 被当成孙辈，显式转授静默拿不到，路由落到兜底（审计实测）。
//
// 拿不到时返 nil、不报错（同 [RegrantHandoffTo]：「这一层没有能力可交」是常态）。
//
// ⚠️ 一个收件人在同一超步里只能有一个决定方：同一超步的几个节点各自转授、而且都做了决定，是图的
// 设计错误（compose_v2.SignalConflictError 并带 ErrRunAborted，终局）。节点里自己起 goroutine、并发调
// 几个被转授的 agent 时框架看不见这层并发，它们的决定被当成先后做出的、谁后交回来听谁的——要么只
// 转授给一个，要么每一路用 compose_v2.CaptureSignals 单独收、再合并（见 compose_v2/signal.go「并发
// 合并要显式做」）：compose_v2.JoinSignals 在几路给出不同决定时返回带 ErrRunAborted 的冲突（往上交
// 就终局），要按次序取用 compose_v2.JoinSignalsFirstWins。
//
// ⚠️ 节点里裸调 `inner.Run(ctx, ...)` 时，被调的 agent 在 Run 入口遮住外层的授予——9 个编排与经
// [EnterRun] 进入的自研实现（含 `a2a.RemoteAgent`）都这样做，它的 agent 级钩子拿不到。不经 EnterRun
// 的自研实现，以及在那个位置上直接跑的别的代码，调本函数拿得到外层的授予。嵌套调用请经 [RunSubAgent]
// （裸调还会让被调方的存档撞进外层的槽）。
func RegrantFromNode(ctx context.Context, to Agent) []RunOption {
	g, _ := ctx.Value(nodeGrantsKey{}).(*memberGrants)
	if g == nil || isAgentNil(to) {
		return nil
	}
	return g.regrantOpts(to)
}
