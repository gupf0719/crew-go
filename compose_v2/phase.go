package compose_v2

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
)

// 相位：一次节点执行里「一段有自己位置的代码」。节点体是默认相位（名字为空）；图的钩子与条件
// 边由引擎开相位（见 withHookPhase）；框架把一段代码交给业务或另一个 agent 时用 [EnterPhase] /
// [HandOver] 开。

// handover 记下「这次执行连同它的输入是从哪儿交过来的」，挂在 [HandOver] 封出来的访问器上。
type handover struct {
	// from 是交接之前调用方的访问器：交过来的输入是它那个分域产的。
	from *nodeStateAccessor
	// ns 是交接时 ctx 上的 ns（调用方先把被调方的 ns 推好再交接）：落在这个 ns 上的那张图就是
	// 被调方的主图，它的输入就是交过来的那份；被调方钩子里另跑的图在别的 ns 上，输入是钩子自己
	// 产的。
	ns string
}

// EnterPhase 返回在当前执行里开一个**相位**用的 ctx：把一段代码交给业务（典型是 BasicAgent 的
// 工具钩子）时，它要有自己的一套位置——
//
//   - 自己的提问计数器与嵌套调用计数器，从 0 数起：跟调用方的计数互不影响。共用的话，这段代码里
//     问一次就占掉调用方的一个序号——这段代码在续跑时不再执行（它发起的调用被拿回）时，调用方
//     后面那一问换了序号、换了 ID，已经答过的问题被再问一遍（实测）；同一次执行里两段这样的代码
//     带同一个 call key 调同一个 agent 时，两次落在同一个位置上，整个 Run 终局失败（实测）；
//   - 相位名（[SubCallScope]，也进提问的身份）：scope 由调用方给，要在这次执行里唯一（工具钩子
//     带钩子名、工具名与所属 tool_call）。没有这一维时，钩子里的提问与 tool_call #0 里的提问
//     在 provider 不回 tool_call ID 时算出同一个 ID，整个 Run 终局失败（实测）；
//   - 节点私有态的一格（同 [WithNodeStateSealed]）：碰不到调用方那一格；
//   - 不在 tool_call 里（[InToolCall] 报 false）：这段代码不是调起它的那次 tool_call 本身。
//
// 不在任何节点执行里时只设相位名，不装计数器（那里没有「节点重跑」这回事，见 [NextSubCallIndex]）。
//
// 采纳这段代码返回的 ctx 时经 [CallHook]：调用方自己的那套（连同其余执行状态）取回来。
//
// 业务自己也用得上：一个按上一次执行记下的进度可能被跳过的提问，放进它自己的相位里问，跳过它就不会让同一
// 相位里后面的提问挪位（见 [Ask]「按同样的次序问」）。
func EnterPhase(ctx context.Context, scope string) context.Context {
	return enterPhase(ctx, scope, false)
}

// HandOver 同 [EnterPhase]，给「把一次执行连同调用方给的输入交给另一个 agent」的边界用：嵌套
// 调用的被调方、编排的成员与内部决策 LLM、转交候选。被调方在自己的图之外（agent 级钩子里）
// 跑的代码处在这个相位里。
//
// 调用前先把被调方的 ns 推好（[WithGraphNS]）：落在那个 ns 上的图是被调方的主图，框架据此认出
// 它的输入是**调用方**产的——主图的入口条件边按 [CondFailRerunFrom] 否决时，作废的是调用方那个
// 分域按 [Rerunnable] 记下的产出（没有这一条时作废的是被调方这一格，调用方的产出原样拿回、
// 续跑永不收敛，实测）。
//
// # 调用方那一层重做，这次调用里的提问不换 ID
//
// 被调方问的问题——在它的主图里，还是在它自己的图之外——都不随**调用方那一层**（调用方节点的这次
// 执行，连同交接之前所在的分域）的重做而换 ID：调用方重做时这是不是同一次调用，由调用的身份决定
// （输入变了、声明了 Rerunnable 的调用换身份、落进新的一格）。身份没变的调用续上自己的存档，里面
// 已经答过的问题不该再问（实测：在图外问的曾被再问一遍）。所以交接这一格的重做代际从交接这一层
// 数起：图外的提问带的是它（InterruptSite 的 RerunGen），被调方主图的外层代际链里承载主图的那一项
// 也是它（OuterRerunGens）。按 [Rerunnable] 记在交接这一格的东西同理，调用方那一层重做时不作废。
//
// 调用方所在那张图**之外**的执行被否决重做时则相反：那张图会从头跑、这次调用的完成记录随它的
// 节点私有态一起没了，被调方从头重来、重新产出——它的提问必须换新，否则调用方累积传的旧答案
// （「批准 v1」）直接兑付给重新产出的内容（实测，`err == nil`）。外层代际链里留着的就是这几层。
// 交接这一格自己被推进（被调方在图外产出的东西被否决）时同理：主图会重跑，它的提问也换新。
//
// scope 是这次交接在调用方执行里唯一的名字（嵌套调用用调用的身份、编排成员用子 ns 后缀）。
func HandOver(ctx context.Context, scope string) context.Context {
	return enterPhase(ctx, "handover\x00"+scope, true)
}

func enterPhase(ctx context.Context, phase string, handedOver bool) context.Context {
	if a := nodeStateAccessorFrom(ctx); a != nil {
		sealed := scopedOf(a, "sealed\x00"+phase)
		if handedOver {
			sealed.handover = &handover{from: a, ns: GetGraphNS(ctx)}
			// 代际从交接这一层数起（见 nodeStateAccessor.handedOver）：chain 的最后一项是刚叠上的这一格
			sealed.handedOver, sealed.genFrom = true, len(a.chain)
		}
		ctx = withAccessor(ctx, sealed)
	}
	if inNodeExecution(ctx) {
		var counter atomic.Int64
		ctx = context.WithValue(ctx, nodeInterruptCounterKey{}, &counter)
		ctx = withSubCallCounter(ctx)
	}
	ctx = context.WithValue(ctx, subCallScopeKey{}, phase)
	if InToolCall(ctx) {
		ctx = context.WithValue(ctx, inToolCallKey{}, false)
	}
	return ctx
}

// hookCallKey 标记一次钩子调用：enterHookCall 放一个这次调用独有的值，adoptHookCtx 据此分辨
// 钩子返回的 ctx 是不是从交给它的那个派生的。它本身是执行状态。
type hookCallKey struct{}

// hookCall 是一次钩子调用的标记。它必须非零大小：零大小类型的 new 可能返回同一个地址，那样两次调用
// 就分不开了（pushedIdentity 让它非零大小）。
type hookCall struct {
	// pushedIdentity 记「这次钩子调用里经公开的 WithThreadIDCtx / WithSessionIDCtx 推过身份」。
	// adoptHookCtx 只在它为真时才去比对身份（见 identityPushes）：比对要沿 ctx 链查两遍，每次采纳都查
	// 的话，一批 tool_call 里采纳层层嵌套，开销按批大小的平方涨（审计实测 N=32 时整次 Run 慢三成）。
	pushedIdentity atomic.Bool
	// outer 是放这个标记时 ctx 上已有的那个标记（没有就是 nil）。钩子里再经 CallHook 调别的钩子（业务的组合
	// 钩子）时，内层那次采纳的结果答的是内层的标记；外层沿 outer 往上找到自己的，才认得出它是从交给自己的
	// ctx 派生的。曾经只比一个标记：外层把内层采纳的结果当成外来的，内层钩子加的超时与配置静默丢掉、还打一条
	// 误报的告警（审计实测）。
	outer *hookCall
}

// markIdentityPush 在 ctx 所在的那次钩子调用、以及包着它的各次调用上记一笔「推过身份」；不在钩子调用里就什么
// 也不做。连外层一起记：业务的组合钩子在内层采纳之后再推身份时，ctx 答的是内层的标记，只记那一个的话外层采纳
// 不去比对、推了不生效却不告警。
func markIdentityPush(ctx context.Context) {
	for hc, _ := ctx.Value(hookCallKey{}).(*hookCall); hc != nil; hc = hc.outer {
		hc.pushedIdentity.Store(true)
	}
}

// CallHook 调一次业务钩子、采纳它返回的 ctx：放这次调用独有的标记、调、按下面的规则采纳，三步收在这一处。
// 框架里每一处接住钩子返回 ctx 的地方都只经它（引擎的 OnGraphStart / OnNodeStart、agent_v2 的 agent 级 /
// 模型 / 工具钩子、a2a.RemoteAgent）。业务的组合钩子（一个钩子里调别的钩子）采纳内层钩子返回的 ctx 也可以用它：
// 外层那次采纳认得出内层采纳的结果是从交给它的 ctx 派生的；内层有好几个时，后一个拿到的执行状态是框架的、不是前
// 一个返回的外来 ctx 上的。直接把内层返回的交回也行，由外层那次采纳。钩子不以返回值、而是以
// 「调 next 时传进来的 ctx」交回 ctx 的（工具中间件）用 [EnterHook]。
//
//	ctx, err := compose_v2.CallHook(ctx, func(c context.Context) (context.Context, error) {
//	    return hooks.OnAgentStart(c, info, in)
//	})
//
// call 收到放好标记的 ctx（要给钩子套相位就在 call 里套），返回钩子交回的 ctx 与它的错。钩子报错时不采纳：
// 返回放好标记的 ctx 与那个错，调用方拿它报 OnError、走早退。钩子交回 nil 时沿用放好标记的 ctx。
//
// 采纳之后的 ctx 只给钩子包住的那次调用用：开始钩子的结果交给那次调用和同一次的结束钩子，结束钩子的结果丢掉；
// 别把它赋回一个之后还要交给别的调用的 ctx（同一批的几个 tool_call 就是这样串过值，见 hook.Hooks 的「ctx 返回值」）。
//
// 曾经各采纳点各写一遍「放标记 → 调钩子 → 采纳」，靠一个按源码位置查的哨兵对账，标记放在循环外、只放在
// 一个分支上、放了标记交给钩子的却是另一个变量这几种写法都查不出（审计实测）。收成一个函数之后没有可以
// 写错的协议。
//
// 按两样东西分：钩子返回的 ctx 是哪来的，key 是谁的。
//
//   - **派生的**（看得见交给钩子之前放的那个标记）：业务推的值、调用方可配的（环境：checkpointer、
//     编排钩子、调用主体、store……；以及软取消）、取消与截止时间，都听钩子的——那是钩子的正当
//     用法：挂 span、加超时、给这个 Run 装软取消。「看得见」按传递关系算：钩子里经 CallHook 再调别的
//     钩子、或者跑了一段嵌套执行，从那里拿回的 ctx 也算派生——那段执行结束时把它取消了的话，取消也
//     一并采纳。所以别把嵌套执行里留下的 ctx 当返回值，返回从入参派生的。
//   - **外来的**（看不见那个标记：别处缓存的 ctx、`context.Background()`）：可配的与取消、截止时间
//     取框架这次的，只借业务自己的值（钩子那份没有的回落到框架那份）。外来 ctx 上的配置是别的
//     请求的，或者什么都没有。业务值框架分不出哪些是 span、哪些是凭证，照用外来那份——跨请求存下
//     的 ctx 会把那个请求的凭证带进来，所以别跨请求存 ctx；每个采纳点第一次采纳外来 ctx 时告警一次，
//     带上采纳点的位置。
//   - 两种都一样：**框架的身份与位置**（所在的位置、这次 Run 的 sid 与任务代号、会话 ID、相位与
//     计数器、答案与兑付账本、上行信号交给谁、历史写入器……，isExecutionKey 里除了软取消）取框架的。
//     会话 ID 与 sid 是文档让业务自己推的两样，要在调用之前推；钩子在派生的 ctx 上推了、跟框架的
//     不同时同样每个采纳点告警一次——推了不生效，不告警就是静默。
//
// 钩子推的可配项从它之后才读 ctx 的地方开始生效，本层在钩子之前已经定下的不跟着变：OnGraphStart
// 推的软取消管不到本图自己的超步循环（只管节点体与嵌套图），OnAgentStart 推的历史配置管不到本层已经
// 建好的写入器。
//
// 执行状态是什么，跟 [DetachExecution] 剥的是**同一份清单**：新加的 ctx key 登记一次，两处都生效，
// 没有第二份要记得同步的拷贝。上层包的 key 实现 [ExecutionStateKey] 或 [EnvironmentKey]。
//
// 为什么要分来源，而不是只按 key 分：钩子返回的 ctx 通常派生自交给它的那个，这时两者的状态本来
// 一样，钩子改了什么就是它想改的。不一样的是钩子违约——返回别处缓存的 ctx（比如沿用第一次调用
// 存下的、想把 span 挂在一起）或 `context.Background()`。曾经只按 key 分：执行状态取框架的、环境与
// 取消跟钩子——外来 ctx 上别的请求的调用主体、编排钩子、checkpointer、store 照收，流式片段送到别人
// 的连接上、长期记忆写进别人的 store、调用方取消了也停不下来；派生的 ctx 上钩子正当装的软取消
// 反而被当成执行状态丢掉（均实测，`err == nil`）。更早各个采纳点各铺各的几样，漏掉的那几样各自
// 静默出错（转交没人取、模型答复记不进会话历史）。
//
// 钩子里开的相位（[EnterPhase] / [HandOver]）同样是执行状态：采纳时铺回调用方自己的，钩子里
// 记下的、问过的留在钩子的相位，调用方接着用自己的计数。
func CallHook(ctx context.Context, call func(ctx context.Context) (context.Context, error)) (context.Context, error) {
	return callHookOnto(ctx, call, nil)
}

// EnterHook 是 [CallHook] 的另一种形状：钩子以「调 next 时传进来的 ctx」交回 ctx（工具中间件）。返回交给钩子的
// ctx（放好了这次调用的标记），以及采纳交回的 ctx 用的函数——绑定了这个标记，采纳规则同 CallHook。
func EnterHook(ctx context.Context) (context.Context, func(hookCtx context.Context) context.Context) {
	in := enterHookCall(ctx)
	return in, func(got context.Context) context.Context { return adoptHookCtx(got, in) }
}

// callHookOnto 同 [CallHook]，只是采纳之后框架接着用的执行状态是 onto(放好标记的 ctx)：钩子之后要在交给钩子的
// 那个 ctx 之上再铺一层自己的状态时用（引擎的 OnGraphStart——钩子的相位不往图里带，铺回图的 sid）。onto 为 nil
// 时就是交给钩子的那个。
func callHookOnto(ctx context.Context, call func(ctx context.Context) (context.Context, error), onto func(in context.Context) context.Context) (context.Context, error) {
	in := enterHookCall(ctx)
	got, err := call(in)
	if err != nil {
		return in, err
	}
	from := in
	if onto != nil {
		from = onto(in)
	}
	return adoptHookCtx(got, from), nil
}

// enterHookCall 放这次钩子调用独有的标记：采纳时凭它分辨钩子返回的 ctx 是派生的还是外来的。只经 CallHook /
// EnterHook / callHookOnto 调。
func enterHookCall(ctx context.Context) context.Context {
	outer, _ := ctx.Value(hookCallKey{}).(*hookCall)
	return markedCtx{Context: ctx, tok: &hookCall{outer: outer}}
}

// derivedFrom 判 hookCtx 是不是从放了 tok 的那个 ctx 派生的：它上面的标记就是 tok，或者是在 tok 那次调用之内
// 放的（沿 outer 往上找得到 tok）。别处存下的 ctx 上的标记是别的调用放的，往上找不到这一个。
func derivedFrom(hookCtx context.Context, tok *hookCall) bool {
	for hc, _ := hookCtx.Value(hookCallKey{}).(*hookCall); hc != nil; hc = hc.outer {
		if hc == tok {
			return true
		}
	}
	return false
}

// markedCtx 是放了这次钩子调用标记的 ctx：只答 hookCallKey，其余交给父 ctx。用自己的类型而不是 context.WithValue，
// 好让采纳时认出「from 只是在上一次采纳的结果上放了个标记」，把执行状态的查找源压平（见 execSource）。
type markedCtx struct {
	context.Context
	tok *hookCall
}

func (c markedCtx) Value(key any) any {
	if _, ok := key.(hookCallKey); ok {
		return c.tok
	}
	return c.Context.Value(key)
}

// execSource 返回采纳之后执行状态（标记除外）该从哪儿查。from 只是在上一次采纳的结果上放了标记时（一批 tool_call
// 的工具钩子逐个采纳就是这样），直接用上一次采纳查执行状态的那个——跳过中间那几层，查找深度不随采纳次数增长。
// 曾经每次采纳都在上一次的结果外面再包一层，一批 64 个 tool_call 之后框架查一次位置要穿过 64 层（实测 10ns →
// 1.9µs，整批开销按批大小的平方涨）。其余情况就是 from 本身。
func execSource(from context.Context) context.Context {
	c := from
	for {
		m, ok := c.(markedCtx)
		if !ok {
			break
		}
		c = m.Context
	}
	if a, ok := c.(adoptedCtx); ok {
		return a.exec
	}
	return c
}

// adoptHookCtx 返回采纳钩子返回的 ctx 时该接着用的 ctx（规则见 [CallHook]）。from 是框架接着用的那个，要从放了
// 标记的 ctx 派生——没有标记时一律按外来处理；hookCtx 为 nil 时原样返回 from。只经 CallHook / EnterHook /
// callHookOnto 调。
func adoptHookCtx(hookCtx, from context.Context) context.Context {
	if hookCtx == nil {
		return from
	}
	if tok, _ := from.Value(hookCallKey{}).(*hookCall); tok != nil && derivedFrom(hookCtx, tok) {
		if tok.pushedIdentity.Load() {
			for _, p := range identityPushes {
				if v := hookCtx.Value(p.key); v != nil && v != from.Value(p.key) {
					warnHookCtxOncePerSite(from, "identity:"+p.name,
						"compose_v2: 业务钩子在返回的 ctx 上推的"+p.name+"不生效：身份与位置一律取框架的",
						p.name+"要在调用之前推（"+p.setter+"）；本告警每个采纳点只打一次")
				}
			}
		}
		return adoptedCtx{Context: hookCtx, hook: hookCtx, from: from, exec: execSource(from), derived: true}
	}
	warnHookCtxOncePerSite(from, "foreign",
		"compose_v2: 业务钩子返回了不是从入参派生的 ctx（别处存下的、或 context.Background()）："+
			"框架的身份与位置、调用方配的环境与取消照用这次调用的，钩子在那个 ctx 上加的超时与配置不生效；业务值沿用它的"+
			"（跨请求存下的 ctx 会带进那个请求的凭证）",
		"钩子的契约是返回从入参派生的 ctx；本告警每个采纳点只打一次")
	return adoptedCtx{Context: from, hook: hookCtx, from: from, exec: execSource(from)}
}

// identityPushes 是文档让业务自己推的两样身份：会话 ID 与 sid。钩子在派生的 ctx 上推它们不生效（见
// [CallHook]），推了就告警。只在这次钩子调用里经公开的推法推过时才比对（见 hookCall.pushedIdentity）；
// 推了但返回的 ctx 上值没变（比如推在一个临时 ctx 上、钩子里跑的嵌套 Run 自己推的）不告警。检测只覆盖
// 公开推法：业务自定义 ctx 类型、覆盖 Value 冒出来的另一份身份不告警——取值照样是框架的。
var identityPushes = [...]struct {
	key          any
	name, setter string
}{
	{threadIDKey{}, "会话 ID", "compose_v2.WithThreadIDCtx"},
	{sessionIDKey{}, "sid", "compose_v2.WithSessionIDCtx"},
}

// hookCtxWarned 记哪个采纳点已经为哪一类问题告过警。违约的钩子每次调用都会走到同一个采纳点，逐次
// 打会刷屏；全进程只打一次又会让第一个违约者之后的都不出声，而且只有一条告警时看不出是哪个钩子（审计
// 实测：33 次外来采纳只打出 2 条，都不带位置）。第一级的键是往上一串定长的 PC，不是解析出来的位置串：违约的
// 钩子每次被采纳都要查一次，解析调用栈要拼串、分配（审计实测每次 1.2µs、8 次分配），只在第一次出现时做。
// 取一串而不是一帧：统一入口（CallHook / EnterHook）之下采纳机制自己的那几帧对所有采纳点都一样，只取一帧
// 会把所有采纳点认成同一个，第一个之后的全不出声。
var (
	hookCtxWarnedMu sync.Mutex
	hookCtxWarned   = map[hookCtxWarnKey]struct{}{}
	// hookCtxWarnedSites 是第二级：按类别 + 解析出的位置。同一个采纳点从不同调用路径走到（PC 串不同）、
	// 调 CallHook 的函数被内联到多处（同一个源码位置有好几个 PC）时，都只告警一次（审计实测：可内联的包装
	// 从两处调用打两条一模一样的告警）。
	hookCtxWarnedSites = map[string]struct{}{}
)

// hookCtxWarnDepth 是第一级去重键取的 PC 帧数：盖过采纳机制自己那几帧、再往上到采纳点。
const hookCtxWarnDepth = 8

type hookCtxWarnKey struct {
	kind string
	pcs  [hookCtxWarnDepth]uintptr
}

// warnHookCtxOncePerSite 在采纳点（调 [CallHook]、调 [EnterHook] 交回的采纳函数的那一处）第一次碰到 kind 这类
// 问题时告警，带上那一处的位置（site）。采纳点在框架里，各钩子各有各的采纳点（agent 级、模型、工具钩子、
// 工具中间件、引擎的 OnGraphStart / OnNodeStart），据 site 就认得出是哪个钩子。只能由 adoptHookCtx 直接调。
//
// 只收两个字符串、不收可变参数：可变参数流向 slog 会被判逃逸，每次调用都得在堆上建一个 []any，去重掉了
// 也照付。
func warnHookCtxOncePerSite(ctx context.Context, kind, msg, hint string) {
	key := hookCtxWarnKey{kind: kind}
	n := runtime.Callers(2, key.pcs[:])
	if n == 0 {
		return
	}
	hookCtxWarnedMu.Lock()
	_, seen := hookCtxWarned[key]
	if !seen {
		hookCtxWarned[key] = struct{}{}
	}
	hookCtxWarnedMu.Unlock()
	if seen {
		return
	}
	// 第一次碰到这个 PC 才解析位置，再按位置查一次第二级。交出去的是拷贝：CallersFrames 会把切片存下来，
	// 直接交 pcs[:n] 会让 pcs 每次都挪到堆上（去重掉了也照付）
	site := adoptionSite(append([]uintptr(nil), key.pcs[:n]...))
	siteKey := kind + "\x00" + site
	hookCtxWarnedMu.Lock()
	_, seen = hookCtxWarnedSites[siteKey]
	if !seen {
		hookCtxWarnedSites[siteKey] = struct{}{}
	}
	hookCtxWarnedMu.Unlock()
	if seen {
		return
	}
	slog.WarnContext(ctx, msg, "site", site, "hint", hint)
}

// phaseFile 是本文件编进二进制时的路径。采纳机制（CallHook、EnterHook 交出的采纳函数、callHookOnto、
// adoptHookCtx、告警本身）全在这个文件里，采纳点就是调用栈上第一帧不在这里的。
var phaseFile = func() string {
	_, f, _, _ := runtime.Caller(0)
	return f
}()

// adoptionSite 把 warnHookCtxOncePerSite 取到的 PC 解析成采纳点：越过采纳机制自己的那几帧（内联了也按逻辑帧
// 走，不按固定层数跳）。按帧所在的文件认，不按函数名：内联会改闭包的名字——EnterHook 被内联进调用方之后，它交出
// 的采纳函数叫「调用方.EnterHook.func1」，曾经按函数名列的名单认不出它，把告警的位置报成了这个文件里的那个闭包
// （审计实测）。
func adoptionSite(pcs []uintptr) string {
	frames := runtime.CallersFrames(pcs)
	for {
		fr, more := frames.Next()
		if fr.File != phaseFile {
			return fmt.Sprintf("%s (%s:%d)", fr.Function, filepath.Base(fr.File), fr.Line)
		}
		if !more {
			return "unknown"
		}
	}
}

// stdCancelCtxKey 是标准库 context 包查「最近的那个 cancelCtx」用的私有 key：context.Cause、派生子
// ctx 时找父 cancelCtx 都拿它查 Value。init 里用一个探针 ctx 截下来。adoptedCtx 让它跟 Done / Err
// 取同一边——不然外来 ctx 上 Err 是这次请求的、Cause 却是那个请求的取消原因（实测）。
var stdCancelCtxKey any

// cancelKeyProbe 的 Err 必须非 nil：context.Cause 先看 Err，为 nil 就直接返回、不查 Value。
type cancelKeyProbe struct{ context.Context }

func (cancelKeyProbe) Err() error { return context.Canceled }

func (cancelKeyProbe) Value(key any) any {
	if stdCancelCtxKey == nil {
		stdCancelCtxKey = key
	}
	return nil
}

func init() {
	_ = context.Cause(cancelKeyProbe{context.Background()})
}

// adoptedCtx 按 [CallHook] 的规则在钩子返回的 ctx 与框架的 ctx 之间取值。嵌入的 Context 决定
// 取消与截止时间：派生的是钩子返回的那个，外来的是框架的。
type adoptedCtx struct {
	context.Context
	hook context.Context
	from context.Context
	// exec 是执行状态（这次调用的标记除外）的查找源：等价于 from，只是跳过了「上一次采纳 + 标记」那几层（见
	// execSource）。
	exec    context.Context
	derived bool
}

func (c adoptedCtx) Value(key any) any {
	switch {
	case stdCancelCtxKey != nil && key == stdCancelCtxKey:
		return c.Context.Value(key) // 跟 Done / Err 同一边
	case isConfigurableKey(key):
		if c.derived {
			return c.hook.Value(key)
		}
		return c.from.Value(key)
	case isExecutionKey(key):
		if _, ok := key.(hookCallKey); ok {
			return c.from.Value(key)
		}
		return c.exec.Value(key)
	}
	if v := c.hook.Value(key); v != nil || c.derived {
		return v
	}
	return c.from.Value(key)
}
