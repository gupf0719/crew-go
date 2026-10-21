package agent_v2

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"crew-go/compose_v2"
)

// handoffGrant 是一次「转交能力授予」——**由 HandoffAgent 亲手交给它即将调用的那个
// 对象**，收到的人才拿得到 transfer_to_agent 工具。
//
// # 为什么授予的载体是 RunOption 而不是 ctx
//
// ctx 是**环境**：谁在这条链上跑都读得到，深到第几层都读得到。转交能力挂在环境上时，
// 「谁能转交」这个问题的答案是「凡是能看到这条 ctx 的人」——而那包括候选内部随手起的
// 一个顾问 agent、业务工具里 `inner.Run(ctx, ...)` 的那个孙辈。它们从来没被谁授权替
// 这场对话选下家，但环境不区分。
//
// RunOption 是**签名**：它只到达被调用的那一个对象，不会自己往下渗。谁想让它继续往下
// 走，必须**显式再交一次**（见 [handoffGrant.regrant]）——而那次显式动作正是「我声明
// 这一层是我的组成部分」这个意图的表达。
//
// ⚠️ **授予不作为环境放进 ctx**。BasicAgent.Run 里有个 `withRunOverrides(ctx, ...)`
// 的先例（per-call model 覆盖走 RunOption 进来、随即 push 进 ctx），照抄那个形状会让
// 这套授予**当场退化回环境**：孙辈照样读得到，本方案的全部收益归零。ctx 上只有一份受控
// 的形态：被授予的 BasicAgent / GraphAgent 跑图时把自己收到的放进自己图的 ctx（nodeGrants），
// 图里的代码（节点、工具、钩子）只能经 [RegrantFromNode] **显式**转授——那仍是一次签名
// 动作；框架调起另一个 agent 的四个入口与每个编排的入口都会遮住它，孙辈读不到。
//
// # 受让方身份为什么是对象而不是名字
//
// 名字有两处不成立：
//
//   - `AddAgent(name, ag)` 的 `name` 是**注册名**（强制唯一），`ag.Name()` 是 agent
//     自己的名字（不配 WithName 时是 `agent_v2_<uuid8>`），两者互相独立——
//     `AddAgent("front", 一个叫 reception 的 agent)` 完全合法。grant 写注册名、
//     BasicAgent 拿 `cfg.Name` 比，所有这类合法转交当场全断。
//   - 名字是业务可控的：业务壳跟它内层取同一个名字，「原样转发 opts」就穿了。
//
// 对象身份两处都不吃：它是 stepFn **即将调用的那个接口值本身**，业务复制不出来，也
// 改不了自己是谁。
//
// # 受让方判定：沿「是同一个东西」的关系向内走，而不是 interface ==
//
// 判据是「self 是不是 grantee 本人、或 grantee 的**嵌入外壳**里那一个」。三条理由：
//
//   - **interface `==` 会 panic**：业务把 Agent 实现在含 map/slice/func 字段的 struct
//     **值**上（合法 Go），`RegrantHandoffTo` 把它当 self 传进来，两边动态类型相同 ⟹
//     当场 `comparing uncomparable type`。而且命中的恰好是**正确用法**——写错时类型不同
//     反而安全。实测过。
//   - **`==` 在可比较值类型上会误判**：两个字段全同的不同实例互相判等。
//   - **Go 的嵌入就是「我就是它」**：`type Shell struct{ *BasicAgent }` 的方法集**就是**
//     内层的，`Run` 是提升上来的、接收者是内层。那一层没有地方写 [RegrantHandoffTo]
//     ——要写就得覆写 `Run`，等于放弃嵌入。用 `==` 判的话，业务**用语言本身声明了
//     「这一层是我的组成部分」反而被惩罚**。
//
// 所以：沿**匿名嵌入字段**和可选的 `Unwrap() Agent` 向内走（限深防环），用
// `reflect.Value.Pointer()` 比指针。具名字段包装（`struct{ inner Agent }`）**仍然不穿**
// ——那是「我**有**一个」不是「我**是**一个」，要授予得显式调 [RegrantHandoffTo]，
// 「转发 ≠ 授予」这条核心不变。
//
// ⚠️ **这个判据给不了的区分**：业务嵌入了框架 agent 却**不想**让它继承能力。Go 里
// 没有既穿透嵌入又能拒绝嵌入的判据——嵌入在语言层就是「我就是它」。
type handoffGrant struct {
	// grantee 是这份授予的受让方——stepFn 铸它时放进去的**那个 Agent 接口值本身**。
	// 存成 any 而不是 Agent 是为了让 grantFor 的入参能直接用具体类型（*BasicAgent 等）
	// 而不必先装箱成 Agent 再比（装箱后动态类型仍是具体类型，两种写法等价，但 any
	// 让调用点更短）。
	grantee any
	// candidates 是可转交候选名单——从 HandoffAgent 的 AddAgent 注册名来。跟着 grant
	// 一起走而不是走 ctx：候选名单是**授予内容的一部分**（授权你转给这几个人），
	// 跟「能不能转」同生共死才对。
	candidates map[string]bool
}

// withHandoffGrant 把 grant 交给**这一次** sub-agent 调用（RunOption）。
//
// 传 nil 时不写——让「上层没给我 grant」和「上层给了我一个空 grant」是同一件事。
func withHandoffGrant(g *handoffGrant) RunOption {
	return func(c *runConfig) {
		if g == nil {
			return
		}
		c.handoffGrant = g
	}
}

// grantFor 校验这份 grant 是不是发给 self 的——是则返回它，不是（或压根没有）返 nil。
//
// **每个消费 grant 的对象都必须自查**，包括编排：不自查的话，业务壳把 `opts...` 原样
// 转发给任意一个框架编排，那个编排就会替它把能力重新发下去，名字/对象校验被一层间接
// 完全绕开。
func (g *handoffGrant) grantFor(self any) *handoffGrant {
	if g == nil || self == nil {
		return nil
	}
	if !sameEntity(g.grantee, self) {
		return nil
	}
	return g
}

// unwrapper 让具名字段包装的业务壳用**一行**声明「内层就是我」，不必覆写 Run。
// 与 Go 生态既有惯例同形（errors.Unwrap / net/http 的 rwUnwrapper）。
type unwrapper interface{ Unwrap() Agent }

// sameEntity 报 self 是不是 grantee 本人、或 grantee 嵌入/Unwrap 链上的那一个。
//
// **只走「是同一个东西」的关系**：匿名嵌入字段（Go 的 is-a）与显式 `Unwrap()`。
// 具名字段（has-a）不走——那条要授予得显式 regrant。
func sameEntity(grantee, self any) bool {
	if grantee == nil || self == nil {
		return false
	}
	sp, st, ok := entityID(self)
	if !ok {
		return false // self 拿不到稳定身份：判否（fail-closed，不 panic）
	}
	const maxDepth = 8 // 限深防环：业务可能写出互相嵌入的结构
	for cur, depth := grantee, 0; cur != nil && depth < maxDepth; depth++ {
		if cp, ct, ok := entityID(cur); ok && cp == sp && ct == st {
			return true
		}
		cur = innerOf(cur)
	}
	return false
}

// entityID 取一个值的身份：**(指针, 动态类型) 两元组**。非指针类型返 false——
// **不做 interface `==` 兜底**，那正是 panic 与误判的来源。
//
// # 为什么指针一个不够
//
// Go 让**所有零大小类型的实例共用同一个地址**（`runtime.zerobase`）：`&A{}` 与
// `&B{}`（两个各自 `new`、字段为空的不同类型）的 `reflect.Value.Pointer()` **相等**。
// 而「业务自己实现的、无状态的 Agent」最自然的写法恰恰就是 `type xxxAgent struct{}`
// ——于是两个毫不相干的空结构体 agent 会互相认作同一个实体。
//
// 实测后果（授予是转交能力的唯一来源，见 [HandoffTo]）：一个**从没被授予过**的孙辈
// 只要自己也是空结构体，就能用上游转发下来的 opts 通过身份判定、转走整场对话；它还
// 能把别人的授予改寄给无关 agent——那两件事正是 [RegrantHandoffTo] 的 godoc 声称
// 做不到的。
//
// 带上动态类型后，不同类型的空结构体不再互相冒充。**同一类型的两个空结构体实例仍
// 不可区分**——Go 层面没有办法区分它们，而那时两个 agent 连一个字节的状态都没有、
// 行为完全相同，认作同一个实体是可接受的近似。要精确区分就给类型加一个字段
// （名字通常就够）。
func entityID(v any) (uintptr, reflect.Type, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.UnsafePointer, reflect.Chan, reflect.Map, reflect.Func:
		if rv.IsNil() {
			return 0, nil, false
		}
		return rv.Pointer(), rv.Type(), true
	}
	return 0, nil, false
}

// innerOf 往里走一层：优先显式 Unwrap()，否则找**唯一**的匿名嵌入字段。
// 多个匿名字段时返 nil（歧义不猜——猜错就是悄悄放宽授权）。
func innerOf(v any) any {
	if u, ok := v.(unwrapper); ok {
		if in := u.Unwrap(); !isAgentNil(in) {
			return in
		}
		return nil
	}
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil
	}
	var found any
	n := 0
	for i := 0; i < rv.NumField(); i++ {
		if !rv.Type().Field(i).Anonymous {
			continue
		}
		f := rv.Field(i)
		if !f.CanInterface() {
			continue // 未导出的嵌入字段取不出来
		}
		n++
		found = f.Interface()
	}
	if n != 1 {
		return nil
	}
	return found
}

// regrant 铸一份发给 child 的新 grant——**编排层显式声明「这一步是我的组成部分」**。
//
// 只有已经自查过（grantFor 返非 nil）的 grant 才该调它：调用点的形状恒为
//
//	sub.Run(ctx, sess, in, withHandoffGrant(myGrant.regrant(sub)))
//
// nil 接收者返 nil，让「本层没被授予」自然地不往下传（调用点不必写 if）。
func (g *handoffGrant) regrant(child Agent) *handoffGrant {
	if g == nil || isAgentNil(child) {
		return nil
	}
	return &handoffGrant{grantee: child, candidates: g.candidates}
}

// ─────────────────────────────────────────────────────────────────────────────
// 公开面：让业务壳 / 业务自写编排也能把转交能力**显式**发给自己的组成部分。
//
// 这是一个**动作**，不是一条通道：把 opts 原样往下转发（`inner.Run(ctx, s, in, opts...)`）
// 什么也不会发生——受让方仍是壳本人，内层对不上。想让内层有资格，必须写出这一行。
// ─────────────────────────────────────────────────────────────────────────────

// RegrantHandoffTo 把「发给 self 的转交授予」重新发一份给 to，返回可直接喂给
// `to.Run` / [RunSubAgent] 的 opts。
//
//	func (w *myShell) Run(ctx context.Context, s *agent_v2.RunSession,
//	    in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
//	    return w.inner.Run(ctx, s, in, agent_v2.RegrantHandoffTo(opts, w, w.inner)...)
//	}
//
// # self 为什么必须传
//
// 它是「我就是这份授予的受让方」这句话的**证据**——手上有 opts 不代表这份授予是发给你的。
// 少了这个参数，任何拿到过一份 opts 的人都能把它改寄给任意对象：把上一跳的候选收到的
// opts 存下来、下一跳换个受让方重放（哨兵 TestGrantBreak_B8g_StashedOptsReplay 就是这个
// 形状）。校验不通过时**原样返回 opts**——不报错，因为「上层没授予我」是常态（顶层直接
// 跑、跑在 HandoffAgent 之外，都走这条）。
//
// # 忘了调是什么下场
//
// 静默不转交：to 那侧连 transfer_to_agent 都不存在，模型硬发这个 tool_call 会拿到
// [HandoffNoCoordinatorErrCode] 软失败回灌、改成自己作答。**没有报错**——因为「这一层
// 不该有转交能力」是绝大多数调用的正确答案，框架分不出「你忘了」和「你就是不想给」。
//
// # 它能做到什么（业务的显式意图，框架不拦）
//
//   - 发给一个跟本次编排毫无关系的对象（`RegrantHandoffTo(opts, w, 随便一个 agent)`）
//   - 发给自己的祖先 / 发给候选自己
//   - 一份授予发给多个对象
//
// 这些都是「业务声明这一层是我的组成部分」的直接后果，跟框架编排替自己的成员 regrant
// 是同一件事。**不能做到**的是把别人的授予改寄（见 self）。
func RegrantHandoffTo(opts []RunOption, self Agent, to Agent) []RunOption {
	if isAgentNil(self) || isAgentNil(to) {
		return opts
	}
	g := grantFromOpts(opts).grantFor(self)
	if g == nil {
		return opts
	}
	out := make([]RunOption, 0, len(opts)+1)
	out = append(out, opts...)
	return append(out, withHandoffGrant(g.regrant(to)))
}

// HandoffTo 让**程序化成员**（业务自己实现的 Agent，不是 BasicAgent）行使它收到的转交
// 授予——把这场对话交给候选里的另一个 agent。
//
// # 什么时候需要它
//
// LLM 驱动的成员经内置的 transfer_to_agent 工具转交；而业务自己写的 Agent 实现没有
// LLM、也没有工具循环，它需要一条能在代码里直接调的路。
//
//	func (a *MyAgent) Run(ctx context.Context, sess *RunSession, in *schema.Message,
//		opts ...RunOption) ([]*AgentEvent, error) {
//		if 该转给账单 {
//			if err := agent_v2.HandoffTo(ctx, opts, a, "billing"); err != nil {
//				// 没被授予 / 目标不在候选里 / 这一层没人接转交
//			}
//		}
//		return []*AgentEvent{agent_v2.NewOutputEvent(...)}, nil
//	}
//
// # 三道校验（与 transfer_to_agent 工具**同一套**）
//
//  1. **有没有被授予**：opts 里那份授予的受让方必须是 self 本人。转授给别人的那份不算
//     （受让方是别人），孙辈也继承不到（授予走签名；ctx 上那份只给被授予方自己图里的代码经
//     [RegrantFromNode] 显式转授，框架调起别的 agent 时遮住）。
//  2. **目标合不合法**：必须在授予随附的候选名单里。
//  3. **这一层有没有人接**：拿不到收件人时是「头顶没有 HandoffAgent」或「本 agent 正被
//     当作一件工具用」——后者的控制流决策本就不该逃出去。
//
// 三道任一不过返 error 且**什么都不写**——不会留下半个信号。
//
// # 返回的 error 带 [compose_v2.ErrRunAborted]
//
// 三道校验拒绝的都是「重跑必然一样」的情形：授予是上层在调用你时发的、候选名单在注册时
// 就定了、拿不到收件人说明这一层结构上就不该收转交。所以它**不可重试**——不带这个哨兵的话，
// 配了重试的编排会把一次确定性的协议违规跑满重试次数，每次都重放你这个成员的副作用
// （实测：MaxRetries=3 时成员被跑 4 次）。
//
// 于是调用方有两种合法姿势，选哪种由你决定：
//
//   - **当致命错**：`return nil, err` —— 本次 Run 终局失败，框架清掉 cp 子树。
//   - **当一次没谈成的尝试**：吞掉它、改成自己作答。这时哨兵不起作用（error 没往上走），
//     跟转交从没发生过一样。
//
// # 为什么不让业务直接写产出消息的 annotation
//
// 那条路曾经存在（HandoffSignalKey），它绕过全部三道校验：**没有被授予**的成员照样转
// 得走（实测：一个连 transfer 工具都没有的候选，一行 annotation 就把对话转走了，err
// 全程 nil）。消息还会流经压缩层、hook、业务壳，任何一处动了那个字段路由就跟着变——
// 而这些层没有一个知道自己在改控制流。控制决策要走**只有被授予方能调用的入口**，不能
// 搭在人人可改的数据上。
func HandoffTo(ctx context.Context, opts []RunOption, self Agent, target string) error {
	if isAgentNil(self) {
		return fmt.Errorf("handoff: self 为 nil（含 Go typed nil interface 陷阱）: %w", compose_v2.ErrRunAborted)
	}
	if target == "" {
		return fmt.Errorf("handoff: 转交目标不能为空: %w", compose_v2.ErrRunAborted)
	}
	g := grantFromOpts(opts).grantFor(self)
	if g == nil {
		return fmt.Errorf("handoff: %q 没有被授予转交能力"+
			"（授予由上层 HandoffAgent 在调用它的直接候选时发出，且只发给受让方本人）: %w",
			self.Name(), compose_v2.ErrRunAborted)
	}
	if g.candidates != nil && !g.candidates[target] {
		names := make([]string, 0, len(g.candidates))
		for n := range g.candidates {
			names = append(names, n)
		}
		sort.Strings(names)
		return fmt.Errorf("handoff: 目标 %q 不在可转交候选 %v 中: %w", target, names, compose_v2.ErrRunAborted)
	}
	dest, ok := getHandoffDest(ctx)
	if !ok || !compose_v2.EmitSignal(ctx, handoffSignal, compose_v2.Signal{Dest: dest, Value: target,
		Origin: signalOrigin(ctx, "HandoffTo("+self.Name()+")")}) {
		return fmt.Errorf("handoff: 当前没有可转交的上层编排"+
			"（本次调用不在 HandoffAgent 的候选位上、本 agent 是被当作工具调用的，"+
			"或这是在 OnNodeEvent 钩子里——那里只作观测，决定要在发事件的那段代码里直接做）: %w",
			compose_v2.ErrRunAborted)
	}
	// 跟 transfer 工具做出的是**同一类**决定：先后做出的后者为准（你在内层 LLM 转交之后再调一次，
	// 以你这次为准）；同一超步另一个成员也做了决定的话是图的设计错误，当场终局
	// （compose_v2.SignalConflictError 并带 ErrRunAborted，见 side_signal.go；同一批 tool_call 里则排在前面
	// 的生效）。决定跟着你这段执行的提交走：你这次 Run 失败了，它就作废。
	return nil
}

// grantFromOpts 把 opts 应用到一个**弃用的** runConfig 上，读出里面的 grant。
// opts 全是字段 setter（无副作用），对调用方那份 opts 只读。
func grantFromOpts(opts []RunOption) *handoffGrant {
	var c runConfig
	for _, o := range opts {
		if o == nil {
			continue
		}
		o(&c)
	}
	return c.handoffGrant
}
