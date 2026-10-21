package compose_v2

import (
	"context"
	"errors"
	"fmt"
)

// ── 出边计算：会失败的那一半归成员，纯机械的那一半留在提交之后 ──
//
// 「决定下一步去哪」在本引擎里被劈成两段：
//
//  1. **`evalOutEdges`（会失败）**——跑业务的 cond。它属于**产出这条出边的那个成员**，
//     排在该成员的执行里、任何提交之前，读的是「已提交 + 该成员自己的写入」。
//     它失败就是那个成员失败。
//  2. **`assembleFrontier`（不会失败）**——去重、分离 EndNode、拼下一帧。纯机械。
//
// 这个划分是本仓一整批症状的根治：先前 cond 排在合并提交**之后**且能失败，于是存在
// 「成员已算成功、写入已提交、代际已推进，却还得回去重跑」这个半成功状态——中断身份
// 漂移、节点私有态键位错配、写入双计全从它来。劈开之后那个状态**不可达**。

// evalOutEdges 算 from 这个节点的出边目标；条件边的 cond 在这里跑。
//
// chs 决定它看得见什么：
//
//   - 正在执行的成员 → 传 `cm.channelsWithPending(它自己的 writes)`
//   - completedSibling（写入早已提交、本超步不执行）→ 传 `cm.Channels()` 即可
//
// 返回的 targets 可能含 EndNode；去重与 isEnd 判定交给 [assembleFrontier]。
//
// **返回错误 = 产出这条出边的那个成员执行失败**，调用方据此把它归进失败集合：写入不
// 提交、代际不推进、只有它自己重跑、兄弟照常记入 completed。
//
// ph 给每条 cond 铺相位（见 [condPhase]）：cond 里发起的嵌套调用有自己的身份维，做成的
// 步骤记在这个成员这次执行名下。
func evalOutEdges(ctx context.Context, g *Graph, from NodeID, chs Channels, ph condPhase) ([]NodeID, error) {
	outEdges := g.adj[from]
	if len(outEdges) == 0 {
		// 无出边的非 StartNode 节点：等价路由到 EndNode（自然终止）
		if from == StartNode {
			return nil, nil
		}
		return []NodeID{EndNode}, nil
	}

	targets := make([]NodeID, 0, len(outEdges))
	for i, e := range outEdges {
		if e.cond == nil {
			targets = append(targets, e.to)
			continue
		}
		// cond fn panic 转 error——业务自定义 cond panic 时不能让 goroutine crash
		// 直接穿出 Run，绕过 defer SaveNS / OnAgentEnd 兜底。与 safeNodeRun 同款。
		//
		// 顺带：叠加视图里的 reducer 若 panic 也落进这道 recover（`reduceFor` 自己那道
		// recover 只补归因再抛回来），于是变成「这个成员的出边算不出来」= 成员失败，跟 ApplyAll 里
		// reducer panic 变成整批提交失败是同一档处置——两边都不留半应用状态。
		t, err := safeCondRun(ph.forEdge(ctx, i), e.cond, chs, from)
		if err != nil {
			return nil, wrapCondFailure(from, e.condFailPolicy, err)
		}
		if t == "" {
			targets = append(targets, EndNode) // 空字符串 = 路由到 EndNode
			continue
		}
		if t != EndNode {
			if _, ok := g.nodes[t]; !ok {
				// cond 返回了图里不存在的节点——**图定义错，重跑必然一样**，故终局
				// （`%w ErrRunAborted` ⟹ 不重试 + Run 出口清 cp 子树）。
				//
				// 只豁免 EndNode（合法终止 sentinel）。StartNode 是入口 sentinel、不在
				// g.nodes 里，放它过去会让它进 frontier 后炸在「node 未注册」上，且存档
				// 里带着一个跑不动的 NodeID → 每次 Resume 原地再炸。
				return nil, fmt.Errorf(
					"%w: condition from %q returned unregistered/unroutable target %q "+
						"(StartNode 入口 sentinel 不可作 cond target; graph nodes: %d)",
					ErrRunAborted, from, t, len(g.nodes))
			}
		}
		targets = append(targets, t)
	}
	return targets, nil
}

// evalMemberOutEdges 算正在执行的成员 m 的出边（视图 = 已提交 + m 自己的写入 ws）。
//
// cond 按 [CondFailRerunFrom] **否决**时（[condRejected]），推进 m 这次执行的重做代际：此前按
// [Rerunnable] 记下的步骤随之不可达，续跑重跑 m 时那些步骤重新做，m 重做时问的问题也是新问题
// （见 [InterruptSite] 的 RerunGen）。那一档的语义就是「m 的产出不行，重跑它换一份」——不推进
// 的话，m 里做过的那一步（经 `agent_v2.RunSubAgent` 调、声明了 `agent_v2.WithRerunnable` 的
// planner）原样拿回上一份产出，cond 照样报错，续跑多少次都一样（实测过）。副作用记录（不带
// Rerunnable 的）不受影响，重跑时照样拦住重放。
//
// cond 里做出的上行信号（cond 里发起的嵌套调用交上来的）跟着这个成员走：出边算得出来时随它的
// 其余产出一起返回、一起提交；算不出来时一起作废（见 signal.go）。不收的话它们会越过本图的提交
// 直接落进调用这张图的那段执行——成员被否决、重做之后，被否决那次做出的决定仍在。
func evalMemberOutEdges(ctx context.Context, g *Graph, cm *ChannelManager, m FrontierMember, ws []ChannelWrite) ([]NodeID, []SignalRecord, error) {
	visit := cm.GetVisitCount(m.NodeID)
	nodeKey := nodeStateKey(m.NodeID, m.InstanceID, visit)
	sc := &signalScope{}
	targets, err := evalOutEdges(withSignalScope(ctx, sc), g, m.NodeID, cm.channelsWithPending(ws), condPhase{
		from:       m.NodeID,
		instanceID: m.InstanceID,
		visit:      visit,
		acc:        &nodeStateAccessor{cm: cm, key: nodeKey},
	})
	if condRejected(ctx, err) {
		cm.bumpRerunGen(nodeKey, "")
	}
	if err != nil {
		return nil, nil, err
	}
	return targets, sc.drain(), nil
}

// condRejected 报 cond 的这个错是不是一次**否决**：它判过了节点的产出、判为不合格，节点得
// 重新产出（重做代际随之推进，按 Rerunnable 记下的产出作废）。
//
// cond 被外面停下的不算：软取消、陈旧写回、调用方的 ctx 已经死了（[IsRetryFutile]）。那时产出
// 很可能根本没被判完，照旧有效——续跑时按 Rerunnable 记下的照常拿回。当成否决的话，用户只是
// 按了停止，续跑就把已经完成的产出（连同那次调用里的副作用）再做一遍（实测）。
//
// ctx 那档**只看 ctx 死没死、不看错误链上有没有 ctx 错**。判官被打断时返回的错不一定 `%w` 着
// ctx 错（业务自写的判官、第三方 SDK 用 `%v` 包一层就断了链），要求「链上有 ctx 错」才算被
// 停下的话，那种情况被判成否决：用户按了停止，续跑重做了已经完成的产出（实测）。两种误判的
// 代价不对称：把「判完给出否决、ctx 恰在这之后到点」当成没判完，这一次否决记不上、续跑时再判
// 一次；把「被打断」当成否决，重做产出连同里面的副作用。所以 ctx 死了就一律按没判完。
//
// ⚠️ 代价是：判官**每次**都在调用方 ctx 到点之后才给出否决时，一次否决都记不上，续跑永远拿回
// 同一份产出、永不收敛（实测：每次续跑给 30ms deadline、判官比它慢）。给判官留够时间——它的
// 截止应当早于调用方的。
//
// cond 自己出的错——判官的基础设施错（它调的模型 503、它调的后端自己超时而调用方 ctx 还活着）、
// cond panic（比如上游数据缺了一块、读到 nil）——框架分不出是「判为不合格」还是「判不了」，
// 仍按否决处理：这两种情况下让上游重新产出一份，正是 [CondFailRerunFrom] 声明的意图。
func condRejected(ctx context.Context, err error) bool {
	if !errors.Is(err, errCondRerunFrom) {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return !IsRetryFutile(ctx, err)
}

// nestedEntryRejected 标记「这张嵌套图的入口条件边按 [CondFailRerunFrom] 否决了它的输入」：输入
// 是承载它的外层节点这次执行里 scope 那个分域产出的（节点体是执行级、为空）。
//
// 否决当场不推进代际：框架不知道外层会不会因它重做（见 rerunGeneration 的说明）。标记随错误往上
// 走，外层图在**那个成员以这个错失败时**推进（[applyNestedEntryRejections]）；错被兜底吞掉、被
// 重试消化、被转成工具的失败结果，就不推进。否决当场另在宿主的 cm 上记一笔（noteEntryRejection），
// 给「标记没随错误传上来」的情况兜底，见 applyNestedEntryRejections。
type nestedEntryRejected struct {
	cm    *ChannelManager
	key   string
	scope string
	err   error
}

func (e *nestedEntryRejected) Error() string { return e.err.Error() }
func (e *nestedEntryRejected) Unwrap() error { return e.err }

// noteEntryRejection 记下「key 这次执行里 scope 那个分域产出的输入被嵌套图的入口否决过」。
func (cm *ChannelManager) noteEntryRejection(key, scope string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.entryRejections == nil {
		cm.entryRejections = make(map[string]map[string]struct{})
	}
	scopes := cm.entryRejections[key]
	if scopes == nil {
		scopes = make(map[string]struct{}, 1)
		cm.entryRejections[key] = scopes
	}
	scopes[scope] = struct{}{}
}

// takeEntryRejections 取走并清掉 key 这次执行名下记的入口否决。
func (cm *ChannelManager) takeEntryRejections(key string) map[string]struct{} {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	scopes := cm.entryRejections[key]
	delete(cm.entryRejections, key)
	return scopes
}

// applyNestedEntryRejections 在成员 m 结束时（err 为它的结局）结算它这次执行里的嵌套图入口否决：
//
//   - 成功（err 为 nil）：否决被吞掉、被重试消化、被转成工具的失败结果——什么都不推进；
//   - 失败：推进 err 里每一个属于 m 这次执行的 [nestedEntryRejected] 标记的分域代际（err 可能是
//     多错误树：同一批两个 tool_call 各自被否决）。别的图、别的执行的标记不认——那是它们自己
//     那一层的事；
//   - 失败、而且是**非控制流**的错（不是中断，调用方 ctx 也还活着）：这次执行里记过、却没在 err 里
//     找到标记的否决也推进。标记只能沿错误链往上走，而链会断：业务用 `%v` 包一层（实测：续跑永不
//     收敛），扇出的节点只挑一个错往上报（实测：落选分支的否决丢了、多续跑一轮）。代价是「否决被吞
//     掉之后又因为别的业务错失败」时多重做一次产出——那个分域里的提问随之换新，不会兑付旧答案。
//     中断 / 被停下时照旧只认标记：否决被吞掉、转而问人的执行，续跑时要原样重放到那一问。
func applyNestedEntryRejections(ctx context.Context, cm *ChannelManager, m FrontierMember, err error) {
	key := nodeStateKey(m.NodeID, m.InstanceID, cm.GetVisitCount(m.NodeID))
	noted := cm.takeEntryRejections(key)
	if err == nil {
		return
	}
	bumped := map[string]bool{}
	walkErrTree(err, func(e error) {
		rej, ok := e.(*nestedEntryRejected)
		if !ok || rej.cm != cm || rej.key != key || bumped[rej.scope] {
			return
		}
		if condRejected(ctx, rej) {
			bumped[rej.scope] = true
			cm.bumpRerunGen(key, rej.scope)
		}
	})
	if len(noted) == 0 {
		return
	}
	if _, isInt := IsInterrupt(err); isInt || IsRetryFutile(ctx, err) {
		return
	}
	for scope := range noted {
		if !bumped[scope] {
			cm.bumpRerunGen(key, scope)
		}
	}
}

// walkErrTree 按 errors.Is 的遍历方式（单链 Unwrap + 多错误 Unwrap []error）走遍 err 这棵树。
func walkErrTree(err error, visit func(error)) {
	for err != nil {
		visit(err)
		switch u := err.(type) {
		case interface{ Unwrap() []error }:
			for _, e := range u.Unwrap() {
				walkErrTree(e, visit)
			}
			return
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		default:
			return
		}
	}
}

// wrapCondFailure 按声明的策略给 cond 的失败定性——**只决定「返什么错」**，
// 「谁重跑」由调用方把它当成成员失败之后交给既有的部分失败机件。
func wrapCondFailure(from NodeID, policy CondFailPolicy, err error) error {
	// **cond 禁止 interrupt**：中断的本质是要新输入；cond 需要新输入才能判断，说明判断
	// 所需的信息还没备齐——那是**上游 node 没尽责**。让 cond 中断还自相矛盾：cond 是纯
	// 判断，Resume 后要重新执行，用户答案只能从 channel 读——绕一圈还是得让上游备好。
	//
	// **转成非-interrupt error**（不 `%w` 原 err）：若让 IsInterrupt 仍为 true，caller
	// 会当 HITL 等答案，但框架并没有 recordInterrupt ⟹ 无 ID 可配 ⟹ Resume 永久死锁。
	//
	// **恒终局，不受 policy 影响**：协议违规是业务代码 bug，重跑必然同样违规。
	if ie, isInt := IsInterrupt(err); isInt {
		return fmt.Errorf(
			"%w: condition from %q raised InterruptError (reason=%q) — cond fn 是纯路由判断、不能中断。\n"+
				"  中断的本质是要新输入；cond 需要新输入才能判断，说明判断所需的信息还没备齐——那是上游 node 的责任。\n"+
				"  · HITL 请改在 **node 内**抛 InterruptError（node 内 writes 未提交，重跑自洽），后接 cond 读 channel 路由\n"+
				"  · RoutingAgent 场景直接用 WithRoutingClassifier / WithRouter —— 两者都在 classify node 内跑，原生支持 HITL",
			ErrRunAborted, from, ie.Reason)
	}
	// cond 是**纯判断**，故它报错只可能是「判断的前提不成立」= 上游产出的数据缺失/非法。
	// 该重试还是该放弃，取决于**上游是不是确定性的**——纯计算上游重跑必拿同样的坏数据；
	// LLM 上游（planner / classifier）重跑很可能就好了。只有业务知道，故交业务声明。
	switch policy {
	case CondFailRerunFrom:
		// 裹 errCondRerunFrom 让出口把存档标 Pending 而不是 Failed——本档的语义就是
		// 「存着等 Resume 重跑这个成员」，而档位判据看的是**框架对这个错的处置意图**，
		// 不是错的类型（它返的是业务错）。
		return fmt.Errorf("%w: condition from %q: %w", errCondRerunFrom, from, err)
	default: // CondFailAbort
		// 本来就终局的错（判官里嵌套运行被终局、JoinSignals 的冲突……）只补位置、不再叠一层标记：
		// 它自己带着，再叠只会让正文里出现两遍。
		if errors.Is(err, ErrRunAborted) {
			return fmt.Errorf("condition from %q: %w", from, err)
		}
		// 双 %w（Go 1.20+）：errors.Is(err, ErrRunAborted) 与 errors.Is/As(原 err) 都命中。
		return fmt.Errorf("%w: condition from %q: %w", ErrRunAborted, from, err)
	}
}

// assembleFrontier 把一批出边目标去重、分离 EndNode，产出下一帧 frontier + isEnd。
//
// **纯机械、不可能失败**——这是「路由」劈开之后留在提交之后的那一半。
//
// isEnd 判断和 next frontier 计算合在一起：算完发现全是 EndNode 就立即 isEnd=true，
// 调用方据此直接返回成功，不进下一帧（避免 ctx 在窗口期 deadline 误判）。
func assembleFrontier(targets []NodeID) ([]FrontierMember, bool) {
	// **去重保留首次出现序，不 range map。**
	//
	// 出边本来就是有序的（`Graph.adj` 是 `[]edge`、按 `AddEdge` 声明序 append，
	// `evalOutEdges` 也按那个序产 targets）——只有这里一度灌进 map 再 range 把它丢了。
	//
	// 承重的理由不是「让它稳定」（稳定 ≠ 正确，报第一个和报第 K 个都是任选），是
	// **顺序信息本来就有、别丢**：N 个分支同时失败时返给调用方的那一个 error 按 frontier
	// 序取，map 遍历序让「报哪个」跨次漂。实测那个分布是 **87/13** 不是 50/50——
	// 5 次的 CI 大概率一次都撞不上、40 次撞 4~5 次、线上按流量撞。「测得过、线上炸、
	// 复现不了」比五五开难查得多。
	seen := make(map[NodeID]struct{}, len(targets))
	hasEnd := false
	live := make([]NodeID, 0, len(targets))
	for _, id := range targets {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		if id == EndNode {
			hasEnd = true
			continue
		}
		live = append(live, id)
	}
	if hasEnd && len(live) == 0 {
		return nil, true // ← isEnd 信号在这里产生
	}
	// live 已按 targets 的声明序去重 → 全是静态边 / cond 边成员，InstanceID 空。
	// SendFrom 派生的成员由调用方另行 append（带 mint 好的身份）。
	return staticMembers(live), false
}

// computeStartFrontier 算入口帧——StartNode 的出边。
//
// 单独留一个入口，是因为 StartNode **不是被执行的成员**：它没有「自己的写入」可叠，也
// 没有成员身份可以承接 cond 失败。**而这里失败是安全的**——此刻本次 Run 一个字节都还
// 没提交，跟「已提交之后才发现出边算不出来」不是一回事。
//
// acc 是入口条件边记东西的地方：嵌套图里是承载它的外层节点那次执行（按这张图分域），跟图级
// 钩子同一个去处；顶层图没有，传 nil。producer 是产出这张图输入的那个分域：调起这张图的代码
// 所在的访问器（节点体、某个 tool_call、某个钩子相位……），经 [HandOver] 交过来的主图则是交接
// 之前调用方的那一格（见 Run 入口的 producer）；顶层图为 nil。
//
// 入口条件边按 [CondFailRerunFrom] 否决时，给错误打上 [nestedEntryRejected] 标记，外层成员以它
// 失败时推进 producer 那个分域的重做代际：嵌套图从 StartNode 起跑的输入是 producer 给的，「从
// from 重跑、让它重新产出」的上游就是它。不推进的话，入口条件边里经 `agent_v2.RunSubAgent` 调的
// router 连同 producer 按 Rerunnable 记的产出原样拿回，续跑多少次都一样（实测）。顶层图没有外层
// 节点，入口条件边里也没有记下的东西，重跑本来就重新判。
//
// 入口条件边里做出的上行信号算这张图的：出边算得出来就并入本图已提交的状态（第一超步之前），图
// 成功跑完时随其余的一起交出去；算不出来就作废。
func computeStartFrontier(ctx context.Context, g *Graph, cm *ChannelManager, acc, producer *nodeStateAccessor) ([]FrontierMember, bool, error) {
	sc := &signalScope{}
	targets, err := evalOutEdges(withSignalScope(ctx, sc), g, StartNode, cm.Channels(), condPhase{from: StartNode, acc: acc})
	if err != nil {
		if producer != nil && condRejected(ctx, err) {
			producer.cm.noteEntryRejection(producer.key, producer.scope)
			err = &nestedEntryRejected{cm: producer.cm, key: producer.key, scope: producer.scope, err: err}
		}
		return nil, false, err
	}
	cm.commitSignals(sc.drain())
	frontier, isEnd := assembleFrontier(targets)
	return frontier, isEnd, nil
}
