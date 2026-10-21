package compose_v2

import "fmt"

// pendingView 是「本成员自己那批还没提交的写入」叠在已提交态之上的只读视图。
//
// # 它存在的理由：让出边计算能排在提交之前
//
// 出边判断（条件边的 cond）**属于产出它的那个成员**——它读的是那个成员刚写出来的
// 东西，判的是那个成员下一步去哪。若让它读「本超步全体合并后的状态」，它就只能排在
// 合并提交**之后**；而一个排在提交之后、又**能失败**的相位，会造出「成员已经算成功、
// 写入已经提交、代际已经推进，却还得回去重跑」这个半成功状态。
//
// 本仓一整批症状都从那个状态来：中断身份漂移（visit 已推进）、节点私有态键位错配、
// 写入按 Append/Sum 双计、已成功成员被放回待执行集合。
//
// 把可见范围限死成「已提交 + 自己的写入」之后，出边计算可以排到**提交之前**：cond
// 失败就是这个成员失败，走既有的部分失败路径（写入不提交、代际不推进、只有它自己
// 重跑、兄弟照常记入 completed）。「半成功」这个状态**不可达**，而不是它的后果被兜住了。
//
// # 代价（明写在这里，因为它是业务可见的）
//
// cond **看不到同超步兄弟成员的写入**。要「看合并后的全貌再决定去哪」，做法是加一个
// join 节点、把条件边挂到它身上——那时全貌已经是上一超步的提交态了。
//
// # 惰性
//
// 只有 cond 真读到的那个 channel 才过一遍 reducer；没读到的一次计算都不做。
//
// # 安全性依据（不是新增要求）
//
// 叠加值算完即弃，所以 reducer **不能就地改 prev**。这条是 [Reducer] 早有的契约
// （godoc 原文「必须是纯函数：相同输入相同输出，无副作用」），而 `ApplyAll` 的归并
// 段**已经**依赖它：归并跑在一份局部 staged 上、算完才写回，而首次归并拿到的 prev
// **就是 `cm.values[name]` 本身**——reducer 若就地改它，一批写里靠后那条哪怕根本
// 没提交，cm 也已经被改脏了，「失败就等于什么都没发生」当场不成立。
// 四个内置 reducer 全是值语义（`Append` 是 make 新切片再拷）。
type pendingView struct {
	// writes 是本成员这批尚未提交的写入，按产生顺序——顺序承重：同一 channel 被写
	// 多次时要按这个顺序 reduce，跟 ApplyAll 的合并顺序一致。
	writes []ChannelWrite
}

// overlay 把本成员写向 name 的那些 write 依次 reduce 到 base 上。
//
// 没写过 name 时原样返回 base（零分配）。
func (p *pendingView) overlay(cm *ChannelManager, name string, base any, had bool) (any, bool) {
	if p == nil || cm == nil {
		return base, had
	}
	cur, ok := base, had
	for _, w := range p.writes {
		if w == nil || w.channelName() != name {
			continue
		}
		v, err := cm.reduceFor(name, cur, w.valueAny())
		if err != nil {
			// channel 未注册。**不在这里报错**——`ApplyAll` 的归并段会以同样的
			// 理由把整批提交拒掉，让错误在提交点统一出现（错误信息也在那儿更完整）。
			// 这里当作「这条写不存在」，视图退回已提交态。
			continue
		}
		cur, ok = v, true
	}
	return cur, ok
}

// hasWriteTo 报本成员这批写里有没有写向 name 的——`Has` 用，省掉 reduce。
func (p *pendingView) hasWriteTo(name string) bool {
	if p == nil {
		return false
	}
	for _, w := range p.writes {
		if w != nil && w.channelName() == name {
			return true
		}
	}
	return false
}

// reduceFor 用 name 这个 channel 注册的 reducer 算 prev ⊕ next，**不写回 cm**。
//
// 与 `ApplyAll` 归并段的区别只在「算完写不写回」——两者必须用同一个 reducer，否则出边看到的
// 值和提交后的值会分叉。
//
// reducer panic 在这里只补归因再抛回去（下面那道 recover），不改落点：本函数只在出边计算路径上被调用，而那条
// 路径整个跑在 `safeCondRun` 的 recover 之内 ⟹ panic 会变成「这个成员的 cond 失败」，跟 `ApplyAll`
// 里 reducer panic 变成整批提交失败是同一档处置（都不留半应用状态）。
func (cm *ChannelManager) reduceFor(name string, prev, next any) (v any, err error) {
	cm.mu.RLock()
	ch, ok := cm.channels[name]
	cm.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("channel %q not registered", name)
	}
	// 同 `composeRead`：**不吞 panic，只补归因再抛**。不补的话它落进 `safeCondRun` 变成
	// 「condition from %q panic recovered: <payload>」——报文里既没有 "reducer" 也没有
	// channel 名，排查的人会去翻那个 cond 函数。三条 reducer 调用路径现在报文形状一致。
	defer func() {
		if r := recover(); r != nil {
			if rerr, isErr := r.(error); isErr {
				panic(fmt.Errorf("reducer panic during out-edge overlay of channel %q: %w", name, rerr))
			}
			panic(fmt.Errorf("reducer panic during out-edge overlay of channel %q: %v", name, r))
		}
	}()
	return ch.reduceAny(prev, next), nil
}

// channelsWithPending 造一个带叠加层的只读视图——出边计算专用。
//
// writes 为空时返回的视图与 `Channels()` 等价（`pendingView.overlay` 遇空 writes 是
// no-op），所以 completedSibling 那种「写入早已提交、本超步没执行」的成员直接传 nil 即可。
func (cm *ChannelManager) channelsWithPending(writes []ChannelWrite) Channels {
	if len(writes) == 0 {
		return Channels{cm: cm}
	}
	return Channels{cm: cm, pending: &pendingView{writes: writes}}
}
