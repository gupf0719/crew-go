package compose_v2

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// SubgraphOption 配置子图作为父图节点时的边界行为。
//
// **设计决策**（针对 audit F5 多层嵌套传递性 + audit §2.3 子图作用域）：
//   - 默认完全隔离：子图看不到父图任何 channel；子图写的不冒泡父图
//   - 通过 WithChannelImport/Export 显式声明跨边界 channel
//   - 跨边界 channel 用 *Channel[T] 引用（父子图共享同一变量），编译期类型保护
//   - 不做隐式跨边界透传——Go 强类型场景下显式声明更安全
//
// 多层嵌套传递性：每层显式声明 import/export，不自动透传——避免"祖父图改 channel
// 类型，所有间接子图破坏"类型耦合 anti-pattern。
type SubgraphOption func(*subgraphConfig)

type subgraphConfig struct {
	imports []anyChannel // 子图每次进入时把父 cm 这些 channel 的值摆成子 cm 的 baseline（不经 reducer）
	exports []anyChannel // 子图执行完后把这些 channel **子图自己写的部分**经父 reducer 写回父 cm

	// isolateResumeContext: audit Round 9 Bug B2 修复——默认 false（forward 父
	// ResumeContext，95% 场景 caller 一次性 WithResumeAnswers 答全树正确路由）。
	// 业务用 sentinel key（如 PlanAgent `_replan_`）嵌套场景显式 opt-in true 隔离
	// 父子 ResumeContext，避免互相吃对方答案。
	isolateResumeContext bool
}

// WithImport 声明子图能读取父图 channel。
//
// 子图每次进入（新开或续跑）框架都把父 ChannelManager 里该 channel 的当前值摆成子
// ChannelManager 的 **baseline**——一层不经 reducer 的底层值。子图内节点 Get(chs, ch)
// 看到的是 reduce(baseline, 子图自己的写)：没写过就是父图的值，写过就是父图的值叠上
// 自己的（Append 看到 父的 ++ 自己的，Sum 看到 父的 + 自己的，LastValue 看到自己的）。
// 子图内对该 channel 的 write 不影响父图（除非同时 WithExport）。
//
// ⚠️ **合成读 == 顺序应用，当且仅当 reducer 满足关联律、且零值是它的左单位元**
// （`f(zero, x) == x`；内置 LastValue / Append / Sum / OnceFlag 都满足）。**左**不是右：
// own 是从零值起折叠出来的，所以要求的是零值在**左**边不改变右操作数；`LastValue` 的
// `f(x, zero) == zero` 右单位元压根不成立，而它工作正常。子图自己的写先在零值上链式归并成
// own，读时再算一次 reduce(baseline, own)——对不满足的自定义 reducer，子图读到的值跟
// 「把这些写顺序应用在父图值上」不同。实测会分叉的：计次 `p+1`（own 从零数、合成时
// 再加一次 ⟹ 少算）、平均 `(p+n)/2`、不判空的字符串拼接 `p+","+n`（零值 "" 参与 ⟹
// 多一个分隔符）。「不在 prev 里才追加」那种去重反而**能**工作：合成时 reducer 再跑一遍，
// 把 baseline 里已有的去掉了。
//
// ⚠️ 代价与别名：baseline 与 own 两层都有时**每次 `Get` 都合成一次**——Append 的 reducer
// 拷贝，宽 baseline（如万条消息流）下每次读 O(n) 分配（实测 1 万元素 32µs / 164KB）；
// 只有 baseline 时不合成、返回的是父图那份 slice 的**同一个引用**（只读契约照旧，改它
// 就改了父图）。
//
// 为什么是 baseline 而不是写进去：写进去会经 reducer，对 Sum / Append 是相加，续跑时
// 子 cp 里已有上次 import 的那份 ⟹ 每续跑一次多算一份（实测父 5 + 子 3 变成 18）。
//
// **类型保护**：父子图必须共享同一个 *Channel[T] 引用变量——这天然保证类型一致。
func WithImport[T any](ch *Channel[T]) SubgraphOption {
	return func(c *subgraphConfig) {
		c.imports = append(c.imports, ch.asAny())
	}
}

// WithSubgraphIsolatedResumeContext 让子图 NodeFunc 看不到父 Run 的 ResumeContext
// (UserInputs map)——audit Round 9 Bug B2 修复，默认 forward 行为的显式 opt-out。
//
// **默认行为**（forward）：subgraph 继承父的 ResumeContext。caller 一次性
// WithResumeAnswers map 答全树时子图 ask_user 通过框架 auto-ID（编入 graphNS）
// 路由到正确答案——这是 95% 场景。
//
// **opt-out 场景**：业务用 sentinel key（如 PlanAgent `_replan_`）的嵌套场景，
// 父子可能用同 key 互相吃答案——显式 isolate 让子图独立管理自己的 ResumeContext
// （caller 需另起 channel 给子图入口传答案）。
//
// **风险提示**：业务用 sentinel-key 嵌套时如果不 opt-out，父答案被子也消费。
// 业务自管 ID 唯一性 / 主动 opt-out 二选一。
func WithSubgraphIsolatedResumeContext() SubgraphOption {
	return func(c *subgraphConfig) {
		c.isolateResumeContext = true
	}
}

// WithExport 声明子图写入会冒泡到父图。
//
// 子图执行完后框架取该 channel **子图自己写的部分**（不含 WithImport 摆进来的
// baseline），**通过父图的 reducer**写入父 ChannelManager——父图侧的 reducer 决定如何
// 合并（典型用法：父图同名 channel 用 Append 累积多个子图的 export）。子图没写过的
// channel **不回送**：回送的话是把父图刚给的值原样还回去，父图 Sum 5→10、Append
// [a]→[a a]（实测）。
//
// 同一个 channel 同时 WithImport + WithExport 是正当用法：子图读到父图的值、在上面
// 累加、只把增量送回去。
//
// 父子图必须共享同一个 *Channel[T] 引用变量。
func WithExport[T any](ch *Channel[T]) SubgraphOption {
	return func(c *subgraphConfig) {
		c.exports = append(c.exports, ch.asAny())
	}
}

// AddSubgraphNode 把已编译的子图作为父图节点嵌入。
//
// 子图执行语义：
//  1. 创建独立子 ChannelManager（默认隔离）
//  2. 注册子图声明的所有 channel + import channel
//  3. 把父 cm 里 import 列表的 channel 值摆成子 cm 的 baseline（新开与续跑**都**摆，
//     baseline 在子图 Run 的 OnGraphStart **之前**摆好，与 Restore 互不相干——
//     Restore 只写 values 那一层）
//  4. Run 子图（Resume 时透传 WithResumeFrom）
//  5. 中断 → SaveNS(sid, childNS) 写子 cp 物理分离 ns；正常完成 → DeleteSubtree
//     清子 cp 子树
//  6. export 列表里**子图自己写过**的 channel 经父 reducer 写回父 cm（没写过的不回送）
//
// **嵌套 cp 物理分离**（Phase 5.1.a Step 4，对齐 ISSUE-70 Phase 2 Step Ⅰ）：
//   - 业务通过 RunOption WithCheckpointer + WithSessionID 注入
//   - 子图 cp 写到 (sessionID, parentNS+"/"+nodeID) 路径——独立 lookup
//   - 删旧 cm.subgraphSnapshots 嵌入式路径（被 ISSUE-70 决策否决）
//
// **要求**：嵌套 cp 必须配合 Checkpointer 注入。无 Checkpointer 注入时子图
// 中断会**丢失嵌套 cp**——业务必须显式注入。
//
// hooks 经 ctx OrchHooks 透传（sub Run 继承父 GetOrchHooks，graph_loop 兜底）。
func (b *GraphBuilder) AddSubgraphNode(id NodeID, sub *Graph, opts ...SubgraphOption) *GraphBuilder {
	cfg := &subgraphConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	subNodeFunc := func(ctx context.Context, parentChs Channels) ([]ChannelWrite, error) {
		parentCM := parentChs.cm

		// 嵌套 cp 物理分离：从 ctx 取 Checkpointer + sessionID + 父 ns，算子 ns
		cper := GetCheckpointer(ctx)
		sid := GetSessionID(ctx)
		parentNS := GetGraphNS(ctx)
		// ISSUE-96 侧面 2：带上**实例维度**——同一 subgraph 节点被 SendFrom 同 Target
		// 投递多次时，N 个实例各自的子 cp 必须物理分离。此前只用 id → N 个实例撞同一个
		// childNS → 后写的覆盖先写的 → 一个实例的进度和它的 HITL 问题静默蒸发。
		//
		// 静态/cond 成员的实例维是 **visit**（JoinVisitNS）：父循环第 N 轮重进本节点时
		// 子图从头执行、内部维度与上一轮全同，ns 不变则子图内 Ask 算出与上一轮相同的
		// InterruptID ⟹ 撞上已兑付的答案位、终局 ErrRunAborted（实测；答案账本跨子图边界之前是
		// 沿 ctx 活着的旧答案被静默复用、本轮提问从未呈现）。
		// 两维按成员类型互斥：SendFrom 实例 token 每次投递 fresh-mint 已带代际，不叠
		// visit（visitCount 是 per-NodeID 共享计数器，只由**空身份**成员推进，守卫在 advanceVisitsLocked）。
		var childNS string
		if fanoutIdx, ok := instanceFanoutIndex(GetCurrentInstanceID(ctx)); ok {
			childNS = JoinFanoutNS(parentNS, string(id), int64(fanoutIdx))
		} else {
			childNS = JoinVisitNS(parentNS, string(id), GetCurrentNodeVisit(ctx))
		}

		// **原 MED-3 fail-fast guard 已删**（ISSUE-96 侧面 5）：它当年拒绝"同 subgraph
		// 节点经同-Target SendFrom fan-out + 配 Checkpointer"，理由是 branchIdx 会在
		// Resume 时漂移、子 cp 对不上。现在 childNS 由**实例身份**派生（上方），身份
		// 随 cp 持久化、跨 Resume 稳定——那个理由不再成立，能力放开。
		//
		// 顺带说明它当年**根本没拦住**（实测）：① 只拦 branchIdx>0，实例 0 照跑；
		// ② guard 的错只进 FailedBranches、会被兄弟的 InterruptError 顶掉；③ Resume 时
		// frontier 缩成 1 → 走 executeFrontier fast path（硬编码 branchIdx=0）→ 不触发。
		// 净效果是 Run1 报一次、一次 Resume 就走过去、最终 err==nil。

		// 1. 检查 Checkpointer 是否有本子图的 saved cp（Resume 路径）
		var savedSubCp *Checkpoint
		if cper != nil && sid != "" {
			// Phase 2 Sub-3 audit fix: subgraph LoadNS fire cp.load event
			loadStart := time.Now()
			cp, err := cper.LoadNS(ctx, sid, childNS)
			// **判断算一次，事件和决策共用**——以前这里抄了一份下面那个决策的条件，
			// 靠注释「请保持一致」维系。同一份判据的两个拷贝在本仓漂移过四次，
			// 能合就合掉，别留手工同步的义务。
			// `willResume` = **决策**用的判据（存档状态 + 父的意图）。GraphID 不进来
			// ——它是 fail-fast 守卫不是续跑判据：不匹配时**报错**而不是「不续跑」。
			willResume := cp.HasResumableWork() && !IsFreshRunCtx(ctx) &&
				cp.BelongsToTask(getRunTaskID(ctx))
			// 事件那个字段的契约**另含 GraphID**（见 `CpLoadAction.WouldResume` 的 godoc：
			// 「GraphID 冲突 ⟹ false」），根图那处也是这么算的。合并镜像时若只用决策那份，
			// 同一字段在 `Source="auto_load"` 和 `"subgraph"` 两条路上语义就分叉了——
			// 业务按它统计「Resume 次数」会把一次注定报错的加载计成 Resume。
			eventWouldResume := willResume && (cp.GraphID == "" || cp.GraphID == sub.ID())
			loadAction := &CpLoadAction{
				SessionID:   sid,
				NS:          childNS,
				Found:       cp != nil,
				WouldResume: eventWouldResume,
				Source:      "subgraph",
				DurationMs:  time.Since(loadStart).Milliseconds(),
				Ok:          err == nil,
			}
			if cp != nil {
				loadAction.Status = cp.Status.String()
			}
			if err != nil {
				loadAction.Err = err.Error()
			}
			EmitCpEvent(ctx, &CpAction{Type: CpOperationLoad, Load: loadAction})
			if err != nil {
				return nil, fmt.Errorf("subgraph %q LoadNS(%q,%q): %w", id, sid, childNS, err)
			}
			// **判据与根图 auto-load 同一个谓词**（HasResumableWork，按内容判）。这里
			// 曾用 `Status.Resumable()` 而根图只认 `Pending`——同一份失败子档一条路捡、
			// 一条路不捡，实测嵌套 Resume 时副作用重放。判据收敛之后那个分歧不复存在：
			// 失败档（frontier 还指着失败分支）两条路都续，「新任务」由 fresh 信号表达。
			// **父说了这是新任务时，子图不续跑**。这条路径**绕过了根图那个闸门**——
			// 它显式 `WithResumeFrom(cp)`，而那个闸门第一条就是 `resumeFrom == nil`，
			// 于是整套「新开」信号在这里被完整跳过。实测后果：同 sid 跑第二个任务时子图
			// 整段没跑、业务拿到的 final 是**上一个任务的产出**，且 err == nil。
			//
			if willResume { // ← 注意与事件那个 `eventWouldResume` **不是**同一个值（后者另含 GraphID，见上），不再是两份要手工对齐的拷贝
				// Phase 5.1.b ISSUE-50 P0 防串台守卫：cp.GraphID 与当前 sub.ID()
				// 不匹配时 fail-fast——业务用同 (sid, childNS) 跑两个不同子 graph
				// 时 silent restore 会让新子图崩盘
				if cp.GraphID != "" && cp.GraphID != sub.ID() {
					return nil, fmt.Errorf("%w: subgraph %q LoadNS cp.GraphID=%q vs sub.ID()=%q (sid=%q ns=%q)",
						ErrGraphIDConflict, id, cp.GraphID, sub.ID(), sid, childNS)
				}
				savedSubCp = cp
			}
		}

		// 2. 创建子 ChannelManager + 注册子图声明的所有 channel
		subCM := NewChannelManager()
		for _, ch := range sub.channels {
			if err := subCM.register(ch); err != nil {
				return nil, fmt.Errorf("subgraph %q register channel %q: %w", id, ch.name(), err)
			}
		}

		// 3. Resume 路径：直接用 saved cp（Phase 5.1.b.4 合并后无需桥接）
		var subResumeCp *Checkpoint
		if savedSubCp != nil {
			subResumeCp = savedSubCp
		}

		// 4. import：把父 cm 的 channel 值摆成子 cm 的 baseline
		// **F5 修复**（audit Round 1）：Resume 路径**也**要从父 cm 读 import value——
		// 父图 Resume 时父节点可能改了 import channel value，子图应读最新值而非
		// saved cp 中首次执行时的旧值。
		//
		// 实现：用 withImportOverlay 注入——让 Run 内部在 OnGraphStart 之前 setBaseline
		// （新开与续跑共用一处）。直接写 subCM 不行：那是在 Run 之外，拿不到子图注册完
		// channel 之后的时机。**摆成 baseline 而不是
		// 经 reducer 写进去**：写进去对 Sum / Append 是相加，续跑时子 cp 里已有上次
		// 那份 ⟹ 每续跑一次多算一份；而且会把子图自己对该 channel 的写盖回父图的值。
		var importInitialWrites []ChannelWrite
		for _, ich := range cfg.imports {
			if err := subCM.register(ich); err != nil {
				return nil, fmt.Errorf("subgraph %q import %q: %w", id, ich.name(), err)
			}
			parentVal, ok := parentCM.read(ich.name())
			if !ok {
				continue // 父图未写入：子图读到 zero
			}
			importInitialWrites = append(importInitialWrites, &exportWrite{ch: ich, value: parentVal})
		}

		// 5. Run 子图——透传 Checkpointer/sessionID + 子 ns（让子图 inner 子图也能
		// 物理分离 cp 到孙 ns 路径）
		subOpts := []RunOption{}
		if cper != nil {
			subOpts = append(subOpts, WithCheckpointer(cper))
		}
		if sid != "" {
			subOpts = append(subOpts, WithSessionID(sid))
		}
		subOpts = append(subOpts, WithRunNS(childNS))
		if subResumeCp != nil {
			subOpts = append(subOpts, WithResumeFrom(subResumeCp))
		}
		if len(importInitialWrites) > 0 {
			subOpts = append(subOpts, withImportOverlay(importInitialWrites...))
		}

		// **audit Round 9 Bug B2 修复**：默认 forward 父 ResumeContext（95% 场景
		// caller 一次性 WithResumeAnswers 答全树正确路由）。业务用 sentinel key
		// （如 PlanAgent `_replan_`）嵌套场景显式 WithSubgraphIsolatedResumeContext
		// 隔离避免父子互相吃对方答案。
		subRunCtx := ctx
		if cfg.isolateResumeContext {
			subRunCtx = StripResumeContext(ctx)
		}
		runErr := Run(subRunCtx, sub, subCM, subOpts...)

		if runErr != nil {
			// 6. 中断/失败 → graph_loop 内部 defer 已自动 SaveNS 子 cp 到
			// (sid, childNS) ns 物理分离（graph_loop.go:243-258）—— **R27a audit
			// LOW-1 fix**：原 subgraph 这里又调一次 SaveNS 是冗余的（两次写同一
			// (sid, childNS) 内容相同），InMemoryCheckpointer 第二次覆盖无 noop，
			// 但**业务自定义 Checkpointer**（Redis/Postgres/S3）每次 SaveNS 是 RPC
			// → 双倍开销 + 嵌套 N 层时 N 次冗余写。删除让 graph_loop defer 独家负责。
			//
			// snapErr / cp save 失败的 wrap 处理已由 graph_loop defer line 246
			// 完成（含 "channel value type not registered" 提示），caller 拿到的
			// runErr 已含相应 wrap，不再需要 subgraph 重复处理。
			return nil, fmt.Errorf("subgraph %q: %w", id, runErr)
		}

		// 7. export：先构造 export ChannelWrite——稍后由父 graph_loop 在超步合并
		// 点 ApplyAll（与 sibling writes 一起 atomic）
		//
		// **audit Round 9 Bug D4**：原顺序"先 DeleteSubtree → 再返 export writes"
		// 在父 reducer panic 时（父 cm.values 整批不提交）让子 cp 已被删
		// 而父未真正接收 export → Resume 时父重跑子图从头（子图副作用可能不幂等）。
		// 改为"先建 export writes → 再 DeleteSubtree"——即使 reorder 也无法在
		// subNodeFunc 内等待父 ApplyAll 结果（写实际由 graph_loop 后续做），但保
		// 证至少 export writes 在 cp 删之前**完整构造**：业务自定义子 cm.read 时
		// 不会因子 cp 已删导致 read 异常。
		// **只送子图自己写的部分**（ownValue，不含 import 摆进来的 baseline）。用 read
		// 的话，import 进来又没被碰过的 channel 会被原样回送，父图 Sum 5→10、Append
		// [a]→[a a]，err 全程 nil（实测）——read 分不清「子图产出的」和「刚灌进去的」。
		var writes []ChannelWrite
		for _, ech := range cfg.exports {
			subVal, ok := subCM.ownValue(ech.name())
			if !ok {
				continue
			}
			writes = append(writes, &exportWrite{ch: ech, value: subVal})
		}

		// 8. 子图成功完成 → 清子 cp 子树（含本子图自身 cp + 任何 grandchild cp）
		// 清失败仅 warn——不影响成功路径（audit Round 2：兑现注释承诺加 slog.Warn）
		//
		// **D4 残留 caveat**：父 ApplyAll 后续若因 reducer panic 整批不提交，子
		// cp 此时已删 → Resume 时父重跑此 subgraph node 时子图从 scratch 重建。
		// 若业务 subgraph 非幂等（如调外部 RPC 计费），可能重复执行一次。生产场景
		// LLM agent subgraph 通常幂等可重入，影响有限。彻底修需让 graph_loop 把
		// "subgraph DeleteSubtree" 推迟到自己 ApplyAll 成功后——成本过大，留作
		// 已知设计取舍。
		if cper != nil && sid != "" {
			// Phase 2 Sub-3 audit fix: subgraph DeleteSubtree fire cp.cleanup event (统一 helper)
			cleanupStart := time.Now()
			delCount, delErr := cper.DeleteSubtree(ctx, sid, childNS)
			EmitCpCleanupHelper(ctx, sid, childNS, "success", delCount, time.Since(cleanupStart).Milliseconds(), delErr)
			if delErr != nil {
				slog.WarnContext(ctx, "compose_v2: subgraph cp DeleteSubtree on success failed",
					"sid", sid, "childNS", childNS, "subID", id, "err", delErr)
			}
		}

		return writes, nil
	}

	b.nodes[id] = subNodeFunc
	return b
}

// exportWrite 是子图 export 写回父 cm 的 ChannelWrite。
// 用类型擦除版本（不是 channelWrite[T]）——因为 SubgraphOption 收 anyChannel
// 已擦除 T，没办法再恢复 typed channelWrite[T]。
//
// 安全性：父 cm 的 reducer 收 prev / next 都是 any，内部 reduceAny cast 回 T——
// 这里 value 是子图**自己的写**经子 reducer 归并后的结果（不含 import 的 baseline），
// 类型必然匹配父 channel 的 T（因为 import/export 共享同一 *Channel[T] 引用）。
// import 侧复用本类型装父图的值（`setBaseline` 只取 channelName + valueAny）。
type exportWrite struct {
	ch    anyChannel
	value any
}

func (w *exportWrite) channelName() string { return w.ch.name() }

func (w *exportWrite) valueAny() any { return w.value }

func (w *exportWrite) notRegisteredErr() error {
	// 父图未注册同名 channel——业务必须在父图也声明该 channel
	return fmt.Errorf("export channel %q not registered in parent graph", w.ch.name())
}
