package compose_v2

import (
	"context"
	"log/slog"
	"sync"

	"crew-go/internal/panicerr"
)

// SendDirective 节点运行时动态产生的 fan-out 指令（运行时动态 map-reduce）。
//
// 机制：节点 NodeFunc 内**imperative**调 SendFrom(ctx, ...) 写 ctx-injected
// sink（更直观，不强制走 conditional edge 回调），节点 return 后框架 drain sink
// 在提交时调度。普通目标进入下一超步，标记为 [GraphBuilder.WithDeferredNodes] 的目标进入收尾队列。
// Payload 经 channel reducer 写入共享状态，私有 Input 随目标实例保留。
//
// ⚠️ **Payload 写进的是共享 channel 状态，不是「这条指令的私有入参」**。N 条指令的
// payload 在下一超步开始前**一次性归并**进同一批 channel——下一超步的**所有**节点都看到
// 同一份。所以同一个 target 被扇出 N 次时：LastValue 只剩最后一条（引擎当场拒，见
// [LastValue]）；Append 全部累积，但**每个实例拿不到自己是第几个**——[InstanceFanoutIndex]
// 返回的是进程级单调计数器，不是本次扇出里的 0..N-1，也没有别的 API 给序号。
//
// 同一 Target 的各个实例需要不同参数时，给每条投递的 Input 填自己的写入：
//
//	for _, task := range tasks {
//	    compose_v2.SendFrom(ctx, compose_v2.SendDirective{
//	        Target: "worker",
//	        Input: []compose_v2.ChannelWrite{compose_v2.Write(workerInputCh, task)},
//	    })
//	}
//
// 共享写入与仅调度的用法：
//
//   - **不同输入 → 不同 target**（ParallelAgent 就是这么做的，`worker_<name>` 各一个节点）：
//
//     for _, br := range branches {
//     directives = append(directives, compose_v2.SendDirective{
//     Target:  compose_v2.NodeID("worker_" + br.Name),
//     Payload: []compose_v2.ChannelWrite{compose_v2.Write(inputOf(br), br.Input)},
//     })
//     }
//     compose_v2.SendFrom(ctx, directives...)
//
//   - **同 target 扇出 N 次并共享同一输入**：Payload 留空，共享输入由发出方在自己的 writes 里
//     写好，N 个相同 worker 读同一份。各实例靠 [GetCurrentInstanceID] 区分身份，那是不透明
//     token，只能当 key、不能当序号。
//     ⚠️ **别指望「N 条 payload 写同一个值」**：检测不比值，单写入者 channel 上照拒（实测
//     两条同值 payload → `ErrRunAborted`、worker 零执行）；换成 Append/Sum 则累积 N 份，
//     也不是「同一份」。
type SendDirective struct {
	// Target 目标节点 ID——必须已在 graph 注册。图里没有的节点、起点 sentinel 让发出它的节点以 ErrRunAborted 失败
	// （图定义错，重跑必然一样：不重试、清档，同条件边返回坏目标）；EndNode 与空串跳过不投递。
	Target NodeID

	// Payload 是一批 ChannelWrite，framework 在下一超步入口归并进**共享** channel 状态——
	// 下一超步所有节点（不只这条指令的 target）都看到。**它不是 target 的私有入参**，
	// 见类型 godoc 的 ⚠️ 段：同 target 扇出 N 次时 N 条 payload 塌成一份。
	//
	// Payload 为空时没有本条投递的共享写入。
	Payload []ChannelWrite

	// Input 给本条投递产生的目标实例设置私有 channel 值。每个 channel 从零值起按顺序归并，
	// 只覆盖这个实例及其条件边的读视图，未提供的 channel 仍读共享状态。写入和后继实例各自走超步提交。
	// 暂停时 Input 随实例存档。值遵守 channel 的不可变约定和存档类型注册要求。
	Input []ChannelWrite

	// owner 发出本指令的节点 ID——**框架在节点 return 后 drain sink 时无条件盖写**
	// （不导出：业务构造 SendDirective 时既填不了也不该填，伪造无效）。
	//
	// **为什么必须有**：部分失败超步的成功成员 A 的 sends 会存进 cp.DeferredSends
	// 等 Resume 重放（PA-H1）。若 A 之后被降级为重跑目标（自己的 cond 报错 +
	// CondFailRerunFrom），A 重跑会再产一份同样的 sends——框架得靠 owner 认出
	// "哪条旧 deferred 是 A 的"并清掉，否则 Target 被调度两次（issue-95#1）。
	owner NodeID

	// ownerInstanceID **发出者自己**的实例身份（ISSUE-96）——与 owner 配对。
	//
	// **为什么 owner 光有 NodeID 不够**：purge"重跑成员的旧 send"用的 rerun 是个
	// **实例集合**（`map[FrontierMember]struct{}`，成员带实例身份）——completed 那半
	// 天然按成员匹配，deferred 这半要用同一个集合，就得能拼出完整的成员身份：
	// owner 的 NodeID + 发出者自己的实例。少了本字段，匹配键恒是 {NodeID, ""}，
	// 跟带身份的 rerun 成员**永远匹配不上** ⟹ 重跑成员的旧 send 一条都清不掉，
	// Resume 时旧 send 重放叠上重跑新产 = Target 双发（实测 W 从 2 变 4）。
	//
	// 静态边节点发出的 send：空串（它们恒单实例，rerun 里的成员同样是 {NodeID, ""}，
	// 匹配照样精确）。
	ownerInstanceID string

	// instanceID 本指令派生出的目标节点实例的**身份**（ISSUE-96）——同样由框架在
	// drainSendSink 无条件 mint，业务填不了。
	//
	// **为什么 mint 在这里而不是"target 进 frontier"那一刻**：deferred send 从 cp
	// 重放回来时**不该换新身份**——它代表的还是同一次投递。身份跟着 directive 走、
	// 随 cp 持久化，"重跑产新的"才换新 token（那正是代际语义的来源）。
	instanceID string
}

// sendSinkKey 是 ctx 注入键（私有避免碰撞）。
type sendSinkKey struct{}

// sendSink 收集一次节点尝试的动态投递与提交后通知。
type sendSink struct {
	mu          sync.Mutex
	directives  []SendDirective
	afterCommit []func()
}

// SendFrom 在节点函数（NodeFunc）内调用，动态投递 fan-out 指令。
//
// **必须从 NodeFunc 内调用**——否则 ctx 没有 sendSink，调用静默 noop（不报错——
// 业务复用 NodeFunc 在框架外测试时不应崩）。
//
// 成功提交时框架按声明顺序调度，收尾目标在普通工作排空后执行。重复 Target 不去重
// （caller 想要 N 个并发 worker 时按 N 次调用）。
//
// **失败语义**：节点 return error 时 directives 自然被丢弃（compose_v2 BSP 协议：
// 失败分支 writes 不 commit，directives 同理）。Resume 重跑时节点 NodeFunc 重
// 调 SendFrom 再产 directives——保 idempotent 由 caller 保证。
func SendFrom(ctx context.Context, directives ...SendDirective) {
	sink, _ := ctx.Value(sendSinkKey{}).(*sendSink)
	if sink == nil || len(directives) == 0 {
		return
	}
	sink.mu.Lock()
	for _, directive := range directives {
		directive.Input = append([]ChannelWrite(nil), directive.Input...)
		sink.directives = append(sink.directives, directive)
	}
	sink.mu.Unlock()
}

// withSendSink 注入 sendSink 到 ctx——由 graph_loop 在节点入口前调用。
func withSendSink(ctx context.Context, sink *sendSink) context.Context {
	return context.WithValue(ctx, sendSinkKey{}, sink)
}

// AfterNodeCommit 在节点函数内登记一个通知。节点的写入实际提交后，引擎在锁外同步调用 fn。
// 重试、条件边失败或写入回滚时丢弃相应通知。部分提交时只通知提交成功的成员。
// 通知仅供观测，随本次执行存于内存中。图外、条件边和钩子相位的调用无效。
func AfterNodeCommit(ctx context.Context, fn func()) {
	sink, _ := ctx.Value(sendSinkKey{}).(*sendSink)
	if sink == nil || fn == nil || !inNodeExecution(ctx) || SubCallScope(ctx) != "" {
		return
	}
	sink.mu.Lock()
	sink.afterCommit = append(sink.afterCommit, fn)
	sink.mu.Unlock()
}

// drainSendSink 取出成功尝试的投递与通知。
func drainSendSink(s *sendSink) ([]SendDirective, []func()) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	directives, afterCommit := s.directives, s.afterCommit
	s.directives, s.afterCommit = nil, nil
	return directives, afterCommit
}

func notifyNodeCommits(ctx context.Context, notifications []func()) {
	if len(notifications) == 0 {
		return
	}
	// 等通知结束再进下一超步。观察方异常退出也不改变已提交的结局。
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if value := recover(); value != nil {
				panicerr.Log(ctx, slog.LevelError, "compose_v2: 节点提交后通知异常", panicerr.New(value))
			}
		}()
		panicerr.CallEach(ctx, len(notifications), func(i int) { notifications[i]() })
	}()
	<-done
}
