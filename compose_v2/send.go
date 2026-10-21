package compose_v2

import (
	"context"
	"sync"
)

// SendDirective 节点运行时动态产生的 fan-out 指令（运行时动态 map-reduce）。
//
// 机制：节点 NodeFunc 内**imperative**调 SendFrom(ctx, ...) 写 ctx-injected
// sink（更直观，不强制走 conditional edge 回调），节点 return 后框架 drain sink
// 合并到下一超步 frontier；Payload 是 []ChannelWrite 走 channel reducer 流（与
// compose_v2 channel-based 主线一致）。
//
// ⚠️ **Payload 写进的是共享 channel 状态，不是「这条指令的私有入参」**。N 条指令的
// payload 在下一超步开始前**一次性归并**进同一批 channel——下一超步的**所有**节点都看到
// 同一份。所以同一个 target 被扇出 N 次时：LastValue 只剩最后一条（引擎当场拒，见
// [LastValue]）；Append 全部累积，但**每个实例拿不到自己是第几个**——[InstanceFanoutIndex]
// 返回的是进程级单调计数器，不是本次扇出里的 0..N-1，也没有别的 API 给序号。
//
// **结论：「同 target 扇出 N 个实例、每个实例不同输入」今天没有正确写法。** 这段 godoc
// 一度教过两版都不成立的写法（先是每条 `Write(workerInputCh, t)` + 「如 LastValue 单实例」，
// 实测三个 worker 全看到第三个 task 而 err == nil；后是 Append 装全部 + 让实例「取自己
// 那份」，可实例根本拿不到序号），都是没验证就写下的。把 payload 做成目标实例的私有入参 /
// 暴露扇出序号，两条路的取舍在 doc/code/todo.md backlog，未定。
//
// 今天能写的两种形态：
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
//   - **同 target 扇出 N 次**：只适合 payload **为空**——共享输入由发出方在自己的 writes 里
//     写好，N 个相同 worker 读同一份。各实例靠 [GetCurrentInstanceID] 区分身份，那是不透明
//     token，只能当 key、不能当序号。
//     ⚠️ **别指望「N 条 payload 写同一个值」**：检测不比值，单写入者 channel 上照拒（实测
//     两条同值 payload → `ErrRunAborted`、worker 零执行）；换成 Append/Sum 则累积 N 份，
//     也不是「同一份」。
type SendDirective struct {
	// Target 目标节点 ID——必须已在 graph 注册（caller 保证）
	Target NodeID

	// Payload 是一批 ChannelWrite，framework 在下一超步入口归并进**共享** channel 状态——
	// 下一超步所有节点（不只这条指令的 target）都看到。**它不是 target 的私有入参**，
	// 见类型 godoc 的 ⚠️ 段：同 target 扇出 N 次时 N 条 payload 塌成一份。
	//
	// 空 Payload 等价于"只调度不传参"。
	Payload []ChannelWrite

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

// sendSink 持节点执行期间收的 SendDirective——节点 return 后框架 drain。
type sendSink struct {
	mu         sync.Mutex
	directives []SendDirective
}

// SendFrom 在节点函数（NodeFunc）内调用——动态向下一超步投递 fan-out 指令。
//
// **必须从 NodeFunc 内调用**——否则 ctx 没有 sendSink，调用静默 noop（不报错——
// 业务复用 NodeFunc 在框架外测试时不应崩）。
//
// 节点 return 后框架按声明顺序 append 到下一超步 frontier。重复 Target 不去重
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
	sink.directives = append(sink.directives, directives...)
	sink.mu.Unlock()
}

// withSendSink 注入 sendSink 到 ctx——由 graph_loop 在节点入口前调用。
func withSendSink(ctx context.Context, sink *sendSink) context.Context {
	return context.WithValue(ctx, sendSinkKey{}, sink)
}

// drainSendSink 取出并清空 sink 的 directives——节点 return 后由 graph_loop 调用。
func drainSendSink(s *sendSink) []SendDirective {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.directives) == 0 {
		return nil
	}
	out := s.directives
	s.directives = nil
	return out
}
