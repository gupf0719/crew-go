package compose_v2

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
)

// ── 上行信号：执行里做出、交给上方某个消费方的控制决定 ──
//
// 典型的是「这场对话转交给谁」「这次分类走哪条分支」：决定由很深处的一段执行做出（某个节点里
// 某次工具调用里某个嵌套调用的模型），等它的却是上方隔了好几层的编排。
//
// # 一个收件人上的一类信号是一个单值格子
//
// 同一个 (种类, 收件人) 上只有一个值，语义同单写入者的 [LastValue] channel。几次决定碰到一起时按
// 它们之间有没有先后处理：
//
//   - **先后做出的，后做的为准**：同一段执行里先后两次决定、图里前后两个超步各做一次决定，
//     后做的覆盖先做的——Sequential 的第二步、Loop 的下一轮修正前面的决定是常态，两次决定之间
//     有明确的先后；
//   - **同一超步只能有一个成员做决定**：两个成员都对同一个收件人做了决定 ⟹ 图的设计错误，
//     [SignalConflictError]（它带着 [ErrRunAborted]：不重试、Run 出口清档），**值相同也一样**——跟
//     LastValue 被同一超步两个成员写同一条规则。并发的成员之间没有先后，分歧时谁也没有资格裁决。
//     曾经只在值不同时报冲突、不清档、留给续跑：续跑把整个超步的成员连同副作用重做一遍，决定从
//     记录里拿回时还次次再撞，任务过不去（实测）。想让几个成员各出意见，就让意见作为普通产出，由
//     下游一个节点汇总后再定；
//   - **业务经 [JoinSignals] 自己合并的几路，值一致的算一次**（这是业务显式选的合并语义），不一致
//     同样是设计错误：[SignalConflictError]（它带着 [ErrRunAborted]）。调用方可以接住它、自己裁决；把它
//     （或取出来的结构体）往上交就终局。曾经不带终局标记：业务按约定写的分路收、合并，冲突往上交之后
//     续跑原样再撞（决定从嵌套调用的完成记录拿回），跟引擎那条同一个形态（审计实测）。要按次序取就用
//     [JoinSignalsFirstWins]；
//   - **同一批 tool_call，排在前面的生效**：模型在一条回复里同时发出的几个调用有一个定死的次序（那条
//     已提交的消息里的顺序），也有一个收得到反馈的发起者（模型自己）。后面给出不同值的那个调用的结果
//     换成一条「没有生效」的失败结果，模型下一轮看得到（BasicAgent 的 tools 节点经
//     [JoinSignalsFirstWins] 合并）。曾经也报冲突：这批调用续跑时从工具账原样重放、必然再撞，任务
//     永远过不去（实测）。
//
// 一段执行里没再做决定不会抹掉前面的：后做的只覆盖，不撤销。
//
// # 它跟着写入者的提交走
//
// 信号写进**当前这段执行**——节点执行，或一段 [CaptureSignals] 包住的执行——而不是直接写进消费方：
//
//   - 节点执行成功、且这一超步提交成功 ⟹ 信号并入这张图的已提交状态，随存档落盘；同一超步部分
//     成员失败时，成功成员的先记在一边（随存档走），等整个超步完成时跟续跑成功的成员一起按「同一
//     超步只能有一个成员做决定」合并——合并结果跟没停过时一样，不随停没停过变；
//   - 节点执行失败、被中断、出边被否决 ⟹ 信号随这次执行一起作废，重跑时由重跑重新做出；
//   - 图**成功**跑完 ⟹ 它已提交的全部信号（含续跑时从存档恢复的）交给调用这张图的那段执行，
//     由那段执行按它自己的提交纪律继续往上带；图失败或停下 ⟹ 不交（停下时已提交的留在存档里，
//     续跑跑完时一起交）。
//
// 一段执行没成，它做出的决定就不算数；成了，决定就跟它做成的别的事一起留下。这一条决定了信号
// 跨中断、跨失败、跨否决的行为全部跟写入者的产出一致，不需要另一套生命周期。
//
// ⚠️ **「已提交的留下」包括被跳过、被续上的那几段**：重试的下一次尝试拿回上一次尝试里已完成的
// 嵌套调用（`agent_v2.RunSubAgent` 的完成记录）、续上被调方已提交的步骤时，那些决定随记录 / 存档
// 一起交出——它们属于已经做成的那部分，跟它们的产出一样不重做。作废的只是没完成、没提交的。
//
// ⚠️ 曾经信号直接写进消费方准备的容器（每次尝试一个的收件槽、session KV），写入当场生效。实测
// 四种静默错误，全部 `err == nil`：写入者那一步已提交、续跑不再执行它时信号丢失；失败那次尝试写下
// 的信号照样生效；被否决那次执行写下的信号在重做之后仍在；并发写同一格不报冲突、谁赢随调度。
//
// # 并发合并要显式做
//
// 框架自己的并发点由框架合并（同一超步的成员只能有一个做决定、BasicAgent tools 节点里的同一批
// tool_call 按「排在前面的生效」）。业务在一个节点里自己起 goroutine 并发做决定时，每一路用
// [CaptureSignals] 单独收、汇合时经 [JoinSignals]（或 [JoinSignalsFirstWins]）合并再
// [EmitSignals]——几路直接往同一段执行里交的话，它们会被当成先后做出的，谁后到谁为准、随调度变。
// 这一层框架看不见：并发是业务在自己的代码里起的，怎么合并只有业务知道。
//
// # 跳过执行的地方要把信号一起记下
//
// 被跳过的执行做出的信号不会再做一次。框架有两处会「拿回记下的结果、不再执行」：工具调用的完成账
// （BasicAgent 的 tools 节点）与嵌套调用的完成记录（`agent_v2.RunSubAgent`），它们用
// [CaptureSignals] 把那段执行做出的信号连同产出一起记下（[Signals] 可以随记录序列化），拿回时经
// [EmitSignals] 交出。业务自己跳过执行时（例如节点里直接 [Run] 一张嵌套图、用 [SaveStepResult]
// 记下「已跑完」、续跑时跳过）同样要这样做，否则那张图做出的决定续跑后静默丢失。
//
// # 钩子里做出的信号
//
// 节点级钩子（OnNodeStart / OnNodeEnd）与条件边做出的跟着那个成员走。图级钩子
// OnGraphStart / OnGraphEnd 跑在调用这张图的那段执行里，它们做出的直接交给那段执行：OnGraphStart
// 的排在本图交出的信号之前，OnGraphEnd 的排在之后；图失败时本图的不交，钩子的照交（那段执行成没成
// 由它自己的提交定）。
//
// OnNodeEvent 只作观测：在里面做出的不生效（它拿到的 ctx 上没有收信号处，[EmitSignal] 返回 false）。
// 决定要在发事件的那段代码里直接做——事件经 sink 转发、不带它是从哪一段执行发出来的，在 OnNodeEvent
// 里做出的归不到它该归的那一段（失败的调用里做出的照样交出、暂停续跑之后丢失、同一批里谁赢随调度）。
//
// # 消费方
//
// 消费方用 [CaptureSignals] 包住它等的那次调用，成功之后用 [Signals.Take] 取走发给自己的
// （按种类 + 收件人），剩下的经 [EmitSignals] 交给自己所在的那段执行——它们是发给更上方的。
//
// **收件人得是别的代码经正常入口算不出来的。** [EmitSignals] 接受任何一组 [Signals]，而 Signals 的
// JSON 形态是给持久化用的、手写一份也能反序列化——所以一条决定能不能送到消费方手里，取决于写入者
// 知不知道收件人。用 [SignalRecipient] 铸：它带着这个任务的密钥（一个任务一把、随存档走，续跑时算出
// 同一个值），只有消费方和它亲手交给的写入者知道。曾经收件人是 `"handoff:" + 候选所在的 ns`，业务工具用
// `GetGraphNS` 就拼得出来，手写一份 JSON 再 EmitSignals 就转走了对话——没被授予转交能力的终点
// 候选、被当成工具用的孙辈、永不拿转交能力的分类器，三处边界全部绕过，`err == nil`（实测）。
//
// 防护范围到「正常入口」为止：密钥就在存档里（[Checkpoint] 的 RecipientKey），故意经 checkpointer
// 读出来自己算的代码算得出收件人——那跟直接改存档是一回事。同一进程里的业务代码不是信任边界，
// 这里挡的是「顺手一拼就转走了对话」，不是蓄意绕过。

// SignalKind 是一类上行信号的身份。只能经 [NewSignalKind] 铸造，同名只能铸一次。
//
// [EmitSignal] 与 [Signals.Take] 都要它：放在不导出的变量里，别的包就没法经这两个入口发、取这类
// 信号。它不是防伪造的那道防线——[Signals] 的 JSON 形态里种类只是一个名字；防伪造靠收件人
// （见 [SignalRecipient]）。
type SignalKind struct{ name string }

var (
	signalKindsMu sync.Mutex
	// signalKinds 记每个名字由谁铸的（调用位置），重复铸造时报给调用方。
	signalKinds = map[string]string{}
)

// NewSignalKind 铸造一类上行信号。名字用于持久化（存档与记录里按名字存），在进程内必须唯一：
// 重复铸造会 panic——两个包用同一个名字，彼此的信号就会被对方当成自己的。在包级变量里铸。
func NewSignalKind(name string) *SignalKind {
	if name == "" {
		panic("compose_v2: NewSignalKind 的名字不能为空")
	}
	where := "未知位置"
	if _, file, line, ok := runtime.Caller(1); ok {
		where = fmt.Sprintf("%s:%d", file, line)
	}
	signalKindsMu.Lock()
	defer signalKindsMu.Unlock()
	if first, dup := signalKinds[name]; dup {
		panic(fmt.Sprintf("compose_v2: 信号种类 %q 已经在 %s 铸过（这次在 %s）——同名只能铸一次", name, first, where))
	}
	signalKinds[name] = where
	return &SignalKind{name: name}
}

// Name 返回铸造时的名字。
func (k *SignalKind) Name() string {
	if k == nil {
		return ""
	}
	return k.name
}

// Signal 是一条上行信号的内容。
type Signal struct {
	// Dest 是等它的那个消费方（收件人），用 [SignalRecipient] 铸、由消费方交给写入者。
	Dest string
	// Value 是这个决定本身（转交目标、路由分支……）。
	Value string
	// Origin 是写入者的位置（所在的 ns、tool_call 等），只用于报冲突时定位，不参与匹配。
	Origin string
}

// SignalRecord 是一条信号的持久形态——[Checkpoint] 的 Signals / CompletedSignals 字段里的元素。
type SignalRecord struct {
	// Kind 是 [SignalKind] 的名字。
	Kind   string `json:"kind"`
	Dest   string `json:"dest,omitempty"`
	Value  string `json:"value,omitempty"`
	Origin string `json:"origin,omitempty"`
}

func (r SignalRecord) sameSlot(o SignalRecord) bool { return r.Kind == o.Kind && r.Dest == o.Dest }

// putSignal 按「后做的为准」把 rec 放进 items：同一个 (种类, 收件人) 已有就原地替换，否则追加。
func putSignal(items []SignalRecord, rec SignalRecord) []SignalRecord {
	for i := range items {
		if items[i].sameSlot(rec) {
			items[i] = rec
			return items
		}
	}
	return append(items, rec)
}

// Signals 是一段执行做出的一组信号：每个 (种类, 收件人) 至多一条（见包内「单值格子」一节）。
//
// 由 [CaptureSignals] 返回；可以随记录序列化（JSON），拿回时经 [EmitSignals] 交出。零值是空组。
type Signals struct{ items []SignalRecord }

// Len 返回信号条数。
func (s Signals) Len() int { return len(s.items) }

// IsZero 报这组信号是不是空的（给 `json:",omitzero"` 用）。
func (s Signals) IsZero() bool { return len(s.items) == 0 }

// MarshalJSON 实现 [json.Marshaler]。
func (s Signals) MarshalJSON() ([]byte, error) {
	if len(s.items) == 0 {
		return []byte("null"), nil
	}
	return json.Marshal(s.items)
}

// UnmarshalJSON 实现 [json.Unmarshaler]。同一个 (种类, 收件人) 出现多次时按「后者为准」归并。
func (s *Signals) UnmarshalJSON(b []byte) error {
	var items []SignalRecord
	if err := json.Unmarshal(b, &items); err != nil {
		return err
	}
	var out []SignalRecord
	for _, it := range items {
		out = putSignal(out, it)
	}
	s.items = out
	return nil
}

// Take 取出种类为 k、收件人为 dest 的那条信号，并返回剩下的。没有时第二个返回值为 false。
func (s Signals) Take(k *SignalKind, dest string) (Signal, bool, Signals) {
	if k == nil || len(s.items) == 0 {
		return Signal{}, false, s
	}
	for i, it := range s.items {
		if it.Kind == k.name && it.Dest == dest {
			rest := make([]SignalRecord, 0, len(s.items)-1)
			rest = append(rest, s.items[:i]...)
			rest = append(rest, s.items[i+1:]...)
			return Signal{Dest: it.Dest, Value: it.Value, Origin: it.Origin}, true, Signals{items: rest}
		}
	}
	return Signal{}, false, s
}

// SignalConflictError 是并发的几段执行都对同一个收件人做了决定（见包内「单值格子」一节）。它们之间
// 没有先后，框架不替它们挑一个。**它自己就是终局失败**：[SignalConflictError.Unwrap] 返回 [ErrRunAborted]——不重试、Run
// 出口清掉存档子树，同一超步里别的成员提的问题不记（这个 Run 已经终局，答了也用不上）。标记挂在类型
// 本身上，所以业务用 errors.As 取出结构体再往上交、或者自己构造一个，同样终局。两个来源只差在「值相同
// 算不算」：
//
//   - **同一超步的两个成员**（引擎合并时）：两个成员都做了决定就报，**值相同也报**——一个收件人在
//     同一超步里只能有一个决定方，跟单写入者 [LastValue] 被两个成员写同一条规则。出路是只让一个成员
//     做决定：其余成员把意见作为普通产出，由下游一个节点汇总后再定；
//   - **业务经 [JoinSignals] 合并的几路**：值一致的算一次（业务显式选的合并语义），不一致才报。调用方
//     可以用 errors.As 接住它、自己裁决，不往上交就不终局。
//
// 为什么终局：分歧没有谁有资格裁决，留档续跑只会让同一个矛盾再来一次——决定从记录里（`agent_v2.RunSubAgent`
// 的完成记录）拿回时原样再撞、任务过不去，没有记录时成员连同副作用整段重做（均实测）。曾经不清档、
// 留给续跑，就是这两种结局。曾经标记只挂在外面那层包装上：取出来的结构体变回可重试，在 RoutingAgent
// 里被降级成兜底、`err == nil`（审计实测）。
//
// 同一批 tool_call 不报这个错：排在前面的生效（见 [JoinSignalsFirstWins]）。
type SignalConflictError struct {
	// Kind 是信号种类的名字。
	Kind string
	// First / Second 是撞在一起的两个决定（Dest 相同；同一超步的两个成员之间 Value 也可能相同）。
	First, Second Signal
	// Where 说明是哪种并发、哪两路（「同一超步的成员 a 与 b」「并发的 第 0 路 与 第 1 路」……），给人看。
	Where string
	// hint 是按来源给的出路（引擎合并 / [JoinSignals]），拼在正文里。
	hint string
}

// Unwrap 返回 [ErrRunAborted]：冲突是设计错误，终局（见类型说明）。
func (e *SignalConflictError) Unwrap() error { return ErrRunAborted }

func (e *SignalConflictError) Error() string {
	var b strings.Builder
	b.WriteString("上行信号冲突：")
	if e.Where != "" {
		b.WriteString(e.Where)
	} else {
		b.WriteString("并发的两段执行")
	}
	fmt.Fprintf(&b, " 都对同一个收件人做了「%s」决定：%q", e.Kind, e.First.Value)
	if e.First.Origin != "" {
		fmt.Fprintf(&b, "（%s）", e.First.Origin)
	}
	fmt.Fprintf(&b, " 与 %q", e.Second.Value)
	if e.Second.Origin != "" {
		fmt.Fprintf(&b, "（%s）", e.Second.Origin)
	}
	b.WriteString("——它们之间没有先后，框架不替它们挑一个")
	if e.hint != "" {
		b.WriteString("。")
		b.WriteString(e.hint)
	}
	b.WriteString(": ")
	b.WriteString(ErrRunAborted.Error())
	return b.String()
}

// signalBranch 是并发合并的一路：label 给报冲突时定位用。
type signalBranch struct {
	label string
	items []SignalRecord
}

// joinSignalBranches 合并并发的几路，结果的顺序是各收件人第一次出现的顺序。冲突时返回
// [*SignalConflictError]（它带着 [ErrRunAborted]），正文按来源给出路。
//
// singleWriter 为 true（引擎合并同一超步的成员）：同一个 (种类, 收件人) 只能有一路做决定，第二路也做了
// 就是冲突，值相同也算。为 false（[JoinSignals]）：值一致的算一次（留先出现的那条），不一致才报冲突。
func joinSignalBranches(where string, singleWriter bool, branches []signalBranch) ([]SignalRecord, error) {
	var out []SignalRecord
	from := make([]int, 0, 4) // out[i] 来自哪一路
	for bi, br := range branches {
		for _, rec := range br.items {
			idx := -1
			for i := range out {
				if out[i].sameSlot(rec) {
					idx = i
					break
				}
			}
			if idx < 0 {
				out = append(out, rec)
				from = append(from, bi)
				continue
			}
			if !singleWriter && out[idx].Value == rec.Value {
				continue
			}
			w := where
			if a, b := branches[from[idx]].label, br.label; a != "" || b != "" {
				w = fmt.Sprintf("%s %s 与 %s ", where, a, b)
			}
			hint := "并发的几路对同一个收件人给出了不同的决定：只让一路做决定，" +
				"或在合并前自己裁决（按次序取用 JoinSignalsFirstWins）"
			if singleWriter {
				hint = "一个收件人在同一超步里只能有一个成员做决定（值相同也一样）：" +
					"只让一个成员做决定，其余成员把意见作为普通产出、由下游一个节点汇总后再定"
			}
			return nil, &SignalConflictError{
				Kind:   rec.Kind,
				First:  Signal{Dest: out[idx].Dest, Value: out[idx].Value, Origin: out[idx].Origin},
				Second: Signal{Dest: rec.Dest, Value: rec.Value, Origin: rec.Origin},
				Where:  strings.TrimSpace(w),
				hint:   hint,
			}
		}
	}
	return out, nil
}

// JoinSignals 合并**并发**的几段执行各自做出的信号：同一个收件人上各路给的值必须一致，不一致返
// [*SignalConflictError]（它带着 [ErrRunAborted]）——往上交就终局（不重试、清档）；要自己裁决就用
// errors.As 接住，或在合并前改用 [JoinSignalsFirstWins] 按次序取。
//
// 业务在节点里自己起 goroutine 并发做决定时用：每一路用 [CaptureSignals] 单独收，汇合时
// JoinSignals 再 [EmitSignals]。先后做出的不该用它——直接依次 EmitSignals，后交的为准。
//
// 「值一致的算一次」是这里的合并语义，由调用 JoinSignals 的业务显式选择；引擎合并同一超步的成员时
// 没有人选过，按单写入者处理（只能一个成员做决定，见 [SignalConflictError]）。
func JoinSignals(branches ...Signals) (Signals, error) {
	bs := make([]signalBranch, 0, len(branches))
	for i, s := range branches {
		bs = append(bs, signalBranch{label: fmt.Sprintf("第 %d 路", i), items: s.items})
	}
	out, err := joinSignalBranches("并发的", false, bs)
	if err != nil {
		return Signals{}, err
	}
	return Signals{items: out}, nil
}

// SignalSuperseded 是按顺序合并时被排在前面那一路盖掉的一条决定（见 [JoinSignalsFirstWins]）。
type SignalSuperseded struct {
	// Branch 是被盖掉的那一路的下标。
	Branch int
	// Kind 是信号种类的名字。
	Kind string
	// Kept 是生效的那条（排在前面那一路做出的），Dropped 是被盖掉的这一条。
	Kept, Dropped Signal
}

// JoinSignalsFirstWins 合并几路**有先后**的执行做出的信号：同一个 (种类, 收件人) 上排在最前面那一路
// 做出的生效；后面哪一路有任何一条跟已生效的不一致，**这一路整个不算**（它别的、没冲突的决定也不算），
// 冲突的那几条逐条报给调用方。值一致的算一次。
//
// 按路原子是因为调用方把被盖掉的那一路当成「没成」处置（BasicAgent 的 tools 节点把那个调用的结果换成
// 失败结果）——结果说失败、它的另一个决定却照样生效，两边对同一次调用的结论相反。曾经按条丢弃就是这样
// （审计实测）。
//
// 用在先后由数据定死、续跑时不会翻转的地方——例如一条模型回复里的一批 tool_call：顺序来自已提交的
// 那条消息。跟 [JoinSignals] 报冲突的理由不矛盾：那里几路之间没有一个不随调度变的次序可依，挑哪个都
// 随调度、停过之后还会翻转。也跟「先后做出的后者为准」不矛盾：一批 tool_call 里后面那个调用不是看到
// 前面那个的结果之后做的修正，是同一条回复里的自相矛盾。被盖掉的那几条交给调用方处置（BasicAgent 的
// tools 节点把那几个调用的结果换成一条「没有生效」的失败结果，模型下一轮看得到）。
func JoinSignalsFirstWins(branches ...Signals) (Signals, []SignalSuperseded) {
	var out []SignalRecord
	var dropped []SignalSuperseded
	for bi, br := range branches {
		conflicted := false
		for _, rec := range br.items {
			for i := range out {
				if out[i].sameSlot(rec) && out[i].Value != rec.Value {
					conflicted = true
					dropped = append(dropped, SignalSuperseded{
						Branch:  bi,
						Kind:    rec.Kind,
						Kept:    Signal{Dest: out[i].Dest, Value: out[i].Value, Origin: out[i].Origin},
						Dropped: Signal{Dest: rec.Dest, Value: rec.Value, Origin: rec.Origin},
					})
					break
				}
			}
		}
		if conflicted {
			continue // 这一路整个不算
		}
		for _, rec := range br.items {
			seen := false
			for i := range out {
				if out[i].sameSlot(rec) {
					seen = true // 值一致（不一致的上面已经整路跳过）
					break
				}
			}
			if !seen {
				out = append(out, rec)
			}
		}
	}
	return Signals{items: out}, dropped
}

// signalScope 收一段执行做出的信号：节点执行一个（graph_loop 建、成功时取出并随提交并入 cm），
// [CaptureSignals] 每次调用一个。同一个 (种类, 收件人) 后交的为准。
type signalScope struct {
	mu    sync.Mutex
	items []SignalRecord
}

type signalScopeKey struct{}

func withSignalScope(ctx context.Context, s *signalScope) context.Context {
	return context.WithValue(ctx, signalScopeKey{}, s)
}

func getSignalScope(ctx context.Context) *signalScope {
	s, _ := ctx.Value(signalScopeKey{}).(*signalScope)
	return s
}

func (s *signalScope) add(items ...SignalRecord) {
	if s == nil || len(items) == 0 {
		return
	}
	s.mu.Lock()
	for _, it := range items {
		s.items = putSignal(s.items, it)
	}
	s.mu.Unlock()
}

// drain 取出并清空。
func (s *signalScope) drain() []SignalRecord {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.items
	s.items = nil
	return out
}

// EmitSignal 把一条信号交给当前这段执行（见包内「上行信号」一节）。同一段执行里同一个收件人上
// 后交的覆盖先交的。
//
// 返回 false 表示没交出去：k 为 nil，或 ctx 里没有任何执行在收（不在节点执行里，也不在
// [CaptureSignals] 里）——那时上方不可能有消费方在等它。
func EmitSignal(ctx context.Context, k *SignalKind, s Signal) bool {
	if k == nil {
		return false
	}
	sc := getSignalScope(ctx)
	if sc == nil {
		return false
	}
	sc.add(SignalRecord{Kind: k.name, Dest: s.Dest, Value: s.Value, Origin: s.Origin})
	return true
}

// EmitSignals 把一组信号原样交给当前这段执行：消费方转交不归自己的那些，拿回记下的结果时重放
// 当初那段执行做出的那些。它们算作**此刻**做出的（同一个收件人上覆盖先交的）。空组恒返回 true；
// 非空而 ctx 里没有执行在收时返回 false。
func EmitSignals(ctx context.Context, s Signals) bool {
	if len(s.items) == 0 {
		return true
	}
	sc := getSignalScope(ctx)
	if sc == nil {
		return false
	}
	sc.add(s.items...)
	return true
}

// CaptureSignals 在一段新的执行里跑 fn：fn 成功时返回这段执行做出的全部信号，失败时它们一并作废
// （返回空组和 fn 的错）。
//
// 它**不会**自动把信号交给外层——调用方决定怎么处置：消费方取走发给自己的、把剩下的
// [EmitSignals] 出去；记录完成结果的调用方把它们记进记录、再原样 [EmitSignals] 出去；并发的几路
// 各收各的、经 [JoinSignals] 合并。
//
// fn 里嵌套跑的图在成功跑完时把它已提交的信号交进来；fn 被中断时，图里已提交的那些留在那张图的
// 存档里，续跑跑完时一起交上来。fn 里经 EventSink 发事件、在 OnNodeEvent 里做出的不生效（OnNodeEvent
// 只作观测）。
func CaptureSignals(ctx context.Context, fn func(ctx context.Context) error) (Signals, error) {
	if fn == nil {
		return Signals{}, errors.New("compose_v2: CaptureSignals: nil fn")
	}
	sc := &signalScope{}
	if err := fn(withSignalScope(ctx, sc)); err != nil {
		return Signals{}, err
	}
	return Signals{items: sc.drain()}, nil
}

// SignalRecipient 铸一个收件人：消费方在**自己的节点执行里**调，把结果交给它要等的写入者。
//
// 它是 (这个任务的收件人密钥, 本图的 ns, 当前节点执行的位置, label) 的 HMAC。密钥一个任务一把（见
// 下面「收件人密钥」一节）、随存档走，所以：
//
//   - 续跑时同一个位置、同一个 label 算出同一个值——上一个 Run 写下、存在写入者存档里的决定续跑
//     后仍认得出是发给它的；
//   - 图外的代码经正常入口算不出来：密钥在 ctx 上只以遮罩形态存在（打印 ctx 读不到），也不随信号
//     走。手写一份 [Signals] 的 JSON 再 [EmitSignals] 冒充不了一个决定。故意从存档里读出密钥自己算
//     的不在防护范围内（见本文件开头「消费方」一节）。
//
// 节点执行的位置含访问序号：同一个节点的下一次执行（比如下一跳）是另一个收件人。位置也含**分域**：
// 同一次节点执行里，交出去的那一段（[HandOver]：被调方在自己图外跑的代码、它的 agent 级钩子）、
// 钩子相位、某个 tool_call 各是另一个分域，铸出来的不同——消费方在节点体里铸，交给写入者的那段代码
// 自己算不出同一个值。没有这一维时，转交候选自己的 OnAgentStart 钩子把 ns 推回上一层（[WithGraphNS]
// 是导出的）、拿同一个 label 就算出了收件人，没被授予转交能力的终点候选照样转走了对话。
//
// 不在节点执行里（没有可挂的图）时返回 ""——那里不该有消费方。
func SignalRecipient(ctx context.Context, label string) string {
	acc := nodeStateAccessorFrom(ctx)
	if acc == nil || acc.cm == nil {
		return ""
	}
	key := acc.cm.recipientKeyOrMint()
	mac := hmac.New(sha256.New, key)
	for _, part := range []string{GetGraphNS(ctx), acc.key, acc.scope, label} {
		mac.Write([]byte(part))
		mac.Write([]byte{0})
	}
	return hex.EncodeToString(mac.Sum(nil)[:16])
}

// ── 收件人密钥：一个任务一把 ──
//
// 消费方铸的收件人交给了嵌套图里的写入者，写入者的存档先落盘。密钥要是每张图各铸各的、随本图下一次
// 存档才落盘，本图的档没写成（进程崩溃、出口那次写档失败）时续跑重铸、收件人对不上，子层已落盘的
// 决定静默丢失（实测：转交没发生、路由走兜底，三个耐久档都中，`err == nil`）。
//
// 所以密钥跟任务代号一样**一个任务一把**：顶层新开时铸，经 ctx 交给全部嵌套图沿用，每张图的存档都带
// 一份。中间哪一层的档丢了，续跑时它从上层继承回同一把；顶层自己的档丢了、只能新开时，任务代号也换了，
// 子层那些旧存档过不了续跑闸门（[Checkpoint.BelongsToTask]）、不会被载入，决定由重跑重新做出。两种
// 情形都不需要为了密钥多写一次档。

// taskRecipientKeyCtxKey 携带这个任务的收件人密钥（*sealedRecipientKey：打印 ctx 时只露遮罩）。
type taskRecipientKeyCtxKey struct{}

// sealedRecipientKey 是放进 ctx 的密钥。标准库 context 的 String() 会把链上每一层的值原样打印，裸的
// []byte 放进去，业务代码 `fmt.Sprint(ctx)` 就读到了密钥、能自己算出收件人。
type sealedRecipientKey struct{ k []byte }

func (*sealedRecipientKey) String() string   { return "<recipient key>" }
func (*sealedRecipientKey) GoString() string { return "<recipient key>" }

func withTaskRecipientKey(ctx context.Context, key []byte) context.Context {
	return context.WithValue(ctx, taskRecipientKeyCtxKey{}, &sealedRecipientKey{k: key})
}

func taskRecipientKeyFrom(ctx context.Context) []byte {
	if s, ok := ctx.Value(taskRecipientKeyCtxKey{}).(*sealedRecipientKey); ok && s != nil {
		return s.k
	}
	return nil
}

// adoptRecipientKey 定下本图用的收件人密钥并返回它：上层传下来了就沿用（嵌套图）；没有时用存档里恢复
// 的（顶层续跑）；都没有就新铸（顶层新开）。Run 在第一个超步之前调一次。
func (cm *ChannelManager) adoptRecipientKey(inherited []byte) []byte {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if len(inherited) > 0 {
		cm.recipientKey = append([]byte(nil), inherited...)
	} else {
		cm.mintRecipientKeyLocked()
	}
	return cm.recipientKey
}

// recipientKeyOrMint 返回本图的收件人密钥。Run 在第一个超步之前就定好了（adoptRecipientKey），这里铸
// 只是兜底——不经 Run、直接拿 cm 用的调用方。
func (cm *ChannelManager) recipientKeyOrMint() []byte {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.mintRecipientKeyLocked()
	return cm.recipientKey
}

// mintRecipientKeyLocked 在还没有密钥时铸一把。调用方须已持写锁。
func (cm *ChannelManager) mintRecipientKeyLocked() {
	if len(cm.recipientKey) != 0 {
		return
	}
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		// crypto/rand 读不出来是平台级故障，没有安全的退路
		panic(fmt.Sprintf("compose_v2: 铸收件人密钥失败: %v", err))
	}
	cm.recipientKey = k
}

// commitSignalsLocked 把一批**先后**做出的信号并入本图已提交的状态（同一个收件人上后者为准）。
// **调用方须已持写锁**——它跟写入提交同属一个临界区，见 [ChannelManager.commitSuperstepAdvance]。部分
// 提交不并入：成功成员的先记在一边，等整超步完成时合并（见 [ChannelManager.commitSuperstepPartial]）。
func (cm *ChannelManager) commitSignalsLocked(items []SignalRecord) {
	for _, it := range items {
		cm.signals = putSignal(cm.signals, it)
	}
}

// commitSignals 是加锁版本（入口条件边做出的信号在第一超步之前提交）。
func (cm *ChannelManager) commitSignals(items []SignalRecord) {
	if cm == nil || len(items) == 0 {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.commitSignalsLocked(items)
}

// signalsSnapshot 返回本图已提交信号的拷贝。
func (cm *ChannelManager) signalsSnapshot() []SignalRecord {
	if cm == nil {
		return nil
	}
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if len(cm.signals) == 0 {
		return nil
	}
	return append([]SignalRecord(nil), cm.signals...)
}

// completedSignalsSnapshot 返回部分失败超步里成功成员先记在一边的信号（见
// [ChannelManager.commitSuperstepPartial]）。
func (cm *ChannelManager) completedSignalsSnapshot() []SignalRecord {
	if cm == nil {
		return nil
	}
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if len(cm.completedSignals) == 0 {
		return nil
	}
	return append([]SignalRecord(nil), cm.completedSignals...)
}

// bubbleSignalsTo 把本图已提交的全部信号交给调用这张图的那段执行——图**成功出口**调用。caller 是
// Run 入口、ctx 被任何钩子替换之前取下的那个（见 Run 顶部那道专用 defer）；顶层没有调用方在收时
// 为 nil，no-op。
func bubbleSignalsTo(caller *signalScope, cm *ChannelManager) {
	if caller == nil {
		return
	}
	caller.add(cm.signalsSnapshot()...)
}

func init() {
	// 编排把一段执行做出的信号暂存进 channel、等条件成立才交出时（例如反思编排里每一轮的决定等评估
	// 通过），它要随存档走。
	RegisterValueType[Signals]("compose_v2.Signals")
}

// resetSignalsForFreshRun 清掉 cm 上的上行信号与收件人密钥——新开时调（续跑由 Restore 整体换成
// 存档里的）。复用 cm 跑新任务时不清的话，上一次运行已提交的信号在这次成功时再交一次（实测：第二次
// WithFreshRun 交出 "run1,run2"）。
func (cm *ChannelManager) resetSignalsForFreshRun() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.signals, cm.completedSignals, cm.recipientKey = nil, nil, nil
}
