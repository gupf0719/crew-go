package agent_v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// InterjectionAnnotation 标记一条消息是**运行中插话**：用户在 agent 还在执行时补充的话，经 [Inbox] 送进
// 当前回合（bool，值为 true）。消息本身就是用户原话、user 角色、照常进会话历史；标记是给界面与审计看
// 「这句是中途插进来的」。
const InterjectionAnnotation = "interjection"

// Inbox 是运行中插话的收件箱：用户在 agent 执行过程中补充的指令，调用方在任意 goroutine 里 [Inbox.Push]，
// agent 在安全的位置取走、接进当前回合，模型下一次被调用时就能看到。
//
//	inbox := agent_v2.NewInbox()
//	ctx = agent_v2.WithInboxCtx(ctx, inbox)
//	go func() { for text := range userTyping { _ = inbox.Push(schema.UserMessage(text)) } }()
//	events, err := agent.Run(ctx, sess, input)
//	leftover := inbox.Drain() // 回合结束之后才到的：作为下一次 Run 的输入
//
// # 在哪儿取、谁来取
//
// BasicAgent 每次调模型之前取一次（四层压缩之前——压缩看得见刚插进来的话），模型给出最终答复时再取一次
// （迟到的话不留到下一次 Run：接在答复后面、再调一次模型）。插话接在消息流末尾、工具结果之后，不会插进
// 一批工具调用的中间。不会塞进正在进行的那次模型调用或工具执行里——那一步跑完才轮到它。
//
// 编排里由**当前正在对话的那个 agent** 取：收件箱经 ctx 传给编排的成员（Sequential 的一步、Handoff 当前
// 接手的那个、Plan 的执行步……），先到安全位置的那个取走；并发的成员（Parallel 的分支）谁先到谁取，一条
// 只被取一次。**不传给**编排的内部决策 LLM（路由分类器、评审）、被当成工具调用的子 agent（RunSubAgent /
// AgentAsTool / skill fork——用户是在跟调用它的那个 agent 说话）、[DetachTask] 开的独立任务。
//
// # 取走之后
//
// 取走的插话接进消息流，就是这次执行的进度：配了 checkpointer 时随存档走——那次模型调用随后失败、中断，
// 续跑时原样接回（取走的那一刻就记进节点的私有中断态，存档跨进程也带着）。这次调用（一次 Run 或一次
// Resume）以**续不了**的方式结束——没配 checkpointer 时报错、或者终局失败（[compose_v2.ErrRunAborted]，
// 存档已清）——它这次新取的插话按原顺序放回收件箱最前面：从头再来的那次执行（调用方重跑、编排的成员重试）
// 会再取到它，调用方也能 [Inbox.Drain] 出来。已经记进会话历史的不放回（重来那次从历史上文里看得到它）；
// 先前那次调用取走、随被清掉的存档作废的也不放回。只管这个 agent 自己那次调用：它成功之后、或者它以可续的
// 错结束之后，外层失败、整个从头再来或终局清掉整棵存档时，它取走的插话跟它的进度一起作废。
//
// 没被取走的一直留在收件箱里。回合结束得早于它们到达时（最后一次取在模型的最终答复之后、闸门放行之前；
// 最后一轮不取；工具直接收尾（return-direct）的回合在那之后不再调模型），Run 返回之后用 [Inbox.Drain]
// 取出来，作为下一次 Run 的输入。续跑（Resume）时把同一个收件箱放进 ctx 就接着收。
//
// 接进消息流的插话照常进会话历史（开了 [WithHistory] 时）与会话事件（排在它回应的那条最终答复之前），
// 下一个任务从上文里看得到用户说过它。
//
// 插话会重置工具重复调用的计数链（[WithRepeatToolReminder]）：用户换了方向，之后的重复不算原地打转。
type Inbox struct {
	mu    sync.Mutex
	items []*schema.Message
}

// NewInbox 新建一个空收件箱。
func NewInbox() *Inbox { return &Inbox{} }

// errInterjectionRole 是插话不是 user 角色。
var errInterjectionRole = errors.New("agent_v2: 插话必须是 user 角色的消息")

// Push 收一条插话。并发安全。msg 必须是 user 角色（框架回灌给模型的消息只能是 user，它代表用户说的话），
// 而且要存得进存档（标注等字段里没有 JSON 编不了的值——插话随存档走，存不进去就会让之后每次取它都失败）。
// 收进来的是一份独立的拷贝（打上 [InterjectionAnnotation]），之后改 msg（连同它的内容块、标注）不影响它。
func (b *Inbox) Push(msg *schema.Message) error {
	if msg == nil {
		return errors.New("agent_v2: 插话不能是 nil")
	}
	if msg.Role != schema.RoleUser {
		return errInterjectionRole
	}
	c := cloneCalleeInput(msg)
	if c.Annotations == nil {
		c.Annotations = map[string]any{}
	}
	c.Annotations[InterjectionAnnotation] = true
	if _, err := json.Marshal(c); err != nil {
		return fmt.Errorf("agent_v2: 插话存不进存档（JSON 编码失败）：%w", err)
	}
	b.mu.Lock()
	b.items = append(b.items, c)
	b.mu.Unlock()
	return nil
}

// Drain 取走全部还没被 agent 取走的插话（按到达顺序）并清空。并发安全。
func (b *Inbox) Drain() []*schema.Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.items
	b.items = nil
	return out
}

// Len 返回还没被取走的条数。
func (b *Inbox) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items)
}

// putBack 把取走的插话按原顺序放回收件箱最前面（排在取走之后才到的那些前面）。
func (b *Inbox) putBack(msgs []*schema.Message) {
	if len(msgs) == 0 {
		return
	}
	b.mu.Lock()
	b.items = append(append(make([]*schema.Message, 0, len(msgs)+len(b.items)), msgs...), b.items...)
	b.mu.Unlock()
}

// inboxCtxKey 是收件箱的 ctx key。它是执行状态（实现 compose_v2.ExecutionStateKey，见 detach_task.go）：
// 脱离出来的独立任务不带它；采纳钩子返回的 ctx 时取框架这次的，钩子换不掉它。
type inboxCtxKey struct{}

// WithInboxCtx 把收件箱挂到 ctx 上，交给这次 Run / Resume（语义见 [Inbox]）。b 为 nil 等于不挂。
func WithInboxCtx(ctx context.Context, b *Inbox) context.Context {
	if b == nil {
		return ctx
	}
	return context.WithValue(ctx, inboxCtxKey{}, b)
}

func inboxFrom(ctx context.Context) *Inbox {
	b, _ := ctx.Value(inboxCtxKey{}).(*Inbox)
	return b
}

// withoutInbox 摘掉收件箱：内部决策 LLM、被当成工具调用的子 agent 不取用户的插话（见 [Inbox]）。
func withoutInbox(ctx context.Context) context.Context {
	if inboxFrom(ctx) == nil {
		return ctx
	}
	return context.WithValue(ctx, inboxCtxKey{}, (*Inbox)(nil))
}

// interjectionStepKey 是节点私有态里「这次执行已经取走、还没提交的插话」那一条的键。
const interjectionStepKey = "agent_v2.interjections"

// takeInterjections 返回这次节点执行已经取走的插话：先前取走、还没提交的（节点失败 / 中断过，续跑时读回），
// 加上收件箱里新到的。新到的当场记进节点私有态（节点随后失败或中断时续跑接回）、也记进这次调用的取件账
// （见 interjectionLedger）。没有收件箱、也没有先前取走的时候返回 nil、不写任何状态。
func takeInterjections(ctx context.Context) ([]*schema.Message, error) {
	taken, _ := compose_v2.GetStepResult[[]*schema.Message](ctx, interjectionStepKey)
	b := inboxFrom(ctx)
	if b == nil {
		return taken, nil
	}
	fresh := b.Drain()
	if len(fresh) == 0 {
		return taken, nil
	}
	all := append(append(make([]*schema.Message, 0, len(taken)+len(fresh)), taken...), fresh...)
	if err := compose_v2.SaveStepResult(ctx, interjectionStepKey, all, compose_v2.WithStreams(nil)); err != nil {
		b.putBack(fresh) // 记不下就还回去，别取走了又丢掉
		return nil, err
	}
	if l := ledgerFrom(ctx); l != nil {
		l.record(b, fresh)
	}
	return all, nil
}

// interjectionLedger 是一次 BasicAgent 调用（一次 Run 或一次 Resume）的取件账：这次调用从收件箱取走了哪些
// 插话。调用以续不了的方式结束时（见 settleInterjections）照着它把插话还回去。
//
// 账只记这次调用新取的：续跑时从节点私有态读回的那些是先前那次调用取的，它们的去留跟着存档走。
type interjectionLedger struct {
	mu    sync.Mutex
	inbox *Inbox
	taken []*schema.Message
}

func (l *interjectionLedger) record(b *Inbox, msgs []*schema.Message) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inbox = b // 一次调用里只有一个收件箱：它是执行状态，钩子换不掉
	l.taken = append(l.taken, msgs...)
}

// interjectionLedgerCtxKey 是取件账的 ctx key（执行状态，见 detach_task.go）。每次 BasicAgent 调用开自己的
// 一本，盖掉外层的：编排里的成员各记各的。
type interjectionLedgerCtxKey struct{}

// withInterjectionLedger 给这次调用开一本取件账。
func withInterjectionLedger(ctx context.Context) (context.Context, *interjectionLedger) {
	l := &interjectionLedger{}
	return context.WithValue(ctx, interjectionLedgerCtxKey{}, l), l
}

func ledgerFrom(ctx context.Context) *interjectionLedger {
	l, _ := ctx.Value(interjectionLedgerCtxKey{}).(*interjectionLedger)
	return l
}

// settleInterjections 在一次调用结束时结清取件账：以续不了的方式结束（见 runDiscarded：没配 checkpointer 时
// 报错，这次的进度一点没存下；或者终局失败，存档已清）就把这次取走的插话按原顺序放回收件箱最前面；成功、或者
// 留下了可续的存档（插话在消息流或节点私有态里随存档走），账作废。cper 是这次调用交给引擎的 checkpointer。
//
// 已经记进会话历史的不放回：会话历史是 append-only 的，它们留在那儿，重来那次从历史读上文就看得到——再放回的
// 话上文一份、新取一份，模型看到两遍（审计实测）。没开历史时这次的进度全在消息流里、随这次调用作废，都放回。
func settleInterjections(ctx context.Context, l *interjectionLedger, cper compose_v2.Checkpointer, runErr error) {
	if l == nil || !runDiscarded(cper, runErr) {
		return
	}
	l.mu.Lock()
	b, taken := l.inbox, l.taken
	l.taken = nil
	l.mu.Unlock()
	if b == nil {
		return
	}
	if w := historyWriterFrom(ctx); w != nil {
		kept := taken[:0:0]
		for _, m := range taken {
			if !w.alreadyRecorded(m) {
				kept = append(kept, m)
			}
		}
		taken = kept
	}
	b.putBack(taken)
}
