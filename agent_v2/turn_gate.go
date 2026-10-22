package agent_v2

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TurnGateFunc 是回合结束闸门：BasicAgent 的模型给出最终答复（不再调工具）、要结束这一回合时调用。
//
// 返回非空的 feedback = **不准结束**：feedback 作为一条 user 消息接在答复后面，模型接着干（「你还没更新工单
// 状态」「答复里漏了价格」）；返回空串 = 放行。「继续」和「为什么继续」是同一个返回值——不存在「要求继续却
// 没说为什么」的状态（那样模型会在同样的输入上把同样的答复再说一遍）。
//
// 配了多个闸门时按注册顺序都问，所有非空的反馈都交给模型（各成一条消息）；谁先谁后不影响「继不继续」。
//
// 返回的 feedback 去掉首尾空白之后为空就是放行。
//
// 闸门跑在自己的节点里、每个闸门有自己的相位：里面可以 [compose_v2.Ask] 让人复核这次答复（续跑时只重跑闸门、
// 不重调模型）、可以经 [RunSubAgent] 请一个评审 agent 判断——闸门是内部决策，它请教的对话不进会话历史、
// 不取用户的插话（同编排的评审 agent）。返回的错照常往上传（本次 Run 失败、存档可续），拿不准时放行还是
// 拦下由闸门自己决定——框架不替它吞错。
type TurnGateFunc func(ctx context.Context, in TurnGateInput) (feedback string, err error)

// TurnGateInput 是交给 [TurnGateFunc] 的这次收尾的现场。每道闸门拿到的都是一份独立的拷贝：改它不影响交给
// 调用方的答复、消息流、会话历史，也不影响下一道闸门。
type TurnGateInput struct {
	// Final 是模型这次想以之结束回合的答复（assistant，不带工具调用），就是 Messages 的最后一条。
	Final *schema.Message
	// Messages 是到此刻的整条消息流（含 Final；不含 agent 的前言即系统提示——长期记忆注入、调用方自己塞进
	// 消息流的 system 消息照样在）。
	Messages []*schema.Message
	// Vetoes 是这一回合（这个 agent 的这次调用，连同它的续跑）闸门已经否决过几次（不含这一次）——闸门可以
	// 据此自己退让。
	Vetoes int
}

// TurnGateFeedbackAnnotation 标记一条消息是**回合结束闸门的反馈**（bool，值为 true）：user 角色（框架回灌
// 给模型的消息只能是 user），进会话历史，但不是用户说的话——界面可据此区分展示。
const TurnGateFeedbackAnnotation = "turn_gate_feedback"

// DefaultTurnGateMaxVetoes 是一个任务里闸门最多否决几次（[WithTurnGateMaxVetoes] 不设时）。
const DefaultTurnGateMaxVetoes = 3

// turnGateNodeID 是闸门节点的 ID（配了闸门时才有这个节点）。
const turnGateNodeID compose_v2.NodeID = "turn_gate"

// WithTurnGate 给 BasicAgent 加一道回合结束闸门（语义见 [TurnGateFunc]）。可调用多次，按注册顺序都问。
//
// # 什么时候不问
//
//   - 模型自然收尾（最终答复不带工具调用）时才问；以工具直接收尾（final_output、return-direct 的业务工具、
//     转交 / 路由）的回合不经过闸门——那是工具定义的结束。
//   - 轮次预算用完时不问：这已经是最后一次能调模型的机会，否决了也没有下一轮（[WithMaxRounds] 优先）。
//     否决也消耗轮次：在倒数第二次能调模型时否决，下一次就是摘掉工具的收尾调用，之后 Run 以
//     [compose_v2.MaxIterationsError] 结束（答复在 events 里）——否决上限要设得远小于轮次预算。
//   - 这一回合（这个 agent 的这次调用，连同它的续跑）否决次数到了上限（[WithTurnGateMaxVetoes]，默认
//     [DefaultTurnGateMaxVetoes]）就不再问、直接放行——闸门是成本放大器，判据写错时（永远不满意）要有个
//     尽头。最后一次否决的反馈后面框架另补一条消息「这是最后一次被要求继续」，让模型知道下一次结束不会再被
//     拦；那一条只给这一回合的模型看，不进会话历史（下一回合从 0 数，那句话对它不成立）。
//   - 模型答复之后用户又插了话（见 [Inbox]）：先回去回应插话，这一次不问闸门、不算一次否决。
//
// 闸门的反馈打 [TurnGateFeedbackAnnotation]、作为 user 消息进消息流与会话历史（不进会话事件：事件只记用户
// 说的话与最终答复）。
func WithTurnGate(fn TurnGateFunc) Option {
	return func(c *Config) {
		if isFrameworkNil(fn) {
			panic("WithTurnGate: nil gate (含 Go typed nil func 陷阱)")
		}
		c.TurnGates = append(c.TurnGates, fn)
	}
}

// WithTurnGateMaxVetoes 设一回合（这个 agent 的一次调用，连同它的续跑）里回合结束闸门最多否决几次（n <= 0 用
// 默认值 [DefaultTurnGateMaxVetoes]）。次数随存档走：中断续跑接着数；同一个 agent 再被调用一次（新任务、编排
// 里再轮到它）从 0 数。
func WithTurnGateMaxVetoes(n int) Option {
	return func(c *Config) { c.TurnGateMaxVetoes = n }
}

// turnGateLastChance 是最后一次否决时接在反馈后面的那句话。放在最后一次否决的时候而不是放行之后：放行
// 之后不再调模型，写了它也看不到。
const turnGateLastChance = "这是最后一次被要求继续：下次结束时不会再被拦下，直接交还给用户。"

// newTurnGateFeedback 把一条闸门反馈做成交给模型的 user 消息。
func newTurnGateFeedback(text string) *schema.Message {
	m := schema.UserMessage(text)
	m.Annotations = map[string]any{TurnGateFeedbackAnnotation: true}
	return m
}

// newTurnGateLastChance 做出最后一次否决时接在反馈后面的那条消息。它是单独一条、打 [DerivedInputAnnotation]
// 不进会话历史：它说的是这一回合的否决次数，下一回合从 0 数，记进历史就成了一句假话；拼进反馈那一条的话，
// 反馈本身（闸门说的、该进历史的）就跟着带上它。打 [TurnGateFeedbackAnnotation]：消息流以它结尾时照样回
// model（见 isPendingUserInput），压缩层也把它当成刚接进来、模型还没回应的那一类。
func newTurnGateLastChance() *schema.Message {
	m := schema.UserMessage(turnGateLastChance)
	m.Annotations = map[string]any{TurnGateFeedbackAnnotation: true, DerivedInputAnnotation: true}
	return m
}

// turnGateInputCopy 给一道闸门做一份独立的现场：消息流逐条深拷，Final 是拷贝里的最后一条。
func turnGateInputCopy(msgs []*schema.Message, vetoes int) TurnGateInput {
	c := make([]*schema.Message, len(msgs))
	for i, m := range msgs {
		c[i] = cloneCalleeInput(m)
	}
	return TurnGateInput{Final: c[len(c)-1], Messages: c, Vetoes: vetoes}
}

// runTurnGates 按注册顺序问每一道闸门，返回要交给模型的反馈消息（全都放行时为 nil）。每道闸门跑在自己的
// 相位里：自己的提问与嵌套调用计数、自己那一格私有态——续跑时闸门重跑，同一道闸门的提问算出同一个 ID。
//
// 闸门是内部决策（见 decisionCtx）：它请教的对话不进会话历史、它不取用户的插话。每道闸门拿一份独立的现场拷贝。
func runTurnGates(ctx context.Context, gates []TurnGateFunc, msgs []*schema.Message, vetoes int) ([]*schema.Message, error) {
	var feedback []*schema.Message
	for i, g := range gates {
		gctx := decisionCtx(compose_v2.EnterPhase(ctx, "turn_gate\x00"+strconv.Itoa(i)))
		fb, err := g(gctx, turnGateInputCopy(msgs, vetoes))
		if err != nil {
			return nil, fmt.Errorf("turn gate #%d: %w", i, err)
		}
		if strings.TrimSpace(fb) != "" {
			feedback = append(feedback, newTurnGateFeedback(fb))
		}
	}
	return feedback, nil
}
