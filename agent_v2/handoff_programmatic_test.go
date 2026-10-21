package agent_v2_test

import (
	"context"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// progAgent 是测试用的**程序化成员**——没有 LLM、没有工具循环，产出固定文本，
// 并（可选）经 [agent_v2.HandoffTo] 把对话交给候选里的另一个 agent。
//
// 它替掉了此前 fixture 里「产出消息上写一个 annotation」的写法。那条通道已删：
// 它绕过全部三道校验（有没有被授予 / 目标合不合法 / 这一层有没有人接），连
// `WithHandoffTransfer()` 都没配的候选也能靠它转走对话。
//
// 换成这个之后，fixture 走的是**业务真会走的那条路**，顺带给 HandoffTo 提供覆盖。
type progAgent struct {
	name       string
	say        string
	transferTo string // 空 = 不转交
	// onRun 可选：每次 Run 进来时调，返回本次要转交的目标（空 = 本次不转交）。
	// 设了就盖过 transferTo——用于「前几跳自跳、最后一跳收尾」这类逐次不同的 fixture。
	onRun   func(ctx context.Context, run int) string
	runs    int
	lastErr error
}

func newProgAgent(name, say, transferTo string) *progAgent {
	return &progAgent{name: name, say: say, transferTo: transferTo}
}

func (a *progAgent) Name() string        { return a.name }
func (a *progAgent) Description() string { return a.name + " (programmatic)" }

func (a *progAgent) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message,
	opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	a.runs++
	target := a.transferTo
	if a.onRun != nil {
		target = a.onRun(ctx, a.runs)
	}
	if target != "" {
		// 三道校验都在这里面；不过就返 error，且什么都不写。
		a.lastErr = agent_v2.HandoffTo(ctx, opts, a, target)
	}
	// **必须是 ExitEvent**：FinalMessage 只认 Action.Exit=true 的事件，用 OutputEvent
	// 的话 say 在转交边界被整个丢掉、final 恒为空消息——下一个人照签名传 say 再断言
	// 内容会撞上难解释的失败，或者写成否定断言就恒真。
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(a.name, schema.AssistantMessage(a.say))}, nil
}

func (a *progAgent) Resume(ctx context.Context, sid string,
	opts ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return a.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(""))
}
