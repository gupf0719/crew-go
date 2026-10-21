// 真 LLM 冒烟：转交能力按次授予（RunOption）+ 被动转发 / 显式授予的分界。
//
// 跑：go run ./example/orchestration/handoff_carrier_smoke   （需 .env 里的 QWEN_API_KEY）
//
// 为什么要真 LLM 而不是 stub：这两条压的是「模型自己决定调不调 transfer」以及「它的
// 决定能不能跨过 Agent.Run 边界」。stub 能把信号摆到位，摆不出「模型在有工具可用时
// 到底会不会用」。第一版 fixture 就是被真 LLM 打回来的——见 Part 2 的判别设计说明。
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"crew-go/agent_v2"
	"crew-go/example/envloader"
	"crew-go/fallback"
	"crew-go/model"
	"crew-go/model/qwen"
	"crew-go/schema"
	"crew-go/tool"
)

func newModel() model.ChatModel {
	ms := make([]model.ChatModel, 0, len(qwen.TestModelCandidates))
	for _, n := range qwen.TestModelCandidates {
		ms = append(ms, qwen.NewTestClient(n))
	}
	return fallback.New(ms)
}

func main() {
	envloader.Load()
	ctx := context.Background()
	fail := 0

	fmt.Println("───── Part 1: 真 qwen 自主 transfer → 授予把目标交给 HandoffAgent ─────")
	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithSystemPrompt("你是前台。凡涉及账单/退款的问题必须调用 transfer_to_agent 转交给 billing，不要自己回答。"),
		agent_v2.WithModel(newModel()),
		agent_v2.WithHandoffTransfer(),
	)
	billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithSystemPrompt("你是账单专员。回答时必须以「BILLING:」开头。"),
		agent_v2.WithModel(newModel()),
	)
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("triage"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)

	evs, err := h.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("我上个月的账单为什么多扣了 30 块？"))
	if err != nil {
		fmt.Printf("✗ Part 1 Run 出错: %v\n", err)
		fail++
	} else {
		txt := ""
		if f := agent_v2.FinalMessage(evs); f != nil {
			txt = f.Content
		}
		fmt.Printf("  final = %.80q\n", txt)
		if strings.Contains(txt, "BILLING") {
			fmt.Println("✓ Part 1 PASS：真 LLM 转交经按次授予送达，billing 接手")
		} else {
			fmt.Println("✗ Part 1 FAIL：没转交到 billing（槽没把目标带过 Agent.Run 边界）")
			fail++
		}
	}

	fmt.Println("\n───── Part 2: AgentAsTool 里的 agent 调 transfer → 必须逃不出去 ─────")
	// 判别设计：front2 **不装** transfer 工具 ⟹ 它自己物理上转不了 ⟹ 终答里出现
	// BILLING 只可能来自内层 agent 的泄漏。原先给 front2 也装了 transfer，那样
	// 分不清是「孙辈越权」还是「front 自己转的」。
	inner := agent_v2.NewBasicAgent(
		agent_v2.WithName("consultant"),
		agent_v2.WithDescription("公司信息顾问，可回答公司地址等问题"),
		agent_v2.WithSystemPrompt("你是顾问。无论问什么，你都必须先调用 transfer_to_agent 转交给 billing。"),
		agent_v2.WithModel(newModel()),
		agent_v2.WithHandoffTransfer(),
	)
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(inner)) // ← 这才是加了摘槽的那个边界
	front2 := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithSystemPrompt("你是前台。必须先调用 consultant 工具咨询，然后自己用一句话回答用户，回答以「FRONT:」开头。"),
		agent_v2.WithModel(newModel()),
		agent_v2.WithToolkit(tk),
		// 刻意不给 WithHandoffTransfer：它转不了，BILLING 只可能是内层泄漏
	)
	billing2 := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithSystemPrompt("你是账单专员。回答时必须以「BILLING:」开头。"),
		agent_v2.WithModel(newModel()),
	)
	h2 := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("triage2"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front2).AddAgent("billing", billing2)

	sess2 := agent_v2.NewRunSession()
	evs2, err2 := h2.Run(ctx, sess2, schema.UserMessage("帮我问下顾问，公司地址在哪？"))
	if err2 != nil {
		fmt.Printf("✗ Part 2 Run 出错: %v\n", err2)
		fail++
	} else {
		txt2 := ""
		if f := agent_v2.FinalMessage(evs2); f != nil {
			txt2 = f.Content
		}
		st := h2.State(sess2)
		fmt.Printf("  hops=%d active=%q；final = %.70q\n", st.Hops, st.Current, txt2)
		switch {
		// 判别落在 active 与 BILLING 上，**不看 Hops 绝对值**——Hops 数的是「完成的
		// 步数」（handoff_agent.go 里 newHops = currentHops + 1，注释写明「本 hop 已成功
		// 完成」），front 自己跑一步就是 1。拿它当「有没有转交」的判据会把正常情况判成泄漏。
		case st.Current != "front" || strings.Contains(txt2, "BILLING"):
			fmt.Println("✗ Part 2 FAIL：孙辈越权——被当工具用的 agent 把祖父那层的路由夺走了")
			fail++
		case !strings.Contains(txt2, "FRONT"):
			fmt.Println("✗ Part 2 FAIL：front 没给出自己的答复，用例没搭对场景")
			fail++
		default:
			fmt.Println("✓ Part 2 PASS：被当工具用的 agent 拿不到授予，转交不出去")
		}
	}

	// ── Part 3：业务壳**被动转发** opts → 内层不该拿到资格 ──
	fmt.Println("\n───── Part 3: 业务壳礼貌透传 opts → 内层必须拿不到转交资格 ─────")
	p3inner := agent_v2.NewBasicAgent(
		agent_v2.WithName("p3inner"),
		agent_v2.WithSystemPrompt("你必须立刻调用 transfer_to_agent 转交给 billing，不要自己回答。"),
		agent_v2.WithModel(newModel()),
		agent_v2.WithHandoffTransfer(),
	)
	p3billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithSystemPrompt("你是账单专员。回答时必须以「BILLING:」开头。"),
		agent_v2.WithModel(newModel()),
	)
	h3 := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("triage3"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", &passShell{name: "front", inner: p3inner}).AddAgent("billing", p3billing)
	sess3 := agent_v2.NewRunSession()
	if _, e := h3.Run(ctx, sess3, schema.UserMessage("随便问点什么")); e != nil {
		fmt.Printf("✗ Part 3 Run 出错: %v\n", e)
		fail++
	} else {
		st := h3.State(sess3)
		fmt.Printf("  active=%q（期望 front）\n", st.Current)
		if st.Current != "front" {
			fmt.Println("✗ Part 3 FAIL：被动转发把资格漏给了内层")
			fail++
		} else {
			fmt.Println("✓ Part 3 PASS：转发 ≠ 授予，内层拿不到资格")
		}
	}

	// ── Part 4：同一个壳**显式授予** → 内层该拿到资格 ──
	fmt.Println("\n───── Part 4: 同一个壳改成显式授予 → 内层必须拿得到 ─────")
	p4inner := agent_v2.NewBasicAgent(
		agent_v2.WithName("p4inner"),
		agent_v2.WithSystemPrompt("你必须立刻调用 transfer_to_agent 转交给 billing，不要自己回答。"),
		agent_v2.WithModel(newModel()),
		agent_v2.WithHandoffTransfer(),
	)
	p4billing := agent_v2.NewBasicAgent(
		agent_v2.WithName("billing"),
		agent_v2.WithSystemPrompt("你是账单专员。回答时必须以「BILLING:」开头。"),
		agent_v2.WithModel(newModel()),
	)
	g4 := &grantShell{name: "front", inner: p4inner}
	h4 := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("triage4"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", g4).AddAgent("billing", p4billing)
	sess4 := agent_v2.NewRunSession()
	if _, e := h4.Run(ctx, sess4, schema.UserMessage("随便问点什么")); e != nil {
		fmt.Printf("✗ Part 4 Run 出错: %v\n", e)
		fail++
	} else {
		st := h4.State(sess4)
		fmt.Printf("  active=%q（期望 billing）\n", st.Current)
		if st.Current != "billing" {
			fmt.Println("✗ Part 4 FAIL：显式授予没生效，业务壳没有正当出路")
			fail++
		} else {
			fmt.Println("✓ Part 4 PASS：显式授予生效——跟 Part 3 唯一差别就是那一行")
		}
	}

	fmt.Println()
	if fail == 0 {
		fmt.Println("=== 转交换载体 真 LLM 冒烟全 PASS ===")
		return
	}
	fmt.Printf("=== %d 项 FAIL ===\n", fail)
	os.Exit(1)
}

// passShell 把 opts **原样往下转发**——最自然的「礼貌透传」写法。
type passShell struct {
	name  string
	inner agent_v2.Agent
}

func (w *passShell) Run(ctx context.Context, s *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	evs, err := w.inner.Run(ctx, s, in, opts...) // ← 只是转发
	if err != nil {
		return nil, err
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, f)}, nil
}
func (w *passShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *passShell) Name() string        { return w.name }
func (w *passShell) Description() string { return "礼貌透传 opts 的业务壳" }

// grantShell 跟 passShell 逐字相同，**只差 Run 里那一行**——它显式授予。
type grantShell struct {
	name  string
	inner agent_v2.Agent
}

func (w *grantShell) Run(ctx context.Context, s *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	sub := agent_v2.RegrantHandoffTo(opts, w, w.inner) // ← 唯一的差别
	evs, err := w.inner.Run(ctx, s, in, sub...)
	if err != nil {
		return nil, err
	}
	f := agent_v2.FinalMessage(evs)
	if f == nil {
		f = schema.AssistantMessage("")
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.name, f)}, nil
}
func (w *grantShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *grantShell) Name() string        { return w.name }
func (w *grantShell) Description() string { return "显式授予给内层的业务壳" }
