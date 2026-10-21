package agent_v2_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/prompt"
	"crew-go/schema"
)

// capLLM 记录模型每次实收的消息流。
type capLLM struct {
	mu   sync.Mutex
	last []*schema.Message
}

func (m *capLLM) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	m.last = append([]*schema.Message(nil), msgs...)
	m.mu.Unlock()
	return schema.AssistantMessage("最终答复"), nil
}

func (m *capLLM) Stream(ctx context.Context, msgs []*schema.Message, o ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	r, err := m.Generate(ctx, msgs, o...)
	if err != nil {
		return nil, err
	}
	sr, send := schema.NewStreamReader[*schema.Message](1)
	send(r)
	schema.CloseSend(sr)
	return sr, nil
}

func (m *capLLM) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*capLLM)(nil)

// capEndHook 记录 OnAgentEnd 拿到的累积消息流——那就是被四层压缩处置的那条流。
type capEndHook struct {
	hook.NoOpHooks
	mu   sync.Mutex
	msgs []*schema.Message
}

func (h *capEndHook) OnAgentEnd(ctx context.Context, _ *hook.AgentInfo, out *hook.AgentOutput) context.Context {
	h.mu.Lock()
	if out != nil {
		h.msgs = append([]*schema.Message(nil), out.Messages...)
	}
	h.mu.Unlock()
	return ctx
}

// TestPreamble_NotInCompactableStream 钉住「agent 的前言不在被压缩的那条消息流里」。
//
// **bug 历史**：三层压缩曾用「前导位置」当「这是 agent 的 system prompt」的判据——
// 从 messages[0] 起连续的 system 就不动。而装配序是 模板渲染 → sysPrompt → …，
// 模板一旦渲染出 few-shot（`System → Placeholder → User` 正是 `prompt` 包 godoc 教的写法），
// 真 sysPrompt 就排在 few-shot 之后、降级成一个普通可丢组。实测紧急压缩把它直接丢掉
// （5 条进 2 条出，`REAL SYSPROMPT` 不见了），AutoCompact 把它连同 skill 正文摘成一段散文，
// **全程 `err == nil`**；few-shot 自己从来没被任何机制保护过。
//
// **修法不是把判据改准，是让判据消失**：前言走 [agent_v2.BasicAgentPreambleOutput] 的
// System 那条流，压缩层拿到的切片里结构上就没有它。
//
// 所以这条钉两件事，缺一不可：
//
//   - **正向**：模型必须真收到前言（否则「压缩碰不到」可以由「压根没发出去」廉价满足）。
//   - **结构**：累积消息流（`OnAgentEnd` 的 `out.Messages`，即四层压缩处置的那条）里
//     不能有它。这是「压缩碰不到」的充要条件，且**不依赖任何一层记得跳过它**。
//
// ⚠️ 不用「跑一次紧急压缩看它还在不在」当断言：那条路要先凑够可丢组，drop 的算术
// 一变用例就跟着漂，而它钉的其实还是同一个结构性质。
func TestPreamble_NotInCompactableStream(t *testing.T) {
	tmpl := prompt.FromMessages(
		prompt.System("TEMPLATE SYSTEM"),
		prompt.User("fewshot q"),
		prompt.Assistant("fewshot a"),
	)
	llm := &capLLM{}
	endHook := &capEndHook{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("probe"),
		agent_v2.WithModel(llm),
		agent_v2.WithSystemPrompt("REAL SYSPROMPT"),
		agent_v2.WithPromptTemplate(tmpl, nil),
		agent_v2.WithHooks(endHook),
		agent_v2.WithMaxRounds(2),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(),
		schema.UserMessage("用户问题")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	preamble := []string{"TEMPLATE SYSTEM", "REAL SYSPROMPT", "fewshot q", "fewshot a"}

	// 正向：模型真收到了整段前言。
	if len(llm.last) == 0 {
		t.Fatal("模型一次都没被调用，本用例什么也没测")
	}
	for _, w := range preamble {
		if !containsContent(llm.last, w) {
			t.Errorf("模型没收到前言里的 %q——agent 的定义没送出去。实收：%s", w, renderRoles(llm.last))
		}
	}

	// 结构：累积消息流里一条都不能有。
	if len(endHook.msgs) == 0 {
		t.Fatal("OnAgentEnd 没拿到累积消息流，结构断言无处可施")
	}
	for _, w := range preamble {
		if containsContent(endHook.msgs, w) {
			t.Errorf("前言里的 %q 落在了被压缩的消息流里——压缩层能删它/摘要它/把它算进阈值。"+
				"累积流：%s", w, renderRoles(endHook.msgs))
		}
	}
}

func containsContent(msgs []*schema.Message, want string) bool {
	for _, m := range msgs {
		if m != nil && strings.Contains(m.Content, want) {
			return true
		}
	}
	return false
}

func renderRoles(msgs []*schema.Message) string {
	var b strings.Builder
	for i, m := range msgs {
		c := m.Content
		if len(c) > 24 {
			c = c[:24] + "…"
		}
		b.WriteString(fmt.Sprintf("\n  [%d] %s %q", i, m.Role, c))
	}
	return b.String()
}

// rewriteHook 在 OnModelStart 里动前言与对话流——两个都是 [消费] 档。
type rewriteHook struct {
	hook.NoOpHooks
}

func (h *rewriteHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	// ① 就地改前言的内容——业务最自然的写法。
	//
	// ⚠️ **这一条不承重**，如实记下来免得下一个人以为它管用：就地改 `m.Content`（以及
	// 换 `in.Preamble[i]` 的元素）都是在**同一份底层数组/同一批指针**上动手，即使框架
	// 压根不回读 `in.Preamble` 也会「碰巧」生效——实测把框架改成忽略 `modelIn.Preamble`，
	// 这两种写法都**照样绿**。真正要求框架回读这个字段的是「整段换成新切片」，
	// 那条由下面第二个子用例钉。
	for _, m := range in.Preamble {
		if m != nil && strings.Contains(m.Content, "REAL SYSPROMPT") {
			m.Content = "REWRITTEN BY HOOK"
		}
	}
	// ② 往对话流最前面插一条自己的 system——它是业务内容，不是框架前言，
	//    应当留在对话流里、且**排在用户输入之前**（业务放哪儿就是哪儿）。
	in.Messages = append([]*schema.Message{schema.SystemMessage("BUSINESS OWN SYSTEM")}, in.Messages...)
	return ctx, nil
}

// replacePreambleHook 整段换掉前言——「我不要框架那套，用我自己的」。
type replacePreambleHook struct {
	hook.NoOpHooks
}

func (h *replacePreambleHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	in.Preamble = []*schema.Message{schema.SystemMessage("REPLACEMENT PROMPT")}
	return ctx, nil
}

// TestPreamble_HookCanRewriteAndReplace 钉住「业务在 OnModelStart 里改前言、换前言都生效，
// 且不误伤对话流」。
//
// `schema.ModelInput` 把前言与对话流做成**两个并列字段**，正是为了这三件事各自有确定答案。
// 中间试过一版「打 marker 拼进 Messages、hook 返回后按 marker 劈回来」，实测两处静默错，
// 两条都是「N=1 对、N≥2 静默错」：
//
//   - 业务插在前言**之前**的消息，劈完跑到了它**之后**（分区只保各半内部有序，
//     跨半的相对位置整个丢掉）。实测：插 `BUSINESS FIRST` 到最前，出来是
//     `[REAL SYSPROMPT, BUSINESS FIRST, user]`。
//   - 业务造一条新 system 想顶掉前言，**两条都发出去**且旧的还排在前面。实测：
//     `[REAL SYSPROMPT, REPLACEMENT PROMPT, user]`。
//
// 所以本用例的三条断言各自对应一种业务意图，缺一条就有一种意图没有被钉住。
func TestPreamble_HookCanRewriteAndReplace(t *testing.T) {
	t.Run("改前言内容生效，且业务插进对话流的消息不被挪位", func(t *testing.T) {
		llm := &capLLM{}
		ag := agent_v2.NewBasicAgent(
			agent_v2.WithName("probe"),
			agent_v2.WithModel(llm),
			agent_v2.WithSystemPrompt("REAL SYSPROMPT"),
			agent_v2.WithHooks(&rewriteHook{}),
			agent_v2.WithMaxRounds(2),
		)
		if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(),
			schema.UserMessage("用户问题")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if !containsContent(llm.last, "REWRITTEN BY HOOK") {
			t.Errorf("hook 对前言的改写没生效——[消费] 档契约破了：%s", renderRoles(llm.last))
		}
		if containsContent(llm.last, "REAL SYSPROMPT") {
			t.Errorf("被改写掉的原文仍送到了模型（改写被绕开或前言发了两份）：%s", renderRoles(llm.last))
		}
		// 位置断言：业务把它插在对话流最前，它就该排在用户输入**之前**。
		// 断言「在不在」两种情况下都为真，等于没测——要断言的是位置。
		iBiz, iUser := indexOfContent(llm.last, "BUSINESS OWN SYSTEM"), indexOfContent(llm.last, "用户问题")
		if iBiz < 0 || iUser < 0 {
			t.Fatalf("前提没搭对：找不到业务消息或用户输入：%s", renderRoles(llm.last))
		}
		if iBiz > iUser {
			t.Errorf("业务插在对话流最前的消息被挪到了用户输入之后：%s", renderRoles(llm.last))
		}
	})

	t.Run("整段换掉前言，旧的必须消失", func(t *testing.T) {
		llm := &capLLM{}
		ag := agent_v2.NewBasicAgent(
			agent_v2.WithName("probe"),
			agent_v2.WithModel(llm),
			agent_v2.WithSystemPrompt("REAL SYSPROMPT"),
			agent_v2.WithHooks(&replacePreambleHook{}),
			agent_v2.WithMaxRounds(2),
		)
		if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(),
			schema.UserMessage("用户问题")); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if !containsContent(llm.last, "REPLACEMENT PROMPT") {
			t.Errorf("业务换上的前言没送到模型：%s", renderRoles(llm.last))
		}
		if containsContent(llm.last, "REAL SYSPROMPT") {
			t.Errorf("被换掉的框架前言仍送到了模型——「换」退化成了「加」：%s", renderRoles(llm.last))
		}
	})
}

func indexOfContent(msgs []*schema.Message, want string) int {
	for i, m := range msgs {
		if m != nil && strings.Contains(m.Content, want) {
			return i
		}
	}
	return -1
}

// TestPreamble_PublicBuilderHandsItToModelFn 钉住「公开的低阶 builder 路径上，
// preamble 交得到业务自己的 modelFn 手上」。
//
// **bug 历史**：前言最初只经 ctx 递给框架自己的 modelFn，取值口未导出。业务用
// `NewBasicAgentBuilder(myModelFn, nil).WithPreamble(...)`（类型 godoc 明写这是业务入口）
// 返回 `System` 时，modelFn 实收 `[user "hi"]`——**一条前言都没到，且不报错**。
// 而破坏性迁移时把 system 消息放进那个叫 `System` 的字段是最自然的选法，
// 放 `Messages` 才 work，两者没有任何信号区分。
//
// 修法不是「导出一个取值函数让业务记得调」（那还是「helper 写好但没人调」），
// 是把它做成 [agent_v2.BasicAgentModelFn] 的**入参**——「忘了它」在类型上就不成立。
// 拼不拼、拼在哪仍由业务决定（它得拼在自己的压缩之后，那正是它单独成参的理由）。
func TestPreamble_PublicBuilderHandsItToModelFn(t *testing.T) {
	var gotPreamble []*schema.Message
	modelFn := func(_ context.Context, preamble, _ []*schema.Message) (*schema.Message, error) {
		gotPreamble = append([]*schema.Message(nil), preamble...)
		return schema.AssistantMessage("ok"), nil
	}
	preambleFn := func(_ context.Context, in *agent_v2.BasicAgentPreambleInput) (*agent_v2.BasicAgentPreambleOutput, error) {
		return &agent_v2.BasicAgentPreambleOutput{
			System:   []*schema.Message{schema.SystemMessage("BIZ SYSPROMPT")},
			Messages: []*schema.Message{in.UserInput},
		}, nil
	}
	g, err := agent_v2.NewBasicAgentBuilder(modelFn, nil).WithPreamble(preambleFn).Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cm := compose_v2.NewChannelManager()
	if err := compose_v2.Run(context.Background(), g, cm,
		compose_v2.WithInitialWrites(agent_v2.BasicAgentPreambleInputWrites(
			&agent_v2.BasicAgentPreambleInput{UserInput: schema.UserMessage("hi")},
		)...)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !containsContent(gotPreamble, "BIZ SYSPROMPT") {
		t.Fatalf("preamble 没交到业务 modelFn 手上——`System` 那条被静默丢了。实收 %d 条：%s",
			len(gotPreamble), renderRoles(gotPreamble))
	}
}

// inplaceRewriteHook 就地改前言的 Content——框架自己的示例教的就是这个写法。
type inplaceRewriteHook struct{ hook.NoOpHooks }

func (h *inplaceRewriteHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	for _, m := range in.Preamble {
		if m != nil && strings.Contains(m.Content, "FEWSHOT-ANSWER") {
			m.Content = "被 hook 改过了"
		}
	}
	return ctx, nil
}

// nilInjectingHook 往前言里塞一个 nil——业务 `append` 一个零值就会这样。
type nilInjectingHook struct{ hook.NoOpHooks }

func (h *nilInjectingHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	in.Preamble = append(in.Preamble, nil)
	return ctx, nil
}

// TestPreamble_HookRewriteDoesNotLeakIntoBusinessTemplate 钉住「hook 改前言不会写穿到
// 业务自己持有的模板消息上」。
//
// `prompt.Placeholder` 的 godoc 明写它把 `vars[key]` 里的 `[]*schema.Message` **原样插入**
// ——那是业务自己持有、且常在多个 agent 之间复用的指针。前言是 [消费] 档，业务就地改
// `m.Content`（框架示例教的写法）会顺着这条别名写回去。
//
// 承重的量是**另一个从没配过那个 hook 的 agent 收到什么**——只断言「业务那份切片变没变」
// 也对，但前者才说得出后果。
func TestPreamble_HookRewriteDoesNotLeakIntoBusinessTemplate(t *testing.T) {
	shots := []*schema.Message{
		schema.UserMessage("FEWSHOT-Q"),
		schema.AssistantMessage("FEWSHOT-ANSWER"),
	}
	tmpl := prompt.FromMessages(prompt.System("SYS"), prompt.Placeholder("examples", true))
	varsFn := func(context.Context, *schema.Message) map[string]any {
		return map[string]any{"examples": shots}
	}

	// agent A 配了会就地改写的 hook。
	agA := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithModel(&capLLM{}),
		agent_v2.WithPromptTemplate(tmpl, varsFn),
		agent_v2.WithHooks(&inplaceRewriteHook{}), agent_v2.WithMaxRounds(2))
	if _, err := agA.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("q")); err != nil {
		t.Fatalf("agent A Run: %v", err)
	}
	if shots[1].Content != "FEWSHOT-ANSWER" {
		t.Errorf("业务自己持有的 few-shot 被写穿了：%q", shots[1].Content)
	}

	// agent B 从没配过那个 hook——它收到的必须是干净的。
	llmB := &capLLM{}
	agB := agent_v2.NewBasicAgent(
		agent_v2.WithName("b"), agent_v2.WithModel(llmB),
		agent_v2.WithPromptTemplate(tmpl, varsFn), agent_v2.WithMaxRounds(2))
	if _, err := agB.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("q")); err != nil {
		t.Fatalf("agent B Run: %v", err)
	}
	if containsContent(llmB.last, "被 hook 改过了") {
		t.Errorf("没配那个 hook 的 agent 收到了被别人改过的 few-shot：%s", renderRoles(llmB.last))
	}
	if !containsContent(llmB.last, "FEWSHOT-ANSWER") {
		t.Errorf("agent B 该收到原文 few-shot，实收：%s", renderRoles(llmB.last))
	}
}

// TestPreamble_NilFromHookDoesNotReachProvider 钉住「hook 往前言里塞的 nil 不会一路走到
// provider」——真 provider 的消息转换会在那里 deref。
func TestPreamble_NilFromHookDoesNotReachProvider(t *testing.T) {
	llm := &capLLM{}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("probe"), agent_v2.WithModel(llm),
		agent_v2.WithSystemPrompt("REAL SYSPROMPT"),
		agent_v2.WithHooks(&nilInjectingHook{}), agent_v2.WithMaxRounds(2))
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("q")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for i, m := range llm.last {
		if m == nil {
			t.Fatalf("模型入参第 %d 条是 nil——真 provider 在消息转换里会 deref", i)
		}
	}
	// 正向：前言本身还得送到（否则「没有 nil」可以由「什么都没送」廉价满足）。
	if !containsContent(llm.last, "REAL SYSPROMPT") {
		t.Errorf("滤 nil 顺手把前言也滤没了：%s", renderRoles(llm.last))
	}
}
