package agent_v2_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/skill"
	"crew-go/tool"
)

// 框架以 user 角色追加进对话流的提示——工具重复调用提醒、inline skill 正文、后台任务通知——排在用户这一轮的输入之后，
// 不是用户说的话。「挑用户这一轮的输入」的两处都不该挑到它们：AutoCompact 定尾部（挑到了会把用户刚问的话摘进摘要）、
// ToolFilter 的「本轮最后一条真实消息」（挑到了业务按框架提示裁工具）。通知那一类的哨兵在 background_task_regress_test.go。

// noteTestToolkit：grep 每次返回一大段输出（同参数连调触发重复提醒，几次就攒够触发摘要的量）。
func noteTestToolkit() *tool.Toolkit {
	big := strings.Repeat("src/a.go:12: func foo() { return bar }\n", 600) // ~24KB
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("grep", "grep", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: big}, nil
		}))
	return tk
}

const noteTestQuestion = "QUESTION-请把第三个函数改名为 baz"

// runWithPriorThenCompact：Run 1 在同一场会话里攒出一段前情（两次 grep，参数不同、不触发重复提醒，这次不开压缩）；Run 2 带着
// 前情问 noteTestQuestion、按 steps 走，开了压缩（smallWindow）。返回 Run 2 的模型，以及摘要被调的次数。
func runWithPriorThenCompact(t *testing.T, conv string, steps []func([]*schema.Message) (*schema.Message, error), extra ...agent_v2.Option) (*bgScriptModel, int) {
	t.Helper()
	hs := agent_v2.NewInMemoryHistoryStore()
	ctx := compose_v2.WithThreadIDCtx(context.Background(), conv)
	m1 := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
		callStep("grep", `{"p":1}`), callStep("grep", `{"p":2}`), sayStep("搜完了")}}
	if _, err := agent_v2.NewBasicAgent(agent_v2.WithName("coder"), agent_v2.WithModel(m1), agent_v2.WithToolkit(noteTestToolkit()),
		agent_v2.WithMaxRounds(8), agent_v2.WithHistory(hs, "t")).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("先搜一下 foo")); err != nil {
		t.Fatal(err)
	}
	s := &countingSummarizer{}
	m2 := &bgScriptModel{steps: steps}
	opts := append([]agent_v2.Option{agent_v2.WithName("coder"), agent_v2.WithModel(m2), agent_v2.WithToolkit(noteTestToolkit()),
		agent_v2.WithMaxRounds(8), agent_v2.WithHistory(hs, "t"), agent_v2.WithAutoCompact(smallWindow(s))}, extra...)
	if _, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(noteTestQuestion)); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return m2, len(s.inputs)
}

// TestFrameworkNote_RepeatReminderDoesNotPushUserQuestionIntoSummary：带着前情问一句，模型用同样的参数连调两次 grep，第二次之后框架
// 追加重复调用提醒；下一轮触发摘要时，摘掉的是前情，用户这一轮的提问原文留在模型输入里——按提醒定尾部的话，用户的提问
// 连同这一轮的两次 grep 一起被摘掉。对照组不开提醒。
func TestFrameworkNote_RepeatReminderDoesNotPushUserQuestionIntoSummary(t *testing.T) {
	for _, withNote := range []bool{false, true} {
		t.Run(map[bool]string{false: "不开提醒", true: "开提醒"}[withNote], func(t *testing.T) {
			var extra []agent_v2.Option
			if withNote {
				extra = append(extra, agent_v2.WithRepeatToolReminder(&agent_v2.RepeatToolConfig{Enabled: true, Thresholds: []int{2}}))
			}
			m, summarized := runWithPriorThenCompact(t, "conv-rep-"+map[bool]string{false: "a", true: "b"}[withNote],
				[]func([]*schema.Message) (*schema.Message, error){callStep("grep", `{}`), callStep("grep", `{}`), sayStep("好")}, extra...)
			in := inputText(m.input(2))
			if summarized == 0 {
				t.Fatalf("fixture 没搭对：Run 2 没触发摘要")
			}
			if withNote && !strings.Contains(in, "[system_reminder]") {
				t.Fatalf("fixture 没搭对：第三轮输入里应有重复调用提醒")
			}
			if !strings.Contains(in, noteTestQuestion) {
				t.Fatalf("用户这一轮的提问原文不在模型输入里（被摘进了摘要）：\n%s", bgTruncateForLog(in))
			}
		})
	}
}

// TestFrameworkNote_InlineSkillBodyDoesNotPushUserQuestionIntoSummary：开了 inline skill 正文，带着前情问一句，模型先 grep 两次、
// 再激活 skill，正文以 user 消息追加在工具结果之后；下一轮触发摘要时用户的提问原文要留在模型输入里。
func TestFrameworkNote_InlineSkillBodyDoesNotPushUserQuestionIntoSummary(t *testing.T) {
	box := &fakeSkillBox{
		skills:      []skill.SkillDescriptor{{Name: "translate", Description: "翻译"}},
		activations: map[string]string{"translate": "你是翻译专家，请把文本翻译成英文。"},
	}
	m, summarized := runWithPriorThenCompact(t, "conv-skill",
		[]func([]*schema.Message) (*schema.Message, error){callStep("grep", `{"a":1}`), callStep("grep", `{"a":2}`),
			callStep(agent_v2.ActivateSkillToolName, `{"skill":"translate","query":"x"}`), sayStep("好")},
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}), agent_v2.WithInlineSkillBody(true))
	in := inputText(m.input(3))
	if summarized == 0 {
		t.Fatalf("fixture 没搭对：Run 2 没触发摘要")
	}
	if !strings.Contains(in, "你是翻译专家") {
		t.Fatalf("fixture 没搭对：第四轮输入里应有 inline 的 skill 正文")
	}
	if !strings.Contains(in, noteTestQuestion) {
		t.Fatalf("用户这一轮的提问原文不在模型输入里（被摘进了摘要）：\n%s", bgTruncateForLog(in))
	}
}

// TestFrameworkNote_ToolFilterSkipsReminderAndSkillBody：ToolFilter 收到的是本轮最后一条真实消息，框架追加的重复调用提醒、inline
// skill 正文都不算——开不开这两样，回调每轮收到的内容一样。
func TestFrameworkNote_ToolFilterSkipsReminderAndSkillBody(t *testing.T) {
	run := func(withNotes bool) []string {
		box := &fakeSkillBox{
			skills:      []skill.SkillDescriptor{{Name: "translate", Description: "翻译"}},
			activations: map[string]string{"translate": "你是翻译专家，请把文本翻译成英文。"},
		}
		var mu sync.Mutex
		var seen []string
		m := &bgScriptModel{steps: []func([]*schema.Message) (*schema.Message, error){
			callStep("grep", `{}`), callStep("grep", `{}`),
			callStep(agent_v2.ActivateSkillToolName, `{"skill":"translate","query":"x"}`), sayStep("好")}}
		opts := []agent_v2.Option{agent_v2.WithName("coder"), agent_v2.WithModel(m), agent_v2.WithToolkit(noteTestToolkit()),
			agent_v2.WithMaxRounds(8), agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
			agent_v2.WithToolFilter(func(_ context.Context, in *schema.Message) []string {
				mu.Lock()
				defer mu.Unlock()
				c := "<nil>"
				if in != nil {
					r := []rune(string(in.Role) + ":" + in.Content)
					if len(r) > 40 {
						r = r[:40]
					}
					c = string(r)
				}
				seen = append(seen, c)
				return nil
			})}
		if withNotes {
			opts = append(opts, agent_v2.WithInlineSkillBody(true),
				agent_v2.WithRepeatToolReminder(&agent_v2.RepeatToolConfig{Enabled: true, Thresholds: []int{2}}))
		}
		if _, err := agent_v2.NewBasicAgent(opts...).Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage(noteTestQuestion)); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		return seen
	}
	// 每轮该收到的：第一轮是用户的提问，之后是上一批的工具结果（含 activate_skill 那条——它也盖着 SkillBodyAnnotation，
	// 但它是真的工具结果，不是框架追加的提示）。
	want := []string{"user:QUESTION", "tool:", "tool:", "tool:"}
	for name, seen := range map[string][]string{"不开提示": run(false), "开提示": run(true)} {
		if len(seen) != len(want) {
			t.Fatalf("[%s] fixture 没搭对：ToolFilter 被调 %d 次，want %d：%q", name, len(seen), len(want), seen)
		}
		for i, w := range want {
			if !strings.HasPrefix(seen[i], w) {
				t.Errorf("[%s] 第 %d 轮 ToolFilter 收到 %q，want 以 %q 开头", name, i, seen[i], w)
			}
			if strings.Contains(seen[i], "[system_reminder]") {
				t.Errorf("[%s] 第 %d 轮 ToolFilter 收到了框架追加的提示：%q", name, i, seen[i])
			}
		}
	}
}
