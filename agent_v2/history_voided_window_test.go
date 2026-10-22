package agent_v2_test

// 节点处理完之后、提交之前作废（这里用「节点成功结束时 OnNodeEnd 钩子 Goexit 一次」造出窗口）再续跑：定下了的不再
// 处理——model 节点不再调模型、钩子不触发（见 model_call_record.go），tools 节点的这一批结果不重记；会话历史里每次执行
// 的记录只有一份、等于最终提交的那份。处理完之前作废的照常重做：等人或被叫停的沿用原样，报错、panic 的重新生成。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// goexitAfterNodeOnce 在节点 node 第一次成功结束时的 OnNodeEnd 里调 runtime.Goexit（典型来源：日志库的 fatal hook）：节点体
// 已经跑完，这次执行所在的 goroutine 被掐断、那一超步没有提交——这是节点处理完之后、提交之前作废的一条现实来路（钩子 panic
// 不再让节点作废，见 compose_v2 的 notifyNodeEnd）。Run 不返回，用 runExpectingGoexit 跑。
type goexitAfterNodeOnce struct {
	agent_v2.NoOpOrchestrationHooks
	node string
	mu   sync.Mutex
	done bool
}

func (h *goexitAfterNodeOnce) OnNodeEnd(ctx context.Context, id string, err error) context.Context {
	h.mu.Lock()
	fire := id == h.node && err == nil && !h.done
	if fire {
		h.done = true
	}
	h.mu.Unlock()
	if fire {
		runtime.Goexit()
	}
	return ctx
}

// runExpectingGoexit 在新 goroutine 里跑 run，等它被 Goexit 掐断（Run 自己的收尾照跑、留下失败档）再回来；run 正常返回说明
// fixture 没搭对。
func runExpectingGoexit(t *testing.T, run func()) {
	t.Helper()
	done := make(chan bool, 1)
	go func() {
		returned := false
		defer func() { done <- returned }()
		run()
		returned = true
	}()
	if <-done {
		t.Fatal("fixture 没搭对：节点成功之后的 OnNodeEnd 应掐断这次 Run（Goexit），它却正常返回了")
	}
}

// countingModel 每次被调都答出不同的东西：消息流里还没有工具结果时发起一次工具调用（参数带调用序号），有了就给出
// 带序号的最终答复。每个答复都带用量。seen 记下每次被调时输入里那条工具调用消息上的 "seen" 标注。
type countingModel struct {
	n    atomic.Int64
	mu   sync.Mutex
	seen []string
}

func (m *countingModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	k := m.n.Add(1)
	var out *schema.Message
	for _, mm := range msgs {
		if mm == nil {
			continue
		}
		if len(schema.GetToolCalls(mm)) > 0 {
			v, _ := mm.Annotations["seen"].(string)
			m.mu.Lock()
			m.seen = append(m.seen, v)
			m.mu.Unlock()
		}
		if mm.Role == schema.RoleTool {
			out = schema.AssistantMessage(fmt.Sprintf("答复#%d", k))
		}
	}
	if out == nil {
		out = schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "c1", Type: "function",
			Function: schema.FunctionCall{Name: "t", Arguments: fmt.Sprintf(`{"k":%d}`, k)}}})
	}
	out.Metadata = &schema.MessageMetadata{Usage: &schema.MessageUsage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15}}
	return out, nil
}
func (m *countingModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *countingModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// voidedWindowAgent 跑一次（在 node 成功结束时作废）再续跑，返回会话历史的转写与续跑收到的上行信号。
func voidedWindowAgent(t *testing.T, node string, m model.ChatModel, h hook.Hooks) (string, compose_v2.Signals) {
	t.Helper()
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("t", "t", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		return tool.Success("ok"), nil
	}))
	hs := agent_v2.NewInMemoryHistoryStore()
	opts := []agent_v2.Option{agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5), agent_v2.WithHistory(hs, "o")}
	if h != nil {
		opts = append(opts, agent_v2.WithHooks(h))
	}
	ag := agent_v2.NewBasicAgent(opts...)
	ctx := compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(context.Background(), "cv"), compose_v2.NewInMemoryCheckpointer())
	ctx = agent_v2.WithCtxOrchHooks(ctx, &goexitAfterNodeOnce{node: node})
	sess := agent_v2.NewRunSession()
	runExpectingGoexit(t, func() { _, _ = ag.Run(ctx, sess, schema.UserMessage("问")) })
	sigs, err := compose_v2.CaptureSignals(ctx, func(c context.Context) error {
		_, e := ag.Resume(c, sess.RunID, agent_v2.WithResumeSession(sess))
		return e
	})
	if err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	msgs, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "o", ConvID: "cv"})
	if err != nil {
		t.Fatal(err)
	}
	parts := make([]string, 0, len(msgs))
	for _, mm := range msgs {
		c := mm.Content
		if tcs := schema.GetToolCalls(mm); len(tcs) > 0 {
			c = "call" + tcs[0].Function.Arguments
		}
		parts = append(parts, string(mm.Role)+":"+c)
	}
	return strings.Join(parts, " | "), sigs
}

// TestHistory_ModelVoidedAfterProcessing_NoOrphanNoRecall：model 节点这一轮处理完之后作废再续跑——不再调模型，历史里只有
// 定下的那份答复（不会多出作废的那条、带着没有结果的工具调用）。
func TestHistory_ModelVoidedAfterProcessing_NoOrphanNoRecall(t *testing.T) {
	m := &countingModel{}
	got, _ := voidedWindowAgent(t, "model", m, nil)
	want := `user:问 | assistant:call{"k":1} | tool:ok | assistant:答复#2`
	if got != want {
		t.Errorf("会话历史\n got  %s\n want %s", got, want)
	}
	if n := m.n.Load(); n != 2 {
		t.Errorf("模型应只被调 2 次（第一轮定下、续跑不再调），实际 %d 次", n)
	}
}

// TestHistory_ToolsVoidedAfterProcessing_NoDuplicateResults：tools 节点这一批处理完之后作废再续跑——这一批结果只记一份
// （一个调用配两条结果的话，下一次 Run 的 prior 不合法）。
func TestHistory_ToolsVoidedAfterProcessing_NoDuplicateResults(t *testing.T) {
	got, _ := voidedWindowAgent(t, "tools", &countingModel{}, nil)
	want := `user:问 | assistant:call{"k":1} | tool:ok | assistant:答复#2`
	if got != want {
		t.Errorf("会话历史\n got  %s\n want %s", got, want)
	}
}

// modelEndFunc 把一个函数当 OnModelEnd 钩子，并数 OnModelStart 被调的次数。
type modelEndFunc struct {
	hook.NoOpHooks
	starts atomic.Int64
	fn     func(ctx context.Context, out *schema.ModelOutput) error
}

func (h *modelEndFunc) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	h.starts.Add(1)
	return ctx, nil
}

func (h *modelEndFunc) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	return ctx, h.fn(ctx, out)
}

// TestModelCall_FixedAfterProcessing_NotReprocessed：OnModelEnd 钩子给答复打上它被调的序号、记下收到的用量。第一轮处理完
// 打的是「#1」并定下；作废再续跑时定下的那一轮不再处理——OnModelStart / OnModelEnd 都不触发，进消息流的是「#1」那份，
// 用量只报过首跑那一次。
func TestModelCall_FixedAfterProcessing_NotReprocessed(t *testing.T) {
	var n atomic.Int64
	var mu sync.Mutex
	var usages []bool
	h := &modelEndFunc{fn: func(_ context.Context, out *schema.ModelOutput) error {
		mu.Lock()
		usages = append(usages, out.Usage != nil)
		mu.Unlock()
		if out.Message != nil && len(schema.GetToolCalls(out.Message)) > 0 {
			out.Message.Annotations = map[string]any{"seen": fmt.Sprintf("#%d", n.Add(1))}
		}
		return nil
	}}
	m := &countingModel{}
	got, _ := voidedWindowAgent(t, "model", m, h)
	if !strings.HasPrefix(got, `user:问 | assistant:call{"k":1}`) {
		t.Fatalf("fixture 没搭对：%s", got)
	}
	if n.Load() != 1 {
		t.Fatalf("定下了的那一轮续跑不再处理，OnModelEnd 应只处理工具调用那一轮一次，实际 %d 次", n.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(usages) != 2 || !usages[0] || !usages[1] {
		t.Errorf("OnModelEnd 收到用量的情况 %v, want [true true]（首跑工具调用那一轮、第二轮各一次）", usages)
	}
	if s := h.starts.Load(); s != 2 {
		t.Errorf("OnModelStart 应触发 2 次（首跑、第二轮；重放定下的那一轮不触发），实际 %d 次", s)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.seen) != 1 || m.seen[0] != "#1" {
		t.Errorf("第二轮模型输入里那条工具调用消息的标注 %v, want [#1]——重放时钩子的改写（#2）进了消息流", m.seen)
	}
}

// TestModelCall_FixedAfterProcessing_HookDecisionReplayed：OnModelEnd 钩子在工具调用那一轮做决定，第一次定 alpha、之后定 beta。
// 首跑处理完时这一轮的决定是 alpha；作废再续跑，交出的应是定下的 alpha（钩子不再触发）。
func TestModelCall_FixedAfterProcessing_HookDecisionReplayed(t *testing.T) {
	var n atomic.Int64
	h := &modelEndFunc{fn: func(ctx context.Context, out *schema.ModelOutput) error {
		if out.Message == nil || len(schema.GetToolCalls(out.Message)) == 0 {
			return nil
		}
		if n.Add(1) == 1 {
			evSegDecide(ctx, "alpha")
		} else {
			evSegDecide(ctx, "beta")
		}
		return nil
	}}
	_, sigs := voidedWindowAgent(t, "model", &countingModel{}, h)
	if n.Load() != 1 {
		t.Fatalf("定下了的那一轮续跑不再处理，OnModelEnd 应只处理工具调用那一轮一次，实际 %d 次", n.Load())
	}
	if got := evSegDecision(sigs); got != "alpha" {
		t.Fatalf("续跑交出的决定应是定下时的 alpha，实际 %s", got)
	}
}

// answerModel 每次被调都直接答一句带序号的话（不调工具）。
type answerModel struct{ n atomic.Int64 }

func (m *answerModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(fmt.Sprintf("答复#%d", m.n.Add(1))), nil
}
func (m *answerModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *answerModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestModelCall_HookRejectsBeforeFixed_RegeneratesOnResume：OnModelEnd 钩子第一次拒掉答复（报错）——这一轮没处理完、没定下，
// 续跑时重新调模型、换一份新的答复（业务用 OnModelEnd 报错拒掉不合格的答复时，续跑本来就该换一份）。
func TestModelCall_HookRejectsBeforeFixed_RegeneratesOnResume(t *testing.T) {
	var rejected atomic.Bool
	h := &modelEndFunc{fn: func(context.Context, *schema.ModelOutput) error {
		if rejected.CompareAndSwap(false, true) {
			return fmt.Errorf("答复不合格")
		}
		return nil
	}}
	m := &answerModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(5), agent_v2.WithHooks(h))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(ctx, sess, schema.UserMessage("问")); err == nil || !strings.Contains(err.Error(), "答复不合格") {
		t.Fatalf("fixture 没搭对：OnModelEnd 拒掉答复应让 Run 失败，got %v", err)
	}
	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess))
	if err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	if got := agent_v2.FinalMessage(events); got == nil || got.Content != "答复#2" {
		t.Fatalf("钩子拒掉之后续跑应重新调模型、换一份答复（答复#2），实际 %v（模型被调 %d 次）", got, m.n.Load())
	}
}

// TestModelCall_HookAsksBeforeFixed_ApprovalAppliesToSameAnswer：OnModelEnd 钩子把答复拿给人批准（compose_v2.Ask，问题里带着
// 答复内容）。续跑时沿用那次调用的原样答复、不再调模型——用户批准的就是问题里那一份；重新调模型的话批准落到用户从没
// 看过的另一份上。
func TestModelCall_HookAsksBeforeFixed_ApprovalAppliesToSameAnswer(t *testing.T) {
	var mu sync.Mutex
	var approved []string
	h := &modelEndFunc{fn: func(ctx context.Context, out *schema.ModelOutput) error {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准这条答复吗：" + out.Message.Content})
		if err != nil {
			return err
		}
		mu.Lock()
		approved = append(approved, out.Message.Content+" ← "+ans.Content)
		mu.Unlock()
		return nil
	}}
	m := &answerModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(5), agent_v2.WithHooks(h))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("问"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：OnModelEnd 里提问应让 Run 中断，got %v", err)
	}
	q := ie.AllQueries()[0]
	if q.Question != "批准这条答复吗：答复#1" {
		t.Fatalf("fixture 没搭对：问题 %q", q.Question)
	}
	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{q.InterruptID: schema.UserMessage("批准")}))
	if err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(approved) != "[答复#1 ← 批准]" {
		t.Errorf("批准落在 %v, want [答复#1 ← 批准]——续跑重新调了模型（共 %d 次），批准给了用户没看过的那份", approved, m.n.Load())
	}
	if got := agent_v2.FinalMessage(events); got == nil || got.Content != "答复#1" {
		t.Errorf("最终答复应是用户批准的答复#1，实际 %v", got)
	}
}

// jsonAnswerModel 每次被调都答一段 JSON：{"answer":"A#<序号>"}。
type jsonAnswerModel struct{ n atomic.Int64 }

func (m *jsonAnswerModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(fmt.Sprintf(`{"answer":"A#%d"}`, m.n.Add(1))), nil
}
func (m *jsonAnswerModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *jsonAnswerModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestModelCall_FixedReplay_NonIdempotentHookConverges：OnModelEnd 钩子把模型答的 JSON 拆成正文（解析不了就报错拒掉）。首跑
// 处理完、正文是「A#1」并定下，接着作废；续跑时交给钩子的只能是处理完的正文（不是 JSON），照样交给它的话它报错、续跑
// 永远过不去（审计实测）——定下了就不再处理：续跑收敛，答复是首跑那份，模型只调一次。
func TestModelCall_FixedReplay_NonIdempotentHookConverges(t *testing.T) {
	var parses atomic.Int64
	h := &modelEndFunc{fn: func(_ context.Context, out *schema.ModelOutput) error {
		parses.Add(1)
		var v struct{ Answer string }
		if err := json.Unmarshal([]byte(out.Message.Content), &v); err != nil {
			return fmt.Errorf("答复不是合法 JSON: %w", err)
		}
		out.Message = schema.AssistantMessage(v.Answer)
		return nil
	}}
	m := &jsonAnswerModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(5), agent_v2.WithHooks(h))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	ctx = agent_v2.WithCtxOrchHooks(ctx, &goexitAfterNodeOnce{node: "model"})
	sess := agent_v2.NewRunSession()
	runExpectingGoexit(t, func() { _, _ = ag.Run(ctx, sess, schema.UserMessage("问")) })
	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess))
	if err != nil {
		t.Fatalf("续跑应收敛，实际报错：%v", err)
	}
	if got := agent_v2.FinalMessage(events); got == nil || got.Content != "A#1" {
		t.Fatalf("答复应是首跑处理完的 A#1，实际 %v", got)
	}
	if n := m.n.Load(); n != 1 {
		t.Errorf("模型应只被调一次，实际 %d 次", n)
	}
	if n := parses.Load(); n != 1 {
		t.Errorf("钩子应只在首跑处理一次，实际 %d 次", n)
	}
}

// TestModelCall_HookPanicsBeforeFixed_RegeneratesOnResume：OnModelEnd 钩子在「答复#1」上 panic（内容触发的 bug）——这一轮没
// 处理完、也不是在等人，续跑重新调模型换一份（答复#2）；沿用原样的话每次续跑都在同一份上 panic（审计实测）。
func TestModelCall_HookPanicsBeforeFixed_RegeneratesOnResume(t *testing.T) {
	h := &modelEndFunc{fn: func(_ context.Context, out *schema.ModelOutput) error {
		if out.Message.Content == "答复#1" {
			panic("钩子在答复#1 上出错")
		}
		return nil
	}}
	m := &answerModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(5), agent_v2.WithHooks(h))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(ctx, sess, schema.UserMessage("问")); err == nil {
		t.Fatal("fixture 没搭对：钩子 panic 应让 Run 失败")
	}
	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess))
	if err != nil {
		t.Fatalf("续跑应重新生成并收敛，实际报错：%v（模型被调 %d 次）", err, m.n.Load())
	}
	if got := agent_v2.FinalMessage(events); got == nil || got.Content != "答复#2" {
		t.Fatalf("续跑应换一份答复（答复#2），实际 %v", got)
	}
}

// TestModelCall_RawReuse_UsageReportedOnce：OnModelEnd 钩子链是「记用量 → 问人」。首跑那次 OnModelEnd 在问人处停下，排在前面的
// 那个钩子已经收到用量；续跑沿用原样再处理一次时不该再带用量——一次模型调用只报一次。
func TestModelCall_RawReuse_UsageReportedOnce(t *testing.T) {
	var mu sync.Mutex
	usageReports := 0
	record := &modelEndFunc{fn: func(_ context.Context, out *schema.ModelOutput) error {
		if out.Usage != nil {
			mu.Lock()
			usageReports++
			mu.Unlock()
		}
		return nil
	}}
	ask := &modelEndFunc{fn: func(ctx context.Context, out *schema.ModelOutput) error {
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗：" + out.Message.Content})
		return err
	}}
	m := &countingModel{}
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("t", "t", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		return tool.Success("ok"), nil
	}))
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(5),
		agent_v2.WithHooks(hook.NewChainHooks(record, ask)))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("问"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：应在问人处中断，got %v", err)
	}
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")}
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	for i := 0; i < 3 && err != nil; i++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			t.Fatalf("续跑失败：%v", err)
		}
		for _, q := range ie.AllQueries() {
			answers[q.InterruptID] = schema.UserMessage("批准")
		}
		_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	}
	if err != nil {
		t.Fatalf("续跑没收敛：%v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls := m.n.Load(); int(calls) != usageReports {
		t.Errorf("模型被调 %d 次、OnModelEnd 收到用量 %d 次——一次模型调用应只报一次用量", calls, usageReports)
	}
}

// cancelOnModelStart 在装上取消函数之后的第一次 OnModelStart 里调它（模拟调用方在续跑中途撤了请求），返回 ctx 的错误。
type cancelOnModelStart struct {
	modelEndFunc
	cancel atomic.Pointer[context.CancelFunc]
}

func (h *cancelOnModelStart) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	if c := h.cancel.Swap(nil); c != nil {
		(*c)()
		return ctx, ctx.Err()
	}
	return h.modelEndFunc.OnModelStart(ctx, info, in)
}

// TestModelCall_StoppedDuringRawReuse_KeepsSameAnswer：OnModelEnd 钩子把答复拿给人批准、中断；续跑沿用原样时调用方在 OnModelStart
// 处撤了请求——这是被叫停、不是拒掉答复，原样要留着。再续跑时用户批准的仍是问题里那一份（答复#1），模型只调一次；作废原样
// 的话再续跑会重新调模型，累积传的批准落到用户没看过的答复#2 上。
func TestModelCall_StoppedDuringRawReuse_KeepsSameAnswer(t *testing.T) {
	var mu sync.Mutex
	var approved []string
	h := &cancelOnModelStart{modelEndFunc: modelEndFunc{fn: func(ctx context.Context, out *schema.ModelOutput) error {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准这条答复吗：" + out.Message.Content})
		if err != nil {
			return err
		}
		mu.Lock()
		approved = append(approved, out.Message.Content+" ← "+ans.Content)
		mu.Unlock()
		return nil
	}}}
	m := &answerModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(5), agent_v2.WithHooks(h))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("问"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：OnModelEnd 里提问应让 Run 中断，got %v", err)
	}
	answers := agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")})

	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	h.cancel.Store(&cancel)
	if _, err := ag.Resume(rctx, sess.RunID, agent_v2.WithResumeSession(sess), answers); !errors.Is(err, context.Canceled) {
		t.Fatalf("fixture 没搭对：续跑应因调用方撤了请求而停下，got %v", err)
	}
	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), answers)
	if err != nil {
		t.Fatalf("再续跑失败：%v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(approved) != "[答复#1 ← 批准]" {
		t.Errorf("批准落在 %v, want [答复#1 ← 批准]——被叫停时原样被作废了（模型共被调 %d 次）", approved, m.n.Load())
	}
	if got := agent_v2.FinalMessage(events); got == nil || got.Content != "答复#1" {
		t.Errorf("最终答复应是用户批准的答复#1，实际 %v", got)
	}
}

// reuseMarkRecorder 记每次 OnModelStart / OnModelEnd 是否标着「沿用原样」、OnModelEnd 有没有收到用量，并在 OnModelEnd 里
// 把答复拿给人批准。
type reuseMarkRecorder struct {
	hook.NoOpHooks
	mu     sync.Mutex
	starts []bool
	ends   []bool
	usages []bool
}

func (h *reuseMarkRecorder) OnModelStart(ctx context.Context, _ *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	h.mu.Lock()
	h.starts = append(h.starts, hook.IsModelResponseReused(in.Extra))
	h.mu.Unlock()
	return ctx, nil
}

func (h *reuseMarkRecorder) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	h.mu.Lock()
	h.ends = append(h.ends, hook.IsModelResponseReused(out.Extra))
	h.usages = append(h.usages, out.Usage != nil)
	h.mu.Unlock()
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗：" + out.Message.Content})
	return ctx, err
}

// TestModelCall_RawReuse_MarkedReused：两轮模型调用（工具调用一轮、答复一轮），每轮的 OnModelEnd 都问人。每轮都是「真调一次
// 模型 → 停下 → 续跑沿用原样再走一遍钩子」——标记恰好出现在沿用原样的那几遍上、那几遍没有用量，真调模型的那几遍
// 没有标记、带着用量（见 hook.ModelResponseReusedKey）。
func TestModelCall_RawReuse_MarkedReused(t *testing.T) {
	h := &reuseMarkRecorder{}
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("t", "t", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
		return tool.Success("ok"), nil
	}))
	m := &countingModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(5), agent_v2.WithHooks(h))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("问"))
	answers := map[string]*schema.Message{}
	for i := 0; i < 4 && err != nil; i++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			t.Fatalf("续跑失败：%v", err)
		}
		for _, q := range ie.AllQueries() {
			answers[q.InterruptID] = schema.UserMessage("批准")
		}
		_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	}
	if err != nil {
		t.Fatalf("续跑没收敛：%v", err)
	}
	if n := m.n.Load(); n != 2 {
		t.Fatalf("fixture 没搭对：模型应被调两次（每轮一次），实际 %d 次", n)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	want := "[false true false true]"
	if got := fmt.Sprint(h.starts); got != want {
		t.Errorf("OnModelStart 的沿用标记 %s, want %s", got, want)
	}
	if got := fmt.Sprint(h.ends); got != want {
		t.Errorf("OnModelEnd 的沿用标记 %s, want %s", got, want)
	}
	if got := fmt.Sprint(h.usages); got != "[true false true false]" {
		t.Errorf("OnModelEnd 收到用量的情况 %s, want [true false true false]", got)
	}
}

// TestHistory_MemberModelVoidedAfterProcessing_FinalRecordedOnce：BasicAgent 作为 Sequential 的成员，model 节点那一轮处理完之后
// 作废再续跑——定下的答复只在历史里一份。成员续跑时不再写，编排边界补记据「这次调用里记过」不替它记；不登记的话边界
// 把同一条答复（同 ID、同归属）再记一份（审计实测）。顶层 BasicAgent 没有边界，测不到这条。
func TestHistory_MemberModelVoidedAfterProcessing_FinalRecordedOnce(t *testing.T) {
	m := &answerModel{}
	hs := agent_v2.NewInMemoryHistoryStore()
	inner := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(5),
		agent_v2.WithHistory(hs, "o"))
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("step", inner)
	ctx := compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(context.Background(), "cv"), compose_v2.NewInMemoryCheckpointer())
	ctx = agent_v2.WithCtxOrchHooks(ctx, &goexitAfterNodeOnce{node: "model"})
	sess := agent_v2.NewRunSession()
	runExpectingGoexit(t, func() { _, _ = seq.Run(ctx, sess, schema.UserMessage("问")) })
	if _, err := seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess)); err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	msgs, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "o", ConvID: "cv"})
	if err != nil {
		t.Fatal(err)
	}
	parts := make([]string, 0, len(msgs))
	for _, mm := range msgs {
		parts = append(parts, string(mm.Role)+":"+mm.Content)
	}
	if got, want := strings.Join(parts, " | "), "user:问 | assistant:答复#1"; got != want {
		t.Errorf("会话历史\n got  %s\n want %s", got, want)
	}
	if n := m.n.Load(); n != 1 {
		t.Errorf("模型应只被调一次，实际 %d 次", n)
	}
}

// regenApprovalHook：OnModelStart 在第 panicStartAt 次被调时 panic 一次（0 = 不 panic）；OnModelEnd 把答复拿给人批准，批准之后
// failAfterApproval 次报一个业务错；approved 记批准落在哪一份答复上。
type regenApprovalHook struct {
	hook.NoOpHooks
	panicStartAt      int64
	starts            atomic.Int64
	failAfterApproval atomic.Int64
	mu                sync.Mutex
	approved          []string
	questions         []string
}

func (h *regenApprovalHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	if h.starts.Add(1) == h.panicStartAt {
		panic("OnModelStart 瞬时 panic（跟答复内容无关）")
	}
	return ctx, nil
}

func (h *regenApprovalHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	q := "批准这条答复吗：" + out.Message.Content
	h.mu.Lock()
	h.questions = append(h.questions, q)
	h.mu.Unlock()
	ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: q})
	if err != nil {
		return ctx, err
	}
	if h.failAfterApproval.Add(-1) == 0 {
		return ctx, errors.New("批准之后写审计日志失败（瞬时）")
	}
	h.mu.Lock()
	h.approved = append(h.approved, out.Message.Content+" ← "+ans.Content)
	h.mu.Unlock()
	return ctx, nil
}

// interrupted 报 err 是不是中断。
func interrupted(err error) bool {
	_, ok := compose_v2.IsInterrupt(err)
	return ok
}

// runRegenApproval：Run 在 OnModelEnd 的批准问题上中断；第一次续跑带上批准、在沿用原样那一遍出错（原样被作废）；调用方原样
// 重试（答案照旧累积传）。返回第二次续跑的错误。
func runRegenApproval(t *testing.T, h *regenApprovalHook) (*answerModel, error) {
	t.Helper()
	m := &answerModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(5), agent_v2.WithHooks(h))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("问"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：OnModelEnd 里提问应让 Run 中断，got %v", err)
	}
	answers := agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("批准")})
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), answers); err == nil || interrupted(err) {
		t.Fatalf("fixture 没搭对：第一次续跑应在沿用原样那一遍出错，got %v", err)
	}
	_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), answers)
	return m, err
}

// assertAskedAboutNewAnswer：重新生成之后，对新答复的批准问题换了新 ID——累积传的旧批准对不上，续跑停在对答复#2 的提问上。
func assertAskedAboutNewAnswer(t *testing.T, h *regenApprovalHook, m *answerModel, err error) {
	t.Helper()
	ie, ok := compose_v2.IsInterrupt(err)
	h.mu.Lock()
	defer h.mu.Unlock()
	if !ok {
		t.Fatalf("重新生成的答复应换新问题、等用户批准，实际 err=%v，批准落在 %v（模型被调 %d 次）", err, h.approved, m.n.Load())
	}
	if q := ie.AllQueries()[0].Question; q != "批准这条答复吗：答复#2" {
		t.Errorf("续跑应问用户批不批准答复#2，实际问 %q", q)
	}
	if len(h.approved) != 0 {
		t.Errorf("旧的批准不该兑付给任何一份答复，实际 %v", h.approved)
	}
}

// TestModelCall_RegeneratedAfterStartPanic_AsksAgain：沿用原样那一遍 OnModelStart 瞬时 panic（跟答复内容无关），原样被作废、
// 重新调模型——OnModelEnd 对新答复的提问换新 ID（见 modelHookCtx）；不换的话调用方累积传的「批准」按同一个 ID 兑付给用户从没
// 看过的答复#2（审计实测）。
func TestModelCall_RegeneratedAfterStartPanic_AsksAgain(t *testing.T) {
	h := &regenApprovalHook{panicStartAt: 2}
	m, err := runRegenApproval(t, h)
	assertAskedAboutNewAnswer(t, h, m, err)
}

// TestModelCall_RegeneratedAfterBusinessError_AsksAgain：OnModelEnd 拿到批准之后同一个钩子的下一步报业务错，原样被作废、重新
// 调模型——同上，对答复#2 要重新问。
func TestModelCall_RegeneratedAfterBusinessError_AsksAgain(t *testing.T) {
	h := &regenApprovalHook{}
	h.failAfterApproval.Store(1)
	m, err := runRegenApproval(t, h)
	assertAskedAboutNewAnswer(t, h, m, err)
}

// startAskHook：OnModelStart 问一次「开始吗」；OnModelEnd 第一次报业务错（原样被作废、重新调模型）。startQs 记 OnModelStart 的
// 提问出现过几个不同 ID。
type startAskHook struct {
	hook.NoOpHooks
	endFails atomic.Int64
	mu       sync.Mutex
	startIDs map[string]bool
}

func (h *startAskHook) OnModelStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "开始吗"})
	if ie, ok := compose_v2.IsInterrupt(err); ok {
		h.mu.Lock()
		h.startIDs[ie.AllQueries()[0].InterruptID] = true
		h.mu.Unlock()
	}
	return ctx, err
}

func (h *startAskHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	if h.endFails.Add(-1) == 0 {
		return ctx, errors.New("拒掉这份答复")
	}
	return ctx, nil
}

// TestModelCall_Regenerated_StartQuestionNotReasked：OnModelStart 问的是输入，重新调模型不改变输入——它的提问不随调模型的次数
// 换 ID，答过的不再问（续跑一次收敛）；把它的相位也按次数命名的话，重新调模型之后同一个问题换 ID 再问一遍。
func TestModelCall_Regenerated_StartQuestionNotReasked(t *testing.T) {
	h := &startAskHook{startIDs: map[string]bool{}}
	h.endFails.Store(1)
	m := &answerModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(5), agent_v2.WithHooks(h))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("问"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("fixture 没搭对：OnModelStart 里提问应让 Run 中断，got %v", err)
	}
	answers := agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("开始")})
	if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), answers); err == nil || interrupted(err) {
		t.Fatalf("fixture 没搭对：第一次续跑应在 OnModelEnd 报错，got %v", err)
	}
	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), answers)
	if err != nil {
		t.Fatalf("重新调模型之后 OnModelStart 的问题已经答过，续跑应收敛，实际 %v", err)
	}
	if got := agent_v2.FinalMessage(events); got == nil || got.Content != "答复#2" {
		t.Errorf("最终答复应是重新生成的答复#2，实际 %v", got)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.startIDs) != 1 {
		t.Errorf("OnModelStart 的提问应只有一个 ID，实际 %d 个", len(h.startIDs))
	}
}

// TestModelCall_RegeneratedAfterVoid_NormalNestedCallRecalled：OnModelEnd 里先经 RunSubAgent 做一件与答复无关的事（扣一次费），
// 再报业务错拒掉这份答复——原样作废、续跑重新调模型。那次嵌套调用是普通完成记录，重新调模型之后照常拿回、不重做（同条件边
// 否决重做只作废 Rerunnable 的）；把 OnModelEnd 那一格整个换掉的话，扣费跑两次（审计实测）。
func TestModelCall_RegeneratedAfterVoid_NormalNestedCallRecalled(t *testing.T) {
	subM := &answerModel{}
	sub := agent_v2.NewBasicAgent(agent_v2.WithName("guard"), agent_v2.WithModel(subM), agent_v2.WithMaxRounds(3))
	var failed atomic.Bool
	h := &modelEndFunc{fn: func(ctx context.Context, _ *schema.ModelOutput) error {
		if _, err := agent_v2.RunSubAgent(ctx, "guard", sub, schema.UserMessage("扣一次费")); err != nil {
			return err
		}
		if failed.CompareAndSwap(false, true) {
			return errors.New("答复不合格")
		}
		return nil
	}}
	m := &answerModel{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(m), agent_v2.WithMaxRounds(5), agent_v2.WithHooks(h))
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	if _, err := ag.Run(ctx, sess, schema.UserMessage("问")); err == nil {
		t.Fatal("fixture 没搭对：OnModelEnd 报错应让 Run 失败")
	}
	events, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess))
	if err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	if got := agent_v2.FinalMessage(events); got == nil || got.Content != "答复#2" {
		t.Fatalf("fixture 没搭对：续跑应重新调模型（答复#2），实际 %v", got)
	}
	if n := subM.n.Load(); n != 1 {
		t.Errorf("与答复无关的那次嵌套调用应只跑一次（重新调模型之后拿回完成记录），实际 %d 次", n)
	}
}

// regenGraphAgent 把 inner 包进一张 GraphAgent 图：节点里经 RunSubAgent 调起它。
func regenGraphAgent(t *testing.T, inner agent_v2.Agent) agent_v2.Agent {
	t.Helper()
	b := compose_v2.NewBuilder().WithName("g")
	inCh := compose_v2.LastValue[*schema.Message]("in")
	compose_v2.WithChannel(b, inCh)
	b.AddNode("n", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		_, err := agent_v2.RunSubAgent(ctx, "inner", inner, in)
		return nil, err
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("g"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(compose_v2.Channels) *schema.Message { return schema.AssistantMessage("done") }))
}

// TestModelCall_RegeneratedAfterVoid_AcrossCallSites：OnModelEnd 里先做两次嵌套调用（扣费是普通记录、起草声明了 WithRerunnable），
// 再问人批准，批准之后第一次报业务错——原样作废、重新调模型。BasicAgent 放在三种位置上（顶层、编排成员、节点里经 RunSubAgent
// 调起）结果都一样：对新答复的提问换新 ID，扣费拿回不重做，起草重做（同条件边否决那一格）。
func TestModelCall_RegeneratedAfterVoid_AcrossCallSites(t *testing.T) {
	wraps := map[string]func(*testing.T, agent_v2.Agent) agent_v2.Agent{
		"top": func(_ *testing.T, a agent_v2.Agent) agent_v2.Agent { return a },
		"seq_member": func(_ *testing.T, a agent_v2.Agent) agent_v2.Agent {
			return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seq")).AddStep("step", a)
		},
		"graph_node": regenGraphAgent,
	}
	for name, wrap := range wraps {
		t.Run(name, func(t *testing.T) {
			chargeM, draftM := &answerModel{}, &answerModel{}
			charge := agent_v2.NewBasicAgent(agent_v2.WithName("charge"), agent_v2.WithModel(chargeM), agent_v2.WithMaxRounds(3))
			draft := agent_v2.NewBasicAgent(agent_v2.WithName("draft"), agent_v2.WithModel(draftM), agent_v2.WithMaxRounds(3))
			var failed atomic.Bool
			h := &modelEndFunc{fn: func(ctx context.Context, out *schema.ModelOutput) error {
				if _, err := agent_v2.RunSubAgent(ctx, "charge", charge, schema.UserMessage("扣费")); err != nil {
					return err
				}
				if _, err := agent_v2.RunSubAgent(ctx, "draft", draft, schema.UserMessage("起草"), agent_v2.WithRerunnable()); err != nil {
					return err
				}
				if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准吗：" + out.Message.Content}); err != nil {
					return err
				}
				if failed.CompareAndSwap(false, true) {
					return errors.New("批准之后瞬时失败")
				}
				return nil
			}}
			inner := agent_v2.NewBasicAgent(agent_v2.WithName("fx"), agent_v2.WithModel(&answerModel{}), agent_v2.WithMaxRounds(5), agent_v2.WithHooks(h))
			ag := wrap(t, inner)
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			_, err := ag.Run(ctx, sess, schema.UserMessage("问"))
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("fixture 没搭对：OnModelEnd 里提问应让 Run 中断，got %v", err)
			}
			q1 := ie.AllQueries()[0]
			answers := agent_v2.WithResumeUserInputs(map[string]*schema.Message{q1.InterruptID: schema.UserMessage("批准")})
			if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), answers); err == nil || interrupted(err) {
				t.Fatalf("fixture 没搭对：第一次续跑应在批准之后报错，got %v", err)
			}
			_, err = ag.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), answers)
			ie2, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("重新调模型之后应对新答复重新提问，实际 err=%v", err)
			}
			if q2 := ie2.AllQueries()[0]; q2.InterruptID == q1.InterruptID || q2.Question != "批准吗：答复#2" {
				t.Errorf("应以新 ID 问答复#2，实际 %q（ID 相同=%v）", q2.Question, q2.InterruptID == q1.InterruptID)
			}
			if n := chargeM.n.Load(); n != 1 {
				t.Errorf("普通嵌套调用（扣费）应拿回不重做，实际跑 %d 次", n)
			}
			if n := draftM.n.Load(); n != 2 {
				t.Errorf("声明了 WithRerunnable 的嵌套调用（起草）应重做，实际跑 %d 次", n)
			}
		})
	}
}
