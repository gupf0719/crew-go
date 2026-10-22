package a2a_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// 第三轮审计之后重推的规则：RemoteAgent 在节点执行里一律另开自己的相位，不占调用它的那段代码的提问序号；相位名决定
// 续跑时认不认得出同一次调用。每一维一条哨兵：
//   - 调用方在别人给的相位里（框架交接给的业务壳）按进度跳过这次调用时，后面的提问不挪位（审计实测的那条）；
//   - 在别人给的相位里，相位名带上外层相位名（相位名在一次执行里唯一）：同一个 tool_call 的 OnToolStart 与 OnToolEnd
//     各调一次同一个远端，RemoteAgent 自己的钩子在各自的相位里问的两问不撞 ID。远端那一问不靠这一维——它的 ID 还带着
//     远端任务 ID；
//   - 在别人给的相位里，相位名带「在这个相位里第几次」：被交接的业务壳先后调两次，各续各的远端任务；
//   - 在节点体里，相位名按请求内容：先后两次调用、按进度跳过第一次，第二次找回自己挂着的那一问；
//   - 在节点体里，相位名带「同样内容第几次」：同样的请求连发两次，各续各的远端任务。

// progressWrapAgent 是被框架交接的业务壳：先调远端（做完记下进度，续跑时跳过），再依次问两问。
type progressWrapAgent struct{ remote agent_v2.Agent }

func (w *progressWrapAgent) Name() string        { return "progress_wrap" }
func (w *progressWrapAgent) Description() string { return "calls a remote agent, then asks twice" }
func (w *progressWrapAgent) Run(ctx context.Context, sess *agent_v2.RunSession, in *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	ctx, sess, err := agent_v2.EnterRun(ctx, sess, w.Name())
	if err != nil {
		return nil, err
	}
	if _, ok := compose_v2.GetStepResult[string](ctx, "remote_done"); !ok {
		evs, err := w.remote.Run(ctx, sess, in)
		if err != nil {
			return nil, err
		}
		if err := compose_v2.SaveStepResult(ctx, "remote_done", finalText(evs)); err != nil {
			return nil, err
		}
	}
	a, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "QA:转多少钱?"})
	if err != nil {
		return nil, err
	}
	b, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "QB:确认执行吗?"})
	if err != nil {
		return nil, err
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.Name(), schema.AssistantMessage("A="+a.Content+" B="+b.Content))}, nil
}
func (w *progressWrapAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, errors.New("progressWrapAgent: standalone Resume not supported")
}

// 业务壳经 RunSubAgent 调起（在交接相位里），壳里先调 RemoteAgent、远端直接完成，壳记下进度、续跑时跳过它，之后依次问
// 两问。曾经在别人给的相位里 RemoteAgent 不另开相位、取位置时占了那个相位的一个提问序号：跳过它之后第一问挪到它的
// 位置上又问了一遍，第二问拿到第一问的答案、从没问过（审计实测，`err == nil`）。
func TestRemoteAgent_WrapperHandedOver_SkippingRemoteDoesNotShiftLaterAsks(t *testing.T) {
	r := &doneRemote{}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, r))
	if err != nil {
		t.Fatal(err)
	}
	w := &progressWrapAgent{remote: ra}
	host := rsHost(t, func(ctx context.Context, _ string) (string, error) {
		evs, err := agent_v2.RunSubAgent(ctx, "wrap", w, schema.UserMessage("查询"))
		if err != nil {
			return "", err
		}
		return finalText(evs), nil
	})
	asked, final, err := driveByQuestion(t, host, "go", func(q string) string {
		if strings.HasPrefix(q, "QA") {
			return "100"
		}
		return "yes"
	})
	if err != nil || final != "A=100 B=yes" {
		t.Fatalf("got final=%q err=%v（问过 %q）", final, err, asked)
	}
	if want := []string{"QA:转多少钱?", "QB:确认执行吗?"}; !slices.Equal(asked, want) {
		t.Errorf("问过 %q，want %q", asked, want)
	}
	if n := r.calls.Load(); n != 1 {
		t.Errorf("远端应只被调一次，实际 %d 次", n)
	}
}

// twoCallWrapAgent 是被框架交接的业务壳：先后裸调同一个 RemoteAgent 两次（req-A、req-B）。
type twoCallWrapAgent struct{ remote agent_v2.Agent }

func (w *twoCallWrapAgent) Name() string        { return "two_call_wrap" }
func (w *twoCallWrapAgent) Description() string { return "calls a remote agent twice" }
func (w *twoCallWrapAgent) Run(ctx context.Context, sess *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	ctx, sess, err := agent_v2.EnterRun(ctx, sess, w.Name())
	if err != nil {
		return nil, err
	}
	var outs []string
	for _, req := range []string{"req-A", "req-B"} {
		evs, err := w.remote.Run(ctx, sess, schema.UserMessage(req))
		if err != nil {
			return nil, err
		}
		outs = append(outs, finalText(evs))
	}
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent(w.Name(), schema.AssistantMessage(strings.Join(outs, "|")))}, nil
}
func (w *twoCallWrapAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, errors.New("twoCallWrapAgent: standalone Resume not supported")
}

// 被交接的业务壳里先后调两次同一个 RemoteAgent，两边都要人：两次调用各续各的远端任务，收敛（裸调的重跑语义同
// TestHumanGate_BareDoubleCallInNode_EachTaskGetsItsOwnAnswer）。别人给的相位里相位名不看请求内容，只能按次序区分：
// 不带「在这个相位里第几次」的话两次调用落进同一个相位、共用一条等人记录，第一次续上第二次挂着的任务、拿走它的
// 结果，永不收敛。
func TestRemoteAgent_WrapperHandedOver_TwoCalls_EachContinuesItsOwnTask(t *testing.T) {
	r := &taskRemote{}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, fnExec(r.exec)), a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	w := &twoCallWrapAgent{remote: ra}
	host := rsHost(t, func(ctx context.Context, _ string) (string, error) {
		evs, err := agent_v2.RunSubAgent(ctx, "wrap", w, schema.UserMessage("go"))
		if err != nil {
			return "", err
		}
		return finalText(evs), nil
	})
	asked, _, final, err := taskDrive(t, host, "go", true, answerByQuestion)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q，远端收到 %q）", err, asked, r.snapshot())
	}
	r.check(t)
	if want := "续任务[req-A]办了:答:批准吗:req-A|续任务[req-B]办了:答:批准吗:req-B"; final != want {
		t.Errorf("final = %q，want %q", final, want)
	}
}

// toolHookRemoteHooks 在每个 tool_call 的 OnToolStart 与 OnToolEnd 里各调一次同一个 RemoteAgent（各带自己的请求），
// 做完记下进度、续跑时跳过（每次钩子调用有自己的步骤记录，见 compose_v2.EnterPhase）。
type toolHookRemoteHooks struct {
	hook.NoOpHooks
	remote agent_v2.Agent
}

func (h *toolHookRemoteHooks) call(ctx context.Context, req string) error {
	if _, ok := compose_v2.GetStepResult[string](ctx, "remote_done"); ok {
		return nil
	}
	evs, err := h.remote.Run(ctx, agent_v2.GetSession(ctx), schema.UserMessage(req))
	if err != nil {
		return err
	}
	return compose_v2.SaveStepResult(ctx, "remote_done", finalText(evs))
}

func (h *toolHookRemoteHooks) OnToolStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	return ctx, h.call(ctx, "req-start")
}

func (h *toolHookRemoteHooks) OnToolEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolOutput) (context.Context, error) {
	return ctx, h.call(ctx, "req-end")
}

// noopCallModel：第一轮回一个调 noop 的 tool_call，看到工具结果后收尾。
type noopCallModel struct{}

func (noopCallModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, m := range msgs {
		if m != nil && m.Role == schema.RoleTool {
			return schema.AssistantMessage("final"), nil
		}
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "c1", Type: "function", Function: schema.FunctionCall{Name: "noop", Arguments: `{}`}},
	}), nil
}
func (noopCallModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m noopCallModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// 同一个 tool_call 的 OnToolStart 与 OnToolEnd 各调一次同一个 RemoteAgent，远端两边都要人：两问各有各的 ID，各自用
// 自己的答案续自己的远端任务（钩子相位里经 human gate 问人的完整往返）。
func TestRemoteAgent_InStartAndEndToolHooks_EachAsksItsOwnQuestion(t *testing.T) {
	r := &taskRemote{}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, fnExec(r.exec)), a2a.WithHumanGate(), a2a.WithName("remote"))
	if err != nil {
		t.Fatal(err)
	}
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("noop", "does nothing", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: "ok"}, nil
		}))
	host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithDescription("h"),
		agent_v2.WithToolkit(tk), agent_v2.WithModel(noopCallModel{}), agent_v2.WithMaxRounds(4),
		agent_v2.WithHooks(&toolHookRemoteHooks{remote: ra}))
	asked, ids, final, err := taskDrive(t, host, "go", true, answerByQuestion)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q，远端收到 %q）", err, asked, r.snapshot())
	}
	r.check(t)
	if want := []string{"批准吗:req-start", "批准吗:req-end"}; !slices.Equal(asked, want) || ids[0] == ids[1] {
		t.Errorf("问过 %q（ID %q），want %q 且两问 ID 不同", asked, ids, want)
	}
	want := []string{"new:req-start", "cont[req-start]:答:批准吗:req-start", "new:req-end", "cont[req-end]:答:批准吗:req-end"}
	if got := r.snapshot(); !slices.Equal(got, want) {
		t.Errorf("远端收到 %q，want %q", got, want)
	}
	if final != "final" {
		t.Errorf("final = %q", final)
	}
}

// 节点体里先后对同一个 RemoteAgent 发两个不同的请求，两边都要人；节点按进度跳过已经做完的那次。第二次调用在第一次
// 被跳过之后照样找回自己挂着的那一问。节点体里的相位名要是只按次序（不看请求内容）定，第一次被跳过之后第二次落进
// 第一次的相位，撞上第一问已兑付的答案位、或把第一问的答案发给第二个远端任务。
func TestRemoteAgent_InNodeDirectCall_SkippingFirstOfTwo_SecondFindsItsQuestion(t *testing.T) {
	r := &taskRemote{}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, fnExec(r.exec)), a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	host := rsHost(t, func(ctx context.Context, _ string) (string, error) {
		var outs []string
		for _, req := range []string{"req-A", "req-B"} {
			key := "done:" + req
			if out, ok := compose_v2.GetStepResult[string](ctx, key); ok {
				outs = append(outs, out)
				continue
			}
			evs, err := ra.Run(ctx, agent_v2.GetSession(ctx), schema.UserMessage(req))
			if err != nil {
				return "", err
			}
			out := finalText(evs)
			if err := compose_v2.SaveStepResult(ctx, key, out); err != nil {
				return "", err
			}
			outs = append(outs, out)
		}
		return strings.Join(outs, "|"), nil
	})
	asked, _, final, err := taskDrive(t, host, "go", true, answerByQuestion)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q，远端收到 %q）", err, asked, r.snapshot())
	}
	r.check(t)
	if want := []string{"批准吗:req-A", "批准吗:req-B"}; !slices.Equal(asked, want) {
		t.Errorf("问过 %q，want %q", asked, want)
	}
	want := []string{"new:req-A", "cont[req-A]:答:批准吗:req-A", "new:req-B", "cont[req-B]:答:批准吗:req-B"}
	if got := r.snapshot(); !slices.Equal(got, want) {
		t.Errorf("远端收到 %q，want %q", got, want)
	}
	if want := "续任务[req-A]办了:答:批准吗:req-A|续任务[req-B]办了:答:批准吗:req-B"; final != want {
		t.Errorf("final = %q，want %q", final, want)
	}
}

// 节点体里对同一个 RemoteAgent 连发两次同样的请求，两边都要人：两次调用各续各的远端任务，收敛。（裸调没有完成记录：
// 节点因为第二次调用挂起而重跑时，第一次调用会再执行一遍、新开任务再问——同 TestHumanGate_BareDoubleCallInNode_
// EachTaskGetsItsOwnAnswer。）内容相同的几次只能按次序区分，相位名要是不带「同样内容第几次」，两次调用落进同一个
// 相位、共用一条等人记录：第一次续上第二次挂着的任务，第二次再开新任务，每轮多一个任务、永不收敛。
func TestRemoteAgent_InNodeDirectCall_SameRequestTwice_EachContinuesItsOwnTask(t *testing.T) {
	r := &taskRemote{}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, fnExec(r.exec)), a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	host := rsHost(t, func(ctx context.Context, _ string) (string, error) {
		var outs []string
		for range 2 {
			evs, err := ra.Run(ctx, agent_v2.GetSession(ctx), schema.UserMessage("req-A"))
			if err != nil {
				return "", err
			}
			outs = append(outs, finalText(evs))
		}
		return strings.Join(outs, "|"), nil
	})
	asked, ids, final, err := taskDrive(t, host, "go", true, answerByQuestion)
	if err != nil {
		t.Fatalf("没收敛：%v（问过 %q，远端收到 %q）", err, asked, r.snapshot())
	}
	r.check(t)
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Errorf("同一个 ID 问了两次——两次调用共用了一问（问过 %q，ID %q）", asked, ids)
			break
		}
		seen[id] = true
	}
	if want := "续任务[req-A]办了:答:批准吗:req-A|续任务[req-A]办了:答:批准吗:req-A"; final != want {
		t.Errorf("final = %q，want %q", final, want)
	}
}

// confirmOnStartHooks 是 RemoteAgent 自己的 agent 级钩子（a2a.WithHooks）：每次发请求之前问一次确认，记下拿到的答复。
// 它跑在这次调用自己的相位里。
type confirmOnStartHooks struct {
	hook.NoOpHooks
	mu  sync.Mutex
	got []string
}

func (h *confirmOnStartHooks) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	req := in.Messages[len(in.Messages)-1].Content
	ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认发出 " + req + " 吗?"})
	if err != nil {
		return ctx, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.got = append(h.got, req+"="+ans.Content)
	return ctx, nil
}

func (h *confirmOnStartHooks) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.got...)
}

// 同一个 tool_call 的 OnToolStart 与 OnToolEnd 各调一次同一个 RemoteAgent，RemoteAgent 自己的钩子在发请求之前问一次
// 确认：两问各有各的 ID，各自拿到对自己那一问的答复。两个钩子相位里 RemoteAgent 的相位名要是不带外层相位名（相位名
// 在一次执行里不唯一），钩子里的两问算出同一个 ID，第二问拿到第一问的答复。
func TestRemoteAgent_InStartAndEndToolHooks_AsksInsideRemotePhaseDoNotCollide(t *testing.T) {
	r := &doneRemote{}
	confirm := &confirmOnStartHooks{}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, r), a2a.WithHooks(confirm), a2a.WithName("remote"))
	if err != nil {
		t.Fatal(err)
	}
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("noop", "does nothing", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: "ok"}, nil
		}))
	host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithDescription("h"),
		agent_v2.WithToolkit(tk), agent_v2.WithModel(noopCallModel{}), agent_v2.WithMaxRounds(4),
		agent_v2.WithHooks(&toolHookRemoteHooks{remote: ra}))
	asked, ids, final, err := taskDrive(t, host, "go", true, answerByQuestion)
	if err != nil || final != "final" {
		t.Fatalf("没收敛：final=%q err=%v（问过 %q）", final, err, asked)
	}
	if want := []string{"确认发出 req-start 吗?", "确认发出 req-end 吗?"}; !slices.Equal(asked, want) || ids[0] == ids[1] {
		t.Errorf("问过 %q（ID %q），want %q 且两问 ID 不同", asked, ids, want)
	}
	for _, g := range confirm.snapshot() {
		req := g[:strings.Index(g, "=")]
		if want := req + "=答:确认发出 " + req + " 吗?"; g != want {
			t.Errorf("钩子拿到 %q，want %q——拿到了别的问题的答复（全部：%q）", g, want, confirm.snapshot())
		}
	}
	if n := r.calls.Load(); n != 2 {
		t.Errorf("远端应被调两次，实际 %d 次", n)
	}
}
