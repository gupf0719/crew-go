package a2a_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// 不变量：human gate 的答案只用来续它答复的那个远端任务。远端任务结束之后再跑，发给远端的是原始请求；
// 新任务的追问是新问题——换一个 ID、问人，答案续这个新任务。
//
// 远端任务以 Failed 结束时 RemoteAgent 清掉 taskID、返回可重试的错。之后再跑（续跑重试失败的那一步，或编排
// 按 retryCfg 重试）时，这个位置上答过的那一问属于已经结束的那个任务：曾经照样取出来当成新任务的第一条
// 消息发出去，原始请求丢了（`err == nil`，实测）。

// gateRemote 是进程内假远端：收到「原始请求」的新任务先要人确认（input-required「批准吗」）；续任务时按 onCont
// 的决定结束（返回 Failed 或继续要人），否则完成。got 依次记下远端收到的消息（new: 新任务首条 / cont: 续任务）。
type gateRemote struct {
	mu     sync.Mutex
	got    []string
	conts  atomic.Int32
	onCont func(n int32) a2atypes.TaskState // 第 n 次续任务（从 1 起）怎么结束；返回 Completed 则完成
	// contDelay 是续任务时远端收下答复之后、发出结局之前停多久（其间任务仍停在原来那一问上，contWorking 时先切到 working）
	contDelay   time.Duration
	contWorking bool
	working     atomic.Bool // contWorking 时已经发出 working
}

func (g *gateRemote) record(s string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.got = append(g.got, s)
}

func (g *gateRemote) snapshot() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.got...)
}

func (g *gateRemote) exec(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
	txt := inputText(execCtx)
	if execCtx.StoredTask == nil {
		g.record("new:" + txt)
		if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
			return
		}
		if txt == "原始请求" {
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("批准吗"))), nil)
			return
		}
		// 新任务的第一条消息不是原始请求——人的回答被当成了请求
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("新任务办了:"+txt))), nil)
		return
	}
	g.record("cont:" + txt)
	if g.contWorking {
		if !yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateWorking, nil), nil) {
			return
		}
		g.working.Store(true)
	}
	time.Sleep(g.contDelay)
	state := a2atypes.TaskStateCompleted
	if g.onCont != nil {
		state = g.onCont(g.conts.Add(1))
	}
	switch state {
	case a2atypes.TaskStateFailed:
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateFailed,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("远端出错"))), nil)
	case a2atypes.TaskStateInputRequired:
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("还要再确认一次吗"))), nil)
	default:
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("续任务办了:"+txt))), nil)
	}
}

// gateRun 把开了 human gate 的 RemoteAgent 放进 SequentialAgent（withRetry 时带 SimpleRetry(2)），跑到收敛：
// 停在提问上就把答案编号（答#1、答#2……）累积着传；以普通错停下时不带新答案续跑一次（重试失败的那一步）。
func gateRun(t *testing.T, base string, withRetry bool) (asked []string, ids []string, final string, err error) {
	t.Helper()
	ra, err := a2a.NewRemoteAgent(context.Background(), base, a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	opts := []agent_v2.SequentialOption{agent_v2.WithSequentialName("gate_seq")}
	if withRetry {
		opts = append(opts, agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2)))
	}
	seq := agent_v2.NewSequentialAgent(opts...).AddStep("remote", ra)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	evs, err := seq.Run(ctx, sess, schema.UserMessage("原始请求"))
	for round := 0; round < 6 && err != nil; round++ {
		if ie, ok := compose_v2.IsInterrupt(err); ok {
			for _, q := range ie.AllQueries() {
				asked = append(asked, q.Question)
				ids = append(ids, q.InterruptID)
				answers[q.InterruptID] = schema.UserMessage(fmt.Sprintf("答#%d", len(asked)))
			}
		} else if errors.Is(err, compose_v2.ErrRunAborted) {
			break
		}
		evs, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	}
	if m := agent_v2.FinalMessage(evs); m != nil {
		final = m.Content
	}
	return asked, ids, final, err
}

// checkAskedAgainForNewTask 断言：远端任务失败之后，新任务收到的是原始请求、人被重新问了一次（新 ID），第二次的
// 回答续上新任务。
func checkAskedAgainForNewTask(t *testing.T, r *gateRemote, asked, ids []string, final string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("没收敛：%v（远端收到 %q）", err, r.snapshot())
	}
	if want := []string{"new:原始请求", "cont:答#1", "new:原始请求", "cont:答#2"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q——人的回答被当成了新任务的请求，或新任务用了给旧任务的回答", r.snapshot(), want)
	}
	if len(asked) != 2 || ids[0] == ids[1] {
		t.Errorf("新任务的追问应是新问题（换一个 ID 问人）：问过 %q，ID %q", asked, ids)
	}
	if final != "续任务办了:答#2" {
		t.Errorf("final = %q，want 续任务办了:答#2", final)
	}
}

// TestHumanGate_RemoteFailed_ResumeAsksAgainForNewTask：纯续跑。答过之后远端任务失败，节点以普通错停下，续跑重试
// 那一步。
func TestHumanGate_RemoteFailed_ResumeAsksAgainForNewTask(t *testing.T) {
	r := &gateRemote{onCont: func(n int32) a2atypes.TaskState {
		if n == 1 {
			return a2atypes.TaskStateFailed
		}
		return a2atypes.TaskStateCompleted
	}}
	asked, ids, final, err := gateRun(t, startServer(t, fnExec(r.exec)), false)
	checkAskedAgainForNewTask(t, r, asked, ids, final, err)
}

// TestHumanGate_RemoteFailed_RetryAsksAgainForNewTask：编排按 retryCfg 重试那个成员（同一次调用再做一遍）。
func TestHumanGate_RemoteFailed_RetryAsksAgainForNewTask(t *testing.T) {
	r := &gateRemote{onCont: func(n int32) a2atypes.TaskState {
		if n == 1 {
			return a2atypes.TaskStateFailed
		}
		return a2atypes.TaskStateCompleted
	}}
	asked, ids, final, err := gateRun(t, startServer(t, fnExec(r.exec)), true)
	checkAskedAgainForNewTask(t, r, asked, ids, final, err)
}

// startFlakyContinuationServer 同 startServer，但第一次续任务的请求在到达远端之前就以 503 失败（网络中途断开的
// 形状：远端任务还在等人，本地 taskID 也还在）。failed 报那次失败有没有发生。拦截做在同一个服务上：AgentCard
// 里写的调用地址就是这个服务，客户端绕不过去。
func startFlakyContinuationServer(t *testing.T, exec a2asrv.AgentExecutor) (url string, failed *atomic.Bool) {
	t.Helper()
	mux := http.NewServeMux()
	srv := httptest.NewUnstartedServer(nil)
	url = "http://" + srv.Listener.Addr().String()
	card := &a2atypes.AgentCard{
		Name:        "Flaky Agent",
		Description: "in-process test fixture",
		SupportedInterfaces: []*a2atypes.AgentInterface{
			a2atypes.NewAgentInterface(url+"/invoke", a2atypes.TransportProtocolJSONRPC),
		},
		DefaultInputModes:  []string{"text"},
		DefaultOutputModes: []string{"text"},
		Capabilities:       a2atypes.AgentCapabilities{Streaming: true},
	}
	mux.Handle("/invoke", a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(exec)))
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(card))
	failed = &atomic.Bool{}
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body) // 读失败时 body 为空，照常交给 mux
		req.Body = io.NopCloser(bytes.NewReader(body))
		if req.URL.Path == "/invoke" && bytes.Contains(body, []byte(`"taskId"`)) && failed.CompareAndSwap(false, true) {
			http.Error(w, "upstream unavailable", http.StatusServiceUnavailable)
			return
		}
		mux.ServeHTTP(w, req)
	})
	srv.Start()
	t.Cleanup(srv.Close)
	return url, failed
}

// TestHumanGate_SendFailed_RetryContinuesSameTask：答过之后续任务的请求在网络层失败，远端任务还在等人、本地
// taskID 也还在。重试拿回同一个答案续同一个远端任务，不重问。
func TestHumanGate_SendFailed_RetryContinuesSameTask(t *testing.T) {
	r := &gateRemote{}
	url, failed := startFlakyContinuationServer(t, fnExec(r.exec))
	asked, _, final, err := gateRun(t, url, true)
	if !failed.Load() {
		t.Fatal("fixture 没搭对：续任务的请求一次都没失败过")
	}
	if err != nil {
		t.Fatalf("没收敛：%v（远端收到 %q）", err, r.snapshot())
	}
	if want := []string{"new:原始请求", "cont:答#1"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q", r.snapshot(), want)
	}
	if len(asked) != 1 || final != "续任务办了:答#1" {
		t.Errorf("应只问一次、续上同一个远端任务：问过 %q，final=%q", asked, final)
	}
}

// TestHumanGate_SecondQuestionInSameTask_AskedAsNewQuestion：同一个远端任务答完一问之后又追问一次。第二问是新
// 问题：换一个 ID 问人，答案续同一个任务。只按「位置 + 任务」定身份时，第二问跟第一问同一个 ID、落在已兑付的
// 位上，整个任务终局失败。
func TestHumanGate_SecondQuestionInSameTask_AskedAsNewQuestion(t *testing.T) {
	r := &gateRemote{onCont: func(n int32) a2atypes.TaskState {
		if n == 1 {
			return a2atypes.TaskStateInputRequired
		}
		return a2atypes.TaskStateCompleted
	}}
	asked, ids, final, err := gateRun(t, startServer(t, fnExec(r.exec)), false)
	if err != nil {
		t.Fatalf("没收敛：%v（远端收到 %q）", err, r.snapshot())
	}
	if want := []string{"new:原始请求", "cont:答#1", "cont:答#2"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q", r.snapshot(), want)
	}
	if want := []string{"批准吗", "还要再确认一次吗"}; !slices.Equal(asked, want) || ids[0] == ids[1] {
		t.Errorf("第二问应是新问题：问过 %q，ID %q", asked, ids)
	}
	if final != "续任务办了:答#2" {
		t.Errorf("final = %q，want 续任务办了:答#2", final)
	}
}

// taskRemote 是按任务记账的假远端：新任务首条以 req 开头时要人（「批准吗:<请求>」），否则直接完成（人的回答
// 被当成了请求）；续任务时完成，记下 cont[<这个任务的原始请求>]:<收到的>。
type taskRemote struct {
	mu   sync.Mutex
	got  []string
	orig map[string]string
}

func (p *taskRemote) exec(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
	txt := inputText(execCtx)
	p.mu.Lock()
	if p.orig == nil {
		p.orig = map[string]string{}
	}
	if execCtx.StoredTask == nil {
		p.got = append(p.got, "new:"+txt)
		ask := strings.HasPrefix(txt, "req")
		if ask {
			p.orig[string(execCtx.TaskID)] = txt
		}
		p.mu.Unlock()
		if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
			return
		}
		if ask {
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("批准吗:"+txt))), nil)
			return
		}
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("新任务办了:"+txt))), nil)
		return
	}
	o := p.orig[string(execCtx.TaskID)]
	p.got = append(p.got, "cont["+o+"]:"+txt)
	p.mu.Unlock()
	yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
		a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("续任务["+o+"]办了:"+txt))), nil)
}

func (p *taskRemote) snapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.got...)
}

// check 断言不变量：远端每次续任务收到的都是对那个任务那一问的答复（「答:批准吗:<该任务的请求>」），没有新任务
// 以别的东西开头。
func (p *taskRemote) check(t *testing.T) {
	t.Helper()
	for _, g := range p.snapshot() {
		switch {
		case strings.HasPrefix(g, "cont["):
			o := g[len("cont["):strings.Index(g, "]")]
			if want := "cont[" + o + "]:答:批准吗:" + o; g != want {
				t.Errorf("远端任务 %s 收到 %q——不是对它那一问的答复（远端收到 %q）", o, g, p.snapshot())
			}
		case strings.HasPrefix(g, "new:") && !strings.HasPrefix(g, "new:req"):
			t.Errorf("新任务以 %q 开头——人的回答被当成了请求（远端收到 %q）", g, p.snapshot())
		}
	}
}

// taskDrive 跑到收敛：停在提问上就作答（answer 返回 false 时这一问不答；返回的消息为 nil 时给 nil 答案），
// 答案累积着传。withSession=false 时续跑不带 WithResumeSession。
func taskDrive(t *testing.T, ag agent_v2.Agent, in string, withSession bool, answer func(q *compose_v2.AskQuery) (*schema.Message, bool)) (asked, ids []string, final string, err error) {
	t.Helper()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	evs, err := ag.Run(ctx, sess, schema.UserMessage(in))
	for round := 0; round < 6; round++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break
		}
		for _, q := range ie.AllQueries() {
			asked = append(asked, q.Question)
			ids = append(ids, q.InterruptID)
			if a, ok := answer(q); ok {
				answers[q.InterruptID] = a
			}
		}
		ropts := []agent_v2.ResumeOption{agent_v2.WithResumeUserInputs(answers)}
		if withSession {
			ropts = append(ropts, agent_v2.WithResumeSession(sess))
		}
		evs, err = ag.Resume(ctx, sess.RunID, ropts...)
	}
	if m := agent_v2.FinalMessage(evs); m != nil {
		final = m.Content
	}
	return asked, ids, final, err
}

func answerByQuestion(q *compose_v2.AskQuery) (*schema.Message, bool) {
	return schema.UserMessage("答:" + q.Question), true
}

// TestHumanGate_NilAnswer_ReasksWithoutContactingRemote：续跑时给那一问的答案是 nil（文档约定「当没答案、照常再
// 问」）。不联系远端、同一个 ID 再问一次；之后答了才续任务。
//
// 曾经把这次调用的原始输入当成答复发出去，远端照样续上、做完，`err == nil`（实测）。
func TestHumanGate_NilAnswer_ReasksWithoutContactingRemote(t *testing.T) {
	r := &taskRemote{}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, fnExec(r.exec)), a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("gate_nil")).AddStep("remote", ra)
	n := 0
	asked, ids, final, err := taskDrive(t, seq, "req-A", true, func(q *compose_v2.AskQuery) (*schema.Message, bool) {
		n++
		if n == 1 {
			return nil, true
		}
		return answerByQuestion(q)
	})
	if err != nil {
		t.Fatalf("没收敛：%v（远端收到 %q）", err, r.snapshot())
	}
	r.check(t)
	if len(asked) != 2 || ids[0] != ids[1] {
		t.Errorf("没答的那一问应原样再问（同一个 ID）：问过 %q，ID %q", asked, ids)
	}
	if want := []string{"new:req-A", "cont[req-A]:答:批准吗:req-A"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q", r.snapshot(), want)
	}
	if final != "续任务[req-A]办了:答:批准吗:req-A" {
		t.Errorf("final = %q", final)
	}
}

// TestHumanGate_ParallelGates_UnansweredBranchNotContinued：ParallelAgent 两个分支各是一个开了 gate 的 RemoteAgent，
// 两边都要人，第一次续跑只答了一个。没答的那个分支不联系远端、原样再问；答了之后各自用自己的答案续自己的任务。
func TestHumanGate_ParallelGates_UnansweredBranchNotContinued(t *testing.T) {
	r := &taskRemote{}
	base := startServer(t, fnExec(r.exec))
	r1, err := a2a.NewRemoteAgent(context.Background(), base, a2a.WithHumanGate(), a2a.WithName("r1"))
	if err != nil {
		t.Fatal(err)
	}
	r2, err := a2a.NewRemoteAgent(context.Background(), base, a2a.WithHumanGate(), a2a.WithName("r2"))
	if err != nil {
		t.Fatal(err)
	}
	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("gate_par")).AddBranch("b1", r1).AddBranch("b2", r2)
	n := 0
	asked, _, final, err := taskDrive(t, par, "req-X", true, func(q *compose_v2.AskQuery) (*schema.Message, bool) {
		n++
		if n == 2 {
			return nil, false // 第一轮只答一个
		}
		return answerByQuestion(q)
	})
	if err != nil {
		t.Fatalf("没收敛：%v（远端收到 %q）", err, r.snapshot())
	}
	r.check(t)
	if len(asked) != 3 {
		t.Errorf("没答的那一问应再问一次（共 3 问），实际 %q", asked)
	}
	if !strings.Contains(final, "续任务[req-X]办了:答:批准吗:req-X") || strings.Count(final, "续任务[req-X]办了:答:批准吗:req-X") != 2 {
		t.Errorf("两个分支都应用自己的答案续上自己的任务：final=%q", final)
	}
}

// TestHumanGate_BareDoubleCallInNode_EachTaskGetsItsOwnAnswer：节点里先后裸调同一个 RemoteAgent 两次。两次调用
// 各有各的待续任务，谁也不续谁的。（裸调没有完成记录，节点重跑时前一次调用会再执行一遍、新开任务再问——那是
// 裸调的通用语义，要跳过已完成的调用经 agent_v2.RunSubAgent 调。）
//
// 曾经两次调用共用 session 上按 RunID 存的一个任务槽：第一次调用续掉第二次调用挂着的任务，人的答复一次都没送到，
// 第二次调用的任务一轮轮被执行、永不收敛（实测）。
func TestHumanGate_BareDoubleCallInNode_EachTaskGetsItsOwnAnswer(t *testing.T) {
	r := &taskRemote{}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, fnExec(r.exec)), a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	host := rsHost(t, func(ctx context.Context, _ string) (string, error) {
		var outs []string
		for _, in := range []string{"req-A", "req-B"} {
			evs, err := ra.Run(ctx, agent_v2.GetSession(ctx), schema.UserMessage(in))
			if err != nil {
				return "", err
			}
			outs = append(outs, agent_v2.FinalMessage(evs).Content)
		}
		return strings.Join(outs, "|"), nil
	})
	_, _, final, err := taskDrive(t, host, "go", true, answerByQuestion)
	if err != nil {
		t.Fatalf("没收敛：%v（远端收到 %q）", err, r.snapshot())
	}
	r.check(t)
	if final != "续任务[req-A]办了:答:批准吗:req-A|续任务[req-B]办了:答:批准吗:req-B" {
		t.Errorf("final = %q", final)
	}
}

// twoCallModel：第一次回两个并行 tool_call（同一个远端工具，各带不同请求），看到工具结果后收尾。
type twoCallModel struct {
	mu sync.Mutex
}

func (m *twoCallModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var results []string
	for _, mm := range msgs {
		if mm != nil && mm.Role == schema.RoleTool {
			results = append(results, mm.Content)
		}
	}
	if len(results) > 0 {
		slices.Sort(results)
		return schema.AssistantMessage("final:" + strings.Join(results, "|")), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "c1", Type: "function", Function: schema.FunctionCall{Name: "remote", Arguments: `{"input":"req-A"}`}},
		{ID: "c2", Type: "function", Function: schema.FunctionCall{Name: "remote", Arguments: `{"input":"req-B"}`}},
	}), nil
}
func (m *twoCallModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *twoCallModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestHumanGate_SameBatchToolCalls_EachTaskGetsItsOwnAnswer：模型同一轮发两个 tool_call 调同一个远端工具，两边都要人。
// 各自用自己的答案续自己的任务。曾经两个 tool_call 抢 session 上那一个任务槽：一个续了另一个的任务，另一个撞上
// 「任务已在执行」或线程不匹配（实测）。
func TestHumanGate_SameBatchToolCalls_EachTaskGetsItsOwnAnswer(t *testing.T) {
	r := &taskRemote{}
	rt, err := a2a.NewRemoteAgentTool(context.Background(), startServer(t, fnExec(r.exec)), a2a.WithHumanGate(), a2a.WithName("remote"))
	if err != nil {
		t.Fatal(err)
	}
	tk := tool.NewToolkit()
	tk.Register(rt)
	host := agent_v2.NewBasicAgent(agent_v2.WithName("host"), agent_v2.WithDescription("h"),
		agent_v2.WithToolkit(tk), agent_v2.WithModel(&twoCallModel{}), agent_v2.WithMaxRounds(4))
	asked, _, final, err := taskDrive(t, host, "go", true, answerByQuestion)
	if err != nil {
		t.Fatalf("没收敛：%v（远端收到 %q）", err, r.snapshot())
	}
	r.check(t)
	if len(asked) != 2 || final != "final:续任务[req-A]办了:答:批准吗:req-A|续任务[req-B]办了:答:批准吗:req-B" {
		t.Errorf("问过 %q，final=%q", asked, final)
	}
}

// TestHumanGate_ResumeWithoutSession_Converges：续跑不带 WithResumeSession（新的空 session）。待续任务随宿主的存档
// 走，照样续上同一个远端任务。
//
// 曾经任务句柄只在 session 上：不带原 session 时先是把人的回答当成新任务的请求发出去，后来是每轮新开任务、换 ID
// 重问、永不收敛（都实测）。
func TestHumanGate_ResumeWithoutSession_Converges(t *testing.T) {
	r := &taskRemote{}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, fnExec(r.exec)), a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("gate_nosess")).AddStep("remote", ra)
	asked, _, final, err := taskDrive(t, seq, "req-A", false, answerByQuestion)
	if err != nil {
		t.Fatalf("没收敛：%v（远端收到 %q）", err, r.snapshot())
	}
	r.check(t)
	if len(asked) != 1 || final != "续任务[req-A]办了:答:批准吗:req-A" {
		t.Errorf("问过 %q，final=%q", asked, final)
	}
}

// TestHumanGate_ResumeFromFreshInstances_Converges：模拟宿主进程重启——续跑用的是新建的 RemoteAgent 与宿主实例、新的
// 空 session，只有存档是同一份。等人答复的那一问随存档走，照样续上同一个远端任务。
func TestHumanGate_ResumeFromFreshInstances_Converges(t *testing.T) {
	r := &taskRemote{}
	base := startServer(t, fnExec(r.exec))
	build := func() agent_v2.Agent {
		ra, err := a2a.NewRemoteAgent(context.Background(), base, a2a.WithHumanGate())
		if err != nil {
			t.Fatal(err)
		}
		return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("gate_restart")).AddStep("remote", ra)
	}
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err := build().Run(ctx, sess, schema.UserMessage("req-A"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("远端要人，宿主应挂起：%v", err)
	}
	q := ie.AllQueries()[0]
	evs, err := build().Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(map[string]*schema.Message{q.InterruptID: schema.UserMessage("答:" + q.Question)}))
	if err != nil {
		t.Fatalf("续跑：%v（远端收到 %q）", err, r.snapshot())
	}
	r.check(t)
	if f := agent_v2.FinalMessage(evs); f == nil || f.Content != "续任务[req-A]办了:答:批准吗:req-A" {
		t.Errorf("应续上同一个远端任务：final=%v（远端收到 %q）", f, r.snapshot())
	}
}

// TestHumanGate_TopLevel_SecondRunWithAnswerContinuesTask：不在任何节点执行里直接调开了 gate 的 RemoteAgent。那里没有
// 调用位置可记、也没有存档接得住中断（中断的 ID 为空）；业务拿同一个 session 再 Run 一次、带上人的答复时，续的是
// 刚才那个远端任务。
//
// 等人答复的那一问只按调用位置记、不再读 session 上的句柄时，顶层这样再 Run 一次会新开一个任务、把人的答复当成
// 请求发出去，刚才那个任务一直挂着（复审实测）。
func TestHumanGate_TopLevel_SecondRunWithAnswerContinuesTask(t *testing.T) {
	r := &taskRemote{}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, fnExec(r.exec)), a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sess := agent_v2.NewRunSession()
	_, err = ra.Run(ctx, sess, schema.UserMessage("req-A"))
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("远端要人，顶层应拿到中断：%v", err)
	}
	evs, err := ra.Run(ctx, sess, schema.UserMessage("答:批准吗:req-A"))
	if err != nil {
		t.Fatalf("再 Run 一次：%v（远端收到 %q）", err, r.snapshot())
	}
	r.check(t)
	if f := agent_v2.FinalMessage(evs); f == nil || f.Content != "续任务[req-A]办了:答:批准吗:req-A" {
		t.Errorf("应续上刚才那个远端任务：final=%v（远端收到 %q）", f, r.snapshot())
	}
}

// sessionHandleNextRequest 用同一个 session 依次 Run：req-A（远端要人）→ 人的答复（续上、完成）→ req-B。
// 返回远端收到的消息。
func sessionHandleNextRequest(t *testing.T, opts ...a2a.RemoteOption) []string {
	t.Helper()
	r := &taskRemote{}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, fnExec(r.exec)), opts...)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sess := agent_v2.NewRunSession()
	for _, in := range []string{"req-A", "答:批准吗:req-A", "req-B"} {
		if _, err := ra.Run(ctx, sess, schema.UserMessage(in)); err != nil {
			if _, ok := compose_v2.IsInterrupt(err); !ok {
				t.Fatalf("Run(%q)：%v（远端收到 %q）", in, err, r.snapshot())
			}
		}
	}
	r.check(t)
	return r.snapshot()
}

// TestRemoteAgent_SessionHandle_ClearedAfterCompletion：句柄记在 session 上的两种情形（默认模式；开了 gate 但在顶层
// 直接调），远端任务完成之后句柄清掉——下一个请求新开任务，不去续已经结束的那个。
func TestRemoteAgent_SessionHandle_ClearedAfterCompletion(t *testing.T) {
	want := []string{"new:req-A", "cont[req-A]:答:批准吗:req-A", "new:req-B"}
	if got := sessionHandleNextRequest(t); !slices.Equal(got, want) {
		t.Errorf("默认模式：远端收到 %q，want %q", got, want)
	}
	if got := sessionHandleNextRequest(t, a2a.WithHumanGate()); !slices.Equal(got, want) {
		t.Errorf("顶层 gate：远端收到 %q，want %q", got, want)
	}
}

// gateEndHooks 在每个节点的 OnNodeEnd 里调 onEnd（OnNodeEnd 的返回值框架不消费）。
type gateEndHooks struct {
	agent_v2.NoOpOrchestrationHooks
	onEnd func(ctx context.Context, nodeID string)
}

func (h gateEndHooks) OnNodeEnd(ctx context.Context, nodeID string, _ error) context.Context {
	h.onEnd(ctx, nodeID)
	return ctx
}

// gateTwoNodeHost 是 GraphAgent：n1 → n2，节点体把输入原样交出去。
func gateTwoNodeHost(t *testing.T) agent_v2.Agent {
	t.Helper()
	b := compose_v2.NewBuilder().WithName("gate_two")
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	pass := func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("done:"+in.Content))}, nil
	}
	b.AddNode("n1", pass)
	b.AddNode("n2", pass)
	b.AddEdge(compose_v2.StartNode, "n1")
	b.AddEdge("n1", "n2")
	b.AddEdge("n2", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("gate_two_host"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
}

// TestHumanGate_InEngineHook_NoCrossCallContinuation：两个节点的 OnNodeEnd 各调一次同一个开了 gate 的 RemoteAgent，
// 远端都要人。引擎钩子里没有提问位置、抛的中断接不住：每次调用各开各的任务，远端要人时返回错误，谁也不续谁的。
//
// 这里按顶层处理（用 session 上的句柄）时，n1 那次的中断被丢掉，n2 的请求被当成人对 n1 那一问的答复送进 n1 的
// 任务，远端照着执行，`err == nil`（复审实测）。
func TestHumanGate_InEngineHook_NoCrossCallContinuation(t *testing.T) {
	r := &taskRemote{}
	ra, err := a2a.NewRemoteAgent(context.Background(), startServer(t, fnExec(r.exec)), a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	callErr := map[string]error{}
	hooks := gateEndHooks{onEnd: func(ctx context.Context, nodeID string) {
		_, err := ra.Run(ctx, agent_v2.GetSession(ctx), schema.UserMessage("req-"+nodeID))
		mu.Lock()
		callErr[nodeID] = err
		mu.Unlock()
	}}
	ctx := agent_v2.WithCtxOrchHooks(compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer()), hooks)
	if _, err := gateTwoNodeHost(t).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("宿主：%v", err)
	}
	r.check(t)
	if want := []string{"new:req-n1", "new:req-n2"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q——一次调用续了另一次调用的任务", r.snapshot(), want)
	}
	for _, id := range []string{"n1", "n2"} {
		err := callErr[id]
		if _, isInt := compose_v2.IsInterrupt(err); err == nil || isInt || !errors.Is(err, compose_v2.ErrRunAborted) {
			t.Errorf("%s 的钩子里远端要人时应返回终局错误（不是接不住的中断），got %v", id, err)
		}
	}
}

// gateStartHooks 在节点的 OnNodeStart 里调 fn，把它的错原样交给框架。
type gateStartHooks struct {
	agent_v2.NoOpOrchestrationHooks
	fn func(ctx context.Context, nodeID string) error
}

func (h gateStartHooks) OnNodeStart(ctx context.Context, nodeID string) (context.Context, error) {
	return ctx, h.fn(ctx, nodeID)
}

// TestHumanGate_InEngineHook_ErrorIsTerminalNotRetried：OnNodeStart 里调开了 gate 的 RemoteAgent、远端要人，钩子把错
// 原样交给框架，外面的编排配了重试。调用放错了位置，重跑必然一样：这个错是终局失败（不重试、不留可续的存档），
// 远端只被调一次。
//
// 曾经返回普通错误：被判成可重试，重试两次就多开两个停在要人上、没人去续的远端任务，留下的失败存档每次续跑再开
// 一个（复核实测）。
func TestHumanGate_InEngineHook_ErrorIsTerminalNotRetried(t *testing.T) {
	r := &taskRemote{}
	gate, err := a2a.NewRemoteAgent(context.Background(), startServer(t, fnExec(r.exec)), a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	hooks := gateStartHooks{fn: func(ctx context.Context, nodeID string) error {
		if nodeID != "n" { // 只在 rsHost 的节点上调，不在 Sequential 自己的步骤节点上调
			return nil
		}
		_, err := gate.Run(ctx, agent_v2.GetSession(ctx), schema.UserMessage("req-hook"))
		return err
	}}
	inner := rsHost(t, func(context.Context, string) (string, error) { return "body-ok", nil })
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("gate_hook_retry"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).AddStep("inner", inner)
	ctx := agent_v2.WithCtxOrchHooks(compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer()), hooks)
	_, err = seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
	if !errors.Is(err, compose_v2.ErrRunAborted) || compose_v2.IsRetryable(err) {
		t.Errorf("放错位置的调用应是终局失败、不可重试，got %v（ErrRunAborted=%v IsRetryable=%v）",
			err, errors.Is(err, compose_v2.ErrRunAborted), compose_v2.IsRetryable(err))
	}
	if want := []string{"new:req-hook"}; !slices.Equal(r.snapshot(), want) {
		t.Errorf("远端收到 %q，want %q——被重试成了多个远端任务", r.snapshot(), want)
	}
}
