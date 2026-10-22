package agent_v2_test

// 「这个任务里记过没有」在任务交错时仍然判得对，读的量只跟命中的记录有关。
//
// 形状：任务挂着（中断 / 审批），同一会话里接着跑了 k 个别的任务（每个一问一答，2 条记录），再回来续跑
// 或放弃。去重按消息 ID 与任务章直接问后端（见 history_read.go 的 findHistory）。这里的每条用例都打红过
// 一版旧做法：「往前连续看到 64 条别的任务的记录就停」在 k=32、80 时提前停；「任务的起点章 + session 上的
// 标记」在拿不到原 session 对象的入口（无状态服务按 RunID 重建 session、session 传 nil）退回猜、猜错——
// 续跑时同一条输入 / 产出多记一份，放弃时悬空 tool_call 漏补。另一条：节点里经 DetachTask 开的独立任务
// 推回同一会话、它的产出被父图透传时，跨归属的那道去重要看得见独立任务写的那一份。

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

func spanLoadAll(t *testing.T, ctx context.Context, hs agent_v2.HistoryStore, key agent_v2.HistoryKey) []*schema.Message {
	t.Helper()
	msgs, err := agent_v2.LoadAllHistory(ctx, hs, key)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return msgs
}

func spanTaskOf(m *schema.Message) string {
	s, _ := m.Annotations[agent_v2.HistoryTaskAnnotation].(string)
	return s
}

// spanForeignTurns 在同一会话（同 Owner + ConvID）上跑 k 个**别的任务**：每个任务一问一答
// （2 条记录）——模拟「任务 A 挂着审批，用户在同一个会话里接着聊了 k 轮」。
func spanForeignTurns(t *testing.T, ctx context.Context, hs agent_v2.HistoryStore, owner string, k int) {
	t.Helper()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("chat"),
		agent_v2.WithModel(newStubModel(nil)),
		agent_v2.WithHistory(hs, owner),
		agent_v2.WithMaxRounds(2))
	for i := 0; i < k; i++ {
		if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(fmt.Sprintf("闲聊 %d", i))); err != nil {
			t.Fatalf("foreign turn %d: %v", i, err)
		}
	}
}

func spanCount(msgs []*schema.Message, sub string) int {
	n := 0
	for _, m := range msgs {
		if sub == "我的原话" {
			if m.Role == schema.RoleUser && m.Content == sub {
				n++
			}
			continue
		}
		if strings.Contains(m.Content, sub) {
			n++
		}
	}
	return n
}

func spanDump(t *testing.T, msgs []*schema.Message) {
	t.Helper()
	for i, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		if strings.HasPrefix(m.Content, "闲聊") || strings.Contains(m.Content, "stub exhausted") {
			continue
		}
		t.Logf("  [%d] %s owner=%s task=%s tc=%d trid=%s %q", i, m.Role, o, spanTaskOf(m),
			len(schema.GetToolCalls(m)), schema.GetToolResultID(m), m.Content)
	}
}

// spanStampsAll 断言任务 A 的每条记录都盖着 A 的 RunID（排除 foreign 闲聊）。
func spanStampsAll(t *testing.T, msgs []*schema.Message, runID string) {
	t.Helper()
	for i, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		if o == "chat" {
			continue
		}
		if got := spanTaskOf(m); got != runID {
			t.Errorf("记录 [%d] owner=%s 任务章 %q，want %q（%q）", i, o, got, runID, m.Content)
		}
	}
}

// P1：Sequential + 外部执行体中断（边界 per-slot 输入去重 inputAlreadyInHistorySlot 承重）
func TestHistoryTaskSpan_SeqExternalPause_OtherTasksInBetween(t *testing.T) {
	for _, k := range []int{0, 32, 80} {
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			ext := &pausingExternalAgent{name: "extPause", qid: "seq_pq"}
			seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seqP"))
			seq.AddStep("theStep", ext)
			conv := fmt.Sprintf("c_p1_%d", k)
			ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
			ctx = compose_v2.WithThreadIDCtx(ctx, conv)
			ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			if _, err := seq.Run(ctx, sess, schema.UserMessage("我的原话")); err == nil {
				t.Fatal("Run 应中断")
			}
			spanForeignTurns(t, ctx, hs, "t", k)
			if _, err := seq.Resume(ctx, sess.RunID); err != nil {
				t.Fatalf("Resume: %v", err)
			}
			msgs := spanLoadAll(t, ctx, hs, agent_v2.HistoryKey{Owner: "t", ConvID: conv})
			n := spanCount(msgs, "我的原话")
			t.Logf("k=%d（中间 %d 条别的任务的记录）：用户原话 %d 份，外部产出 %d 份", k, 2*k, n, spanCount(msgs, "【外部产出】"))
			spanStampsAll(t, msgs, sess.RunID)
			if n != 1 {
				t.Errorf("用户原话 %d 份, want 1", n)
				spanDump(t, msgs)
			}
		})
	}
}

// P2：Handoff 共享单流 + 外部第一跳中断（any-stream 输入去重 inputAlreadyInAnyStream 承重）
func TestHistoryTaskSpan_HandoffExternalFirstHop_OtherTasksInBetween(t *testing.T) {
	for _, k := range []int{0, 32, 80} {
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			ext := &pausingExternalAgent{name: "extHop", qid: "ho_pq"}
			ho := agent_v2.NewHandoffAgent(
				agent_v2.WithHandoffName("hoP"),
				agent_v2.WithHandoffInitial("front"),
			).AddAgent("front", ext)
			conv := fmt.Sprintf("c_p2_%d", k)
			ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
			ctx = compose_v2.WithThreadIDCtx(ctx, conv)
			ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			if _, err := ho.Run(ctx, sess, schema.UserMessage("我的原话")); err == nil {
				t.Fatal("Run 应中断")
			}
			spanForeignTurns(t, ctx, hs, "t", k)
			if _, err := ho.Resume(ctx, sess.RunID); err != nil {
				t.Fatalf("Resume: %v", err)
			}
			msgs := spanLoadAll(t, ctx, hs, agent_v2.HistoryKey{Owner: "t", ConvID: conv})
			n := spanCount(msgs, "我的原话")
			t.Logf("k=%d（中间 %d 条别的任务的记录）：用户原话 %d 份", k, 2*k, n)
			spanStampsAll(t, msgs, sess.RunID)
			if n != 1 {
				t.Errorf("共享单流里用户原话 %d 份, want 1", n)
				spanDump(t, msgs)
			}
		})
	}
}

// P3：GraphAgent 透传子产出指针 + 中断跨 Resume（finalAlreadyInOtherSlot 承重）
func TestHistoryTaskSpan_GraphPassthrough_OtherTasksInBetween(t *testing.T) {
	for _, k := range []int{0, 32, 80} {
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			inCh := compose_v2.LastValue[*schema.Message]("__span_in__")
			outCh := compose_v2.LastValue[*schema.Message]("__span_out__")
			gateCh := compose_v2.LastValue[*schema.Message]("__span_gate__")
			leaf := agent_v2.NewBasicAgent(
				agent_v2.WithName("leaf"),
				agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("【子产出】")})),
				agent_v2.WithMaxRounds(2))
			var asked bool
			bld := compose_v2.NewBuilder().WithName("span_pt")
			compose_v2.WithChannel(bld, inCh)
			compose_v2.WithChannel(bld, outCh)
			compose_v2.WithChannel(bld, gateCh)
			bld.AddNode("n1", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				in, _ := compose_v2.Get(chs, inCh)
				ev, err := agent_v2.RunSubAgent(ctx, "n1", leaf, in)
				if err != nil {
					return nil, err
				}
				return []compose_v2.ChannelWrite{
					compose_v2.Write(outCh, agent_v2.FinalMessage(ev)),
					compose_v2.Write(gateCh, schema.AssistantMessage("go")),
				}, nil
			})
			bld.AddNode("n2", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				if !asked {
					asked = true
					return nil, &compose_v2.InterruptError{Reason: "approve",
						Query: &compose_v2.AskQuery{InterruptID: "span_q", Question: "ok?"}}
				}
				return nil, nil
			})
			bld.AddEdge(compose_v2.StartNode, "n1")
			bld.AddEdge("n1", "n2")
			bld.AddEdge("n2", compose_v2.EndNode)
			g, err := bld.Compile()
			if err != nil {
				t.Fatalf("Compile: %v", err)
			}
			ga := agent_v2.NewGraphAgent(g,
				agent_v2.WithGraphAgentName("ptGraph"),
				agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
					return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
				}),
				agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
					out, _ := compose_v2.Get(chs, outCh)
					return out
				}))
			conv := fmt.Sprintf("c_p3_%d", k)
			ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
			ctx = compose_v2.WithThreadIDCtx(ctx, conv)
			ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			if _, err := ga.Run(ctx, sess, schema.UserMessage("我的原话")); err == nil {
				t.Fatal("Run 应中断")
			}
			spanForeignTurns(t, ctx, hs, "t", k)
			if _, err := ga.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(
				map[string]*schema.Message{"span_q": schema.UserMessage("ok")})); err != nil {
				t.Fatalf("Resume: %v", err)
			}
			msgs := spanLoadAll(t, ctx, hs, agent_v2.HistoryKey{Owner: "t", ConvID: conv})
			n := spanCount(msgs, "【子产出】")
			t.Logf("k=%d（中间 %d 条别的任务的记录）：透传子产出 %d 份，原话 %d 份", k, 2*k, n, spanCount(msgs, "我的原话"))
			spanStampsAll(t, msgs, sess.RunID)
			if n != 1 {
				t.Errorf("透传子产出 %d 份, want 1", n)
				spanDump(t, msgs)
			}
		})
	}
}

// P4：放弃任务时补悬空 tool_call（closeUnansweredToolCallsOnAbort 按任务扫）
func TestHistoryTaskSpan_AbortCloser_OtherTasksInBetween(t *testing.T) {
	for _, k := range []int{0, 32, 80} {
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			rt := &recordTool{name: "do_thing"}
			tk := agent_v2_newToolkit()
			tk.Register(rt)
			ag := agent_v2.NewBasicAgent(
				agent_v2.WithName("worker"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(4),
				agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
				agent_v2.WithHistory(hs, "tenant1"),
				agent_v2.WithModel(newStubModel([]*schema.Message{
					oneToolCall("do_thing"), schema.AssistantMessage("final")})))
			conv := fmt.Sprintf("c_p4_%d", k)
			ctx := compose_v2.WithCheckpointerCtx(
				compose_v2.WithThreadIDCtx(context.Background(), conv), compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			_, err := ag.Run(ctx, sess, schema.UserMessage("hi"))
			if _, ok := compose_v2.IsInterrupt(err); !ok {
				t.Fatalf("该抛审批中断，got %v", err)
			}
			spanForeignTurns(t, ctx, hs, "tenant1", k)
			if _, aerr := ag.Resume(ctx, sess.RunID,
				agent_v2.WithResumeAction(agent_v2.ResumeActionAbort)); aerr != nil {
				t.Fatalf("Abort: %v", aerr)
			}
			after := spanLoadAll(t, ctx, hs, agent_v2.HistoryKey{Owner: "tenant1", ConvID: conv})
			d := danglingCount(after)
			t.Logf("k=%d（中间 %d 条别的任务的记录）：Abort 后悬空 tool_call %d 个", k, 2*k, d)
			spanStampsAll(t, after, sess.RunID)
			if d != 0 {
				t.Errorf("Abort 后仍有 %d 个悬空 tool_call", d)
				spanDump(t, after)
			}
		})
	}
}

// S4：节点里 DetachTask 开独立任务、推回同一个 ConvID：独立任务的章是它自己的 RunID
func TestHistoryTaskSpan_DetachTaskSameConv_PassthroughOnce(t *testing.T) {
	detachSameConvPassthroughOnce(t, agent_v2.NewInMemoryHistoryStore())
}

// TestHistoryTaskSpan_DetachTaskSameConv_PassthroughOnce_StringOnlyBackend：同上，后端只保得住字符串标注（把别的类型
// 转成文本存，像 map[string]string 列、Redis hash）——这样的后端照样过全部 conformance。来源章若存成 []string，读回来
// 是一个文本、认不出，父图透传出去的独立任务答复会再记一份（审计实测）。
func TestHistoryTaskSpan_DetachTaskSameConv_PassthroughOnce_StringOnlyBackend(t *testing.T) {
	detachSameConvPassthroughOnce(t, &stringAnnotationStore{inner: agent_v2.NewInMemoryHistoryStore()})
}

// stringAnnotationStore 把非字符串的标注值转成文本再存。
type stringAnnotationStore struct {
	inner *agent_v2.InMemoryHistoryStore
}

func (s *stringAnnotationStore) Append(ctx context.Context, key agent_v2.HistoryKey, msgs []*schema.Message) error {
	out := make([]*schema.Message, 0, len(msgs))
	for _, m := range msgs {
		if m == nil || m.Annotations == nil {
			out = append(out, m)
			continue
		}
		cp := *m
		cp.Annotations = make(map[string]any, len(m.Annotations))
		for k, v := range m.Annotations {
			if sv, ok := v.(string); ok {
				cp.Annotations[k] = sv
			} else {
				cp.Annotations[k] = fmt.Sprint(v)
			}
		}
		out = append(out, &cp)
	}
	return s.inner.Append(ctx, key, out)
}

func (s *stringAnnotationStore) Load(ctx context.Context, key agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	return s.inner.Load(ctx, key, q)
}

func detachSameConvPassthroughOnce(t *testing.T, hs agent_v2.HistoryStore) {
	t.Helper()
	inCh := compose_v2.LastValue[*schema.Message]("__span4_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__span4_out__")
	indep := agent_v2.NewBasicAgent(agent_v2.WithName("indep"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("【独立任务答复】")})),
		agent_v2.WithHistory(hs, "t"), agent_v2.WithMaxRounds(2))
	indepSess := agent_v2.NewRunSession()
	bld := compose_v2.NewBuilder().WithName("span4g")
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("n1", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		dctx := agent_v2.DetachTask(ctx)
		dctx = compose_v2.WithThreadIDCtx(dctx, "c_s4")
		ev, err := indep.Run(dctx, indepSess, schema.UserMessage("独立任务的问题"))
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, agent_v2.FinalMessage(ev))}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "n1")
	bld.AddEdge("n1", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ga := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("s4"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
	ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_s4")
	sess := agent_v2.NewRunSession()
	if _, err := ga.Run(ctx, sess, schema.UserMessage("我的原话")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	msgs := spanLoadAll(t, ctx, hs, agent_v2.HistoryKey{Owner: "t", ConvID: "c_s4"})
	spanDump(t, msgs)
	if n := spanCount(msgs, "【独立任务答复】"); n != 1 {
		t.Errorf("独立任务的产出在同一会话里 %d 份, want 1（同一条消息两个归属）", n)
	}
	for _, m := range msgs {
		o, _ := m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		want := sess.RunID
		if o == "indep" {
			want = indepSess.RunID
		}
		if got := spanTaskOf(m); got != want {
			t.Errorf("owner=%s %q 任务章 %q, want %q", o, m.Content, got, want)
		}
	}
}

// countLoadStore 数读回了多少条。
type countLoadStore struct {
	*agent_v2.InMemoryHistoryStore
	n int
}

func (s *countLoadStore) Load(ctx context.Context, k agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	p, err := s.InMemoryHistoryStore.Load(ctx, k, q)
	s.n += len(p.Messages)
	return p, err
}

// TestHistoryTaskSpan_FreshTaskDedupReadsAlmostNothing：新任务的这一次调用里，边界补记那几道去重读回的只有
// 命中的记录（按消息 ID 查）——会话再长也一样。
//
// 用外部执行体（不是 BasicAgent，没有 prior 要读），读回的条数就只来自去重。
func TestHistoryTaskSpan_FreshTaskDedupReadsAlmostNothing(t *testing.T) {
	st := &countLoadStore{InMemoryHistoryStore: agent_v2.NewInMemoryHistoryStore()}
	ctx := agent_v2.WithHistoryCtx(context.Background(), st, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_fresh_reads")
	spanForeignTurns(t, ctx, st, "t", 600) // 1200 条别的任务的记录
	st.n = 0

	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seqF"))
	seq.AddStep("s1", &pausingExternalAgent{name: "ext1", qid: "q1", paused: true})
	seq.AddStep("s2", &pausingExternalAgent{name: "ext2", qid: "q2", paused: true})
	if _, err := seq.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("新任务")); err != nil {
		t.Fatal(err)
	}
	if st.n > 8 {
		t.Fatalf("会话 1200 多条，新任务这一次调用的去重读回了 %d 条——没按条件查", st.n)
	}
	t.Logf("会话 1200 多条，新任务这一次调用读回 %d 条", st.n)
}

// TestHistoryTaskSpan_SameSessionRunAgain_OtherTasksInBetween：同一个 session 对象（同一个 RunID）中断之后
// 又 Run 一次、传同一个消息对象——按任务章它仍是那个任务，用户原话只该有一份；中间插进来多少别的任务的
// 记录都一样。曾经按「会话最后一条是不是本任务写的」判延续，中间只要夹 1 条就判错、记两份（复核实测）；
// 现在「这个任务写过历史」记在 session 的 KV 上。
func TestHistoryTaskSpan_SameSessionRunAgain_OtherTasksInBetween(t *testing.T) {
	for _, k := range []int{0, 1, 32} {
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seqQ1"))
			seq.AddStep("theStep", &pausingExternalAgent{name: "extPause", qid: "q1"})
			conv := fmt.Sprintf("c_rerun_%d", k)
			ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
			ctx = compose_v2.WithThreadIDCtx(ctx, conv)
			ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			in := schema.UserMessage("我的原话")
			if _, err := seq.Run(ctx, sess, in); err == nil {
				t.Fatal("Run 1 应中断")
			}
			spanForeignTurns(t, ctx, hs, "t", k)
			if _, err := seq.Run(ctx, sess, in); err != nil {
				t.Fatalf("Run 2: %v", err)
			}
			msgs := spanLoadAll(t, ctx, hs, agent_v2.HistoryKey{Owner: "t", ConvID: conv})
			if n := spanCount(msgs, "我的原话"); n != 1 {
				t.Errorf("同一 session 再 Run 后用户原话 %d 份, want 1", n)
				spanDump(t, msgs)
			}
		})
	}
}

// TestHistoryTaskSpan_RunAgainThenAbort_OtherTasksInBetween：同一 session 中断 → 夹 k 轮别的任务 → 再 Run
// 又中断 → 放弃。两次 Run 留下的悬空调用都要补上——起点要是第一次 Run 的那个，不是第二次的。
func TestHistoryTaskSpan_RunAgainThenAbort_OtherTasksInBetween(t *testing.T) {
	for _, k := range []int{0, 1, 32} {
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			rt := &recordTool{name: "do_thing"}
			tk := agent_v2_newToolkit()
			tk.Register(rt)
			tc := func(id string) *schema.Message {
				return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Type: "function",
					Function: schema.FunctionCall{Name: "do_thing", Arguments: `{"x":1}`}}})
			}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithToolkit(tk),
				agent_v2.WithMaxRounds(4), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
				agent_v2.WithHistory(hs, "tenant1"),
				agent_v2.WithModel(newStubModel([]*schema.Message{tc("tcA"), tc("tcB"), schema.AssistantMessage("final")})))
			conv := fmt.Sprintf("c_rerun_abort_%d", k)
			ctx := compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(context.Background(), conv),
				compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			if _, err := ag.Run(ctx, sess, schema.UserMessage("hi")); err == nil {
				t.Fatal("Run 1 应中断")
			}
			spanForeignTurns(t, ctx, hs, "tenant1", k)
			if _, err := ag.Run(ctx, sess, schema.UserMessage("hi again")); err == nil {
				t.Fatal("Run 2 应中断")
			}
			if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort)); err != nil {
				t.Fatalf("Abort: %v", err)
			}
			msgs := spanLoadAll(t, ctx, hs, agent_v2.HistoryKey{Owner: "tenant1", ConvID: conv})
			if d := danglingCount(msgs); d != 0 {
				t.Errorf("放弃后仍有 %d 个悬空 tool_call", d)
				spanDump(t, msgs)
			}
		})
	}
}

// TestHistoryTaskSpan_ResumeReadsOnlyMatches：续跑时边界补记的每道去重读回的只有命中的记录——挂起期间别的
// 任务写了 1200 条，6 步外部执行体（12 道去重）读回的条数跟它们无关（曾经每道去重各扫一遍「这个任务开始
// 以来」，约 7 倍那一段；缓存那一段之后约 1 倍）。
func TestHistoryTaskSpan_ResumeReadsOnlyMatches(t *testing.T) {
	st := &countLoadStore{InMemoryHistoryStore: agent_v2.NewInMemoryHistoryStore()}
	ctx := agent_v2.WithHistoryCtx(context.Background(), st, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_span_once")
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seqR1"))
	seq.AddStep("s0", &pausingExternalAgent{name: "e0", qid: "r1q"})
	for i := 1; i <= 5; i++ {
		seq.AddStep(fmt.Sprintf("s%d", i), &pausingExternalAgent{name: fmt.Sprintf("e%d", i), qid: fmt.Sprintf("q%d", i), paused: true})
	}
	sess := agent_v2.NewRunSession()
	if _, err := seq.Run(ctx, sess, schema.UserMessage("任务A")); err == nil {
		t.Fatal("应中断")
	}
	spanForeignTurns(t, ctx, st, "t", 600) // 1200 条
	st.n = 0
	if _, err := seq.Resume(ctx, sess.RunID); err != nil {
		t.Fatal(err)
	}
	if st.n > 8 {
		t.Fatalf("续跑读回 %d 条——去重在翻「这个任务开始以来」那一段（约 1200 条），没按条件查", st.n)
	}
	t.Logf("续跑读回 %d 条（任务开始以来约 1200 条）", st.n)
}

// TestHistoryTaskSpan_NestedWriterReadsOnlyMatches：外层没配历史、内层编排自己建写入器（嵌套位置）。会话里已有
// 1200 条别的任务的记录：去重按条件查，内层 BasicAgent 只看自己的视图按归属过滤——一个从没说过话的 agent
// 读回 0 条（曾经每道去重扫一遍整场会话，6009 条；之后剩 prior 往前翻到会话开头那一遍，约 1400 条）。
func TestHistoryTaskSpan_NestedWriterReadsOnlyMatches(t *testing.T) {
	st := &countLoadStore{InMemoryHistoryStore: agent_v2.NewInMemoryHistoryStore()}
	base := compose_v2.WithThreadIDCtx(context.Background(), "c_nested_writer")
	spanForeignTurns(t, agent_v2.WithHistoryCtx(base, st, "t"), st, "t", 600)
	st.n = 0

	inner := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("inner"))
	for i := 0; i < 3; i++ {
		inner.AddStep(fmt.Sprintf("e%d", i), &pausingExternalAgent{name: fmt.Sprintf("ext%d", i), qid: fmt.Sprintf("nq%d", i), paused: true})
	}
	inner.AddStep("leaf", agent_v2.NewBasicAgent(agent_v2.WithName("leaf"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("叶子答复")})),
		agent_v2.WithHistory(st, "t"), agent_v2.WithMaxRounds(2)))
	inCh := compose_v2.LastValue[*schema.Message]("__nw_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__nw_out__")
	bld := compose_v2.NewBuilder().WithName("nw_graph")
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("n1", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		ev, err := agent_v2.RunSubAgent(ctx, "n1", inner, in)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, agent_v2.FinalMessage(ev))}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "n1")
	bld.AddEdge("n1", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ga := agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("nwg"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
	if _, err := ga.Run(base, agent_v2.NewRunSession(), schema.UserMessage("新任务")); err != nil {
		t.Fatal(err)
	}
	if st.n > 8 {
		t.Fatalf("嵌套写入器的新任务读回 %d 条——去重在扫会话，或只看自己的视图没按归属过滤", st.n)
	}
	t.Logf("嵌套写入器的新任务读回 %d 条（会话 1200 多条）", st.n)
}

// TestHistoryTaskSpan_NewSessionObjectSameRunID_OtherTasksInBetween：中断之后用一个**新建的 session 对象**（同
// 一个 RunID）再 Run、传同一个输入对象——无状态服务端按存下的 RunID 重建 session 就是这样。它仍是那个任务，
// 用户原话只该有一份。曾经「这个任务写过历史」记在 session 对象的 KV 上，新对象 KV 是空的，退回看会话
// 最后一条是谁写的；中间夹 1 条别的任务的记录就判错、记两份（审计实测）。
func TestHistoryTaskSpan_NewSessionObjectSameRunID_OtherTasksInBetween(t *testing.T) {
	for _, k := range []int{0, 1, 32} {
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seqNewObj"))
			seq.AddStep("theStep", &pausingExternalAgent{name: "extPause", qid: "qn"})
			conv := fmt.Sprintf("c_newobj_%d", k)
			ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
			ctx = compose_v2.WithThreadIDCtx(ctx, conv)
			ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			in := schema.UserMessage("我的原话")
			if _, err := seq.Run(ctx, sess, in); err == nil {
				t.Fatal("Run 1 应中断")
			}
			spanForeignTurns(t, ctx, hs, "t", k)
			again := &agent_v2.RunSession{RunID: sess.RunID}
			if _, err := seq.Run(ctx, again, in); err != nil {
				t.Fatalf("Run 2: %v", err)
			}
			msgs := spanLoadAll(t, ctx, hs, agent_v2.HistoryKey{Owner: "t", ConvID: conv})
			if n := spanCount(msgs, "我的原话"); n != 1 {
				t.Errorf("新 session 对象（同 RunID）再 Run 后用户原话 %d 份, want 1", n)
				spanDump(t, msgs)
			}
		})
	}
}

// TestHistoryTaskSpan_NewSessionObjectThenAbort_OtherTasksInBetween：中断 → 夹 k 轮别的任务 → 新 session 对象
// （同 RunID）再 Run 又中断 → 放弃。两次 Run 留下的悬空调用都要补上：放弃按任务章找这个任务写下的全部
// 记录。曾经往前扫到任务的起点章为止，第二次 Run 被判成新任务、又盖了一个起点章，扫到它就停，第一次的
// 悬空调用漏补（审计实测）。
func TestHistoryTaskSpan_NewSessionObjectThenAbort_OtherTasksInBetween(t *testing.T) {
	for _, k := range []int{0, 1, 32} {
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			rt := &recordTool{name: "do_thing"}
			tk := agent_v2_newToolkit()
			tk.Register(rt)
			tc := func(id string) *schema.Message {
				return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Type: "function",
					Function: schema.FunctionCall{Name: "do_thing", Arguments: `{"x":1}`}}})
			}
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithToolkit(tk),
				agent_v2.WithMaxRounds(4), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
				agent_v2.WithHistory(hs, "tenant1"),
				agent_v2.WithModel(newStubModel([]*schema.Message{tc("tcA"), tc("tcB"), schema.AssistantMessage("final")})))
			conv := fmt.Sprintf("c_newobj_abort_%d", k)
			ctx := compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(context.Background(), conv),
				compose_v2.NewInMemoryCheckpointer())
			sess := agent_v2.NewRunSession()
			if _, err := ag.Run(ctx, sess, schema.UserMessage("hi")); err == nil {
				t.Fatal("Run 1 应中断")
			}
			spanForeignTurns(t, ctx, hs, "tenant1", k)
			again := &agent_v2.RunSession{RunID: sess.RunID}
			if _, err := ag.Run(ctx, again, schema.UserMessage("hi again")); err == nil {
				t.Fatal("Run 2 应中断")
			}
			if _, err := ag.Resume(ctx, sess.RunID, agent_v2.WithResumeAction(agent_v2.ResumeActionAbort)); err != nil {
				t.Fatalf("Abort: %v", err)
			}
			msgs := spanLoadAll(t, ctx, hs, agent_v2.HistoryKey{Owner: "tenant1", ConvID: conv})
			if tcs := spanToolCallCount(msgs); tcs != 2 {
				t.Fatalf("fixture 没搭对：两次 Run 应各留下一个 tool_call，实际 %d 个", tcs)
			}
			if d := danglingCount(msgs); d != 0 {
				t.Errorf("放弃后仍有 %d 个悬空 tool_call", d)
				spanDump(t, msgs)
			}
		})
	}
}

// TestHistoryTaskSpan_NilSessionPinnedSID_OtherTasksInBetween：session 传 nil、ctx 上钉着 sid——框架每次 Run 现铸
// 一个 session 对象，任务就是那个 sid。中断后（中间夹 k 轮别的任务）再 Run，用户原话只该有一份（曾经同
// 「新 session 对象」那条一样退回猜、记两份）。
func TestHistoryTaskSpan_NilSessionPinnedSID_OtherTasksInBetween(t *testing.T) {
	for _, k := range []int{0, 1, 32} {
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			hs := agent_v2.NewInMemoryHistoryStore()
			seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seqNilSess"))
			seq.AddStep("theStep", &pausingExternalAgent{name: "extPause", qid: "qs"})
			conv := fmt.Sprintf("c_nilsess_%d", k)
			ctx := agent_v2.WithHistoryCtx(context.Background(), hs, "t")
			ctx = compose_v2.WithThreadIDCtx(ctx, conv)
			ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
			pinned := compose_v2.WithSessionIDCtx(ctx, fmt.Sprintf("sid-nil-%d", k))
			in := schema.UserMessage("我的原话")
			if _, err := seq.Run(pinned, nil, in); err == nil {
				t.Fatal("Run 1 应中断")
			}
			spanForeignTurns(t, ctx, hs, "t", k)
			if _, err := seq.Run(pinned, nil, in); err != nil {
				t.Fatalf("Run 2: %v", err)
			}
			msgs := spanLoadAll(t, ctx, hs, agent_v2.HistoryKey{Owner: "t", ConvID: conv})
			if n := spanCount(msgs, "我的原话"); n != 1 {
				t.Errorf("nil session + 钉住的 sid 再 Run 后用户原话 %d 份, want 1", n)
				spanDump(t, msgs)
			}
		})
	}
}

// TestHistoryTaskSpan_RunIDReuseReadsStayFlat：业务复用同一个 RunID 跑一串新问题（没调 NextTask）时这一串都算
// 同一个任务；去重按消息 ID 查，第 300 轮读回的条数跟第 1 轮一样——不随这个任务写下的量线性增长（曾经要把
// 「这个任务开始以来」那一段翻一遍）。
func TestHistoryTaskSpan_RunIDReuseReadsStayFlat(t *testing.T) {
	st := &countLoadStore{InMemoryHistoryStore: agent_v2.NewInMemoryHistoryStore()}
	ctx := agent_v2.WithHistoryCtx(context.Background(), st, "t")
	ctx = compose_v2.WithThreadIDCtx(ctx, "c_runid_reuse")
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("seqReuse"))
	seq.AddStep("s1", &externalAgent{name: "ext"})
	sess := agent_v2.NewRunSession()
	var first, last int
	for i := 1; i <= 300; i++ {
		st.n = 0
		if _, err := seq.Run(ctx, sess, schema.UserMessage(fmt.Sprintf("第 %d 轮", i))); err != nil {
			t.Fatal(err)
		}
		switch i {
		case 1:
			first = st.n
		case 300:
			last = st.n
		}
	}
	if last > first+2 {
		t.Fatalf("同一 RunID 第 300 轮读回 %d 条、第 1 轮 %d 条——读的量随任务写下的记录增长", last, first)
	}
	t.Logf("同一 RunID：第 1 轮读回 %d 条，第 300 轮 %d 条", first, last)
}

// spanToolCallCount 数历史里声明过的 tool_call 个数。
func spanToolCallCount(msgs []*schema.Message) int {
	n := 0
	for _, m := range msgs {
		n += len(schema.GetToolCalls(m))
	}
	return n
}
