package agent_v2_test

// 中断与恢复路径上的历史——**此前整条零覆盖**。
//
// 这条路径的特殊在于「同生共死」这条约定要在这里兑现：节点失败时它的 writes 会被整批丢弃
// （这正是 Resume 要重跑全部工具的原因），历史若先写了就会在重跑后进两遍。反过来，Resume
// 那一轮产出的东西又必须补上——否则历史停在「声明了工具调用」的半截状态，下一轮的 prior
// 只能给它补一条「这次没留下结果」的记账文本，模型看到的是一件没发生的事。
//
// 写入点从「节点成功后」挪到「节点入口」这类改动，在这条路径之外不会有任何用例 fail。

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// TestHistory_HITLResume_CompleteAndNoDuplicate：中断 + 恢复之后，历史既不缺也不重。
func TestHistory_HITLResume_CompleteAndNoDuplicate(t *testing.T) {
	askTool := tool.NewFuncTool("ask", "问用户",
		map[string]any{"type": "object", "properties": map[string]any{
			"q": map[string]any{"type": "string"}}},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			if ans, ok := compose_v2.GetResumeAnswerText(ctx, "ask_id"); ok {
				return tool.Success("用户回答：" + ans), nil
			}
			return nil, &compose_v2.InterruptError{
				Query: &compose_v2.AskQuery{InterruptID: "ask_id", Question: "要哪个？"},
			}
		})
	tk := tool.NewToolkit()
	tk.Register(askTool)

	hs := agent_v2.NewInMemoryHistoryStore()
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("hitl"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "tc_1", Function: schema.FunctionCall{Name: "ask", Arguments: `{"q":"x"}`},
			}}),
			schema.AssistantMessage("FINAL_MARK"),
		})),
		agent_v2.WithToolkit(tk),
		agent_v2.WithHistory(hs, "tenant-a"),
		agent_v2.WithMaxRounds(5),
	)

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	ctx = compose_v2.WithThreadIDCtx(ctx, "conv-hitl")
	key := agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-hitl"}
	sess := agent_v2.NewRunSession()

	_, err := ag.Run(ctx, sess, schema.UserMessage("INPUT_MARK"))
	ie, isInterrupt := compose_v2.IsInterrupt(err)
	if !isInterrupt {
		t.Fatalf("Run 应抛中断, got %v", err)
	}

	// 中断当下：工具轮整批作废（Resume 会重跑全部工具），所以历史里不该有工具结果。
	mid, _ := agent_v2.LoadAllHistory(ctx, hs, key)
	dumpPrompt(t, "中断后的历史", mid)
	if len(mid) != 2 {
		t.Errorf("中断后历史 %d 条, want 2（用户输入 + 声明了工具调用的 assistant）——"+
			"工具轮整批作废时不该有结果落库，否则 Resume 重跑后同一条会进两遍", len(mid))
	}

	if _, err := ag.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{
			ie.Query.InterruptID: schema.UserMessage("第二个"),
		}),
		agent_v2.WithResumeSession(sess),
	); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	got, _ := agent_v2.LoadAllHistory(ctx, hs, key)
	dumpPrompt(t, "Resume 后的历史", got)
	if len(got) != 4 {
		t.Fatalf("Resume 后历史 %d 条, want 4（用户输入 / 声明 / 工具结果 / 最终答复）", len(got))
	}
	var inputSeen int
	for _, m := range got {
		if strings.Contains(m.Content, "INPUT_MARK") {
			inputSeen++
		}
	}
	if inputSeen != 1 {
		t.Errorf("用户输入出现 %d 次, want 1——Resume 重跑 preamble 时又记了一遍", inputSeen)
	}
	if !containsMsg(got, "用户回答：第二个") {
		t.Error("Resume 里产生的工具结果没进历史——那一轮的往返就此断掉，" +
			"下一轮的 prior 只能给它补一条记账文本")
	}
	if !containsMsg(got, "FINAL_MARK") {
		t.Error("Resume 的最终答复没进历史")
	}
	if bad := assertPairingValid(got); bad != "" {
		t.Errorf("历史流本身配对违规: %s —— 下一轮读回来直接 400", bad)
	}
}

// concurrentStub 是线程安全的模型替身。
//
// 并发用例**必须**用它而不是普通 stub：后者的调用计数自己就有竞态，用它跑 -race 会打出
// 一堆与被测对象无关的报告，把真问题埋掉。
type concurrentStub struct {
	mu    sync.Mutex
	reply string
}

func (m *concurrentStub) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return schema.AssistantMessage(m.reply), nil
}

func (m *concurrentStub) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (m *concurrentStub) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

var _ model.ChatModel = (*concurrentStub)(nil)

// TestHistory_ConcurrentRuns_SameConversation_NoLoss：并发跑同一场会话，一条都不能丢。
//
// 同一个 ConvID 上并发发起多个 Run 是真实形状——用户连点、前端重试、多标签页。每个 Run
// 有自己的写入器，它们写同一个 (Owner, ConvID)。**顺序在这里没有意义**（并发本来就没有
// 全序），但**条数必须对**：丢了就是有人的话没被记下。
//
// 跑 -race 时这条同时在验后端的并发安全。
func TestHistory_ConcurrentRuns_SameConversation_NoLoss(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-concurrent")

	const n = 8
	var wg sync.WaitGroup
	start := make(chan struct{}) // 让它们尽量同时起跑
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ag := agent_v2.NewBasicAgent(
				agent_v2.WithName("worker"),
				agent_v2.WithModel(&concurrentStub{reply: "答复"}),
				agent_v2.WithHistory(hs, "tenant-a"),
			)
			<-start
			if _, err := ag.Run(ctx, agent_v2.NewRunSession(),
				schema.UserMessage("问题-"+itoa(i))); err != nil {
				t.Errorf("Run %d: %v", i, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	got, _ := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "tenant-a", ConvID: "conv-concurrent"})
	if len(got) != n*2 {
		t.Errorf("历史 %d 条, want %d（%d 个 Run × 输入+产出）——并发写掉了消息",
			len(got), n*2, n)
	}
	// 每个问题都得在，一个不少。
	for i := 0; i < n; i++ {
		if !containsMsg(got, "问题-"+itoa(i)) {
			t.Errorf("第 %d 个 Run 的用户输入没进历史", i)
		}
	}
}

// delayedHistoryStore 是带写延迟的后端——**这个延迟是本用例的核心**，不是装饰。
//
// 内存实现的 Append 是瞬时的，异步写在下一次读之前就落盘了，于是「读跑到写前面」这类问题
// 在它上面**永远看不见**。真实后端（Redis/PG）哪怕只有几百微秒也足以暴露。
type delayedHistoryStore struct {
	mu    sync.Mutex
	convs map[agent_v2.HistoryKey][]*schema.Message
	delay time.Duration
}

func (s *delayedHistoryStore) Append(_ context.Context, k agent_v2.HistoryKey, msgs []*schema.Message) error {
	time.Sleep(s.delay)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.convs == nil {
		s.convs = map[agent_v2.HistoryKey][]*schema.Message{}
	}
	s.convs[k] = append(s.convs[k], msgs...)
	return nil
}

func (s *delayedHistoryStore) Load(_ context.Context, k agent_v2.HistoryKey, _ agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*schema.Message, len(s.convs[k]))
	copy(out, s.convs[k])
	return agent_v2.HistoryPage{Messages: out}, nil
}

var _ agent_v2.HistoryStore = (*delayedHistoryStore)(nil)

// TestHistory_NestedReadSeesOwnPendingWrites：同一个 Run 内，子读得到父刚写的。
//
// 写是异步的、读是同步的，而 Run 内部没有排空点——父建了写入器、子沿用它，子读历史时父那
// 批可能还在队列里。读到空之后，因为是「读成功只是没内容」而**不回落 session events**，
// 子于是彻底失忆。
//
// 循环体是最尖锐的形状：迭代之间没有 LLM 往返来掩盖异步延迟。
//
// **必须跑多档延迟**：0 延迟那档是对照——它恒过，正是它让这个问题在内存后端上藏了这么久。
func TestHistory_NestedReadSeesOwnPendingWrites(t *testing.T) {
	for _, delay := range []time.Duration{0, 200 * time.Microsecond, 5 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			hs := &delayedHistoryStore{delay: delay}
			bodyM := &promptRecorder{resp: []*schema.Message{
				schema.AssistantMessage("产出一"),
				schema.AssistantMessage("产出二"),
				schema.AssistantMessage("产出三"),
			}}
			body := agent_v2.NewBasicAgent(
				agent_v2.WithName("body"),
				agent_v2.WithModel(bodyM),
			)
			loop := agent_v2.NewLoopAgent(
				agent_v2.WithLoopName("loop"),
				agent_v2.WithLoopMaxIterations(3),
			).WithBody(body)

			// 历史开在顶层：写入器建在这里，往下都沿用它——子的 Run 出口不会排空。
			tk := tool.NewToolkit()
			tk.Register(agent_v2.AgentAsTool(loop, agent_v2.WithAgentToolDescription("跑循环")))
			topM := &promptRecorder{resp: []*schema.Message{
				{Role: schema.RoleAssistant, ContentBlocks: []*schema.ContentBlock{{
					Type: "tool_call",
					Toolcall: &schema.ToolcallContentBlock{
						Id: "t1", Name: "loop", Arguments: `{"input":"MARK_最初那句"}`,
					},
				}}},
				schema.AssistantMessage("顶层收工"),
			}}
			top := agent_v2.NewBasicAgent(
				agent_v2.WithName("top"),
				agent_v2.WithModel(topM),
				agent_v2.WithToolkit(tk),
				agent_v2.WithHistory(hs, "tenant-a"),
			)

			ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-nested")
			if _, err := top.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("开始")); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if len(bodyM.seen) < 2 {
				t.Fatalf("循环体只跑了 %d 轮", len(bodyM.seen))
			}
			if !containsMsg(bodyM.seen[1], "MARK_最初那句") {
				t.Errorf("后端延迟 %v：第 2 轮循环体丢了最初那句——读跑到了自己的异步写前面，"+
					"而读到空之后不回落 events，于是它彻底失忆\n%s", delay, bodyM.dump())
			}
		})
	}
}
