package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// 同一个位置上的几次嵌套调用，调用的次序在两次执行之间变了（按进度跳过已完成的调用、条件
// 分支插进一次调用、重试里重建了输入、链式调用的上游产出变了）。这里钉三件事：
//
//   - 身份按输入内容定：跳过、插入都不会让一次调用落到另一次调用的槽上、拿到另一次调用的
//     答案，不带 key 也跑对；
//   - 已完成的调用记在父节点这次执行里，重跑时直接拿回结果、不再执行——副作用不重放、问过
//     的不重问，链式调用因此收敛；
//   - 带 call key 的调用身份就是 key：同一个 key 输入变了照样续、告警；同一次执行里重复用
//     同一个 key 终局失败。

// skipDoneHost 造一个照 SaveNodeState godoc 的典型用法写的父节点：依次处理三项，每做完一项
// 记进节点私有态，重跑时跳过已完成的。withKey 时每次调用以条目名作 call key。
func skipDoneHost(t *testing.T, sub agent_v2.Agent, items []string, withKey bool) agent_v2.Agent {
	t.Helper()
	return newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
		done, _ := compose_v2.GetNodeState[[]string](ctx)
		outs := map[string]string{}
		for _, kv := range done {
			k, v, _ := strings.Cut(kv, "\x00")
			outs[k] = v
		}
		for _, item := range items {
			if _, ok := outs[item]; ok {
				continue // 上次已做完，跳过
			}
			var opts []agent_v2.RunOption
			if withKey {
				opts = append(opts, agent_v2.WithCallKey(item))
			}
			events, err := agent_v2.RunSubAgent(ctx, "w", sub, schema.UserMessage(item), opts...)
			if err != nil {
				return "", err
			}
			outs[item] = nestedFinal(events)
			done = append(done, item+"\x00"+outs[item])
			if err := compose_v2.SaveNodeState(ctx, done); err != nil {
				return "", err
			}
		}
		parts := make([]string, 0, len(items))
		for _, item := range items {
			parts = append(parts, outs[item])
		}
		return strings.Join(parts, " | "), nil
	})
}

// flakyWorker 造一个两节点子 agent：两个节点各自记下处理过哪一项；第二个节点处理 item2 时
// 第一次失败（瞬时错，留下可续的失败存档）。
func flakyWorker(t *testing.T, firsts, seconds *countLog) agent_v2.Agent {
	t.Helper()
	var failed atomic.Bool
	return newTwoNodeAgent(t, "worker",
		func(_ context.Context, c string) (string, error) {
			firsts.inc(c)
			return "prep:" + c, nil
		},
		func(_ context.Context, c, _ string) (string, error) {
			seconds.inc(c)
			if c == "item2" && failed.CompareAndSwap(false, true) {
				return "", errors.New("item2 瞬时失败")
			}
			return "处理了:" + c, nil
		})
}

// TestNestedCall_SkipCompletedCallsResumeOwnCheckpoint：按进度跳过已完成的调用。
//
// 首跑 item1 成功、item2 失败。续跑时 item1 被跳过，item2 找回自己的失败存档、只重跑失败的
// 第二个节点，item3 新开。带不带 key 都一样——身份跟调用次序无关。
//
// 按次序定身份时，跳过 item1 让 item2 成了「第 0 次」、item3 成了「第 1 次」：item3 落到 item2
// 留下的失败存档上，把 item2 的进度续完、拿「处理了:item2」当自己的结果，`err == nil`。
func TestNestedCall_SkipCompletedCallsResumeOwnCheckpoint(t *testing.T) {
	for _, withKey := range []bool{false, true} {
		name := "不带 key"
		if withKey {
			name = "带 key"
		}
		t.Run(name, func(t *testing.T) {
			firsts, seconds := &countLog{}, &countLog{}
			host := skipDoneHost(t, flakyWorker(t, firsts, seconds), []string{"item1", "item2", "item3"}, withKey)

			cper := compose_v2.NewInMemoryCheckpointer()
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
			sess := agent_v2.NewRunSession()
			if _, err := host.Run(ctx, sess, schema.UserMessage("go")); err == nil {
				t.Fatal("首跑应因 item2 瞬时失败而失败")
			}
			events, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess))
			if err != nil {
				t.Fatalf("续跑失败：%v", err)
			}
			if got, want := nestedFinal(events), "处理了:item1 | 处理了:item2 | 处理了:item3"; got != want {
				t.Fatalf("最终产出 = %q，want %q", got, want)
			}
			if n := firsts.get("item2"); n != 1 {
				t.Errorf("item2 的第一个节点应只跑 1 次（续上自己的存档），实际 %d——从头跑、已完成的步骤重放了", n)
			}
			if n := seconds.get("item2"); n != 2 {
				t.Errorf("item2 的第二个节点应跑 2 次（首跑失败 + 续跑重做），实际 %d", n)
			}
			if n := firsts.get("item3"); n != 1 {
				t.Errorf("item3 的第一个节点应跑 1 次，实际 %d", n)
			}
		})
	}
}

// approvingWorker 造一个单节点子 agent：问「批准「<输入>」？」，答「是」产出「批准了:<输入>」，
// 否则产出「拒绝了:<输入>」。asked 记下每个被问到的输入。
func approvingWorker(t *testing.T, asked *inputLog) agent_v2.Agent {
	t.Helper()
	return newSingleNodeAgent(t, "approver", "approve", nil, func(ctx context.Context, c string) (string, error) {
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准「" + c + "」？"})
		if err != nil {
			if _, ok := compose_v2.IsInterrupt(err); ok && asked != nil {
				asked.add(c)
			}
			return "", err
		}
		if ans.Content == "是" {
			return "批准了:" + c, nil
		}
		return "拒绝了:" + c, nil
	})
}

// driveToEnd 跑到收敛：每次停下，按 reply 给这个提问作答（答案一路累积着传，照 CLAUDE.md
// 示例的写法）。返回最终产出。
func driveToEnd(t *testing.T, host agent_v2.Agent, reply func(q *compose_v2.AskQuery) string) string {
	t.Helper()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	events, err := host.Run(ctx, sess, schema.UserMessage("go"))
	for round := 0; round < 8; round++ {
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			break
		}
		for _, q := range ie.AllQueries() {
			answers[q.InterruptID] = schema.UserMessage(reply(q))
		}
		events, err = host.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeSession(sess))
	}
	if err != nil {
		t.Fatalf("八次续跑内没有收敛：%v", err)
	}
	return nestedFinal(events)
}

// TestNestedCall_SkipWithAccumulatedAnswersKeepsEachAnswerOnItsItem：按进度跳过已完成的调用，
// 每项都要人批准；调用方把答过的答案一路累积着传。用户批准 a、拒绝 b、批准 c——每个答案只作用
// 在它被问的那一项上。
//
// 按次序定身份时：跳过 a 之后 b 成了「第 0 次」，落在 a 的槽上、提问身份与 a 那一问相同——
// 累积的答案里 a 的「是」被直接套到 b 头上，用户拒绝 b 的那个答案从没被用上（审计实测）。
func TestNestedCall_SkipWithAccumulatedAnswersKeepsEachAnswerOnItsItem(t *testing.T) {
	asked := &inputLog{}
	host := skipDoneHost(t, approvingWorker(t, asked), []string{"a", "b", "c"}, false)
	final := driveToEnd(t, host, func(q *compose_v2.AskQuery) string {
		if strings.Contains(q.Question, "「b」") {
			return "否"
		}
		return "是"
	})
	if want := "批准了:a | 拒绝了:b | 批准了:c"; final != want {
		t.Fatalf("最终产出 = %q，want %q——有答案被套到了别的项上", final, want)
	}
	if got := strings.Join(asked.snapshot(), ","); got != "a,b,c" {
		t.Errorf("每项应恰好被问一次，依次被问 %s", got)
	}
}

// itemTransferModel 让子 BasicAgent 对自己收到的那一项调一次 transfer；看到工具结果后给出
// 最终答复。按对话内容出回复、不按调用次数，续跑与新开两条路径都对。
type itemTransferModel struct{}

func (itemTransferModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	item := ""
	for _, m := range msgs {
		if m == nil {
			continue
		}
		switch m.Role {
		case schema.RoleTool:
			return schema.AssistantMessage("完成:" + item), nil
		case schema.RoleUser:
			item = m.Content
		}
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID:       "call_0",
		Type:     "function",
		Function: schema.FunctionCall{Name: "transfer", Arguments: fmt.Sprintf(`{"item":%q}`, item)},
	}}), nil
}

func (itemTransferModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}

func (m itemTransferModel) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// TestNestedCall_SkipWithAccumulatedAnswersRespectsToolApproval：同上一条，批准换成框架自带的
// 工具审批（`WithApprovalMode(ApprovalAsk)`）。用户放行 a、拒绝 b、放行 c——断言落在**工具
// 有没有真执行**上：b 的转账一次都不能发生。
func TestNestedCall_SkipWithAccumulatedAnswersRespectsToolApproval(t *testing.T) {
	executed := &countLog{}
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool("transfer", "转账",
		map[string]any{"type": "object", "properties": map[string]any{"item": map[string]any{"type": "string"}}},
		func(_ context.Context, args map[string]any) (*tool.Result, error) {
			item, _ := args["item"].(string)
			executed.inc(item)
			return tool.Success("转了:" + item), nil
		}))
	sub := agent_v2.NewBasicAgent(
		agent_v2.WithName("payer"),
		agent_v2.WithModel(itemTransferModel{}),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(4),
		agent_v2.WithApprovalMode(agent_v2.ApprovalAsk),
	)
	host := skipDoneHost(t, sub, []string{"a", "b", "c"}, false)

	// 审批的提问文本里带着工具参数（`参数：{"item":"b"}`），按它认出是哪一项
	final := driveToEnd(t, host, func(q *compose_v2.AskQuery) string {
		if strings.Contains(q.Question, `"item":"b"`) {
			return agent_v2.ApprovalOptionDeny
		}
		return agent_v2.ApprovalOptionApprove
	})
	if n := executed.get("b"); n != 0 {
		t.Fatalf("用户拒绝了 b，b 的转账却执行了 %d 次——另一项的放行被套到了 b 头上（最终产出 %q）", n, final)
	}
	if executed.get("a") != 1 || executed.get("c") != 1 {
		t.Errorf("a、c 各应执行 1 次，实际 a=%d c=%d（最终产出 %q）", executed.get("a"), executed.get("c"), final)
	}
}

// insertingHost 造一个父节点：依次调用 items() 给出的每一项，拼起结果。
func insertingHost(t *testing.T, sub agent_v2.Agent, items func() []string, withKey bool) agent_v2.Agent {
	t.Helper()
	return newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
		var outs []string
		for _, it := range items() {
			var opts []agent_v2.RunOption
			if withKey {
				opts = append(opts, agent_v2.WithCallKey(it))
			}
			events, err := agent_v2.RunSubAgent(ctx, "w", sub, schema.UserMessage(it), opts...)
			if err != nil {
				return "", err
			}
			outs = append(outs, nestedFinal(events))
		}
		return strings.Join(outs, " | "), nil
	})
}

// TestNestedCall_InsertedCallKeepsEachAnswerOnItsOwnCall：条件分支在续跑时插进一次调用。首跑
// 只调 B，B 停在提问上；续跑时先调 A 再调 B。A 新开、问自己的问题；用户给 B 的批准不会被套到
// A 头上，B 续上自己的存档。带不带 key 都一样。
//
// 按次序定身份时：A 成了「第 0 次」、接到 B 的存档，拿「批准了:B」当自己的结果；B 在新槽上
// 从头问一遍——用户对 B 的批准记在了 A 名下。
func TestNestedCall_InsertedCallKeepsEachAnswerOnItsOwnCall(t *testing.T) {
	for _, withKey := range []bool{false, true} {
		name := "不带 key"
		if withKey {
			name = "带 key"
		}
		t.Run(name, func(t *testing.T) {
			var insertA atomic.Bool
			items := func() []string {
				if insertA.Load() {
					return []string{"A", "B"}
				}
				return []string{"B"}
			}
			host := insertingHost(t, approvingWorker(t, nil), items, withKey)

			cper := compose_v2.NewInMemoryCheckpointer()
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
			sess := agent_v2.NewRunSession()
			_, err := host.Run(ctx, sess, schema.UserMessage("go"))
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("首跑应停在 B 的提问，got %v", err)
			}
			if !strings.Contains(ie.Query.Question, "「B」") {
				t.Fatalf("首跑问的应是 B，got %q", ie.Query.Question)
			}
			answers := map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")}
			insertA.Store(true)

			_, err = host.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeSession(sess))
			ie, ok = compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("续跑应停在 A 自己的提问，got %v", err)
			}
			if !strings.Contains(ie.Query.Question, "「A」") {
				t.Fatalf("续跑问的应是 A——给 B 的批准不能被 A 拿走，got %q", ie.Query.Question)
			}
			answers[ie.Query.InterruptID] = schema.UserMessage("否")

			events, err := host.Resume(ctx, sess.RunID, agent_v2.WithResumeUserInputs(answers), agent_v2.WithResumeSession(sess))
			if err != nil {
				t.Fatalf("两边都答完后应跑完：%v", err)
			}
			if got, want := nestedFinal(events), "拒绝了:A | 批准了:B"; got != want {
				t.Fatalf("最终产出 = %q，want %q", got, want)
			}
		})
	}
}

// TestNestedCall_DuplicateCallKeyAborts：同一次节点执行里两次调用给了同一个 key——它们会落在
// 同一个存档槽上，第二次终局失败，且它的输入没有进子 agent。
func TestNestedCall_DuplicateCallKeyAborts(t *testing.T) {
	log := &inputLog{}
	sub := newSingleNodeAgent(t, "sub", "work", log, func(_ context.Context, c string) (string, error) {
		return "done:" + c, nil
	})
	host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
		for _, in := range []string{"第一次", "第二次"} {
			if _, err := agent_v2.RunSubAgent(ctx, "w", sub, schema.UserMessage(in), agent_v2.WithCallKey("same")); err != nil {
				return "", err
			}
		}
		return "unreachable", nil
	})
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	_, err := host.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("重复的 call key 应终局失败，got %v", err)
	}
	if !strings.Contains(err.Error(), `"same"`) {
		t.Errorf("错误正文应带上惹事的 key，got %v", err)
	}
	if got := log.snapshot(); len(got) != 1 || got[0] != "第一次" {
		t.Errorf("只有第一次调用该进子 agent，got %v", got)
	}
}

// TestNestedCall_RetryWithCallKeyResumesOwnCheckpoint：带 call key 的调用包在 RunWithRetry 里。
// 重试的每次尝试是同一次调用：不算重复的 key，且续上第一次尝试的存档（第一个节点只跑 1 次）。
func TestNestedCall_RetryWithCallKeyResumesOwnCheckpoint(t *testing.T) {
	counts := &countLog{}
	var failed atomic.Bool
	child := newTwoNodeAgent(t, "child",
		func(_ context.Context, _ string) (string, error) {
			counts.inc("first")
			return "charged", nil
		},
		func(_ context.Context, _, mid string) (string, error) {
			counts.inc("second")
			if failed.CompareAndSwap(false, true) {
				return "", errors.New("下游抖了一下")
			}
			return "done:" + mid, nil
		})
	host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, c string) (string, error) {
		return compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (string, error) {
			events, err := agent_v2.RunSubAgent(ctx, "child", child, schema.UserMessage(c), agent_v2.WithCallKey("order-42"))
			if err != nil {
				return "", err
			}
			return nestedFinal(events), nil
		})
	})
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	events, err := host.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := nestedFinal(events); got != "done:charged" {
		t.Fatalf("最终产出 = %q", got)
	}
	if n := counts.get("first"); n != 1 {
		t.Fatalf("子 agent 的第一个节点应只跑 1 次（重试续上第一次尝试的存档），实际 %d", n)
	}
}

// TestNestedCall_RetryWithChangedInputRunsTheNewInput：RunWithRetry 的每次尝试重建了输入（内容
// 不同）。不带 key 时输入变了就是另一次调用：第二次尝试处理的是新输入、从头跑，不续第一次尝试
// 的存档，也不告警；第一次尝试留下的失败存档在任务成功收尾时跟着清掉。
func TestNestedCall_RetryWithChangedInputRunsTheNewInput(t *testing.T) {
	capture := installWarnCapture(t)
	counts := &countLog{}
	var failed atomic.Bool
	child := newTwoNodeAgent(t, "child",
		func(_ context.Context, c string) (string, error) {
			counts.inc(c)
			return "charged", nil
		},
		func(_ context.Context, c, mid string) (string, error) {
			if failed.CompareAndSwap(false, true) {
				return "", errors.New("下游抖了一下")
			}
			return "done:" + c + ":" + mid, nil
		})
	var attempt atomic.Int32
	host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
		return compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (string, error) {
			in := fmt.Sprintf("第 %d 次尝试", attempt.Add(1))
			events, err := agent_v2.RunSubAgent(ctx, "child", child, schema.UserMessage(in))
			if err != nil {
				return "", err
			}
			return nestedFinal(events), nil
		})
	})
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	before := len(capture.digestWarnings())
	events, err := host.Run(ctx, sess, schema.UserMessage("go"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := nestedFinal(events); got != "done:第 2 次尝试:charged" {
		t.Fatalf("最终产出 = %q，want 第二次尝试处理自己的输入", got)
	}
	if counts.get("第 1 次尝试") != 1 || counts.get("第 2 次尝试") != 1 {
		t.Errorf("两次尝试的第一个节点各应跑 1 次，实际 %d / %d", counts.get("第 1 次尝试"), counts.get("第 2 次尝试"))
	}
	if got := capture.digestWarnings()[before:]; len(got) != 0 {
		t.Errorf("不带 key 时输入变了就是另一次调用，不告警，got %v", got)
	}
	left, err := cper.LoadAllInSession(ctx, sess.RunID)
	if err != nil {
		t.Fatalf("LoadAllInSession: %v", err)
	}
	if len(left) != 0 {
		t.Errorf("任务成功收尾后仍残留 %d 份存档", len(left))
	}
}

// TestNestedCall_KeyedCallChangedInputResumesAndWarns：带 key 的调用，父节点重跑时对同一个
// key 给了不同的输入——框架分不出「同一件事、输入变了」与「同一个 key 这次做了别的事」，
// 照样续存档、告警（key 是业务声明的身份，同一个 key 就是同一件事）。
func TestNestedCall_KeyedCallChangedInputResumesAndWarns(t *testing.T) {
	capture := installWarnCapture(t)
	log := &inputLog{}
	sub := approvingWorker(t, nil)
	var executions atomic.Int32
	host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
		in := "原来的输入"
		if executions.Add(1) > 1 {
			in = "换过的输入"
		}
		log.add(in)
		events, err := agent_v2.RunSubAgent(ctx, "w", sub, schema.UserMessage(in), agent_v2.WithCallKey("k"))
		if err != nil {
			return "", err
		}
		return nestedFinal(events), nil
	})
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在提问，got %v", err)
	}
	before := len(capture.digestWarnings())
	events, err := host.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")}),
		agent_v2.WithResumeSession(sess))
	if err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	if got := nestedFinal(events); got != "批准了:原来的输入" {
		t.Fatalf("最终产出 = %q，want 续存档里原来的输入", got)
	}
	if log.count("换过的输入") != 1 {
		t.Fatalf("前提不成立：续跑时父节点应给出换过的输入，依次给的 %v", log.snapshot())
	}
	if got := capture.digestWarnings()[before:]; len(got) != 1 {
		t.Errorf("输入变了应告警一次，got %v", got)
	}
}

// recallWarnings 数「带 key 的嵌套调用这次的输入与它完成时不同」那条告警。
func (h *warnCapture) recallWarnings() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.recs {
		if strings.Contains(r.Message, "与它完成时不同") {
			n++
		}
	}
	return n
}

// TestNestedCall_KeyedCompletedCallChangedInputReturnsRecordedResult：带 key 的调用已经完成，
// 父节点随后问人、续跑时对同一个 key 给了不同的输入——拿回完成时记下的结果、不再执行，并告警。
//
// 对照组：不带 key 时那是另一次调用，按新输入执行。
func TestNestedCall_KeyedCompletedCallChangedInputReturnsRecordedResult(t *testing.T) {
	capture := installWarnCapture(t)
	for _, withKey := range []bool{true, false} {
		name := "带 key：拿回记下的结果"
		if !withKey {
			name = "不带 key：另一次调用"
		}
		t.Run(name, func(t *testing.T) {
			log := &inputLog{}
			sub := newSingleNodeAgent(t, "sub", "work", log, func(_ context.Context, c string) (string, error) {
				return "处理了:" + c, nil
			})
			var executions atomic.Int32
			host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
				in := "原来的输入"
				if executions.Add(1) > 1 {
					in = "换过的输入"
				}
				var opts []agent_v2.RunOption
				if withKey {
					opts = append(opts, agent_v2.WithCallKey("k"))
				}
				events, err := agent_v2.RunSubAgent(ctx, "w", sub, schema.UserMessage(in), opts...)
				if err != nil {
					return "", err
				}
				ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"})
				if err != nil {
					return "", err
				}
				return nestedFinal(events) + "(" + ans.Content + ")", nil
			})
			cper := compose_v2.NewInMemoryCheckpointer()
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
			sess := agent_v2.NewRunSession()
			_, err := host.Run(ctx, sess, schema.UserMessage("go"))
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("首跑应停在父节点的提问，got %v", err)
			}
			before := capture.recallWarnings()
			events, err := host.Resume(ctx, sess.RunID,
				agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}),
				agent_v2.WithResumeSession(sess))
			if err != nil {
				t.Fatalf("续跑失败：%v", err)
			}
			got := nestedFinal(events)
			if withKey {
				if got != "处理了:原来的输入(好)" {
					t.Fatalf("最终产出 = %q，want 完成时记下的结果", got)
				}
				if n := log.count("换过的输入"); n != 0 {
					t.Errorf("同一个 key 的调用已完成，不该再执行，实际处理了换过的输入 %d 次", n)
				}
				if n := capture.recallWarnings() - before; n != 1 {
					t.Errorf("输入变了应告警一次，got %d", n)
				}
				return
			}
			if got != "处理了:换过的输入(好)" {
				t.Fatalf("最终产出 = %q，want 按新输入执行", got)
			}
			if n := capture.recallWarnings() - before; n != 0 {
				t.Errorf("不带 key 不该有这条告警，got %d", n)
			}
		})
	}
}

// TestNestedCall_RunWithRetryDoesNotReaskCompletedCall：RunWithRetry 的一次尝试里，先调一个会
// 问人的子 agent（答完、完成），后面一步瞬时失败。重试的第二次尝试拿回那次调用记下的结果，不再
// 执行、不再问。
//
// 不记住已完成的调用时：第二次尝试从头跑那个子 agent（它成功时存档已清），提问算出与刚才相同
// 的身份——答案账本认出这一位已经兑付过，整个任务终局失败（审计实测）。
func TestNestedCall_RunWithRetryDoesNotReaskCompletedCall(t *testing.T) {
	var askerRuns atomic.Int32
	asker := newSingleNodeAgent(t, "asker", "work", nil, func(ctx context.Context, c string) (string, error) {
		askerRuns.Add(1)
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"})
		if err != nil {
			return "", err
		}
		return "确认了:" + c + "(" + ans.Content + ")", nil
	})
	var flaked atomic.Bool
	host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, c string) (string, error) {
		return compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (string, error) {
			events, err := agent_v2.RunSubAgent(ctx, "ask", asker, schema.UserMessage(c))
			if err != nil {
				return "", err
			}
			if flaked.CompareAndSwap(false, true) {
				return "", errors.New("后面一步抖了一下")
			}
			return nestedFinal(events), nil
		})
	})
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在子 agent 的提问，got %v", err)
	}
	events, err := host.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}),
		agent_v2.WithResumeSession(sess))
	if err != nil {
		t.Fatalf("续跑失败：%v——重试把已完成的调用又跑了一遍", err)
	}
	if got := nestedFinal(events); got != "确认了:go(好)" {
		t.Fatalf("最终产出 = %q", got)
	}
	// 首跑一次（停在提问）+ 续跑里答完一次；重试那次拿回记下的结果
	if n := askerRuns.Load(); n != 2 {
		t.Errorf("子 agent 的节点应跑 2 次，实际 %d", n)
	}
}

// TestNestedCall_IdenticalInputsEachKeepTheirOwnCall：同一个节点里对同一个子 agent 用**同一份
// 输入**调三次（投票），第二次停在提问上。续跑时三次各自接回自己的东西：第一次拿回记下的结果、
// 第二次续上自己的存档、第三次新开——每次调用的第一个节点恰好各跑一次。
func TestNestedCall_IdenticalInputsEachKeepTheirOwnCall(t *testing.T) {
	var seq atomic.Int32
	firstRuns := &countLog{}
	voter := newTwoNodeAgent(t, "voter",
		func(_ context.Context, _ string) (string, error) {
			n := seq.Add(1)
			firstRuns.inc(fmt.Sprint(n))
			return fmt.Sprint(n), nil
		},
		func(ctx context.Context, _, n string) (string, error) {
			if n != "2" {
				return "票" + n, nil
			}
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "第 2 票投什么？"})
			if err != nil {
				return "", err
			}
			return "票2(" + ans.Content + ")", nil
		})
	host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
		votes := make([]string, 0, 3)
		for i := 0; i < 3; i++ {
			events, err := agent_v2.RunSubAgent(ctx, "vote", voter, schema.UserMessage("同一个问题"))
			if err != nil {
				return "", err
			}
			votes = append(votes, nestedFinal(events))
		}
		return strings.Join(votes, " | "), nil
	})
	final := driveToEnd(t, host, func(*compose_v2.AskQuery) string { return "赞成" })
	if want := "票1 | 票2(赞成) | 票3"; final != want {
		t.Fatalf("最终产出 = %q，want %q", final, want)
	}
	if n := seq.Load(); n != 3 {
		t.Errorf("三次调用的第一个节点合计应恰好跑 3 次（已完成的不重跑、挂起的续自己的存档），实际 %d 次", n)
	}
}

// volatileStartHook 在 OnAgentStart 里给输入加一个每次都不同的后缀（模拟钩子往输入里塞
// 时间戳、请求号）。replace 时换一条新消息，否则原地改写调用方交来的那条。
type volatileStartHook struct {
	hook.NoOpHooks
	n       atomic.Int32
	replace bool
}

func (h *volatileStartHook) OnAgentStart(ctx context.Context, _ *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	if len(in.Messages) == 0 || in.Messages[len(in.Messages)-1] == nil {
		return ctx, nil
	}
	suffix := fmt.Sprintf("#%d", h.n.Add(1))
	last := in.Messages[len(in.Messages)-1]
	if h.replace {
		in.Messages[len(in.Messages)-1] = schema.UserMessage(last.Content + suffix)
	} else {
		last.Content += suffix
	}
	return ctx, nil
}

// TestNestedCall_IdentityTakenBeforeAgentStartHook：子 agent 自己的 OnAgentStart 钩子每次都往
// 输入里加点不同的东西。父节点重跑时给的是同一句话——仍是同一次调用、续上自己的存档：身份取的
// 是调用方交来的那条、在钩子改写之前。钩子原地改写同一个指针时也一样。
//
// 身份若取在钩子改写之后，每次重跑都成了一次新调用：换个提问身份重问，永不收敛。
func TestNestedCall_IdentityTakenBeforeAgentStartHook(t *testing.T) {
	capture := installWarnCapture(t)
	for _, replace := range []bool{false, true} {
		name := "原地改写"
		if replace {
			name = "换一条消息"
		}
		t.Run(name, func(t *testing.T) {
			h := &volatileStartHook{replace: replace}
			b := compose_v2.NewBuilder()
			inCh := compose_v2.LastValue[*schema.Message]("in")
			outCh := compose_v2.LastValue[*schema.Message]("out")
			compose_v2.WithChannel(b, inCh)
			compose_v2.WithChannel(b, outCh)
			b.AddNode("work", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"})
				if err != nil {
					return nil, err
				}
				return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("ok:"+ans.Content))}, nil
			})
			b.AddEdge(compose_v2.StartNode, "work")
			b.AddEdge("work", compose_v2.EndNode)
			g, err := b.Compile()
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			sub := agent_v2.NewGraphAgent(g,
				agent_v2.WithGraphAgentName("sub"),
				agent_v2.WithGraphAgentHooks(h),
				agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
					return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
				}),
				agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
					out, _ := compose_v2.Get(chs, outCh)
					return out
				}),
			)
			host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
				events, err := agent_v2.RunSubAgent(ctx, "sub", sub, schema.UserMessage("同样的任务"))
				if err != nil {
					return "", err
				}
				return nestedFinal(events), nil
			})
			cper := compose_v2.NewInMemoryCheckpointer()
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
			sess := agent_v2.NewRunSession()
			_, err = host.Run(ctx, sess, schema.UserMessage("go"))
			ie, ok := compose_v2.IsInterrupt(err)
			if !ok {
				t.Fatalf("首跑应停在提问，got %v", err)
			}
			before := len(capture.digestWarnings())
			events, err := host.Resume(ctx, sess.RunID,
				agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}),
				agent_v2.WithResumeSession(sess))
			if err != nil {
				t.Fatalf("续跑应收敛，got %v——钩子加的后缀被算进了身份，续跑成了一次新调用", err)
			}
			if got := nestedFinal(events); got != "ok:好" {
				t.Fatalf("最终产出 = %q", got)
			}
			// 前提：钩子两次确实给了不同的东西（否则这条用例测不到「取在钩子之前」）
			if h.n.Load() < 2 {
				t.Fatalf("前提不成立：钩子应在首跑与续跑各跑一次，实际 %d 次", h.n.Load())
			}
			if got := capture.digestWarnings()[before:]; len(got) != 0 {
				t.Fatalf("父节点给的是同一句话，不该告警，got %v", got)
			}
		})
	}
}

// chainHost 造一个链式调用的父节点：起草（像模型一样每次产出不同）→ 审批（问用户）→ 父节点
// 自己再问一句「发布吗」。drafts 数起草实际执行了几次。
func chainHost(t *testing.T, drafts *atomic.Int32, asked *inputLog) agent_v2.Agent {
	t.Helper()
	drafter := newSingleNodeAgent(t, "drafter", "d", nil, func(_ context.Context, _ string) (string, error) {
		return fmt.Sprintf("草稿v%d", drafts.Add(1)), nil
	})
	approver := approvingWorker(t, asked)
	return newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
		ev, err := agent_v2.RunSubAgent(ctx, "drafter", drafter, schema.UserMessage("写草稿"))
		if err != nil {
			return "", err
		}
		draft := nestedFinal(ev)
		ev, err = agent_v2.RunSubAgent(ctx, "approver", approver, schema.UserMessage(draft))
		if err != nil {
			return "", err
		}
		verdict := nestedFinal(ev)
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "发布「" + draft + "」吗？"})
		if err != nil {
			return "", err
		}
		return verdict + " / 发布:" + draft + "(" + ans.Content + ")", nil
	})
}

// TestNestedCall_ChainedCallsConverge：节点里链式调用——起草 → 审批（问用户）→ 父节点问「发布
// 吗」。起草每次执行产出都不同；两次续跑都拿回起草记下的结果，审批与发布针对的始终是用户看过的
// 那一版草稿，每个问题只问一次。
//
// 不记住已完成的调用时：续跑里起草重跑、产出变了，审批这次调用的输入跟着变——按位置定身份时
// 它接上旧存档、用户对 v1 的批准被套到 v2 上；续第二问时审批（已完成、存档已清）又从头跑，
// 提问身份按位置算与上次相同，用户对 v1 的「是」直接批准了一版用户从没看过的草稿（审计实测）。
func TestNestedCall_ChainedCallsConverge(t *testing.T) {
	var drafts atomic.Int32
	asked := &inputLog{}
	final := driveToEnd(t, chainHost(t, &drafts, asked), func(*compose_v2.AskQuery) string { return "是" })
	if want := "批准了:草稿v1 / 发布:草稿v1(是)"; final != want {
		t.Fatalf("最终产出 = %q，want %q", final, want)
	}
	if n := drafts.Load(); n != 1 {
		t.Errorf("起草应只执行 1 次（续跑拿回记下的结果），实际 %d 次", n)
	}
	if got := strings.Join(asked.snapshot(), ","); got != "草稿v1" {
		t.Errorf("审批只该问一次、问的是草稿v1，依次问了 %s", got)
	}
}

// TestNestedCall_RetryAttemptBackToSavedInput：续跑里 RunWithRetry 的第 1 次尝试换了输入——那是
// 另一次调用，从头跑、瞬时失败；第 2 次尝试的输入回到原值，续上原值那次调用的存档、用上答案。
func TestNestedCall_RetryAttemptBackToSavedInput(t *testing.T) {
	var resuming atomic.Bool
	var attempt atomic.Int32
	var secondCalls atomic.Int32
	child := newTwoNodeAgent(t, "child",
		func(_ context.Context, c string) (string, error) { return "prep:" + c, nil },
		func(ctx context.Context, c, _ string) (string, error) {
			// 续跑里第 1 次尝试（总第 2 次进这个节点）瞬时失败
			if secondCalls.Add(1) == 2 {
				return "", errors.New("瞬时失败")
			}
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认？"})
			if err != nil {
				return "", err
			}
			return "done:" + c + "(" + ans.Content + ")", nil
		})
	host := newSingleNodeAgent(t, "host", "host", nil, func(ctx context.Context, _ string) (string, error) {
		return compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (string, error) {
			in := "v1"
			if resuming.Load() && attempt.Add(1) == 1 {
				in = "v2"
			}
			events, err := agent_v2.RunSubAgent(ctx, "c", child, schema.UserMessage(in))
			if err != nil {
				return "", err
			}
			return nestedFinal(events), nil
		})
	})
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	_, err := host.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在提问，got %v", err)
	}
	resuming.Store(true)
	events, err := host.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}),
		agent_v2.WithResumeSession(sess))
	if err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	if got := nestedFinal(events); got != "done:v1(好)" {
		t.Fatalf("最终产出 = %q，want 续存档里的 v1 并用上答案", got)
	}
}
