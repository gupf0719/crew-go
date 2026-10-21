package agent_v2_test

// 子 agent 以 **tool 形态**嵌套时，中断错误链上的其他错误必须活着穿过 `makeToolsFn`。
//
// # 为什么这条会漏
//
// `makeToolsFn` 收集中断时曾只留 `IsInterrupt` 抠出来的裸 `*InterruptError`，把原始链整条
// 丢掉——链上挂着的别的错误就此蒸发。当初挂在上面的是 `ErrCheckpointStale`（引擎出口写档撞上
// 陈旧写回时把它跟中断一起 `%w`）；引擎现在不再产出这种形状（档没写成时返
// `ErrCheckpointNotPersisted`，中断不在链上），但业务自研的 agent、远程 agent 的壳仍可能在
// 中断的链上挂别的错误，tool 边界不能替它们剥掉。下面用一个业务自己的哨兵代表它。
//
// 走 NodeFunc 的嵌套（Sequential / Parallel / Loop / GraphAgent）不受影响，它们全程 `%w` 保链。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// errChildMarker 是子 agent 挂在中断错误链上的业务哨兵。
var errChildMarker = errors.New("child marker")

// doubleWrapAgent 返回一个中断，链上另挂一个业务哨兵。
type doubleWrapAgent struct{ name string }

func (a *doubleWrapAgent) Name() string        { return a.name }
func (a *doubleWrapAgent) Description() string { return "returns interrupt wrapped with a marker" }

func (a *doubleWrapAgent) Run(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	ie := &compose_v2.InterruptError{
		Reason: "child_ask",
		// ⚠️ ID 必须**带上自己的身份**：这个 stub 被 N 个并发工具共用，都给 "child_q"
		// 的话 MergeInterrupts 合出来的那一批里两条撞 ID ——第二条会被静默丢弃，
		// 而框架现在会当场拒掉整批（那正是 validateInterrupt 要抓的）。
		Query: &compose_v2.AskQuery{InterruptID: "child_q:" + a.name, Question: "?"},
	}
	return nil, fmt.Errorf("%w (%w)", ie, errChildMarker)
}

func (a *doubleWrapAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, errors.New("not impl")
}

// callNToolsModel 让父 agent 一轮调 N 个子 agent tool（N=1 走 inline、N>=2 走并发 fanout）
type callNToolsModel struct{ names []string }

func (m *callNToolsModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for _, ms := range msgs {
		if ms.Role == schema.RoleTool {
			return schema.AssistantMessage("done"), nil
		}
	}
	tcs := make([]schema.ToolCall, 0, len(m.names))
	for i, n := range m.names {
		tcs = append(tcs, schema.ToolCall{
			ID: fmt.Sprintf("tc%d", i), Type: "function",
			Function: schema.FunctionCall{Name: n, Arguments: `{"input":"go"}`},
		})
	}
	return schema.AssistantToolCallMessage(tcs), nil
}

func (m *callNToolsModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *callNToolsModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func TestSentinel_SurvivesToolBoundary(t *testing.T) {
	for _, tc := range []struct {
		name string
		subs []string
		why  string
	}{
		{"单 tool（inline 路径）", []string{"child_a"},
			"len==1 时直接返原始错误链"},
		{"多 tool 并发（fanout + MergeInterrupts）", []string{"child_a", "child_b"},
			"len>=2 时 merged 承载合并 Queries，原始链并列挂 errors.Join 保第二 sentinel"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := tool.NewToolkit()
			for _, n := range tc.subs {
				tk.Register(agent_v2.AgentAsTool(&doubleWrapAgent{name: n}))
			}
			parent := agent_v2.NewBasicAgent(
				agent_v2.WithName("parent"),
				agent_v2.WithModel(&callNToolsModel{names: tc.subs}),
				agent_v2.WithToolkit(tk),
				agent_v2.WithMaxRounds(3),
			)
			_, err := parent.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go"))

			ie, isInt := compose_v2.IsInterrupt(err)
			if !isInt {
				t.Fatalf("InterruptError 必须穿过 tool 边界（HITL 靠它挂起），got %v", err)
			}
			if len(ie.AllQueries()) != len(tc.subs) {
				t.Errorf("应带 %d 个 query（每个子 agent 一个），got %d",
					len(tc.subs), len(ie.AllQueries()))
			}
			if !errors.Is(err, errChildMarker) {
				t.Errorf("中断错误链上的业务哨兵在 tool 边界被剥掉了（%s）\n  err=%v", tc.why, err)
			}
		})
	}
}

// TestSentinel_GraphIDConflictNotFlattened：Run 出口 defer 里的 cp 写失败要保住哨兵。
//
// `SaveNS` 是 `ErrGraphIDConflict` 的两个产生点之一，而它的 godoc 明确承诺 errors.Is 友好；曾经
// 那条分支用 `%v`，业务判不出来。档没写成时返 `ErrCheckpointNotPersisted`：中断不在链上，写档的
// 原因在；槽里是另一张图的档，不带 `ErrRunAborted`（带的话上层出口会跟着清档，删掉别人的档）。
func TestSentinel_GraphIDConflictNotFlattened(t *testing.T) {
	inner := compose_v2.NewInMemoryCheckpointer()
	cper := &conflictOnSaveCheckpointer{Checkpointer: inner}

	sub := &doubleWrapAgent{name: "x"} // 借它产 interrupt 让 Run 走"存档"分支
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("gidc")).AddStep("s1", sub)

	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	sess.RunID = "gidc_sid"
	_, err := seq.Run(ctx, sess, schema.UserMessage("go"))

	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("存档没写成，不该再报告成中断（上层会请用户回答一个续不上的问题），got %v", err)
	}
	if !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) {
		t.Errorf("应返 ErrCheckpointNotPersisted，got %v", err)
	}
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) {
		t.Errorf("cp 写失败带的 ErrGraphIDConflict 被拍平了——godoc 承诺它 errors.Is 友好\n"+
			"  err=%v", err)
	}
	if errors.Is(err, compose_v2.ErrRunAborted) {
		t.Errorf("槽里是另一张图的档，不该带 ErrRunAborted（上层出口会跟着清档），got %v", err)
	}
}

// conflictOnSaveCheckpointer 让 SaveNS 恒返 ErrGraphIDConflict（模拟槽里是别的 graph 的 cp）
type conflictOnSaveCheckpointer struct{ compose_v2.Checkpointer }

func (c *conflictOnSaveCheckpointer) SaveNS(context.Context, string, string, *compose_v2.Checkpoint) error {
	return fmt.Errorf("%w: sid=%q existing=%q incoming=%q",
		compose_v2.ErrGraphIDConflict, "gidc_sid", "other_graph", "this_graph")
}

// TestSentinel_MultiInterrupt_ResumeClosesLoop：新错误形状下 HITL 闭环要能真正合上。
//
// 上面那条只验了「query 数量对」，但 query 记进 cp 之后还要**答得回去**——Resume 时
// 框架按 InterruptID 配对答案。如果 `errors.Join` 让 graph_loop 取到了错误的 ie（比如
// 原始链里只带一个 query 的那个），record 进 cp 的就只有一条，另一条永远没 ID 可配，
// 用户答完照样卡住。
//
// 走两个并发 `ask_user`（ISSUE-81 fanout 的原始动机场景），全程真跑 Run → Resume。
func TestSentinel_MultiInterrupt_ResumeClosesLoop(t *testing.T) {
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("multi_ask"),
		agent_v2.WithAskUser(),
		agent_v2.WithMaxRounds(5),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "a1", Type: "function", Function: schema.FunctionCall{
					Name: agent_v2.AskUserToolName, Arguments: `{"question":"颜色?"}`}},
				{ID: "a2", Type: "function", Function: schema.FunctionCall{
					Name: agent_v2.AskUserToolName, Arguments: `{"question":"尺寸?"}`}},
			}),
			schema.AssistantMessage("都收到了"),
		})),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()
	const sid = "multi_ask_sid"
	sess.RunID = sid

	_, err := ag.Run(ctx, sess, schema.UserMessage("下单"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("两个 ask_user 应触发中断, got %v", err)
	}
	qs := ie.AllQueries()
	if len(qs) != 2 {
		t.Fatalf("应带 2 个 query（每个 ask_user 一个），got %d——"+
			"取到了 Join 树里带单个 query 的那个 ie 就会是 1", len(qs))
	}

	// 两个 query 都答上——ID 必须是框架 record 进 cp 的那两个
	answers := map[string]*schema.Message{}
	for i, q := range qs {
		if q.InterruptID == "" {
			t.Fatalf("query[%d] 没有 InterruptID，答案无从配对", i)
		}
		answers[q.InterruptID] = schema.UserMessage("答案" + q.InterruptID)
	}

	events, rerr := ag.Resume(ctx, sid,
		agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(answers))
	if rerr != nil {
		t.Fatalf("两个答案都给了，Resume 应完成: %v\n"+
			"  若报 unmatched / 仍中断，说明 record 进 cp 的 query 与返回给业务的对不上", rerr)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content == "" {
		t.Errorf("Resume 应产出最终答复, got %v", final)
	}
}

// TestSentinel_TypedNilInterruptDoesNotEatMessage：typed-nil `*InterruptError` 混进
// `errors.Join` 后不能把整条文案吞掉。
//
// 业务 tool 写 `var ie *InterruptError; return nil, ie`（Go interface typed-nil 陷阱）
// 时 `IsInterrupt` 返 `(nil, true)`，于是这个 typed nil 被当成合法中断收进错误链。单条
// 链上无害（`%w` 只记 wrap 关系、不调 Error），但一旦跟别的错误一起进 `errors.Join`，
// `joinError.Error()` 会逐个调成员的 `Error()`——没有 nil 守卫就变成
// `%!v(PANIC=...)`，中断信息和 stale 提示全被吞掉。
//
// 不 crash（fmt 自带 recover）、语义也不受影响，纯粹毁诊断——而观测层会把这坨记进
// trace 的 error.message。
func TestSentinel_TypedNilInterruptDoesNotEatMessage(t *testing.T) {
	var typedNil *compose_v2.InterruptError
	real := &compose_v2.InterruptError{
		Reason: "ask", Query: &compose_v2.AskQuery{InterruptID: "q1", Question: "真问题?"},
	}
	joined := errors.Join(real, typedNil,
		fmt.Errorf("t3: %w", compose_v2.ErrCheckpointStale))

	msg := joined.Error()
	if strings.Contains(msg, "PANIC") {
		t.Errorf("typed-nil 成员把整条文案吞了: %s\n"+
			"  (*InterruptError).Error() 缺 nil 守卫——AllQueries / Validate 都有，它是唯一漏的", msg)
	}
	// 真中断的信息必须还在
	if !strings.Contains(msg, "ask") {
		t.Errorf("真中断的 Reason 应保留在文案里: %s", msg)
	}
	// 语义不受影响（这条是回归的对照，确认修复没改语义）
	if _, ok := compose_v2.IsInterrupt(joined); !ok {
		t.Error("IsInterrupt 应仍成立")
	}
	if !errors.Is(joined, compose_v2.ErrCheckpointStale) {
		t.Error("stale 应仍判得出")
	}
}
