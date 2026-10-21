package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// slowTool 睡到 ctx 被取消或睡满——**响应 ctx** 的那一类（HTTP / DB / 沙箱执行都是）。
type slowTool struct {
	name string
	dur  time.Duration
	ran  chan struct{}
}

func (s *slowTool) Name() string        { return s.name }
func (s *slowTool) Description() string { return "慢工具" }
func (s *slowTool) InputSchema() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{}}
}
func (s *slowTool) Call(ctx context.Context, _ map[string]any) (*tool.Result, error) {
	if s.ran != nil {
		close(s.ran)
	}
	select {
	case <-time.After(s.dur):
		return tool.Success("慢工具跑完了"), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TestToolTimeout_DeadlineReachesTool 钉住 [agent_v2.WithToolTimeout] 真的把 deadline
// 交到了工具手上。
//
// ⚠️ 断言选在**承重的维度**上：不是「Run 返回得快」（不设超时时慢工具跑完也会返回），
// 而是**工具收到了 ctx 取消**——它是这个 option 唯一的作用机制。响应 ctx 的工具会因此
// 提前返回 `context.DeadlineExceeded`。
func TestToolTimeout_DeadlineReachesTool(t *testing.T) {
	st := &slowTool{name: "slow", dur: 30 * time.Second}
	tk := agent_v2_newToolkit()
	tk.Register(st)
	hs := agent_v2.NewInMemoryHistoryStore()
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-to1")
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3),
		agent_v2.WithToolTimeout(40*time.Millisecond), agent_v2.WithHistory(hs, "t"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("slow"), schema.AssistantMessage("final")})),
	)
	start := time.Now()
	_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("超时是**这次工具调用**的失败，不该让整个 Run 失败: %v", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("等满了 %v——deadline 没交到工具手上", elapsed)
	}
	if got := lastToolResult(t, hs, ctx); !strings.Contains(got, "超时") {
		t.Errorf("模型看到的 tool_result 是 %q，期望是一条超时结果", got)
	}
	t.Logf("耗时 %v（deadline 40ms，工具本来要睡 30s）", elapsed)
}

// TestToolTimeout_ZeroMeansNoDeadline 钉住**默认零开销**：不设时工具拿到的 ctx 没有 deadline。
func TestToolTimeout_ZeroMeansNoDeadline(t *testing.T) {
	var hadDeadline bool
	probe := tool.NewFuncTool("probe", "探针", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			_, hadDeadline = ctx.Deadline()
			return tool.Success("ok"), nil
		})
	tk := agent_v2_newToolkit()
	tk.Register(probe)
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("probe"), schema.AssistantMessage("final")})),
	)
	if _, err := ag.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if hadDeadline {
		t.Error("没配 WithToolTimeout，工具却拿到了 deadline——默认该零开销")
	}
}

// stubbornTool **不看 ctx**——纯 CPU 循环、或者忘了 select 的实现都是这一类。
type stubbornTool struct{ dur time.Duration }

func (s *stubbornTool) Name() string        { return "stubborn" }
func (s *stubbornTool) Description() string { return "不看 ctx 的工具" }
func (s *stubbornTool) InputSchema() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{}}
}
func (s *stubbornTool) Call(context.Context, map[string]any) (*tool.Result, error) {
	time.Sleep(s.dur) // 故意不 select ctx.Done()
	return tool.Success("我跑完了，才不管你的 deadline"), nil
}

// TestToolTimeout_StubbornToolStillReportsTimeout 钉住**不看 ctx 的工具也不会静默通过**。
//
// ⚠️ 这条是这个 option 的承重用例。Go 没有安全中止 goroutine 的办法，所以框架**等它返回**
// ——但返回之后按超时处理：丢弃它那个迟到的成功、给模型一条超时的 tool_result、并 warn
// 点名这个工具。
//
// **一个不生效又不出声的超时比没有超时更坏**（业务以为有保护）。参照的那家在这个问题上
// 选择「同步节点根本不允许设 timeout」，Go 这边给不了那么干净的二分（是否响应 ctx 由实现
// 决定、框架看不出来），所以取的是让失效**可见**。
func TestToolTimeout_StubbornToolStillReportsTimeout(t *testing.T) {
	tk := agent_v2_newToolkit()
	tk.Register(&stubbornTool{dur: 80 * time.Millisecond})
	hs := agent_v2.NewInMemoryHistoryStore()
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-to2")
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3),
		agent_v2.WithToolTimeout(20*time.Millisecond), agent_v2.WithHistory(hs, "t"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("stubborn"), schema.AssistantMessage("final")})),
	)
	if _, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("不该让整个 Run 失败: %v", err)
	}
	toolMsg := lastToolResult(t, hs, ctx)
	if !strings.Contains(toolMsg, "超时") {
		t.Errorf("不看 ctx 的工具超了 deadline，模型看到的却是 %q——"+
			"它那个迟到的成功该被丢弃、换成超时结果，否则这个 option 在这类工具上静默失效", toolMsg)
	}
}

// lastToolResult 从历史里取最后一条 tool_result 的正文。
//
// ⚠️ **不能从 `Run` 返回的 events 里找**——`RunSession.Events` 只存最终 assistant，
// tool_result 从来不进（实测：一次带工具的 Run 只有 `[user, assistant]` 两条 event）。
func lastToolResult(t *testing.T, hs agent_v2.HistoryStore, ctx context.Context) string {
	t.Helper()
	msgs, err := hs.Load(ctx, agent_v2.HistoryKey{
		Owner: "t", ConvID: compose_v2.GetThreadID(ctx)})
	if err != nil {
		t.Fatalf("读历史: %v", err)
	}
	got := ""
	for _, m := range msgs {
		if schema.GetToolResultID(m) != "" {
			got = m.Content
		}
	}
	return got
}

var _ = errors.Is

// TestToolTimeout_DoesNotSwallowMustPropagate 钉住**超时收尾也是一个吞点，它必须问
// [compose_v2.MustPropagate]**。
//
// ⚠️ 独立审计抓到的真 bug：收尾那段排在 `if err != nil` **之前**且完全不看 err。
// 于是工具在超时窗口内抛的中断 / 终局失败 / 坏存档，全被换成一条超时 tool_result +
// `err=nil`。实测伤害：HITL 中断被吞后 `cp 挂起数=0`（人再也答不上了）；
// `ErrRunAborted` 被吞后工具被重跑 3 次（它的定义是「重跑必然一样」）。
//
// **这是「立一条新规矩，同一次要问哪几条路能到这儿」的复发**：吞不吞的守卫收敛在
// `MustPropagate` 上，而这个 feature 新开的出口一次都没调它。既有的机械对账闸也看不见
// ——它只 grep 误用 `IsControlFlowSignal` 的地方，**一个「谁都不问」的吞点对它是隐形的**。
func TestToolTimeout_DoesNotSwallowMustPropagate(t *testing.T) {
	for _, c := range []struct {
		name string
		mk   func() error
		want func(t *testing.T, err error)
	}{
		{"中断", func() error {
			return &compose_v2.InterruptError{Query: &compose_v2.AskQuery{
				InterruptID: "q1", Question: "确认?"}}
		}, func(t *testing.T, err error) {
			if _, ok := compose_v2.IsInterrupt(err); !ok {
				t.Errorf("中断被超时收尾吞掉了（err=%v）——挂起档不会落，人再也答不上", err)
			}
		}},
		{"终局失败", func() error {
			return fmt.Errorf("业务违反协议: %w", compose_v2.ErrRunAborted)
		}, func(t *testing.T, err error) {
			if !errors.Is(err, compose_v2.ErrRunAborted) {
				t.Errorf("终局失败被超时收尾吞掉了（err=%v）——它的定义是「重跑必然一样」，"+
					"被吞成工具失败之后模型会反复重试", err)
			}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			mkErr := c.mk
			tk := agent_v2_newToolkit()
			tk.Register(tool.NewFuncTool("slowfail", "超时窗口内抛哨兵",
				map[string]any{"type": "object"},
				func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
					<-ctx.Done() // 等到 deadline 过，再抛哨兵
					return nil, mkErr()
				}))
			ag := agent_v2.NewBasicAgent(
				agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3),
				agent_v2.WithToolTimeout(15*time.Millisecond),
				agent_v2.WithModel(newStubModel([]*schema.Message{
					oneToolCall("slowfail"), schema.AssistantMessage("final")})),
			)
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			_, err := ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
			c.want(t, err)
		})
	}
}

// TestToolTimeout_ParentDeadlineIsNotToolTimeout 钉住**调用方的 deadline 到期不算工具超时**。
//
// ⚠️ 派生 ctx **继承父的 deadline**，父被 deadline 杀掉时它的 `Err()` 同样是
// `DeadlineExceeded`。只看派生 ctx 就会把「整个 Run 超时」写成一句
// 「工具调用超时（超过 10m0s 未返回）」——而那句谎话会**落进会话历史**，
// 也就落进 Resume 之后模型读到的 prior。
//
// 而 CLAUDE.md 自己要求「用 compose_v2.Run + cp 时必须设 ctx deadline」——
// 这个组合是被规约推荐的常态，不是边角。
func TestToolTimeout_ParentDeadlineIsNotToolTimeout(t *testing.T) {
	hs := agent_v2.NewInMemoryHistoryStore()
	tk := agent_v2_newToolkit()
	tk.Register(&slowTool{name: "slow", dur: 30 * time.Second})
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("a"), agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(3),
		agent_v2.WithToolTimeout(10*time.Minute), // 远大于父的 deadline
		agent_v2.WithHistory(hs, "t"),
		agent_v2.WithModel(newStubModel([]*schema.Message{
			oneToolCall("slow"), schema.AssistantMessage("final")})),
	)
	ctx := compose_v2.WithThreadIDCtx(context.Background(), "conv-parent-dl")
	ctx, cancel := context.WithTimeout(ctx, 60*time.Millisecond)
	defer cancel()
	_, _ = ag.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("hi"))
	if got := lastToolResult(t, hs, ctx); strings.Contains(got, "超时") {
		t.Errorf("调用方 deadline 到期被写成了「工具超时」并落进历史：%q\n"+
			"工具的 deadline 是 10 分钟、根本没到——这句谎话会进 Resume 后模型读到的 prior", got)
	}
}
