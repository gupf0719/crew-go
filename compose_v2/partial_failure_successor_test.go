package compose_v2_test

import (
	"context"
	"sync"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// runCounter 并发安全的节点执行计数（末超步 [X,Y] 走 executeFrontier goroutine
// 并发——plain map 会 data race，audit² M2 实证 -race -count=10 FAIL）。
type runCounter struct {
	mu sync.Mutex
	m  map[string]int
}

func newRunCounter() *runCounter { return &runCounter{m: map[string]int{}} }
func (c *runCounter) inc(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[id]++
}
func (c *runCounter) get(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[id]
}

// 四编排三方对照 review PA-H1 回归：发散 fan-out 部分失败时，成功分支的后继出边 +
// SendDirectives 必须在 Resume 重跑成功后恢复（cp.CompletedFrontier / cp.DeferredSends），
// 否则成功分支后继静默不跑、graph 假成功。
//
// sanity-revert：删 graph_loop 失败路径 appendCompletedSiblingsLocked（或成功路径
// peekCompletedSiblings 合并）→ TestR33_PartialFailure_DivergentSuccessorPreserved
// fail（X 没跑）。

// buildDivergentGraph 构造 split → {A→X, B→Y} 发散拓扑；B 首跑抛 interrupt。
// 返回 graph + 各节点执行计数器指针。
func buildDivergentGraph(t *testing.T, name string) (*compose_v2.Graph, *runCounter) {
	t.Helper()
	runs := newRunCounter()
	mark := func(id string) compose_v2.NodeFunc {
		return func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			runs.inc(id)
			return nil, nil
		}
	}
	b := compose_v2.NewBuilder().WithName(name).
		AddNode("split", mark("split")).
		AddNode("A", mark("A")).
		AddNode("B", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "continue?"}); err != nil {
				return nil, err
			}
			runs.inc("B")
			return nil, nil
		}).
		AddNode("X", mark("X")).
		AddNode("Y", mark("Y")).
		AddEdge(compose_v2.StartNode, "split").
		AddEdge("split", "A").
		AddEdge("split", "B").
		AddEdge("A", "X").
		AddEdge("B", "Y").
		AddEdge("X", compose_v2.EndNode).
		AddEdge("Y", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return g, runs
}

// TestR33_PartialFailure_DivergentSuccessorPreserved 核心场景：A 成功 + B 中断 →
// Resume 答 B → X（A 的后继）必须执行且 A 不重跑。
func TestR33_PartialFailure_DivergentSuccessorPreserved(t *testing.T) {
	g, runs := buildDivergentGraph(t, "r33_pf_div")
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	cm := compose_v2.NewChannelManager()
	err := compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("pf_sid1"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expect interrupt, got %v", err)
	}

	// cp 断言：CurrentFrontier=[B 失败成员] + CompletedFrontier=[A 成功成员]
	cp, _ := cper.LoadNS(ctx, "pf_sid1", "")
	if cp == nil {
		t.Fatal("no cp")
	}
	if len(cp.CurrentFrontier) != 1 || cp.CurrentFrontier[0].NodeID != "B" {
		t.Fatalf("CurrentFrontier 应 [B]，got %v", cp.CurrentFrontier)
	}
	if len(cp.CompletedFrontier) != 1 || cp.CompletedFrontier[0].NodeID != "A" {
		t.Fatalf("PA-H1: cp.CompletedFrontier 应记录成功成员 [A]，got %v", cp.CompletedFrontier)
	}

	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm2,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("pf_sid1"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("yes")}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	if got := runs.get("A"); got != 1 {
		t.Errorf("成功分支 A 不该重跑（writes 已 commit），ran %d 次", got)
	}
	if got := runs.get("B"); got != 1 {
		t.Errorf("B Resume 后应跑 1 次，got %d", got)
	}
	if got := runs.get("X"); got != 1 {
		t.Errorf("PA-H1: X（成功分支 A 的后继）Resume 后应执行，ran %d 次（=0 即后继静默丢失 graph 假成功）", got)
	}
	if got := runs.get("Y"); got != 1 {
		t.Errorf("Y（B 的后继）应执行，ran %d 次", got)
	}
}

// TestR33_PartialFailure_DeferredSendsReplayed：成功分支 A 通过 SendFrom 动态投递
// Z（带 payload），B 中断——Resume 后 Z 必须被调度且 payload 生效。
func TestR33_PartialFailure_DeferredSendsReplayed(t *testing.T) {
	payloadCh := compose_v2.LastValue[string]("__r33_pf_payload__")
	gotPayload := ""
	ranZ := 0

	b := compose_v2.NewBuilder().WithName("r33_pf_sends")
	compose_v2.WithChannel(b, payloadCh)
	b.AddNode("split", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	}).
		AddNode("A", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			// 成功分支动态投递 Z + payload
			compose_v2.SendFrom(ctx, compose_v2.SendDirective{
				Target:  "Z",
				Payload: []compose_v2.ChannelWrite{compose_v2.Write(payloadCh, "FROM_A")},
			})
			return nil, nil
		}).
		AddNode("B", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "continue?"}); err != nil {
				return nil, err
			}
			return nil, nil
		}).
		AddNode("Z", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			ranZ++
			gotPayload, _ = compose_v2.Get(chs, payloadCh)
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "split").
		AddEdge("split", "A").
		AddEdge("split", "B").
		AddEdge("B", compose_v2.EndNode).
		AddEdge("Z", compose_v2.EndNode)
	// A 无静态出边——后继完全靠 SendFrom 动态投递
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()
	cm := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("pf_sid2"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expect interrupt, got %v", err)
	}

	// cp 断言：DeferredSends 持久化了 A 的动态投递
	cp, _ := cper.LoadNS(ctx, "pf_sid2", "")
	if cp == nil || len(cp.DeferredSends) != 1 || cp.DeferredSends[0].Target != "Z" {
		t.Fatalf("PA-H1: cp.DeferredSends 应记录成功分支的 SendFrom [Z]，got %+v", cp.DeferredSends)
	}

	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm2,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("pf_sid2"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("yes")}))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if ranZ != 1 {
		t.Fatalf("PA-H1: Z（A 的 SendFrom 动态后继）Resume 后应执行 1 次，got %d", ranZ)
	}
	if gotPayload != "FROM_A" {
		t.Fatalf("PA-H1: deferred send 的 payload 应生效（FROM_A），got %q", gotPayload)
	}
}

// TestR33_PartialFailure_SecondInterruptAccumulates：B 第一次 Resume 后又抛新
// interrupt（第二问）——CompletedFrontier 累积语义下 A 的后继 X 在最终完成时
// 仍恰执行 1 次（不因二次中断丢失、也不重复执行）。
func TestR33_PartialFailure_SecondInterruptAccumulates(t *testing.T) {
	runs := newRunCounter()
	mark := func(id string) compose_v2.NodeFunc {
		return func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			runs.inc(id)
			return nil, nil
		}
	}
	b := compose_v2.NewBuilder().WithName("r33_pf_two").
		AddNode("split", mark("split")).
		AddNode("A", mark("A")).
		AddNode("B", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "first?"}); err != nil {
				return nil, err
			}
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "second?"}); err != nil {
				return nil, err
			}
			runs.inc("B")
			return nil, nil
		}).
		AddNode("X", mark("X")).
		AddNode("Y", mark("Y")).
		AddEdge(compose_v2.StartNode, "split").
		AddEdge("split", "A").
		AddEdge("split", "B").
		AddEdge("A", "X").
		AddEdge("B", "Y").
		AddEdge("X", compose_v2.EndNode).
		AddEdge("Y", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// Run 1：A 成功 + B 抛第一问
	cm := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("pf_sid3"))
	ie1, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("Run1 expect interrupt, got %v", err)
	}

	// Resume 1：答第一问 → B 又抛第二问（CompletedFrontier 须保留 A）
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm2,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("pf_sid3"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie1.AllQueries()[0].InterruptID: schema.UserMessage("yes")}))
	ie2, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("Resume1 expect second interrupt, got %v", err)
	}
	cp, _ := cper.LoadNS(ctx, "pf_sid3", "")
	if cp == nil || len(cp.CompletedFrontier) != 1 || cp.CompletedFrontier[0].NodeID != "A" {
		t.Fatalf("二次中断后 CompletedFrontier 应仍保留 [A]（累积语义），got %+v", cp)
	}

	// Resume 2：答第二问 → 全图完成
	cm3 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm3,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("pf_sid3"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{
			ie1.AllQueries()[0].InterruptID: schema.UserMessage("yes"),
			ie2.AllQueries()[0].InterruptID: schema.UserMessage("yes"),
		}))
	if err != nil {
		t.Fatalf("Resume2: %v", err)
	}
	if got := runs.get("A"); got != 1 {
		t.Errorf("A 应恰跑 1 次（不因两次 Resume 重跑），got %d", got)
	}
	if got := runs.get("X"); got != 1 {
		t.Errorf("X 应恰跑 1 次（二次中断不丢失也不重复），got %d", got)
	}
	if got := runs.get("Y"); got != 1 {
		t.Errorf("Y 应恰跑 1 次，got %d", got)
	}
}

// TestR33_EdgeFailure_CompletedPreserved 复现 audit² H1：take 改 peek 前，
// Resume 重跑成功 → take 清空 completed → cond 业务错 → SaveNS 持久化空字段 →
// 下次 Resume 后 X 永久丢失（PA-H1 复活）。
//
// 拓扑：split → {A→X, B→cond→Y}；B 首跑 interrupt；B 的 cond 首次评估返业务错。
// sanity-revert：peek 改回 take（清空提前）→ Run2 后 cp.CompletedFrontier 空 → fail。
func TestR33_EdgeFailure_CompletedPreserved(t *testing.T) {
	runs := newRunCounter()
	condCalls := 0
	b := compose_v2.NewBuilder().WithName("r33_pf_edgefail").
		AddNode("split", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddNode("A", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			runs.inc("A")
			return nil, nil
		}).
		AddNode("B", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "go?"}); err != nil {
				return nil, err
			}
			runs.inc("B")
			return nil, nil
		}).
		AddNode("X", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			runs.inc("X")
			return nil, nil
		}).
		AddNode("Y", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			runs.inc("Y")
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "split").
		AddEdge("split", "A").
		AddEdge("split", "B").
		AddEdge("A", "X").
		AddEdge("X", compose_v2.EndNode).
		AddEdge("Y", compose_v2.EndNode)
	// transient 错 + Resume 重跑 B 让 cond 重评 —— CondFailRerunFrom 的典型场景，故显式声明。
	// （默认 CondFailAbort 下 Run2 的 cond 报错不存新 cp，测试会读到 Run1 的旧 cp 而"侥幸通过"——
	// 那是假绿：测的就不再是"cond 失败后 completed 记录是否保留"了。）
	b.AddConditionalEdge("B", func(_ context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		condCalls++
		if condCalls == 1 {
			return "", context.DeadlineExceeded // 首次评估 transient 业务错
		}
		return "Y", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// Run1：A 成功 + B 中断 → completed=[A]
	cm := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("ef_sid"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("Run1 expect interrupt, got %v", err)
	}

	// Run2（Resume）：B 成功 → cond 业务错 → completed 必须仍在 cp（peek 非破坏性）
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm2,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("ef_sid"),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("yes")}))
	if err == nil {
		t.Fatal("Run2 expect cond business error")
	}
	cp, _ := cper.LoadNS(ctx, "ef_sid", "")
	if cp == nil || len(cp.CompletedFrontier) == 0 {
		t.Fatalf("audit² H1: cond 失败后 cp.CompletedFrontier 应保留 [A]（peek 非破坏性），got %+v", cp)
	}

	// Run3（再 Resume）：cond 二次评估成功 → 全图完成，X 必须执行。B 被否决之后重做是新的一代，
	// 它再问的「go?」是个新问题（见 InterruptSite.RerunGen），照挂起的那条答完即可——本用例钉的
	// 是 completed 记录，不管问几次。
	answers := map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("yes")}
	for i := 0; i < 3; i++ {
		err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(),
			compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("ef_sid"),
			compose_v2.WithResumeAnswers(answers))
		ie3, again := compose_v2.IsInterrupt(err)
		if !again {
			break
		}
		for _, q := range ie3.AllQueries() {
			answers[q.InterruptID] = schema.UserMessage("yes")
		}
	}
	if err != nil {
		t.Fatalf("Run3: %v", err)
	}
	if got := runs.get("X"); got != 1 {
		t.Fatalf("audit² H1: X（A 后继）应恰执行 1 次（taken 不因 cond 失败丢失），got %d", got)
	}
	if got := runs.get("Y"); got != 1 {
		t.Fatalf("Y 应恰执行 1 次，got %d", got)
	}
}

// TestR33_EdgeFailure_FullSuccessSiblingPreserved 复现 audit² M1：全成功超步
// （FirstErr==nil，appendCompletedSiblingsLocked 失败路径没跑）+ cond 阶段失败 →
// 兄弟成功成员身份丢失 → Resume 后其后继静默不跑。
//
// 拓扑：start → {A→cond→X, B→Y}；A/B 都成功；A 的 cond **首次评估返 transient 业务错**、
// 二次成功。
//
// **触发方式变更**（cond 禁 interrupt 重构）：原用「A 的 cond 抛 InterruptError」触发 —
// 该形态已随「cond 禁止 interrupt」协议废止（cond interrupt 会把已成功成员拉回重跑、
// 违反 PA-H1，详见 graph_loop.go cond 分支）。改用 **cond 业务错**触发**同一条** M1
// 回填路径（graph_loop 的 edge 失败回填对任何 cond 失败都跑），M1 断言原样保留。
//
// sanity-revert：删主循环 edge 失败回填块 → Y 不跑 → fail。
func TestR33_EdgeFailure_FullSuccessSiblingPreserved(t *testing.T) {
	runs := newRunCounter()
	condCalls := 0
	b := compose_v2.NewBuilder().WithName("r33_pf_fullsucc").
		AddNode("start", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			return nil, nil
		}).
		AddNode("A", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			runs.inc("A")
			return nil, nil
		}).
		AddNode("B", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			runs.inc("B")
			return nil, nil
		}).
		AddNode("X", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			runs.inc("X")
			return nil, nil
		}).
		AddNode("Y", func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
			runs.inc("Y")
			return nil, nil
		}).
		AddEdge(compose_v2.StartNode, "start").
		AddEdge("start", "A").
		AddEdge("start", "B").
		AddEdge("B", "Y").
		AddEdge("X", compose_v2.EndNode).
		AddEdge("Y", compose_v2.EndNode)
	// transient 错 + Resume 重跑 A 让 cond 重评 —— CondFailRerunFrom 的典型场景，
	// 故显式声明（默认 CondFailAbort 会清 frontier + 不存 cp，无从 Resume 测 M1）。
	b.AddConditionalEdge("A", func(_ context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		condCalls++
		if condCalls == 1 {
			return "", context.DeadlineExceeded // 首次评估 transient 业务错
		}
		return "X", nil
	}, compose_v2.WithCondFailPolicy(compose_v2.CondFailRerunFrom))
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := context.Background()

	// Run1：A/B 全成功 → A 的 cond 业务错 → cp 应记录兄弟 B 进 CompletedFrontier
	cm := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fs_sid"))
	if err == nil {
		t.Fatalf("Run1 expect cond transient error")
	}
	cp, _ := cper.LoadNS(ctx, "fs_sid", "")
	if cp == nil {
		t.Fatal("no cp")
	}
	foundB := false
	for _, m := range cp.CompletedFrontier {
		if m.NodeID == "B" {
			foundB = true
		}
	}
	if !foundB {
		t.Fatalf("audit² M1: 全成功超步 cond 失败后 cp.CompletedFrontier 应含兄弟 B，got %v", cp.CompletedFrontier)
	}

	// Resume（auto-load）：A 重跑 → cond 二次评估成功 → X 执行；B 不重跑但 Y 必须执行
	cm2 := compose_v2.NewChannelManager()
	err = compose_v2.Run(ctx, g, cm2,
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("fs_sid"))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got := runs.get("B"); got != 1 {
		t.Errorf("兄弟 B 不该重跑，ran %d 次", got)
	}
	if got := runs.get("Y"); got != 1 {
		t.Errorf("audit² M1: Y（兄弟 B 的后继）应恰执行 1 次（身份不丢失），got %d", got)
	}
	if got := runs.get("X"); got != 1 {
		t.Errorf("X 应恰执行 1 次，got %d", got)
	}
}
