package compose_v2_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestStepResult_SurvivesInterruptAcrossCheckpoint：节点里先做完一步（记下结果），再在第二步
// 提问中断；续跑时节点重跑，第一步读回记下的结果、不再执行。
//
// 承重的是「存档投影把步骤记录一起导出」：投影只导出节点执行键那一条的话，步骤记录随中断
// 整条掉出存档，续跑时第一步会再做一遍。
func TestStepResult_SurvivesInterruptAcrossCheckpoint(t *testing.T) {
	var sideEffects atomic.Int32
	b := compose_v2.NewBuilder()
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if _, ok := compose_v2.GetStepResult[string](ctx, "charge"); !ok {
			sideEffects.Add(1) // 扣款
			if err := compose_v2.SaveStepResult(ctx, "charge", "扣过了"); err != nil {
				return nil, err
			}
		}
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "发货吗？"}); err != nil {
			return nil, err
		}
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("s")}

	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在提问上，got %v", err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		append(opts, compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("好")}))...); err != nil {
		t.Fatalf("续跑失败：%v", err)
	}
	if n := sideEffects.Load(); n != 1 {
		t.Fatalf("已做完的那一步应只执行 1 次（续跑读回记下的结果），实际 %d 次", n)
	}
}

// TestStepResult_KeysAreIndependentOfNodeState：同一次节点执行里，业务那一个 SaveNodeState 槽与
// 按 key 记下的步骤互不覆盖，不同 key 之间也互不覆盖。
func TestStepResult_KeysAreIndependentOfNodeState(t *testing.T) {
	var got struct {
		state string
		a, b  string
		okA   bool
		okB   bool
		okMis bool
	}
	b := compose_v2.NewBuilder()
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if err := compose_v2.SaveNodeState(ctx, "节点进度"); err != nil {
			return nil, err
		}
		if err := compose_v2.SaveStepResult(ctx, "a", "A 的结果"); err != nil {
			return nil, err
		}
		if err := compose_v2.SaveStepResult(ctx, "b", "B 的结果"); err != nil {
			return nil, err
		}
		got.state, _ = compose_v2.GetNodeState[string](ctx)
		got.a, got.okA = compose_v2.GetStepResult[string](ctx, "a")
		got.b, got.okB = compose_v2.GetStepResult[string](ctx, "b")
		_, got.okMis = compose_v2.GetStepResult[string](ctx, "c")
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got.state != "节点进度" || got.a != "A 的结果" || got.b != "B 的结果" || !got.okA || !got.okB {
		t.Fatalf("各槽应互不覆盖，got %+v", got)
	}
	if got.okMis {
		t.Errorf("没记过的步骤不该读到东西")
	}
}

// TestStepResult_OutsideNodeIsNoop：不在节点执行里（图外直接调）时，记是 no-op、读恒为没有。
func TestStepResult_OutsideNodeIsNoop(t *testing.T) {
	if err := compose_v2.SaveStepResult(context.Background(), "k", "v"); err != nil {
		t.Fatalf("节点外记应是 no-op，got %v", err)
	}
	if _, ok := compose_v2.GetStepResult[string](context.Background(), "k"); ok {
		t.Fatalf("节点外读应恒为没有")
	}
}

// hookPhaseWriter 在嵌套图的每个图级钩子相位里都试着写节点私有态。
type hookPhaseWriter struct {
	compose_v2.NoOpOrchestrationHooks
}

func (hookPhaseWriter) write(ctx context.Context, who string) {
	_ = compose_v2.SaveNodeState(ctx, who)
	_ = compose_v2.SaveStepResult(ctx, "k", who)
}

func (h hookPhaseWriter) OnGraphStart(ctx context.Context, _ compose_v2.Channels) (context.Context, error) {
	h.write(ctx, "OnGraphStart")
	return ctx, nil
}

func (h hookPhaseWriter) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, _ error) context.Context {
	h.write(ctx, "OnGraphEnd")
	return ctx
}

func (h hookPhaseWriter) OnNodeStart(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	h.write(ctx, "OnNodeStart")
	return ctx, nil
}

func (h hookPhaseWriter) OnNodeEnd(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels, _ error) context.Context {
	h.write(ctx, "OnNodeEnd")
	return ctx
}

// TestNodeState_NestedGraphHookPhaseDoesNotWriteOuterNode：节点里跑一张嵌套图，嵌套图的图级
// 钩子相位（以及条件边）里写节点私有态不会落到外层节点名下。
//
// 访问器绑的是外层节点与外层 cm，曾经进新图时不清：嵌套图 OnGraphStart 里的一次
// SaveNodeState 把外层节点自己记的进度覆盖掉了（实测），`err == nil`。
func TestNodeState_NestedGraphHookPhaseDoesNotWriteOuterNode(t *testing.T) {
	ib := compose_v2.NewBuilder()
	ib.AddNode("in", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	ib.AddEdge(compose_v2.StartNode, "in")
	ib.AddConditionalEdge("in", func(ctx context.Context, _ compose_v2.Channels) (compose_v2.NodeID, error) {
		hookPhaseWriter{}.write(ctx, "cond")
		return compose_v2.EndNode, nil
	})
	inner, err := ib.Compile()
	if err != nil {
		t.Fatalf("compile inner: %v", err)
	}

	var state, step string
	var stepOK bool
	ob := compose_v2.NewBuilder()
	ob.AddNode("outer", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if err := compose_v2.SaveNodeState(ctx, "outer 自己的进度"); err != nil {
			return nil, err
		}
		if err := compose_v2.Run(ctx, inner, compose_v2.NewChannelManager(),
			compose_v2.WithHooks(hookPhaseWriter{}), compose_v2.WithRunNS("sub")); err != nil {
			return nil, err
		}
		state, _ = compose_v2.GetNodeState[string](ctx)
		step, stepOK = compose_v2.GetStepResult[string](ctx, "k")
		return nil, nil
	})
	ob.AddEdge(compose_v2.StartNode, "outer")
	ob.AddEdge("outer", compose_v2.EndNode)
	outer, err := ob.Compile()
	if err != nil {
		t.Fatalf("compile outer: %v", err)
	}
	if err := compose_v2.Run(context.Background(), outer, compose_v2.NewChannelManager()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if state != "outer 自己的进度" {
		t.Errorf("外层节点的进度被嵌套图的钩子相位改成了 %q", state)
	}
	if stepOK {
		t.Errorf("嵌套图钩子相位里记的步骤落到了外层节点名下：%q", step)
	}
}

// TestStepResult_UnregisteredTypeErrors：值的类型没注册时记不下来，返 error 而不是静默丢。
func TestStepResult_UnregisteredTypeErrors(t *testing.T) {
	type notRegistered struct{ X int }
	var saveErr error
	b := compose_v2.NewBuilder()
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		saveErr = compose_v2.SaveStepResult(ctx, "k", notRegistered{X: 1})
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if saveErr == nil || errors.Is(saveErr, compose_v2.ErrRunAborted) {
		t.Fatalf("没注册的类型应返普通 error，got %v", saveErr)
	}
}
