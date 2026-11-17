package agent_v2_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

type terminalToolWriteError struct{}

func (*terminalToolWriteError) Error() string { return "工具已写入外部资源" }

func TestSequentialToolBatchTerminalCheckpointConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	sess := agent_v2.NewRunSession()
	var writes atomic.Int32
	afterPause := false

	buildChild := func(name string) *compose_v2.Graph {
		t.Helper()
		graph, err := compose_v2.NewBuilder().WithName(name).
			AddNode("approve", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续执行子图？"})
				return nil, err
			}).AddEdge(compose_v2.StartNode, "approve").AddEdge("approve", compose_v2.EndNode).Compile()
		if err != nil {
			t.Fatal(err)
		}
		return graph
	}
	original, incompatible := buildChild("child-v1"), buildChild("child-v2")
	child := original
	toolkit := tool.NewToolkit()
	inputSchema := map[string]any{"type": "object", "properties": map[string]any{"input": map[string]any{"type": "string"}}}
	toolkit.Register(tool.NewFuncTool("child", "执行子图", inputSchema, func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
		childNS := compose_v2.JoinNS(compose_v2.GetGraphNS(ctx), "approval")
		if err := compose_v2.Run(ctx, child, compose_v2.NewChannelManager(), compose_v2.WithRunNS(childNS)); err != nil {
			return nil, err
		}
		return tool.Success("子图完成"), nil
	}))
	toolkit.Register(tool.NewFuncTool("terminal", "执行写操作", inputSchema, func(context.Context, map[string]any) (*tool.Result, error) {
		if !afterPause {
			return nil, errors.New("写入前遇到暂时故障")
		}
		if writes.Add(1) == 1 {
			return nil, fmt.Errorf("%w: %w", compose_v2.ErrRunAborted, &terminalToolWriteError{})
		}
		return tool.Success("已写入"), nil
	}))
	basic := agent_v2.NewBasicAgent(agent_v2.WithName("worker"), agent_v2.WithToolkit(toolkit),
		agent_v2.WithModel(&callNToolsModel{names: []string{"child", "terminal"}}),
		agent_v2.WithMaxConcurrentTools(2), agent_v2.WithMaxRounds(3))
	agent := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("workflow")).AddStep("worker", basic)
	_, err := agent.Run(ctx, sess, schema.UserMessage("执行任务"))
	ie, interrupted := compose_v2.IsInterrupt(err)
	if !interrupted || len(ie.AllQueries()) != 1 || writes.Load() != 0 {
		t.Fatalf("应先挂起一条子图问题，写入尚未开始，实际 err=%v writes=%d", err, writes.Load())
	}

	slots, err := cp.LoadAllInSession(ctx, sess.RunID)
	if err != nil || len(slots) != 3 {
		t.Fatalf("应有顺序 Agent、BasicAgent 和子图的存档，实际 %d 份，err=%v", len(slots), err)
	}
	var childNS string
	before := make(map[string][]byte)
	for _, slot := range slots {
		wire, err := compose_v2.MarshalCheckpoint(slot.Checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		before[slot.NS] = wire
		if slot.Checkpoint.GraphID == original.CheckpointIdentity() {
			childNS = slot.NS
		}
	}
	if childNS == "" {
		t.Fatal("子图未写入独立的 namespace")
	}
	child, afterPause = incompatible, true
	_, err = agent.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess))
	var cause *terminalToolWriteError
	if !errors.Is(err, compose_v2.ErrGraphIDConflict) || !errors.Is(err, compose_v2.ErrRunAborted) || !errors.As(err, &cause) {
		t.Errorf("祖先应收到冲突及终局失败原链，实际 %v", err)
	}
	if _, waiting := compose_v2.IsInterrupt(err); waiting {
		t.Errorf("终局失败不应继续询问，实际 %v", err)
	}
	for ns, wire := range before {
		saved, err := cp.LoadNS(ctx, sess.RunID, ns)
		if err != nil || saved == nil {
			t.Fatalf("namespace %q 存档缺失，err=%v", ns, err)
		}
		if ns == childNS {
			current, err := compose_v2.MarshalCheckpoint(saved)
			if err != nil || !bytes.Equal(wire, current) {
				t.Fatalf("冲突子档应原样保留，err=%v", err)
			}
		} else if saved.HasResumableWork() {
			t.Errorf("namespace %q 仍可续跑", ns)
		}
	}

	child = original
	q := ie.AllQueries()[0]
	_, err = agent.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{q.InterruptID: schema.UserMessage("同意")}))
	if !errors.Is(err, compose_v2.ErrNoPendingCheckpoint) || writes.Load() != 1 {
		t.Fatalf("改回原子图也不能重放终局工具，实际 writes=%d err=%v", writes.Load(), err)
	}
}
