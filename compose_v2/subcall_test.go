package compose_v2_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestNextSubCallIndex_PerKeyAndRestartsEachExecution 钉住嵌套调用次序计数器的三条语义：
//
//  1. 按计数键分开计：别的键上的调用不移动本键的编号；
//  2. 每次执行从 0 起——节点中断后重跑时，第 k 次调用拿到跟首跑一样的 k（续跑才找得回
//     各自的存档）；
//  3. 每个 tool_call 各有自己的计数器，不跟所在节点共用。
func TestNextSubCallIndex_PerKeyAndRestartsEachExecution(t *testing.T) {
	var mu sync.Mutex
	var executions [][]int
	b := compose_v2.NewBuilder()
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		got := []int{
			compose_v2.NextSubCallIndex(ctx, "a"),
			compose_v2.NextSubCallIndex(ctx, "a"),
			compose_v2.NextSubCallIndex(ctx, "b"),
			compose_v2.NextSubCallIndex(ctx, "a"),
		}
		tc := compose_v2.WithCurrentToolCallContext(ctx, "tc", 0)
		got = append(got, compose_v2.NextSubCallIndex(tc, "a"), compose_v2.NextSubCallIndex(tc, "a"))
		// tool_call 里的调用不影响节点自己的计数
		got = append(got, compose_v2.NextSubCallIndex(ctx, "a"))
		mu.Lock()
		executions = append(executions, got)
		mu.Unlock()
		if _, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续？"}); err != nil {
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
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("s1")}
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), opts...)
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在提问，got %v", err)
	}
	err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), append(opts,
		compose_v2.WithResumeAnswers(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")}))...)
	if err != nil {
		t.Fatalf("续跑: %v", err)
	}

	want := []int{0, 1, 0, 2, 0, 1, 3}
	if len(executions) != 2 {
		t.Fatalf("节点应执行 2 次（首跑 + 续跑），实际 %d", len(executions))
	}
	for i, got := range executions {
		if !slices.Equal(got, want) {
			t.Errorf("第 %d 次执行的编号 = %v，want %v", i+1, got, want)
		}
	}

	// 不在图执行里：没有计数器，恒 0
	if a, b := compose_v2.NextSubCallIndex(context.Background(), "a"), compose_v2.NextSubCallIndex(context.Background(), "a"); a != 0 || b != 0 {
		t.Errorf("图外应恒返回 0，got %d, %d", a, b)
	}
}

// guardHooks 在每个节点的 OnNodeStart 里对同一个位置连数两次。
type guardHooks struct {
	compose_v2.NoOpOrchestrationHooks
	mu  sync.Mutex
	got []int
}

func (h *guardHooks) OnNodeStart(ctx context.Context, _ compose_v2.NodeID, _ compose_v2.Channels) (context.Context, error) {
	a := compose_v2.NextSubCallIndex(ctx, "guard")
	b := compose_v2.NextSubCallIndex(ctx, "guard")
	h.mu.Lock()
	h.got = append(h.got, a, b)
	h.mu.Unlock()
	return ctx, nil
}

// TestNextSubCallIndex_NotInheritedIntoNestedGraph：节点里起一张嵌套图，嵌套图的钩子相位
// 不在任何节点执行里，不能接着外层节点的计数器数——否则序号取决于「子图里此前已触发过钩子
// 的节点数」，续跑时子图 frontier 缩了、序号就漂（审计实测：用户答过的问题被再问一次）。
//
// 每次钩子调用各配一个从 0 数的计数器：同一次钩子调用里对同一个键数两次得 0、1（两次调用各有
// 各的编号），接着外层节点数的话是 1、2。
func TestNextSubCallIndex_NotInheritedIntoNestedGraph(t *testing.T) {
	ib := compose_v2.NewBuilder()
	ib.AddNode("in", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil })
	ib.AddEdge(compose_v2.StartNode, "in")
	ib.AddEdge("in", compose_v2.EndNode)
	inner, err := ib.Compile()
	if err != nil {
		t.Fatalf("compile inner: %v", err)
	}
	hooks := &guardHooks{}

	ob := compose_v2.NewBuilder()
	ob.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		// 外层节点先在同一个位置上数过一次
		compose_v2.NextSubCallIndex(ctx, "guard")
		return nil, compose_v2.Run(ctx, inner, compose_v2.NewChannelManager(), compose_v2.WithHooks(hooks))
	})
	ob.AddEdge(compose_v2.StartNode, "host")
	ob.AddEdge("host", compose_v2.EndNode)
	outer, err := ob.Compile()
	if err != nil {
		t.Fatalf("compile outer: %v", err)
	}
	if err := compose_v2.Run(context.Background(), outer, compose_v2.NewChannelManager()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !slices.Equal(hooks.got, []int{0, 1}) {
		t.Fatalf("嵌套图 OnNodeStart 里的编号 = %v，want [0 1]（[1 2] 是接着外层节点的计数器在数，"+
			"[0 0] 是钩子相位没有计数器、两次调用撞在一起）", hooks.got)
	}
}

// TestRunWithRetry_RewindsSubCallCounter：同一次调用的重试，每次尝试都从进入重试那一刻的
// 编号数起；成功那次之后，节点里接下来的调用接着它数。
func TestRunWithRetry_RewindsSubCallCounter(t *testing.T) {
	var perAttempt []int
	var after int
	b := compose_v2.NewBuilder()
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		attempts := 0
		_, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (int, error) {
			attempts++
			perAttempt = append(perAttempt, compose_v2.NextSubCallIndex(ctx, "a"))
			if attempts < 3 {
				return 0, errors.New("transient")
			}
			return 1, nil
		})
		if err != nil {
			return nil, err
		}
		after = compose_v2.NextSubCallIndex(ctx, "a")
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
	if !slices.Equal(perAttempt, []int{0, 0, 0}) {
		t.Fatalf("三次尝试拿到的编号 = %v，want [0 0 0]——重试被当成了新调用", perAttempt)
	}
	if after != 1 {
		t.Errorf("重试成功之后的下一次调用应是第 1 次，got %d", after)
	}
}
