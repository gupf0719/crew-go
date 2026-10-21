package compose_v2_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/compose_v2"
)

// askOnceGraph 造一个单节点图：节点问一句，拿到答案就结束。
func askOnceGraph(t *testing.T) *compose_v2.Graph {
	t.Helper()
	b := compose_v2.NewBuilder()
	b.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
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
	return g
}

// TestWithInputDigest_EntryPaths：摘要在三条入口上的处理——存档一路沿用新开时那一份。
func TestWithInputDigest_EntryPaths(t *testing.T) {
	g := askOnceGraph(t)
	cper := compose_v2.NewInMemoryCheckpointer()
	base := []compose_v2.RunOption{
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("s"), compose_v2.WithRunNS("x"),
	}
	load := func() *compose_v2.Checkpoint {
		t.Helper()
		cp, err := cper.LoadNS(context.Background(), "s", "x")
		if err != nil || cp == nil {
			t.Fatalf("存档应在：cp=%v err=%v", cp, err)
		}
		return cp
	}

	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		append(base, compose_v2.WithInputDigest("A"))...)); !ok {
		t.Fatal("首跑应停在提问上")
	}
	if got := load().InputDigest; got != "A" {
		t.Fatalf("新开时应记下本次的摘要 A，got %q", got)
	}

	// 显式 WithResumeFrom：调用方自己选的档，不比；存档仍记新开时那份
	_ = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		append(base, compose_v2.WithResumeFrom(load()), compose_v2.WithInputDigest("B"))...)
	if got := load().InputDigest; got != "A" {
		t.Fatalf("续跑不该换掉存档里的摘要，got %q", got)
	}

	// auto-load 给了不同的摘要（只作核对）：照样续跑，存档仍记新开时那份
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		append(base, compose_v2.WithInputDigest("B"))...)
	if _, ok := compose_v2.IsInterrupt(err); !ok {
		t.Fatalf("只作核对的摘要对不上时应照样续跑（停在同一个提问上），got %v", err)
	}
	if got := load().InputDigest; got != "A" {
		t.Fatalf("auto-load 续跑不该换掉存档里的摘要，got %q", got)
	}

	// 新开：记本次的
	_ = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		append(base, compose_v2.WithFreshRun(), compose_v2.WithInputDigest("C"))...)
	if got := load().InputDigest; got != "C" {
		t.Fatalf("新开时应记下本次的摘要 C，got %q", got)
	}
}

// TestWithInputIdentity_MismatchRejectedWithoutPurge：摘要就是这次调用的身份时（存档槽由它
// 派生），同一个槽上对不上只可能是两次不同的调用撞进了同一个槽——续下去就是拿别人的进度当
// 自己的结果，所以拒绝；但**不清档**：槽里那份属于另一次调用，本次在图开始执行之前就停了。
// 撞槽是确定性的，错误不可重试。原来那次调用照常续得上。
func TestWithInputIdentity_MismatchRejectedWithoutPurge(t *testing.T) {
	g := askOnceGraph(t)
	cper := compose_v2.NewInMemoryCheckpointer()
	base := []compose_v2.RunOption{
		compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID("s"), compose_v2.WithRunNS("x"),
	}
	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		append(base, compose_v2.WithInputIdentity("A"))...)); !ok {
		t.Fatal("首跑应停在提问上")
	}
	// 同一份身份：照常续跑
	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		append(base, compose_v2.WithInputIdentity("A"))...)); !ok {
		t.Fatal("同一份身份应续上存档、停在同一个提问上")
	}
	err := compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		append(base, compose_v2.WithInputIdentity("B"))...)
	if err == nil {
		t.Fatal("身份对不上（撞槽）应拒绝，got nil")
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("撞槽不该表现成等答案的中断: %v", err)
	}
	if errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("撞槽不该是终局失败（那会清掉槽里另一次调用的存档）: %v", err)
	}
	if compose_v2.IsRetryable(err) {
		t.Fatalf("撞槽是确定性的，不该可重试: %v", err)
	}
	cp, lerr := cper.LoadNS(context.Background(), "s", "x")
	if lerr != nil || cp == nil || !cp.HasResumableWork() {
		t.Fatalf("槽里那份属于另一次调用的存档应原样留着: cp=%v err=%v", cp, lerr)
	}
	if _, ok := compose_v2.IsInterrupt(compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(),
		append(base, compose_v2.WithInputIdentity("A"))...)); !ok {
		t.Fatal("撞槽被拒之后，原来那次调用应照常续上自己的存档")
	}
}
