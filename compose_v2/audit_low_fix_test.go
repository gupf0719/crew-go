package compose_v2_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

// TestCondReturnStartNode_FailFast 验证 LOW-B 修复：cond 返回 StartNode（入口 sentinel，
// 不在 g.nodes）应立即 fail-fast，而非溜过校验、进 frontier 后 executeFrontier
// "node __start__ not registered" + cp.CurrentFrontier 含 StartNode 死状态不可恢复。
//
// sanity：revert 修复（恢复 `t != EndNode && t != StartNode` 豁免）后，StartNode 溜过
// 校验 → 错误延迟到下超步 "node \"__start__\" not registered"（不指向 cond 来源）。
func TestCondReturnStartNode_FailFast(t *testing.T) {
	b := compose_v2.NewBuilder()
	b.AddNode("a", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, nil
	})
	b.AddEdge(compose_v2.StartNode, "a")
	b.AddConditionalEdge("a", func(ctx context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		return compose_v2.StartNode, nil // 非法：cond 返回入口 sentinel
	})
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cm := compose_v2.NewChannelManager()
	err = compose_v2.Run(context.Background(), g, cm)
	if err == nil {
		t.Fatal("cond 返回 StartNode 应 fail-fast，得到 nil")
	}
	// 修复后错误在 computeNextTasks 指向 cond 来源 + StartNode 不可路由，
	// 而非延迟到 executeFrontier 的 "node __start__ not registered"。
	if !strings.Contains(err.Error(), "unroutable") && !strings.Contains(err.Error(), "condition from") {
		t.Fatalf("错误应指向 cond 返回 StartNode 不可路由，got: %v", err)
	}
}

// TestInterruptError_NilQueryElement 验证 LOW-C 修复：Queries 含 nil 元素时
// Validate fail-fast（挡住下游校验循环 deref；cond 路径已禁
// interrupt 不再调 Validate）+ Error() 不裸 panic（业务经 fmt.Errorf %w / slog 触发
// .Error() 时不受 framework recover 保护）。
func TestInterruptError_NilQueryElement(t *testing.T) {
	// Validate 拒绝 nil 元素
	ie := &compose_v2.InterruptError{Queries: []*compose_v2.AskQuery{
		{InterruptID: "a", Question: "q"}, nil,
	}}
	if err := ie.Validate(); err == nil {
		t.Fatal("Validate 应拒绝 Queries 含 nil 元素")
	} else if !strings.Contains(err.Error(), "nil") {
		t.Fatalf("Validate 错误应指向 nil 元素，got: %v", err)
	}

	// Error() 不 panic——Queries[0]==nil + Reason=="" 命中原裸 deref 点
	ieNil := &compose_v2.InterruptError{Queries: []*compose_v2.AskQuery{nil}}
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Error() 不应 panic（业务栈 deref nil），got panic: %v", r)
			}
		}()
		_ = ieNil.Error()
		// 业务真实触发路径：fmt.Errorf %w 会调 .Error()
		_ = fmt.Errorf("wrap: %w", ieNil).Error()
	}()

	// 正常 Queries（无 nil）Validate 通过——不误伤合法路径
	ieOK := &compose_v2.InterruptError{Queries: []*compose_v2.AskQuery{{InterruptID: "x", Question: "q"}}}
	if err := ieOK.Validate(); err != nil {
		t.Fatalf("正常 Queries 不应被 Validate 拒绝，got: %v", err)
	}
}
