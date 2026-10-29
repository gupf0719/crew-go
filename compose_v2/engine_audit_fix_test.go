package compose_v2_test

// 引擎深审 F-D fix 的回归测试。
//
// F-D: outer recover 无条件覆盖 runErr → defer 链 panic 抹掉 InterruptError →
//      IsInterrupt=false 但 cleanup 已存 Pending cp（状态矛盾，HITL 静默失效）。
//      当时的修法是 runErr 已有值时 errors.Join 保双链；现在 OnGraphEnd 的 panic 在它自己的 defer 里
//      就地兜住、不进错误，落不到 outer recover（那道只剩兜框架自己的出口代码）。
//      sanity-revert: 去掉 graph_loop.go OnGraphEnd defer 里的 recover → 错误里多出 panic → 本测试 fail。
//
// 注：原 F-C（cond "空 query" 路径漏 updateFrontier）回归测试已随「cond 禁止 interrupt」
// 协议移除——该独立分支已并入统一的 cond-interrupt 拒绝分支，行为由
// issue86_cond_path_internal_test.go::TestCondInterrupt_AlwaysRejected/nil_query 覆盖
//（同样用 fan-out 拓扑保证 frontier 收缩断言非假绿）。

import (
	"context"
	"errors"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

type engAuditPanicOnGraphEnd struct {
	compose_v2.NoOpOrchestrationHooks
}

func (engAuditPanicOnGraphEnd) OnGraphEnd(ctx context.Context, _ compose_v2.Channels, _ error) context.Context {
	panic("boom in OnGraphEnd hook")
}

// TestEngineAudit_FD_DeferPanicPreservesInterrupt：节点抛 InterruptError + OnGraphEnd
// hook panic → 返回 err 必须仍 IsInterrupt==true。OnGraphEnd 跑在图结束之后，它的 panic 只记日志、
// 不并进错误（错误文本与没有 panic 时相同，见 graph_loop.go OnGraphEnd 的 defer）。
func TestEngineAudit_FD_DeferPanicPreservesInterrupt(t *testing.T) {
	interruptNode := func(_ context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return nil, &compose_v2.InterruptError{
			Query: &compose_v2.AskQuery{InterruptID: "fd_q1", Question: "confirm?"},
		}
	}
	b := compose_v2.NewBuilder()
	b.AddNode("N", interruptNode)
	b.AddEdge(compose_v2.StartNode, "N")
	b.AddEdge("N", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cm := compose_v2.NewChannelManager()
	runErr := compose_v2.Run(context.Background(), g, cm, compose_v2.WithHooks(engAuditPanicOnGraphEnd{}))

	if runErr == nil {
		t.Fatal("expect error (interrupt + defer panic)")
	}
	// F-D 核心：InterruptError 不被 defer 链 panic 抹掉——业务 IsInterrupt 仍能识别 HITL。
	if _, ok := compose_v2.IsInterrupt(runErr); !ok {
		t.Errorf("F-D regression: defer 链 panic 后 IsInterrupt=false，InterruptError 被覆盖丢失。err=%v", runErr)
	}
	// 观测钩子的 panic 不改变结局：错误文本里没有它、错误链上没有 PanicError（看得见靠 ERROR 日志）。
	var pe *compose_v2.PanicError
	if strings.Contains(runErr.Error(), "panic") || errors.As(runErr, &pe) {
		t.Errorf("OnGraphEnd 的 panic 不该并进错误，err=%v", runErr)
	}
}
