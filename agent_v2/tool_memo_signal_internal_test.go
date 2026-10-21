package agent_v2

import (
	"context"
	"testing"

	"crew-go/compose_v2"
)

// TestShouldMemoize_JudgedByReplayabilityNotName：进不进工具账，判据是「这次调用的副作用能不能
// 随账重放」，不是工具名字。它同时钉住两个方向：
//
//   - 做出了上行信号的框架工具**要**进记账（transfer_to_agent / route_to 的决定随账进 cp、命中时
//     原样交出——否则续跑仍靠函数体重跑）；
//   - 没做出信号的框架工具**不能**进记账（tool_search 往 channel 写揭示集发生在工具体里，命中
//     跳过就没人写，后果静默）。
func TestShouldMemoize_JudgedByReplayabilityNotName(t *testing.T) {
	sigs, err := compose_v2.CaptureSignals(context.Background(), func(ctx context.Context) error {
		compose_v2.EmitSignal(ctx, handoffSignal, compose_v2.Signal{Dest: "d", Value: "billing"})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// 第一个参数是「这次派发的是不是框架铸的那件」（按实例判，见 frameworkMintedTool）。
	if !shouldMemoize(true, sigs) {
		t.Error("做出了转交决定的框架工具必须能进记账——那正是「不再依赖函数体重跑」的前提")
	}
	if shouldMemoize(true, compose_v2.Signals{}) {
		t.Error("没做出信号的框架工具不能进记账：它的副作用（如揭示集）由工具体写、框架无从重放，" +
			"命中跳过后果是静默的（tool_result 齐全但事没发生）")
	}
	if !shouldMemoize(false, compose_v2.Signals{}) {
		t.Error("业务工具默认进记账，不该被这次改动波及")
	}
	if !shouldMemoize(false, sigs) {
		t.Error("业务工具里经嵌套调用交上来的决定同样随账走")
	}
}
