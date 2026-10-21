package agent_v2_test

import (
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// 结构化输出与转交**必须**在构造期互斥——两件同为 return-direct，而收尾裁决那条
// channel 是 OnceFlag、没有「谁赢」的语义。同轮都调时转交赢、结构化输出被**静默**
// 丢弃（err=nil，业务 json.Unmarshal 拿到的是转交 ack）。
//
// ⚠️ 这条守卫曾经被打死过一次：判据写的是「toolkit 里有没有那件工具」，而转交能力
// 改成按次授予之后 `WithHandoffTransfer()` 不再往 toolkit 塞工具 ⟹ 判据永远读不到 ⟹
// 守卫静默失效。判据必须跟着「能力从哪来」走，不能盯着工具的落地形式。
func TestOutputSchema_MustRejectHandoffTransfer(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("WithOutputSchema + WithHandoffTransfer 必须构造期 panic——" +
				"不 panic 就是把「静默丢结构化输出」放进了生产")
		}
		msg, _ := r.(string)
		if !strings.Contains(msg, agent_v2.HandoffTransferToolName) {
			t.Errorf("panic 信息该点名冲突的那件东西，实际 %q", msg)
		}
	}()
	_ = agent_v2.NewBasicAgent(
		agent_v2.WithName("bot"),
		agent_v2.WithModel(newStubModel([]*schema.Message{schema.AssistantMessage("x")})),
		agent_v2.WithHandoffTransfer(),
		agent_v2.WithOutputSchema("Ans", map[string]any{"type": "object"}),
	)
}
