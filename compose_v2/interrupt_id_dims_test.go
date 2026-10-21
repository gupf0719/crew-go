package compose_v2_test

import (
	"testing"

	"crew-go/compose_v2"
)

// TestInterruptID_ToolCallIdxNotRedundant 钉住 `ToolCallIdx` **不能删**。
//
// 它看起来是冗余的——`ToolCallID` 也在哈希里，同批的两个 tool_call 各有各的 ID，
// 抹平下标照样区分得开。一次三方调研就是这么论证的，建议删掉这一维。
//
// **但那个论证借用了「tc.ID 一批内唯一」这个前提，而框架别处明确不假设它**
// （见 `agent_v2/basic_agent.go` 的 memoKey：同批两个 tc 用同一个 ID 时，只按 ID 记
// 会让后跑的命中前一个的条目、静默不执行）。ID 真撞上时，下标是**唯一**分得开两次
// 提问的维度——没有它，两个不同的提问共用一个答案位。
//
// 「ID 为空就 fail-fast」同理不该做：别家那么干是因为它们没有别的区分维，我们有。
func TestInterruptID_ToolCallIdxNotRedundant(t *testing.T) {
	mk := func(tcID string, tcIdx int) string {
		return compose_v2.InterruptSite{
			TaskID: "t1", GraphNS: "ns", NodeID: "tools",
			ToolCallID: tcID, ToolCallIdx: tcIdx, Visit: 1,
		}.InterruptID()
	}

	t.Run("ID 重复时靠下标分开", func(t *testing.T) {
		if mk("call_SAME", 0) == mk("call_SAME", 1) {
			t.Error("同批两个 tc 用同一个 ID 时，下标必须还能分开它们——否则两个提问共用一个答案位")
		}
	})
	t.Run("ID 为空时靠下标分开", func(t *testing.T) {
		if mk("", 0) == mk("", 1) {
			t.Error("provider 不返 tool_call id 时，下标是唯一区分维")
		}
	})
	t.Run("ID 正常时下标确实冗余", func(t *testing.T) {
		// 这一条不是要求，是**记录事实**：正常路径上删掉下标也能跑，所以它看起来可删。
		// 上面两条才是它存在的理由。
		if mk("call_a", 0) == mk("call_b", 0) {
			t.Error("不同 ID 本来就该分得开")
		}
	})
}
