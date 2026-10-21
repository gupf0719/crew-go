package agent_v2

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"crew-go/compose_v2"
)

// TestHashNSIndex_NonNegativeAndWide：嵌套调用的槽号必须非负（SplitNS 拒绝负下标），并且要用满
// int 的位数——槽号是身份的有损投影，31 位时一万次各留着存档的调用里撞一次槽的概率就到了
// 百分之几（生日界），撞了后一次调用会接到前一次的存档。
func TestHashNSIndex_NonNegativeAndWide(t *testing.T) {
	wide := false
	for i := 0; i < 1000; i++ {
		v := hashNSIndex(fmt.Sprintf("身份 %d", i))
		if v < 0 {
			t.Fatalf("槽号为负：%d", v)
		}
		if v > math.MaxInt32 {
			wide = true
		}
	}
	if strconv.IntSize == 64 && !wide {
		t.Fatalf("64 位平台上一千个槽号没有一个超过 31 位——槽号被截短了")
	}
	// 拼进 ns 之后能被原样读回
	idx := hashNSIndex("x")
	segs, err := compose_v2.SplitNS(compose_v2.JoinFanoutNS("p", "w", idx))
	if err != nil || len(segs) != 2 || segs[1].Kind != "fanout" || segs[1].Index != idx {
		t.Fatalf("槽号拼进 ns 后读不回来：segs=%+v err=%v", segs, err)
	}
}
