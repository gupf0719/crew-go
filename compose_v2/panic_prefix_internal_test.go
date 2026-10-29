package compose_v2

import (
	"strings"
	"testing"
)

// Run 两道兜底的前缀不能被抹平：主体那道兜住时存档已存成 failed、可续跑，入口那道兜的是出口 defer 链自身、存档状态以
// 错误链上的结论为准，处置不同。两个前缀曾被一次不限次数的字符串替换改成同一个，全套测试照常全绿。入口那道业务代码触发
// 不到（业务钩子与存储调用都另有兜底），所以在这里直接比两个常量。
//
// sanity-revert：把两个常量改成同一个值（或一个是另一个的前缀）→ 本用例红。
func TestPanicPrefixes_StayDistinct(t *testing.T) {
	if strings.Contains(bodyPanicPrefix, runPanicPrefix) || strings.Contains(runPanicPrefix, bodyPanicPrefix) {
		t.Fatalf("两道兜底的前缀必须互不包含，实际 body=%q run=%q", bodyPanicPrefix, runPanicPrefix)
	}
}
