package agent_v2_test

// sliceEqual 从 compose_v2/hooks_test.go 复制——通用 slice 比较 helper（移出的 basic 测试用）。
func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
