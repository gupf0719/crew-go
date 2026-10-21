package agent_v2

// RegrantHandoffToNoSelfCheck 是**探针，不是提案**：它就是 RegrantHandoffTo 去掉 self 参数的
// 样子，用来量「省掉那个证据参数」的代价。
//
// **刻意只存在于测试二进制里**（package agent_v2 的 _test.go，外部测试包够得到、
// 生产 API 里没有）：它一旦出现在生产面上，任何拿到过一份 opts 的人都能
// 把授予改寄给任意对象——那正是 self 参数要挡的东西。哨兵 TestGrantBreak_B6c 用它证明
// 「省掉 self 会当场重开 B8g」。
func RegrantHandoffToNoSelfCheck(opts []RunOption, to Agent) []RunOption {
	if isAgentNil(to) {
		return opts
	}
	g := grantFromOpts(opts)
	if g == nil {
		return opts
	}
	out := make([]RunOption, 0, len(opts)+1)
	out = append(out, opts...)
	return append(out, withHandoffGrant(g.regrant(to)))
}
