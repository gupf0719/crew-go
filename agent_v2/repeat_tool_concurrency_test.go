package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// mkSameNameBranch 造一个连着调 n 次同一个工具（同样参数）的 BasicAgent。
//
// ⚠️ 两个分支**故意同名**——这是整条 bug 的全部触发条件，不是疏忽。
func mkSameNameBranch(rt *recordTool, n int, seen *[]*schema.Message) agent_v2.Agent {
	tk := agent_v2_newToolkit()
	tk.Register(rt)
	resps := make([]*schema.Message, 0, n+1)
	for i := 0; i < n; i++ {
		resps = append(resps, oneToolCall(rt.name))
	}
	resps = append(resps, schema.AssistantMessage("done"))
	return agent_v2.NewBasicAgent(
		agent_v2.WithName("worker"), // ⬅️ 两个分支同名
		agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(n+3),
		agent_v2.WithRepeatToolReminder(&agent_v2.RepeatToolConfig{
			Enabled: true, Thresholds: []int{3},
		}),
		agent_v2.WithModel(newStubModelCapturing(resps, seen)),
	)
}

// TestRepeatTool_ChainsDoNotMergeAcrossParallelBranches 钉住**重复链按调用流分，不按 agent 名分**。
//
// 构造：ParallelAgent 两个分支，两个 BasicAgent **同名**，各自连着调同一个工具
// **恰好 2 次**（阈值 3）。任何一个分支单看都没到阈值，所以一句提醒都不该出现。
//
// 修复前：`RunSession` 的 values map 在分支之间共享（`ForkLane`），而链的键只带 agent 名
// ⟹ 两个分支撞进同一条链 ⟹ 2 + 2 = 4 越过阈值 3 ⟹ 凭空喊出一句「你在原地打转」，
// 而两边各自都只调了两次。这条断言在那个版本下必红。
//
// ⚠️ 断言选在「提醒有没有到模型眼前」上，不在「链的内部计数」上——后者白盒测已覆盖，
// 而这条要钉的正是「业务会不会收到一句不该有的话」。
func TestRepeatTool_ChainsDoNotMergeAcrossParallelBranches(t *testing.T) {
	rt1 := &recordTool{name: "biz_tool"}
	rt2 := &recordTool{name: "biz_tool"}
	var seen1, seen2 []*schema.Message

	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("b1", mkSameNameBranch(rt1, 2, &seen1)).
		AddBranch("b2", mkSameNameBranch(rt2, 2, &seen2))

	if _, err := par.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 前置假设：两个分支真的各调了 2 次。不成立的话下面那条断言恒真、什么都没测。
	if g1, g2 := rt1.calls.Load(), rt2.calls.Load(); g1 != 2 || g2 != 2 {
		t.Fatalf("前置假设不成立：两个分支应各调工具 2 次，实际 b1=%d b2=%d ⟹ "+
			"下面的「不该有提醒」恒真，这条用例什么都没测", g1, g2)
	}

	for _, tc := range []struct {
		lane string
		msgs []*schema.Message
	}{{"b1", seen1}, {"b2", seen2}} {
		for _, m := range tc.msgs {
			if strings.Contains(m.Content, "[system_reminder]") && strings.Contains(m.Content, "工具") {
				t.Errorf("分支 %s 只调了 2 次（阈值 3），却收到了重复提醒 ⟹ "+
					"两个同名分支的链被合并了：%q", tc.lane, m.Content)
			}
		}
	}
}

// TestRepeatTool_NoRaceAcrossParallelBranches 是上一条的 `-race` 侧面。
//
// 计数合并之外还有更硬的一层（当时链挂在会话 KV 上）：两个分支的 goroutine 同时改同一条链——
// `count++` 与 `fired` 的 map 写都在锁外（valuesMu 保的是 map 槽，不是指针背后的结构）。
// `fired` 那条是**并发 map 写**，不只是被 race detector 抓到，是会真的 panic。
//
// 跑 `go test -race` 时这条在修复前必现 DATA RACE（实测 2~3 条）。
func TestRepeatTool_NoRaceAcrossParallelBranches(t *testing.T) {
	rt1 := &recordTool{name: "biz_tool"}
	rt2 := &recordTool{name: "biz_tool"}
	var seen1, seen2 []*schema.Message

	par := agent_v2.NewParallelAgent(agent_v2.WithParallelName("par")).
		AddBranch("b1", mkSameNameBranch(rt1, 11, &seen1)).
		AddBranch("b2", mkSameNameBranch(rt2, 11, &seen2))

	if _, err := par.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("go")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 前置假设：并发窗口真的打开了——两边都跑满 11 轮才有重叠可言。
	if g1, g2 := rt1.calls.Load(), rt2.calls.Load(); g1 != 11 || g2 != 11 {
		t.Fatalf("前置假设不成立：两个分支应各调工具 11 次，实际 b1=%d b2=%d ⟹ "+
			"并发窗口没打开，这条用例抓不到 race", g1, g2)
	}
}
