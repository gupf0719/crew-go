package observability

import (
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// 续跑输入的观测面必须**两条通道都算**。
//
// # 修的是什么
//
// 续跑负载有两条独立通道：寻址型（`UserInputs`，按 InterruptID 投递给某个提问）和
// 非寻址型（`UserText`，「用户这轮说了什么」）。观测层原先只看前者。
//
// 后果：只走非寻址通道的续跑（Replan 的推荐形态）在 trace 上显示 **「用户 0 输入」**，
// 而且 `gen_ai_content` 整个 attr 不发（即便开了记录内容）——**Replan 恰恰是最需要看见
// 用户说了什么的那种续跑**：他中途说了一句改变计划的话，排查时却看不到。
//
// 实测过（OTLP 抓取两组对照）：迁移前 `user_inputs_count=1` / `messages_count=1` /
// content 有原话；迁移后全变 0、content attr 不发。

func TestResumeInputCount_CountsBothChannels(t *testing.T) {
	cases := []struct {
		name string
		rc   *compose_v2.ResumeContext
		want int
	}{
		{"只有寻址型", &compose_v2.ResumeContext{
			UserInputs: map[string]*schema.Message{"id1": schema.UserMessage("批准")},
		}, 1},
		{"只有非寻址型（Replan 的推荐形态）", &compose_v2.ResumeContext{
			UserText: "换个方向",
		}, 1},
		{"两条都有", &compose_v2.ResumeContext{
			UserInputs: map[string]*schema.Message{"id1": schema.UserMessage("批准")},
			UserText:   "顺便把预算调到 3000",
		}, 2},
		{"都没有", &compose_v2.ResumeContext{}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := resumeInputCount(c.rc); got != c.want {
				t.Fatalf("数出 %d 条，期望 %d\n"+
					"（漏数非寻址通道时，只走它的续跑在 trace 上显示「用户 0 输入」——"+
					"而那正是 Replan 的推荐形态）", got, c.want)
			}
		})
	}
}

func TestResumeInputContent_CarriesUserText(t *testing.T) {
	// 只有非寻址型：内容必须带上，否则 gen_ai_content 整个 attr 不发
	rc := &compose_v2.ResumeContext{UserText: "换个方向：先做 X"}
	got := resumeInputContent(rc)
	if got == nil {
		t.Fatal("只有非寻址型时内容为 nil ⟹ gen_ai_content 不发 ⟹ 排查时看不到用户说了什么")
	}
	m, ok := got.(map[string]any)
	if !ok || m["__user_text__"] != "换个方向：先做 X" {
		t.Fatalf("非寻址型的内容没带上：%#v", got)
	}

	// 只有寻址型：**保持原样返 map**，不改既有 wire 形状
	only := map[string]*schema.Message{"id1": schema.UserMessage("批准")}
	rc2 := &compose_v2.ResumeContext{UserInputs: only}
	if got2 := resumeInputContent(rc2); got2 == nil {
		t.Fatal("只有寻址型时内容不该为 nil")
	} else if _, isPlainMap := got2.(map[string]*schema.Message); !isPlainMap {
		t.Fatalf("只有寻址型时不该改 wire 形状，实际 %T", got2)
	}
}
