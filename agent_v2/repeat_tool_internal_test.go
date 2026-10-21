package agent_v2

import (
	"strings"
	"testing"

	"crew-go/compose_v2"
)

// TestRepeatToolKey_OrderInsensitive 钉住检测键对**参数字段顺序**不敏感。
//
// 这条是承重的：模型（或 provider 的 JSON 序列化）换一下字段顺序就能骗过检测的话，
// 这个功能基本等于没有——而字段顺序在 JSON 里本来就没有保证。
func TestRepeatToolKey_OrderInsensitive(t *testing.T) {
	a := repeatToolKey("f", `{"a":1,"b":{"x":1,"y":2},"c":[1,2]}`)
	b := repeatToolKey("f", `{"c":[1,2],"b":{"y":2,"x":1},"a":1}`)
	if a != b {
		t.Errorf("同一次调用换了字段顺序就算成两次了：\n  %s\n  %s", a, b)
	}
	// 数组顺序**是**语义的一部分，不能一起排掉。
	if repeatToolKey("f", `{"c":[1,2]}`) == repeatToolKey("f", `{"c":[2,1]}`) {
		t.Error("数组顺序被当成无关了——[1,2] 和 [2,1] 不是同一次调用")
	}
	// 工具名不同就是不同调用。
	if repeatToolKey("f", `{}`) == repeatToolKey("g", `{}`) {
		t.Error("不同工具算成同一次调用了")
	}
	// 解析不出来时退回原始串，逐字相同仍算同一次。
	if repeatToolKey("f", "not json") != repeatToolKey("f", "not json") {
		t.Error("非法 JSON 的两次相同调用没被认出来")
	}
	if repeatToolKey("f", "not json") == repeatToolKey("f", "other junk") {
		t.Error("两个不同的非法 JSON 被算成同一次了")
	}
}

// TestRepeatToolChain_CountsAndResets 钉住链的计数与重置语义。
func TestRepeatToolChain_CountsAndResets(t *testing.T) {
	cfg := &RepeatToolConfig{Enabled: true, Thresholds: []int{3, 5}}
	turn := newRepeatTurn(nil)

	// 前两次不喊。
	for i := 0; i < 2; i++ {
		if m := observeRepeatToolCall(turn, cfg, "f", `{"a":1}`); m != nil {
			t.Fatalf("第 %d 次就喊了，阈值是 3", i+1)
		}
	}
	// 第三次命中第一档。
	m := observeRepeatToolCall(turn, cfg, "f", `{"a":1}`)
	if m == nil {
		t.Fatal("连续第 3 次没喊——阈值 3 没生效")
	}
	if v, _ := m.Annotations[RepeatToolReminderAnnotation].(bool); !v {
		t.Error("提醒没打 RepeatToolReminderAnnotation")
	}
	if v, _ := m.Annotations[DerivedInputAnnotation].(bool); !v {
		t.Error("提醒没打 DerivedInputAnnotation——它不该进会话历史")
	}

	// 第四次不再喊（同一档只喊一次）。
	if m := observeRepeatToolCall(turn, cfg, "f", `{"a":1}`); m != nil {
		t.Error("同一档喊了第二次——链继续增长时只该在跨过下一档时再喊")
	}
	// 第五次命中第二档。
	if m := observeRepeatToolCall(turn, cfg, "f", `{"a":1}`); m == nil {
		t.Error("连续第 5 次没喊——第二档没生效")
	}

	// 换个调用 → 链重置，又要重新数 3 次。
	observeRepeatToolCall(turn, cfg, "f", `{"a":2}`)
	for i := 0; i < 1; i++ {
		if m := observeRepeatToolCall(turn, cfg, "f", `{"a":2}`); m != nil {
			t.Fatal("换了参数之后链没重置")
		}
	}
	if m := observeRepeatToolCall(turn, cfg, "f", `{"a":2}`); m == nil {
		t.Error("新链数到 3 没喊")
	}
}

// TestRepeatToolReminder_PreviewTruncatedKeyIsNot 钉住「预览截断、检测键不截断」。
//
// 两个只在第 600 个字符处不同的调用**不是**同一次调用，所以键必须用完整串；
// 而预览必须截断——大 payload 会无界地骑进下一个请求，恰恰是在循环场景下。
func TestRepeatToolReminder_PreviewTruncatedKeyIsNot(t *testing.T) {
	long1 := `{"s":"` + strings.Repeat("x", 600) + `A"}`
	long2 := `{"s":"` + strings.Repeat("x", 600) + `B"}`
	if repeatToolKey("f", long1) == repeatToolKey("f", long2) {
		t.Error("只在末尾不同的两次调用被算成同一次了——检测键被截断了")
	}

	cfg := &RepeatToolConfig{Enabled: true, Thresholds: []int{2}, PreviewMaxChars: 50}
	turn := newRepeatTurn(nil)
	observeRepeatToolCall(turn, cfg, "f", long1)
	m := observeRepeatToolCall(turn, cfg, "f", long1)
	if m == nil {
		t.Fatal("第 2 次没喊")
	}
	if len(m.Content) > 600 {
		t.Errorf("提醒正文 %d 字节——参数预览没被截断，大 payload 会骑进下一个请求", len(m.Content))
	}
}

// TestRepeatTool_DisabledIsNoOp 钉住不开时零行为。
func TestRepeatTool_DisabledIsNoOp(t *testing.T) {
	turn := newRepeatTurn(nil)
	for i := 0; i < 20; i++ {
		if m := observeRepeatToolCall(turn, &RepeatToolConfig{Enabled: false}, "f", `{}`); m != nil {
			t.Fatal("没开却喊了")
		}
		if m := observeRepeatToolCall(turn, nil, "f", `{}`); m != nil {
			t.Fatal("nil 配置却喊了")
		}
	}
	if turn.result() != nil {
		t.Error("没开却动了链——这一批会多写一次 channel")
	}
}

// TestRepeatTurn_DoesNotMutateCommitted 钉住「这一批改的是拷贝」：channel 里那份是已提交的状态，这一批没成
// （中断、失败）之前不能动它——就地改的话，中断的那一批的计数照样留在已提交的状态里，续跑重跑这一批时再算一遍。
func TestRepeatTurn_DoesNotMutateCommitted(t *testing.T) {
	cfg := &RepeatToolConfig{Enabled: true, Thresholds: []int{2}}
	committed := &repeatToolState{Key: repeatToolKey("f", `{}`), Count: 1}
	turn := newRepeatTurn(committed)
	if m := observeRepeatToolCall(turn, cfg, "f", `{}`); m == nil {
		t.Fatal("接着已提交的 1 次再调 1 次应命中阈值 2")
	}
	if committed.Count != 1 || len(committed.Fired) != 0 {
		t.Errorf("已提交的状态被就地改了：%+v", committed)
	}
	got := turn.result()
	if got == nil || got.Count != 2 || len(got.Fired) != 1 || got.Fired[0] != 2 {
		t.Errorf("这一批之后的链不对：%+v", got)
	}
}

// TestRepeatToolState_SurvivesCheckpointRoundTrip：计数链经存档往返（JSON）之后接着计。检测键存的是摘要——参数解析失败
// 时键用原始串，原始串里的非法 UTF-8 经 JSON 往返会被换成 U+FFFD，存原文的话续跑后跟同一个调用对不上、从 1 重计；
// 参数很大时存原文还让每份存档多一份同样大的副本（参数本身已经在消息里存了一份）。
func TestRepeatToolState_SurvivesCheckpointRoundTrip(t *testing.T) {
	cfg := &RepeatToolConfig{Enabled: true, Thresholds: []int{3}}
	args := "not json \xff\xfe" // 解析失败、含非法 UTF-8
	turn := newRepeatTurn(nil)
	observeRepeatToolCall(turn, cfg, "f", args)
	observeRepeatToolCall(turn, cfg, "f", args)
	data, err := compose_v2.MarshalTagged(turn.result())
	if err != nil {
		t.Fatal(err)
	}
	v, err := compose_v2.UnmarshalTagged(data)
	if err != nil {
		t.Fatal(err)
	}
	committed, ok := v.(*repeatToolState)
	if !ok {
		t.Fatalf("往返之后类型是 %T", v)
	}
	if m := observeRepeatToolCall(newRepeatTurn(committed), cfg, "f", args); m == nil {
		t.Error("经存档往返之后第 3 次同参数调用没收到提醒——链跟同一个调用对不上、从 1 重计了")
	}

	big := `{"s":"` + strings.Repeat("x", 100000) + `"}`
	turn = newRepeatTurn(nil)
	observeRepeatToolCall(turn, cfg, "f", big)
	data, err = compose_v2.MarshalTagged(turn.result())
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 1000 {
		t.Errorf("链在存档里占 %d 字节——检测键存了参数原文，每份存档多一份同样大的副本", len(data))
	}
}
