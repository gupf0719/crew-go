package agent_v2

import (
	"encoding/json"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestMemoryItemFingerprint_CoversWholeMessage 钉住**指纹覆盖整条消息，不是手挑几个字段**。
//
// ⚠️ 手挑是 allowlist——**新字段默认漏**，而漏掉的后果跟这个指纹要修的那个 bug 一模一样
// （不同内容撞键、后写静默覆盖、`err` 全程 nil）。第一版只取 Role + Content + block 的
// Type/Text，审计实测至少 6 类会撞，其中 A 那类**端到端复现**：同一子 agent 两次分别以
// 不同的 return-direct 工具收尾，两条消息 Content 都空 ⟹ 键逐字相同 ⟹ 2 次 Put 落 1 条。
//
// 所以判据是「**整条消息序列化**」而不是「我想得到的那几个字段」。对照组守住反面：
// 同内容不同 `Message.ID` **必须撞**（那是 Resume 幂等的基础）。
func TestMemoryItemFingerprint_CoversWholeMessage(t *testing.T) {
	txt := func(s, reason, refusal string, ann map[string]any) *schema.Message {
		m := schema.AssistantMessage(s)
		m.ReasoningContent = reason
		m.Refusal = refusal
		m.Annotations = ann
		return m
	}
	for _, c := range []struct {
		name string
		a, b *schema.Message
	}{
		{"tool_call 载荷（端到端复现过的那类）",
			schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "1", Function: schema.FunctionCall{Name: "transfer_to_agent", Arguments: `{"target":"billing"}`}}}),
			schema.AssistantToolCallMessage([]schema.ToolCall{{ID: "1", Function: schema.FunctionCall{Name: "route_to", Arguments: `{"target":"other"}`}}})},
		{"ReasoningContent（provider 会回填）", txt("同", "思路一", "", nil), txt("同", "思路二", "", nil)},
		{"Refusal（provider 会回填）", txt("同", "", "拒绝A", nil), txt("同", "", "拒绝B", nil)},
		{"Annotations（框架会盖 marker）", txt("同", "", "", map[string]any{"k": "1"}), txt("同", "", "", map[string]any{"k": "2"})},
	} {
		if memoryItemFingerprint(c.a) == memoryItemFingerprint(c.b) {
			t.Errorf("%s：两条**不同**的消息指纹相同——后写会静默覆盖先写", c.name)
		}
	}

	// 反面：同内容不同 ID 必须撞（Resume 重跑靠它幂等）
	g, h := schema.AssistantMessage("同一句话"), schema.AssistantMessage("同一句话")
	if g.ID == h.ID {
		t.Fatal("fixture 没搭对：两条消息该有不同的 ID")
	}
	if memoryItemFingerprint(g) != memoryItemFingerprint(h) {
		t.Errorf("同内容不同 ID 的两条指纹不同——Message.ID 漏进指纹了，" +
			"那等于随机键，Resume 重跑必然多写一条")
	}

	// 不改调用方那条消息
	m := schema.AssistantMessage("x")
	before := m.ID
	_ = memoryItemFingerprint(m)
	if m.ID != before {
		t.Errorf("指纹计算改写了入参的 ID（%q → %q）——它随后要进 events / 存储", before, m.ID)
	}
}

// TestMemoryItemFingerprint_SurvivesCheckpointRoundTrip 钉住**指纹跨 cp 序列化不变**。
//
// ⚠️ 这条守的是 Resume 幂等的**前提**，而那个前提之前没人验过：Resume 路径上那条消息是
// **从 cp 反序列化回来的**，如果 round-trip 改变了任何字段的形状（nil map 变空 map、
// 数值类型漂移、字段顺序变），指纹就会变——而指纹一变，`commitFinal` 重跑就会写第二条，
// 正是这个指纹要修的那个失效换个方向重现。
//
// 序列化整条的代价就在这儿：手挑字段时只有那几个字段要稳，整条序列化要求**每个字段**都稳。
//
// ⚠️ **不变量的准确措辞是「标量 annotation 值 + 框架自己写的那些字段稳」**，不是「每个字段
// 都稳」——审计实测 struct 值 annotation / `Data any` / 超过 2^53 的整数跨 round-trip 指纹
// **确实会变**。框架自己写的 annotation 全是 bool/string 标量所以当前不可达，但把话说满了
// 会让下一个人以为这里已经保证了它保证不了的东西。
func TestMemoryItemFingerprint_SurvivesCheckpointRoundTrip(t *testing.T) {
	compose_v2.RegisterValueType[[]*schema.Message]("fp_probe_msgs")

	withAnn := schema.AssistantMessage("最终结论")
	withAnn.Annotations = map[string]any{"run_id": "abc", "reflect_rounds": 2}
	withAnn.ReasoningContent = "推理"

	for _, c := range []struct {
		name string
		m    *schema.Message
	}{
		{"纯文本", schema.AssistantMessage("答复")},
		{"带 annotations + reasoning", withAnn},
		{"tool_call", schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "1", Function: schema.FunctionCall{Name: "f", Arguments: `{"a":1}`}}})},
		// 嵌套 map 是稳的（`json.Marshal` 对 map key 排序）——留着作为「非标量不等于不稳」
		// 的对照，别让人以为凡非标量都危险。
		{"嵌套 map annotation", func() *schema.Message {
			m := schema.AssistantMessage("答复")
			m.Annotations = map[string]any{"nested": map[string]any{"b": 2, "a": 1}}
			return m
		}()},
	} {
		before := memoryItemFingerprint(c.m)
		// **走真实的 channel 值编解码**（`MarshalTagged`/`UnmarshalTagged` 是 cp 里 Channels
		// 的实际路径），不是裸 json——两者底层都是 `json.Marshal`，但真实路径多一层 tagged
		// 封装 + 类型注册表还原。用裸 json 会让「这颗哨兵测的是不是真实路径」变成一个下一个
		// 人还要重新怀疑一遍的问题。
		raw, err := compose_v2.MarshalTagged([]*schema.Message{c.m})
		if err != nil {
			t.Fatalf("%s: MarshalTagged: %v", c.name, err)
		}
		// 再套一层整 cp 的 json（MarshalCheckpoint 底层就是 json.Marshal）
		outer, _ := json.Marshal(map[string]json.RawMessage{"c": json.RawMessage(raw)})
		var wrapped map[string]json.RawMessage
		if err := json.Unmarshal(outer, &wrapped); err != nil {
			t.Fatalf("%s: outer unmarshal: %v", c.name, err)
		}
		v, err := compose_v2.UnmarshalTagged(wrapped["c"])
		if err != nil {
			t.Fatalf("%s: UnmarshalTagged: %v", c.name, err)
		}
		msgs, ok := v.([]*schema.Message)
		if !ok || len(msgs) != 1 {
			t.Fatalf("%s: 类型还原失败 %T", c.name, v)
		}
		back := *msgs[0]
		if after := memoryItemFingerprint(&back); after != before {
			t.Errorf("%s：跨 cp round-trip 指纹变了（%s → %s）——Resume 重跑会写第二条记忆",
				c.name, before, after)
		}
	}
}

type fpProbeStruct struct {
	B int `json:"b"`
	A int `json:"a"`
}

// TestMemoryItemFingerprint_UnstableValueKindsAreKnown 钉住**指纹稳定性的边界在哪**。
//
// ⚠️ 这条断言的是**现状而非理想**：struct 值的 annotation 和超过 2^53 的整数跨 round-trip
// **确实会让指纹变**（struct 走字段声明序、大整数经 `any` 还原成 float64 掉精度）。框架
// 自己写的 annotation 全是 bool/string 标量，所以当前**不是活 bug**——暴露面是业务 hook
// 自己往 annotation 里塞的东西。
//
// 写下来是为了让这条边界**可见**：哪天它红了（某个 kind 变稳了）说明底层序列化换了，
// 那时指纹的稳定性承诺可以放宽；而它绿着的时候，谁想往 annotation 里塞结构体，先看这里。
//
// ⚠️ 第一版 fixture 挑的是**嵌套 map**——那恰好是唯一稳的那种（map key 有序），
// 于是测出「非标量也稳」，跟审计的实测相反。**验一个已知缺陷时测不出来，先怀疑 fixture。**
func TestMemoryItemFingerprint_UnstableValueKindsAreKnown(t *testing.T) {
	for _, c := range []struct {
		name string
		v    any
	}{
		{"struct 值（字段声明序 vs map 排序序）", fpProbeStruct{B: 2, A: 1}},
		{"大整数（经 any 还原成 float64 掉精度）", int64(1) << 60},
	} {
		m := schema.AssistantMessage("答复")
		m.Annotations = map[string]any{"x": c.v}
		before := memoryItemFingerprint(m)
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("%s: marshal: %v", c.name, err)
		}
		var back schema.Message
		if err := json.Unmarshal(b, &back); err != nil {
			t.Fatalf("%s: unmarshal: %v", c.name, err)
		}
		if memoryItemFingerprint(&back) == before {
			t.Errorf("%s：跨 round-trip 指纹居然稳了——底层序列化行为变了，"+
				"指纹的稳定性承诺可以放宽，去更新那条边界说明", c.name)
		}
	}
}

// TestMemoryItemFingerprint_MarshalFailureIsContentDerived 钉住**降级分支的指纹仍由内容派生**。
//
// ⚠️ 这里栽过两次，两次的形态相反，所以三条断言缺一不可：
//
//  1. 第一次是降级退回 `Role + Content`——两条 tool_call 载荷不同、Content 都空的消息指纹
//     相同 ⟹ 2 次 Put 落 1 条、`err` 全程 nil。
//  2. 第二次是改用 `%#v`——它**只修好了上面那半的表象**：`fmt` 只在 depth 0 解引用指针，
//     `ContentBlocks []*ContentBlock` 的元素在 depth 2 ⟹ 打的是**分配地址**，载荷根本不
//     进指纹。两条载荷不同的消息之所以指纹不同，靠的是地址不同**而不是内容不同**。
//
// 第 2 次那个退化**通不过下面第 2、3 条**，但**能通过第 1 条**——只有否定式断言
// （「不同的别撞」）的哨兵在它上面是全绿的。这就是为什么必须有正向断言：
//
//  1. 不同内容 ⟹ 不同键        （否定：防合并）
//  2. 相同内容 ⟹ **相同**键     （正向：防分裂，`%#v` 在这条红）
//  3. 同一个对象**原地**改载荷 ⟹ 键必须变（不依赖地址差异，`%#v` 在这条红）
//
// 第 3 条是最硬的一条：它连「两个对象碰巧地址不同」这个偶然都排除掉了。
func TestMemoryItemFingerprint_MarshalFailureIsContentDerived(t *testing.T) {
	mk := func(name, args string) *schema.Message {
		m := schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "1", Function: schema.FunctionCall{Name: name, Arguments: args}}})
		// 业务 hook 往 Annotations 塞不可序列化的值（`OnModelEnd` 的 output.Message 是
		// [消费] 档，业务改得动）——这是降级分支的可达路径。
		m.Annotations = map[string]any{"bad": func() {}}
		return m
	}

	a := mk("transfer_to_agent", `{"t":"x"}`)
	// fixture 自检：确认真的走了降级分支，否则测的是正常分支
	if _, err := json.Marshal(a); err == nil {
		t.Fatal("fixture 没搭对：这条消息该 marshal 失败，否则测的是正常分支")
	}

	if memoryItemFingerprint(a) == memoryItemFingerprint(mk("route_to", `{"t":"y"}`)) {
		t.Error("① 两条**不同**的消息指纹相同——后写会静默覆盖先写")
	}
	if fa, fb := memoryItemFingerprint(mk("transfer_to_agent", `{"t":"x"}`)),
		memoryItemFingerprint(mk("transfer_to_agent", `{"t":"x"}`)); fa != fb {
		t.Errorf("② 两条**逐字相同**的消息指纹不同（%s != %s）——同一条事实会被写成两条，"+
			"说明指纹掺进了地址之类的非内容属性", fa, fb)
	}
	c := mk("transfer", `{"amount":1}`)
	before := memoryItemFingerprint(c)
	c.ContentBlocks[0].Toolcall.Arguments = `{"amount":1000000}`
	if before == memoryItemFingerprint(c) {
		t.Error("③ 同一个对象原地改掉 tool_call 载荷，指纹没变——载荷根本没进指纹")
	}
}

// TestMemoryItemFingerprint_CyclicAnnotationDoesNotHang 钉住降级分支遇环不挂死。
//
// `Annotations map[string]any` 里可以挂自引用的值。那种值 `json.Marshal` 报 cycle ⟹
// 每一层都失败、每一层都递归——没有深度上限就是无限递归。
func TestMemoryItemFingerprint_CyclicAnnotationDoesNotHang(t *testing.T) {
	cyc := map[string]any{}
	cyc["self"] = cyc
	m := schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "1", Function: schema.FunctionCall{Name: "t", Arguments: `{}`}}})
	m.Annotations = map[string]any{"cyc": cyc}
	if got := memoryItemFingerprint(m); got == "" {
		t.Error("环形 annotation 下指纹为空")
	}
}
