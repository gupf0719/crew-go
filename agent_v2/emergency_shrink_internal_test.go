package agent_v2

import (
	"strings"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// protectedBig 造一组「全部受保护、且里面有超大单条」的消息——这正是原先的死局形态。
func protectedBig() []*schema.Message {
	mem := schema.SystemMessage("记忆")
	mem.Annotations = map[string]any{StoreInjectedAnnotation: true}
	skill := schema.ToolResultMessage("c1", "HEAD"+strings.Repeat("z", 40000)+"TAIL")
	skill.Annotations = map[string]any{SkillBodyAnnotation: true}
	return []*schema.Message{
		schema.SystemMessage("前导"),
		mem,
		schema.UserMessage("u1"),
		skill,
	}
}

// TestEmergencyDrop_ShrinksWhenNothingDroppable 钉住「一组可丢的都没有时，不再直接返原样，
// 而是就地把超大的单条挖空」。
//
// **原先的死局**：受保护的组自己就超预算 ⟹ `droppableIdx` 为空 ⟹ 返原样 ⟹ 上游 retry
// 循环发现没进展当场 break ⟹ 业务拿到 prompt-too-long。那个「精准信号」本身没错，
// 但它是在**还有别的手段没用**的情况下放弃的。
//
// **为什么就地挖空是安全的，而丢组不是**：它不动条数、不动 role 序、不碰 tool_call ID
// ⟹ 配对约束天然不受影响。所以这条同时断言「变小了」**和**「一条都没少、配对锚点没动」。
func TestEmergencyDrop_ShrinksWhenNothingDroppable(t *testing.T) {
	// **能挖空的三个出口各跑一遍**：只挂其中一个的话，其余照旧直接放弃——第一版就是那样，
	// 而 fixture 恰好先撞上「只剩一组」那个早退，被测路径根本没执行（fixture 陷阱：
	// 早退兜底盖过 fix 路径）。
	//
	// ⚠️ 这里原先写着「三个出口」而 cases 只有两条，`dropCount 归零` 那个出口一直没人跑
	// （coverage 实测 0）——而它是最常见的形态：**只有一组可丢**时 dropCount 被钳成 0。
	// 第四个出口 `len(conv) == 0`（整条消息流全是前导 system）**结构上挖不动**：挖空只
	// 认 `RoleTool` 且 >2000 字节，而前导 system 块里不可能有 tool 消息 ⟹ 它是纯放弃，
	// 由下面的 TestEmergencyDrop_AllSystemGivesUp 单独钉。
	cases := map[string][]*schema.Message{
		// 出口一：conv 只切出一组
		"只剩一组": protectedBig(),
		// 出口二：多组但全部受保护
		"多组全 protected": func() []*schema.Message {
			out := protectedBig()
			s2 := schema.ToolResultMessage("c2", "H2"+strings.Repeat("y", 40000)+"T2")
			s2.Annotations = map[string]any{SkillBodyAnnotation: true}
			return append(out, schema.UserMessage("u2"), s2)
		}(),
		// 出口三：多组、有可丢的，但**只有一组**可丢 ⟹ dropCount 被钳成 0（保底留 1 组）
		"只有一组可丢": func() []*schema.Message {
			out := protectedBig() // [system 前导][system 记忆][user u1][tool skill body]（受保护）
			// 第二组不带任何保护 marker ⟹ droppable，且正文更大，确保它被优先挖空
			free := schema.ToolResultMessage("c3", "H3"+strings.Repeat("w", 50000)+"T3")
			return append(out, schema.UserMessage("u2"), free)
		}(),
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) { assertShrunkNotDropped(t, in) })
	}
}

func assertShrunkNotDropped(t *testing.T, in []*schema.Message) {
	t.Helper()
	out := emergencyDropOldestGroups(in, 50, 5000, "[MARKER]")

	if len(out) != len(in) {
		t.Errorf("条数变了（%d → %d）——就地挖空不该增删消息，那会破坏配对", len(in), len(out))
	}
	before := compose_v2.EstimateMessageTokens(in)
	after := compose_v2.EstimateMessageTokens(out)
	if after >= before {
		t.Errorf("没腾出空间：%d → %d token。原先这里是直接返原样，业务只能拿到 prompt-too-long",
			before, after)
	}
	// 配对锚点必须原样
	for i := range in {
		if in[i].Role != out[i].Role {
			t.Errorf("第 %d 条 role 变了：%s → %s", i, in[i].Role, out[i].Role)
		}
	}
	// 两份内容都要改到——ToolResultMessage 把正文同时存在 Content 和 block 里
	last := out[len(out)-1]
	if len(last.ContentBlocks) > 0 && last.ContentBlocks[0] != nil && last.ContentBlocks[0].Toolresult != nil {
		if last.ContentBlocks[0].Toolresult.Content != last.Content {
			t.Errorf("只改了一份正文，block 里那份还是旧的（%d vs %d 字节）——"+
				"落盘和上 wire 各取一份，会出现两份不一致",
				len(last.ContentBlocks[0].Toolresult.Content), len(last.Content))
		}
	}
	// 不改调用方手里那份（它们还挂在 channel 上）
	if len(in[len(in)-1].Content) < 40000 {
		t.Errorf("原消息被就地改写了：调用方那份变成了 %d 字节", len(in[len(in)-1].Content))
	}
}

// TestEmergencyDrop_NoOversizedStillReturnsOriginal 钉住「没有超大单条时仍然返原样」
// ——那时确实无能为力，把精确的 prompt-too-long 还给业务是有意的取舍，不该被这次改动破坏。
func TestEmergencyDrop_NoOversizedStillReturnsOriginal(t *testing.T) {
	mem := schema.SystemMessage("记忆")
	mem.Annotations = map[string]any{StoreInjectedAnnotation: true}
	in := []*schema.Message{schema.SystemMessage("前导"), mem, schema.UserMessage("短")}
	out := emergencyDropOldestGroups(in, 50, 5000, "[MARKER]")
	if compose_v2.EstimateMessageTokens(out) != compose_v2.EstimateMessageTokens(in) {
		t.Errorf("没有超大单条却动了它——那时该返原样让业务拿到精准信号")
	}
}

// TestEmergencyShrink_OnlyToolResults 钉住「就地挖空只挖工具结果，不碰 system / user」。
//
// **bug 历史**：第一版按**位置**遍历，而下标 0 恰好是 `emergencyDropOldestGroups` 隔离
// 出来的前导 `sys` 块——丢组路径结构上碰不到它，挖空路径却恒第一个碰。实测 15038 字节的
// system 指令被挖到 2000，而同一条流里 **200000 字节的工具结果一个字没动**：
// **挖空路径专挑丢组路径保护的那个下手**。
//
// 「只挖工具产出」不是保守，是照着别人的实际代码定的——做单条截断的三家无一例外只截
// 工具产出，其中一家的分派函数逐个 item 类型列过，消息正文与推理内容全部原样透传。
//
// 断言选在**两个方向**：巨型 system 必须一字不动 **且** 巨型工具结果必须被挖——
// 只断言前者的话，一个「什么都不挖」的实现照样全绿。
func TestEmergencyShrink_OnlyToolResults(t *testing.T) {
	// ⚠️ **system / user 必须造得比工具结果还大**：候选是按体积降序挑的，
	// 若 system 更小，那么即使把「只挑 tool_result」这道过滤去掉，降序也会先挑中工具结果、
	// 腾够就停、system 永远轮不到 ⟹ 用例对那个退化**恒绿**。第一版就是这么写的，
	// 变异验证时才发现它不承重。
	bigSys := schema.SystemMessage("SYSHEAD" + strings.Repeat("规", 100000) + "SYSTAIL")
	bigUser := schema.UserMessage("USERHEAD" + strings.Repeat("问", 100000) + "USERTAIL")
	bigTool := schema.ToolResultMessage("c1", strings.Repeat("z", 200000))
	bigTool.Annotations = map[string]any{SkillBodyAnnotation: true}
	in := []*schema.Message{bigSys, bigUser, bigTool}

	out := emergencyDropOldestGroups(in, 50, 500, "[M]")

	if len(out) != 3 {
		t.Fatalf("条数变了：%d", len(out))
	}
	if len(out[0].Content) != len(bigSys.Content) {
		t.Errorf("巨型 system 被挖了（%d → %d 字节）——那是算法自己声明「永不删」的东西",
			len(bigSys.Content), len(out[0].Content))
	}
	if len(out[1].Content) != len(bigUser.Content) {
		t.Errorf("巨型 user 被挖了（%d → %d 字节）——用户的原话不该被框架静默改写",
			len(bigUser.Content), len(out[1].Content))
	}
	if len(out[2].Content) >= len(bigTool.Content) {
		t.Errorf("工具结果没被挖（%d 字节）——那样这个兜底等于没做", len(out[2].Content))
	}
}

// TestEmergencyShrink_DoesNotLeakIntoOriginal 钉住「改返回值不会写穿到还挂在 channel 上的原消息」。
//
// `hook.Hooks.OnModelStart` 的 `input.Messages` 是 [消费] 档——契约在**邀请**业务改它。
// 浅拷的话业务打个 tag 就写进原消息的 `Annotations` map 里（实测过端到端泄漏）。
// 同包另两个 clone 函数早就深拷了，这里不该例外。
func TestEmergencyShrink_DoesNotLeakIntoOriginal(t *testing.T) {
	bigTool := schema.ToolResultMessage("c1", strings.Repeat("z", 200000))
	bigTool.Annotations = map[string]any{SkillBodyAnnotation: true}
	in := []*schema.Message{schema.UserMessage("u"), bigTool}

	out := emergencyDropOldestGroups(in, 50, 500, "[M]")
	last := out[len(out)-1]
	if last.Annotations == nil {
		t.Fatal("前提没搭对：挖空后 annotation 整个没了，marker 保护会失效")
	}
	last.Annotations["业务打的tag"] = true

	if _, leaked := bigTool.Annotations["业务打的tag"]; leaked {
		t.Error("写穿了：业务在 hook 里打的 tag 进了还挂在 channel 上的那条原消息")
	}
	if len(bigTool.Content) != 200000 {
		t.Errorf("原消息正文被就地改写了：%d 字节", len(bigTool.Content))
	}
}

// TestEmergencyDrop_AllSystemGivesUp 钉住第四个出口 `len(conv) == 0`：整条消息流全是
// 前导 system 时**没有任何手段可用**——挖空只认 `RoleTool` 且正文超 2000 字节，而前导
// system 块里不可能有 tool 消息。
//
// 承重的是「原样返回」这个否定结论：它把「挖空这条路在这儿走不通」钉成事实，
// 防止有人把挖空扩到 system 上（那会静默改写长期记忆注入与调用方自己塞的 system）。
func TestEmergencyDrop_AllSystemGivesUp(t *testing.T) {
	mem := schema.SystemMessage("记忆" + strings.Repeat("m", 40000))
	mem.Annotations = map[string]any{StoreInjectedAnnotation: true}
	in := []*schema.Message{schema.SystemMessage("前导"), mem}

	out := emergencyDropOldestGroups(in, 50, 5000, "[MARKER]")

	if len(out) != len(in) {
		t.Fatalf("条数变了（%d → %d）——这条路上什么都不该动", len(in), len(out))
	}
	if compose_v2.EstimateMessageTokens(out) != compose_v2.EstimateMessageTokens(in) {
		t.Error("全是 system 时居然腾出了空间——挖空被扩到 system 上了？" +
			"那会静默改写长期记忆注入与调用方塞的 system")
	}
	if len(out[1].Content) != len(in[1].Content) {
		t.Errorf("system 正文被改写了：%d → %d 字节", len(in[1].Content), len(out[1].Content))
	}
}
