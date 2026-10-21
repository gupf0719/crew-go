package agent_v2

import (
	"context"
	"fmt"
	"log/slog"
	"unicode/utf8"

	"crew-go/tool"
)

// truncatedMarkerFmt 是中间挖空后放在接缝处的说明，**带的是被删掉的量、不是剩下的量**。
//
// 这个方向是有意的：模型据它判断「值不值得去把原文读回来」，而「还剩多少」它自己看得见。
//
// ⚠️ **原文这里写「三个成熟实现独立收敛」——那个数是错的，实际是两家**（全仓 grep 核过）。
// 更要紧的是原文**漏了那两家里唯一有两层截断的那家的关键做法**：它的中缝标记**恒与一条
// 带「原始总量」的抬头配对**。少了那条抬头，两层截断下第一层的标记会落在第二层挖走的
// 中段里被吞掉，最终只报第二次删的量——实测偏小 100 倍，**而且方向正相反**
// （报小了模型就不去读回原文）。那条抬头见 [truncateMiddleKnownOriginal]。
//
// **必须留痕**：本框架的截断是在业务背后自动跑的，模型看到一段被静默截短的内容会当成
// 完整内容用。有一家的裁剪工具确实不留痕，但那是给调用方显式调用的无状态函数——
// 调用方自己知道发生了什么，前提跟我们不同。
const truncatedMarkerFmt = "\n…（中间省略 %d 字节）…\n"

// truncMiddleHeadShare 是头部占可用预算的份额（分母 5 ⟹ 头:尾 = 4:1）。
//
// **不用对半分**，虽然有两家是 50/50：第三家用 4:1 偏头，并写明是为编码 agent 的输出调的——
// 我们的场景（工具结果、文件内容、命令输出）跟它更近：**头部是内容主体，尾部主要留错误
// 与退出状态**。对半分会把一半预算花在通常没信息量的中段结尾上。
const truncMiddleHeadShare = 4

// truncMiddleMinUseful 是「中间挖空」还划得来的最小可用预算（字节）。
// 低于它就退化成只留头——那时 marker 自己就吃掉大半预算，留个两三字节的尾巴没有意义。
const truncMiddleMinUseful = 16

// truncateMiddle 把 s 压到 maxBytes 字节以内：保留头尾、中间挖空，接缝处放一条
// 说明被删了多少字节的标记。
//
// 保证（有测试钉住）：
//   - 返回值长度 <= maxBytes；
//   - s 本来就不超时**原样返回**（不白加标记——拿一个上百字节的标记去「压缩」
//     一条两百字节的内容是净亏，有两家为此各设了一道闸）；
//   - 需要截断时返回值**严格短于** s；
//   - ⚠️ **预算小到放不下标记时会退化成只留头、不留痕**（见 [truncMiddleMinUseful]）——
//     那时标记本身就吃掉大半预算，留个两三字节的尾巴没有意义。这是有意的退化，
//     但它跟上面「必须留痕」那条是**冲突的**：调用方给出这么小的预算时，得自己知道
//     拿到的是一段没有任何标记的残文。实际用它的两个调用点给的都是几千字节，够放标记。
//   - 切点回退到 UTF-8 rune 边界，不会切出半个字符。
//
// ⚠️ 它按**字节**算不按 token 算。token 只是估算，而这里要的是「一定装得下」这个硬保证；
// 调用方拿 token 预算进来时自己按经验比例换算成字节即可。
func truncateMiddle(s string, maxBytes int) string {
	return truncateMiddleKnownOriginal(s, maxBytes, 0)
}

// truncatedHeaderFmt 是「这段内容在**更早的某一刀**就已经被截过」时补的抬头。
//
// **为什么必须是抬头而不是又一条中缝标记**：中缝标记的语义天生是局部的（「这一刀在这里」），
// 而它会被后续的中间挖空整个吞掉。抬头是唯一「每次都在最前面、每次都能被前置」的锚点，
// 也是唯一装得下「原始总量」的位置——原始总量必须在**任何一刀之前**就算好。
const truncatedHeaderFmt = "【注意：这段内容此前已被截断过，原始约 %d 字节，你现在看到的远不是全部】\n"

// truncateMiddleKnownOriginal 跟 [truncateMiddle] 一样挖空，但额外接收**上游告诉它的
// 真原始字节数**；`originalBytes` 大于 `len(s)` 时（说明这段内容此前已经被截过一刀）
// 在最前面补一条抬头，把真原始量交给模型。
//
// ⚠️ **光改措辞不够，必须把真原始结构化地传进来**：第二刀手上的 `len(s)` 是第一刀之后的
// 残长，不是真原始。只把中缝措辞从「删了 N」改成「原始 M」，M 仍然是残长——偏差只会从
// 100 倍变成 6 倍，方向不变。免疫力来自**上游把真原始带过来**这个动作，措辞只是让那个
// 值可见。对照的那家正是这么做的：它优先读结构化字段，自己数正文只是没有元数据时的降级。
func truncateMiddleKnownOriginal(s string, maxBytes int, originalBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if originalBytes > len(s) {
		header := fmt.Sprintf(truncatedHeaderFmt, originalBytes)
		if len(header) < maxBytes {
			// **先给抬头留预算再截**——否则抬头会把结果顶出上限。
			return header + truncateMiddleKnownOriginal(s, maxBytes-len(header), 0)
		}
	}
	if len(s) <= maxBytes {
		return s
	}

	// 标记长度取上界：被删量不可能超过 s 自身长度，按它渲染出来的就是最长的那版。
	// 用上界预留，保证最终渲染出的标记一定塞得下，不必回头重算。
	reserve := len(fmt.Sprintf(truncatedMarkerFmt, len(s)))
	avail := maxBytes - reserve
	if avail < truncMiddleMinUseful {
		// 预算太小，挖空不划算——退化成只留头。
		return clampPrefixToUTF8(s, maxBytes)
	}
	head := clampPrefixToUTF8(s, avail*truncMiddleHeadShare/(truncMiddleHeadShare+1))
	tail := clampSuffixToUTF8(s, avail-len(head))
	removed := len(s) - len(head) - len(tail)
	return head + fmt.Sprintf(truncatedMarkerFmt, removed) + tail
}

// clampPrefixToUTF8 取 s 的前 n 字节，切点回退到 UTF-8 rune 边界（不切出半个字符）。
func clampPrefixToUTF8(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if n >= len(s) {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// clampSuffixToUTF8 取 s 的后 n 字节，切点**向后**推进到 UTF-8 rune 边界。
//
// 方向跟 [clampPrefixToUTF8] 相反：前缀往回退保证不超预算，后缀也必须往「更短」的方向
// 走才不超——所以是把起点往后推，而不是往前拉。
func clampSuffixToUTF8(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if n >= len(s) {
		return s
	}
	start := len(s) - n
	for start < len(s) && !utf8.RuneStart(s[start]) {
		start++
	}
	return s[start:]
}

// truncMaxBytesFor 给这条工具结果算适用的字节上限；返回 0 表示**不截**。
//
// 判据是**这条结果的去向**，不是「工具叫什么」——两个豁免各有各的理由：
//
//   - **框架铸的工具**（minted，见 frameworkMintedTool——按实例判，不按名字）：`final_output`
//     装的就是最终答案本身，`activate_skill` 的正文另有 `WithSkillBodyMaxChars` 管。
//   - **return-direct**：`tool.ReturnDirectMetaKey` 的 godoc 写明「短路之后没有任何人
//     再说话，**产出即答复**」——它**不进消息流、不经过任何一层压缩**，所以上限在这条
//     路上收益恒为零，代价是把对外答案改坏（实测：合法 JSON 变成非法）。不是取舍失衡，
//     是那个取舍根本不存在。
//
// ⚠️ **return-direct 必须按 `res.Metadata` 判、不能按工具名判**：它是**运行时**声明的，
// 同一个工具这次 return-direct、下次可以不是。
func truncMaxBytesFor(cfg Config, minted bool, res *tool.Result) int {
	if cfg.ToolResultMaxBytes <= 0 || res == nil {
		return 0
	}
	if minted {
		return 0
	}
	if rd, ok := res.Metadata[tool.ReturnDirectMetaKey].(bool); ok && rd {
		return 0
	}
	return cfg.ToolResultMaxBytes
}

// logToolResultTruncated 报一次截断。**只在真截了之后调**——曾经先打日志再让下游把截断
// 撤销掉，业务照那条日志建监控会以为保护生效。
func logToolResultTruncated(ctx context.Context, agent, toolName string, origBytes, keptBytes, max int) {
	slog.WarnContext(ctx, "agent_v2: 工具结果超过 ToolResultMaxBytes，已从中间挖空",
		"agent", agent, "tool", toolName,
		"original_bytes", origBytes, "kept_bytes", keptBytes, "max", max)
}
