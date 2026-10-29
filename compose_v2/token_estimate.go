package compose_v2

import (
	"encoding/json"

	"crew-go/schema"
)

// Token 估算（ISSUE-65）
//
// crew-go 的上下文压缩 4 层（MicroCompact / AutoCompact / ReactiveCompact）
// 都需要估算消息的 token 数。优先用 LLM 调用返回的真实 Usage，不可用时回退
// 到字符级估算 + 4/3 保守填充——实际 token 通常比字符估算多 ~33%，宁多估
// 不少估（少估会触发 prompt-too-long）。
//
// 设计参考 Claude Code 的 estimateMessageTokens（基础原则、image/document
// 固定 2K、×4/3 填充），但适配我们 schema.Message 的 ContentBlocks 结构。

const (
	// charsPerToken 是经验法则：4 字符 ≈ 1 token（英文中等密度）。
	// 中文密度更高（~2 字符/token），但 ×4/3 填充覆盖了误差。
	charsPerToken = 4

	// imageTokenEstimate 是 image_url block 的固定 token 估算。
	// 参考 Claude Code：图片不论大小都按 2000 估（vision model 的 patch 数量近似）。
	imageTokenEstimate = 2000

	// fileTokenEstimate 是 file block 的固定 token 估算。
	fileTokenEstimate = 2000

	// audioTokenEstimate 是 input_audio block 的固定 token 估算。
	audioTokenEstimate = 2000

	// fillFactorNumerator / fillFactorDenominator 是保守填充比例：
	// 估算结果 × 4 / 3 ≈ 1.33 倍——实际 token 通常多 ~33%。
	fillFactorNumerator   = 4
	fillFactorDenominator = 3
)

// EstimateMessageTokens 估算消息序列的 token 数（按 ContentBlock 类型精细计算）。
//
// 估算规则：
//   - 顶层 Content / ReasoningContent 字段: 各 len/4（reasoning 多轮回传上 wire，故计入）
//   - text block:  len/4
//   - tool_use:    len(name + json(args))/4
//   - tool_result: 字符串 len/4；image/file/audio block 固定 2000
//   - 兜底:        len(json(block))/4
//
// 同一份正文只算一次：Content 非空时，第一个内容与它相同的 text / tool_result 块不再计入
// （schema.ToolResultMessage 把正文同时放进 Content 与 tool_result 块，wire 上只发一份）。
// 曾经两处都算，工具结果估成两倍，工具多的会话里 AutoCompact 在真实用量一半时就触发。
// 内容不同的块照算（宁多估），同一条消息里后面的 tool_result 在 wire 上各自单独发、也照算。
//
// 最后 ×4/3 保守填充。
//
// 注意：本函数不读 message.id / RunID 等元数据，纯按 content 估算。
// 真实 token 数应优先走 estimateTokensFromAPIUsage（API 返回值）。
func EstimateMessageTokens(messages []*schema.Message) int {
	total := 0
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		// 1. 直接 Content 字段（user / assistant / tool 短消息）
		if msg.Content != "" {
			total += len(msg.Content) / charsPerToken
		}
		// 1b. ReasoningContent（thinking 模型推理文本）——多轮回传时随 assistant 消息
		// 上 wire（见 model/openai convertMessages），故必须计入估算，否则 AutoCompact
		// 对 reasoning-heavy 历史低估 token、压缩触发偏晚（ISSUE-89 follow-up）。
		if msg.ReasoningContent != "" {
			total += len(msg.ReasoningContent) / charsPerToken
		}
		// 2. ContentBlocks（多模态 / tool_use / tool_result）；与 Content 同一份正文的那个块已算过
		dupPending := msg.Content != ""
		for _, cb := range msg.ContentBlocks {
			if cb == nil {
				continue
			}
			if dupPending && blockRepeatsContent(cb, msg.Content) {
				dupPending = false
				continue
			}
			total += estimateContentBlockTokens(cb)
		}
	}
	// 保守填充
	return total * fillFactorNumerator / fillFactorDenominator
}

// blockRepeatsContent 判一个 text / tool_result 块的正文是不是就是消息的 Content。
func blockRepeatsContent(cb *schema.ContentBlock, content string) bool {
	switch {
	case cb.Text != nil:
		return cb.Text.Text == content
	case cb.Toolresult != nil:
		return cb.Toolresult.Content == content
	default:
		return false
	}
}

// estimateContentBlockTokens 估算单个 ContentBlock 的 token 数。
func estimateContentBlockTokens(cb *schema.ContentBlock) int {
	if cb == nil {
		return 0
	}
	switch {
	case cb.Text != nil:
		return len(cb.Text.Text) / charsPerToken
	case cb.Toolcall != nil:
		// tool_use: name + JSON args（不算 wrapper / id）
		size := len(cb.Toolcall.Name) + len(cb.Toolcall.Arguments)
		return size / charsPerToken
	case cb.Toolresult != nil:
		// tool_result: 内容字符数。Toolresult.Content 是 string
		return len(cb.Toolresult.Content) / charsPerToken
	case cb.ImageUrl != nil:
		return imageTokenEstimate
	case cb.File != nil:
		return fileTokenEstimate
	case cb.InputAudio != nil:
		return audioTokenEstimate
	default:
		// 未知类型兜底：JSON 序列化按字符估算
		if b, err := json.Marshal(cb); err == nil {
			return len(b) / charsPerToken
		}
		return 0
	}
}

// estimateTokensFromAPIUsage 优先返回最近一轮 LLM 调用的 Usage.InputTokens
// （API 真实数字，比字符估算精确）。若无可用 Usage 数据返回 (0, false)，调用方
// 应回退到 EstimateMessageTokens。
//
// 实现：倒扫 messages，找最近一条 assistant 消息的 Metadata.Usage。
// 该 Usage 由 callModelStreaming / callModelStep 在 OnModelEnd 之前写入。
func estimateTokensFromAPIUsage(messages []*schema.Message) (int, bool) {
	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		if msg == nil || msg.Role != schema.RoleAssistant {
			continue
		}
		if msg.Metadata == nil || msg.Metadata.Usage == nil {
			continue
		}
		// InputTokens 是上一轮 LLM 看到的真实输入 token 数
		// 当前 messages 比那时多一些（assistant 输出 + 后续 tool_result + user）
		// 但近似已足够触发阈值判断
		input := int(msg.Metadata.Usage.InputTokens)
		if input > 0 {
			return input, true
		}
	}
	return 0, false
}

// EstimateTokens 优先 API 真实 Usage，回退字符估算（推荐统一入口）。
func EstimateTokens(messages []*schema.Message) int {
	if n, ok := estimateTokensFromAPIUsage(messages); ok {
		return n
	}
	return EstimateMessageTokens(messages)
}
