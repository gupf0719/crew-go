package common

// MessageUsage LLM 调用 token 消耗统计.
//
// **crew-go 扩展**: 比 spec 多 CachedInputTokens — 业务降本核心指标 (OpenAI prompt
// cache 命中率 = CachedInputTokens / InputTokens). spec 未来同步时此字段保留, 不
// 影响向后兼容 (新增字段, 0 值无影响).
//
// 字段语义:
//   - InputTokens: 喂 LLM 的 prompt 总 token (含 cached 部分)
//   - CachedInputTokens: InputTokens 中命中 cache 的部分 (provider 半价计费, 业务降本指标)
//   - OutputTokens: LLM 输出 token (不含 think)
//   - ThinkTokens: 模型推理 think token (OpenAI o1 / qwen-thinking 系列)
//   - TotalTokens: provider 返的合计 (业务校验用, 通常 = InputTokens + OutputTokens + ThinkTokens)
type MessageUsage struct {
	ThinkTokens       int32 `json:"thinkTokens"`
	InputTokens       int32 `json:"inputTokens"`
	CachedInputTokens int32 `json:"cachedInputTokens,omitempty"`
	OutputTokens      int32 `json:"outputTokens"`
	TotalTokens       int32 `json:"totalTokens"`
}
