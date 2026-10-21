package common

// MessageUsage LLM 调用 token 消耗统计.
//
// **crew-go 扩展**: 比 spec 多 CachedInputTokens / CacheCreationInputTokens — 业务降本核心
// 指标 (prompt cache 命中率 = CachedInputTokens / InputTokens). 新增字段, 0 值无影响, 向后兼容.
//
// 字段语义:
//   - InputTokens: 喂 LLM 的 prompt 总 token (含命中 cache 与写入 cache 的部分)
//   - CachedInputTokens: InputTokens 中命中 cache 的部分 (provider 折价计费, 业务降本指标)
//   - CacheCreationInputTokens: InputTokens 中本次写入 cache 的部分 (Anthropic 按写入价计费,
//     比普通输入贵; 只报 cache 读的 provider 恒为 0)
//   - OutputTokens: LLM 输出 token (不含 think)
//   - ThinkTokens: 模型推理 think token (OpenAI o1 / qwen-thinking / Claude thinking)
//   - TotalTokens: provider 返的合计 (业务校验用, 通常 = InputTokens + OutputTokens + ThinkTokens)
type MessageUsage struct {
	ThinkTokens              int32 `json:"thinkTokens"`
	InputTokens              int32 `json:"inputTokens"`
	CachedInputTokens        int32 `json:"cachedInputTokens,omitempty"`
	CacheCreationInputTokens int32 `json:"cacheCreationInputTokens,omitempty"`
	OutputTokens             int32 `json:"outputTokens"`
	TotalTokens              int32 `json:"totalTokens"`
}
