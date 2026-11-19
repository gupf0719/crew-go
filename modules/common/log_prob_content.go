package common

// LogProbContent 描述输出序列中一个 token 位置的对数概率信息。
// 内嵌 LogProb 携带当前词元的身份与对数概率。
// TopLogprobs 列出供应商返回的该位置候选词元及其对数概率。
type LogProbContent struct {
	LogProb
	TopLogprobs []*LogProb `json:"topLogprobs"`
}
