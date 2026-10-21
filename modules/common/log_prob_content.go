package common

// LogProbContent 描述输出序列中一个 token 位置的对数概率信息。
// 内嵌 LogProb 携带当前 token 自身的概率数据（Token/Bytes/Logprob），
// TopLogprobs 列出该位置 top-N 候选 token 的概率，对齐 OpenAI logprobs 响应结构。
type LogProbContent struct {
	LogProb
	TopLogprobs []*LogProb `json:"topLogprobs"`
}
