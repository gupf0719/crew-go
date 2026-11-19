package common

// LogProb 是一个词元的原始对数概率。Bytes 保留供应商返回的 UTF-8 字节值。
type LogProb struct {
	// Token 是词元 ID，TokenText 是供应商提供的词元文本。
	Token     int32   `json:"token"`
	TokenText string  `json:"tokenText,omitempty"`
	Bytes     []int32 `json:"bytes"`
	// Logprob 为 nil 表示供应商未给出数值。零是有效的对数概率。
	Logprob *float64 `json:"logprob"`
}
