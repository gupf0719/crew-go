package common

type MessageMetadata struct {
	Created int32 `json:"created"`
	// Model provider 返回的实际 model name (response model). model client 填,
	// observability 从此读 gen_ai.response.model trace attr + 指标维度
	// (不破坏 ChatModel interface — 业务自定义 model 可选填).
	// 空字符串表示 client 未填.
	Model string `json:"model,omitempty"`
	// System model 供应商 / provider (e.g. "openai" / "deepseek" / "dashscope" /
	// "qwen"). model client 填 (openai.WithSystem 注入), observability 从此读
	// gen_ai.provider.name trace span attr + 指标维度. 空字符串表示 client 未填.
	System       string            `json:"system,omitempty"`
	Usage        *MessageUsage     `json:"usage"`
	FinishReason string            `json:"finishReason"`
	Logprobs     []*LogProbContent `json:"logprobs"`
}
