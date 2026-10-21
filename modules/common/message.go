package common

type Message struct {
	// ID 是消息的稳定身份，构造期铸造（见 schema 的构造函数）。**实验字段**
	ID      string      `json:"id,omitempty"`
	Role    MessageRole `json:"role"`
	Content string      `json:"content"`
	// ContentBlocks 跟 spec 默认 `json:"-"` 不同——crew-go 用 ContentBlocks 承载
	// tool_call / tool_result 等结构化信息（schema.GetToolCalls / GetToolResultID 都
	// 从这里读），checkpoint 跨 InterruptError + Resume 必须保留。spec 可能假定 wire
	// 上只走 Content 字符串；我们的内部持久化与 wire 解耦（model/openai 等适配器自己
	// 做 schema → API payload 转换），改成可序列化不影响外部协议（ISSUE-59 Phase 2）。
	ContentBlocks []*ContentBlock `json:"content_blocks,omitempty"`
	Refusal       string          `json:"refusal,omitempty"`
	// ReasoningContent 是 thinking / reasoning 模型（DeepSeek deepseek-reasoner /
	// Qwen3 thinking 等）返回的推理过程文本。spec 无此字段——crew-go 为多轮 thinking
	// 场景新增（ISSUE-89）：这类模型的 wire 协议要求把上一轮 assistant 的 reasoning_content
	// 原样回传，服务端用于延续推理上下文，不回传则第 2 轮 400。model adapter（openai
	// client）解析响应的 reasoning_content 存入此字段，拼下一轮请求时附回 wire payload。
	// 非 thinking 模型该字段为空（omitempty 不序列化，多轮无影响；cp 向后兼容）。
	ReasoningContent string           `json:"reasoning_content,omitempty"`
	Annotations      map[string]any   `json:"annotations,omitempty"`
	Metadata         *MessageMetadata `json:"metadata,omitempty"`
}

// GetContent 返回这条消息的文本内容。
//
// spec 无此方法——crew-go 新增，为的是让**不认识 schema 包的下层**（`store` 是
// 叶子包，依赖方向上不能反过来 import）也能从一条消息里取出文本。典型消费者是
// `store` 的语义检索：它按 `interface{ GetContent() string }` 断言，认得出这个方法
// 的 value 才参与 embedding。
//
// ⚠️ **只返回 Content，不拼 ContentBlocks**。多模态消息的图片/音频块没有有意义的
// 文本表示，硬拼进来会污染 embedding；纯 tool_call 消息的 Content 本来就是空的，
// 那类消息也不该参与语义检索。
func (m *Message) GetContent() string {
	if m == nil {
		return ""
	}
	return m.Content
}
