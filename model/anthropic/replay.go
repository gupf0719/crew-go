package anthropic

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	"crew-go/schema"
)

const (
	// AssistantContentAnnotation 是本包给返回的 assistant 消息打的标注键。值是一个字符串（JSON）：
	// 这条消息在 API 那边的 content（请求形状，块的顺序、文本边界、thinking 签名都在），外加一个
	// 领域字段（Content 与工具调用）的指纹、一个只算 Content 的指纹。下一轮转换这条消息时，指纹对得上
	// 就原样回放这份 content；对不上（文本或工具调用被改过）就按这份 content 的块序合并现在的内容
	// （见 mergeEdited）。
	//
	// 值是字符串而不是结构体：标注要经过存档的 JSON 往返，字符串往返前后一字不差。业务不需要读它；
	// 删掉它的话这条消息按领域字段重建，thinking 块随之丢失、后面的 thinking 跟着作废。
	AssistantContentAnnotation = "anthropic.assistant_content"
	// InputTransformationsAnnotation 记 API 对这次请求的输入做过的改动（响应的 input_transformations，
	// 典型是丢掉了对不上前缀的 thinking 块），按（种类, 原因）归并，每组 {"type", "reason", "count",
	// "first_path"}；没有改动时不打。用 [InputTransformations] 读。
	InputTransformationsAnnotation = "anthropic.input_transformations"
	// FallbackAnnotation 记这次响应里服务端回退的切换，每项 {"from": 模型, "to": 模型}；没有回退时
	// 不打。用 [Fallbacks] 读。
	FallbackAnnotation = "anthropic.fallback"
	// CompactionAnnotation 记这次响应里服务端压缩（[WithCompaction]）的用量，每次压缩一项
	// {"input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"}——这些不在
	// 消息的 Usage 里（Usage 只算模型答的那一轮，也就是压缩之后的上下文）；没有压缩时不打。用 [Compactions] 读。
	CompactionAnnotation = "anthropic.compaction"
)

// replayVersion 是 [AssistantContentAnnotation] 的格式版本：读到别的版本时按「没有快照」处理。
const replayVersion = 1

// replayEnvelope 是 [AssistantContentAnnotation] 的值解出来的样子。
type replayEnvelope struct {
	V int `json:"v"`
	// Digest 是 Content 与工具调用合起来的指纹：对得上就原样回放。
	Digest string `json:"digest"`
	// TextDigest 只算 Content：整体对不上时据它判断文本改没改，文本没改就原样保留文本块。
	TextDigest string `json:"text_digest"`
	// Content 是 assistant 消息的 content 块（请求形状），json.RawMessage 保住每块的原样字节。
	Content []json.RawMessage `json:"content"`
}

// encodeReplay 编码回放快照：content 块原样，指纹按 text 与工具调用算。
func encodeReplay(content []json.RawMessage, text string, calls []schema.ToolCall) (string, error) {
	raw, err := json.Marshal(replayEnvelope{
		V:          replayVersion,
		Digest:     replayDigest(text, calls),
		TextDigest: replayDigest(text, nil),
		Content:    content,
	})
	if err != nil {
		return "", fmt.Errorf("encode replay snapshot: %w", err)
	}
	return string(raw), nil
}

// decodeReplay 取出消息上的回放快照。没有、格式不对、版本不认识时 ok 为 false。
func decodeReplay(m *schema.Message) (replayEnvelope, bool) {
	s, ok := m.Annotations[AssistantContentAnnotation].(string)
	if !ok || s == "" {
		return replayEnvelope{}, false
	}
	var env replayEnvelope
	if err := json.Unmarshal([]byte(s), &env); err != nil || env.V != replayVersion {
		return replayEnvelope{}, false
	}
	return env, true
}

// replayDigest 是领域字段的指纹：Content 与按序的工具调用（ID、名字、参数）。每个字段带长度前缀，
// 拼接无歧义。
func replayDigest(text string, calls []schema.ToolCall) string {
	h := sha256.New()
	write := func(s string) {
		_, _ = h.Write([]byte(strconv.Itoa(len(s)))) // hash.Hash 的 Write 按契约不返回错误
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(s))
	}
	write(text)
	for _, tc := range calls {
		write(tc.ID)
		write(tc.Function.Name)
		write(tc.Function.Arguments)
	}
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:16])
}

// rawBlock 是原样块里合并改动时要看的几个字段；解不出时 Type 为空。
type rawBlock struct {
	Type  string          `json:"type"`
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

func parseRawBlock(raw json.RawMessage) rawBlock {
	var b rawBlock
	if json.Unmarshal(raw, &b) != nil {
		return rawBlock{}
	}
	return b
}

// InputTransformation 是 API 对这次请求输入的一类改动（响应 input_transformations 按种类与原因
// 归并）。
//
// 按类计数而不是逐块记：一处历史改动会让之后的 thinking 块在每次请求里都被丢一遍，逐块记的话第 N 轮
// 的消息带 N 项，整场会话的标注按 N² 增长。
type InputTransformation struct {
	// Type 是改动的种类：thinking_dropped（块被丢掉）、thinking_mismatch_allowed（块对不上前缀但
	// 账号未强制校验，原样用了）等；以后可能增加。
	Type string
	// Reason 是原因：prefix_binding_mismatch（前缀被改过）、model_binding_mismatch（换了读不了这个
	// 块的模型）等；以后可能增加。
	Reason string
	// Count 是这一类涉及的块数。
	Count int
	// FirstPath 是其中第一处在请求里的位置，如 messages.7.content.0（按本包发出的 messages 数组计）；
	// 前缀被改过时它大致指向改动之后的第一个 thinking 块。
	FirstPath string
}

// InputTransformations 读出 msg 上 [InputTransformationsAnnotation] 记的改动；没有时返回 nil。
// 标注经过存档往返后是 []any 与 map[string]any、数字成了 float64，这里按那个形状读。
func InputTransformations(msg *schema.Message) []InputTransformation {
	var out []InputTransformation
	for _, item := range annotationItems(msg, InputTransformationsAnnotation) {
		out = append(out, InputTransformation{
			Type:      stringOf(item["type"]),
			Reason:    stringOf(item["reason"]),
			Count:     intOf(item["count"]),
			FirstPath: stringOf(item["first_path"]),
		})
	}
	return out
}

// FallbackHop 是服务端回退的一次切换。
type FallbackHop struct {
	From string // 拒绝了请求的模型
	To   string // 接着答的模型
}

// Fallbacks 读出 msg 上 [FallbackAnnotation] 记的回退切换；没有回退时返回 nil。
func Fallbacks(msg *schema.Message) []FallbackHop {
	var out []FallbackHop
	for _, item := range annotationItems(msg, FallbackAnnotation) {
		out = append(out, FallbackHop{From: stringOf(item["from"]), To: stringOf(item["to"])})
	}
	return out
}

// CompactionUsage 是一次服务端压缩（摘要那一步）的用量。
type CompactionUsage struct {
	InputTokens              int
	OutputTokens             int
	CacheReadInputTokens     int
	CacheCreationInputTokens int
}

// Compactions 读出 msg 上 [CompactionAnnotation] 记的服务端压缩；没有压缩时返回 nil。
func Compactions(msg *schema.Message) []CompactionUsage {
	var out []CompactionUsage
	for _, item := range annotationItems(msg, CompactionAnnotation) {
		out = append(out, CompactionUsage{
			InputTokens:              intOf(item["input_tokens"]),
			OutputTokens:             intOf(item["output_tokens"]),
			CacheReadInputTokens:     intOf(item["cache_read_input_tokens"]),
			CacheCreationInputTokens: intOf(item["cache_creation_input_tokens"]),
		})
	}
	return out
}

// annotationItems 把标注值读成 []map[string]any（本包写入的形状是 []any of map[string]any，JSON
// 往返后不变）；不是这个形状的项跳过。
func annotationItems(msg *schema.Message, key string) []map[string]any {
	if msg == nil {
		return nil
	}
	list, ok := msg.Annotations[key].([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(list))
	for _, v := range list {
		if m, ok := v.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func stringOf(v any) string {
	s, _ := v.(string)
	return s
}

// intOf 读标注里的整数：写入时是 int，JSON 往返后是 float64。
func intOf(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	default:
		return 0
	}
}
