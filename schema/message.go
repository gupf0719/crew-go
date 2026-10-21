package schema

import (
	"crew-go/modules/common"
	crand "crypto/rand"
	"encoding/hex"
)

// 类型别名 — 重新导出 spec 生成的类型，保持跨语言一致性。
type (
	Message                = common.Message
	MessageRole            = common.MessageRole
	ContentBlock           = common.ContentBlock
	ContentBlockType       = common.ContentBlockType
	TextContentBlock       = common.TextContentBlock
	ImageUrlContentBlock   = common.ImageUrlContentBlock
	ImageUrl               = common.ImageUrl
	InputAudioContentBlock = common.InputAudioContentBlock
	InputAudio             = common.InputAudio
	FileContentBlock       = common.FileContentBlock
	FileData               = common.FileData
	DataContentBlock       = common.DataContentBlock
	ToolcallContentBlock   = common.ToolcallContentBlock
	ToolresultContentBlock = common.ToolresultContentBlock
	MessageMetadata        = common.MessageMetadata
	MessageUsage           = common.MessageUsage
	LogProbContent         = common.LogProbContent
	LogProb                = common.LogProb

	// 模型 I/O 类型
	ModelConfig = common.ModelConfig
	ModelInput  = common.ModelInput
	ModelOutput = common.ModelOutput

	// 工具 I/O 类型
	ToolInput      = common.ToolInput
	ToolOutput     = common.ToolOutput
	ToolCallResult = common.ToolCallResult

	// 错误类型
	CommError = common.CommonError
)

// 角色常量。
const (
	RoleSystem    = common.MessageRoleSystem
	RoleUser      = common.MessageRoleUser
	RoleAssistant = common.MessageRoleAssistant
	RoleTool      = common.MessageRoleTool
)

// ContentBlock 类型常量（re-export）——a2a/convert.go 等按类型构造 / 判别 block。
const (
	ContentBlockTypeText       = common.ContentBlockTypeText
	ContentBlockTypeImageUrl   = common.ContentBlockTypeImageUrl
	ContentBlockTypeInputAudio = common.ContentBlockTypeInputAudio
	ContentBlockTypeFile       = common.ContentBlockTypeFile
	ContentBlockTypeToolcall   = common.ContentBlockTypeToolcall
	ContentBlockTypeToolresult = common.ContentBlockTypeToolresult
	ContentBlockTypeData       = common.ContentBlockTypeData
)

// --- 辅助构造函数 ---

// SystemMessage 创建系统消息。
func SystemMessage(content string) *Message {
	return &Message{ID: newMsgID(), Role: RoleSystem, Content: content}
}

// UserMessage 创建用户消息。
func UserMessage(content string) *Message {
	return &Message{ID: newMsgID(), Role: RoleUser, Content: content}
}

// AssistantMessage 创建助手消息。
func AssistantMessage(content string) *Message {
	return &Message{ID: newMsgID(), Role: RoleAssistant, Content: content}
}

// ToolResultMessage 创建一个 role=tool 消息。
// 工具调用 ID 和内容存储在 ToolresultContentBlock 中；
// Content 也为直接读取平面字段的提供商设置。
func ToolResultMessage(callID, _ /* name unused */, content string) *Message {
	return &Message{
		Role:    RoleTool,
		Content: content,
		ContentBlocks: []*ContentBlock{{
			Type: "tool_result",
			Toolresult: &ToolresultContentBlock{
				ToolCallId: callID,
				Content:    content,
			},
		}},
	}
}

// AssistantToolCallMessage 创建一个携带工具调用的助手消息。
func AssistantToolCallMessage(calls []ToolCall) *Message {
	msg := &Message{Role: RoleAssistant}
	for _, tc := range calls {
		msg.ContentBlocks = append(msg.ContentBlocks, &ContentBlock{
			Type: "tool_call",
			Toolcall: &ToolcallContentBlock{
				Id:        tc.ID,
				Name:      tc.Function.Name,
				Arguments: tc.Function.Arguments,
			},
		})
	}
	return msg
}

// GetToolCalls 从消息的 ContentBlocks 提取工具调用。
func GetToolCalls(msg *Message) []ToolCall {
	if msg == nil {
		return nil
	}
	var calls []ToolCall
	for _, cb := range msg.ContentBlocks {
		if cb != nil && cb.Toolcall != nil {
			calls = append(calls, ToolCall{
				ID:   cb.Toolcall.Id,
				Type: "function",
				Function: FunctionCall{
					Name:      cb.Toolcall.Name,
					Arguments: cb.Toolcall.Arguments,
				},
			})
		}
	}
	return calls
}

// GetToolResultID 从 role=tool 消息返回 tool_call_id。
func GetToolResultID(msg *Message) string {
	if msg == nil {
		return ""
	}
	for _, cb := range msg.ContentBlocks {
		if cb != nil && cb.Toolresult != nil {
			return cb.Toolresult.ToolCallId
		}
	}
	return ""
}

// newMsgID 铸一个消息级稳定身份。**实验**
func newMsgID() string {
	var b [16]byte
	_, _ = crand.Read(b[:])
	return hex.EncodeToString(b[:])
}
