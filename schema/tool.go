package schema

import (
	"encoding/json"

	"crew-go/modules/common"
)

// ToolInfo 是 spec 生成类型的 alias，跨语言保持一致。
type ToolInfo = common.ToolInfo

// ToolCall 表示模型请求的工具调用。
type ToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"` // 始终为 "function"
	Function FunctionCall `json:"function"`
}

// FunctionCall 保存函数名和 JSON 编码的参数。
type FunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // JSON 字符串
}

// ParseArguments 将 JSON 参数解码为映射。
func (fc *FunctionCall) ParseArguments() (map[string]any, error) {
	var args map[string]any
	if err := json.Unmarshal([]byte(fc.Arguments), &args); err != nil {
		return nil, err
	}
	return args, nil
}

// ToolChoice 控制模型如何选择工具。
type ToolChoice string

const (
	ToolChoiceAuto     ToolChoice = "auto"
	ToolChoiceNone     ToolChoice = "none"
	ToolChoiceRequired ToolChoice = "required"
)
