package anthropic

import (
	"errors"
	"slices"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/packages/param"
	"github.com/anthropics/anthropic-sdk-go/shared/constant"
)

// ContextEditing 配置服务端清理历史工具结果和 thinking 块。
type ContextEditing struct {
	ToolUses *ToolUseClearing  // nil 时不发送工具清理策略
	Thinking *ThinkingClearing // nil 时不发送 thinking 清理策略
}

// ToolUseClearing 控制达到阈值后清理哪些旧工具调用的结果。
type ToolUseClearing struct {
	// 两种触发阈值至多设置一个。两者均为零时用 API 的默认输入 token 阈值。
	TriggerTokens   int64
	TriggerToolUses int64
	// KeepToolUses 是保留的最近工具调用数量。nil 用 API 默认值，指向 0 表示保留零条。
	KeepToolUses *int64
	// ClearAtLeastTokens 是每次清理至少释放的 token 数。零表示沿用 API 默认。
	ClearAtLeastTokens int64
	// ExcludeTools 保留这些工具的调用及结果。
	ExcludeTools []string
	// ClearToolInputs 为 true 时同时清除被选中调用的参数。
	// 只清理部分工具的参数时，使用 ClearToolInputsFor 填工具名。
	ClearToolInputs    bool
	ClearToolInputsFor []string
}

// ThinkingClearing 控制保留的 thinking 块。两项均为零值时用 API 默认的一轮。
type ThinkingClearing struct {
	KeepTurns int64 // 保留最近几轮，API 将当前轮计入
	KeepAll   bool  // 保留全部 thinking，要求 KeepTurns 为零
}

// WithContextEditing 设置服务端上下文编辑。未配置的策略沿用 API 默认行为。
// 同时启用两个清理策略时先发 thinking 清理。配置 [WithCompaction] 时，压缩策略排在清理策略后。
// 客户端回放原消息。对支持 preserved thinking 的模型，服务端清理时保持其余 thinking 签名有效。
// 清理使对应位置起的 prompt cache 失效，后续请求可复用新缓存。非法配置在构建请求时报错。
func WithContextEditing(cfg ContextEditing) ClientOption {
	if cfg.ToolUses != nil {
		toolUses := *cfg.ToolUses
		toolUses.ExcludeTools = slices.Clone(toolUses.ExcludeTools)
		toolUses.ClearToolInputsFor = slices.Clone(toolUses.ClearToolInputsFor)
		if toolUses.KeepToolUses != nil {
			keep := *toolUses.KeepToolUses
			toolUses.KeepToolUses = &keep
		}
		cfg.ToolUses = &toolUses
	}
	if cfg.Thinking != nil {
		thinking := *cfg.Thinking
		cfg.Thinking = &thinking
	}
	return func(c *Client) { c.contextEditing = cfg }
}

func (cfg ContextEditing) edits() ([]anthropic.BetaContextManagementConfigEditUnionParam, error) {
	var edits []anthropic.BetaContextManagementConfigEditUnionParam
	if thinking := cfg.Thinking; thinking != nil {
		edit, err := thinking.editParam()
		if err != nil {
			return nil, err
		}
		edits = append(edits, anthropic.BetaContextManagementConfigEditUnionParam{OfClearThinking20251015: &edit})
	}
	if toolUses := cfg.ToolUses; toolUses != nil {
		edit, err := toolUses.editParam()
		if err != nil {
			return nil, err
		}
		edits = append(edits, anthropic.BetaContextManagementConfigEditUnionParam{OfClearToolUses20250919: &edit})
	}
	return edits, nil
}

func (cfg ThinkingClearing) editParam() (anthropic.BetaClearThinking20251015EditParam, error) {
	var edit anthropic.BetaClearThinking20251015EditParam
	switch {
	case cfg.KeepTurns < 0:
		return edit, errors.New("anthropic: context editing keep thinking turns must be non-negative")
	case cfg.KeepAll && cfg.KeepTurns > 0:
		return edit, errors.New("anthropic: context editing cannot keep all thinking and set a turn count")
	case cfg.KeepAll:
		edit.Keep.OfAll = constant.ValueOf[constant.All]()
	case cfg.KeepTurns > 0:
		edit.Keep.OfThinkingTurns = &anthropic.BetaThinkingTurnsParam{Value: cfg.KeepTurns}
	}
	return edit, nil
}

func (cfg ToolUseClearing) editParam() (anthropic.BetaClearToolUses20250919EditParam, error) {
	var edit anthropic.BetaClearToolUses20250919EditParam
	switch {
	case cfg.TriggerTokens < 0 || cfg.TriggerToolUses < 0:
		return edit, errors.New("anthropic: context editing tool clearing thresholds must be non-negative")
	case cfg.TriggerTokens > 0 && cfg.TriggerToolUses > 0:
		return edit, errors.New("anthropic: context editing tool clearing accepts one trigger")
	case cfg.TriggerTokens > 0:
		edit.Trigger.OfInputTokens = &anthropic.BetaInputTokensTriggerParam{Value: cfg.TriggerTokens}
	case cfg.TriggerToolUses > 0:
		edit.Trigger.OfToolUses = &anthropic.BetaToolUsesTriggerParam{Value: cfg.TriggerToolUses}
	}
	if cfg.KeepToolUses != nil {
		if *cfg.KeepToolUses < 0 {
			return edit, errors.New("anthropic: context editing keep tool uses must be non-negative")
		}
		// 显式填 Type，让 value 为 0 时也能序列化 keep。
		edit.Keep = anthropic.BetaToolUsesKeepParam{Type: constant.ValueOf[constant.ToolUses](), Value: *cfg.KeepToolUses}
	}
	if cfg.ClearAtLeastTokens < 0 {
		return edit, errors.New("anthropic: context editing minimum cleared tokens must be non-negative")
	}
	if cfg.ClearAtLeastTokens > 0 {
		edit.ClearAtLeast = anthropic.BetaInputTokensClearAtLeastParam{Value: cfg.ClearAtLeastTokens}
	}
	edit.ExcludeTools = cfg.ExcludeTools
	if cfg.ClearToolInputs {
		if len(cfg.ClearToolInputsFor) > 0 {
			return edit, errors.New("anthropic: context editing cannot clear all tool inputs and specify tools")
		}
		edit.ClearToolInputs.OfBool = param.NewOpt(true)
	} else if len(cfg.ClearToolInputsFor) > 0 {
		edit.ClearToolInputs.OfStringArray = cfg.ClearToolInputsFor
	}
	return edit, nil
}
