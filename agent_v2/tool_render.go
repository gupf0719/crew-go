package agent_v2

import (
	"context"
	"strings"

	"crew-go/schema"
	"crew-go/tool"
)

// toolResultSizePendingAnnotation 标「这条 tool_result 的正文还没按大小处理」：开了转存时，业务工具的结果在 OnToolEnd
// 之后才转存 / 截断（见 renderToolContent），tools 节点在钩子与批内裁决之后按它处理、处理完摘掉。只在一次 tools 节点
// 执行里活着（工具账里的那份续跑时照样按它处理）。
const toolResultSizePendingAnnotation = "crew_tool_result_size_pending"

// renderedToolContent 是一条工具结果交给模型的正文，连同处理时记下的东西。
type renderedToolContent struct {
	content string
	// origBytes 记「截断之前正文有多少字节」——0 表示没截过。它要一路带到消息的 annotation 上，因为
	// **下一刀（紧急压缩兜底）手上的 len 是这一刀之后的残长**。
	origBytes int
	// spillID / spillInfo 是超大输出转存后的取回 id 与说明（见 tool_spill.go）；空 = 没转存。
	spillID, spillInfo string
	// toolErr 是业务工具声明的失败（tool.Failure），message 已按大小处理——见 runOne 里的 markToolFailure。
	toolErr *tool.Error
	// pending：正文没按大小处理，等 OnToolEnd 之后再做（deferSize 时，见 renderToolContent）。
	pending bool
}

// renderToolContent 把工具返回的 res 变成交给模型的正文：失败按 `[code] message`、成功按 Output，再按大小处理
// （sizeToolContent：超过转存阈值的转存、超过 ToolResultMaxBytes 的从中间挖空）。框架铸的工具与 return-direct
// 结果不按大小处理——前者的输出是结构化的或者本身有上限（`final_output` 装的就是答案），后者产出即答复。
//
// deferSize 为 true 且开了转存时，按大小处理留到 OnToolEnd 之后（同步执行的路径，见 basic_builder.go 的 tools
// 节点）：钩子要看到、改写的是全文——转存之后能取回的是全文，钩子之前就转存的话，结果脱敏这类改写只作用在预览上，
// 模型用 read_tool_output 照样读回没脱敏的原文。后台任务结束时没有钩子这一步，传 false 当场处理。
func renderToolContent(ctx context.Context, cfg *Config, minted bool, toolName string, res *tool.Result, deferSize bool) renderedToolContent {
	var r renderedToolContent
	if res == nil {
		return r
	}
	failed := res.Error != nil
	code, body := "", ""
	if failed {
		code, body = res.Error.Code, res.Error.Message
	} else {
		body = tool.OutputText(res.Output)
	}
	sized := sizedToolContent{body: body}
	switch {
	case minted || isReturnDirectResult(res):
		// 不按大小处理
	case deferSize && cfg.ToolResultSpill != nil:
		r.pending = true
	default:
		sized = sizeToolContent(ctx, cfg, toolName, body)
	}
	r.origBytes, r.spillID, r.spillInfo = sized.origBytes, sized.spillID, sized.spillInfo
	if failed {
		r.content = toolErrorContent(code, sized.body)
		// **不改业务那份 res.Error**——另铸一个，免得业务持有的对象被我们写穿。
		r.toolErr = &tool.Error{Code: code, Message: sized.body}
		return r
	}
	r.content = sized.body
	return r
}

func isReturnDirectResult(res *tool.Result) bool {
	if res == nil {
		return false
	}
	rd, ok := res.Metadata[tool.ReturnDirectMetaKey].(bool)
	return ok && rd
}

// sizedToolContent 是 sizeToolContent 的结果：处理后的正文（失败时是 message 部分），连同记下的东西。
type sizedToolContent struct {
	body               string
	origBytes          int
	spillID, spillInfo string
}

// sizeToolContent 按大小处理一段工具结果正文（失败结果传 message 部分，`[code]` 前缀由调用方拼）：
//
//   - 超过转存阈值（开了 [WithToolResultSpill]）：全文转存，正文换成说明 + 头尾预览 + 取回 id；转存不了时截断到阈值，
//     原始大小记下来给下一刀看。
//   - 超过 ToolResultMaxBytes：从中间挖空。转存之后剩下的是预览，通常不再碰它。
//
// 同步执行（OnToolEnd 之后，见 finishPendingToolResult）与后台任务结束时（renderToolContent）用的是这同一份。
//
// ⚠️ **作用在 message / Output 上，不在拼好的 `[code] message` 上**：失败路径下游按 `(ErrorCode, Error)` 重算
// content，在拼好的串上截会被完全撤销（实测模型实收 40027 字节一个字节没少，而框架已经打了一条
// `kept_bytes=2000` 的 WARN）。
func sizeToolContent(ctx context.Context, cfg *Config, toolName, body string) sizedToolContent {
	r := sizedToolContent{body: body}
	if sp := cfg.ToolResultSpill; sp != nil && len(r.body) > sp.threshold() {
		orig := len(r.body)
		r.body, r.spillID, r.spillInfo = spillToolResult(ctx, sp, toolName, r.body)
		if r.spillID == "" {
			r.origBytes = orig
		}
	}
	if trunc := cfg.ToolResultMaxBytes; trunc > 0 && len(r.body) > trunc {
		if r.origBytes == 0 {
			r.origBytes = len(r.body)
		}
		before := len(r.body)
		r.body = truncateMiddle(r.body, trunc)
		logToolResultTruncated(ctx, cfg.Name, toolName, before, len(r.body), trunc)
	}
	return r
}

// finishPendingToolResult 在 OnToolEnd 与批内裁决之后，按大小处理一条还没处理过的 tool_result（就地改 msg）：
// 正文是钩子改写之后的那份，转存的就是它——模型取回的是钩子允许它看的内容。failed 是钩子之后这条算不算失败。
//
// code 是钩子之后的错误码（钩子可能改了它，消息上的失败标注是钩子之前的）。
func finishPendingToolResult(ctx context.Context, cfg *Config, toolName string, msg *schema.Message, failed bool, code string) {
	if msg == nil {
		return
	}
	delete(msg.Annotations, toolResultSizePendingAnnotation)
	content := msg.Content
	if content == "" && len(msg.ContentBlocks) > 0 && msg.ContentBlocks[0].Toolresult != nil {
		content = msg.ContentBlocks[0].Toolresult.Content
	}
	body := content
	if failed {
		body = strings.TrimPrefix(content, toolErrorContent(code, ""))
	}
	sized := sizeToolContent(ctx, cfg, toolName, body)
	if sized.body == body {
		return
	}
	if failed {
		content = toolErrorContent(code, sized.body)
		if _, ok := msg.Annotations[toolErrorMessageAnnotation]; ok {
			msg.Annotations[toolErrorMessageAnnotation] = sized.body
		}
	} else {
		content = sized.body
	}
	msg.Content = content
	if len(msg.ContentBlocks) > 0 && msg.ContentBlocks[0].Toolresult != nil {
		msg.ContentBlocks[0].Toolresult.Content = content
	}
	annotateSizedToolResult(msg, sized)
}

// annotateSizedToolResult 把按大小处理时记下的东西盖到消息上：转存了盖取回 id 与说明，截断了盖原始字节数。
func annotateSizedToolResult(msg *schema.Message, sized sizedToolContent) {
	if sized.spillID == "" && sized.origBytes == 0 {
		return
	}
	if msg.Annotations == nil {
		msg.Annotations = map[string]any{}
	}
	if sized.origBytes > 0 {
		msg.Annotations[TruncatedOriginalBytesAnnotation] = sized.origBytes
	}
	if sized.spillID != "" {
		msg.Annotations[ToolResultSpillAnnotation] = sized.spillID
		msg.Annotations[toolResultSpillInfoAnnotation] = sized.spillInfo
	}
}
