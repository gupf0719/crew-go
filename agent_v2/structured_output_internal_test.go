package agent_v2

import (
	"context"
	"testing"

	"crew-go/model"
	"crew-go/schema"
)

// TestValidateStructured 表驱动验校验器：required 存在 + 顶层类型匹配。
func TestValidateStructured(t *testing.T) {
	sc := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
			"age":  map[string]any{"type": "integer"},
			"tags": map[string]any{"type": "array"},
		},
		"required": []string{"name", "age"},
	}
	cases := []struct {
		name    string
		args    map[string]any
		wantErr bool
	}{
		{"通过", map[string]any{"name": "Alice", "age": float64(30)}, false},
		{"integer接受整数值float", map[string]any{"name": "A", "age": float64(30)}, false},
		{"缺required", map[string]any{"name": "Bob"}, true},
		{"required为null", map[string]any{"name": "Bob", "age": nil}, true},
		{"类型错-age给string", map[string]any{"name": "A", "age": "30"}, true},
		{"integer拒小数", map[string]any{"name": "A", "age": float64(30.5)}, true},
		{"未声明字段放行", map[string]any{"name": "A", "age": float64(1), "extra": "x"}, false},
		{"可选字段类型对", map[string]any{"name": "A", "age": float64(1), "tags": []any{"x"}}, false},
		{"可选字段类型错", map[string]any{"name": "A", "age": float64(1), "tags": "notarray"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateStructured(c.args, sc)
			if (err != nil) != c.wantErr {
				t.Errorf("validateStructured() err=%v; wantErr=%v", err, c.wantErr)
			}
		})
	}
}

// TestRequiredKeys 验兼容 []string / []any。
func TestRequiredKeys(t *testing.T) {
	if got := requiredKeys(map[string]any{"required": []string{"a", "b"}}); len(got) != 2 {
		t.Errorf("[]string required 解析错: %v", got)
	}
	if got := requiredKeys(map[string]any{"required": []any{"a", "b", 1}}); len(got) != 2 {
		t.Errorf("[]any required 应过滤非 string: %v", got)
	}
	if got := requiredKeys(map[string]any{}); got != nil {
		t.Errorf("无 required 应返 nil: %v", got)
	}
}

// TestStructuredFinalOf 验「本轮收尾的是不是成功的 final_output」这个判据。
//
// 上一版是 `structuredFinalMessage(msgs)`——倒扫**含 prior 的消息列表**找带 annotation 的
// tool_result。那个洞已实测坐实：本轮没产出时会捞到上一轮的结构化结果并静默返回，而那
// 正是该抛 StructuredOutputError 的路径。现在判据只看**决策点交出来的本轮产出**。
func TestStructuredFinalOf(t *testing.T) {
	// 决策点交出来的形态：tools 节点把 final_output 的 tool_result 写进 basicFinalCh，
	// presentFinal 转成同内容的 assistant 并带上原 annotations。
	presented := func(content string, annos map[string]any) *schema.Message {
		m := schema.AssistantMessage(content)
		m.Annotations = annos
		return m
	}
	okFinal := func(content string) *schema.Message {
		return presented(content, map[string]any{
			ReturnDirectToolAnnotation:   FinalOutputToolName,
			ReturnDirectAnnotation:       true,
			finalOutputAttemptAnnotation: true, // 派发层认出是框架那件时盖的
		})
	}

	t.Run("成功：取校验后的规范 JSON", func(t *testing.T) {
		sm := structuredFinalOf(okFinal(`{"age":25,"name":"Bob"}`))
		if sm == nil {
			t.Fatal("应认出这是成功的 final_output 收尾")
		}
		if sm.Content != `{"age":25,"name":"Bob"}` {
			t.Errorf("Content = %q; want 工具校验后 marshal 的规范 JSON", sm.Content)
		}
	})

	t.Run("空对象产 {} 非空串", func(t *testing.T) {
		if sm := structuredFinalOf(okFinal(`{}`)); sm == nil || sm.Content != `{}` {
			t.Errorf("空对象应产 {}，got %+v", sm)
		}
	})

	t.Run("校验失败的不算——它没有 return-direct 标记", func(t *testing.T) {
		// newFinalOutputTool 校验失败走 tool.Failure、**不置** return-direct（回炉重试），
		// 所以那个标记同时就是「这次是成功的」判据。
		bad := presented(`{"name":"Bob"}`, map[string]any{
			ReturnDirectToolAnnotation:   FinalOutputToolName, // 是它，但没成功
			finalOutputAttemptAnnotation: true,
		})
		if sm := structuredFinalOf(bad); sm != nil {
			t.Errorf("失败尝试不该被当成最终答复，got %q", sm.Content)
		}
	})

	t.Run("名字是 final_output、却不是框架那件的不算", func(t *testing.T) {
		// 没配结构化输出时业务能注册一件同名的 return-direct 工具，派发层不给它盖章——按名字认的话，
		// 它的产出会被当成结构化答复。
		biz := presented(`业务产出`, map[string]any{
			ReturnDirectToolAnnotation: FinalOutputToolName,
			ReturnDirectAnnotation:     true,
		})
		if sm := structuredFinalOf(biz); sm != nil {
			t.Errorf("业务同名工具的产出不该被当成结构化答复，got %q", sm.Content)
		}
	})

	t.Run("别的 return-direct 工具收尾不算", func(t *testing.T) {
		// 业务自己的 return-direct 工具收尾时压根没有结构化产出，该抛 StructuredOutputError
		other := presented("已转交给 billing。", map[string]any{
			ReturnDirectToolAnnotation: HandoffTransferToolName,
			ReturnDirectAnnotation:     true,
		})
		if sm := structuredFinalOf(other); sm != nil {
			t.Errorf("别的工具收尾不该被当成结构化产出，got %q", sm.Content)
		}
	})

	t.Run("模型自由文本终止 / 本轮无产出 → nil", func(t *testing.T) {
		if structuredFinalOf(schema.AssistantMessage("我放弃结构化，改自由文本")) != nil {
			t.Error("自由文本终止应返 nil（让 caller 抛 StructuredOutputError）")
		}
		if structuredFinalOf(nil) != nil {
			t.Error("nil 应返 nil")
		}
	})
}

// TestLastStructuredError 验取最后一次失败的 final_output 结果（foID 锚定、按失败标记认）。
//
// 夹具照生产的形状造：final_output 失败（schema 校验失败、被 OnToolEnd 否决）的结果都带
// finalOutputRetryAnnotation——makeToolsFn / toolsNode 打的，正是决定这一轮回炉的那个判据。
func TestLastStructuredError(t *testing.T) {
	foCall := func(id string) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: id, Function: schema.FunctionCall{Name: FinalOutputToolName, Arguments: "{}"},
		}})
	}
	foFail := func(id, content string) *schema.Message {
		m := schema.ToolResultMessage(id, "", content)
		m.Annotations = map[string]any{finalOutputAttemptAnnotation: true, finalOutputRetryAnnotation: true}
		return m
	}
	// 两次 final_output 失败 → 取最后一条
	msgs := []*schema.Message{
		foCall("a"),
		foFail("a", "[structured_output_invalid] 缺 name"),
		foCall("c"),
		foFail("c", "[biz_reject] 年龄不可信"),
	}
	if got := lastStructuredError(msgs); got != "[biz_reject] 年龄不可信" {
		t.Errorf("lastStructuredError = %q; want 最后一次失败（不管是 schema 失败还是被钩子否决）", got)
	}
	if got := lastStructuredError([]*schema.Message{schema.UserMessage("x")}); got != "" {
		t.Errorf("无错误应返空, got %q", got)
	}

	// 回归（LOW-2）：业务工具 tool_result 恰好含 error code 散文，不应被误取——foID 锚定
	// 只认 final_output 的 tool_result。
	t.Run("业务工具散文含errorcode不误取", func(t *testing.T) {
		bizTool := schema.AssistantToolCallMessage([]schema.ToolCall{{
			ID: "biz", Function: schema.FunctionCall{Name: "search", Arguments: "{}"},
		}})
		msgs := []*schema.Message{
			foCall("fo"),
			foFail("fo", "[structured_output_invalid] 缺 age"),
			bizTool,
			// 业务工具输出恰好含框架内部 error code 字符串（倒扫在后）
			schema.ToolResultMessage("biz", "", "搜索结果：文档提到 structured_output_invalid 这个错误码"),
		}
		if got := lastStructuredError(msgs); got != "[structured_output_invalid] 缺 age" {
			t.Errorf("业务工具散文被误取（应锚定 final_output）: got %q", got)
		}
	})

	// 判据是失败标记、不是内容：成功的 final_output 结果碰巧含错误码字样也不算失败。曾经按内容里有没有
	// 错误码认——只认得 schema 失败，被钩子否决的认不出（LastError 为空或指错，实测）。
	t.Run("成功结果含错误码字样不算失败", func(t *testing.T) {
		msgs := []*schema.Message{
			foCall("a"),
			foFail("a", "[structured_output_invalid] 缺 name"),
			foCall("b"),
			schema.ToolResultMessage("b", "", `{"note":"structured_output_invalid"}`),
		}
		if got := lastStructuredError(msgs); got != "[structured_output_invalid] 缺 name" {
			t.Errorf("成功的结果不该被当成失败: got %q", got)
		}
	})
}

// --- supportsForcedToolChoice ---

type fakeCapModel struct{ cap model.StructuredCapability }

func (f fakeCapModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, nil
}
func (f fakeCapModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (f fakeCapModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error)  { return f, nil }
func (f fakeCapModel) StructuredCapability(string) model.StructuredCapability { return f.cap }

type plainCapModel struct{}

func (plainCapModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, nil
}
func (plainCapModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (plainCapModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) {
	return plainCapModel{}, nil
}

// TestSupportsForcedToolChoice 验能力门控：显式 false 不强制；未实现 capability 默认支持。
func TestSupportsForcedToolChoice(t *testing.T) {
	if !supportsForcedToolChoice(fakeCapModel{model.NonStrictToolCapability()}) {
		t.Error("NonStrict（SupportsToolChoiceRequired=true）应支持")
	}
	noReq := model.StructuredCapability{SupportsToolChoiceRequired: false}
	if supportsForcedToolChoice(fakeCapModel{noReq}) {
		t.Error("显式 SupportsToolChoiceRequired=false 应不强制（qwen-3-coder）")
	}
	if !supportsForcedToolChoice(plainCapModel{}) {
		t.Error("未实现 StructuredCapable 的 model 应默认支持")
	}
}
