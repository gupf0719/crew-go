package agent_v2

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"

	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// 结构化输出（StructuredOutput）——聚焦 P1：业务声明一次 output schema，框架注册一个
// final_output 工具（return-direct 自校验），模型调它产出 typed JSON；校验失败返 error
// tool_result → 复用 BasicAgent 既有 ReAct 循环回炉重生成（tools→model 边天然重试，零
// 额外引擎）。最终消息由 postamble 从 final_output 的 Arguments 合成（**不是**从消息列表
// 里倒扫挑一条——那条老路会把内部记账文本或上一轮的陈旧答复当成本轮答案）。
// 机制固定为 ModeTool（forced output tool），覆盖最广、任何支持
// function calling 的 provider 都认。设计与取舍见 doc/v2/structured-output.md。
//
// P1 不做（文档化约束）：response_format strict 路径 / 多模式自动选 / fallback 交互 /
// 嵌套跨 AgentAsTool 边界保对象 / refusal 分流（这些属 P2/P3，本轮不实现）。

// FinalOutputToolName 是结构化输出终结工具名——模型调它提交最终 typed JSON 答复。
const FinalOutputToolName = "final_output"

// StructuredOutputInvalidCode 是 final_output 校验失败的 tool error code（业务级，反馈
// LLM 让其修正重调，不 abort 整 Run）。不在 tool.IsFrameworkError 列表。
const StructuredOutputInvalidCode = "structured_output_invalid"

// outputConfig 是 WithOutputSchema 的内部配置。
type outputConfig struct {
	name       string         // schema 名（标识本次结构，进错误信息）
	schema     map[string]any // 规范化后的 output schema（ProfileInlineDefs 内联 $defs）
	maxRetries int            // 校验失败最多回炉几次（默认 1，对齐 Pydantic AI）
	tool       tool.Tool      // 框架那件 final_output（带封闭标记），每次 Run 注入，见 withFrameworkTools
}

// OutputOption 配置结构化输出行为。
type OutputOption func(*outputConfig)

// WithMaxValidationRetries 设校验失败最多回炉几次（默认 1）。n<0 归零（0=不回炉，首次
// 失败即抛 StructuredOutputError）。语义对齐 Pydantic AI：n 次重试 = 最多 n+1 次尝试。
func WithMaxValidationRetries(n int) OutputOption {
	return func(oc *outputConfig) {
		if n < 0 {
			n = 0
		}
		oc.maxRetries = n
	}
}

// WithOutputSchema 声明 BasicAgent 的最终答复走结构化输出（仅 BasicAgent）。
//
// 业务只声明一次 schema（Go 原生 map[string]any，零依赖）；框架注册一个 final_output
// 工具让模型调用提交 typed JSON，自动校验（required 字段 + 顶层类型），失败经既有 ReAct
// 循环回炉重生成，默认重试 1 次，耗尽抛 StructuredOutputError。成功后最终答复即结构化
// JSON——业务 `json.Unmarshal(FinalMessage(events).Content, &target)` 即可。
//
// 机制固定 ModeTool（forced output tool），跨 provider 覆盖最广（qwen/deepseek/OpenAI
// 凡支持 function calling 都行）。模型能力允许时框架设 tool_choice=required 逼模型经
// 工具收尾（不自由文本绕过）。
//
// final_output 由框架每次 Run 注入，不写进 [WithToolkit] 传进来的那个 toolkit，所以跟 WithToolkit 的
// 先后无关。曾经直接注册进业务的 toolkit：几个 agent 共用一个 toolkit 时，没配结构化输出的 agent 也拿到
// 了这件工具，模型一调就拿别的 agent 的 schema JSON 当自己的最终答复收尾，`err == nil`（实测）。业务
// toolkit 里已有同名工具时框架那件生效（构造期告警一次）。
//
// **组合约束**（P1 文档化，不强制拦）：适合顶层终态 / 纯抽取；不适合需要输出自由文本
// 传值的 sub-agent（Reflect evaluator / Sequential 中间步——强制结构化会断文本流）。与
// Handoff/Routing 同轮 return-direct 的裁决、嵌套 AgentAsTool 跨边界保对象属 P2/P3。
//
// ⚠️ 软降落轮（轮次预算最后一轮，见 [withFinalRound]）工具集**只剩框架这件 `final_output`**。
// 一度连它也摘，后果实测有三层：请求体畸形（`required` 配零工具、
// provider 返 400）、熔断信号被 `StructuredOutputError` 替换（业务再也检测不到预算耗尽）、
// 诊断正文写「已尝试 0 次」把排查方向指反。
func WithOutputSchema(name string, s map[string]any, opts ...OutputOption) Option {
	return func(c *Config) {
		oc := &outputConfig{name: name, maxRetries: 1}
		// 规范化：内联 $defs/$ref 让不解引用的 provider（qwen 等）也能用；无 $defs 时是深拷贝
		// no-op。弱档不强制全 required、不 strip 校验关键词（ModeTool 不需 strict）。
		normalized, _ := model.NormalizeJSONSchema(s, model.ProfileInlineDefs)
		oc.schema = normalized
		for _, o := range opts {
			o(oc)
		}
		oc.tool = frameworkFinalOutput{Tool: newFinalOutputTool(oc)}
		c.OutputSchema = oc
	}
}

// validateOutputSchemaConfig 在 NewBasicAgent 装配后校验结构化输出配置——construct 期
// fail-fast 而非 runtime silent 错误（项目未上线，不留兼容路径）。
func validateOutputSchemaConfig(cfg *Config) {
	if cfg.OutputSchema == nil {
		return
	}
	// 业务 toolkit 里有同名工具：框架那件生效（每次 Run 拼工具表时顶掉同名的，业务的 toolkit 不动），
	// 构造期告警一次。
	if cfg.Toolkit != nil && cfg.Toolkit.Get(FinalOutputToolName) != nil {
		slog.Warn("framework final_output overrides business tool with the same name", "agent", cfg.Name)
	}
	// 不能与控制转移（Handoff transfer / Routing route）组合：三者同为 return-direct，
	// basicReturnDirectCh 是 OnceFlag 无"谁赢"语义 → 结构化输出会被静默当作转交输入丢失
	// （A3 未实现）。construct 期 fail-fast，而非 silent 数据错误。
	//
	// ⚠️ **判据盯的是「能力从哪来」，不是「工具在不在 toolkit 里」**。转交能力改成按次
	// 授予之后，`WithHandoffTransfer()` 只翻一个 bool、不再往 `cfg.Toolkit` 塞东西——
	// 原先那条 `cfg.Toolkit.Get(HandoffTransferToolName) != nil` 于是**永远读不到**，
	// 整条守卫静默失效（实测：两件都调时转交赢、结构化输出被丢，err=nil，正是这段注释
	// 要防的那个后果）。哨兵 TestOutputSchema_MustRejectHandoffTransfer。
	// **两条来路都要判**：转交能力现在经 opt-in 声明（工具由框架每轮注入，toolkit 里
	// 看不到），而业务也可能自己往 toolkit 里注册一件同名的——后者同样会撞 return-direct。
	if cfg.HandoffTransferOptIn {
		panic(fmt.Sprintf("agent_v2: WithOutputSchema 不能与 %s 组合（结构化输出与控制转移同为 return-direct，未实现裁决——二选一）", HandoffTransferToolName))
	}
	// 路由同理：WithRouteTool 也只翻 opt-in、工具由框架按授予注入，toolkit 里同样看不到。
	if cfg.RouteToolOptIn {
		panic(fmt.Sprintf("agent_v2: WithOutputSchema 不能与 %s 组合（结构化输出与控制转移同为 return-direct，未实现裁决——二选一）", RouteTool))
	}
	for _, name := range []string{HandoffTransferToolName, RouteTool} {
		if cfg.Toolkit != nil && cfg.Toolkit.Get(name) != nil {
			panic(fmt.Sprintf("agent_v2: WithOutputSchema 不能与 %s 组合（结构化输出与控制转移同为 return-direct，未实现裁决——二选一）", name))
		}
	}
}

// finalOutputTool 是「这件工具是框架结构化输出的那件 final_output」的**封闭标记**——接口方法未导出，
// 业务实现不了。派发层据它认「这次调用是不是一次结构化输出的尝试」（见 finalOutputAttemptAnnotation），
// 不据工具名：没配 WithOutputSchema 时 final_output 这个名字业务占得到，那件就是普通业务工具。
type finalOutputTool interface{ isFinalOutputTool() }

// frameworkFinalOutput 给框架那件 final_output 打上封闭标记（WithOutputSchema 构造它时包这一层）。
type frameworkFinalOutput struct{ tool.Tool }

func (frameworkFinalOutput) isFinalOutputTool() {}

// isFrameworkFinalOutput 判断 toolkit 里这个名字下挂的是不是框架结构化输出的那件 final_output。
func isFrameworkFinalOutput(tk *tool.Toolkit, name string) bool {
	if tk == nil {
		return false
	}
	_, ok := tk.Get(name).(finalOutputTool)
	return ok
}

// newFinalOutputTool 构造 final_output 工具：参数=output schema；校验通过 → return-direct
// 短路 ReAct + 结构化 JSON 作 Output；失败 → 业务级 error tool_result（非 return-direct）
// → tools→model 边回炉重生成。
func newFinalOutputTool(oc *outputConfig) *tool.FuncTool {
	desc := fmt.Sprintf("提交你的最终答复，作为符合 schema 的结构化 JSON（结构名 %q）。"+
		"你必须调用本工具来结束——不要用自由文本回答。字段不合要求会被退回让你修正重调。", oc.name)
	return tool.NewFuncTool(
		FinalOutputToolName,
		desc,
		oc.schema,
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			if verr := validateStructured(args, oc.schema); verr != nil {
				// 校验失败 → 业务级 error tool_result（Go error 返 nil，否则 makeToolsFn 会
				// 当致命错中止 Run）；不置 return-direct → ReAct 循环回炉。
				return tool.Failure(StructuredOutputInvalidCode,
					verr.Error()+" — 修正这些字段后重新调用 final_output。"), nil
			}
			// 校验通过 → 结构化 JSON 作 Output + return-direct 短路到 postamble。
			data, err := json.Marshal(args)
			if err != nil {
				return tool.Failure(StructuredOutputInvalidCode,
					fmt.Sprintf("序列化失败: %v", err)), nil
			}
			return &tool.Result{
				Success:  true,
				Output:   string(data),
				Metadata: map[string]any{tool.ReturnDirectMetaKey: true},
			}, nil
		},
	)
}

// validateStructured 对 args 做聚焦校验（P1）：required 字段存在且非 null + 顶层字段类型
// 匹配 schema 声明。嵌套深校验 / 枚举 / 约束区间属 P2（forced-tool 下 provider 已按 tool
// params 约束结构，本校验是非 strict provider 的安全网）。
func validateStructured(args, s map[string]any) error {
	for _, r := range requiredKeys(s) {
		v, ok := args[r]
		if !ok || v == nil {
			return fmt.Errorf("缺少必填字段 %q", r)
		}
	}
	props, _ := s["properties"].(map[string]any)
	for key, val := range args {
		ps, ok := props[key].(map[string]any)
		if !ok {
			continue // 未声明的字段不检（P1 不强制 additionalProperties）
		}
		types := schemaTypes(ps)
		if len(types) == 0 {
			continue // 无 type 声明不检
		}
		matched := false
		for _, t := range types {
			if jsonTypeMatches(val, t) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("字段 %q 类型错误：期望 %v，实际 %T", key, types, val)
		}
	}
	return nil
}

// requiredKeys 提取 schema 的 required 列表（兼容 []string / []any）。
func requiredKeys(s map[string]any) []string {
	switch r := s["required"].(type) {
	case []string:
		return r
	case []any:
		out := make([]string, 0, len(r))
		for _, x := range r {
			if str, ok := x.(string); ok {
				out = append(out, str)
			}
		}
		return out
	}
	return nil
}

// schemaTypes 提取字段的 type 声明（兼容 string / []any 联合类型）。
func schemaTypes(s map[string]any) []string {
	switch t := s["type"].(type) {
	case string:
		return []string{t}
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			if str, ok := x.(string); ok {
				out = append(out, str)
			}
		}
		return out
	}
	return nil
}

// jsonTypeMatches 判定 JSON 值（json.Unmarshal 产物）是否匹配 JSON Schema 类型。数字统一
// 是 float64；integer 要求整数值。未知类型不阻拦（宽松放行）。
func jsonTypeMatches(val any, jsonType string) bool {
	switch jsonType {
	case "string":
		_, ok := val.(string)
		return ok
	case "number":
		_, ok := val.(float64)
		return ok
	case "integer":
		f, ok := val.(float64)
		return ok && f == math.Trunc(f)
	case "boolean":
		_, ok := val.(bool)
		return ok
	case "array":
		_, ok := val.([]any)
		return ok
	case "object":
		_, ok := val.(map[string]any)
		return ok
	case "null":
		return val == nil
	default:
		return true // 未知类型不阻拦
	}
}

// structuredFinalOf 判断「本轮收尾的是不是**成功的** final_output」，是则返回它作最终消息。
//
// 入参是**决策点交出来的本轮产出**（`BasicAgentPostambleInput.Final`），不是消息列表。
// 上一版倒扫含 prior 的消息列表找带 annotation 的 tool_result——上一轮成功的 final_output
// 结果同样带那个 annotation、call ID 同样在 foIDs 里，于是**本轮零个 final_output 时会捞到
// 上一轮的结果并静默返回**（实测：Run 2 模型自由文本终止，却返回了 Run 1 的
// `{"age":30,"name":"Alice"}`），而那正是该抛 StructuredOutputError 的路径。
//
// 判据两条都要：
//   - 派发层盖的 finalOutputAttemptAnnotation：收尾的确实是框架那件 final_output（按封闭标记认、
//     不按名字认，见该常量）。业务自己的 return-direct 工具收尾时不算——那时压根没有结构化产出，该抛错。
//   - `ReturnDirectAnnotation`：**校验通过才置**（校验失败走 `tool.Failure` 不置
//     return-direct、经 ReAct 循环回炉），所以它同时就是「这次是成功的」这个判据。
//
// 返回消息的 Content 是工具 `json.Marshal` 出的规范 JSON（空对象产 "{}" 而非空串），
// 不是模型给的 raw Arguments。
func structuredFinalOf(final *schema.Message) *schema.Message {
	if final == nil {
		return nil
	}
	if !isFinalOutputAttempt(final) {
		return nil
	}
	if rd, _ := final.Annotations[ReturnDirectAnnotation].(bool); !rd {
		return nil
	}
	return final
}

// foAttemptsKey 是 ctx-value key——model 节点把本次调用累计的 final_output 尝试次数
// 透进去，给 `makeModelFn` 里的预算裁决用。
//
// **为什么走 ctx 而不是让 modelFn 自己数消息**：数消息就得扫那个含 prior 的列表，
// 而那正是这次要摆脱的做法（上几轮的尝试会被算进本轮预算，模型压根不被调用就抛错）。
// 计数在 channel 里，只有节点拿得到 channels，所以由节点透。
type foAttemptsKey struct{}

// withFinalOutputAttempts push 本次调用累计的 final_output 尝试次数。
func withFinalOutputAttempts(ctx context.Context, n int) context.Context {
	return context.WithValue(ctx, foAttemptsKey{}, n)
}

// getFinalOutputAttempts 读本次调用累计的 final_output 尝试次数（未 push 时为 0）。
func getFinalOutputAttempts(ctx context.Context) int {
	n, _ := ctx.Value(foAttemptsKey{}).(int)
	return n
}

// lastStructuredError 倒扫取最后一次失败的 final_output 结果（进 StructuredOutputError 供业务定位）。
//
// 「失败」按 finalOutputRetryAnnotation 认——这次 final_output 没产出通过 schema 校验的答复（参数非法、
// schema 校验失败、被 `OnToolEnd` 否决、被钩子改成成功）都打它，正是让这一轮回炉、计入预算的那个判据；
// 被钩子改成成功的那次报钩子之前的失败（finalOutputFailureAnnotation）。曾经按 content 里有没有
// StructuredOutputInvalidCode 认，只认得 schema 失败：最后几次都是钩子否决时 LastError 为空、或指向更早那次
// schema 失败（实测）。
//
// **只认带派发层章（finalOutputAttemptAnnotation）的 tool_result**——框架那件实际跑过的，业务工具的输出不会
// 被误取，`OnToolStart` 改名两个方向也对得上。曾经按模型原始的 tool_call 名配对 call ID：别的工具被改名成
// final_output 时失败原因取不到（LastError 为空，实测）。
//
// ⚠️ 扫的是**含 prior 的完整列表**：取到上一轮的失败原因只是诊断信息略旧，别拿它定位「本轮的产出」。
func lastStructuredError(msgs []*schema.Message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		if m == nil || m.Role != schema.RoleTool || !isFinalOutputAttempt(m) {
			continue
		}
		if failed, _ := m.Annotations[finalOutputRetryAnnotation].(bool); failed {
			if reason, _ := m.Annotations[finalOutputFailureAnnotation].(string); reason != "" {
				return reason // 钩子把失败改成了成功：模型看到的不是失败原因
			}
			return m.Content
		}
	}
	return ""
}

// supportsForcedToolChoice 判定 model 是否支持 tool_choice=required（结构化输出逼模型经
// final_output 收尾）。model 实现 model.StructuredCapable 且显式 SupportsToolChoiceRequired
// =false 时不强制（如 qwen-3-coder，业务 override）；否则（含未实现 capability 的 model /
// fallback 装饰器）默认支持——ModeTool 本身 fallback-safe。
func supportsForcedToolChoice(m model.ChatModel) bool {
	if sc, ok := m.(model.StructuredCapable); ok {
		return sc.StructuredCapability("").SupportsToolChoiceRequired
	}
	return true
}

// StructuredOutputError 是结构化输出重试耗尽后抛出的 typed error（绝不静默退化）。业务
// errors.As 区分"结构化失败"与其他 Run 错误。
type StructuredOutputError struct {
	SchemaName string // 声明的 schema 名
	Attempts   int    // 已尝试次数（失败的 final_output 调用数）
	LastError  string // 最后一次校验反馈（供定位）
}

func (e *StructuredOutputError) Error() string {
	return fmt.Sprintf("结构化输出 %q 校验失败，已尝试 %d 次：%s", e.SchemaName, e.Attempts, e.LastError)
}

// NonRetryable 实现 [compose_v2.NonRetryable]——校验预算耗尽属「重放代价已知是最大值」
// 那一类：尝试次数记在随存档走的 channel 里、**跨 Resume 累积**，所以抛出来时这次调用已经
// 把 maxRetries+1 轮 LLM 全烧过了。外层编排再重试一遍等于把它乘以
// `MaxRetries+1`。
//
// ⚠️ 声明在这儿而不是 compose_v2 的清单里，正是这个接口存在的理由——compose_v2 依赖
// 方向上 import 不了 agent_v2，而「这个错重试有没有意义」在定义处最清楚。
func (e *StructuredOutputError) NonRetryable() {}
