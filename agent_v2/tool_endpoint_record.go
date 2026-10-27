package agent_v2

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"

	"crew-go/compose_v2"
	"crew-go/tool"
)

// 中间件链最内层的工具端点记下每一次执行的结果：中间件在 next 之后挂起（结果确认类中间件在 next 之后 Ask）时，续跑
// 重跑这个 tool_call，next 拿回记下的那一份、工具不再执行。
//
// 不变量：一个 tool_call 里工具成功返回至多一次，跟这个 tool_call 被续跑几次无关。工具账（toolCallMemo）只在整个
// tool_call 完成时记，覆盖不到「工具已经执行、中间件还没收尾就挂起」这一段——那里曾经每次续跑都把工具再执行一遍
// （实测扣款两次）。中间件在一次执行里只能调一次 next（第二次被 guardControlFlow 拒），所以一个 tool_call 一条记录。
//
// 拿回的 Output：类型用 compose_v2.RegisterValueType 注册过的按原类型拿回；字符串原样；别的按交给模型的那段文本
// （`%v`）记、拿回时是那个字符串——停没停过，模型看到的都是同一段文本（曾按 JSON 形状存，int64 的订单号续跑后变成
// 1.8345678901234568e+18、结构体变成 map[...]，实测）。Metadata 按 JSON 形状往返（框架只读里面的 bool 标记）；编不成
// JSON 时不记（告警，续跑会再执行一次工具）。工具返回 Go error 的那次不记（它没有产出，续跑照常再试）。
//
// 超过框架给这次调用设的期限（WithToolTimeout）之后才返回的成功也记下（工具已经执行了），连同「已超时」的标记：拿回时
// 照样按超时收尾，结局跟没停过时一样（不带标记的话续跑时期限没到，迟到的成功被当成成功交给模型，实测）。记录在这个 tool_call 自己的私有态里（按工具名再分一层），tool_call 完成之后由工具账接管。

// endpointRecordStep 是端点记录在 tool_call 私有态里的键。
const endpointRecordStep = "agent_v2.tool_endpoint"

// endpointResult 是 tool.Result 的可入档形状：Metadata 在 tool.Result 上不进 JSON（json:"-"），而 return-direct 等运行时
// 标记就在里面，要一起记下。
type endpointResult struct {
	Success bool `json:"success"`
	// Output 的类型注册过时存在 OutputTagged（带类型名，拿回原类型），否则存在 Output（字符串，见文件头）。nil 表示
	// 没有产出，跟空串区分开：空串渲染出来是空，nil 渲染出来是 "<nil>"
	Output       *string         `json:"output,omitempty"`
	OutputTagged json.RawMessage `json:"output_tagged,omitempty"`
	Error        *tool.Error     `json:"error,omitempty"`
	Metadata     map[string]any  `json:"metadata,omitempty"`
}

// endpointRecord 是一次工具执行的记录：结果，连同执行时做出的上行信号（转交 / 路由决定，拿回时交出，同工具账）。
type endpointRecord struct {
	Result  *endpointResult    `json:"result"`
	Signals compose_v2.Signals `json:"signals,omitzero"`
	// TimedOut：工具返回时框架给这次调用设的期限已经到了（迟到的成功）
	TimedOut bool `json:"timed_out,omitempty"`
}

func init() {
	// 随存档落盘 ⟹ 必须注册（同 toolCallMemo）
	compose_v2.RegisterValueType[*endpointRecord]("*agent_v2.endpointRecord")
}

// recordedEndpoint 包住工具端点：先找这个 tool_call 的记录，有就交回记下的结果，没有就执行、成功返回后记下。
// ctx 必须是这次调用的位置（采纳过中间件交来的 ctx 之后的那个）：记录按它的 tool_call 分域找。
//
// timedOut 在工具返回时问「框架给这次调用设的期限到了没有」，记进记录；拿回的记录带着这个标记时调 onRecalledTimeout，
// 由调用方按超时收尾。
func recordedEndpoint(next ToolEndpoint, timedOut func() bool, onRecalledTimeout func()) ToolEndpoint {
	return func(ctx context.Context, name string, args map[string]any) (*tool.Result, error) {
		rec, ok := compose_v2.GetStepResult[*endpointRecord](ctx, endpointRecordStep)
		if !ok || rec == nil || rec.Result == nil {
			return runAndRecord(ctx, next, timedOut, name, args)
		}
		var out any
		if rec.Result.Output != nil {
			out = *rec.Result.Output
		}
		if len(rec.Result.OutputTagged) > 0 {
			v, err := compose_v2.UnmarshalTagged(rec.Result.OutputTagged)
			if err != nil {
				// 类型在存档之后被取消注册：照常执行，比交回一个形状不对的结果安全
				slog.WarnContext(ctx, "agent_v2: 工具记下的结果读不回来，这次续跑会再执行一次工具",
					"tool", name, "err", err)
				return runAndRecord(ctx, next, timedOut, name, args)
			}
			out = v
		}
		if rec.TimedOut && onRecalledTimeout != nil {
			onRecalledTimeout()
		}
		compose_v2.EmitSignals(ctx, rec.Signals)
		return &tool.Result{Success: rec.Result.Success, Output: out, Error: rec.Result.Error,
			Metadata: rec.Result.Metadata}, nil
	}
}

// runAndRecord 执行工具，成功返回后记下结果与它做出的上行信号，再把信号交出去。
func runAndRecord(ctx context.Context, next ToolEndpoint, timedOut func() bool, name string, args map[string]any) (*tool.Result, error) {
	var res *tool.Result
	sigs, err := compose_v2.CaptureSignals(ctx, func(c context.Context) error {
		var e error
		res, e = next(c, name, args)
		return e
	})
	if err != nil || res == nil {
		return res, err
	}
	er := &endpointResult{Success: res.Success, Error: res.Error, Metadata: res.Metadata}
	switch out := res.Output.(type) {
	case nil:
	case string:
		er.Output = &out
	default:
		text := fmt.Sprintf("%v", out) // 同 renderToolContent 交给模型的那段；String() panic 由 fmt 兜住、渲染成 %!v(PANIC=…)
		er.Output = &text
		if _, registered := compose_v2.LookupKey(reflect.TypeOf(out)); registered {
			if b, merr := compose_v2.MarshalTagged(out); merr == nil {
				er.Output, er.OutputTagged = nil, b
			}
		}
	}
	rec := &endpointRecord{Result: er, Signals: sigs, TimedOut: timedOut != nil && timedOut()}
	if serr := compose_v2.SaveStepResult(ctx, endpointRecordStep, rec); serr != nil {
		slog.WarnContext(ctx, "agent_v2: 记下工具这次执行的结果失败，中间件在 next 之后挂起时续跑会再执行一次工具",
			"tool", name, "err", serr)
	}
	compose_v2.EmitSignals(ctx, sigs)
	return res, nil
}
