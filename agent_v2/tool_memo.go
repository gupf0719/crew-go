package agent_v2

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// toolCallMemo 只用于读取旧存档。首次恢复到工具批次时，将它转换为逐调用账项。
type toolCallMemo struct {
	Done     map[string]*toolMemoEntry   `json:"done,omitempty"`
	Approved map[string]bool             `json:"approved,omitempty"`
	Decided  map[string]ApprovalDecision `json:"decided,omitempty"`
}

// toolCallRecord 属于一次节点执行中的一个工具调用，审批与产出随这一格进入存档。
type toolCallRecord struct {
	Result   *toolMemoEntry    `json:"result,omitempty"`
	Approved bool              `json:"approved,omitempty"`
	Decision *ApprovalDecision `json:"decision,omitempty"`
}

// toolMemoEntry 一个已完成 tool_call 的产出。
type toolMemoEntry struct {
	// ToolMsg 是 tool_result 消息（`role=tool`，带 CallId 配对锚点）。
	ToolMsg *schema.Message `json:"tool_msg,omitempty"`
	// BodyMsg 是 P2.D inline skill body 那条附加 UserMessage（多数工具为 nil）。
	BodyMsg *schema.Message `json:"body_msg,omitempty"`
	// Signals 是这次调用做出的上行信号（转交 / 路由决定，多数工具为空）——工具自己做出的
	// （transfer_to_agent / route_to），以及它里面经 RunSubAgent 等嵌套调用交上来的。
	//
	// 命中记账、函数体被跳过时由框架原样交出——否则 tool_result 齐全而决定丢了，且静默
	// （业务工具里经 RunSubAgent 定的路由，在同批兄弟工具中断续跑之后落到兜底，实测）。
	// 记的是框架在这次调用期间**收到的**，不靠工具自己声明：转交 / 路由只能经框架那几个入口
	// 做出（见 side_signal.go）——手写一份信号 JSON 冒充不了，因为收件人由引擎铸、算不出来。
	// 结果是失败的（超时、tool.Failure）也照记——作废在 OnToolEnd 之后按钩子改写后的结果判，命中的是原样
	// （这一批没处理完就停下了）时钩子照样 fire、按同一套规则重判（见 tool_batch_signals.go）；定下了的
	// （Final）不再交给钩子，按定下时的结论判。
	Signals compose_v2.Signals `json:"signals,omitzero"`
	// Final 报 ToolMsg 是这一批处理完（OnToolEnd、批内裁决、按大小处理）之后定下的那份，不是工具刚返回的原样——
	// 见 [finalizeToolMemo]。
	Final bool `json:"final,omitempty"`
	// HookSignals 是定下时这次调用的 OnToolEnd 钩子做出的上行信号（只在 Final 时有意义）：定下了的续跑时不再交给
	// 钩子，这里记的原样交出。
	HookSignals compose_v2.Signals `json:"hook_signals,omitzero"`
}

// shouldMemoize 判断这次 tool_call 的产出能不能进重放记账。
//
// **判据是「这次调用的副作用能不能重放」，不是「工具叫什么名字」。**
//
//   - 做出了上行信号的（转交 / 路由决定）：那半随记账进 cp，命中时原样交出 ⟹ 可以进
//     记账，函数体不必重跑。
//
//   - 没做出上行信号的框架元工具：产出不止 tool_result 而框架无从重放 ⟹ 排除在记账外，靠
//     函数体重跑补回来。命中跳过的后果是**静默**的：err=nil、tool_result 齐全，
//     只是事没发生。
//
//     ⚠️ `tool_search` **仍必须排除，但理由已经变了**：它的已解锁集现在进 channel
//     state（随 cp 走），不再是当初那条「副作用不进 cp」。现在的理由是**写 channel
//     这一步发生在工具体里**——命中记账跳过函数体，就没人往 channel 写。写错理由
//     下一个人很可能顺手把它从名单里"优化"掉，那会让同批中断场景静默退化。
//     判据动之前先看 tool_search_resume_test.go 那四条哨兵。
//
//   - 业务工具：默认进记账；它里面经嵌套调用交上来的决定同样随记账走。
//
// minted 是「这次派发的那件是不是框架铸的」（[isFrameworkMinted]），不是按名字判——业务工具恰好叫
// plan / respond / final_output 时按名字判会被排除在记账外，兄弟工具中断续跑后它再执行一次（实测）。
func shouldMemoize(minted bool, signals compose_v2.Signals) bool {
	return signals.Len() > 0 || !minted
}

// frameworkControlTool 是「这件工具是框架自己铸的」的**封闭标记**——接口方法未导出，
// 业务实现不了。return-direct 收尾时据它区分「这是框架那件 transfer_to_agent / route_to」与业务
// 自己注册的同名工具（见 [ReturnDirectMintedAnnotation]）。`final_output` 不盖这个章——它不是控制工具，
// 它的产出就是答复——它有自己的封闭标记（frameworkFinalOutput，见 finalOutputAttemptAnnotation）。
//
// # 为什么不能用工具名判
//
// **那些名字业务占得到**：没被授予转交时，框架刻意不挂软失败桩（`basic_agent.go` 那段注释写明
// 理由——否则任何一个恰好叫 `transfer_to_agent` 的业务工具会被框架静默顶掉），于是业务那件
// 同名工具留在 toolkit 里；`route_to` 同理（没被授予且业务占了这个名字时不挂桩）。
type frameworkControlTool interface{ isFrameworkControlTool() }

// frameworkTool 把框架自己铸的工具打上封闭标记。
type frameworkTool struct{ tool.Tool }

func (frameworkTool) isFrameworkControlTool() {}

// markFrameworkTool 给框架铸的控制流工具盖章。**新增框架控制流工具时必须包一层**，
// 否则 return-direct 收尾时它被当成业务工具。
func markFrameworkTool(t tool.Tool) tool.Tool { return frameworkTool{Tool: t} }

// isFrameworkControlToolIn 判断 toolkit 里这个名字下挂的是不是框架那件控制工具（transfer_to_agent /
// route_to）。「是不是框架铸的」这个更宽的问题用 [isFrameworkMintedIn]。
func isFrameworkControlToolIn(tk *tool.Toolkit, name string) bool {
	if tk == nil {
		return false
	}
	_, ok := tk.Get(name).(frameworkControlTool)
	return ok
}

func init() {
	compose_v2.RegisterValueType[*toolCallMemo]("*agent_v2.toolCallMemo")
	compose_v2.RegisterValueType[*toolCallRecord]("*agent_v2.toolCallRecord")
}

// toolMemoContext 按调用 ID、批内下标和实际派发的工具名分账。下标区分同批重复或空 ID，
// 工具名来自首次派发定下的内容。分域挂在节点执行下，重试同一批时保留，跨轮次和任务隔离。
func toolMemoContext(ctx context.Context, tc schema.ToolCall, idx int) context.Context {
	return compose_v2.WithNodeStateSealed(ctx, "tool_memo\x00"+identityString(tc.ID, strconv.Itoa(idx), tc.Function.Name))
}

// loadToolMemo 在派发之前读一次账。旧批次先复制各账项，全部写入后再覆盖旧值。
// 若转换中断，已存在的新账项优先，下次恢复仍可从旧值补齐。
func loadToolMemo(ctx context.Context, calls []schema.ToolCall) ([]*toolCallRecord, error) {
	records := make([]*toolCallRecord, len(calls))
	legacy, _ := compose_v2.GetNodeState[*toolCallMemo](ctx)
	for i, tc := range calls {
		callCtx := toolMemoContext(ctx, tc, i)
		record, _ := compose_v2.GetNodeState[*toolCallRecord](callCtx)
		if record == nil && legacy != nil {
			key := tc.ID + "#" + strconv.Itoa(i) + "#" + tc.Function.Name
			result, approved := legacy.Done[key], legacy.Approved[key]
			decision, decided := legacy.Decided[key]
			if result != nil || approved || decided {
				record = &toolCallRecord{Result: result, Approved: approved}
				if decided {
					record.Decision = &decision
				}
				if err := compose_v2.SaveNodeState(callCtx, record); err != nil {
					return nil, fmt.Errorf("migrate tool call %q at index %d: %w", tc.ID, i, err)
				}
			}
		}
		records[i] = record
	}
	if legacy != nil {
		// nil 清空旧值，保存时不会再次编码整份工具账。
		if err := compose_v2.SaveNodeState(ctx, nil); err != nil {
			return nil, err
		}
	}
	return records, nil
}

// finalizeToolMemo 逐项保存钩子改写、批内裁决和转存后的结果。此后节点提交若失败，恢复时直接
// 重放定稿及 OnToolEnd 产生的信号。批次提前中断时，账里仍是工具刚返回的内容，供下一次钩子处理。
func finalizeToolMemo(ctx context.Context, calls []schema.ToolCall, records []*toolCallRecord, results []*schema.Message, hookSignals []compose_v2.Signals) {
	for i, record := range records {
		if record == nil || record.Result == nil || record.Result.Final || i >= len(results) || results[i] == nil {
			continue
		}
		final := *record.Result
		final.ToolMsg, final.Final, final.HookSignals = results[i], true, hookSignals[i]
		next := *record
		next.Result = &final
		if err := compose_v2.SaveNodeState(toolMemoContext(ctx, calls[i], i), &next); err != nil {
			slog.WarnContext(ctx, "agent_v2: 定下工具账失败，该调用续跑时会重新处理",
				"tool_call_id", calls[i].ID, "err", err)
		}
	}
}
