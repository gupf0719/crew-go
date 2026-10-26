package agent_v2

import (
	"context"

	"crew-go/compose_v2"
)

// ToolNotAvailableErrCode 是「这个工具这一轮没有交给模型」的 tool error code：[WithToolFilter] 这一轮没放行它，模型
// 照样发了调它的 tool_call。工具不执行，模型收到一条失败结果（业务级软
// 失败，不 abort 整个 Run）。
//
// 模型能调的就是交给它的那些：只裁模型看得见的、执行时不拦的话，模型凭上文记得的工具名（之前的轮次里绑过、或者
// 从对话里学来的）照样调得动，裁剪就只是建议。判据只有一条：执行的就是这一轮绑定的那些。[WithToolFilter] 豁免
// 框架工具（ask_user、activate_skill、tool_search……），它们留在绑定里，照常执行。没解锁的延迟工具先被渐进披露那道闸
// 拦下（`tool_not_revealed`）。
//
// 轮次预算的最后一轮（软降落轮）不靠这道闸：crew-go 的软降落轮没配结构化输出时工具照旧绑着、发 tool_choice=none，
// 模型硬调的工具（框架工具也算，只放行结构化输出的 final_output）由前面那道闸以 `round_budget_exhausted` 拦下，见
// withFinalRound。
const ToolNotAvailableErrCode = "tool_not_available"

// toolBinding 是 model 节点这一轮实际交给模型的工具：Restricted 为真时，这一批 tool_call 里只执行 Names 里的工具。
//
// 只在有裁剪时为真：配了 [WithToolFilter] 且它这一轮返回了名单，或者是软降落轮（名单是这一轮实际绑着的：没配结构化输出时是整套工具，配了时只有 final_output——两种情形下硬调都先被
// 软降落那道闸拦下，见 ToolNotAvailableErrCode）。
// 没有裁剪时为假——不受这道闸的情形（没配 ToolFilter、自定义 modelFn 不填它）照旧，执行什么由派发表决定。
//
// 它随 model 节点的写入进 basicToolBindingCh、也记在模型调用记录里：续跑时 tools 节点从存档恢复、直接进节点，
// model 节点这一轮没再跑，判的仍是当初交给模型的那份——在 tools 节点里重算 ToolFilter 的话，回调看的是此刻的
// 环境，跟模型当时看到的未必一样。
type toolBinding struct {
	Restricted bool     `json:"restricted,omitempty"`
	Names      []string `json:"names,omitempty"`
}

func init() {
	// 随存档落盘 ⟹ 必须注册（同 toolCallMemo）
	compose_v2.RegisterValueType[*toolBinding]("*agent_v2.toolBinding")
}

// allows 报这一轮能不能执行这个工具名；nil 或没有裁剪时一律能。
func (b *toolBinding) allows(name string) bool {
	if b == nil || !b.Restricted {
		return true
	}
	for _, n := range b.Names {
		if n == name {
			return true
		}
	}
	return false
}

// basicToolBindingCh 是这一轮交给模型的工具（见 toolBinding）。model 节点每一轮都写（没有裁剪时写 nil），
// 所以 tools 节点读到的总是产出这一批 tool_call 的那一轮的绑定。
var basicToolBindingCh = compose_v2.LastValue[*toolBinding]("__basic_agent_tool_binding__")

// toolBindingSinkKey：model 节点交给 modelFn 一个空的 toolBinding，modelFn 绑工具时填进去（[BasicAgentModelFn] 的
// 签名拿不到 channel，同 revealedChKey 的理由）。
type toolBindingSinkKey struct{}

func withToolBindingSink(ctx context.Context, b *toolBinding) context.Context {
	return context.WithValue(ctx, toolBindingSinkKey{}, b)
}

func toolBindingSink(ctx context.Context) *toolBinding {
	b, _ := ctx.Value(toolBindingSinkKey{}).(*toolBinding)
	return b
}

// toolBindingKey：tools 节点把 basicToolBindingCh 读出来放进 ctx，toolsFn 的执行闸据它判。
type toolBindingKey struct{}

func withToolBinding(ctx context.Context, b *toolBinding) context.Context {
	return context.WithValue(ctx, toolBindingKey{}, b)
}

func toolBindingFrom(ctx context.Context) *toolBinding {
	b, _ := ctx.Value(toolBindingKey{}).(*toolBinding)
	return b
}
