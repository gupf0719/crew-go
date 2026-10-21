package agent_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// ParallelAgent 的分支拿不到转交 / 路由能力（见 ParallelAgent 的 godoc）：并行的几个分支各自表态，分歧时
// 谁也没有资格裁决。下面每条都配一个串行的对照——同一个成员放进 Sequential，能力照常转授、决定照常
// 生效——确认测到的是「并行不转授」，不是成员本身做不了决定。

// capCallModel 记下每次 WithTools 绑定的工具名；第一轮吐 tc，看到工具结果后作答（并记下那条工具结果）。
type capCallModel struct {
	mu    sync.Mutex
	tc    schema.ToolCall
	bound [][]string
	last  *schema.Message
}

func (m *capCallModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if n := len(msgs); n > 0 && msgs[n-1].Role == schema.RoleTool {
		m.mu.Lock()
		m.last = msgs[n-1]
		m.mu.Unlock()
		return schema.AssistantMessage("分支自己答"), nil
	}
	return schema.AssistantToolCallMessage([]schema.ToolCall{m.tc}), nil
}
func (m *capCallModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *capCallModel) WithTools(infos []*schema.ToolInfo) (model.ChatModel, error) {
	names := make([]string, 0, len(infos))
	for _, ti := range infos {
		names = append(names, ti.Name)
	}
	m.mu.Lock()
	m.bound = append(m.bound, names)
	m.mu.Unlock()
	return m, nil
}

// sawTool 报模型有没有被绑定过名叫 name 的工具。
func (m *capCallModel) sawTool(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, names := range m.bound {
		for _, n := range names {
			if n == name {
				return true
			}
		}
	}
	return false
}

// lastToolResult 返回模型看到的最后一条工具结果的内容（没看到过时为空串）。
func (m *capCallModel) lastToolResult() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.last == nil {
		return ""
	}
	return m.last.Content
}

// wrapOne 把一个成员包进 Parallel（唯一的分支）或 Sequential（唯一的一步）。
func wrapOne(kind string, member agent_v2.Agent) agent_v2.Agent {
	if kind == "parallel" {
		return agent_v2.NewParallelAgent(agent_v2.WithParallelName("front")).AddBranch("m", member)
	}
	return agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("front")).AddStep("m", member)
}

// TestParallel_BranchGetsNoTransferTool：HandoffAgent 的候选是 Parallel，分支是配了 WithHandoffTransfer 的
// BasicAgent。分支的模型看不到 transfer_to_agent；照着习惯硬发一个，拿到 HandoffNoCoordinatorErrCode 软
// 失败、接着作答，谁也不接手。
func TestParallel_BranchGetsNoTransferTool(t *testing.T) {
	for _, kind := range []string{"parallel", "sequential"} {
		t.Run(kind, func(t *testing.T) {
			m := &capCallModel{tc: schema.ToolCall{ID: "t1", Type: "function", Function: schema.FunctionCall{
				Name: agent_v2.HandoffTransferToolName, Arguments: `{"agent_name":"billing"}`}}}
			member := agent_v2.NewBasicAgent(agent_v2.WithName("m_llm"), agent_v2.WithModel(m),
				agent_v2.WithHandoffTransfer(), agent_v2.WithMaxRounds(3))
			h, billing := effDesk(wrapOne(kind, member))
			got := effDrive(t, h, "我要退款")
			if kind == "sequential" {
				if got != "BILLING 已处理" || billing.runs.Load() != 1 || !m.sawTool(agent_v2.HandoffTransferToolName) {
					t.Fatalf("对照：串行成员拿得到转交能力，转交应生效，实际最终 %q（billing %d 次）", got, billing.runs.Load())
				}
				return
			}
			if n := billing.runs.Load(); n != 0 {
				t.Errorf("并行分支不该转得走对话，billing 执行了 %d 次（最终 %q）", n, got)
			}
			if m.sawTool(agent_v2.HandoffTransferToolName) {
				t.Errorf("并行分支的模型不该看到 %s", agent_v2.HandoffTransferToolName)
			}
			if r := m.lastToolResult(); !strings.Contains(r, agent_v2.HandoffNoCoordinatorErrCode) {
				t.Errorf("硬发的转交应拿到 %s 软失败，实际 %q", agent_v2.HandoffNoCoordinatorErrCode, r)
			}
		})
	}
}

// TestParallel_BranchProgrammaticHandoffRejected：分支在代码里调 HandoffTo，拿到「没有被授予」的拒绝
// （`%w ErrRunAborted`，见 HandoffTo 的 godoc：拒绝的都是重跑必然一样的情形），谁也不接手。
func TestParallel_BranchProgrammaticHandoffRejected(t *testing.T) {
	for _, kind := range []string{"parallel", "sequential"} {
		t.Run(kind, func(t *testing.T) {
			var runs atomic.Int32
			h, billing := effDesk(wrapOne(kind, &countingTransfer{name: "m", target: "billing", runs: &runs}))
			ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
			evs, err := h.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("我要退款"))
			if kind == "sequential" {
				if err != nil || billing.runs.Load() != 1 {
					t.Fatalf("对照：串行成员的程序化转交应生效，实际 err=%v（billing %d 次）", err, billing.runs.Load())
				}
				return
			}
			if !errors.Is(err, compose_v2.ErrRunAborted) || !strings.Contains(err.Error(), "没有被授予转交能力") {
				t.Fatalf("并行分支调 HandoffTo 应被拒绝（没有被授予、带 ErrRunAborted），实际 err=%v final=%v",
					err, agent_v2.FinalMessage(evs))
			}
			if n := billing.runs.Load(); n != 0 {
				t.Errorf("并行分支不该转得走对话，billing 执行了 %d 次", n)
			}
		})
	}
}

// TestParallel_BranchGetsNoRouteTool：RoutingAgent 的分类器是 Parallel，分支是配了 WithRouteTool 的
// BasicAgent。分支的模型看不到 route_to；硬发一个拿到 RouteNoCoordinatorErrCode 软失败，路由落到兜底。
func TestParallel_BranchGetsNoRouteTool(t *testing.T) {
	for _, kind := range []string{"parallel", "sequential"} {
		t.Run(kind, func(t *testing.T) {
			m := &capCallModel{tc: schema.ToolCall{ID: "r1", Type: "function", Function: schema.FunctionCall{
				Name: agent_v2.RouteTool, Arguments: `{"target":"math"}`}}}
			member := agent_v2.NewBasicAgent(agent_v2.WithName("m_llm"), agent_v2.WithModel(m),
				agent_v2.WithRouteTool(), agent_v2.WithMaxRounds(3))
			rt, math, fb := effRouting(wrapOne(kind, member))
			got := effDrive(t, rt, "1+1")
			if kind == "sequential" {
				if got != "MATH 已处理" || math.runs.Load() != 1 || !m.sawTool(agent_v2.RouteTool) {
					t.Fatalf("对照：串行成员拿得到路由能力，路由应生效，实际最终 %q（math %d 次）", got, math.runs.Load())
				}
				return
			}
			if math.runs.Load() != 0 || fb.runs.Load() != 1 {
				t.Errorf("并行分支不该定得了路由：math %d 次、兜底 %d 次（最终 %q）", math.runs.Load(), fb.runs.Load(), got)
			}
			if m.sawTool(agent_v2.RouteTool) {
				t.Errorf("并行分支的模型不该看到 %s", agent_v2.RouteTool)
			}
			if r := m.lastToolResult(); !strings.Contains(r, agent_v2.RouteNoCoordinatorErrCode) {
				t.Errorf("硬发的 route_to 应拿到 %s 软失败，实际 %q", agent_v2.RouteNoCoordinatorErrCode, r)
			}
		})
	}
}
