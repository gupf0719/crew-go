package agent_v2

import (
	"context"
	"fmt"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// planExecStep 是 PlanAgent 内部的"已解析可序列化执行态"——plannerFn 把高层
// PlanStep（含 SubAgentName / TmpSubAgent / InputRefs []RefSpec）解析成本结构
// 后写入 planCurrentCh，worker 节点读出执行。
//
// 与高层 PlanStep 区别：高层是"业务描述态"（Planner 返回），本结构是"图执行态"
// （Target 已选定具体 worker NodeID、InputRefs 已 resolve 成 0-based 下标）。
// 之前是 compose_v2.PlanStep（低层公开类型），消除低层分层后内化为 agent_v2
// unexported 类型——只有 plannerFn ↔ worker 节点经 planCurrentCh 传递，业务不接触。
type planExecStep struct {
	// Description 步骤描述（透传给 sub-agent 参考 + 写 ExecutedStep.Step）
	Description string
	// Target 本步路由到的 worker NodeID（subAgent name / tmpDispatcher / executor dispatcher）
	Target compose_v2.NodeID
	// InputRefs 已 resolve 的 0-based executed 下标列表（高层 resolveRefs 的产出）
	InputRefs []int
	// TmpSubAgent 是 Target 为临时子 agent（tmpDispatcherWorker）时那一步的配置。跟着这一步进 channel、随存档
	// 走：续跑（换进程也一样）从存档读回；任务结束，存档清掉它就没了，被放弃的任务也不在 PlanAgent 实例上
	// 留东西。曾经放在实例上的一张表里，换进程续跑时找不到（no config for description），被放弃的任务的
	// 配置一直留在表里。
	TmpSubAgent *TmpSubAgentConfig
}

// 内部 channel——PlanAgent 编排专用，业务不直接接触。
//
// planExecutedCh 存高层 ExecutedStep（字段与原低层 ExecutedStep 全同，消除低层后
// 只保留高层一种）；planCurrentCh 存 *planExecStep（已解析执行态）。
var (
	planOriginalCh = compose_v2.LastValue[string]("__plan_original__")
	planExecutedCh = compose_v2.Append[ExecutedStep]("__plan_executed__")
	planCurrentCh  = compose_v2.LastValue[*planExecStep]("__plan_current__")
	planAnswerCh   = compose_v2.LastValue[string]("__plan_final_answer__")
	planDoneCh     = compose_v2.OnceFlag("__plan_done__")
	// planBuiltInputCh 存 build 节点拼好的 worker 输入（*schema.Message 已在
	// basic_agent init 注册序列化）。**拼一次、提交进 channel、worker 只读**——
	// worker 中断重跑时从 cp 恢复同一条消息（身份稳定），不重拼。
	planBuiltInputCh = compose_v2.LastValue[*schema.Message]("__plan_built_input__")
)

func init() {
	// 注册跨 cp 边界持久化的 channel value 类型（HITL Resume 时 marshal/unmarshal）。
	// planExecStep 是 unexported——由本包 init 注册（之前 *compose_v2.PlanStep /
	// []compose_v2.ExecutedStep 在 basic_agent.go init 注册，消除低层后迁来此处）。
	compose_v2.RegisterValueType[*planExecStep]("*agent_v2.planExecStep")
	compose_v2.RegisterValueType[[]ExecutedStep]("[]agent_v2.ExecutedStep")
}

// compilePlanGraph 把 plan 节点 + build 节点 + worker 节点集编译成 plan↔exec 循环图。
//
// 内部拓扑：
//
//	StartNode → plan → conditional( EndNode 或 plan_build_input )
//	plan_build_input → conditional( worker_X )
//	worker_X → plan (回循环)
//
// plan 的 conditional edge：
//   - planDoneCh true → EndNode
//   - 否则校验 planCurrentCh.Target 已注册，路由到 plan_build_input
//
// **拼输入独立成节点的原因**：worker 输入是派生值（原始目标 + 已执行步骤 + 本步描述
// 的纯函数），若在 worker 节点内现拼，worker 中断重跑 = 重拼 = 重铸消息身份 ⟹ 历史层
// 按身份认的去重守卫恒不命中，任务描述随 HITL 轮次线性重复（审计实测 1→2→3）。独立
// 节点拼一次、写 planBuiltInputCh 提交，worker 只读——重跑从 cp 恢复同一条，身份天然
// 稳定。同款形状见 ReflectAgent 的 feedback 节点（拼派生输入 → 写 reflectActorInputCh
// → actor 只读，审计实测免疫）。**顺带的行为改进**：ExecInputBuilder 抛 InterruptError
// 做输入审批时，Resume 只重跑 build 节点（planner 的写已提交不重跑）——把拼输入挪进
// plan 节点的变体会让审批中断重跑 planner（LLM 非确定、步骤都可能换），故不取。
//
// 之前是 compose_v2.PlanBuilder.Compile（低层），消除低层分层后内化进 agent_v2，
// 直接用通用 compose_v2.GraphBuilder 公开 API（NewBuilder / WithChannel / AddNode /
// AddEdge / AddConditionalEdge / Compile）。
func compilePlanGraph(planNode, buildInputNode compose_v2.NodeFunc, workers map[compose_v2.NodeID]compose_v2.NodeFunc, maxSteps int, name string) (*compose_v2.Graph, error) {
	if planNode == nil {
		return nil, fmt.Errorf("plan: plan node required")
	}
	if buildInputNode == nil {
		return nil, fmt.Errorf("plan: build input node required")
	}
	if len(workers) == 0 {
		return nil, fmt.Errorf("plan: no workers registered")
	}
	if maxSteps <= 0 {
		maxSteps = 20
	}

	// worker 名不在这里查：子 agent 名在注册时已由 checkPlanSubAgentName 校验（它还拦两个
	// 内部 worker 名），worker 表里其余的键就是那两个内部 worker 本身。

	b := compose_v2.NewBuilder().WithMaxVisit(maxSteps)
	// **ISSUE-85 MED-2 fix**: 显式 GraphID name 让 fingerprint 不变（sub-agents 集合
	// 变更不再引发 ErrGraphIDConflict）。
	if name != "" {
		b = b.WithName(name)
	}
	// 注册内部 channels（公开 WithChannel API，不碰 unexported b.channels）。
	compose_v2.WithChannel(b, planOriginalCh)
	compose_v2.WithChannel(b, planExecutedCh)
	compose_v2.WithChannel(b, planCurrentCh)
	compose_v2.WithChannel(b, planAnswerCh)
	compose_v2.WithChannel(b, planDoneCh)
	compose_v2.WithChannel(b, planBuiltInputCh)

	// plan 节点 + build 节点
	b.AddNode("plan", planNode)
	b.AddNode(planBuildInputNode, buildInputNode)
	b.AddEdge(compose_v2.StartNode, "plan")

	// 注册所有 worker，每个回 plan
	allowedWorkers := make(map[compose_v2.NodeID]bool, len(workers))
	for id, fn := range workers {
		b.AddNode(id, fn)
		b.AddEdge(id, "plan")
		allowedWorkers[id] = true
	}

	// plan 的 conditional edge：done → End；否则**先校验 target 再进 build 节点**。
	// 校验留在这里不挪去 build——fail-fast 时机不变（拼输入之前），错误文案不变。
	b.AddConditionalEdge("plan", func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		if done, _ := compose_v2.Get(chs, planDoneCh); done {
			return compose_v2.EndNode, nil
		}
		current, ok := compose_v2.Get(chs, planCurrentCh)
		if !ok || current == nil {
			return "", fmt.Errorf("plan: planner returned non-empty remaining but currentStep nil")
		}
		if !allowedWorkers[current.Target] {
			return "", fmt.Errorf("plan: target %q not registered as worker", current.Target)
		}
		return planBuildInputNode, nil
	})

	// build 的 conditional edge：路由到已校验的 target（二次校验只防 channel 被改）
	b.AddConditionalEdge(planBuildInputNode, func(_ context.Context, chs compose_v2.Channels) (compose_v2.NodeID, error) {
		current, ok := compose_v2.Get(chs, planCurrentCh)
		if !ok || current == nil {
			return "", fmt.Errorf("plan: build routed but currentStep nil")
		}
		if !allowedWorkers[current.Target] {
			return "", fmt.Errorf("plan: target %q not registered as worker", current.Target)
		}
		return current.Target, nil
	})

	return b.Compile()
}

// planInputWrites 是 plan 编排启动时的辅助 helper——把 original input 写到内部 channel。
func planInputWrites(original string) []compose_v2.ChannelWrite {
	return []compose_v2.ChannelWrite{compose_v2.Write(planOriginalCh, original)}
}

// planFinalAnswer 取 plan 编排完成后的最终答复。
//
// 优先级：
//  1. Planner 给的 finalAnswer（非空）
//  2. 已执行步骤拼接（fallback）
func planFinalAnswer(chs compose_v2.Channels) string {
	if ans, _ := compose_v2.Get(chs, planAnswerCh); ans != "" {
		return ans
	}
	executed, _ := compose_v2.Get(chs, planExecutedCh)
	if len(executed) == 0 {
		return ""
	}
	out := "计划已完成。\n\n"
	for i, e := range executed {
		if i > 0 {
			out += "\n\n"
		}
		out += fmt.Sprintf("步骤 %d (%s): %s", i+1, e.Agent, e.Result)
	}
	return out
}
