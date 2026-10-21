package agent_v2

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"crew-go/compose_v2"
	"crew-go/skill"
	"crew-go/tool"
)

// RunSkillActionToolName 是 run_skill_action 框架工具名——LLM 调它触发 skill 暴露的 action
// （确定性执行：跑进程内 Go 函数，区别于 activate_skill 注入上下文）。
const RunSkillActionToolName = "run_skill_action"

// skillActionTool 是 run_skill_action 工具：LLM 传 (skill, action, args) → 框架调
// SkillBox.RunSkillAction → 结果回灌 LLM。框架只在有 ≥1 个带 action 的 skill 时挂出（boxHasActions）。
//
// 与 activate_skill 不冲突：两工具职责正交——activate_skill 管 body/上下文（inline/fork），
// run_skill_action 管 declared go-func action（确定性执行）。一个 skill 可既能激活又有 action。
type skillActionTool struct {
	box skill.SkillBox
}

func newSkillActionTool(box skill.SkillBox) *skillActionTool {
	return &skillActionTool{box: box}
}

func (t *skillActionTool) Name() string { return RunSkillActionToolName }

// Description 列出可运行的 action（skill/action + 描述 + 参数 schema）。禁止 LLM 调
// （DisableModelInvocation）的 skill 的 action 不列出。
func (t *skillActionTool) Description() string {
	type entry struct {
		skill, action, desc string
		schema              map[string]any
	}
	var entries []entry
	for _, s := range t.box.ListSkills() {
		if s.DisableModelInvocation {
			continue
		}
		for _, a := range t.box.ListSkillActions(s.Name) {
			entries = append(entries, entry{s.Name, a.Name, a.Description, a.ArgumentsSchema})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].skill != entries[j].skill {
			return entries[i].skill < entries[j].skill
		}
		return entries[i].action < entries[j].action
	})
	var b strings.Builder
	b.WriteString("运行某个 skill 暴露的 action（确定性执行：跑进程内 Go 函数，区别于 activate_skill 注入上下文）。可运行的 action：\n")
	for _, e := range entries {
		b.WriteString("- " + e.skill + " / " + e.action)
		if e.desc != "" {
			b.WriteString("：" + e.desc)
		}
		if len(e.schema) > 0 {
			if js, err := json.Marshal(e.schema); err == nil {
				b.WriteString("（args: " + string(js) + "）")
			}
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func (t *skillActionTool) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"skill":  map[string]any{"type": "string", "description": "skill 名"},
			"action": map[string]any{"type": "string", "description": "action 名（见本工具描述里的清单）"},
			"args":   map[string]any{"type": "object", "description": "传给 action 的参数（按该 action 的 args schema 填）"},
		},
		"required": []any{"skill", "action"},
	}
}

func (t *skillActionTool) Call(ctx context.Context, args map[string]any) (*tool.Result, error) {
	skillName, _ := args["skill"].(string)
	actionName, _ := args["action"].(string)
	if skillName == "" || actionName == "" {
		return tool.Failure(tool.ErrCodeInvalidArguments, "run_skill_action 需要 skill + action"), nil
	}
	actionArgs, _ := args["args"].(map[string]any)

	// DisableModelInvocation gate（同 activate_skill）：该 skill 禁止 LLM 调则其 action 也不可调。
	for _, s := range t.box.ListSkills() {
		if s.Name == skillName && s.DisableModelInvocation {
			return tool.Failure(SkillDisabledErrCode,
				fmt.Sprintf("skill %q is not model-invocable (disable-model-invocation=true)", skillName)), nil
		}
	}

	res, err := t.box.RunSkillAction(ctx, skillName, actionName, actionArgs)
	if err != nil {
		// 必传的错原样往上走（compose_v2.MustPropagate）：中断（业务 ActionFunc 抛 InterruptError
		// 做 HITL、敏感 action 执行前审批——吞掉的话 HITL 链断）、调用方喊停、软取消、陈旧写回、
		// 终局失败、坏档、身份冲突。
		//
		// 这里曾经手写判据、只认中断与 ctx 错：动作返回 ErrRunAborted / ErrSoftCanceled 被转成
		// 一条工具结果回灌模型，Run 照常成功（实测：最终答复「算好了」，err == nil）。ctx 那一档
		// 也从「错误链上有 ctx 错」改成「调用方的 ctx 死了没」：动作自己的超时是这次调用失败了，
		// 该让模型看见、换条路，不是整个 Run 被喊停。
		if compose_v2.MustPropagate(ctx, err) {
			return nil, err
		}
		// ErrSkillActionNotFound / 业务错 → 反馈 LLM（非 abort）。
		return tool.Failure(tool.ErrCodeNotFound,
			fmt.Sprintf("run_skill_action %s/%s: %v", skillName, actionName, err)), nil
	}
	return tool.Success(formatActionResult(res)), nil
}

// formatActionResult 把 SkillExecutionResult 格式化成回灌 LLM 的文本（结构化字段汇总）。
func formatActionResult(res *skill.SkillExecutionResult) string {
	if res == nil {
		return "(no result)"
	}
	var b strings.Builder
	if res.Output != "" {
		b.WriteString(res.Output)
	}
	if res.Stdout != "" {
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(res.Stdout)
	}
	if res.Stderr != "" {
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString("[stderr] " + res.Stderr)
	}
	if !res.Success {
		msg := res.Error
		if msg == "" {
			msg = "action failed"
		}
		if res.ExitCode != 0 {
			msg = fmt.Sprintf("%s (exit %d)", msg, res.ExitCode)
		}
		if b.Len() > 0 {
			return "[failed] " + msg + "\n" + b.String()
		}
		return "[failed] " + msg
	}
	if b.Len() == 0 {
		return "(action succeeded, no output)"
	}
	return b.String()
}

// boxHasActions 判断 box 是否有 ≥1 个**可被 LLM 调**的带 action skill（决定是否挂
// run_skill_action 工具）。跳过 DisableModelInvocation 的 skill——全禁则不挂工具（与
// Description/Call 的 gate 一致，避免挂个空壳工具）。
func boxHasActions(box skill.SkillBox) bool {
	for _, s := range box.ListSkills() {
		if s.DisableModelInvocation {
			continue
		}
		if len(box.ListSkillActions(s.Name)) > 0 {
			return true
		}
	}
	return false
}
