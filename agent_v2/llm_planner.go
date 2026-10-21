package agent_v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
)

// LLMPlanner 错误哨兵——caller 按错因分流（如"空 steps 时降级单步"）用 errors.Is
// 判定，避免 strings.Contains(err.Error(), ...) 这种脆弱 match。
var (
	// ErrPlannerNoToolCall LLM 没产 tool_call（违反 tool_choice=required）
	ErrPlannerNoToolCall = errors.New("planner returned no tool_call")
	// ErrPlannerEmptySteps Planner plan tool steps 数组为空
	ErrPlannerEmptySteps = errors.New("planner returned empty steps")
	// ErrPlannerBadToolCall LLM 调了既不是 plan 也不是 respond 的 tool
	ErrPlannerBadToolCall = errors.New("planner emitted unexpected tool")
	// ErrPlannerParseArgs LLM tool_call.arguments JSON 解析失败
	ErrPlannerParseArgs = errors.New("planner failed to parse tool args")
)

// LLMPlannerOption 配置 NewLLMPlanner 行为。
type LLMPlannerOption func(*llmPlannerConfig)

// llmSubAgentMeta 是 LLMPlanner 内部 sub-agent 元数据视图——通过 Agent.Description()
// 自动抽取（interface contract pattern）。
type llmSubAgentMeta struct {
	Name        string
	Description string
}

type llmPlannerConfig struct {
	systemPrompt string
	subAgents    []llmSubAgentMeta // 可选：广告给 LLM 的 sub-agent 池
}

// WithLLMPlannerSystemPrompt 自定义 Planner 的 system prompt。空时用默认。
func WithLLMPlannerSystemPrompt(s string) LLMPlannerOption {
	return func(c *llmPlannerConfig) { c.systemPrompt = s }
}

// WithLLMPlannerSubAgents 告诉 Planner 可选的 sub-agent map——LLM 在 plan tool 的
// 每个 step 里选填 sub_agent 字段。框架自动从 Agent.Description() 抽取描述注入
// prompt + tool schema enum。
//
// **设计：interface contract pattern**：sub-agent 在
// NewBasicAgent 等构造时通过 WithDescription("...") 设好，Planner 这里无需重复
// 手写——加 / 删专家只动 agent 构造，Planner prompt 不用改。
//
// 注意：本函数只抽元数据（name + description），不持 Agent 引用——实际派发由
// PlanAgent.WithSubAgents 注册的 map 完成。两边 key set 应一致。
func WithLLMPlannerSubAgents(agents map[string]Agent) LLMPlannerOption {
	return func(c *llmPlannerConfig) {
		c.subAgents = make([]llmSubAgentMeta, 0, len(agents))
		for name, ag := range agents {
			// audit⁴ HIGH-2 fix: isAgentNil 检 typed nil interface (与 WithSubAgents 协议对称).
			// 旧 `if ag != nil` 漏 Go typed nil interface: `var b *BasicAgent; WithLLMPlannerSubAgents(map{...})`
			// 时 ag != nil 为 true → ag.Description() nil deref panic.
			if isAgentNil(ag) {
				panic(fmt.Sprintf("WithLLMPlannerSubAgents: name %q has nil agent (含 Go typed nil interface 陷阱)", name))
			}
			desc := ag.Description()
			c.subAgents = append(c.subAgents, llmSubAgentMeta{Name: name, Description: desc})
		}
		sort.Slice(c.subAgents, func(i, j int) bool {
			return c.subAgents[i].Name < c.subAgents[j].Name
		})
	}
}

// NewLLMPlanner 构造一个用 LLM 驱动的默认 Planner——内部借 tool_call 机制做结构
// 化输出，把 agent 生态细节藏起来。
//
// 工作机制：
//   - 首次调用（executed == nil）：只挂 plan tool，强制 LLM 返 plan
//   - 后续调用（executed 非空）：plan + respond 两个 tool 都挂，LLM 自己选——
//     plan tool steps 非空 → 继续；respond tool answer → 作为 finalAnswer 终止
//
// plan / respond 是"信号 tool"——不真正执行，只借 tool_call schema 让 provider
// 替我们强校验结构化输出，比解析自由形式 JSON 鲁棒。
//
// tool_choice 强制为 required：禁止 LLM 返裸文本绕过 tool 约束。
//
// **provider 兼容注记**：dashscope qwen3 的 thinking 模式不支持 tool_choice=required
// （HTTP 400）——用 qwen.NewTestClient（默认已关 thinking）或业务 client 上
// qwen.WithEnableThinking(false) / 等价 extra body 关闭。
//
// HITL：plan 节点 ctx 含 ResumeContext 时（user 在 Resume 时插话），Planner 把
// user 文本注入 prompt 让 LLM 据此调整剩余步骤。
func NewLLMPlanner(m model.ChatModel, opts ...LLMPlannerOption) Planner {
	// 协议第七复证：同包同类型的 WithPlanModel / WithModel 都守，**工厂档漏了**。
	// 不守的话 typed nil 会一路活到 planner 首次被调用——业务看到的是
	// `node "plan": node panic recovered: invalid memory address or nil pointer
	// dereference`，零线索指向"你传了个 typed nil model"。
	if isFrameworkNil(m) {
		panic("NewLLMPlanner: typed nil ChatModel (含 Go interface 陷阱)")
	}
	cfg := llmPlannerConfig{systemPrompt: defaultLLMPlannerSystem}
	for _, opt := range opts {
		opt(&cfg)
	}

	respondTool := buildRespondToolInfo()

	return func(ctx context.Context, sess *RunSession, original string, executed []ExecutedStep) ([]PlanStep, string, error) {
		// plan tool schema 每次动态生成：depends_on 的 maximum = len(executed) - 1
		planTool := buildPlanToolInfo(cfg.subAgents, len(executed))
		// 首次只挂 plan tool；重规划时 plan + respond 都挂
		tools := []*schema.ToolInfo{planTool}
		if len(executed) > 0 {
			tools = append(tools, respondTool)
		}
		boundModel, err := m.WithTools(tools)
		if err != nil {
			return nil, "", fmt.Errorf("llm planner: bind tools: %w", err)
		}

		// HITL：Resume 时框架通过 ResumeContext 透传 user 新指令——Planner 把它
		// 塞进 prompt 让 LLM 看到并据此调整剩余步骤。
		//
		// 走**非寻址型**通道：planner 不在节点里、没有自己的 InterruptID，它要的是
		// 「用户这轮说了什么」而不是「某个提问的答案」。曾经用「取 map 里任意一条」实现，
		// 那会吃到别人的答案（实测：用户答任务 A 的 ID，节点执行了任务 B 那件事）。
		resumeUserMsg, _ := compose_v2.GetResumeUserText(ctx)
		msgs := []*schema.Message{
			schema.SystemMessage(cfg.systemPrompt),
			schema.UserMessage(buildLLMPlannerUserInput(original, executed, cfg.subAgents, resumeUserMsg)),
		}
		_ = sess // 保留参数签名兼容（未来想读 sess 历史扩展时用）
		out, err := boundModel.Generate(ctx, msgs, model.WithToolChoice(schema.ToolChoiceRequired))
		if err != nil {
			return nil, "", fmt.Errorf("llm planner: generate: %w", err)
		}
		calls := schema.GetToolCalls(out)
		if len(calls) == 0 {
			return nil, "", fmt.Errorf("llm planner: %w (content=%q)", ErrPlannerNoToolCall, out.Content)
		}
		call := calls[0]
		switch call.Function.Name {
		case planToolName:
			steps, perr := parsePlanToolArgs(call.Function.Arguments)
			if perr != nil {
				return nil, "", fmt.Errorf("llm planner: %w: plan args: %v", ErrPlannerParseArgs, perr)
			}
			if len(steps) == 0 {
				return nil, "", fmt.Errorf("llm planner: %w", ErrPlannerEmptySteps)
			}
			return steps, "", nil
		case respondToolName:
			answer, perr := parseRespondToolArgs(call.Function.Arguments)
			if perr != nil {
				return nil, "", fmt.Errorf("llm planner: %w: respond args: %v", ErrPlannerParseArgs, perr)
			}
			return nil, answer, nil
		default:
			return nil, "", fmt.Errorf("llm planner: %w: %q", ErrPlannerBadToolCall, call.Function.Name)
		}
	}
}

const (
	planToolName    = "plan"
	respondToolName = "respond"
)

const defaultLLMPlannerSystem = `你是计划执行者。给定一个目标，把它拆解成有序的可执行步骤。

规则：
- 首次规划：调用 plan 工具输出完整步骤列表
- 后续调用时会收到"已完成步骤"摘要：
  - 若目标已达成 → 调用 respond 工具给出最终答复
  - 若还需继续 → 调用 plan 工具输出剩余步骤（不要重复已完成的）
- 必须调用工具，不要输出自由文本

【每步依赖声明 depends_on】
对每一步判断它分配的 sub-agent 执行的任务，**本身是否依赖**前面已完成步骤的原文产出：
- 依赖：在 depends_on 里列出对应 index（0-based，指 executed 数组下标）。
  原文会原样附给 sub-agent。
- 独立：留空数组 []。

判断起点是"这个 agent 的任务本身需不需要前面的产出"，而不是"description 要不要提到前面"。

注意：
- 不要在 description 里复制粘贴前面步骤的结果——设了 depends_on 框架会自动附原文
- 若 sub-agent 只需要前面结果的**摘要/结论**，正确做法是规划时插一个 summarizer step
- depends_on 的 index 和 user 消息里"已完成步骤"列表前的 [N] 标号一致（都从 0 开始）`

// buildLLMPlannerUserInput 构造每轮喂给 Planner LLM 的 user input：目标(original) + 可选
// sub_agent 列表 + 已完成步骤(executed：[i] Step→Result) + Resume 补充指示。
//
// **设计决策：有意不注入"最初规划的完整 plan 蓝图"——这是 replan 的精髓，不是缺失**
// （设计已反复确认；别 audit/agent 又当"缺蓝图"想补）：
//   - 最初蓝图是「信息最不完整时」做的、最可能错；注入它给 LLM 一个 anchoring bias，反而让
//     LLM 倾向维持/补全旧规划、抗拒根据中间结果大幅调整——正好违背 replan 的目的。
//   - replan 该锚定的是「不变的目标(original)」+「动态现实(executed 真实结果)」，让 LLM 每步
//     基于现实重新规划剩余，不被可能错的旧蓝图绑架。目标一致、规划随现实演进，才是 replan。
//   - 注入原始蓝图(原始 {plan})反而是潜在 anchoring 弱点，crew-go 有意不这么做。
func buildLLMPlannerUserInput(original string, executed []ExecutedStep, subAgents []llmSubAgentMeta, resumeUserMsg string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "目标：%s\n", original)
	if len(subAgents) > 0 {
		// 列 sub-agent name + Description（interface contract pattern）
		sb.WriteString("\n可选 sub_agent：\n")
		for _, m := range subAgents {
			if m.Description == "" {
				fmt.Fprintf(&sb, "- %s\n", m.Name)
			} else {
				fmt.Fprintf(&sb, "- %s: %s\n", m.Name, m.Description)
			}
		}
	}
	if len(executed) > 0 {
		sb.WriteString("\n已完成步骤（index 从 0 开始）：\n")
		for i, e := range executed {
			fmt.Fprintf(&sb, "[%d] %s → %s\n", i, e.Step, e.Result)
		}
	}
	if resumeUserMsg != "" {
		fmt.Fprintf(&sb, "\n用户补充指示（请据此调整剩余步骤）：%s\n", resumeUserMsg)
	}
	return sb.String()
}

// buildPlanToolInfo 每次 replan 时动态构造 plan tool schema。
// executedLen 用于生成 depends_on 的 maximum 边界——provider 替我们校验 index
// 不越界，减少 LLM hallucination 的 fallback 依赖。
func buildPlanToolInfo(subAgents []llmSubAgentMeta, executedLen int) *schema.ToolInfo {
	stepProps := map[string]any{
		"description": map[string]any{
			"type":        "string",
			"description": "本步骤要完成的具体任务描述（不要搬运前面步骤的原文，原文通过 depends_on 自动附加）",
		},
	}
	required := []string{"description", "depends_on"}
	if len(subAgents) > 0 {
		var agentDesc strings.Builder
		agentDesc.WriteString("执行本步骤的 sub-agent 名称。可选：")
		for i, m := range subAgents {
			if i > 0 {
				agentDesc.WriteString("; ")
			}
			if m.Description == "" {
				agentDesc.WriteString(m.Name)
			} else {
				fmt.Fprintf(&agentDesc, "%s（%s）", m.Name, m.Description)
			}
		}
		names := make([]string, 0, len(subAgents))
		for _, m := range subAgents {
			names = append(names, m.Name)
		}
		stepProps["sub_agent"] = map[string]any{
			"type":        "string",
			"description": agentDesc.String(),
			"enum":        names,
		}
	}
	dependsItems := map[string]any{
		"type":    "integer",
		"minimum": 0,
	}
	if executedLen > 0 {
		dependsItems["maximum"] = executedLen - 1
	}
	stepProps["depends_on"] = map[string]any{
		"type":        "array",
		"items":       dependsItems,
		"description": "这一步需要哪些已完成步骤的**原文**附给 sub-agent。独立任务留空数组 []。index 从 0 开始。",
	}
	return &schema.ToolInfo{
		Name: planToolName,
		Desc: "输出（剩余）执行步骤列表。首次规划输出全量步骤；后续调用只输出尚未完成的步骤。每步必须声明 depends_on。",
		Params: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"steps": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type":       "object",
						"properties": stepProps,
						"required":   required,
					},
				},
			},
			"required": []string{"steps"},
		},
	}
}

func buildRespondToolInfo() *schema.ToolInfo {
	return &schema.ToolInfo{
		Name: respondToolName,
		Desc: "目标已达成时，输出最终答复，结束计划执行。",
		Params: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"answer": map[string]any{
					"type":        "string",
					"description": "给用户的最终答复",
				},
			},
			"required": []string{"answer"},
		},
	}
}

func parsePlanToolArgs(argsJSON string) ([]PlanStep, error) {
	var raw struct {
		Steps []struct {
			Description string `json:"description"`
			SubAgent    string `json:"sub_agent"`
			DependsOn   []int  `json:"depends_on"`
		} `json:"steps"`
	}
	if err := json.Unmarshal([]byte(argsJSON), &raw); err != nil {
		return nil, fmt.Errorf("unmarshal: %w (raw=%q)", err, argsJSON)
	}
	steps := make([]PlanStep, 0, len(raw.Steps))
	for _, s := range raw.Steps {
		desc := strings.TrimSpace(s.Description)
		if desc == "" {
			continue
		}
		refs := make([]RefSpec, 0, len(s.DependsOn))
		for _, idx := range s.DependsOn {
			refs = append(refs, IndexRef(idx))
		}
		steps = append(steps, PlanStep{
			Description:  desc,
			SubAgentName: strings.TrimSpace(s.SubAgent),
			InputRefs:    refs,
		})
	}
	return steps, nil
}

func parseRespondToolArgs(argsJSON string) (string, error) {
	var raw struct {
		Answer string `json:"answer"`
	}
	if err := json.Unmarshal([]byte(argsJSON), &raw); err != nil {
		return "", fmt.Errorf("unmarshal: %w (raw=%q)", err, argsJSON)
	}
	return raw.Answer, nil
}
