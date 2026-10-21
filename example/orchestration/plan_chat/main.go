// plan_chat 演示 PlanAgent 的 replan-每步多轮协作。
//
// 通过 -scenario 切换不同场景，对比 replan 在不同任务结构下的价值：
//
//	go run ./example/orchestration/plan_chat -scenario=default       # researcher/writer/critic 线性写作（基线，replan 价值低）
//	go run ./example/orchestration/plan_chat -scenario=code_review   # A：写→评审→修→再评审 循环，循环次数动态
//	go run ./example/orchestration/plan_chat -scenario=feasibility   # B：调研可行性，发现不行就早收尾砍掉后续
//	go run ./example/orchestration/plan_chat -scenario=fault         # C：故障诊断，每步发现决定下一步查哪
//	go run ./example/orchestration/plan_chat -scenario=budget        # D：预算约束，超支后回退/替换已规划步骤
//	go run ./example/orchestration/plan_chat -scenario=drill         # E：codebase 递归钻取，深度事前不定
//	go run ./example/orchestration/plan_chat -scenario=arbiter       # F：多源对比，分歧出现时追加查证
//
// 启动后会打印该场景的对话脚本建议；输入 /exit 或 Ctrl-D 退出。
//
// ISSUE-72 R7：scenario 定义已抽到 example/scenarios 中立公共包，plan_chat CLI / plan_chat_web / channel_chat_web 三个 demo 共享。
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"crew-go/agent_v2"
	"crew-go/example/envloader"
	"crew-go/example/llmtrace"
	"crew-go/example/scenarios"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/model/deepseek"
	"crew-go/schema"
)

func main() {
	envloader.Load()

	scenarioName := flag.String("scenario", "default", "demo 场景名（default/code_review/feasibility/fault/budget/drill/arbiter/writing_team）")
	provider := flag.String("provider", "qwen", "真 LLM provider: qwen (默认, dashscope, 需 QWEN_API_KEY) / deepseek (需 DEEPSEEK_API_KEY)")
	flag.Parse()

	// iter-5-O: 加 -provider qwen|deepseek 跟 CLAUDE.md "测试 LLM 默认 qwen" 对齐.
	// plan_chat 是 CLI Plan-Execute demo, Plan↔Exec replan 真价值靠 LLM 决策 — 不
	// 加 -mock (业务想 mock 用 plan_chat_web 内置 mock.go).
	var m model.ChatModel
	switch *provider {
	case "qwen":
		if os.Getenv("QWEN_API_KEY") == "" {
			log.Fatal("QWEN_API_KEY 未设置 (.env 加 QWEN_API_KEY=xxx), 或 -provider=deepseek")
		}
		m, _ = llmtrace.NewQwenFallback()
		log.Printf("[plan_chat] qwen 备选链模式 (dashscope; 候选取 qwen.TestModelCandidates, 额度耗尽自动切)")
	case "deepseek":
		apiKey := os.Getenv("DEEPSEEK_API_KEY")
		if apiKey == "" {
			log.Fatal("DEEPSEEK_API_KEY 未设置, 或 -provider=qwen")
		}
		m = deepseek.NewClient(apiKey, "")
		log.Printf("[plan_chat] deepseek 模式")
	default:
		log.Fatalf("未知 -provider %q (支持 qwen / deepseek)", *provider)
	}
	hooks := traceHook{}

	scn, ok := scenarios.Build(*scenarioName, m, hooks)
	if !ok {
		fmt.Fprintf(os.Stderr, "未知 scenario: %q\n可选：\n", *scenarioName)
		for _, name := range scenarios.Names() {
			fmt.Fprintf(os.Stderr, "  - %s\n", name)
		}
		os.Exit(2)
	}

	plannerAgent := agent_v2.NewBasicAgent(
		agent_v2.WithName("planner"),
		agent_v2.WithSystemPrompt(scn.PlannerPrompt),
		agent_v2.WithModel(m),
	)
	planner := newJSONPlanner(plannerAgent, scn.SubAgents)

	ag := agent_v2.NewPlanAgent(
		planner,
		agent_v2.PlanName("chat_plan"),
		agent_v2.WithSubAgents(scn.SubAgents),
		agent_v2.WithExecInputBuilder(makeExecInputBuilder()),
	).SetExecutor(scn.Fallback)
	sess := agent_v2.NewRunSession()

	printBanner(scn)

	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	fmt.Print("\nyou> ")
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			fmt.Print("you> ")
			continue
		}
		if line == "/exit" || line == "/quit" {
			fmt.Println("bye")
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		events, err := ag.Run(ctx, sess, schema.UserMessage(line))
		cancel()
		if err != nil {
			fmt.Printf("[error] %v\n", err)
			fmt.Print("\nyou> ")
			continue
		}
		if msg := agent_v2.FinalMessage(events); msg != nil {
			fmt.Printf("\nagent> %s\n", msg.Content)
		}
		fmt.Printf("[history size: %d]\n\nyou> ", len(sess.History()))
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("读取输入失败：%v", err)
	}
	fmt.Println()
}

// printBanner 打印当前 scenario 的描述和建议对话脚本。
func printBanner(scn scenarios.Scenario) {
	fmt.Println("────────────────────────────────────────")
	fmt.Printf(" PlanAgent demo · scenario=%s\n", scn.Name)
	fmt.Println(" " + scn.Description)
	if scn.ReplanHint != "" {
		fmt.Println(" replan 看点：" + scn.ReplanHint)
	}
	subs := make([]string, 0, len(scn.SubAgents))
	for k := range scn.SubAgents {
		subs = append(subs, k)
	}
	sort.Strings(subs)
	fmt.Printf(" 专家：%s\n", strings.Join(subs, " / "))
	if len(scn.DialogueScript) > 0 {
		fmt.Println(" 建议对话脚本：")
		for i, line := range scn.DialogueScript {
			fmt.Printf("   %d. %s\n", i+1, line)
		}
	}
	fmt.Println(" 输入 /exit 或 Ctrl-D 退出")
	fmt.Println("────────────────────────────────────────")
}

// ───────────── 共享 Planner 实现（所有 scenario 共用） ─────────────

// agentMeta 是 Planner 内部用的 sub-agent 元数据视图（ISSUE-72 R7）。
type agentMeta struct {
	Name        string
	Description string
}

// extractAgentMetas 从 subAgents map 抽 metas 字典序排序。
func extractAgentMetas(subAgents map[string]agent_v2.Agent) []agentMeta {
	metas := make([]agentMeta, 0, len(subAgents))
	for name, ag := range subAgents {
		desc := ""
		if ag != nil {
			desc = ag.Description()
		}
		metas = append(metas, agentMeta{Name: name, Description: desc})
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].Name < metas[j].Name })
	return metas
}

// newJSONPlanner 真 replan-每步：每次调 plannerAgent，把 executed 摘要塞进 prompt
// 让 LLM 决定剩余步骤或收尾。闭包内缓存 prevRemaining 以打印 diff 行。
//
// ISSUE-72 R7：subAgents map 透传进来抽 Description() 注入 prompt——加 / 删专家
// 时只动 scenario builder，无需改 PlannerPrompt（webPlanner 同款机制）。
func newJSONPlanner(plannerAgent agent_v2.Agent, subAgents map[string]agent_v2.Agent) agent_v2.Planner {
	metas := extractAgentMetas(subAgents)
	var prevRemaining []agent_v2.PlanStep
	return func(ctx context.Context, sess *agent_v2.RunSession, input string, executed []agent_v2.ExecutedStep) ([]agent_v2.PlanStep, string, error) {
		prompt := buildReplanPrompt(input, executed, metas)
		events, err := plannerAgent.Run(ctx, sess, schema.UserMessage(prompt))
		if err != nil {
			return nil, "", fmt.Errorf("planner: %w", err)
		}
		out := agent_v2.FinalMessage(events)
		if out == nil {
			return nil, "", fmt.Errorf("planner: empty output")
		}
		raw := extractJSONArray(out.Content)
		// ISSUE-47：refs 元素允许 int 或 string；string 按 agent 名在 executed 里取最近一个匹配。
		// 所以这里用 json.RawMessage 接，再分别解析。
		var items []struct {
			Desc  string            `json:"desc"`
			Agent string            `json:"agent"`
			Refs  []json.RawMessage `json:"refs"`
		}
		if err := json.Unmarshal([]byte(raw), &items); err != nil {
			return nil, "", fmt.Errorf("planner: parse json %q: %w", raw, err)
		}
		steps := make([]agent_v2.PlanStep, 0, len(items))
		for _, it := range items {
			// ISSUE-54：闭包不再 resolve refs；只把 LLM 输出的引用塞进 RefSpec，
			// 解析在 PlanAgent.execNode 即将执行该 step 时进行。这样 sibling refs
			// 不再触发"executed 里查不到"的伪错误（PlanAgent 只取 remaining[0]，
			// step[1:] 永远在下次 replan 时被丢弃，根本不会走到 resolve）。
			refs := parseRawRefs(it.Refs)
			steps = append(steps, agent_v2.PlanStep{
				Description:  it.Desc,
				SubAgentName: it.Agent,
				InputRefs:    refs,
			})
		}

		var expected []agent_v2.PlanStep
		if len(prevRemaining) > 1 {
			expected = prevRemaining[1:]
		}

		switch {
		case len(executed) == 0:
			fmt.Printf("  [plan←] 初始计划：%d 步\n", len(steps))
			printPlan("    ", steps)
		case len(steps) == 0:
			fmt.Printf("  [plan←] 重规划：已完成 %d 步，目标已达成 → 收尾\n", len(executed))
		case planEqual(steps, expected):
			fmt.Printf("  [plan←] 重规划：已完成 %d 步，剩余 %d 步（与上次一致，未优化）\n", len(executed), len(steps))
			printPlan("    ", steps)
		default:
			fmt.Printf("  [plan←] 重规划：已完成 %d 步，剩余 %d 步（已优化，原 %d 步）\n", len(executed), len(steps), len(expected))
			printPlanDiff("    ", expected, steps)
		}

		prevRemaining = steps
		if len(steps) == 0 {
			return nil, "", nil
		}
		return steps, "", nil
	}
}

func buildReplanPrompt(original string, executed []agent_v2.ExecutedStep, metas []agentMeta) string {
	var b strings.Builder
	fmt.Fprintf(&b, "原始目标：%s\n\n", original)
	// ISSUE-72 R7：把可选 agent 列表（含 description）注入 prompt——避免在 system
	// prompt 里写死，加专家只动 scenario builder。
	if len(metas) > 0 {
		b.WriteString("可选 agent（按目标和已完成步骤选最合适的）：\n")
		for _, mt := range metas {
			if mt.Description == "" {
				fmt.Fprintf(&b, "  - %s\n", mt.Name)
			} else {
				fmt.Fprintf(&b, "  - %s: %s\n", mt.Name, mt.Description)
			}
		}
		b.WriteString("\n")
	}
	if len(executed) == 0 {
		b.WriteString("已完成步骤：（无，请给出初始计划）\n\n")
	} else {
		fmt.Fprintf(&b, "已完成 %d 步（refs 中可用 0-based [N] 或 agent 名引用）：\n", len(executed))
		for i, e := range executed {
			fmt.Fprintf(&b, "  [%d] (%s) %s\n      产出摘要：%s\n", i, e.Agent, e.Step, truncate(e.Result, 200))
		}
		b.WriteString("\n")
	}
	b.WriteString("请输出剩余步骤的 JSON 数组（若已完成请输出 []）。")
	return b.String()
}

// parseRawRefs 把 LLM 输出的 refs 数组（int 或 string）转成 []agent_v2.RefSpec。
// 不做任何 executed 关联校验（ISSUE-54）——校验在 PlanAgent.execNode 即将执行该 step
// 时由 agent_v2.ResolveInputRefs 进行。
//
// 元素：
//   - int  → IndexRef(i)
//   - string → AgentRef(name)
//   - 既不是 int 也不是 string → 跳过并 warn 一行（不阻断；最多就是这步少注入一个 ref）
func parseRawRefs(raw []json.RawMessage) []agent_v2.RefSpec {
	if len(raw) == 0 {
		return nil
	}
	out := make([]agent_v2.RefSpec, 0, len(raw))
	for i, r := range raw {
		var asInt int
		if err := json.Unmarshal(r, &asInt); err == nil {
			out = append(out, agent_v2.IndexRef(asInt))
			continue
		}
		var asStr string
		if err := json.Unmarshal(r, &asStr); err == nil {
			out = append(out, agent_v2.AgentRef(asStr))
			continue
		}
		fmt.Printf("    [warn] refs[%d] (%s) 既不是 int 也不是 string，跳过\n", i, string(r))
	}
	return out
}

func planEqual(a, b []agent_v2.PlanStep) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].SubAgentName != b[i].SubAgentName || a[i].Description != b[i].Description {
			return false
		}
	}
	return true
}

func printPlan(indent string, steps []agent_v2.PlanStep) {
	for i, s := range steps {
		fmt.Printf("%s%d. [%s] %s\n", indent, i+1, s.SubAgentName, s.Description)
	}
}

func printPlanDiff(indent string, oldSteps, newSteps []agent_v2.PlanStep) {
	n := len(oldSteps)
	if len(newSteps) > n {
		n = len(newSteps)
	}
	for i := 0; i < n; i++ {
		switch {
		case i >= len(oldSteps):
			s := newSteps[i]
			fmt.Printf("%s+ [%s] %s\n", indent, s.SubAgentName, s.Description)
		case i >= len(newSteps):
			s := oldSteps[i]
			fmt.Printf("%s- [%s] %s\n", indent, s.SubAgentName, s.Description)
		case oldSteps[i].SubAgentName == newSteps[i].SubAgentName && oldSteps[i].Description == newSteps[i].Description:
			s := newSteps[i]
			fmt.Printf("%s= [%s] %s\n", indent, s.SubAgentName, s.Description)
		default:
			o, n2 := oldSteps[i], newSteps[i]
			fmt.Printf("%s~ [%s] %s  →  [%s] %s\n", indent, o.SubAgentName, o.Description, n2.SubAgentName, n2.Description)
		}
	}
}

func extractJSONArray(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "```") {
		s = strings.TrimPrefix(s, "```json")
		s = strings.TrimPrefix(s, "```")
		if idx := strings.LastIndex(s, "```"); idx >= 0 {
			s = s[:idx]
		}
		s = strings.TrimSpace(s)
	}
	lb := strings.Index(s, "[")
	rb := strings.LastIndex(s, "]")
	if lb >= 0 && rb > lb {
		return s[lb : rb+1]
	}
	return s
}

// makeExecInputBuilder 返回一个带 trace 打印的 ExecInputBuilder。
// 行为与 agent_v2 默认的 defaultExecInputBuilder 保持一致（按 resolvedRefs 注入原文），
// 仅多 trace 一行，方便 demo 时直观看到每步派发。
//
// ISSUE-54：refs 已由 PlanAgent.execNode 通过 ResolveInputRefs 解析为 0-based 下标，
// 这里直接消费 resolvedRefs 即可，不再触碰 RefSpec。
func makeExecInputBuilder() agent_v2.ExecInputBuilder {
	return func(
		_ context.Context,
		original string,
		executed []agent_v2.ExecutedStep,
		current agent_v2.PlanStep,
		resolvedRefs []int,
	) (*schema.Message, error) {
		stepIdx := len(executed)
		target := current.SubAgentName
		if target == "" {
			target = "(default)"
		}
		fmt.Printf("  [step %d → %s] %s\n", stepIdx, target, current.Description)

		var b strings.Builder
		fmt.Fprintf(&b, "原始目标：%s\n\n你的任务：%s", original, current.Description)
		for _, idx := range resolvedRefs {
			// ResolveInputRefs 已过滤越界；这里 paranoia 兜底
			if idx < 0 || idx >= len(executed) {
				continue
			}
			e := executed[idx]
			fmt.Fprintf(&b, "\n\n步骤 %d（%s）的产出：\n%s", idx+1, e.Step, e.Result)
		}
		return schema.UserMessage(b.String()), nil
	}
}

// ───────────── 共享 traceHook（所有 scenario 共用） ─────────────

type traceHook struct{ hook.NoOpHooks }

func (traceHook) OnAgentStart(ctx context.Context, info *hook.AgentInfo, in *hook.AgentInput) (context.Context, error) {
	if info.Round != 0 || len(in.Messages) == 0 {
		return ctx, nil
	}
	last := in.Messages[len(in.Messages)-1]
	fmt.Printf("    [agent→] %s input(%s, %d chars): %s\n",
		info.Name, last.Role, len(last.Content), truncate(last.Content, 120))
	return ctx, nil
}

func (traceHook) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	fmt.Printf("    [llm→] %s round=%d msgs=%d\n", info.Name, info.Round, len(in.Messages))
	return ctx, nil
}

func (traceHook) OnModelEnd(ctx context.Context, info *hook.AgentInfo, output *schema.ModelOutput) (context.Context, error) {
	if output == nil || output.Message == nil {
		return ctx, nil
	}
	msg := output.Message
	if len(schema.GetToolCalls(msg)) > 0 {
		return ctx, nil
	}
	fmt.Printf("    [agent←] %s output(%d chars): %s\n",
		info.Name, len(msg.Content), truncate(msg.Content, 120))
	return ctx, nil
}

func (traceHook) OnToolStart(ctx context.Context, info *hook.AgentInfo, in *schema.ToolInput) (context.Context, error) {
	fmt.Printf("    [tool→] %s.%s args=%s\n", info.Name, in.Name, in.ArgumentsJSON)
	return ctx, nil
}

func (traceHook) OnToolEnd(ctx context.Context, info *hook.AgentInfo, out *schema.ToolOutput) (context.Context, error) {
	if out.Result == nil {
		return ctx, nil
	}
	if out.Result.Success {
		fmt.Printf("    [tool←] %s: ok\n", info.Name)
	} else {
		fmt.Printf("    [tool←] %s: fail(%s)\n", info.Name, out.Result.ErrorCode)
	}
	return ctx, nil
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	r := []rune(s)
	if len(r) <= n {
		return string(r)
	}
	return string(r[:n]) + "..."
}
