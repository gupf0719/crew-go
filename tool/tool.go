package tool

import (
	"context"
	"fmt"
	"sync"

	"crew-go/schema"
)

// Tool 是可由代理调用的可调用工具。
//
// **并发协议（reentrant 约定 — ISSUE-81）**：`Call` 必须 reentrant 安全——
// agent_v2/compose_v2 框架内 BasicAgent 同 LLM 轮 emit 多 tool_call (N>=2) 时
// **并发**调用 N 个 tool（goroutine 并行调 `Call`），可能：
//   - 同一 *Tool 实例被多 goroutine 同时调用 `Call`（同 toolkit 同名 tool 多次 emit）
//   - 不同 *Tool 实例同 goroutine 调用各自 `Call`
//
// 业务实现 Tool.Call 时需注意：
//   - 实例字段（如 `*MyTool` 的 `t.cache` / `t.counter`）跨调用共享时必须 mutex / atomic 保护
//   - 调用外部资源（DB / file / HTTP）时按外部资源自身 thread-safety 假设
//   - 闭包捕获的变量（FuncTool）同样需 reentrant
//   - **ctx 中携带的 mutable 共享对象**（业务通过 `ctx.WithValue(ctx, key, &cache)` 注入
//     的 cache / counter）也需 mutex/atomic 保护——多 goroutine Call 共享同 ctx，
//     直接 mutate ctx-stored 对象会 race（audit C LOW-2）
//
// **不 reentrant 的 tool 实现是 bug**——历史 v1 时代 toolsFn 串行执行掩盖了此 bug，
// ISSUE-81 BasicAgent toolsFn fanout 化后必然显现。framework 内置 *agentTool
// (AgentAsTool) / *skillTool / *FuncTool 都已 reentrant。
//
// 业务自定义 Tool.Call 实现想强制串行（无法改 reentrant）的可在 Call 内部全局
// mutex 包整个 body，但这等价禁用 fanout 性能收益。
type Tool interface {
	Name() string
	Description() string
	InputSchema() map[string]any
	Call(ctx context.Context, arguments map[string]any) (*Result, error)
}

// StreamableTool 是可选接口，表示工具支持流式返回结果。
// 实现此接口后框架通过 type switch 自动识别并走流式路径；
// 未实现的工具在流式调用时自动降级为单 chunk 流，向后兼容。
type StreamableTool interface {
	Tool
	// StreamCall 以流式方式执行工具，调用方负责关闭 StreamReader。
	StreamCall(ctx context.Context, arguments map[string]any) (*schema.StreamReader[*Result], error)
}

// Categorizer 是可选接口, 业务自定义 tool 实现它声明 trace 上的细分类型.
//
// **trace 后端对照** (业务自标 category attr):
//   - langfuse: `langfuse.observation.type=retriever` attr 业务自标
//
// 我们 Go 项目用可选接口风格 (Go-idiomatic): 业务自定义 tool 实现 Categorizer 后,
// framework 在 OnToolStart fire 之前自动透传 category 让 observability hook 创对应
// span kind (retrieval / embedding / guardrail), 监控 UI 按类型渲染
// 不同图标 / 颜色 / 过滤维度.
//
// **未实现 Categorizer 的 tool 默认 tool kind** — 向后兼容, 业务现有 tool 0 改动.
//
// **典型业务场景** (BasicAgent + RAG tool LLM-driven 路径):
//
//	type MyVectorRetriever struct{}
//	func (r *MyVectorRetriever) Name() string { return "vector_search" }
//	func (r *MyVectorRetriever) Description() string { return "RAG 检索向量库" }
//	func (r *MyVectorRetriever) InputSchema() map[string]any { return ... }
//	func (r *MyVectorRetriever) Call(ctx, args) (*Result, error) { ... }
//	func (r *MyVectorRetriever) Category() string {
//	    return tool.CategoryRetrieval  // ← 唯一额外, framework 自动按 retrieval 标
//	}
type Categorizer interface {
	Category() string
}

// Category 常量 — 字符串值小写, 跟 langfuse OBSERVATION_TYPE / OTel
// `gen_ai.operation.name` semconv 一致.
//
// 业务自定义 Categorizer 应返这些常量之一. 返其他字符串 framework fallback 到
// tool kind (未知 category 跳过路由, 保向后兼容业务 typo).
const (
	// CategoryRetrieval RAG 检索 tool — vector store / 全文检索 / SQL 查询等.
	// 值 "retrieval" 对齐 CS-OSS AI_TRACE_SPEC `gen_ai.retrieval.*` + OTel operation.name
	// (动作名 retrieval 而非主体名 retriever).
	CategoryRetrieval = "retrieval"
	// CategoryEmbedding 向量化 tool — text/image embedding 模型调用.
	CategoryEmbedding = "embedding"
	// CategoryGuardrail 安全护栏 tool — PII 检测 / 内容审查 / 输出过滤.
	CategoryGuardrail = "guardrail"
	// 注: 无 CategoryEvaluator — CS-OSS/OTel operation.name 无 evaluator, LLM-as-judge
	// sub-agent 走默认 agent kind (invoke_agent), 不强造 operation.name.
)

// IsStreamable 检查工具是否支持流式调用。
func IsStreamable(t Tool) bool {
	_, ok := t.(StreamableTool)
	return ok
}

// Result 保存工具调用的结果。
//
// **Metadata**：tool 实现可在此放运行时信号供框架消费——不进 LLM input（不像
// Output 会序列化成 tool_result content）。典型场景：tool 想告诉框架"本次调用走
// 了特殊路径"以便框架做后续处理决策。
//
// 已知约定 key：
//   - `agent_v2.SkillForkExecutedMetaKey = "__skill_fork_executed__"`：activate_skill
//     fork 模式信号——makeToolsFn 检到此 flag 跳过 P2.D inline body wrapping
//     （fork final answer 不是 skill body，不能当 body 包装）
type Result struct {
	Success  bool           `json:"success"`
	Output   any            `json:"output,omitempty"`
	Error    *Error         `json:"error,omitempty"`
	Metadata map[string]any `json:"-"` // 不序列化（运行时信号给框架，非 LLM input）
}

// ReturnDirectMetaKey 是 Result.Metadata 的约定 key（值 bool）——工具声明"调用后立即
// 终止当前 BasicAgent 的 ReAct 循环、不再回 LLM"（return-direct 语义）。
//
// 用法：工具返 Result.Metadata[ReturnDirectMetaKey]=true 时，BasicAgent 执行完该工具后
// 短路到终止路径（postamble/End），不把结果喂回 LLM 继续生成。
//
// # 谁的产出算答案，判据是「短路之后还有没有别人来说话」
//
//   - **业务自己的 return-direct 工具**（exit 类）：短路之后没有任何人再说话，
//     **产出即答复**。
//   - **框架铸的** `transfer_to_agent` / `route_to`：**控制动作**，答复仍是带 tool_call
//     的 assistant。真实承重点是**历史可见性**（HandoffAgent Rewrite 模式下后接看前接
//     说了什么——取了 transfer 的产出，后接看到的就是机械文本）。判「框架铸的」用的是
//     铸造事实（`agent_v2.ReturnDirectMintedAnnotation`），**不是名字**：业务注册同名
//     工具（`respond` / `route_to` 这类）时走上一档，产出即答复。
//   - `final_output`：契约就是「我的产出即答案」，同一批里**恒胜**。
//
// ⚠️ 一批里出现多个候选时**按语义挑，不按位置**。这里一度写成「第一个命中就 break」，
// 于是业务工具排在 `final_output` 前面就把它遮蔽掉了
// （`TestStructuredOutput_FinalOutputOrderIndependent` 钉的正是这个）。
//
// **典型用途**：HandoffAgent 的 transfer 工具（一调即转交，不让原 agent 继续唠叨）、
// exit 类工具。框架侧机制详见 agent_v2.ReturnDirectAnnotation。
//
// **零副作用**：不设此 key 的工具行为完全不变（BasicAgent 正常回 model 继续 ReAct）。
const ReturnDirectMetaKey = "__return_direct__"

// Error 描述工具级错误。Code 确定框架行为。
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) String() string {
	return fmt.Sprintf("%s：%s", e.Code, e.Message)
}

// 框架级错误代码。代理层拦截这些并中止执行。
const (
	ErrCodeTimeout          = "timeout"
	ErrCodeNotFound         = "tool_not_found"
	ErrCodeInternal         = "internal_error"
	ErrCodeRetryLimit       = "retry_limit_exceeded"
	ErrCodeInvalidArguments = "invalid_arguments"
)

// IsFrameworkError 检测 result 是否含框架预设错误代码（5 个：Timeout / NotFound /
// Internal / RetryLimit / InvalidArguments）。
//
// **R17 audit MED-1 重要澄清**：本函数是**业务侧 categorization helper**——
// agent_v2 / compose_v2 框架代码**全无消费**，所有 tool 错误（含框架级 code）
// 当前都按业务级失败处理：result 序列化进 tool_result content 反馈给 LLM 让其
// 看到错误信息继续 ReAct 循环。
//
// 历史上设计意图是"框架级 = abort 整 Run；业务级 = 反馈 LLM"，但实际框架未实现
// abort 路径——所有错误统一反馈 LLM。设计意图 vs 现实分歧，业务**不要**依赖
// "返 framework error code 框架会 abort"——目前不会。
//
// 业务可继续用本函数做自己的分类（如日志按 code 分级），但不要用作"框架行为
// 触发器"。
func IsFrameworkError(r *Result) bool {
	if r == nil || r.Error == nil {
		return false
	}
	switch r.Error.Code {
	case ErrCodeTimeout, ErrCodeNotFound, ErrCodeInternal, ErrCodeRetryLimit, ErrCodeInvalidArguments:
		return true
	}
	return false
}

// Success 创建成功的结果。
func Success(output any) *Result {
	return &Result{Success: true, Output: output}
}

// Failure 创建业务级故障结果（返回给 LLM）。
func Failure(code, message string) *Result {
	return &Result{Success: false, Error: &Error{Code: code, Message: message}}
}

// ToolInfo 从工具提取 schema.ToolInfo（用于传递给 LLM）。
func ToolInfo(t Tool) *schema.ToolInfo {
	return &schema.ToolInfo{
		Name:   t.Name(),
		Desc:   t.Description(),
		Params: t.InputSchema(),
	}
}

// --- 工具包 ---

// Toolkit 是工具的注册表。List / ToolInfos 按注册顺序列出；同名重新注册替换实例、保持原位。
type Toolkit struct {
	mu    sync.RWMutex
	tools map[string]Tool
	// order 是注册顺序。按 map 遍历列出的话顺序每次随机：发给模型的工具段跟着变，而 provider 的 prompt
	// cache 按前缀命中（工具段在消息之前），每一轮都命不中。
	order []string
}

// NewToolkit 创建一个空工具包。
func NewToolkit() *Toolkit {
	return &Toolkit{tools: make(map[string]Tool)}
}

// Register 将工具添加到工具包中。
//
// **audit⁷ HIGH-4 fix**: isToolNil 检 typed nil tool (协议七复证). 业务
// `var t *MyTool; tk.Register(t)` typed nil → t.Name() 立即 nil deref panic 难定位.
func (tk *Toolkit) Register(t Tool) {
	if isToolNil(t) {
		panic("Toolkit.Register: nil tool (含 Go typed nil interface 陷阱)")
	}
	tk.mu.Lock()
	defer tk.mu.Unlock()
	name := t.Name()
	if _, ok := tk.tools[name]; !ok {
		tk.order = append(tk.order, name)
	}
	tk.tools[name] = t
}

// RegisterAll 添加多个工具。
func (tk *Toolkit) RegisterAll(tools ...Tool) {
	for _, t := range tools {
		tk.Register(t)
	}
}

// Get 按名称返回工具，如果未找到则返回 nil。
func (tk *Toolkit) Get(name string) Tool {
	tk.mu.RLock()
	defer tk.mu.RUnlock()
	return tk.tools[name]
}

// List 按注册顺序返回所有已注册的工具。
func (tk *Toolkit) List() []Tool {
	tk.mu.RLock()
	defer tk.mu.RUnlock()
	out := make([]Tool, 0, len(tk.order))
	for _, name := range tk.order {
		out = append(out, tk.tools[name])
	}
	return out
}

// ToolInfos 按注册顺序为所有已注册的工具返回 schema.ToolInfo。
func (tk *Toolkit) ToolInfos() []*schema.ToolInfo {
	tools := tk.List()
	infos := make([]*schema.ToolInfo, 0, len(tools))
	for _, t := range tools {
		infos = append(infos, ToolInfo(t))
	}
	return infos
}

// Call 按名称调用工具。
func (tk *Toolkit) Call(ctx context.Context, name string, arguments map[string]any) (*Result, error) {
	t := tk.Get(name)
	if t == nil {
		return &Result{
			Success: false,
			Error:   &Error{Code: ErrCodeNotFound, Message: fmt.Sprintf("工具 %q 未找到", name)},
		}, nil
	}
	return t.Call(ctx, arguments)
}

// StreamCall 按名称以流式方式调用工具。
// 若工具未实现 StreamableTool，回退到 Call 并包装为单 chunk StreamReader。
func (tk *Toolkit) StreamCall(ctx context.Context, name string, arguments map[string]any) (*schema.StreamReader[*Result], error) {
	t := tk.Get(name)
	if t == nil {
		return nil, fmt.Errorf("toolkit: tool %q not found", name)
	}
	if st, ok := t.(StreamableTool); ok {
		return st.StreamCall(ctx, arguments)
	}
	// 降级：执行同步调用，包装为单 chunk 流
	result, err := t.Call(ctx, arguments)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(result), nil
}

// --- FuncTool：简单的基于函数的工具 ---

// FuncTool 将普通函数包装为工具。
type FuncTool struct {
	name        string
	description string
	inputSchema map[string]any
	fn          func(ctx context.Context, args map[string]any) (*Result, error)
}

// NewFuncTool 从函数创建工具。
func NewFuncTool(name, description string, inputSchema map[string]any, fn func(ctx context.Context, args map[string]any) (*Result, error)) *FuncTool {
	// audit¹⁷ HIGH-1 fix: nil fn 检 (协议十七复证). 业务 `var fn FuncType; NewFuncTool(...)`
	// typed nil func → Call 时 fn(...) nil deref panic 难定位.
	if fn == nil {
		panic("NewFuncTool: nil fn — tool 必须有可调用实现")
	}
	return &FuncTool{
		name:        name,
		description: description,
		inputSchema: inputSchema,
		fn:          fn,
	}
}

func (t *FuncTool) Name() string                { return t.name }
func (t *FuncTool) Description() string         { return t.description }
func (t *FuncTool) InputSchema() map[string]any { return t.inputSchema }
func (t *FuncTool) Call(ctx context.Context, args map[string]any) (*Result, error) {
	return t.fn(ctx, args)
}
