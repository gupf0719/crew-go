package qwen

import (
	"os"

	"crew-go/model"
	"crew-go/model/openai"
)

const (
	// 阿里云 dashscope 官方 compatible-mode endpoint。
	baseURL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
	// NewClient 留空模型名时的默认。
	defaultModel = "qwen3.6-plus"
)

// testDefaultModel 是 NewTestClient 留空模型名时的默认——**取候选首位**，不写死
// 字面量：模型免费额度会耗尽，写死的那个耗尽后所有 `NewTestClient("")` 一起 403
// （曾发生：整套 tutorial integration 测试全挂）。改这里只需调整 TestModelCandidates。
var testDefaultModel = TestModelCandidates[0]

// TestModelCandidates 是 dashscope 测试端可用的模型候选，**按推荐顺序**排列。
//
// # 为什么带日期的排在前面
//
// 带日期的是**固定快照**（`qwen3.7-max-2026-06-08`）——同一个名字永远是同一个模型，
// 测试行为可复现；不带日期的是**滚动别名**（`qwen3.7-max`），阿里侧会把它指向新版本，
// 今天跑通的用例明天可能因模型换代而行为漂移。测试要的是确定性，所以固定快照优先，
// 滚动别名只作兜底。
//
// # 怎么用
//
// 单个模型直接 `NewTestClient(TestModelCandidates[0])`；想让额度耗尽时自动切下一个，
// 用 `fallback` 包把它们串起来（它认得 dashscope 403 响应体里的 "AllocationQuota.FreeTierOnly" 错误码
// 并会切换，见 `fallback/policy.go` 的 isQuotaExhausted）：
//
//	models := make([]model.ChatModel, 0, len(qwen.TestModelCandidates))
//	for _, name := range qwen.TestModelCandidates {
//	    models = append(models, qwen.NewTestClient(name))
//	}
//	m := fallback.New(models)
//
// 本包不直接提供该 helper —— `fallback` 依赖 `compose_v2`，model 层 import 它会破坏
// 分层（schema ← model ← compose_v2）。组装留在 example / 测试侧。
//
// **入列前必须逐个探活**——不能只看控制台"这个模型有额度"。两类会被剔除：
//
//   - **额度耗尽**：403 `AllocationQuota.FreeTierOnly`。上一批候选（`qwen3.7-max*` /
//     `qwen3.7-plus*`）五个全挂在这上面，曾让整套 tutorial integration 测试全 403
//   - **与 preset 参数不兼容**：强制要求 `enable_thinking=true`，而 `presetDashscope`
//     写死 `false` → 经 `NewTestClient` **恒 400**，不是额度问题、加额度也没用
//
// **不兼容的比耗尽的更毒**：`fallback.DefaultFailoverPolicy` 只对额度类 4xx 切换
// （`policy.go` 的 isQuotaExhausted），普通 400 直接抛——链走到这种候选会**硬停**，
// 后面的健康候选一个都够不着。放一个进列表等于把链在那里截断。
//
// ⚠️ **这一条最近一次就抓到了**（2026-09-03 换代时逐个探活）：`qwen3.8-2.4t-a95b`
// 控制台显示额度满格 100 万，实调 400 `InternalError.Algo.InvalidParameter: The value
// of the enable_thinking parameter is restricted` —— **照控制台抄会把链截断在第 4 位**。
//
// # 排序：flash 档在前，同档内固定快照优先
//
// 降级链首先要保证「还能跑」，其次是「别跳档烧额度」——所以三个 flash 排在前面，
// 中/大档只在 flash 全挂时才够得着。同档内固定快照在前（可复现），滚动别名兜底。
//
// 后续某个候选耗尽/失效时从列表里挪走即可——**别在 example 里写死模型名**。
// `testDefaultModel = TestModelCandidates[0]`，所以改这一个列表，`NewTestClient("")`
// 全仓自动跟上。
//
// 2026-09-03 全部逐个探活通过（`say OK` 单轮，耗时见注）。同批探活可用但**不入列**的：
// `deepseek-v4-flash-0731` / `deepseek-v4-pro-0813` / `glm-5.2` / `kimi-k3` /
// `kimi-k2.7-code` —— dashscope 同一个链接也服务它们、额度各自独立，但本包的
// `presetDashscope` 把 `gen_ai.system` 标成 `dashscope`、参数按千问调（`enable_thinking`），
// 拿它跑别家模型会让观测面失真。千问候选真的全挂时，它们是现成的应急池。
var TestModelCandidates = []string{
	// flash 档（首选：快、便宜）——固定快照在前
	"qwen3.7-flash-2026-07-15", // 快照 · 332ms · 额度满
	"qwen3.8-flash",            // 滚动 · 1.07s · 额度满
	"qwen3.7-flash",            // 滚动 · 248ms · 余量约 90%
	// 中/大档兜底（flash 全挂时才用，贵且慢）——固定快照在前
	"qwen3.8-max-0902", // 快照 · 486ms · 额度满
	"qwen3.8-27b",      // 滚动 · 367ms · 额度满
	"qwen3.8-max",      // 滚动 · 836ms · 额度满
}

// qwenCap 千问结构化输出能力画像：不支持 json_schema strict（dashscope qwen3 官方未确认、
// flash 大概率无），走 json_object + 覆盖最广的 forced tool（ModeTool）。qwen-3-coder 等
// 不支持 tool_choice=required 的变体需业务 openai.WithStructuredCapability 覆盖。
var qwenCap = model.NonStrictToolCapability()

// presetClient 是 NewClient 的千问配置（阿里云 dashscope 官方 compatible-mode）。千问兼容
// OpenAI API，复用 openai.Client。thinking 不显式设置、随模型默认；要显式开关用 WithEnableThinking。
var presetClient = openai.Preset{
	System:               "dashscope",
	BaseURL:              baseURL,
	DefaultModel:         defaultModel,
	StructuredCapability: &qwenCap,
}

// presetDashscope 测试千问（同一个 dashscope compatible-mode）。默认关 thinking
// （见 NewTestClient / WithEnableThinking 说明）。
var presetDashscope = openai.Preset{
	System:               "dashscope",
	BaseURL:              baseURL,
	DefaultModel:         testDefaultModel,
	ExtraBody:            map[string]any{"enable_thinking": false},
	StructuredCapability: &qwenCap,
}

// NewClient 创建千问 ChatModel（阿里云 dashscope 官方 compatible-mode endpoint）。千问兼容
// OpenAI API，直接复用 openai.Client。
//
// apiKey 由业务管理（test/example 默认走本包 NewTestClient——读 QWEN_API_KEY）。
// modelName 留空时使用 qwen3.6-plus（旧版 qwen-max-latest 已下线）。
func NewClient(apiKey string, modelName string, opts ...openai.ClientOption) model.ChatModel {
	return openai.NewCompatible(presetClient, apiKey, modelName, opts...)
}

// WithEnableThinking 控制 dashscope qwen3 的思考模式（compatible-mode 顶层参数
// `enable_thinking`）——openai.WithExtraBody 的便利包装。
//
// **为什么需要关**：dashscope qwen3 thinking 模式**不支持 `tool_choice=required/具名`**
// （HTTP 400 InvalidParameter）——而框架的结构化输出路径强制 tool_choice（如
// `agent_v2.NewLLMPlanner` 防裸文本绕过 tool 约束）。thinking 开着时 PlanAgent 等
// 直接 400。
func WithEnableThinking(enabled bool) openai.ClientOption {
	return openai.WithExtraBody(map[string]any{"enable_thinking": enabled})
}

// WithEnableThinkingOption 是 WithEnableThinking 的 per-request 版本（返 model.Option）——
// 业务按次控制 dashscope qwen3 thinking（页面开关场景），配合 agent_v2.WithRunModelOptions
// 透传到本次 Run，无需为切换重建 client。
//
// 注意 thinking 与 forced tool_choice 互斥（见 WithEnableThinking）——本次 Run 若走
// LLMPlanner 等强制 tool_choice 的路径，别按次开 thinking。构造期默认关；本 option 按次覆盖。
func WithEnableThinkingOption(enabled bool) model.Option {
	return model.WithExtraBody(map[string]any{"enable_thinking": enabled})
}

// NewTestClient 创建测试千问 ChatModel — 与 NewClient 是同一个 dashscope compatible-mode
// endpoint，区别在于 key 读环境变量、默认关 thinking、留空模型名取 TestModelCandidates 首位。
//
// **key 从环境变量 `QWEN_API_KEY` 读**（不 hardcode 进源码，走项目根 .env，
// 跟 DEEPSEEK_API_KEY 同款，.gitignore 已排除不入仓库）。caller 需先
// `envloader.Load()`（example/test main 第一行）或 `export QWEN_API_KEY=...`
// 让 os.Getenv 拿到 key；key 空时返的 client 调用会 401，caller 自查。
//
// modelName 留空用 TestModelCandidates 首位。**默认关 thinking**（qwen3 thinking 默认开，但
// thinking 与 forced tool_choice 互斥——见 WithEnableThinking；测试 client 必须兼容
// LLMPlanner 等全部框架能力，且关 thinking 更快更省）。要 thinking 的 example 传
// `qwen.WithEnableThinking(true)` 覆盖（extraBody 后值覆盖前值）。
func NewTestClient(modelName string, opts ...openai.ClientOption) model.ChatModel {
	return openai.NewCompatible(presetDashscope, os.Getenv("QWEN_API_KEY"), modelName, opts...)
}
