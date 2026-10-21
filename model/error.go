package model

import (
	"errors"
	"fmt"
	"time"
)

// ErrStreamUnsupported 是 [ChatModel.Stream] 用来声明「这个实现不支持流式」的哨兵。
//
// BasicAgent 只在 Stream 返回它（或返回 nil reader 且没有错误）时改调 Generate；Stream 返回
// 的其他错误——限流、5xx、网络错误——原样交给上层的重试与故障切换。任何错误都改调 Generate
// 的话，一次 429 会立刻再打一遍、不看 Retry-After，配了 fallback 时整条候选链会再走一遍。
var ErrStreamUnsupported = errors.New("model: streaming not supported")

// ErrContentFiltered 表示 provider 的内容过滤截掉了输出（结束原因是 content_filter，流式与 Generate 都是）。
//
// 已经收到的那部分输出不完整，不能当成最终答复。它不可重试（实现 compose_v2.NonRetryable）：
// 同一个模型对同一份输入多半还会被过滤。fallback 的默认策略对它不切换，跟 HTTP 400 报的
// 内容违规一样；要换候选试试，自定义 fallback.FailoverPolicy 用 errors.Is 认它。
var ErrContentFiltered error = contentFilteredError{}

// contentFilteredError 是 [ErrContentFiltered] 的类型——用值类型的结构体，errors.Is 按值比较即可认出。
type contentFilteredError struct{}

func (contentFilteredError) Error() string { return "model: output blocked by content filter" }

// NonRetryable 实现 compose_v2.NonRetryable：重试同一个模型多半还会被过滤，白花一次调用。
func (contentFilteredError) NonRetryable() {}

// ErrContextWindowExceeded 表示 provider 在响应里报告输入超出了模型的上下文窗口（结束原因是
// model_context_window_exceeded 这类取值，流式与 Generate 都是）。
//
// 不可重试（实现 compose_v2.NonRetryable）：同一份输入发给同一个模型必然一样。错误文本含
// context length exceeded，上下文压缩的第 3 层（agent_v2.IsPromptTooLongError）据此认出来、
// 压缩之后重试。fallback 的默认策略对它不切换，跟 HTTP 400 报的上下文超限一样。
var ErrContextWindowExceeded error = contextWindowExceededError{}

// contextWindowExceededError 是 [ErrContextWindowExceeded] 的类型。
type contextWindowExceededError struct{}

func (contextWindowExceededError) Error() string { return "model: context length exceeded" }

// NonRetryable 实现 compose_v2.NonRetryable：同一份输入重试必然一样。
func (contextWindowExceededError) NonRetryable() {}

// APIError 是 provider HTTP 调用的结构化错误，让上层（如故障切换装饰器
// FallbackChatModel）能按 StatusCode 分类，而不必正则解析错误字符串（脆弱、易误判）。
//
// 约定：
//   - StatusCode != 0：provider 返回了 HTTP 错误响应，Body 持响应体
//   - StatusCode == 0：非 HTTP 层错误（网络中断 / 超时 / 响应读取或解析失败），
//     Err 持底层错误，可经 errors.Is/As 进一步判定（如 context.Canceled）
//
// Error() 会把 Body / Err 拼进字符串——既有的基于错误文本的判定（如 ReactiveCompact
// 的 prompt-too-long 检测）因此保持有效；不要让 Error() 丢掉 Body。
type APIError struct {
	// Provider 是 provider 标识（"openai" / "qwen" / "deepseek" 等），取自 client
	// 的 system 配置；未配置时由 client 填默认值。
	Provider string
	// StatusCode 是 HTTP 状态码；0 表示非 HTTP 错误。
	StatusCode int
	// Body 是 HTTP 错误响应体（StatusCode != 0 时填充）。
	Body string
	// Err 是底层错误（网络层 / IO 层 / 解析层），StatusCode == 0 时填充，可为 nil。
	Err error

	// RetryAfter 是服务端 `Retry-After` 响应头解析出的建议等待时长；0 表示服务端
	// 没给（或给的是过去时间 / 无法解析）。典型出现在 429 与部分 503。
	//
	// 重试层（`compose_v2.RunWithRetry` / fallback per-member 重试）**优先按它退避**
	// ——服务端明说等多久时，我们猜的指数退避只会要么太早（再吃一次 429、还可能延长
	// 封禁）要么太晚（白等）。
	RetryAfter time.Duration
}

// HTTPStatus 实现 `compose_v2.HTTPStatusProvider`——让重试层免 import model 即可按
// 状态码分类（400/401/403/404/422 判确定性失败不重试、429 走独立限流上限）。
func (e *APIError) HTTPStatus() int {
	if e == nil {
		return 0
	}
	return e.StatusCode
}

// RetryAfterDelay 实现 `compose_v2.RetryAfterProvider`。
//
// 用可选接口而非让 compose_v2 import model：retry 层按「接口定义在使用方」识别，
// 业务自定义 error 类型实现同一方法即可接管退避时长。
func (e *APIError) RetryAfterDelay() time.Duration {
	if e == nil {
		return 0
	}
	return e.RetryAfter
}

// Error 实现 error 接口。HTTP 错误保留 "<provider>: HTTP <code>: <body>" 格式
// （与历史错误串一致，不破坏依赖文本匹配的上层逻辑，如 IsPromptTooLongError）。
func (e *APIError) Error() string {
	provider := e.Provider
	if provider == "" {
		provider = "model"
	}
	if e.StatusCode != 0 {
		return fmt.Sprintf("%s: HTTP %d: %s", provider, e.StatusCode, e.Body)
	}
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", provider, e.Err)
	}
	return provider + ": api error"
}

// Unwrap 暴露底层错误，让 errors.Is/As 穿透到网络层错误（如 context.Canceled），
// 故障切换层据此把控制流信号判为"不该切换"。
func (e *APIError) Unwrap() error { return e.Err }
