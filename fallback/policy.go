package fallback

import (
	"context"
	"errors"
	"strings"

	"crew-go/compose_v2"
	"crew-go/model"
)

// FailoverPolicy 判断一个错误是否应触发切到下一个候选 model。
// 接口定义在使用方（本包），业务可自定义并经 WithFailoverPolicy 注入。
//
// 控制流信号（InterruptError / ctx 取消）无需分类——框架层 guardedPolicy 在本
// 接口之前无条件豁免，自定义实现只需关心"哪些真实 API 错误换候选有用"。
//
// **返回 false 只表示不换候选，不表示不重试**：在当前候选上要不要重试由错误本身决定（见 [PerModelRetry]
// 的「重试哪些错」）——几个候选共用一个后端时「503 不换」仍该在原候选上重试。业务自己的错误不该重试时实现
// [compose_v2.NonRetryable]。它也决定要等很久的候选能不能先推迟、去试别的候选（见 [WithPerModelRetry]）。
type FailoverPolicy interface {
	// ShouldFailover 返回 true 表示"换下一个候选可能有用"。
	ShouldFailover(ctx context.Context, err error) bool
}

// guardedPolicy 框架层控制流豁免——包装业务 policy，ShouldFailover 先无条件排除
// 「再做一次没有可能有用」的错（compose_v2.IsRetryFutile：三个框架自铸哨兵按 error 判，
// ctx 那档问调用方的 ctx.Err()）再委托业务分类。
//
// **为什么在框架层而不是留给业务 policy**：interrupt-is-control-flow 是框架内部
// 协议知识——业务写自定义 policy 时想的是"哪些 HTTP 错误该切换"，不该要求它知道
// InterruptError 的存在。漏写豁免的后果是静默吞中断：interrupt 被误判"该切" →
// 换候选重跑成功 → HITL gate 被绕过、调用方拿到 err=nil 完全无感，且断路器被误计。
// New() 构造期包装一次（而非在各决策点前置检查），读 policy 的切换判断（切换点、推迟点、未来新增）
// 自动被 guard，不依赖逐点纪律。重试判断不读 policy：shouldRetryMember 自己先问 IsRetryFutile。
//
// 锁死的配置空间全是语义错误的配置——"对 interrupt 换模型重跑"（暂停等人的信号）与
// "对已死 ctx 切换"（立即再失败）没有合理场景，业务自定义自由度无损。
type guardedPolicy struct{ inner FailoverPolicy }

// ShouldFailover 实现 FailoverPolicy——先豁免控制流，再委托业务 policy。
func (g guardedPolicy) ShouldFailover(ctx context.Context, err error) bool {
	// **用 IsRetryFutile 不用 IsControlFlowSignal**：这一层回答的是「换个候选有没有可能
	// 有用」，而 `IsControlFlowSignal` 回答的是「错误链上有没有 ctx 错」。两者只在 ctx
	// 那一档分叉，而那一档正是承重的——`http.Client{Timeout}`（标准配法）产出的错匹配
	// `DeadlineExceeded`，而 caller 的 ctx 还活着。混用的实测后果：**provider 变慢时
	// fallback 永不切换**，正是它存在的理由那个场景。
	if compose_v2.IsRetryFutile(ctx, err) {
		return false
	}
	return g.inner.ShouldFailover(ctx, err)
}

// DefaultFailoverPolicy 按错误结构分类（对齐 model-fallback.md §4.3）。
//
// 分类顺序（短路）：
//  1. 控制流信号 → 不切——InterruptError（HITL 挂起，切了吞中断）/ context.Canceled /
//     context.DeadlineExceeded（ctx 已取消，切了立即再失败）。注意网络层 ctx 取消会被
//     包成 *model.APIError{StatusCode:0}，errors.Is 经 Unwrap 仍能识别，故必须在判
//     StatusCode 之前先判 ctx。
//  2. provider 在响应里报的上下文超限、内容过滤（model.ErrContextWindowExceeded /
//     model.ErrContentFiltered）→ 不切，跟 HTTP 400 报的同一类错误同一个答案。它们出现在
//     建流成功之后，被包成 StatusCode 为 0 的 *model.APIError，按状态码会被当成网络错误。
//  3. *model.APIError → 按 StatusCode：0（网络/超时/解析，接入点不可达）/ 408（请求超时）/
//     429（限流）/ 5xx（provider 故障）→ 切；其他 4xx（400 参数 / 401 鉴权 / 404 模型不
//     存在 / 上下文超限）→ 不切（换模型同样失败；上下文超限交第 3 层 ReactiveCompact 压缩）。
//  4. 未知错误 → 保守切换（最后一个候选失败仍抛真实错误，不吞）。
type DefaultFailoverPolicy struct{}

// ShouldFailover 实现 FailoverPolicy。
func (DefaultFailoverPolicy) ShouldFailover(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	// 1. 再做一次没有可能有用——不切（单一真相源 compose_v2.IsRetryFutile）。框架层
	// guardedPolicy 已在本 policy 之前兜底同一判断——此分支是冗余确认，保留让
	// DefaultFailoverPolicy 作为导出分类器自身答案正确（业务直接调用 / 参考实现
	// 自定义 policy 时不被误导）。
	if compose_v2.IsRetryFutile(ctx, err) {
		return false
	}
	// 2. 输入或输出本身的问题——在状态码之前判：这两个哨兵包在 StatusCode 为 0 的 APIError 里，
	// 按状态码会被当成网络错误，每个候选逐个重试再切换（实测每个候选请求 3 次），而同样的问题经
	// HTTP 400 报上来时一次都不重试、不切。
	if errors.Is(err, model.ErrContextWindowExceeded) || errors.Is(err, model.ErrContentFiltered) {
		return false
	}
	// 3. 结构化 API 错误——按状态码
	var apiErr *model.APIError
	if errors.As(err, &apiErr) {
		switch {
		case apiErr.StatusCode == 0:
			return true // 网络 / 超时 / 解析——接入点不可达，换 provider 有用
		case apiErr.StatusCode == 408 || apiErr.StatusCode == 429:
			return true // 请求超时 / 限流——换 provider 有用
		case apiErr.StatusCode >= 400 && apiErr.StatusCode < 500:
			// 额度/配额耗尽类先切——这个 model/账号暂不可用，换候选有用。部分 provider 用
			// 非 429 状态码表达额度耗尽（dashscope 403 AllocationQuota.FreeTierOnly），故按
			// body 关键词识别而非只看 StatusCode。区别于 400 参数 / 401 鉴权 / 404 模型不存在
			// 这些"换了也白搭"的 4xx。
			if isQuotaExhausted(apiErr) {
				return true
			}
			return false // 其他 4xx——请求本身有问题，换模型同样失败
		case apiErr.StatusCode >= 500:
			return true // provider 故障
		default:
			return true // 非预期状态码，保守切换
		}
	}
	// 4. 未知错误——保守切换
	return true
}

// shouldRetryMember 回答「在同一个候选上再试一次有没有可能好」：只重试会自己恢复的错误——5xx、429、408、
// 网络错误（StatusCode 为 0）与不认识的错误；计费类额度用完（model.ErrBillingExhausted，等多久都不会恢复，它实现
// compose_v2.NonRetryable、下面那道闸就认）、其余 4xx
// （请求本身有问题）、声明了 [compose_v2.NonRetryable] 的错误（上下文超限、内容过滤等）、再做一次不可能有用的
// （[compose_v2.IsRetryFutile]）不重试。跟 [FailoverPolicy]（换下一个候选有没有可能好）是两个问题，见
// callMemberWithRetry。
//
// 按时间窗口的配额（「每分钟请求数超了」）是限流、等一会儿就恢复，照样重试：切换判据的额度关键词（isQuotaExhausted）
// 不能借来用——它是为「换候选有没有用」挑的，误报无害；拿来判「等多久都不会恢复」，Google 的 "Quota exceeded for
// ... per_minute" 这类就不再等 Retry-After 重试，唯一或最后一个候选上直接失败（审计实测）。
//
// 不做成参数：[PerModelRetry] 不带判据字段，理由见它的 godoc。业务自己的错误类型不该重试时实现
// [compose_v2.NonRetryable]，在错误的定义处声明。
func shouldRetryMember(ctx context.Context, err error) bool {
	if compose_v2.IsRetryFutile(ctx, err) {
		return false
	}
	var nr compose_v2.NonRetryable
	if errors.As(err, &nr) {
		return false
	}
	var apiErr *model.APIError
	if !errors.As(err, &apiErr) {
		return true // 不认识的错误：跟以前一样重试
	}
	switch {
	case apiErr.StatusCode == 0, apiErr.StatusCode >= 500:
		return true
	case apiErr.StatusCode == 408, apiErr.StatusCode == 429:
		return true
	default:
		return false
	}
}

// isQuotaExhausted 判「额度/配额」类错误——这类换候选 model/provider 有用（切换判据用）。按响应体识别而非只看
// StatusCode：不同 provider 用不同状态码表达额度耗尽（dashscope 403 "AllocationQuota.FreeTierOnly" / OpenAI 429
// "insufficient_quota" / 余额不足类），且都区别于 400 参数 / 401 鉴权 / 404 模型不存在这些"换模型也白搭"的 4xx（那些
// body 不含额度关键词）。包括计费类额度用完（model.ErrBillingExhausted）与按时间窗口的配额（会自己恢复，但换个候选不用等）。
func isQuotaExhausted(apiErr *model.APIError) bool {
	if errors.Is(apiErr, model.ErrBillingExhausted) {
		return true
	}
	b := strings.ToLower(apiErr.Body)
	// anchor 到具体短语而非 bare "quota"——避免误命中 "quota_limit_override" 参数错等无关 4xx
	// （那些 body 含 "quota" 但换模型也白搭，cross-verify CLAIM 3）
	return strings.Contains(b, "quota exceeded") || // 按时间窗口的配额（Google "Quota exceeded for ... per_minute"）
		strings.Contains(b, "allocationquota") || // dashscope 的 AllocationQuota 类（429 Throttling.AllocationQuota 是限流）
		strings.Contains(b, "free tier") // 免费额度相关（有的是按时间窗口的限流）
}
