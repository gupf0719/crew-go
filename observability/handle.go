package observability

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"crew-go/compose_v2"
)

type handleKind int

const (
	kindAgent handleKind = iota + 1
	kindGraph
	kindNode
	kindModel
	kindTool
)

// handle 是本包放进 ctx 的一个 span 句柄。开始一侧的钩子建好放进返回的 ctx，结束一侧从 ctx 取回。
type handle struct {
	kind    handleKind
	span    trace.Span
	start   time.Time
	name    string // agent 名 / 节点 ID / 工具名
	graphNS string
	scope   *agentScope // 所在 agent 调用；agent 句柄指向自己的 scope

	// agent
	agentKind string
	workflow  bool // invoke_workflow（编排）而不是 invoke_agent

	// model
	requestModel  string
	inputModality string       // 输入 token 的 gen_ai.token.modality（见 tokenModality）
	firstChunk    atomic.Int64 // 流式首片到达时间（UnixNano），0 = 还没到

	// tool
	callID   string
	category string
	skill    string // activate_skill 调用激活的 skill

	// node：流式片段统计
	chunks     atomic.Int64
	firstNano  atomic.Int64
	partialBuf *boundedBuffer

	ended atomic.Bool
}

// agentScope 是一次 agent 调用的范围：记它下面还开着的模型 / 工具 span（错误路径上框架不调结束钩子时
// 由 OnAgentEnd 收尾），以及这次调用里调了几次模型、几次工具（gen_ai.invoke_agent.* 指标）。
type agentScope struct {
	mu        sync.Mutex
	open      map[*handle]struct{}
	inference atomic.Int64
	toolCalls atomic.Int64
	lastModel atomic.Value // string：最近一次模型调用请求的模型名（不知道请求模型时不记）
}

func (s *agentScope) add(h *handle) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.open == nil {
		s.open = make(map[*handle]struct{})
	}
	s.open[h] = struct{}{}
	s.mu.Unlock()
}

func (s *agentScope) remove(h *handle) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.open, h)
	s.mu.Unlock()
}

// drain 取走所有还开着的句柄（只取指定种类；kind 为 0 时全取）。
func (s *agentScope) drain(kind handleKind) []*handle {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*handle
	for h := range s.open {
		if kind == 0 || h.kind == kind {
			out = append(out, h)
			delete(s.open, h)
		}
	}
	return out
}

// findTool 按 tool_call ID 找还开着的工具句柄（OnToolEnd 拿不到自己那份 ctx 时的退路）。
func (s *agentScope) findTool(callID string) *handle {
	if s == nil || callID == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for h := range s.open {
		if h.kind == kindTool && h.callID == callID {
			return h
		}
	}
	return nil
}

type handleKey struct{}      // 最内层的句柄
type agentHandleKey struct{} // 最内层的 agent 句柄

func withHandle(ctx context.Context, h *handle) context.Context {
	ctx = context.WithValue(ctx, handleKey{}, h)
	if h.kind == kindAgent {
		ctx = context.WithValue(ctx, agentHandleKey{}, h)
	}
	return ctx
}

func handleFrom(ctx context.Context) *handle {
	h, _ := ctx.Value(handleKey{}).(*handle)
	return h
}

func agentHandleFrom(ctx context.Context) *handle {
	h, _ := ctx.Value(agentHandleKey{}).(*handle)
	return h
}

func scopeFrom(ctx context.Context) *agentScope {
	if h := agentHandleFrom(ctx); h != nil {
		return h.scope
	}
	return nil
}

// result 是一次执行的结局：outcome 进 crew.outcome；errType / errMsg 只在 error / timeout 时有值。
type result struct {
	outcome string
	errType string
	errMsg  string
}

func (r result) failed() bool { return r.outcome == OutcomeError || r.outcome == OutcomeTimeout }

// classify 把一次执行的错误归成结局。ctx 已结束时按 ctx 判（超时是失败，取消不是）；中断是等人答复的挂起；
// 软取消是业务主动喊停，跟 ctx 取消同档——它不动 ctx，不单列的话「点停止」会被计成错误。
//
// 取消只认 ctx 与软取消：调用方的 ctx 还活着、错误链上却有 context.Canceled（errgroup 里被连带取消的兄弟、
// 业务自己派生又取消的子 ctx），这次执行是失败了，记成 cancelled 会把失败藏起来。错误链上的
// DeadlineExceeded 照样记 timeout——它也是失败，只是换个类型。
func classify(ctx context.Context, err error) result {
	if ctx != nil && ctx.Err() != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return result{outcome: OutcomeTimeout, errType: "timeout", errMsg: errMsgOf(err, ctx.Err())}
		}
		return result{outcome: OutcomeCancelled}
	}
	if err == nil {
		return result{outcome: OutcomeSuccess}
	}
	if errors.Is(err, compose_v2.ErrCheckpointStale) {
		return result{outcome: OutcomeError, errType: "checkpoint_stale", errMsg: err.Error()}
	}
	if _, ok := compose_v2.IsInterrupt(err); ok && !errors.Is(err, compose_v2.ErrCheckpointNotPersisted) {
		return result{outcome: OutcomeBlocked}
	}
	if errors.Is(err, compose_v2.ErrSoftCanceled) {
		return result{outcome: OutcomeCancelled}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return result{outcome: OutcomeTimeout, errType: "timeout", errMsg: err.Error()}
	}
	return result{outcome: OutcomeError, errType: errorTypeOf(err), errMsg: err.Error()}
}

func errMsgOf(errs ...error) string {
	for _, e := range errs {
		if e != nil {
			return e.Error()
		}
	}
	return ""
}

// errorTypeOf 给 error.type 一个低基数的值：框架认得的错误用固定名，其余统一 _OTHER（GenAI 约定的兜底值）。
func errorTypeOf(err error) string {
	switch {
	case errors.Is(err, compose_v2.ErrRunAborted):
		return "run_aborted"
	case errors.Is(err, compose_v2.ErrCheckpointNotPersisted):
		return "checkpoint_not_persisted"
	case errors.Is(err, compose_v2.ErrGraphIDConflict):
		return "graph_id_conflict"
	case errors.Is(err, compose_v2.ErrNoPendingCheckpoint):
		return "no_pending_checkpoint"
	}
	var mi *compose_v2.MaxIterationsError
	if errors.As(err, &mi) {
		return "max_iterations"
	}
	return "_OTHER"
}

// maxErrChars 是写进 span 的错误信息上限（按字符）。
const maxErrChars = 1024

// errText 是错误信息写进 span 之前的处理：先脱敏再截断——反过来的话，跨在截断点上的敏感片段会被截成
// 半段、正则匹配不上，前半段原样漏出去。
func (o *Observability) errText(s string) string {
	return truncate(o.redactString(s), maxErrChars)
}

// finish 写结局、收尾 span。只有第一次调用生效（OnError 与结束钩子可能都想收尾同一个 span）。
func (o *Observability) finish(h *handle, r result, extra ...attribute.KeyValue) bool {
	if h == nil || !h.ended.CompareAndSwap(false, true) {
		return false
	}
	attrs := append(extra, AttrOutcome.String(r.outcome))
	if r.failed() {
		attrs = append(attrs, AttrErrorType.String(r.errType))
		msg := o.errText(r.errMsg)
		h.span.SetStatus(codes.Error, msg)
		if msg != "" {
			h.span.AddEvent("exception", trace.WithAttributes(
				attribute.String("exception.type", r.errType),
				attribute.String("exception.message", msg)))
		}
	}
	h.span.SetAttributes(attrs...)
	h.span.End()
	return true
}

func seconds(since time.Time) float64 { return time.Since(since).Seconds() }
