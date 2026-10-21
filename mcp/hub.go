package mcp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"crew-go/internal/panicerr"
	"crew-go/tool"
)

// Hub 管理多个 MCP Client，并行拉起后聚合成统一的 *tool.Toolkit。
// 设计上是"一次启动、多处复用"：Start 触发握手+工具发现，
// Toolkit() 返回可直接传给 agent 的工具注册表，Close 并行释放所有 Client。
type Hub struct {
	clients []Client
	toolkit *tool.Toolkit

	mu      sync.Mutex
	started bool
	tools   []*RemoteTool
}

// HubOption 配置 Hub。
type HubOption func(*Hub)

// WithClient 向 Hub 注册一个 Client。可重复调用以加入多个 Server。
//
// **audit¹⁸ HIGH-1 fix**: isClientNil 检 typed nil Client (协议十八复证).
// 业务 `var c *MyClient; WithClient(c)` typed nil → Hub.ListTools 时 c.ListTools nil deref.
func WithClient(c Client) HubOption {
	return func(h *Hub) {
		if isClientNil(c) {
			panic("WithClient: nil Client (含 Go typed nil interface 陷阱)")
		}
		h.clients = append(h.clients, c)
	}
}

// NewHub 构造一个空 Hub，使用 With* 选项注入 Client。
func NewHub(opts ...HubOption) *Hub {
	h := &Hub{toolkit: tool.NewToolkit()}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Start 并发对所有 Client 执行 Initialize + ListTools，并把远端工具注册进内部 Toolkit。
// 任一 Client 失败则整体失败，已初始化的 Client 会被 Close 回滚。
// 重复调用是幂等的（仅首次生效）。
func (h *Hub) Start(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.started {
		return nil
	}
	// **F3 修复**：全程持 mu 让并发 Start 串行——第二个阻塞到第一个完成、见 started=true 返回
	// nil（此时 register 已完成）。原 check(顶部) 与 set(末尾) 分两个临界区，并发 Start 都过
	// started==false 门 → 重复 ListTools(冗余 RPC) + 重复 append h.tools + 重复 Register。
	// 不用"乐观置位于首个临界区"——那样第二个 Start 见 true 提前 return 时第一个尚在 register，
	// caller 拿到未 register 完的空 Toolkit。doStart 的 goroutine 只写局部 results 不碰 h.mu、
	// register 在锁内故安全持 mu；失败不置 started(保持 false)允许重试。
	type result struct {
		client Client
		tools  []ToolSpec
		err    error
	}
	results := make([]result, len(h.clients))
	var wg sync.WaitGroup
	for i, c := range h.clients {
		wg.Add(1)
		go func(i int, c Client) {
			defer wg.Done()
			// review-v6 M-21 fix: 业务自定义 Client (自管 HTTP/DB/缓存) 实现 Initialize /
			// ListTools panic 时 recover 转 result.err — 不让 goroutine panic 直接 crash
			// 整 Hub.Start 调用方进程. 多 MCP server 启停场景一个 panic 不连坐其余.
			defer func() {
				if r := recover(); r != nil {
					pe := panicerr.New(r)
					err := panicerr.WrapOpaque(c.Name()+": panic recovered", pe)
					panicerr.Log(ctx, slog.LevelError, "mcp: client 启动兜住 panic，已转成错误", pe, panicerr.KeyError, err.Error())
					results[i] = result{client: c, err: err}
				}
			}()
			if err := c.Initialize(ctx); err != nil {
				results[i] = result{client: c, err: fmt.Errorf("%s: initialize: %w", c.Name(), err)}
				return
			}
			tools, err := c.ListTools(ctx)
			if err != nil {
				results[i] = result{client: c, err: fmt.Errorf("%s: list tools: %w", c.Name(), err)}
				return
			}
			results[i] = result{client: c, tools: tools}
		}(i, c)
	}
	wg.Wait()

	var startErrs []error
	for _, r := range results {
		if r.err != nil {
			startErrs = append(startErrs, r.err)
		}
	}
	if len(startErrs) > 0 {
		// 关闭所有已打开的 Client；started 保持 false（全程持 mu、未置位）允许重试。
		for _, c := range h.clients {
			_ = c.Close()
		}
		return errors.Join(startErrs...)
	}

	for _, r := range results {
		for _, spec := range r.tools {
			rt := NewRemoteTool(r.client, spec)
			h.tools = append(h.tools, rt)
			h.toolkit.Register(rt)
		}
	}
	h.started = true
	return nil
}

// Toolkit 返回聚合后的工具注册表；在 Start 前调用得到空 Toolkit。
func (h *Hub) Toolkit() *tool.Toolkit {
	return h.toolkit
}

// Tools 返回所有已注册的 RemoteTool 视图，便于调试或手动组装。
func (h *Hub) Tools() []*RemoteTool {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*RemoteTool, len(h.tools))
	copy(out, h.tools)
	return out
}

// Close 并发关闭所有 Client，用 errors.Join 聚合所有错误。
func (h *Hub) Close() error {
	errs := make([]error, len(h.clients))
	var wg sync.WaitGroup
	for i, c := range h.clients {
		wg.Add(1)
		go func(i int, c Client) {
			defer wg.Done()
			// review-v6 M-21 fix: 业务自定义 Client Close() panic 时 recover 转 err
			defer func() {
				if r := recover(); r != nil {
					pe := panicerr.New(r)
					errs[i] = panicerr.WrapOpaque(c.Name()+": close panic recovered", pe)
					// Close 没有 ctx 参数
					panicerr.Log(context.Background(), slog.LevelError, "mcp: client 关闭兜住 panic，已转成错误", pe, panicerr.KeyError, errs[i].Error())
				}
			}()
			errs[i] = c.Close()
		}(i, c)
	}
	wg.Wait()
	return errors.Join(errs...)
}
