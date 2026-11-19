package agent_v2

import (
	"fmt"
	"slices"

	"crew-go/compose_v2"
)

type graphBreakpoints struct {
	before []compose_v2.NodeID
	after  []compose_v2.NodeID
}

// WithRunInterruptBefore 在本次 GraphAgent.Run 中给指定节点加 before 断点。
// 可用 compose_v2.AllNodes 匹配本图所有节点。断点与图及 GraphAgent 的既有断点取并集，
// 调用级配置只在本次调用生效。其他内置编排拒绝非空的节点列表。
func WithRunInterruptBefore(nodes ...compose_v2.NodeID) RunOption {
	nodes = slices.Clone(nodes)
	return func(c *runConfig) { c.breakpoints.before = append(c.breakpoints.before, nodes...) }
}

// WithRunInterruptAfter 在本次 GraphAgent.Run 中给指定节点加 after 断点。
// 断点在该超步整体提交后判定。作用域与 [WithRunInterruptBefore] 相同。
func WithRunInterruptAfter(nodes ...compose_v2.NodeID) RunOption {
	nodes = slices.Clone(nodes)
	return func(c *runConfig) { c.breakpoints.after = append(c.breakpoints.after, nodes...) }
}

// WithResumeInterruptBefore 在本次 GraphAgent.Resume 中给指定节点加 before 断点。
// 续跑越过原断点边界，之后按本次传入的节点继续判定，支持 compose_v2.AllNodes。
// 其他内置编排拒绝非空的节点列表。
func WithResumeInterruptBefore(nodes ...compose_v2.NodeID) ResumeOption {
	nodes = slices.Clone(nodes)
	return func(c *resumeConfig) { c.breakpoints.before = append(c.breakpoints.before, nodes...) }
}

// WithResumeInterruptAfter 在本次 GraphAgent.Resume 中给指定节点加 after 断点。
// 作用域与 [WithResumeInterruptBefore] 相同，断点在超步整体提交后判定。
func WithResumeInterruptAfter(nodes ...compose_v2.NodeID) ResumeOption {
	nodes = slices.Clone(nodes)
	return func(c *resumeConfig) { c.breakpoints.after = append(c.breakpoints.after, nodes...) }
}

func (b graphBreakpoints) empty() bool { return len(b.before) == 0 && len(b.after) == 0 }

func (b graphBreakpoints) validate(ag Agent) error {
	if b.empty() {
		return nil
	}
	target, ok := ag.(interface {
		validateBreakpointNodes(graphBreakpoints) error
	})
	if !ok {
		return &errCallRejected{msg: fmt.Sprintf("agent %q: per-call breakpoints require GraphAgent", ag.Name())}
	}
	return target.validateBreakpointNodes(b)
}

func (g *GraphAgent) validateBreakpointNodes(b graphBreakpoints) error {
	if g == nil {
		return &errCallRejected{msg: "per-call breakpoints require a configured GraphAgent"}
	}
	return g.graph.ValidateBreakpointNodes(b.before, b.after)
}

func (b graphBreakpoints) appendOptions(opts []compose_v2.RunOption) []compose_v2.RunOption {
	if len(b.before) > 0 {
		opts = append(opts, compose_v2.WithInterruptBefore(b.before...))
	}
	if len(b.after) > 0 {
		opts = append(opts, compose_v2.WithInterruptAfter(b.after...))
	}
	return opts
}
