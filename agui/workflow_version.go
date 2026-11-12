package agui

import (
	"maps"
	"strings"
)

// WithWorkflowVersions 将 [WithSessionStore] 的任务绑定到工作流定义版本。Handler 的工厂处理 current，
// previous 中的工厂处理它们各自版本的挂起任务。previous 的空键可显式接管旧版未记录版本的任务。
// 同一版本标识须指向兼容的图、工具和审批定义。所有实例须使用相同的版本含义。
func WithWorkflowVersions(current string, previous map[string]AgentFactory) Option {
	if strings.TrimSpace(current) == "" {
		panic("agui: current workflow version must be non-empty")
	}
	previous = maps.Clone(previous)
	for version, factory := range previous {
		if version == current || factory == nil {
			panic("agui: previous workflow versions must be distinct from current and have a factory")
		}
	}
	return func(h *handler) {
		h.workflowVersion = current
		h.previousFactories = previous
	}
}

func (h *handler) workflowFactory(version string) AgentFactory {
	if version == h.workflowVersion {
		return h.build
	}
	return h.previousFactories[version]
}
