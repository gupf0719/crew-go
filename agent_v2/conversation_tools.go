package agent_v2

import (
	"context"
	"crypto/sha256"
	"fmt"

	"crew-go/compose_v2"
	"crew-go/tool"
)

// conversationTool 标记依赖当前任务授权的工具。保持业务工具审批，限制为同步执行。
// 只读查询在未提交的批次恢复时重查权限和数据，写入沿用工具账防止重做。
type conversationTool struct {
	*tool.FuncTool
	approvalScope string
}

type conversationRequestKey struct{}

func (conversationRequestKey) ExecutionState() {}

type conversationRequest struct{ context.Context }

// 只有任务入口能提供认证请求值。钩子采纳业务上下文时沿用本次任务的授权来源。
func withConversationRequest(ctx context.Context) context.Context {
	return context.WithValue(ctx, conversationRequestKey{}, conversationRequest{ctx})
}

func canUseConversationTools(ctx context.Context, owner string) bool {
	return owner != "" && compose_v2.GetOwner(ctx) == owner && !historySuppressed(ctx)
}

// bindConversationTools 固定入口认证主体及会话写入器。工具调用的上下文只提供当前授权与期限。
func bindConversationTools(ctx context.Context, cfg Config) ([]tool.Tool, string, error) {
	if cfg.notes == nil && cfg.historySearch == nil {
		return nil, "", nil
	}
	if cfg.Toolkit != nil {
		var names []string
		if cfg.notes != nil {
			names = append(names, listNotesToolName, readNoteToolName, writeNoteToolName)
		}
		if cfg.historySearch != nil {
			names = append(names, historySearchToolName)
		}
		for _, name := range names {
			if cfg.Toolkit.Get(name) != nil {
				return nil, "", fmt.Errorf("agent %q: conversation tool %q conflicts with the toolkit", cfg.Name, name)
			}
		}
	}
	request, ok := ctx.Value(conversationRequestKey{}).(conversationRequest)
	requestCtx := request.Context
	if !ok {
		// 低阶图入口没有 Agent 任务章，沿用外层图的请求来源。
		requestCtx = compose_v2.TaskCommitContext(ctx)
	}
	owner := compose_v2.GetOwner(requestCtx)
	if !canUseConversationTools(ctx, owner) {
		return nil, "", nil
	}
	var tools []tool.Tool
	var writer, member string
	var key HistoryKey
	if cfg.notes != nil {
		writer = noteWriter(ctx, cfg.Name, cfg.nameExplicit)
		tools = cfg.notes.tools(owner, writer)
	}
	if w := historyWriterFrom(ctx); cfg.historySearch != nil && w != nil && !w.clientPrior && w.task != "" {
		member = historyOwner(ctx, cfg.Name)
		key = w.key
		source := historySource{store: w.store, key: key, task: w.task}
		tools = append(tools, newHistorySearchTool(requestCtx, owner, member, source, cfg.historySearch))
	}
	if len(tools) == 0 {
		return nil, "", nil
	}
	// 直接 Resume 和嵌套续跑共用图身份守卫，不能把另一个主体的调用账或视图带入本次请求。
	revision := sha256.Sum256([]byte(identityString(owner, writer, member, key.Owner, key.ConvID)))
	return tools, fmt.Sprintf("|conversation-tools-v1:%x", revision), nil
}
