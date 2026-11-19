package agui

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// WithHistoryStore 为 Handler 启用按 owner 和 threadId 隔离的服务端历史。
// 该后端也用于查询消息引用及准备编辑或重生分支。
func WithHistoryStore(store agent_v2.HistoryStore) Option {
	if nilInterface(store) {
		panic("agui: WithHistoryStore requires a HistoryStore")
	}
	return func(h *handler) { h.historyStore = store }
}

func (h *handler) hostedHistory(key sessionKey) agent_v2.TaskHistoryDestination {
	if h.historyStore == nil {
		return agent_v2.TaskHistoryDestination{}
	}
	return agent_v2.TaskHistoryDestination{
		Key:   agent_v2.HistoryKey{Owner: key.owner, ConvID: key.threadID},
		Store: h.historyStore, Source: agent_v2.TaskHistoryHost,
	}
}

func (h *handler) historyForCancellation(s *sharedSession) func(context.Context, agent_v2.HistoryKey) (agent_v2.HistoryStore, error) {
	if s.record.HistorySource != agent_v2.TaskHistoryHost {
		return h.cancellationHistory
	}
	store := h.historyStore
	if store == nil {
		return nil
	}
	return func(context.Context, agent_v2.HistoryKey) (agent_v2.HistoryStore, error) { return store, nil }
}

// HistoryReference 将一条服务端记录与前端消息和工具调用相关联。
type HistoryReference struct {
	ID        string                          `json:"id"`
	Role      schema.MessageRole              `json:"role"`
	WireID    string                          `json:"wireId,omitempty"`
	ToolCalls []agent_v2.HistoryToolReference `json:"toolCalls,omitempty"`
}

func historyReference(m *schema.Message) HistoryReference {
	ref := HistoryReference{ID: m.ID, Role: m.Role, ToolCalls: agent_v2.HistoryToolReferences(m)}
	switch m.Role {
	case schema.RoleUser:
		ref.WireID, _ = m.Annotations[agent_v2.HistoryInputMessageIDAnnotation].(string)
	case schema.RoleAssistant:
		ref.WireID, _ = m.Annotations[compose_v2.RunIDAnnotationKey].(string)
	case schema.RoleTool:
		if len(ref.ToolCalls) == 1 && ref.ToolCalls[0].InstanceID != "" {
			ref.WireID = toolResultWireID(ref.ToolCalls[0].InstanceID)
		}
	}
	return ref
}

// HistoryForkRequest 声明一个已有消息位置和分叉动作。
// edit 选择用户输入，regenerate 可选择该轮的用户输入、助手消息或工具结果。
type HistoryForkRequest struct {
	Action    string `json:"action"`
	MessageID string `json:"messageId"`
}

// HistoryFork 是准备好的新会话。Input 为已存用户输入的 AG-UI 消息，带新会话的消息 ID。
// 编辑时可改写 Input 的内容，重生时可直接作为新会话的一轮输入。
type HistoryFork struct {
	ThreadID        string          `json:"threadId"`
	SourceMessageID string          `json:"sourceMessageId"`
	SourceWireID    string          `json:"sourceWireId,omitempty"`
	Input           json.RawMessage `json:"input"`
}

func (h *handler) serveHistory(w http.ResponseWriter, r *http.Request, op string) {
	if h.historyStore == nil {
		_ = writeReject(w, http.StatusNotImplemented, "history_unavailable", "configure a history store on the handler")
		return
	}
	if (op == "references" && r.Method != http.MethodGet) || (op == "fork" && r.Method != http.MethodPost) {
		_ = writeReject(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	thread := r.URL.Query().Get("threadId")
	if thread == "" {
		_ = writeReject(w, http.StatusBadRequest, "no_thread_id", "history requires threadId")
		return
	}
	ctx, key := h.requestContext(r, thread)
	if key.owner == "" {
		_ = writeReject(w, http.StatusUnauthorized, "owner_required", "conversation history requires an authenticated owner")
		return
	}
	var req HistoryForkRequest
	if op == "fork" {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.MessageID == "" ||
			(req.Action != "edit" && req.Action != "regenerate") {
			_ = writeReject(w, http.StatusBadRequest, "invalid_history_fork", "use a messageId and an edit or regenerate action")
			return
		}
	}
	messages, err := agent_v2.LoadAllHistory(ctx, h.historyStore, agent_v2.HistoryKey{Owner: key.owner, ConvID: thread})
	if err != nil {
		slog.WarnContext(ctx, "agui: 对话历史读取失败", "err", err)
		_ = writeReject(w, http.StatusServiceUnavailable, "history_read_failed", "the server could not load conversation history")
		return
	}
	if op == "references" {
		refs := make([]HistoryReference, 0, len(messages))
		for _, m := range messages {
			if visibleHistoryRole(m) {
				refs = append(refs, historyReference(m))
			}
		}
		writeHistoryJSON(w, http.StatusOK, refs)
		return
	}
	h.forkHistory(ctx, w, key, req, messages)
}

func visibleHistoryRole(m *schema.Message) bool {
	return m != nil && (m.Role == schema.RoleUser || m.Role == schema.RoleAssistant || m.Role == schema.RoleTool)
}

func (h *handler) forkHistory(ctx context.Context, w http.ResponseWriter, key sessionKey, req HistoryForkRequest, messages []*schema.Message) {
	var canonical, aliases []int
	for i, m := range messages {
		if !visibleHistoryRole(m) || (req.Action == "edit" && m.Role != schema.RoleUser) {
			continue
		}
		ref := historyReference(m)
		if req.MessageID == ref.ID {
			canonical = append(canonical, i)
		} else if matchesHistoryWireID(ref, req.MessageID) {
			aliases = append(aliases, i)
		}
	}
	selected := canonical
	if len(selected) == 0 {
		selected = aliases
	}
	if len(selected) == 0 {
		_ = writeReject(w, http.StatusNotFound, "history_message_not_found", "message was not found in this conversation")
		return
	}
	var start int
	var user *schema.Message
	for _, at := range selected {
		atStart, atUser, ok := agent_v2.HistoryTurnAt(messages, at)
		if !ok {
			_ = writeReject(w, http.StatusConflict, "history_turn_unavailable", "the selected message has no available user turn")
			return
		}
		if user != nil && (start != atStart || user != atUser) {
			_ = writeReject(w, http.StatusConflict, "ambiguous_history_message", "use a unique server history message ID")
			return
		}
		start, user = atStart, atUser
	}
	input, err := forkUserMessage(user)
	if err != nil {
		_ = writeReject(w, http.StatusUnprocessableEntity, "history_input_unsupported", "the stored input cannot be represented as an AG-UI message")
		return
	}
	thread := newThreadID()
	destination := agent_v2.HistoryKey{Owner: key.owner, ConvID: thread}
	if start > 0 {
		ctx := compose_v2.WithThreadIDCtx(ctx, thread)
		if err := h.historyStore.Append(ctx, destination, messages[:start]); err != nil {
			slog.WarnContext(ctx, "agui: 历史分支写入失败", "err", err)
			_ = writeReject(w, http.StatusServiceUnavailable, "history_fork_failed", "the server could not create the history branch")
			return
		}
	}
	ref := historyReference(user)
	writeHistoryJSON(w, http.StatusCreated, HistoryFork{
		ThreadID: thread, SourceMessageID: ref.ID, SourceWireID: ref.WireID, Input: input,
	})
}

func matchesHistoryWireID(ref HistoryReference, id string) bool {
	if ref.WireID == "" {
		return false
	}
	if ref.WireID == id {
		return true
	}
	if ref.Role != schema.RoleTool {
		return false
	}
	// 客户端列表已占用基准 ID 时，SSE 会为这个工具结果追加数字后缀。
	suffix, ok := strings.CutPrefix(id, ref.WireID+"#")
	n, err := strconv.ParseUint(suffix, 10, 64)
	return ok && err == nil && n >= 2
}

func writeHistoryJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value) // 数据由本端构造，连接断开时交由 HTTP 服务收尾。
}
