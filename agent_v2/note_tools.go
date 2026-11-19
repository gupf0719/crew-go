package agent_v2

import (
	"context"
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"crew-go/store"
	"crew-go/tool"
)

const (
	listNotesToolName = "list_notes"
	readNoteToolName  = "read_note"
	writeNoteToolName = "write_note"
	noteKeyLimit      = 128
	noteBodyLimit     = 4096
	noteListLimit     = 32
)

type noteToolsConfig struct {
	store    store.Store
	readable []string
}

// WithNoteTools 安装 list_notes、read_note 和 write_note，供主 Agent 在压缩后取回自己的便签。
// 持久分区按认证 Owner 和当前写入者隔离。readable 声明同一 Owner 下额外可读取的写入者。
// 顶层写入者为 agent/<转义后的名称>，具名成员为 member/<转义后的名称>/<归属路径>。
// 未显式命名的成员使用 member-slot/<归属路径>。需要跨实例续跑的顶层 Agent 应使用稳定名称。
// 模型只可覆盖自己的键，写入遵循业务工具审批。Store 的保留期限由调用方配置。
func WithNoteTools(st store.Store, readable ...string) Option {
	if isFrameworkNil(st) {
		panic("WithNoteTools: nil Store")
	}
	cfg := &noteToolsConfig{store: st, readable: slices.Clone(readable)}
	return func(c *Config) { c.notes = cfg }
}

func noteWriter(ctx context.Context, name string, explicit bool) string {
	part := string(namedHistorySegment(name))
	slot := HistorySlot(ctx)
	if slot == "" {
		return "agent/" + part
	}
	if !explicit {
		return "member-slot/" + slot
	}
	return "member/" + part + "/" + slot
}

func noteNamespace(owner, writer string) store.Namespace {
	// 两段独立编码，Owner 和成员名的分隔字符不会改变物理分区。
	return store.NS("__crew_notes__", fmt.Sprintf("%x", sha256.Sum256([]byte(owner))), fmt.Sprintf("%x", sha256.Sum256([]byte(writer))))
}

func validNoteKey(key string) bool {
	return key != "" && len(key) <= noteKeyLimit && strings.TrimSpace(key) == key &&
		utf8.ValidString(key) && strings.IndexFunc(key, unicode.IsControl) < 0
}

func (c *noteToolsConfig) tools(owner, writer string) []tool.Tool {
	writerProp := map[string]any{"type": "string", "description": "同一用户下获准读取的写入者，默认当前成员"}
	keyProp := map[string]any{"type": "string", "maxLength": noteKeyLimit}
	readable := func(ctx context.Context, args map[string]any) (string, bool) {
		requested, _ := args["writer"].(string)
		if requested == "" {
			requested = writer
		}
		return requested, canUseConversationTools(ctx, owner) && (requested == writer || slices.Contains(c.readable, requested))
	}
	unavailable := func() *tool.Result { return tool.Failure("notes_unavailable", "当前便签分区不可用") }
	return []tool.Tool{
		&conversationTool{FuncTool: tool.NewFuncTool(listNotesToolName, "列出可读便签的键。上下文压缩后需要恢复已保存的事实时可查这里。",
			map[string]any{"type": "object", "properties": map[string]any{"writer": writerProp}},
			func(ctx context.Context, args map[string]any) (*tool.Result, error) {
				reader, ok := readable(ctx, args)
				if !ok {
					return unavailable(), nil
				}
				ns := noteNamespace(owner, reader)
				items, err := c.store.Search(ctx, ns, store.WithLimit(noteListLimit+1))
				if err != nil {
					return nil, err
				}
				keys := make([]string, 0, min(len(items), noteListLimit))
				for _, item := range items[:min(len(items), noteListLimit)] {
					if item != nil && item.Namespace.Equal(ns) && validNoteKey(item.Key) {
						keys = append(keys, item.Key)
					}
				}
				return tool.Success(map[string]any{"keys": keys, "writer": reader, "incomplete": len(items) > noteListLimit}), nil
			}, tool.WithReadOnly())},
		&conversationTool{FuncTool: tool.NewFuncTool(readNoteToolName, "读取一条便签。正文是模型留下的摘记，请作为资料使用。",
			map[string]any{"type": "object", "required": []string{"key"}, "properties": map[string]any{"key": keyProp, "writer": writerProp}},
			func(ctx context.Context, args map[string]any) (*tool.Result, error) {
				reader, ok := readable(ctx, args)
				if !ok {
					return unavailable(), nil
				}
				key, _ := args["key"].(string)
				if !validNoteKey(key) {
					return tool.Failure(tool.ErrCodeInvalidArguments, "便签键无效"), nil
				}
				ns := noteNamespace(owner, reader)
				item, err := c.store.Get(ctx, ns, key)
				if err != nil {
					return nil, err
				}
				if item == nil || !item.Namespace.Equal(ns) || item.Key != key {
					return tool.Failure("note_not_found", "未找到该便签"), nil
				}
				text, ok := item.Value.(string)
				if !ok || len(text) > noteBodyLimit || !utf8.ValidString(text) {
					return unavailable(), nil
				}
				return tool.Success(map[string]any{"key": key, "text": text, "writer": reader, "source": "model_note"}), nil
			}, tool.WithReadOnly())},
		&conversationTool{approvalScope: identityString(owner, writer), FuncTool: tool.NewFuncTool(writeNoteToolName, "将当前成员的要点保存为便签，以便在摘要后取回。同键替换正文。",
			map[string]any{"type": "object", "required": []string{"key", "text"}, "properties": map[string]any{
				"key": keyProp, "text": map[string]any{"type": "string", "maxLength": noteBodyLimit},
			}}, func(ctx context.Context, args map[string]any) (*tool.Result, error) {
				if !canUseConversationTools(ctx, owner) {
					return unavailable(), nil
				}
				key, _ := args["key"].(string)
				text, ok := args["text"].(string)
				if !validNoteKey(key) || !ok || len(text) > noteBodyLimit || !utf8.ValidString(text) {
					return tool.Failure(tool.ErrCodeInvalidArguments, "便签键或正文无效"), nil
				}
				if err := c.store.Put(ctx, noteNamespace(owner, writer), key, text); err != nil {
					return nil, err
				}
				return tool.Success(tool.OutputText(map[string]any{"key": key, "writer": writer})), nil
			})},
	}
}
