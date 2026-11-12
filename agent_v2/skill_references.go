package agent_v2

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/skill"
	"crew-go/tool"
)

const (
	readSkillResourceToolName = "read_skill_resource"
	skillReferenceActivation  = "crew.skill_reference.activation"
	skillReferencePathsKey    = "crew.skill_reference.paths"
	skillReferenceMessage     = "crew.skill_reference.message"
	skillReferenceReadKey     = "crew.skill_reference.read"
	skillInlineCallIndex      = "crew.skill_body.call_index"
	skillReferenceBatchStep   = "skill-reference-batch"
)

type skillReferenceBatchMemo struct {
	Entries map[int]skillReferenceLoad
}

type skillReferenceLoad struct {
	Skill    string
	Allowed  []string
	Selected []string
	Loaded   []string
	Messages []*schema.Message
}

func init() {
	compose_v2.RegisterValueType[*skillReferenceBatchMemo]("agent_v2.skillReferenceBatchMemo")
}

func hasSkillReferences(box skill.SkillBox) bool {
	for _, descriptor := range filterModelInvocableSkills(box.ListSkills()) {
		if len(skillReferencePaths(box.ListSkillResources(descriptor.Name))) > 0 {
			return true
		}
	}
	return false
}

func skillReferencePaths(resources []skill.SkillResource) []string {
	var paths []string
	for _, ref := range resources {
		if ref.ResourceType == "reference" && ref.Path != "" {
			paths = append(paths, ref.Path)
		}
	}
	return paths
}

func skillPromptWithReferences(prompt string, refs []skill.SkillResource) string {
	var listing strings.Builder
	for _, ref := range refs {
		if ref.ResourceType != "reference" || ref.Path == "" {
			continue
		}
		if listing.Len() == 0 {
			listing.WriteString("\n\nReferences (read details with read_skill_resource):\n")
		}
		entry := fmt.Sprintf("- %s: %s\n", ref.Path, ref.Description)
		if listing.Len()+len(entry) > 4096 {
			listing.WriteString("- additional references omitted\n")
			break
		}
		listing.WriteString(entry)
	}
	return prompt + listing.String()
}

type skillReferenceTool struct {
	box       skill.SkillBox
	maxTokens int
}

func (t *skillReferenceTool) Name() string { return readSkillResourceToolName }

func (t *skillReferenceTool) Description() string {
	return "读取已配置 skill 的一份文本参考文档。使用激活结果所列的 skill 名和路径。"
}

func (t *skillReferenceTool) InputSchema() map[string]any {
	return map[string]any{"type": "object", "required": []string{"skill", "path"},
		"properties": map[string]any{"skill": map[string]any{"type": "string"}, "path": map[string]any{"type": "string"}}}
}

func (t *skillReferenceTool) Call(ctx context.Context, args map[string]any) (*tool.Result, error) {
	name, _ := args["skill"].(string)
	path, _ := args["path"].(string)
	if name == "" || path == "" {
		return tool.Failure(tool.ErrCodeInvalidArguments, "skill and path are required"), nil
	}
	visible := filterModelInvocableSkills(t.box.ListSkills())
	if !slices.ContainsFunc(visible, func(s skill.SkillDescriptor) bool { return s.Name == name }) ||
		!slices.Contains(skillReferencePaths(t.box.ListSkillResources(name)), path) {
		return tool.Failure(tool.ErrCodeNotFound, "reference is not available for this skill"), nil
	}
	key := name + "\x00" + path
	var reads *skillReferenceReads
	if batch := toolBatchFrom(ctx); batch != nil {
		reads = batch.refReads
	}
	if !reads.reserve(key) {
		return tool.Success("Reference already read or being read in this run."), nil
	}
	var read bool
	defer func() {
		if !read {
			reads.release(key)
		}
	}()
	// 自定义 SkillBox 的激活可能带审批，直接读文件会绕开这道入口。
	active, err := t.box.ActivateSkill(ctx, name)
	if err != nil {
		if compose_v2.MustPropagate(ctx, err) {
			return nil, err
		}
		return tool.Failure(tool.ErrCodeNotFound, "skill reference is not authorized"), nil
	}
	if active == nil || !slices.Contains(skillReferencePaths(active.Resources), path) {
		return tool.Failure(tool.ErrCodeNotFound, "reference is not available for this skill"), nil
	}
	text, complete := readSkillReference(t.box, name, path, t.maxTokens)
	if text == "" {
		return tool.Failure(tool.ErrCodeNotFound, "reference could not be read"), nil
	}
	if !complete {
		text = limitReferenceTokens(text+"\n…", t.maxTokens)
	}
	read = true
	result := tool.Success(text)
	result.Metadata = map[string]any{skillReferenceReadKey: key}
	return result, nil
}

// skillReferenceReads 在并行工具调用间共享读取进度，来源是已提交消息和当前工具账。
type skillReferenceReads struct {
	mu         sync.Mutex
	byKey      map[string]bool
	annotation string
}

func skillReferenceTask(ctx context.Context) string {
	if session := GetSession(ctx); session != nil {
		return session.RunID
	}
	return compose_v2.GetSessionID(ctx)
}

func newSkillReferenceReads(ctx context.Context, messages []*schema.Message, memo map[string]*toolMemoEntry) *skillReferenceReads {
	reads := &skillReferenceReads{byKey: make(map[string]bool), annotation: skillReferenceReadKey + "." + skillReferenceTask(ctx)}
	for _, message := range messages {
		if message != nil {
			for _, key := range referenceStringList(message.Annotations[reads.annotation]) {
				reads.byKey[key] = true
			}
		}
	}
	for _, entry := range memo {
		if entry != nil && entry.ToolMsg != nil {
			if key, _ := entry.ToolMsg.Annotations[skillReferenceReadKey].(string); key != "" {
				reads.byKey[key] = true
			}
		}
	}
	return reads
}

func (r *skillReferenceReads) reserve(key string) bool {
	if r == nil {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byKey[key] {
		return false
	}
	r.byKey[key] = true
	return true
}

func (r *skillReferenceReads) release(key string) {
	if r != nil {
		r.mu.Lock()
		delete(r.byKey, key)
		r.mu.Unlock()
	}
}

func discardRejectedSkillBodies(results []*schema.Message, failed []bool) []*schema.Message {
	return slices.DeleteFunc(results, func(message *schema.Message) bool {
		if message == nil || message.Role != schema.RoleUser || !hasSkillMarker(message) {
			return false
		}
		index, _ := message.Annotations[skillInlineCallIndex].(string)
		i, err := strconv.Atoi(index)
		return err == nil && i >= 0 && i < len(failed) && failed[i]
	})
}

type boundedReferenceReader interface {
	ReadSkillReference(name, path string, maxBytes int) (string, bool, error)
}

func referenceMaxTokens(cfg *Config) int {
	if cfg.skillReferenceMaxTokens <= 0 {
		return 512
	}
	return cfg.skillReferenceMaxTokens
}

func readSkillReference(box skill.SkillBox, name, path string, maxTokens int) (string, bool) {
	var text string
	complete := true
	if bounded, ok := box.(boundedReferenceReader); ok {
		var err error
		text, complete, err = bounded.ReadSkillReference(name, path, min(maxTokens, 1<<18)*4)
		if err != nil {
			return "", false
		}
	} else {
		text = box.ReadSkillResource(name, path)
	}
	if refTokens(text) > maxTokens {
		return limitReferenceTokens(text, maxTokens), false
	}
	return text, complete
}

func refTokens(text string) int {
	return compose_v2.EstimateMessageTokens([]*schema.Message{schema.UserMessage(text)})
}

func limitReferenceTokens(text string, maxTokens int) string {
	if refTokens(text) <= maxTokens {
		return text
	}
	runes := []rune(text)
	n := sort.Search(len(runes), func(n int) bool { return refTokens(string(runes[:n+1])) > maxTokens })
	return string(runes[:n])
}

func referenceStringList(v any) []string {
	switch values := v.(type) {
	case []string:
		return values
	case []any: // annotations 经 JSON 存档恢复。
		var out []string
		for _, value := range values {
			if s, ok := value.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

// loadActivatedReferences 在结束钩子认可激活后依次读取相关参考文档。正文作为普通消息写进当次执行，
// 交由通用压缩处理。选过的路径（包括预算跳过）随原结果留在存档，同一任务后续激活可据此去重。
func loadActivatedReferences(ctx context.Context, cfg *Config, reads *skillReferenceReads, preamble, messages, results []*schema.Message, failed []bool) []*schema.Message {
	seenKey := "crew.skill_reference.seen." + skillReferenceTask(ctx)
	seen := make(map[string]bool)
	query := ""
	for _, message := range messages {
		if message == nil {
			continue
		}
		for _, key := range referenceStringList(message.Annotations[seenKey]) {
			seen[key] = true
		}
		if message.Role == schema.RoleUser && message.Content != "" && !IsFrameworkNote(message) {
			query = message.Content
		}
	}
	remaining := cfg.skillReferenceBudgetTokens
	if remaining == 0 {
		remaining = 1024
	}
	if cfg.AutoCompact != nil && cfg.AutoCompact.ModelContextWindow > 0 {
		estimated := compose_v2.EstimateMessageTokens(preamble) + compose_v2.EstimateMessageTokens(messages)
		used := max(estimated, compose_v2.EstimateTokens(messages))
		available := cfg.AutoCompact.ModelContextWindow - used - compose_v2.EstimateMessageTokens(results)
		remaining = min(remaining, available/4)
	}
	previous, _ := compose_v2.GetStepResult[*skillReferenceBatchMemo](ctx, skillReferenceBatchStep)
	current := &skillReferenceBatchMemo{Entries: make(map[int]skillReferenceLoad)}
	changed := false
	var out []*schema.Message
	for i, result := range results {
		if i >= len(failed) || failed[i] || result == nil {
			continue
		}
		if key, _ := result.Annotations[skillReferenceReadKey].(string); key != "" {
			result.Annotations[reads.annotation] = []string{key}
		}
		name, _ := result.Annotations[skillReferenceActivation].(string)
		if name == "" {
			continue
		}
		allowed := referenceStringList(result.Annotations[skillReferencePathsKey])
		if previous != nil {
			entry := previous.Entries[i]
			if entry.Skill == name && slices.Equal(entry.Allowed, allowed) {
				current.Entries[i] = entry
				result.Annotations[seenKey] = entry.Selected
				result.Annotations[reads.annotation] = entry.Loaded
				for _, key := range entry.Selected {
					seen[key] = true
				}
				for _, key := range entry.Loaded {
					reads.reserve(key)
				}
				out = append(out, entry.Messages...)
				continue
			}
		}
		matches := skill.MatchReferences(query, cfg.Skill.SkillBox.ListSkillResources(name))
		var selected []string
		var loaded []string
		var additions []*schema.Message
		for _, match := range matches {
			ref := match.Resource
			key := name + "\x00" + ref.Path
			if seen[key] || !slices.Contains(allowed, ref.Path) || (!ref.AlwaysLoad && match.Score < 0.5) {
				continue
			}
			seen[key] = true
			selected = append(selected, key)
			if (!ref.AlwaysLoad && remaining <= 0) || !reads.reserve(key) {
				continue
			}
			text, complete := readSkillReference(cfg.Skill.SkillBox, name, ref.Path, referenceMaxTokens(cfg))
			if text == "" || (!ref.AlwaysLoad && !complete) {
				reads.release(key)
				continue
			}
			body := fmt.Sprintf("Skill %q reference %q:\n%s", name, ref.Path, text)
			cost := refTokens(body)
			if !ref.AlwaysLoad && cost > remaining {
				reads.release(key)
				continue
			}
			if !ref.AlwaysLoad {
				remaining -= cost
			}
			msg := schema.UserMessage(body)
			msg.Annotations = map[string]any{skillReferenceMessage: true}
			additions = append(additions, msg)
			loaded = append(loaded, key)
			slog.DebugContext(ctx, "agent_v2: 自动读取 skill reference", "skill", name, "path", ref.Path, "estimated_tokens", cost)
		}
		if len(selected) > 0 {
			result.Annotations[seenKey] = selected
		}
		if len(loaded) > 0 {
			result.Annotations[reads.annotation] = loaded
		}
		if len(selected) > 0 {
			current.Entries[i] = skillReferenceLoad{Skill: name, Allowed: allowed, Selected: selected, Loaded: loaded, Messages: additions}
			changed = true
		}
		out = append(out, additions...)
	}
	if changed {
		if err := compose_v2.SaveStepResult(ctx, skillReferenceBatchStep, current, compose_v2.Rerunnable(), compose_v2.WithStreams(nil)); err != nil {
			slog.WarnContext(ctx, "agent_v2: 无法记录本批 skill 参考文档，续跑可能重读", "err", err)
		}
	}
	return out
}
