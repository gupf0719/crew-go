package skill_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"slices"
	"testing"

	"crew-go/skill"
)

type schemaJSON struct {
	fields map[string]any
}

func (s *schemaJSON) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.fields)
}

func TestDefaultSkillBox_MetadataSnapshots(t *testing.T) {
	root := t.TempDir()
	writeSkill(t, root, "audit", "---\nname: audit\ndescription: review\ntags: [retention]\nexamples: [quarterly]\nallowed-tools: [read]\n---\n审计流程")
	writeSkillResource(t, filepath.Join(root, "audit"), "references/guide.md", "---\ntopics: [policy]\n---\n指南")
	box, err := skill.NewDefaultSkillBox(root)
	if err != nil {
		t.Fatal(err)
	}

	list := box.ListSkills()
	list[0].Tags[0] = "foreign tag"
	match := box.MatchSkills(t.Context(), "retention")
	if len(match) != 1 || match[0].Score <= 0 || !slices.Equal(match[0].Skill.Tags, []string{"retention"}) {
		t.Errorf("修改列表后，匹配索引和元数据应仍一致，实际 %+v", match)
	}
	match[0].Skill.Examples[0] = "foreign example"
	active, err := box.ActivateSkill(t.Context(), "audit")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(active.Skill.Examples, []string{"quarterly"}) || active.Prompt != "审计流程" {
		t.Errorf("激活应读取原始示例和正文，实际 %+v", active)
	}
	active.Skill.AllowedTools[0] = "foreign tool"
	active.Resources[0].Topics[0] = "foreign topic"
	if got := box.ListSkills()[0].AllowedTools; !slices.Equal(got, []string{"read"}) {
		t.Errorf("修改激活结果后，工具列表为 %v", got)
	}
	if got := box.ListSkillResources("audit")[0].Topics; !slices.Equal(got, []string{"policy"}) {
		t.Errorf("修改激活结果后，资源标签为 %v", got)
	}
}

func TestInMemorySkillBox_ActionSchemaSnapshots(t *testing.T) {
	type variants []map[string]any
	required := []string{"id"}
	fields := map[string]any{"minimum": int64(9007199254740993)}
	choices := variants{{"required": required, "id": &schemaJSON{fields: fields}}}
	examples := []string{`{"id":42}`}
	option := skill.WithSkillActionFunc("calc", skill.SkillActionDescriptor{
		Name: "add", Examples: examples,
		ArgumentsSchema: map[string]any{"anyOf": choices, "empty": []any{}, "nil": map[string]any(nil)},
	}, func(_ context.Context, args map[string]any) (*skill.SkillExecutionResult, error) {
		return &skill.SkillExecutionResult{Success: true, Output: args["id"].(string)}, nil
	})

	required[0] = "changed input"
	fields["minimum"] = 0
	examples[0] = "changed example"
	contents := []skill.SkillContent{{Descriptor: skill.SkillDescriptor{Name: "calc"}, Body: "body"}}
	box, err := skill.NewInMemorySkillBox("local", contents, option)
	if err != nil {
		t.Fatal(err)
	}
	second, err := skill.NewInMemorySkillBox("other", contents, option)
	if err != nil {
		t.Fatal(err)
	}

	check := func(a skill.SkillActionDescriptor) {
		t.Helper()
		data, err := json.Marshal(a.ArgumentsSchema)
		if a.Name != "add" || a.Runtime != "go" || err != nil ||
			string(data) != `{"anyOf":[{"id":{"minimum":9007199254740993},"required":["id"]}],"empty":[],"nil":null}` ||
			!slices.Equal(a.Examples, []string{`{"id":42}`}) ||
			a.ArgumentsSchema["empty"] == nil {
			t.Errorf("action schema 或示例发生变化，实际 %+v，schema=%s，err=%v", a, data, err)
		}
	}
	mutate := func(action *skill.SkillActionDescriptor) {
		t.Helper()
		values, ok := action.ArgumentsSchema["anyOf"].([]any)
		if !ok {
			t.Fatalf("action schema 的 anyOf 应为 JSON 数组，实际 %T", action.ArgumentsSchema["anyOf"])
		}
		tree := values[0].(map[string]any)
		tree["required"].([]any)[0] = "changed snapshot"
		tree["id"].(map[string]any)["minimum"] = json.Number("-1")
	}
	list := box.ListSkillActions("calc")
	check(list[0])
	mutate(&list[0])
	list[0].Examples[0] = "changed list example"
	list[0].Runtime = "unsupported"

	active, err := box.ActivateSkill(t.Context(), "calc")
	if err != nil {
		t.Fatal(err)
	}
	check(active.Actions[0])
	mutate(&active.Actions[0])
	active.Actions[0].Name = "other"
	check(box.ListSkillActions("calc")[0])
	check(second.ListSkillActions("calc")[0])
	result, err := box.RunSkillAction(t.Context(), "calc", "add", map[string]any{"id": "42"})
	if err != nil || result == nil || !result.Success || result.Output != "42" {
		t.Fatalf("修改快照后应仍能执行注册的 action，实际 result=%+v err=%v", result, err)
	}
}

func TestInMemorySkillBox_InvalidActionSchemaStillRuns(t *testing.T) {
	box, err := skill.NewInMemorySkillBox("local", []skill.SkillContent{
		{Descriptor: skill.SkillDescriptor{Name: "calc"}},
	}, skill.WithSkillActionFunc("calc", skill.SkillActionDescriptor{
		Name: "add", ArgumentsSchema: map[string]any{"invalid": make(chan struct{})},
	}, func(context.Context, map[string]any) (*skill.SkillExecutionResult, error) {
		return &skill.SkillExecutionResult{Success: true, Output: "sum=42"}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	list := box.ListSkillActions("calc")
	if len(list) != 1 || list[0].ArgumentsSchema != nil {
		t.Fatalf("无效 schema 应省略，action 仍应列出，实际 %+v", list)
	}
	result, err := box.RunSkillAction(t.Context(), "calc", "add", nil)
	if err != nil || result == nil || !result.Success || result.Output != "sum=42" {
		t.Fatalf("应执行 action，实际 result=%+v err=%v", result, err)
	}
}
