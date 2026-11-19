package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/skill"
)

type observedToolModel struct {
	tools    []*schema.ToolInfo
	generate func(context.Context, []*schema.Message, []*schema.ToolInfo) (*schema.Message, error)
}

func (m observedToolModel) Generate(ctx context.Context, messages []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return m.generate(ctx, messages, m.tools)
}

func (m observedToolModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m observedToolModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	m.tools = tools
	return m, nil
}

func writeAgentSkill(t *testing.T, path, manifest string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestSkillReloadKeepsBindingAndDispatchOnTheSameCatalog(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	root := t.TempDir()
	manifest := filepath.Join(root, "ops", "inventory", "SKILL.md")
	writeAgentSkill(t, manifest, "---\nname: inventory\ndescription: 旧版库存\n---\nold playbook")
	var writes atomic.Int32
	box, err := skill.NewDefaultSkillBox(root, skill.WithRecursiveDiscovery(true), skill.WithHotReload(true),
		skill.WithSkillActionFunc("inventory", skill.SkillActionDescriptor{Name: "record", Description: "登记库存"},
			func(context.Context, map[string]any) (*skill.SkillExecutionResult, error) {
				writes.Add(1)
				return &skill.SkillExecutionResult{Stdout: "inventory recorded"}, nil
			}))
	if err != nil {
		t.Fatal(err)
	}
	ready, release := make(chan struct{}), make(chan struct{})
	var rounds int
	oldModel := observedToolModel{generate: func(ctx context.Context, messages []*schema.Message, tools []*schema.ToolInfo) (*schema.Message, error) {
		rounds++
		if !skillToolShows(tools, "旧版库存") || skillToolShows(tools, "新版复核") {
			return nil, errors.New("in-flight model binding left its original catalog")
		}
		switch rounds {
		case 1:
			close(ready)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return oneCall("activate_skill", `{"skill":"inventory"}`), nil
		case 2:
			if !strings.Contains(concatContent(messages), "old playbook") {
				return nil, errors.New("activation did not use the advertised playbook")
			}
			return oneCall("run_skill_action", `{"skill":"inventory","action":"record"}`), nil
		default:
			if !strings.Contains(concatContent(messages), "inventory recorded") {
				return nil, errors.New("action was missing from the execution catalog")
			}
			return schema.AssistantMessage("old task complete"), nil
		}
	}}
	a := agent_v2.NewBasicAgent(agent_v2.WithName("inventory"), agent_v2.WithModel(oldModel),
		agent_v2.WithSkills(skill.SkillSource{SkillBox: box}))
	first := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := a.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("盘点"))
		first <- err
	}()
	defer func() {
		cancel()
		<-done
	}()
	select {
	case <-ready:
	case err := <-first:
		t.Fatalf("旧任务未完成首次绑定: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	writeAgentSkill(t, manifest, "---\nname: review\ndescription: 新版复核\n---\nnew playbook")
	if err := box.Reload(); err != nil {
		t.Fatal(err)
	}
	newModel := observedToolModel{generate: func(_ context.Context, _ []*schema.Message, tools []*schema.ToolInfo) (*schema.Message, error) {
		if !skillToolShows(tools, "新版复核") || skillToolShows(tools, "旧版库存") {
			return nil, errors.New("new task did not use the published catalog")
		}
		for _, tool := range tools {
			if tool.Name == "run_skill_action" {
				return nil, errors.New("new task still exposes the previous action")
			}
		}
		return schema.AssistantMessage("new task complete"), nil
	}}
	if _, err := a.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("复核"), agent_v2.WithRunModel(newModel)); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-first; err != nil || writes.Load() != 1 {
		t.Fatalf("旧任务应继续执行一次原 action: writes=%d err=%v", writes.Load(), err)
	}
}

func skillToolShows(tools []*schema.ToolInfo, want string) bool {
	for _, tool := range tools {
		if tool.Name == "activate_skill" && strings.Contains(tool.Desc, want) {
			return true
		}
	}
	return false
}

func TestSkillReloadRejectsResumeWithChangedCatalog(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	root := t.TempDir()
	manifest := filepath.Join(root, "inventory", "SKILL.md")
	const original = "---\nname: inventory\n---\nfirst version"
	writeAgentSkill(t, manifest, original)
	load := func() *skill.DefaultSkillBox {
		t.Helper()
		box, err := skill.NewDefaultSkillBox(root, skill.WithHotReload(true))
		if err != nil {
			t.Fatal(err)
		}
		return box
	}
	var calls atomic.Int32
	build := func(box *skill.DefaultSkillBox) *agent_v2.BasicAgent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("inventory-review"), agent_v2.WithSkills(skill.SkillSource{SkillBox: box}),
			agent_v2.WithModel(&fnModel{fn: func(ctx context.Context, _ []*schema.Message) (*schema.Message, error) {
				calls.Add(1)
				answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "继续处理库存？"})
				if err != nil {
					return nil, err
				}
				return schema.AssistantMessage(fmt.Sprintf("库存复核: %s", answer.Content)), nil
			}}))
	}
	box := load()
	a := build(box)
	session := agent_v2.NewRunSession()
	_, err := a.Run(ctx, session, schema.UserMessage("复核库存"))
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok || len(interrupt.AllQueries()) != 1 {
		t.Fatalf("任务应挂起: %v", err)
	}
	answer := agent_v2.WithResumeUserInputs(map[string]*schema.Message{
		interrupt.AllQueries()[0].InterruptID: schema.UserMessage("确认"),
	})
	writeAgentSkill(t, manifest, "---\nname: inventory\n---\nsecond version")
	if err := box.Reload(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Resume(ctx, session.RunID, answer); !errors.Is(err, compose_v2.ErrGraphIDConflict) || calls.Load() != 1 {
		t.Fatalf("目录变更后应保留挂起并在执行前拒绝续跑: calls=%d err=%v", calls.Load(), err)
	}
	writeAgentSkill(t, manifest, original)
	events, err := build(load()).Resume(ctx, session.RunID, answer)
	if err != nil {
		t.Fatal(err)
	}
	if final := agent_v2.FinalMessage(events); final == nil || final.Content != "库存复核: 确认" || calls.Load() != 2 {
		t.Fatalf("同内容的新实例应可使用原挂起寻址: final=%v calls=%d", final, calls.Load())
	}
}
