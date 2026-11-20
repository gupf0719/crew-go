package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

type customNestedGraphAgent struct {
	agent_v2.Agent
	resumes atomic.Int32
}

func (a *customNestedGraphAgent) Run(ctx context.Context, session *agent_v2.RunSession, input *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	// 自研实现的入口校验应看到子会话，不能误记进父会话。
	if agent_v2.GetSession(ctx) != session || session.RootSession() == session {
		return nil, errors.New("低阶图交接时没有绑定子会话")
	}
	if value, _ := session.GetValue("example-account"); value != "account-a" {
		return nil, errors.New("只有子会话的事件应隔离，共享业务值应可读取")
	}
	ctx, session, err := agent_v2.EnterRun(ctx, session, a.Name())
	if err != nil {
		return nil, err
	}
	session.AddEvent(agent_v2.NewOutputEvent(a.Name(), schema.AssistantMessage("子会话私有审计")))
	return a.Agent.Run(ctx, session, input, opts...)
}

func (a *customNestedGraphAgent) Resume(ctx context.Context, sid string, opts ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	a.resumes.Add(1)
	return a.Agent.Resume(ctx, sid, opts...)
}

func TestCustomAgent_LowLevelNestedResume(t *testing.T) {
	cp := compose_v2.NewInMemoryCheckpointer()
	history := agent_v2.NewInMemoryHistoryStore()
	ctx, cancel := context.WithTimeout(compose_v2.WithCheckpointerCtx(t.Context(), cp), 5*time.Second)
	defer cancel()
	ctx = compose_v2.WithThreadIDCtx(agent_v2.WithHistoryCtx(ctx, history, "t"), "c15-nested")
	session := agent_v2.NewRunSession()
	session.SetValue("example-account", "account-a")

	var preparations, writes atomic.Int32
	worker := &customNestedGraphAgent{Agent: agent_v2.NewAgentFunc("custom-worker", "", func(ctx context.Context, input string) (string, error) {
		sess := agent_v2.GetSession(ctx)
		if sess == nil || sess == session || sess.RunID != session.RunID || sess.RootSession() != session {
			return "", errors.New("子图的会话归属与调用时不一致")
		}
		if input == "准备" {
			preparations.Add(1)
			return "草稿-v1", nil
		}
		answer, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "是否写入 " + input + "？"})
		if err != nil {
			return "", err
		}
		if answer == nil || answer.Content != "确认 v1" || input != "草稿-v1" {
			return "", fmt.Errorf("写入收到错误的原输入或答复：%q %v", input, answer)
		}
		writes.Add(1)
		return "写入 " + input, nil
	})}

	output := compose_v2.LastValue[string]("output")
	stage := compose_v2.NewBuilder().WithName("custom-stage")
	compose_v2.WithChannel(stage, output)
	stage.AddNode("invoke", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		draft, err := agent_v2.RunSubAgent(ctx, "prepare", worker, schema.UserMessage("准备"))
		if err != nil {
			return nil, err
		}
		prepared := agent_v2.FinalMessage(draft)
		if prepared == nil {
			return nil, errors.New("缺少准备结果")
		}
		out, err := agent_v2.RunSubAgent(ctx, "write", worker, schema.UserMessage(prepared.Content))
		if err != nil {
			return nil, err
		}
		result := agent_v2.FinalMessage(out)
		if result == nil {
			return nil, errors.New("缺少写入结果")
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(output, result.Content)}, nil
	}).AddEdge(compose_v2.StartNode, "invoke").AddEdge("invoke", compose_v2.EndNode)
	child, err := stage.Compile()
	if err != nil {
		t.Fatal(err)
	}

	builder := compose_v2.NewBuilder().WithName("custom-parent")
	compose_v2.WithChannel(builder, output)
	builder.AddSubgraphNode("stage", child, compose_v2.WithExport(output)).
		AddEdge(compose_v2.StartNode, "stage").AddEdge("stage", compose_v2.EndNode)
	parent, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	host := agent_v2.NewGraphAgent(parent, agent_v2.WithGraphAgentName("custom-host"),
		agent_v2.WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		agent_v2.WithGraphOutputReader(func(ch compose_v2.Channels) *schema.Message {
			text, ok := compose_v2.Get(ch, output)
			if !ok {
				return nil
			}
			return schema.AssistantMessage("汇总 " + text)
		}))

	_, err = host.Run(ctx, session, schema.UserMessage("归档 v1"))
	pause, ok := compose_v2.IsInterrupt(err)
	if !ok || len(pause.AllQueries()) != 1 || preparations.Load() != 1 || writes.Load() != 0 {
		t.Fatalf("应先保留草稿并等待批准：prep=%d writes=%d err=%v", preparations.Load(), writes.Load(), err)
	}
	query := pause.AllQueries()[0]
	pending, err := cp.ListPending(ctx, session.RunID)
	if err != nil || len(pending) < 2 {
		t.Fatalf("父图和低阶子图应留下可恢复存档：%v err=%v", pending, err)
	}
	if query.InterruptID == "" || query.Question != "是否写入 草稿-v1？" {
		t.Fatalf("子图问题缺少原输入或身份：%+v", query)
	}
	assertCustomNestedHistory(t, loadHist(t, history, ctx, "c15-nested"), session.RunID, "write", schema.RoleUser, "草稿-v1")

	events, err := host.Resume(ctx, session.RunID, agent_v2.WithResumeSession(session),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{query.InterruptID: schema.UserMessage("确认 v1")}))
	if final := agent_v2.FinalMessage(events); err != nil || final == nil || final.Content != "汇总 写入 草稿-v1" {
		t.Fatalf("原答案应恢复低阶子图的原调用：final=%+v err=%v", final, err)
	}
	if preparations.Load() != 1 || writes.Load() != 1 || worker.resumes.Load() != 0 {
		t.Fatalf("只重入挂起的子 Run：prep=%d writes=%d childResume=%d", preparations.Load(), writes.Load(), worker.resumes.Load())
	}
	if slots, err := cp.LoadAllInSession(ctx, session.RunID); err != nil || len(slots) != 0 {
		t.Fatalf("成功应清理整个任务的存档：%v err=%v", slots, err)
	}

	msgs := loadHist(t, history, ctx, "c15-nested")
	assertCount(t, msgs, "归档 v1", 1)
	assertCount(t, msgs, "汇总 写入 草稿-v1", 1)
	assertCustomNestedHistory(t, msgs, session.RunID, "prepare", schema.RoleAssistant, "草稿-v1")
	assertCustomNestedHistory(t, msgs, session.RunID, "write", schema.RoleUser, "草稿-v1")
	assertCustomNestedHistory(t, msgs, session.RunID, "write", schema.RoleAssistant, "写入 草稿-v1")
	for _, event := range session.Events() {
		if event.Output != nil && event.Output.Message != nil && event.Output.Message.Content == "子会话私有审计" {
			t.Fatal("子会话的私有事件串入父会话")
		}
	}
}

func assertCustomNestedHistory(t *testing.T, msgs []*schema.Message, sid, slot string, role schema.MessageRole, content string) {
	t.Helper()
	count := 0
	for _, msg := range msgs {
		if msg.Role == role && msg.Content == content && msg.Annotations[agent_v2.HistoryAgentAnnotation] == slot &&
			msg.Annotations[agent_v2.HistoryTaskAnnotation] == sid {
			count++
		}
	}
	if count != 1 {
		t.Errorf("任务 %q 的 %s 应有一条 %s 记录 %q，实际 %d：%s", sid, slot, role, content, count, histDump(msgs))
	}
}
