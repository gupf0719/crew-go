package agent_v2_test

// 工具重复调用检测的计数链按任务分：上一个任务里的重复次数不累计到下一个任务。

import (
	"context"
	"sync/atomic"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

// reminderSpyModel 包住 boundRecordingModel，记下它收到的输入里有没有出现过重复调用提醒。
type reminderSpyModel struct {
	*boundRecordingModel
	saw atomic.Bool
}

func (m *reminderSpyModel) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	for _, msg := range in {
		if msg.Annotations[agent_v2.RepeatToolReminderAnnotation] != nil {
			m.saw.Store(true)
		}
	}
	return m.boundRecordingModel.Generate(ctx, in, opts...)
}

func (m *reminderSpyModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	if _, err := m.boundRecordingModel.WithTools(tools); err != nil {
		return nil, err
	}
	return m, nil
}

// repeatThen 先用同样的参数调 times 次 foo，再以文本收尾。
func repeatThen(times int) *reminderSpyModel {
	return &reminderSpyModel{boundRecordingModel: &boundRecordingModel{script: func(n int) *schema.Message {
		if n < times {
			return oneCall("foo", `{"x":1}`)
		}
		return schema.AssistantMessage("完成")
	}}}
}

// TestRepeatTool_ChainPerTask：任务 A 用同样参数调了 2 次；同一个 session 开任务 B（NextTask，或复用同一个 RunID 再跑
// 一次顶层 Run），B 只调 1 次，不该收到「连续重复」提醒——计数不跨任务累计。对照：同一个任务里连调 3 次会收到。曾经
// 计数链挂在会话上只按位置与 agent 名分，A 的 2 次加 B 的 1 次凑到阈值 3，B 第一次调就被提醒（审计实测）。
func TestRepeatTool_ChainPerTask(t *testing.T) {
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("foo", "foo", map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) { return tool.Success("ok"), nil }))
	newAgent := func(m model.ChatModel) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("rep_task"), agent_v2.WithModel(m), agent_v2.WithToolkit(kit),
			agent_v2.WithRepeatToolReminder(&agent_v2.RepeatToolConfig{Enabled: true}), agent_v2.WithMaxRounds(6))
	}

	ctrl := repeatThen(3)
	if _, err := newAgent(ctrl).Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("C")); err != nil {
		t.Fatal(err)
	}
	if !ctrl.saw.Load() {
		t.Fatal("fixture 没搭对：同一个任务里连调 3 次应收到提醒")
	}

	for _, nextTask := range []bool{true, false} {
		sess := agent_v2.NewRunSession()
		if _, err := newAgent(repeatThen(2)).Run(context.Background(), sess, schema.UserMessage("A")); err != nil {
			t.Fatal(err)
		}
		if nextTask {
			sess.NextTask()
		}
		b := repeatThen(1)
		if _, err := newAgent(b).Run(context.Background(), sess, schema.UserMessage("B")); err != nil {
			t.Fatal(err)
		}
		if b.saw.Load() {
			t.Errorf("NextTask=%v：任务 B 只调了 1 次，不该收到连续重复的提醒（上一个任务的次数累计过来了）", nextTask)
		}
	}
}
