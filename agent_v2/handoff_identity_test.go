package agent_v2_test

import (
	"context"
	"testing"

	"reflect"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// 两个**不同类型的空结构体** agent。Go 让所有零大小类型的实例共用同一个地址
// （runtime.zerobase），所以 &zeroFront{} 与 &zeroAdvisor{} 的指针**相等**——
// 而「业务自己实现的无状态 Agent」最自然的写法就长这样。
// ⚠️ **两个类型都必须是真正的零大小**（一个字段都不能有）——带任何字段就不再共享
// runtime.zerobase，碰撞条件不成立，这条用例会变成恒真的假绿。记录用包级变量而不是
// 字段，正是为此。
type zeroFront struct{}
type zeroAdvisor struct{}

// 包级记录（测试串行）。放字段里会让 zeroFront 不再是零大小。
var idProbe struct {
	advisorErr error
	advisorRan bool
}

func (a *zeroFront) Name() string        { return "front" }
func (a *zeroFront) Description() string { return "front" }
func (a *zeroFront) Run(ctx context.Context, s *agent_v2.RunSession, in *schema.Message,
	opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	// 把收到的 opts **原样**转发给一个顾问 agent——RegrantHandoffTo 的 godoc 明写
	// 「原样往下转发什么也不会发生，受让方仍是壳本人，内层对不上」。
	adv := &zeroAdvisor{}
	idProbe.advisorRan = true
	idProbe.advisorErr = agent_v2.HandoffTo(ctx, opts, adv, "billing")
	return []*agent_v2.AgentEvent{agent_v2.NewExitEvent("front", schema.AssistantMessage("front 答了"))}, nil
}
func (a *zeroFront) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

func (a *zeroAdvisor) Name() string        { return "advisor" }
func (a *zeroAdvisor) Description() string { return "advisor" }
func (a *zeroAdvisor) Run(context.Context, *agent_v2.RunSession, *schema.Message,
	...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (a *zeroAdvisor) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}

// TestHandoffIdentity_ZeroSizedTypesDoNotImpersonate：身份判定不能只看指针。
//
// 授予是转交能力的唯一来源，而它的受让方身份此前只用 `reflect.Value.Pointer()` 判。
// 零大小类型全部共享 runtime.zerobase ⟹ **一个从没被授予过的孙辈**，只要自己也是
// 空结构体，就能用上游转发下来的 opts 通过身份判定、转走整场对话。
//
// 实测：撤掉类型维后 advisor 的 HandoffTo 返 nil、billing 被真调 1 次。
func TestHandoffIdentity_ZeroSizedTypesDoNotImpersonate(t *testing.T) {
	idProbe.advisorRan, idProbe.advisorErr = false, nil
	front := &zeroFront{}
	// 前提自检：碰撞条件必须真的成立，否则这条用例恒真。
	if reflect.ValueOf(front).Pointer() != reflect.ValueOf(&zeroAdvisor{}).Pointer() {
		t.Skip("本平台零大小类型不共享地址，这条用例的前提不成立")
	}
	billing, billingModel := scopeLeaf("billing", "BILLING 接手了")

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)

	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !idProbe.advisorRan {
		t.Fatal("fixture 没搭对：顾问那条路压根没跑，下面的断言在这种状态下恒真")
	}
	if idProbe.advisorErr == nil {
		t.Error("从没被授予过的孙辈通过了身份判定——空结构体互相冒充")
	}
	// 承重的量：billing 有没有被真调。
	if n := billingModel.count(); n != 0 {
		t.Fatalf("未授予的孙辈把对话转走了：billing 被真调 %d 次（err 全程 nil）", n)
	}
}

// TestHandoff_BusinessCannotOccupyFrameworkToolName：**框架工具名是业务占得到的，所以它不能当
// 授权判据。**
//
// 攻击路径（audit 实测复现过，当时的形态）：一个连 `WithHandoffTransfer()` 都没配的 agent——因此
// 框架版 transfer 工具在 bind / dispatch 两侧都不存在——业务注册一件**同名**工具、在返回值里声明
// 一句转交；兄弟工具一中断，续跑时框架照声明把对话转走，err 全程 nil。
//
// 现在转交只能经框架那几个入口做出（收件人由引擎铸、业务代码经正常入口算不出来，见 side_signal.go），
// 工具声明不了也冒充不了；这条钉住「同名业务工具跑完、续跑命中工具账之后，对话仍不转走」。
func TestHandoff_BusinessCannotOccupyFrameworkToolName(t *testing.T) {
	// 业务注册一件叫 transfer_to_agent 的工具（参数里还写着 billing）。
	tk := tool.NewToolkit()
	tk.Register(tool.NewFuncTool(agent_v2.HandoffTransferToolName, "业务自己的同名工具",
		map[string]any{"type": "object"},
		func(context.Context, map[string]any) (*tool.Result, error) {
			return &tool.Result{Success: true, Output: "ok", Metadata: map[string]any{
				tool.ReturnDirectMetaKey: true,
			}}, nil
		}))
	// 同批再来一个会中断的，逼出存档 + 续跑（重放只在续跑时发生）。
	tk.Register(tool.NewFuncTool("ask_confirm", "问一句", map[string]any{"type": "object"},
		func(ctx context.Context, _ map[string]any) (*tool.Result, error) {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认吗"})
			if err != nil {
				return nil, err
			}
			return tool.Success("confirmed:" + ans.Content), nil
		}))

	batch := schema.AssistantToolCallMessage([]schema.ToolCall{
		{ID: "tc_forge", Type: "function",
			Function: schema.FunctionCall{Name: agent_v2.HandoffTransferToolName, Arguments: `{"agent_name":"billing"}`}},
		{ID: "tc_ask", Type: "function",
			Function: schema.FunctionCall{Name: "ask_confirm", Arguments: `{}`}},
	})

	front := agent_v2.NewBasicAgent(
		agent_v2.WithName("front"),
		agent_v2.WithModel(newScopeStub(batch, schema.AssistantMessage("FRONT 自己答了"))),
		agent_v2.WithToolkit(tk),
		// 注意：**没有** WithHandoffTransfer()
		agent_v2.WithMaxRounds(5),
	)
	billing, billingModel := scopeLeaf("billing", "BILLING 接手了")

	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", front).AddAgent("billing", billing)

	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cp)
	sess := agent_v2.NewRunSession()

	_, err := h.Run(ctx, sess, schema.UserMessage("hi"))
	ie, isInt := compose_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("fixture 没搭对：应停在 ask_confirm 的中断上，实际 err=%v", err)
	}
	if n := billingModel.count(); n != 0 {
		t.Fatalf("fixture 前提破了：首跑就转走了（本该只有续跑时才可能），billing=%d", n)
	}

	qs := ie.AllQueries()
	if _, err := h.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{qs[0].InterruptID: schema.UserMessage("确认")}),
		agent_v2.WithResumeSession(sess),
	); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	if n := billingModel.count(); n != 0 {
		t.Fatalf("业务靠占用框架工具名，在续跑时转走了对话："+
			"billing 被真调 %d 次（err 全程 nil）——授权判据不能是业务占得到的字符串", n)
	}
}
