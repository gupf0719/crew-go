package agent_v2_test

import (
	"context"
	"testing"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// ───────── 值类型 self 不许 panic（曾经会）─────────

type idValShell struct {
	tags  map[string]string // 让这个类型**不可比较**
	inner agent_v2.Agent
	name  string
}

func (w idValShell) Run(ctx context.Context, s *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	// godoc 里推荐的那一行——它自己就是当年的触发路径
	return w.inner.Run(ctx, s, in, agent_v2.RegrantHandoffTo(opts, w, w.inner)...)
}
func (w idValShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w idValShell) Name() string        { return w.name }
func (w idValShell) Description() string { return "值类型业务壳" }

// 业务把 Agent 实现在**含 map 字段的 struct 值**上（合法 Go）时，照 godoc 写不许炸。
//
// 曾经的表现：`node panic recovered: runtime error: comparing uncomparable type ...`
// ——interface `==` 在两边动态类型相同且不可比较时 panic，而**命中的恰好是正确用法**
// （写错时类型不同反而安全）。同一份 godoc 里还有一整节论证「比较为什么不会 panic」，
// 那个论证从 RegrantHandoffTo 被导出那一刻起就不成立了：self 的类型由业务决定。
func TestGrantIdentity_ValueTypeSelfDoesNotPanic(t *testing.T) {
	inner, innerModel := grantInnerTransfer("inner", "billing")
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", idValShell{tags: map[string]string{"k": "v"}, inner: inner, name: "front"}).
		AddAgent("billing", billing)

	_, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi"))
	t.Logf("值类型 self：err=%v inner 模型=%d billing=%d", err, innerModel.count(), billingModel.count())
	if err != nil {
		t.Errorf("值类型 self 不许炸：err=%v——判据必须避开 interface ==（那条在不可比较类型上 panic）", err)
	}
}

// ───────── 嵌入壳零代码继承资格（曾经静默失去）─────────

type idEmbedShell struct{ *agent_v2.BasicAgent }

// `type Shell struct{ *BasicAgent }` 是 Go 里表达「这一层**是**那个 agent 加点东西」
// 的标准写法——方法集就是内层的、`Run` 是提升上来的、接收者是内层。
//
// 曾经的表现：grantee 是外层 `*Shell`、自查时的 self 是内层 `*BasicAgent`，指针不等
// ⟹ **静默失去转交能力**。而这一层**没有地方**写 RegrantHandoffTo——要写就得覆写 Run，
// 等于放弃嵌入。也就是说：业务用语言本身声明了「这一层是我的组成部分」，反而被惩罚。
func TestGrantIdentity_EmbeddedShellInheritsGrant(t *testing.T) {
	inner, innerModel := grantInnerTransfer("front", "billing")
	bi, ok := inner.(*agent_v2.BasicAgent)
	if !ok {
		t.Fatalf("fixture 没搭对：inner 不是 *BasicAgent（%T），嵌入形态搭不起来", inner)
	}
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", &idEmbedShell{BasicAgent: bi}).AddAgent("billing", billing)

	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("嵌入壳：inner 模型=%d billing=%d", innerModel.count(), billingModel.count())
	if billingModel.count() != 1 {
		t.Errorf("嵌入壳静默失去转交能力：billing 被真调 %d 次，期望 1——"+
			"嵌入在 Go 里就是「我就是它」，判据必须沿匿名字段走", billingModel.count())
	}
}

// ───────── 具名字段包装仍然不穿（转发 ≠ 授予）─────────

// ⚠️ 字段**导出**是刻意的：未导出字段 `CanInterface()` 为假、会被 innerOf 直接跳过，
// 那样「具名字段不该穿」这条就没被真正走到——变异「连具名字段也走」时用例照绿（栽过）。
type idNamedShell struct {
	Inner agent_v2.Agent // 导出，让 innerOf 真的看得见它
	name  string
}

func (w *idNamedShell) Run(ctx context.Context, s *agent_v2.RunSession, in *schema.Message, opts ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return w.Inner.Run(ctx, s, in, opts...) // 只转发，不授予
}
func (w *idNamedShell) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, nil
}
func (w *idNamedShell) Name() string        { return w.name }
func (w *idNamedShell) Description() string { return "具名字段包装" }

// **具名字段是「我有一个」不是「我是一个」**——放宽到嵌入之后，这条边界不能跟着松。
// 它红了说明 sameEntity 走过头了：连 has-a 都认，那「转发 ≠ 授予」当场作废。
func TestGrantIdentity_NamedFieldStillBlocked(t *testing.T) {
	inner, innerModel := grantInnerTransfer("inner", "billing")
	billing, billingModel := brkLeaf("billing", "BILLING 接手了")
	h := agent_v2.NewHandoffAgent(
		agent_v2.WithHandoffName("router"),
		agent_v2.WithHandoffInitial("front"),
	).AddAgent("front", &idNamedShell{Inner: inner, name: "front"}).AddAgent("billing", billing)

	if _, err := h.Run(context.Background(), agent_v2.NewRunSession(), schema.UserMessage("hi")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("具名字段包装：inner 模型=%d billing=%d", innerModel.count(), billingModel.count())
	if billingModel.count() != 0 {
		t.Errorf("具名字段包装穿透：billing 被真调 %d 次，期望 0——"+
			"has-a 不该继承资格，否则「转发 ≠ 授予」作废", billingModel.count())
	}
}
