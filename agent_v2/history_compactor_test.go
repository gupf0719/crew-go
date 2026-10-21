package agent_v2_test

import (
	"context"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/model"
	"crew-go/schema"
)

// TestMaxHistoryPairs_TruncatesPriorMessages 验证 MaxHistoryPairs 简单截断。
//
// **R11 跨仓 audit A2 修订（第 5 类炸弹极端版：测试体根本没断言）**：
// 旧版只跑两次 Run 看 err nil 就过——stub 不读 input、零断言、Run nil err 是基本
// pass 条件，sanity-revert 完全删除截断逻辑测试照样 PASS（HIGH 假绿）。
//
// 新版用 inputCaptureStub 捕获 stub 每次调用时收到的 msgs 长度——跑 3 次 Run 让
// prior 累积到 2 对（4 条）超 MaxHistoryPairs=1（应截到 1 对 2 条）。第 3 次 stub
// 收到 msgs len 必须 ≤ 1+2+1 = 4（含潜在 system + 截后 prior 2 + new input 1）。
// sanity-revert 删 basic_agent.go 截断逻辑后 prior 不截 → msgs len > 4 → fail。
func TestMaxHistoryPairs_TruncatesPriorMessages(t *testing.T) {
	stub := &inputCaptureStub{
		responses: []*schema.Message{
			schema.AssistantMessage("first"),
			schema.AssistantMessage("second"),
			schema.AssistantMessage("third"),
		},
	}
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(stub),
		agent_v2.WithMaxHistoryPairs(1),
	)
	sess := agent_v2.NewRunSession()

	// 跑 3 次让 prior 累积 2 对（q1+a1, q2+a2）超 MaxHistoryPairs=1
	for i, q := range []string{"q1", "q2", "q3"} {
		if _, err := ag.Run(context.Background(), sess, schema.UserMessage(q)); err != nil {
			t.Fatalf("Run %d: %v", i+1, err)
		}
	}

	// **R11 A2 关键断言**：stub 共被调 3 次，第 3 次（lastMsgsLen[2]）收到的 msgs：
	//   - 不截断：prior(4) + input(1) = 5 条（也可能 + system，>=5）
	//   - 截断生效（MaxHistoryPairs=1 → prior 截到 1 对 2 条）：≤ 1(system) + 2(prior) + 1(input) = 4
	if len(stub.lastMsgsLen) != 3 {
		t.Fatalf("stub 应被调 3 次，实际 %d 次", len(stub.lastMsgsLen))
	}
	thirdLen := stub.lastMsgsLen[2]
	if thirdLen > 4 {
		t.Errorf("R11 A2 回归：MaxHistoryPairs=1 应让第 3 次 stub 收到 ≤ 4 条 messages，"+
			"实际 %d 条——截断逻辑可能失效\n  全部 stub 调用 msgs len: %v",
			thirdLen, stub.lastMsgsLen)
	}
}

// inputCaptureStub 是 model.ChatModel mock——按 calls 索引返响应 + 记录每次收到的 msgs 长度。
//
// 跟 sequential_agent_test.go::captureInputStub 区别：那个捕获 last user content 单字符串；
// 这个捕获每次调用 msgs 长度的完整序列（验"截断后第 N 次 input 长度上限"）。
type inputCaptureStub struct {
	responses   []*schema.Message
	calls       int
	lastMsgsLen []int // 每次 Generate 调用时收到的 msgs 长度
}

func (s *inputCaptureStub) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	s.lastMsgsLen = append(s.lastMsgsLen, len(msgs))
	if s.calls < len(s.responses) {
		out := s.responses[s.calls]
		s.calls++
		return out, nil
	}
	s.calls++
	return schema.AssistantMessage("[exhausted]"), nil
}

func (s *inputCaptureStub) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, nil
}
func (s *inputCaptureStub) WithTools(_ []*schema.ToolInfo) (model.ChatModel, error) { return s, nil }

var _ model.ChatModel = (*inputCaptureStub)(nil)

// TestMaxHistoryPairs_DefaultOn 验证 defaultConfig **默认开启** MaxHistoryPairs=20 兜底
// 截断——**不配** WithMaxHistoryPairs 时，复用同 session 的跨 Run 历史超 20 对（40 条）
// 也自动截断到最后 20 对。
//
// **行为变更回归**：defaultConfig 的 MaxHistoryPairs 从 0（禁用）改为 defaultMaxHistoryPairs
// =20（默认开启）。此前不配 option = 无截断、长会话复用 session 时 ctx 无上限累积。
//
// 设计：**不配** WithMaxHistoryPairs（走默认 20）+ 无 system prompt（避免干扰计数），复用同
// session 跑 30 轮——每轮往 session 加 1 对历史（user input + assistant final），第 30 轮
// prior = 2*29 = 58 条 > 40 → 截到最后 40 条 → 第 30 次 stub 收到 msgs = 40(prior 截后) +
// 1(input) = 41 条。断言 <= 45（留 buffer）。
//
// **sanity-revert**：把 defaultConfig 里 `MaxHistoryPairs: defaultMaxHistoryPairs` 删掉
// （默认回 0=禁用）后，prior 不截 = 58 条 → 第 30 次 msgs = 59 > 45 → 本测试 FAIL，精确
// 指向"默认截断未生效"。区别显式档 TestMaxHistoryPairs_TruncatesPriorMessages（WithMaxHistoryPairs(1)），
// 本测试专钉**默认档**（不传 option）行为。
func TestMaxHistoryPairs_DefaultOn(t *testing.T) {
	stub := &inputCaptureStub{} // responses 空 → 全返无 tool_call 的 "[exhausted]"，每轮 1 次 model 调用即终止
	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(stub),
		// **不配** WithMaxHistoryPairs——验证 defaultConfig 默认 20 生效
	)
	sess := agent_v2.NewRunSession()

	const rounds = 30 // 第 30 轮 prior = 2*29 = 58 条 > 40 → 触发默认 20 截断
	for i := 0; i < rounds; i++ {
		if _, err := ag.Run(context.Background(), sess, schema.UserMessage("q")); err != nil {
			t.Fatalf("Run %d: %v", i+1, err)
		}
	}
	if len(stub.lastMsgsLen) != rounds {
		t.Fatalf("stub 应被调 %d 次，实际 %d 次", rounds, len(stub.lastMsgsLen))
	}
	// 第 30 次（最后一次）：默认 20 截断生效 → prior 截到 40 + input 1 = 41；
	// 默认禁用（revert）→ prior 58 + input 1 = 59。断 <= 45 精确区分。
	last := stub.lastMsgsLen[rounds-1]
	if last > 45 {
		t.Errorf("默认 MaxHistoryPairs=20 应让第 %d 轮 stub 收到 <= 45 条（prior 截到 40 + input 1 = 41），"+
			"实际 %d 条——defaultConfig 默认截断未生效（是否 revert 了 defaultConfig 的 MaxHistoryPairs）\n  全部 stub 调用 msgs len: %v",
			rounds, last, stub.lastMsgsLen)
	}
}

// TestHistoryCompactor_CalledWhenSet 验证自定义 HistoryCompactor 被调用。
func TestHistoryCompactor_CalledWhenSet(t *testing.T) {
	stub := newStubModel([]*schema.Message{
		schema.AssistantMessage("a1"),
		schema.AssistantMessage("a2"),
	})
	called := 0
	compactor := func(_ context.Context, msgs []*schema.Message, keepPairs int) ([]*schema.Message, error) {
		called++
		// 测试用：返空 slice，让 BasicAgent 完全无 prior
		return nil, nil
	}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(stub),
		agent_v2.WithHistoryCompactor(compactor),
		agent_v2.WithMaxHistoryPairs(5), // 作 keepPairs hint
	)
	sess := agent_v2.NewRunSession()

	// 跑 2 次让 prior 非空触发 compactor（第 2 次进入时 prior=Run1 的 turns）
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("q1")); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if _, err := ag.Run(context.Background(), sess, schema.UserMessage("q2")); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if called < 1 {
		t.Errorf("compactor not called")
	}
}

// TestHistoryCompactor_ErrorPropagates 验证 compactor error 终止 Run。
func TestHistoryCompactor_ErrorPropagates(t *testing.T) {
	stub := newStubModel([]*schema.Message{
		schema.AssistantMessage("a1"),
		schema.AssistantMessage("a2"),
	})

	compactor := func(_ context.Context, _ []*schema.Message, _ int) ([]*schema.Message, error) {
		return nil, &compactorErr{}
	}

	ag := agent_v2.NewBasicAgent(
		agent_v2.WithName("ag"),
		agent_v2.WithModel(stub),
		agent_v2.WithHistoryCompactor(compactor),
	)
	sess := agent_v2.NewRunSession()

	// Run 1 无 prior history，compactor 不会失败；先跑一次产生 history
	_, err := ag.Run(context.Background(), sess, schema.UserMessage("q1"))
	if err != nil {
		t.Logf("Run 1 may fail since prior is empty - skipping check: %v", err)
	}
	// Run 2 有 prior → compactor 触发返 err → Run 应失败
	_, err = ag.Run(context.Background(), sess, schema.UserMessage("q2"))
	if err == nil || !strings.Contains(err.Error(), "history compactor") {
		t.Errorf("expect compactor err, got %v", err)
	}
}

type compactorErr struct{}

func (c *compactorErr) Error() string { return "compactor failed" }
