package agui_test

// 守「没带 threadId 的请求不会留下没人回收的存档」。
//
// 这一层**无条件**给 ctx 挂 checkpointer，所以匿名请求一旦中断照样落存档；而回收动作
// （占用/覆盖清档、挂起登记、LRU 淘汰）当初全部挂在「threadID 非空」上 ⟹ 匿名整条绕过
// ⟹ 存档没人回收，持久后端下无界增长。修法是入口铸一个 id，让匿名态根本不存在。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/agui"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/schema"
)

// TestAnonymousRun_MintsThreadIDAndReclaims 连发 5 个不带 threadId 的中断请求，
// 存档不该累积。
//
// sanity-revert：把 ServeHTTP 里 `in.ThreadID = newThreadID()` 改成 no-op → 5 份存档
// 全留下（每份都是一个永远等不到答复、也没人回收的挂起）。
func TestAnonymousRun_MintsThreadIDAndReclaims(t *testing.T) {
	const rounds, maxSessions = 5, 2
	cper := compose_v2.NewInMemoryCheckpointer()
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(
			agent_v2.WithName("anon"),
			agent_v2.WithModel(&askThenAnswerModel{}),
			agent_v2.WithAskUser(),
			agent_v2.WithHooks(hooks),
			agent_v2.WithMaxRounds(3),
		)
	}, agui.WithCheckpointer(cper), agui.WithMaxSessions(maxSessions))

	seen := map[string]bool{}
	for i := 0; i < rounds; i++ {
		rec := postAGUI(h, `{"runId":"r","messages":[{"role":"user","content":"hi"}]}`)
		tid := threadIDFromRunStarted(t, rec.Body.String())
		if tid == "" {
			t.Fatalf("第 %d 轮：RunStarted 没回传 threadId——前端拿不到可续跑的句柄，铸了等于没铸", i+1)
		}
		if seen[tid] {
			t.Fatalf("第 %d 轮铸出了重复的 threadId %q——两个匿名会话会互相覆盖", i+1, tid)
		}
		seen[tid] = true
	}

	// **承重断言**：存档总数不随轮数线性涨。
	//
	// 铸了 id ⟹ 每份存档都有对应的 sessionEntry 受 WithMaxSessions 的 LRU 管，淘汰时
	// 连它的 cp 一起清；没铸 ⟹ 匿名整条绕过登记与淘汰，5 轮留 5 份**没有任何句柄能指向
	// 的**存档，持久后端下无界增长。
	//
	// 这里数的是「还剩几份」而不是「有没有中断」——中断是预期的（用户可能真的回来答），
	// 要防的是没人管得着的那种残留。Cleanup(0) 删全部并返删除条数，正好当计数器用。
	left, err := cper.Cleanup(context.Background(), 0)
	if err != nil {
		t.Fatalf("cleanup 计数: %v", err)
	}
	if int(left) > maxSessions+1 {
		t.Errorf("%d 轮匿名中断之后残留 %d 份存档（上限设的是 %d 个会话）——匿名会话绕过了回收",
			rounds, left, maxSessions)
	}
}

// TestResumeWithoutThreadID_Rejected 续跑不铸 id——那时 threadId 是客户端手上已有的
// 续跑句柄，缺了是客户端的错；铸一个只会静默开出一个空会话。
func TestResumeWithoutThreadID_Rejected(t *testing.T) {
	cper := compose_v2.NewInMemoryCheckpointer()
	h := ownedHandler(func(hooks hook.Hooks) agent_v2.Agent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("x"),
			agent_v2.WithModel(&askThenAnswerModel{}), agent_v2.WithHooks(hooks))
	}, agui.WithCheckpointer(cper))

	rec := postAGUI(h, `{"runId":"r","resume":[{"interruptId":"q","status":"resolved","payload":"y"}],`+
		`"messages":[{"role":"user","content":"hi"}]}`)
	body := rec.Body.String()
	if !strings.Contains(body, "no_thread_id") {
		t.Errorf("续跑没带 threadId 该拒并给专属 code，实际: %s", body)
	}
}

// threadIDFromRunStarted 从 SSE 流里取 RUN_STARTED 回传的 threadId。
func threadIDFromRunStarted(t *testing.T, sse string) string {
	t.Helper()
	for _, line := range strings.Split(sse, "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev); err != nil {
			continue
		}
		if ev["type"] == "RUN_STARTED" {
			s, _ := ev["threadId"].(string)
			return s
		}
	}
	return ""
}

// zzErrAgent 是个只负责返回指定错误的 agent，用来验错误分派。
type zzErrAgent struct{ err error }

func (a *zzErrAgent) Run(context.Context, *agent_v2.RunSession, *schema.Message, ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return nil, a.err
}
func (a *zzErrAgent) Resume(context.Context, string, ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, a.err
}
func (a *zzErrAgent) Name() string        { return "zz_err" }
func (a *zzErrAgent) Description() string { return "" }

// TestRunError_HasMachineReadableCode 守「最常走的那条错误分支要给 code」。
//
// 前端要在**三条相反的动作**之间选：会话结束了该开新的 / 终局失败别再试 / 可以原样重来。
// 没有 code 就只剩一段人读的文本，这三条分不出来。
//
// sanity-revert：把 `agentErrorCode(err)` 换回 `""` → 三个子用例全红。
func TestRunError_HasMachineReadableCode(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"会话没有在等你的东西", fmt.Errorf("x: %w", compose_v2.ErrNoPendingCheckpoint), "no_pending_checkpoint"},
		{"终局失败别再试", fmt.Errorf("x: %w", compose_v2.ErrRunAborted), "run_aborted"},
		{"其余可能是瞬时的", errors.New("下游 503"), "agent_error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := ownedHandler(func(hook.Hooks) agent_v2.Agent { return &zzErrAgent{err: tc.err} })
			body := postAGUI(h, `{"threadId":"t","runId":"r","messages":[{"role":"user","content":"hi"}]}`).Body.String()
			if !strings.Contains(body, `"code":"`+tc.want+`"`) {
				t.Errorf("该给 code=%q，实际: %s", tc.want, body)
			}
		})
	}
}
