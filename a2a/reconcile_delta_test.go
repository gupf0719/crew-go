package a2a_test

// 按快照或重新挂上收尾时，取回的答复等于正常续任务时这一轮的流会收到的内容：远端往问人之前就有的 artifact 里追加，只收
// 追加的那段；整段替换了，收新的全文；问人之前流出过、这一轮没动的不收。曾经只按 artifact ID 整段跳过，远端跨轮往同一个
// artifact 里追加时这一轮的内容整段被跳掉、答复变空（err == nil，审计实测）。另：GetTask 报确定性错误（鉴权、参数）时照常
// 发，不当成「查不到状态」一直卡着。

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// docRemote：整个任务只用一个固定 ID 的 artifact。新任务先写 "draft;"，再要人；续任务时 appendMode 决定是往里追加
// "result:<答复>" 还是整段换成 "final:<答复>"，完成状态不带消息（答复只在 artifact 里）。
func docRemote(r *recRemote, appendMode bool) a2asrv.AgentExecutor {
	return fnExec(func(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		txt := inputText(execCtx)
		if execCtx.StoredTask == nil {
			r.record("new:" + txt)
			if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
				return
			}
			first := a2atypes.NewArtifactUpdateEvent(execCtx, "doc", a2atypes.NewTextPart("draft;"))
			first.Append = false
			if !yield(first, nil) {
				return
			}
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("批准吗"))), nil)
			return
		}
		r.record("cont:" + txt)
		var ev *a2atypes.TaskArtifactUpdateEvent
		if appendMode {
			ev = a2atypes.NewArtifactUpdateEvent(execCtx, "doc", a2atypes.NewTextPart("result:"+txt))
		} else {
			ev = a2atypes.NewArtifactUpdateEvent(execCtx, "doc", a2atypes.NewTextPart("final:"+txt))
			ev.Append = false
		}
		if !yield(ev, nil) {
			return
		}
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted, nil), nil)
	})
}

func TestHumanGate_LostResponse_ReusedArtifactYieldsThisRound(t *testing.T) {
	for _, tc := range []struct {
		name       string
		appendMode bool
		want       string
	}{{"追加", true, "result:答#1"}, {"整段替换", false, "final:答#1"}} {
		t.Run(tc.name, func(t *testing.T) {
			normalURL := interceptServer(t, docRemote(&recRemote{}, tc.appendMode), func(http.ResponseWriter, *http.Request, []byte, http.Handler) bool { return false })
			_, normal, err := driveGate(t, normalURL)
			if err != nil || normal != tc.want {
				t.Fatalf("对照组（不丢响应）应收到 %q，实际 %q err=%v", tc.want, normal, err)
			}
			var lost atomic.Bool
			lossyURL := interceptServer(t, docRemote(&recRemote{}, tc.appendMode), loseFirstContinuation(&lost))
			_, recovered, err := driveGate(t, lossyURL)
			if !lost.Load() || err != nil {
				t.Fatalf("fixture 没搭对或没收敛：lost=%v err=%v", lost.Load(), err)
			}
			if recovered != normal {
				t.Fatalf("取回的答复应跟正常收尾一样（%q），实际 %q", normal, recovered)
			}
		})
	}
}

// 远端先流出一段 artifact，再要授权；人批准之后重新挂上任务读结果：授权之前就有的那段不算这一轮的答复。
func TestRemoteAgent_AuthReattach_SkipsEarlierArtifacts(t *testing.T) {
	granted := make(chan struct{})
	done := make(chan struct{})
	exec := fnExec(func(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
			return
		}
		if !yield(a2atypes.NewArtifactEvent(execCtx, a2atypes.NewTextPart("PRE.")), nil) {
			return
		}
		if !yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateAuthRequired,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("请到 https://auth.example/x 授权"))), nil) {
			return
		}
		<-granted
		if !yield(a2atypes.NewArtifactEvent(execCtx, a2atypes.NewTextPart("报表内容")), nil) {
			return
		}
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted, nil), nil)
		close(done)
	})
	url := interceptServer(t, exec, func(http.ResponseWriter, *http.Request, []byte, http.Handler) bool { return false })
	_, final, err := driveAuth(t, url, func() {
		close(granted)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("fixture 没搭对：远端拿到凭证之后应跑完")
		}
	})
	if err != nil || final != "报表内容" {
		t.Fatalf("应只拿到授权之后的产出（报表内容），实际 %q err=%v", final, err)
	}
}

// 答人那一问之前查任务状态，远端的 GetTask 一直报鉴权失败（确定性错误、再查也一样）：照常发答复、续上任务，不一直卡着。
func TestHumanGate_GetTaskDeterministicErrorStillSends(t *testing.T) {
	r := &recRemote{}
	exec := fnExec(func(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		txt := inputText(execCtx)
		if execCtx.StoredTask == nil {
			r.record("new:" + txt)
			if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
				return
			}
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("批准吗"))), nil)
			return
		}
		r.record("cont:" + txt)
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("done:"+txt))), nil)
	})
	var denied atomic.Int32
	url := interceptServer(t, exec, func(w http.ResponseWriter, req *http.Request, body []byte, _ http.Handler) bool {
		var rpc struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
		}
		if json.Unmarshal(body, &rpc) != nil || rpc.Method != "GetTask" || !bytes.Contains(body, []byte(`"id"`)) {
			return false
		}
		denied.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": rpc.ID,
			"error": map[string]any{"code": -31403, "message": "permission denied"}})
		return true
	})
	asked, final, err := driveGate(t, url)
	if denied.Load() == 0 {
		t.Fatal("fixture 没搭对：GetTask 一次都没被拒过")
	}
	if err != nil || final != "done:答#1" || len(asked) != 1 {
		t.Fatalf("GetTask 被拒（确定性）时应照常发答复、续上任务：err=%v final=%q 问过 %q", err, final, asked)
	}
}

// driveAuth 把默认模式的 RemoteAgent 放进 SequentialAgent，跑到它停在授权请求上；beforeResume 之后答「已授权」续跑。
func driveAuth(t *testing.T, url string, beforeResume func()) (asked []string, final string, err error) {
	t.Helper()
	ra, err := a2a.NewRemoteAgent(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("auth_seq")).AddStep("remote", ra)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err = seq.Run(ctx, sess, schema.UserMessage("出报表"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok || len(ie.AllQueries()) != 1 {
		t.Fatalf("前置：远端要授权时应交给真人一问，got err=%v", err)
	}
	q := ie.AllQueries()[0]
	asked = append(asked, q.Question)
	beforeResume()
	evs, err := seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{q.InterruptID: schema.UserMessage("已授权")}))
	if m := agent_v2.FinalMessage(evs); m != nil {
		final = strings.TrimSpace(m.Content)
	}
	return asked, final, err
}

// chartRemote：同一个 artifact 正文不变（"Chart:"），只把数据块从 {v:1} 整段换成 {v:2}，完成状态不带消息。
func chartRemote() a2asrv.AgentExecutor {
	return fnExec(func(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		chart := func(v int) *a2atypes.TaskArtifactUpdateEvent {
			ev := a2atypes.NewArtifactUpdateEvent(execCtx, "chart", a2atypes.NewTextPart("Chart:"), a2atypes.NewDataPart(map[string]any{"v": v}))
			ev.Append = false
			return ev
		}
		if execCtx.StoredTask == nil {
			if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) || !yield(chart(1), nil) {
				return
			}
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("批准吗"))), nil)
			return
		}
		if !yield(chart(2), nil) {
			return
		}
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted, nil), nil)
	})
}

// gateFinal 把开了 human gate 的 RemoteAgent 放进 SequentialAgent（重试 2 次），问到人就答「批准」续跑，返回最终正文 +
// 内容块的 JSON。
func gateFinal(t *testing.T, url string) (string, error) {
	t.Helper()
	ra, err := a2a.NewRemoteAgent(context.Background(), url, a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	seq := agent_v2.NewSequentialAgent(agent_v2.WithSequentialName("gate_seq"),
		agent_v2.WithSequentialRetryConfig(compose_v2.SimpleRetry(2))).AddStep("remote", ra)
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	answers := map[string]*schema.Message{}
	evs, err := seq.Run(ctx, sess, schema.UserMessage("请求"))
	for round := 0; round < 4 && err != nil; round++ {
		if ie, ok := compose_v2.IsInterrupt(err); ok {
			for _, q := range ie.AllQueries() {
				answers[q.InterruptID] = schema.UserMessage("批准")
			}
		}
		evs, err = seq.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(answers))
	}
	m := agent_v2.FinalMessage(evs)
	if m == nil {
		return "", err
	}
	raw, jerr := json.Marshal(m.ContentBlocks)
	if jerr != nil {
		t.Fatalf("编码 ContentBlocks：%v", jerr)
	}
	return m.Content + " " + string(raw), err
}

// recoveredMatchesNormal 让同一个远端跑两遍：一遍正常收尾，一遍丢掉第一次续任务的响应、按快照收尾。两遍的答复应一样。
func recoveredMatchesNormal(t *testing.T, exec func() a2asrv.AgentExecutor) (normal string) {
	t.Helper()
	normal, err := gateFinal(t, interceptServer(t, exec(), func(http.ResponseWriter, *http.Request, []byte, http.Handler) bool { return false }))
	if err != nil {
		t.Fatalf("对照组（不丢响应）出错：%v", err)
	}
	var lost atomic.Bool
	recovered, err := gateFinal(t, interceptServer(t, exec(), loseFirstContinuation(&lost)))
	if !lost.Load() || err != nil {
		t.Fatalf("fixture 没搭对或没收敛：lost=%v err=%v", lost.Load(), err)
	}
	if recovered != normal {
		t.Fatalf("取回的答复应跟正常收尾一样（%s），实际 %s", normal, recovered)
	}
	return normal
}

// 正文没变、只换了数据块的整段替换：取回的答复跟正常收尾一样，带着新的数据块（曾经块只比个数，判成没动、答复变空，审计实测）。
func TestHumanGate_LostResponse_BlockReplacedSameText(t *testing.T) {
	if normal := recoveredMatchesNormal(t, chartRemote); !strings.Contains(normal, `"v":2`) {
		t.Fatalf("对照组应收到新的数据块，实际 %q", normal)
	}
}

// appendDocRemote：新任务往 "doc" 写 first（整段），要人；续任务往同一个 artifact 追加 more，完成。
func appendDocRemote(first, more []*a2atypes.Part) func() a2asrv.AgentExecutor {
	return func() a2asrv.AgentExecutor {
		return fnExec(func(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
			if execCtx.StoredTask == nil {
				if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
					return
				}
				ev := a2atypes.NewArtifactUpdateEvent(execCtx, "doc", first...)
				ev.Append = false
				if !yield(ev, nil) {
					return
				}
				yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired,
					a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("批准吗"))), nil)
				return
			}
			ev := a2atypes.NewArtifactUpdateEvent(execCtx, "doc", more...)
			ev.Append = true
			if !yield(ev, nil) {
				return
			}
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted, nil), nil)
		})
	}
}

// 这一轮往问人之前就有的 artifact 里追加：取回的只有这一轮追加的部分。问人时没有块、这一轮加了块那格曾把旧正文也收进来
// ——没有块时记下的哈希是 nil 切片的形状、取回时是空切片的，对不上就当成整段替换（审计实测）。
func TestHumanGate_LostResponse_AppendAfterAskYieldsOnlyThisRound(t *testing.T) {
	for _, tc := range []struct {
		name        string
		first, more []*a2atypes.Part
	}{
		{"问人时没有块、这一轮追加了块",
			[]*a2atypes.Part{a2atypes.NewTextPart("draft;")},
			[]*a2atypes.Part{a2atypes.NewTextPart("result;"), a2atypes.NewDataPart(map[string]any{"v": 1})}},
		{"问人时有块、这一轮只追加正文",
			[]*a2atypes.Part{a2atypes.NewTextPart("draft;"), a2atypes.NewDataPart(map[string]any{"v": 1})},
			[]*a2atypes.Part{a2atypes.NewTextPart("result;")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if normal := recoveredMatchesNormal(t, appendDocRemote(tc.first, tc.more)); strings.Contains(normal, "draft;") {
				t.Fatalf("对照组不该含问人之前的正文，实际 %q", normal)
			}
		})
	}
}
