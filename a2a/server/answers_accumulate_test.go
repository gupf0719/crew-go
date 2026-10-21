package server_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/a2a"
	"crew-go/a2a/server"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

// 续发的答复按任务累积：每次续发都把这个任务里答过的全部交给 Resume。没进工具账的提问（ask_user、节点里直接 Ask
// 的）续跑时会重问，只交这一条消息里的答复的话，一个节点先后问两问、或挂着两问只答其一，都会来回重问、永远答不完
// （实测：Q-1、Q-2 交替出现，任务一直停在 input-required）。

// seqAsker 是一个先后问两问的节点（先确认金额、再确认收款人这种），asked 记每一问被问了几次。
func seqAsker(asked map[string]*atomic.Int32) agent_v2.Agent {
	inCh := compose_v2.LastValue[*schema.Message]("__seq_in__")
	outCh := compose_v2.LastValue[*schema.Message]("__seq_out__")
	bld := compose_v2.NewBuilder()
	compose_v2.WithChannel(bld, inCh)
	compose_v2.WithChannel(bld, outCh)
	bld.AddNode("ask2", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		var got []string
		for _, q := range []string{"Q-1?", "Q-2?"} {
			ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: q})
			if err != nil {
				asked[q].Add(1)
				return nil, err
			}
			got = append(got, ans.Content)
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage("got: "+strings.Join(got, "+")))}, nil
	})
	bld.AddEdge(compose_v2.StartNode, "ask2")
	bld.AddEdge("ask2", compose_v2.EndNode)
	g, err := bld.Compile()
	if err != nil {
		panic(err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("seq_asker"),
		agent_v2.WithGraphInputWriter(func(in *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, in)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
}

func clientFor(t *testing.T, ag agent_v2.Agent) *a2aclient.Client {
	t.Helper()
	c, err := a2aclient.NewFromCard(context.Background(), server.Card(ag, serveAgent(t, ag)))
	if err != nil {
		t.Fatalf("NewFromCard: %v", err)
	}
	return c
}

func TestServerAnswers_SequentialAsksConverge(t *testing.T) {
	asked := map[string]*atomic.Int32{"Q-1?": {}, "Q-2?": {}}
	client := clientFor(t, seqAsker(asked))
	ans := map[string]string{"Q-1?": "one", "Q-2?": "two"}
	cur := send(t, client, a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("go")))
	for i := 0; i < 4 && cur.state == a2atypes.TaskStateInputRequired; i++ {
		parts := make([]*a2atypes.Part, 0, len(cur.pending))
		for _, p := range cur.pending {
			parts = append(parts, a2a.AnswerPart(p.InterruptID, nil, ans[p.Question]))
		}
		cur = send(t, client, reply(cur, parts...))
	}
	if cur.state != a2atypes.TaskStateCompleted || cur.text != "got: one+two" {
		t.Fatalf("先后两问、每次答当时挂着的那一问，应完成且答复为 got: one+two，实际 state=%s text=%q", cur.state, cur.text)
	}
	// 答过的不再问：每一问只问一次
	if a, b := asked["Q-1?"].Load(), asked["Q-2?"].Load(); a != 1 || b != 1 {
		t.Fatalf("每一问应只问一次，实际 Q-1=%d Q-2=%d（答过的又被问了）", a, b)
	}
}

// 挂着两条 ask_user 时只答其一：没答的那一问继续挂着，答了它之后任务完成，两问的答案都在。
func TestServerAnswers_PartialAnswersConverge(t *testing.T) {
	client := clientFor(t, newAsker(t, "Q-A?", "Q-B?"))
	cur := send(t, client, a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("go")))
	idOf := map[string]string{}
	for _, p := range cur.pending {
		idOf[p.Question] = p.InterruptID
	}
	if len(idOf) != 2 {
		t.Fatalf("前置：应挂着两问，实际 %+v", cur.pending)
	}
	cur = send(t, client, reply(cur, a2a.AnswerPart(idOf["Q-A?"], nil, "a-ans")))
	if cur.state != a2atypes.TaskStateInputRequired || len(cur.pending) != 1 || cur.pending[0].Question != "Q-B?" {
		t.Fatalf("只答了 A：应只剩 B 挂着，实际 state=%s pending=%+v", cur.state, cur.pending)
	}
	cur = send(t, client, reply(cur, a2a.AnswerPart(cur.pending[0].InterruptID, nil, "b-ans")))
	if cur.state != a2atypes.TaskStateCompleted || cur.text != "got: a-ans+b-ans" {
		t.Fatalf("再答 B 应完成且两问的答案都在，实际 state=%s text=%q", cur.state, cur.text)
	}
}

// 重发已经答过的那一问：请重答并说明它答过了；只答还挂着的那一问之后照常完成。
func TestServerAnswers_AlreadyAnsweredIDReasks(t *testing.T) {
	client := clientFor(t, newAsker(t, "Q-A?", "Q-B?"))
	cur := send(t, client, a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("go")))
	idOf := map[string]string{}
	for _, p := range cur.pending {
		idOf[p.Question] = p.InterruptID
	}
	cur = send(t, client, reply(cur, a2a.AnswerPart(idOf["Q-A?"], nil, "a-ans")))
	cur = send(t, client, reply(cur, a2a.AnswerPart(idOf["Q-A?"], nil, "a-ans"), a2a.AnswerPart(idOf["Q-B?"], nil, "b-ans")))
	if cur.state != a2atypes.TaskStateInputRequired || !strings.Contains(cur.text, "already answered") {
		t.Fatalf("重发答过的那一问应请重答并说明答过了，实际 state=%s text=%q", cur.state, cur.text)
	}
	cur = send(t, client, reply(cur, a2a.AnswerPart(idOf["Q-B?"], nil, "b-ans")))
	if cur.state != a2atypes.TaskStateCompleted || cur.text != "got: a-ans+b-ans" {
		t.Fatalf("只答还挂着的那一问之后应完成，实际 state=%s text=%q", cur.state, cur.text)
	}
}

// 形状不对的答复 DataPart（answer 不是字符串）：请重答，不能把空答案交给提问；不带 interruptId 的 {"answer": ...}
// 在只挂一问时就是这一问的答复。曾经前者让提问收到 ""、任务照常完成，后者被忽略、同样收到 ""。
func TestServerAnswers_MalformedAnswerReasks(t *testing.T) {
	client := clientFor(t, newAsker(t, "How many?"))
	cur := send(t, client, a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("go")))
	if len(cur.pending) != 1 {
		t.Fatalf("前置：应挂着一问，实际 %+v", cur.pending)
	}
	id := cur.pending[0].InterruptID
	cur = send(t, client, reply(cur, a2atypes.NewDataPart(map[string]any{"interruptId": id, "answer": 42})))
	if cur.state != a2atypes.TaskStateInputRequired || !strings.Contains(cur.text, `"answer" must be a string`) {
		t.Fatalf("answer 不是字符串应请重答，实际 state=%s text=%q", cur.state, cur.text)
	}
	cur = send(t, client, reply(cur, a2atypes.NewDataPart(map[string]any{"answer": "three"})))
	if cur.state != a2atypes.TaskStateCompleted || cur.text != "got: three" {
		t.Fatalf("不带 interruptId 的 {\"answer\": ...} 应答这唯一的一问，实际 state=%s text=%q", cur.state, cur.text)
	}
}

// 同一问的两种写法不一致（DataPart 里 approved 与 answer 相反、文本与 DataPart 相反——DataPart 带不带 interruptId
// 都一样）：请重答，工具不执行。审批不能在两个说法里挑一个——曾经 approved 优先，approved=true + answer=deny、
// 文本 deny + {"approved": true} 都执行了；后来只查了不带 interruptId 的那条路，文本 deny + 带 interruptId 的
// approved=true 照样执行（审计实测）。
func TestServerApproval_ConflictingFormsReask(t *testing.T) {
	for _, tc := range []struct {
		name  string
		parts func(id string) []*a2atypes.Part
	}{
		{"同一个 DataPart 里相反", func(id string) []*a2atypes.Part {
			return []*a2atypes.Part{a2atypes.NewDataPart(map[string]any{"interruptId": id, "approved": true, "answer": "deny"})}
		}},
		{"文本与 DataPart 相反", func(string) []*a2atypes.Part {
			return []*a2atypes.Part{a2atypes.NewTextPart("deny"), a2atypes.NewDataPart(map[string]any{"approved": true})}
		}},
		{"文本与带 interruptId 的 DataPart 相反", func(id string) []*a2atypes.Part {
			return []*a2atypes.Part{a2atypes.NewTextPart("deny"), a2atypes.NewDataPart(map[string]any{"interruptId": id, "approved": true})}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, calls := approvalServer(t, "refund")
			cur := send(t, client, a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("go")))
			if len(cur.pending) != 1 {
				t.Fatalf("前置：应停在一条审批上，实际 %+v", cur.pending)
			}
			id := cur.pending[0].InterruptID
			cur = send(t, client, reply(cur, tc.parts(id)...))
			if cur.state != a2atypes.TaskStateInputRequired {
				t.Fatalf("两种写法不一致应请重答，实际 state=%s text=%q", cur.state, cur.text)
			}
			if got := calls["refund"].Load(); got != 0 {
				t.Fatalf("两种写法不一致时工具不该执行，实际 %d 次", got)
			}
			yes := true
			cur = send(t, client, reply(cur, a2a.AnswerPart(id, &yes, "")))
			if cur.state != a2atypes.TaskStateCompleted || calls["refund"].Load() != 1 {
				t.Fatalf("重答批准之后应完成且工具执行 1 次，实际 state=%s refund=%d", cur.state, calls["refund"].Load())
			}
		})
	}
}

// 审批答复带一句给人看的文本（不是固定词）：当附注，按 DataPart 执行——带不带 interruptId 两条路一样。曾经不带
// interruptId 的那条路把它当成另一个答复请重答，带 interruptId 的照常执行（审计实测）。
func TestServerApproval_RemarkTextAccepted(t *testing.T) {
	for _, tc := range []struct {
		name  string
		parts func(id string) []*a2atypes.Part
	}{
		{"不带 interruptId", func(string) []*a2atypes.Part {
			return []*a2atypes.Part{a2atypes.NewTextPart("Approved."), a2atypes.NewDataPart(map[string]any{"approved": true})}
		}},
		{"带 interruptId", func(id string) []*a2atypes.Part {
			return []*a2atypes.Part{a2atypes.NewTextPart("Approved."), a2atypes.NewDataPart(map[string]any{"interruptId": id, "approved": true})}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, calls := approvalServer(t, "refund")
			cur := send(t, client, a2atypes.NewMessage(a2atypes.MessageRoleUser, a2atypes.NewTextPart("go")))
			if len(cur.pending) != 1 {
				t.Fatalf("前置：应停在一条审批上，实际 %+v", cur.pending)
			}
			cur = send(t, client, reply(cur, tc.parts(cur.pending[0].InterruptID)...))
			if cur.state != a2atypes.TaskStateCompleted || calls["refund"].Load() != 1 {
				t.Fatalf("附注文本加 approved=true 应按批准执行，实际 state=%s refund=%d text=%q", cur.state, calls["refund"].Load(), cur.text)
			}
		})
	}
}
