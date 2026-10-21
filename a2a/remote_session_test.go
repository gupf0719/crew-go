package a2a_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"crew-go/a2a"
	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"

	a2atypes "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

// rsHost 是单节点 GraphAgent：节点体里调 body。
func rsHost(t *testing.T, body func(ctx context.Context, in string) (string, error)) agent_v2.Agent {
	t.Helper()
	b := compose_v2.NewBuilder().WithName("rs_host")
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("n", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in, _ := compose_v2.Get(chs, inCh)
		out, err := body(ctx, in.Content)
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(out))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "n")
	b.AddEdge("n", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return agent_v2.NewGraphAgent(g, agent_v2.WithGraphAgentName("host"),
		agent_v2.WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		agent_v2.WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}))
}

// rsAskOnceServer 起一个远端：新 task 先要人（input-required），续上同一个 task 时完成。
func rsAskOnceServer(t *testing.T, newTasks, continuations *atomic.Int32) string {
	return startServer(t, fnExec(func(execCtx *a2asrv.ExecutorContext, yield func(a2atypes.Event, error) bool) {
		if execCtx.StoredTask == nil {
			newTasks.Add(1)
			if !yield(a2atypes.NewSubmittedTask(execCtx, execCtx.Message), nil) {
				return
			}
			yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateInputRequired,
				a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("approve?"))), nil)
			return
		}
		continuations.Add(1)
		yield(a2atypes.NewStatusUpdateEvent(execCtx, a2atypes.TaskStateCompleted,
			a2atypes.NewMessage(a2atypes.MessageRoleAgent, a2atypes.NewTextPart("done with "+inputText(execCtx)))), nil)
	}))
}

// TestRemoteAgent_ForeignSessionInNode_RejectedBeforeSending：节点里给 RemoteAgent 传一个不属于当前任务的
// session。远端 task 句柄按 session 的 RunID 存——随手给新 session 的话父节点每次重跑都是一个新的远端
// task、续跑时读不到上一次的句柄。入口直接拒，一条消息都不发出去。
//
// 曾经 RemoteAgent 不走框架的会话校验：用户的回答被当成新远端 task 的第一条消息发出去，原来那个远端
// task 一直挂着，宿主最后以「答案已兑付」终局失败（实测）。
func TestRemoteAgent_ForeignSessionInNode_RejectedBeforeSending(t *testing.T) {
	var newTasks, continuations atomic.Int32
	base := rsAskOnceServer(t, &newTasks, &continuations)
	ra, err := a2a.NewRemoteAgent(context.Background(), base, a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	var remoteErr error
	host := rsHost(t, func(ctx context.Context, in string) (string, error) {
		_, remoteErr = ra.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(in))
		return "", remoteErr
	})
	root := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	_, err = host.Run(root, agent_v2.NewRunSession(), schema.UserMessage("job"))
	if remoteErr == nil || !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("别的任务的 session 应在入口被拒（终局），实际 remoteErr=%v err=%v", remoteErr, err)
	}
	if n := newTasks.Load(); n != 0 {
		t.Errorf("拒绝应发生在任何网络调用之前，远端却收到 %d 个新 task", n)
	}
}

// TestRemoteAgent_CurrentSessionInNode_Continues：对照组——传当前任务的 session（GetSession(ctx)），远端要人、
// 续跑时续上同一个远端 task、完成。
func TestRemoteAgent_CurrentSessionInNode_Continues(t *testing.T) {
	var newTasks, continuations atomic.Int32
	base := rsAskOnceServer(t, &newTasks, &continuations)
	ra, err := a2a.NewRemoteAgent(context.Background(), base, a2a.WithHumanGate())
	if err != nil {
		t.Fatal(err)
	}
	host := rsHost(t, func(ctx context.Context, in string) (string, error) {
		evs, err := ra.Run(ctx, agent_v2.GetSession(ctx), schema.UserMessage(in))
		if err != nil {
			return "", err
		}
		return agent_v2.FinalMessage(evs).Content, nil
	})
	root := compose_v2.WithCheckpointerCtx(context.Background(), compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	_, err = host.Run(root, sess, schema.UserMessage("job"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("远端要人，宿主应挂起：%v", err)
	}
	evs, err := host.Resume(root, sess.RunID, agent_v2.WithResumeSession(sess),
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.AllQueries()[0].InterruptID: schema.UserMessage("yes")}))
	if err != nil {
		t.Fatalf("续跑：%v", err)
	}
	if f := agent_v2.FinalMessage(evs); f == nil || f.Content != "done with yes" {
		t.Errorf("应续上同一个远端 task 并完成，实际 %v", f)
	}
	if newTasks.Load() != 1 || continuations.Load() != 1 {
		t.Errorf("应只开一个远端 task、续一次，实际新开 %d、续 %d", newTasks.Load(), continuations.Load())
	}
}
