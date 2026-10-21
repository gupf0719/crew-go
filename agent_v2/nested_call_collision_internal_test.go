package agent_v2

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// TestNestedCall_SlotCollisionRejected：两次输入不同的调用撞进同一个存档槽时（槽号是身份的哈希，
// 见 hashNSIndex），后一次调用接到的存档属于另一次调用——被调方拒绝（不可重试），不拿别人的进度
// 当自己的结果、不把给别人的答案套到自己头上；也**不清档**：槽里那份是别人的。
//
// 撞槽靠被调方的身份摘要核对发现（`compose_v2.WithInputIdentity`）：存档记着新开时那次调用的
// 身份摘要，续跑时对不上只可能是另一次调用。只作核对、对不上仍续跑的话，这里的第二次调用会
// 把第一次的提问续完、拿着给第一次的答案产出，`err == nil`。
//
// 63 位槽号撞不出来，这里直接制造一次：把第一次调用的挂起档原样放到第二次调用的槽上。
func TestNestedCall_SlotCollisionRejected(t *testing.T) {
	const x, y = "第一件事", "第二件事"
	// 槽要用宿主节点里的真实 ctx 算（身份含调用方所在的节点与相位）。算一次会让「同一份输入
	// 的第几次」加一，所以每份输入只在不会真去调它的那次节点执行里算：y 的槽在首跑（调 x）时算，
	// x 的槽在续跑（调 y）时算、用来核对首跑那次调用确实落在模型算出的槽上。
	slotOf := func(ctx context.Context, in string) string {
		ns, _, err := nestedCallNS(ctx, "w", "", inputDigest(schema.UserMessage(in)), false, false)
		if err != nil {
			t.Fatalf("nestedCallNS: %v", err)
		}
		return ns
	}
	var slotX, slotY atomic.Value

	var seenNS atomic.Value
	var processed atomic.Int32
	b := compose_v2.NewBuilder()
	inCh := compose_v2.LastValue[*schema.Message]("in")
	outCh := compose_v2.LastValue[*schema.Message]("out")
	compose_v2.WithChannel(b, inCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("work", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		seenNS.Store(compose_v2.GetGraphNS(ctx))
		in, _ := compose_v2.Get(chs, inCh)
		processed.Add(1)
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "确认「" + in.Content + "」？"})
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, schema.AssistantMessage(in.Content+"("+ans.Content+")"))}, nil
	})
	b.AddEdge(compose_v2.StartNode, "work")
	b.AddEdge("work", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatalf("compile sub: %v", err)
	}
	sub := NewGraphAgent(g,
		WithGraphAgentName("sub"),
		WithGraphInputWriter(func(m *schema.Message) []compose_v2.ChannelWrite {
			return []compose_v2.ChannelWrite{compose_v2.Write(inCh, m)}
		}),
		WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, outCh)
			return out
		}),
	)

	var executions atomic.Int32
	hb := compose_v2.NewBuilder()
	hostOut := compose_v2.LastValue[*schema.Message]("host_out")
	compose_v2.WithChannel(hb, hostOut)
	hb.AddNode("host", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		in := x
		if executions.Add(1) > 1 {
			in = y
			slotX.Store(slotOf(ctx, x))
		} else {
			slotY.Store(slotOf(ctx, y))
		}
		events, err := RunSubAgent(ctx, "w", sub, schema.UserMessage(in))
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(hostOut, FinalMessage(events))}, nil
	})
	hb.AddEdge(compose_v2.StartNode, "host")
	hb.AddEdge("host", compose_v2.EndNode)
	hg, err := hb.Compile()
	if err != nil {
		t.Fatalf("compile host: %v", err)
	}
	host := NewGraphAgent(hg,
		WithGraphAgentName("host"),
		WithGraphInputWriter(func(*schema.Message) []compose_v2.ChannelWrite { return nil }),
		WithGraphOutputReader(func(chs compose_v2.Channels) *schema.Message {
			out, _ := compose_v2.Get(chs, hostOut)
			return out
		}),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := NewRunSession()
	_, err = host.Run(ctx, sess, schema.UserMessage("go"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("首跑应停在子 agent 的提问，got %v", err)
	}
	firstNS, _ := seenNS.Load().(string)
	sy, _ := slotY.Load().(string)
	// 制造撞槽：第一次调用的挂起档原样出现在第二次调用的槽上
	planted, err := cper.LoadNS(ctx, sess.RunID, firstNS)
	if err != nil || planted == nil {
		t.Fatalf("第一次调用的挂起档应在：cp=%v err=%v", planted, err)
	}
	if err := cper.SaveNS(ctx, sess.RunID, sy, planted); err != nil {
		t.Fatalf("SaveNS: %v", err)
	}
	_, err = host.Resume(ctx, sess.RunID,
		WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("是")}),
		WithResumeSession(sess))
	// 前提：宿主节点里的调用确实落在按位置模型算出来的那个槽上
	if sx, _ := slotX.Load().(string); sx != firstNS {
		t.Fatalf("前提不成立：第一次调用落在 %q，按位置模型算出的是 %q", firstNS, sx)
	}
	if err == nil {
		t.Fatal("第二次调用撞进了第一次的槽，应拒绝，got nil")
	}
	if _, isInt := compose_v2.IsInterrupt(err); isInt {
		t.Fatalf("撞槽不该表现成等答案的中断: %v", err)
	}
	if errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("撞槽不该终局清档（槽里那份是别人的）: %v", err)
	}
	if compose_v2.IsRetryable(err) {
		t.Errorf("撞槽是确定性的，不该可重试: %v", err)
	}
	if !strings.Contains(err.Error(), "WithCallKey") {
		t.Errorf("错误正文应给出出路（agent_v2.WithCallKey），got %v", err)
	}
	if n := processed.Load(); n != 1 {
		t.Errorf("撞槽的那次调用不该在别人的存档上往前跑，子 agent 节点实际执行 %d 次（want 1：只有首跑）", n)
	}
	if cp, lerr := cper.LoadNS(ctx, sess.RunID, sy); lerr != nil || cp == nil {
		t.Errorf("被撞的槽里那份存档应原样留着: cp=%v err=%v", cp, lerr)
	}
}
