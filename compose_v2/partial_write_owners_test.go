package compose_v2_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
)

func TestSingleWriter_PartialCommitKeepsOwnersAcrossRetries(t *testing.T) {
	for _, scenario := range []string{"member", "payload", "same_owner_payload", "missing_owners"} {
		t.Run(scenario, func(t *testing.T) { testPartialWriteOwners(t, scenario) })
	}
}

func testPartialWriteOwners(t *testing.T, scenario string) {
	t.Helper()
	x := compose_v2.LastValue[string]("x")
	b := compose_v2.NewBuilder()
	compose_v2.WithChannel(b, x)
	var aCalls, bCalls atomic.Int32
	temporary := errors.New("retry B")
	b.AddNode("A", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		aCalls.Add(1)
		if scenario == "same_owner_payload" {
			compose_v2.SendFrom(ctx, compose_v2.SendDirective{
				Target: "next", Payload: []compose_v2.ChannelWrite{compose_v2.Write(x, "A-payload")},
			})
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(x, "A")}, nil
	})
	b.AddNode("B", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		if bCalls.Add(1) < 3 {
			return nil, temporary
		}
		switch scenario {
		case "member":
			return []compose_v2.ChannelWrite{compose_v2.Write(x, "B")}, nil
		case "payload":
			compose_v2.SendFrom(ctx, compose_v2.SendDirective{
				Target: "next", Payload: []compose_v2.ChannelWrite{compose_v2.Write(x, "B-payload")},
			})
		}
		return nil, nil
	})
	var nextSaw string
	b.AddNode("next", func(_ context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		nextSaw = compose_v2.MustGet(chs, x)
		return []compose_v2.ChannelWrite{compose_v2.Write(x, "next-step")}, nil
	})
	b.AddEdge(compose_v2.StartNode, "A").AddEdge(compose_v2.StartNode, "B")
	switch scenario {
	case "same_owner_payload":
		b.AddEdge("B", compose_v2.EndNode)
	case "payload":
		b.AddEdge("A", compose_v2.EndNode)
	default:
		b.AddEdge("A", "next").AddEdge("B", "next")
	}
	b.AddEdge("next", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cper := compose_v2.NewInMemoryCheckpointer()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	const sid = "partial"
	run := func(cm *compose_v2.ChannelManager) error {
		return compose_v2.Run(ctx, g, cm, compose_v2.WithCheckpointer(cper), compose_v2.WithSessionID(sid))
	}
	for round := 0; round < 2; round++ {
		if err := run(compose_v2.NewChannelManager()); !errors.Is(err, temporary) {
			t.Fatalf("第 %d 次执行应只失败 B，实际 %v", round+1, err)
		}
		saved, err := cper.LoadNS(ctx, sid, "")
		if err != nil || saved == nil || len(saved.CompletedFrontier) != 1 {
			t.Fatalf("缺部分提交存档，err=%v cp=%+v", err, saved)
		}
		wire, err := compose_v2.MarshalCheckpoint(saved)
		if err != nil {
			t.Fatal(err)
		}
		copyCP, err := compose_v2.UnmarshalCheckpoint(wire)
		if err != nil {
			t.Fatal(err)
		}
		preloaded := compose_v2.NewChannelManager()
		if scenario == "missing_owners" {
			if err := compose_v2.RegisterChannel(preloaded, x); err != nil {
				t.Fatal(err)
			}
			if err := preloaded.Restore(copyCP); err != nil {
				t.Fatal(err)
			}
			copyCP.CompletedWriteOwners = nil
		}
		if err := cper.SaveNS(ctx, sid, "", copyCP); err != nil {
			t.Fatal(err)
		}
		if scenario == "missing_owners" {
			if err := run(preloaded); !errors.Is(err, compose_v2.ErrCheckpointNotResumable) {
				t.Fatalf("缺归属的部分存档应拒绝恢复，实际 %v", err)
			}
			after, err := cper.LoadNS(ctx, sid, "")
			if err != nil || after != copyCP || aCalls.Load() != 1 || bCalls.Load() != 1 {
				t.Fatalf("恢复拒绝覆盖了原档或执行了节点，A=%d B=%d err=%v", aCalls.Load(), bCalls.Load(), err)
			}
			return
		}
	}
	cm := compose_v2.NewChannelManager()
	err = run(cm)
	if scenario == "same_owner_payload" {
		if err != nil || nextSaw != "A-payload" || compose_v2.MustGet(cm.Channels(), x) != "next-step" {
			t.Fatalf("同一成员的 payload 和下一超步写入应可提交，nextSaw=%q err=%v", nextSaw, err)
		}
	} else {
		if !errors.Is(err, compose_v2.ErrRunAborted) || !strings.Contains(err.Error(), "A") || !strings.Contains(err.Error(), "B") || nextSaw != "" {
			t.Fatalf("应在调度后继前拒绝冲突，nextSaw=%q err=%v", nextSaw, err)
		}
		if got := compose_v2.MustGet(cm.Channels(), x); got != "A" {
			t.Fatalf("未提交的写覆盖了已提交值，x=%q", got)
		}
	}
	saved, loadErr := cper.LoadNS(ctx, sid, "")
	if loadErr != nil || saved != nil || aCalls.Load() != 1 || bCalls.Load() != 3 {
		t.Fatalf("结束应清档且已成功的 A 不重跑，cp=%+v err=%v A=%d B=%d", saved, loadErr, aCalls.Load(), bCalls.Load())
	}
}
