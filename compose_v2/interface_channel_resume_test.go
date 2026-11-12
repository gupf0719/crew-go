package compose_v2_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

type stringChannelValue string

func (v stringChannelValue) String() string { return string(v) }

func TestInterfaceChannel_ConcreteValueSurvivesResume(t *testing.T) {
	compose_v2.RegisterValueType[stringChannelValue]("test.stringChannelValue")
	t.Run("any", func(t *testing.T) {
		testInterfaceChannelResume(t, compose_v2.LastValue[any]("value"), "hello")
	})
	t.Run("stringer", func(t *testing.T) {
		testInterfaceChannelResume[fmt.Stringer](t, compose_v2.LastValue[fmt.Stringer]("value"), stringChannelValue("hello"))
	})
}

func testInterfaceChannelResume[T any](t *testing.T, ch *compose_v2.Channel[T], value T) {
	t.Helper()
	b := compose_v2.NewBuilder()
	compose_v2.WithChannel(b, ch)
	writes, reads := 0, 0
	b.AddNode("write", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		writes++
		return []compose_v2.ChannelWrite{compose_v2.Write(ch, value)}, nil
	})
	b.AddNode("read", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		reads++
		if got, ok := compose_v2.Get(chs, ch); !ok || !reflect.DeepEqual(got, value) {
			return nil, fmt.Errorf("read %T(%v), want %T(%v)", got, got, value, value)
		}
		_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "continue?"})
		return nil, err
	})
	b.AddEdge(compose_v2.StartNode, "write").AddEdge("write", "read").AddEdge("read", compose_v2.EndNode)
	g, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	const sid = "interface-channel"
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(cp), compose_v2.WithSessionID(sid)}
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), opts...)
	interrupt, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("期望首次中断，实际 %v", err)
	}
	saved, err := cp.LoadNS(ctx, sid, "")
	if err != nil || saved == nil {
		t.Fatalf("缺少待恢复存档，err=%v", err)
	}
	wire, err := compose_v2.MarshalCheckpoint(saved)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := compose_v2.UnmarshalCheckpoint(wire)
	if err != nil {
		t.Fatal(err)
	}
	err = compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(opts,
		compose_v2.WithResumeFrom(restored),
		compose_v2.WithResumeAnswers(map[string]*schema.Message{interrupt.AllQueries()[0].InterruptID: schema.UserMessage("yes")}))...)
	if err != nil || writes != 1 || reads != 2 {
		t.Fatalf("接口值未正确恢复，err=%v writes=%d reads=%d", err, writes, reads)
	}
}

func TestInterfaceChannel_RejectsIncompatibleConcreteValue(t *testing.T) {
	invalid := compose_v2.LastValue[string]("value")
	src := compose_v2.NewChannelManager()
	if err := compose_v2.RegisterChannel(src, invalid); err != nil {
		t.Fatal(err)
	}
	if err := src.ApplyAll([]compose_v2.ChannelWrite{compose_v2.Write(invalid, "plain string")}); err != nil {
		t.Fatal(err)
	}
	cp, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	ch := compose_v2.LastValue[fmt.Stringer]("value")
	dst := compose_v2.NewChannelManager()
	if err := compose_v2.RegisterChannel(dst, ch); err != nil {
		t.Fatal(err)
	}
	if err := dst.ApplyAll([]compose_v2.ChannelWrite{compose_v2.Write[fmt.Stringer](ch, stringChannelValue("prior"))}); err != nil {
		t.Fatal(err)
	}
	if err := dst.Restore(cp); err == nil {
		t.Fatal("不实现目标接口的存档值被接受")
	}
	if got := compose_v2.MustGet(dst.Channels(), ch).String(); got != "prior" {
		t.Fatalf("恢复失败后旧值被修改，got=%q", got)
	}
}
