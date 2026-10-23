// Package regenisolation_test 在一个只 import compose_v2 的测试二进制里验嵌套图重生：compose_v2 包内的测试文件注册了
// int 等类型，跟它们编进同一个二进制的哨兵测不出「框架自己存的值类型没注册」——那样的修复在只用 compose_v2 的业务上
// 静默失效（审计实测）。本目录只放这一类测试，别在这里注册框架内部用到的类型。
package regenisolation_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

func init() { compose_v2.RegisterValueType[string]("string") } // 业务 channel 用到的类型，业务自己注册；int 没用到就不注册

// 节点里直接跑的嵌套图跑完之后节点失败、续跑：重生的图问新问题，批准用在 v2 上（见 compose_v2 的 nestedRegenKey）。
func TestRegen_OnlyComposeV2_AsksNewQuestion(t *testing.T) {
	var drafts atomic.Int32
	draftCh := compose_v2.LastValue[string]("draft")
	outCh := compose_v2.LastValue[string]("out")
	b := compose_v2.NewBuilder().WithName("noreg_draft")
	compose_v2.WithChannel(b, draftCh)
	compose_v2.WithChannel(b, outCh)
	b.AddNode("draft", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		return []compose_v2.ChannelWrite{compose_v2.Write(draftCh, fmt.Sprintf("v%d", drafts.Add(1)))}, nil
	})
	b.AddNode("confirm", func(ctx context.Context, chs compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		d, _ := compose_v2.Get(chs, draftCh)
		ans, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准 " + d + " 吗"})
		if err != nil {
			return nil, err
		}
		return []compose_v2.ChannelWrite{compose_v2.Write(outCh, d+" 已批准("+ans.Content+")")}, nil
	})
	b.AddEdge(compose_v2.StartNode, "draft")
	b.AddEdge("draft", "confirm")
	b.AddEdge("confirm", compose_v2.EndNode)
	inner, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var failed atomic.Bool
	var final string
	ob := compose_v2.NewBuilder()
	ob.AddNode("n", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
		cm := compose_v2.NewChannelManager()
		if err := compose_v2.Run(ctx, inner, cm, compose_v2.WithRunNS("inner")); err != nil {
			return nil, err
		}
		if failed.CompareAndSwap(false, true) {
			return nil, errors.New("下游错误")
		}
		final, _ = compose_v2.Get(cm.Channels(), outCh)
		return nil, nil
	})
	ob.AddEdge(compose_v2.StartNode, "n")
	ob.AddEdge("n", compose_v2.EndNode)
	g, err := ob.Compile()
	if err != nil {
		t.Fatal(err)
	}
	opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(compose_v2.NewInMemoryCheckpointer()), compose_v2.WithSessionID("noreg")}
	answers := map[string]*schema.Message{}
	var asked []string
	for round := 0; round < 8; round++ {
		err = compose_v2.Run(context.Background(), g, compose_v2.NewChannelManager(), append(opts, compose_v2.WithResumeAnswers(answers))...)
		if err == nil {
			break
		}
		ie, ok := compose_v2.IsInterrupt(err)
		if !ok {
			continue
		}
		for _, q := range ie.AllQueries() {
			asked = append(asked, q.Question)
			answers[q.InterruptID] = schema.UserMessage("答:" + q.Question)
		}
	}
	if err != nil || final != "v2 已批准(答:批准 v2 吗)" {
		t.Errorf("重生没生效：err=%v asked=%q drafts=%d final=%q", err, asked, drafts.Load(), final)
	}
}
