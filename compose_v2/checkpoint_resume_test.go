package compose_v2_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

type resumeStepStore struct {
	*compose_v2.InMemoryCheckpointer
	stepStored chan struct{}
	once       sync.Once
}

func (s *resumeStepStore) SaveNS(ctx context.Context, sid, ns string, cp *compose_v2.Checkpoint) error {
	if err := s.InMemoryCheckpointer.SaveNS(ctx, sid, ns, cp); err != nil {
		return err
	}
	if len(cp.CurrentFrontier) == 1 && cp.CurrentFrontier[0].NodeID == "write" {
		s.once.Do(func() { close(s.stepStored) })
	}
	return nil
}

func TestResumeAdmissionPersistsAcrossDurableSteps(t *testing.T) {
	for _, mode := range []compose_v2.Durability{compose_v2.DurabilitySync, compose_v2.DurabilityAsync} {
		t.Run(string(mode), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			store := &resumeStepStore{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer(), stepStored: make(chan struct{})}
			var writes atomic.Int32
			release := make(chan struct{})
			g, err := compose_v2.NewBuilder().WithName("resume-steps").
				AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "批准写入？"})
					return nil, err
				}).AddNode("write", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
				if writes.Add(1) == 1 {
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return nil, nil
			}).AddEdge(compose_v2.StartNode, "ask").AddEdge("ask", "write").AddEdge("write", compose_v2.EndNode).Compile()
			if err != nil {
				t.Fatal(err)
			}
			const sid = "resume-steps"
			run := func(options ...compose_v2.RunOption) error {
				base := []compose_v2.RunOption{compose_v2.WithCheckpointer(store), compose_v2.WithSessionID(sid), compose_v2.WithDurability(mode)}
				return compose_v2.Run(ctx, g, compose_v2.NewChannelManager(), append(base, options...)...)
			}
			pause, ok := compose_v2.IsInterrupt(run())
			if !ok || len(pause.AllQueries()) != 1 {
				t.Fatalf("需要真实的待答档，实际 %v", pause)
			}
			result := make(chan error, 1)
			go func() {
				answer := map[string]*schema.Message{pause.AllQueries()[0].InterruptID: schema.UserMessage("同意")}
				result <- run(compose_v2.WithResumeAnswers(answer))
			}()
			select {
			case <-store.stepStored:
			case <-ctx.Done():
				close(release)
				<-result
				t.Fatal("未观察到续跑过程档")
			}
			retry := run()
			close(release)
			if err := <-result; err != nil || !errors.Is(retry, compose_v2.ErrCheckpointResumeUnconfirmed) || writes.Load() != 1 {
				t.Fatalf("过程档不能重新开放，同一写入只执行一次，实际首个=%v 重试=%v 写入=%d", err, retry, writes.Load())
			}
		})
	}
}

// 父续跑中新建的子图也不能发布可被另一个调用独立恢复的过程档。
func TestResumeAdmissionBlocksDirectChildResumeDuringDurableStep(t *testing.T) {
	for _, mode := range []compose_v2.Durability{compose_v2.DurabilitySync, compose_v2.DurabilityAsync} {
		t.Run(string(mode), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			store := &resumeStepStore{InMemoryCheckpointer: compose_v2.NewInMemoryCheckpointer(), stepStored: make(chan struct{})}
			release, written := make(chan struct{}), make(chan struct{})
			var writes atomic.Int32
			child, err := compose_v2.NewBuilder().WithName("new-child").
				AddNode("prepare", func(context.Context, compose_v2.Channels) ([]compose_v2.ChannelWrite, error) { return nil, nil }).
				AddNode("write", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					if writes.Add(1) == 1 {
						close(written)
						select {
						case <-release:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					return nil, nil
				}).AddEdge(compose_v2.StartNode, "prepare").AddEdge("prepare", "write").AddEdge("write", compose_v2.EndNode).Compile()
			if err != nil {
				t.Fatal(err)
			}
			const childNS, sid = "child", "resume-admission-child"
			parent, err := compose_v2.NewBuilder().WithName("resumed-parent").
				AddNode("ask", func(ctx context.Context, _ compose_v2.Channels) ([]compose_v2.ChannelWrite, error) {
					_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "启动子图？"})
					return nil, err
				}).AddSubgraphNode(childNS, child).
				AddEdge(compose_v2.StartNode, "ask").AddEdge("ask", childNS).AddEdge(childNS, compose_v2.EndNode).Compile()
			if err != nil {
				t.Fatal(err)
			}
			opts := []compose_v2.RunOption{compose_v2.WithCheckpointer(store), compose_v2.WithSessionID(sid), compose_v2.WithDurability(mode)}
			pause, ok := compose_v2.IsInterrupt(compose_v2.Run(ctx, parent, compose_v2.NewChannelManager(), opts...))
			if !ok || len(pause.AllQueries()) != 1 {
				t.Fatal("父图应在启动子图前挂起")
			}
			answer := compose_v2.WithResumeAnswers(map[string]*schema.Message{
				pause.AllQueries()[0].InterruptID: schema.UserMessage("同意"),
			})
			result := make(chan error, 1)
			go func() {
				result <- compose_v2.Run(ctx, parent, compose_v2.NewChannelManager(), append(opts, answer)...)
			}()
			for _, ready := range []<-chan struct{}{store.stepStored, written} {
				select {
				case <-ready:
				case <-ctx.Done():
					close(release)
					<-result
					t.Fatal("前提：过程档已提交，子图写入已经发生")
				}
			}
			retry := compose_v2.Run(ctx, child, compose_v2.NewChannelManager(), append(opts, compose_v2.WithRunNS(childNS))...)
			close(release)
			if err := <-result; err != nil || !errors.Is(retry, compose_v2.ErrCheckpointResumeUnconfirmed) || writes.Load() != 1 {
				t.Fatalf("独立子图续跑须拒绝重复写入，父图=%v 子图=%v 写入=%d", err, retry, writes.Load())
			}
		})
	}
}
