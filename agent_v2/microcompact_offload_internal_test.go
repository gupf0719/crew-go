package agent_v2

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/schema"
	"crew-go/store"
)

func TestMicroCompact_MergedResultsKeepUnlistedBlock(t *testing.T) {
	read := func(path string) schema.ToolCall {
		return schema.ToolCall{ID: "shared", Function: schema.FunctionCall{Name: "Read", Arguments: `{"path":"` + path + `"}`}}
	}
	merged := schema.ToolResultMessage("shared", "旧文件正文")
	other := schema.ToolResultMessage("shared", "检查结果")
	merged.ContentBlocks = append(merged.ContentBlocks, other.ContentBlocks...)
	merged.Content = "两条结果的合并正文"
	messages := []*schema.Message{
		schema.AssistantToolCallMessage([]schema.ToolCall{read("old"), {ID: "shared", Function: schema.FunctionCall{Name: "Inspect"}}}),
		merged,
		schema.AssistantToolCallMessage([]schema.ToolCall{read("new")}),
		schema.ToolResultMessage("shared", "近期文件正文"),
	}
	out := MicroCompactRun(t.Context(), messages, &MicroCompactConfig{Enabled: true, KeepRecent: 1})
	if out[1] != merged || out[3] != messages[3] {
		t.Fatalf("合并消息含白名单外结果，整条与近期结果均应保留，实际 %v", out)
	}
	want := []reinjectedFile{{Path: "new", Content: "近期文件正文"}, {Path: "old", Content: "旧文件正文"}}
	if got := extractRecentFiles(out, nil, 3, 100, 100); !slices.Equal(got, want) {
		t.Fatalf("文件应使用各自配对块的正文，实际 %+v", got)
	}
}

type blockedOffloadStore struct {
	store.Store
	started  chan struct{}
	release  chan struct{}
	attempts atomic.Int32
}

func (s *blockedOffloadStore) Put(ctx context.Context, ns store.Namespace, key string, value any, opts ...store.PutOption) error {
	if s.attempts.Add(1) == 1 {
		close(s.started)
		select {
		case <-s.release:
			return errors.New("offload temporarily unavailable")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Store.Put(ctx, ns, key, value, opts...)
}

type offloadWaitContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *offloadWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func TestMicroCompactOffload_ConcurrentFailureWaitsAndRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	st := &blockedOffloadStore{Store: store.NewInMemoryStore(), started: make(chan struct{}), release: make(chan struct{})}
	ctx = withOffloadDedup(store.WithContext(ctx, st))
	cfg := &MicroCompactConfig{Enabled: true, KeepRecent: 1, OffloadEnabled: true, Placeholder: "cleared"}
	call := func(id string) *schema.Message {
		return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Function: schema.FunctionCall{Name: "Read"}}})
	}
	messages := []*schema.Message{call("old"), schema.ToolResultMessage("old", "旧正文"), call("new"), schema.ToolResultMessage("new", "新正文")}
	first, second := make(chan []*schema.Message, 1), make(chan []*schema.Message, 1)
	go func() { first <- MicroCompactRun(ctx, messages, cfg) }()
	select {
	case <-st.started:
	case <-ctx.Done():
		t.Fatal("首次写入未开始")
	}
	waiter := &offloadWaitContext{Context: ctx, observed: make(chan struct{})}
	go func() { second <- MicroCompactRun(waiter, messages, cfg) }()
	select {
	case <-waiter.observed:
	case <-second:
		t.Fatal("并行压缩应等候写入完成")
	case <-ctx.Done():
		t.Fatal("并行压缩未进入等待")
	}
	close(st.release)
	for _, result := range []chan []*schema.Message{first, second} {
		select {
		case out := <-result:
			if out[1].Content != "cleared" || out[1].Annotations[MicroCompactOffloadKeyAnnotation] != nil {
				t.Errorf("失败的写入只能生成清理占位符，实际 %q", out[1].Content)
			}
		case <-ctx.Done():
			t.Fatal("压缩未结束")
		}
	}
	out := MicroCompactRun(ctx, messages, cfg)
	key, _ := out[1].Annotations[MicroCompactOffloadKeyAnnotation].(string)
	item, err := st.Get(ctx, cfg.OffloadNamespace(ctx), key)
	if key == "" || err != nil || item == nil || st.attempts.Load() != 2 {
		t.Fatalf("后续应重试一次并生成可读引用，key=%q item=%v err=%v 尝试=%d", key, item, err, st.attempts.Load())
	}
}
