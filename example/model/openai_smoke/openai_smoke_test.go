//go:build integration

package openai_smoke_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/example/envloader"
	"crew-go/model"
	"crew-go/model/openai"
	"crew-go/schema"
	"crew-go/tool"
)

func smokeConfig(t *testing.T, modelEnv string) (string, string) {
	t.Helper()
	envloader.Load()
	key := strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))
	if key == "" {
		t.Skip("OPENAI_API_KEY 未设置（环境变量或项目根目录 .env）")
	}
	name := strings.TrimSpace(os.Getenv(modelEnv))
	if name == "" {
		name = "gpt-5.4-mini"
	}
	return key, name
}

func newClient(t *testing.T) model.ChatModel {
	t.Helper()
	key, name := smokeConfig(t, "OPENAI_SMOKE_MODEL")
	return openai.NewClient(key, name, openai.WithExtraBody(map[string]any{"max_completion_tokens": 512}))
}

func newResponsesClient(t *testing.T) model.ChatModel {
	t.Helper()
	key, name := smokeConfig(t, "OPENAI_RESPONSES_SMOKE_MODEL")
	return openai.NewResponsesClient(key, name, openai.WithExtraBody(map[string]any{"max_output_tokens": 512}))
}

func TestSmoke_Stream(t *testing.T) { smokeStream(t, newClient(t)) }

func TestSmoke_ResponsesStream(t *testing.T) { smokeStream(t, newResponsesClient(t)) }

func smokeStream(t *testing.T, client model.ChatModel) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	const marker = "crew-openai-stream-ok"
	stream, err := client.Stream(ctx, []*schema.Message{
		schema.UserMessage("Reply with exactly this text: " + marker),
	})
	if err != nil {
		t.Fatalf("建立 OpenAI 流: %v", err)
	}
	defer stream.Close()

	var output strings.Builder
	chunks := 0
	for {
		msg, err := stream.Recv(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("读取 OpenAI 流: %v", err)
		}
		if msg != nil && msg.Content != "" {
			chunks++
			output.WriteString(msg.Content)
		}
	}
	if chunks == 0 || !strings.Contains(output.String(), marker) {
		t.Fatalf("期望流式文本包含 %q，收到 %d 个文本分片，正文 %q", marker, chunks, output.String())
	}
	t.Logf("收到 %d 个文本分片并正常结束", chunks)
}

func TestSmoke_ToolCalling(t *testing.T) { smokeToolCalling(t, newClient(t)) }

func TestSmoke_ResponsesToolCalling(t *testing.T) { smokeToolCalling(t, newResponsesClient(t)) }

func smokeToolCalling(t *testing.T, client model.ChatModel) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	const item = "cedar"
	code := "crew-" + rand.Text()[:10]
	var calls atomic.Int32
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool("lookup_code", "Return the lookup code for an item.", map[string]any{
		"type": "object",
		"properties": map[string]any{
			"item": map[string]any{"type": "string"},
		},
		"required": []string{"item"},
	}, func(_ context.Context, args map[string]any) (*tool.Result, error) {
		calls.Add(1)
		if args["item"] != item {
			return nil, fmt.Errorf("lookup_code expected item %q, got %v", item, args["item"])
		}
		return tool.Success(code), nil
	}))

	agent := agent_v2.NewBasicAgent(
		agent_v2.WithName("openai-smoke"),
		agent_v2.WithModel(client),
		agent_v2.WithToolkit(kit),
		agent_v2.WithMaxRounds(2),
	)
	events, err := agent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage(
		"Call lookup_code exactly once with item "+item+". Reply with the code from the tool result."),
		agent_v2.WithRunModelOptions(
			model.WithToolChoice(schema.ToolChoiceRequired),
			model.WithExtraBody(map[string]any{"parallel_tool_calls": false}),
		),
	)
	if err != nil {
		t.Fatalf("OpenAI 工具调用闭环失败: %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("期望工具执行一次，实际执行 %d 次", n)
	}
	final := agent_v2.FinalMessage(events)
	if final == nil || !strings.Contains(final.Content, code) {
		t.Fatalf("最终答复未使用工具返回的校验码 %q，得到 %v", code, final)
	}
}
