//go:build integration

// qwen dashscope 测试配置集成验证 — 跑真 dashscope endpoint, 默认 make test /
// go test ./... 不编译 (integration tag). 跑: go test -tags=integration ./model/qwen -v
//
// 需 .env QWEN_API_KEY (envloader.Load 加载). 验 NewTestClient dashscope baseURL +
// 留空 = TestModelCandidates 首位 model + Generate/Stream token usage 填充 (跟 deepseek 同款验证).
package qwen_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"crew-go/example/envloader"
	"crew-go/model/qwen"
	"crew-go/schema"
)

func TestQwenTestClient_DashScopeGenerate(t *testing.T) {
	envloader.Load()
	m := qwen.NewTestClient("") // 留空 = TestModelCandidates 首位
	resp, err := m.Generate(context.Background(), []*schema.Message{
		schema.UserMessage("用一句话介绍北京"),
	})
	if err != nil {
		t.Fatalf("dashscope generate: %v", err)
	}
	if resp == nil || resp.Content == "" {
		t.Fatal("dashscope 返空响应")
	}
	t.Logf("qwen dashscope generate 响应: %s", resp.Content)
	// 验 token usage (generate 路径 parseResponse 填 Metadata.Usage)
	if resp.Metadata == nil || resp.Metadata.Usage == nil {
		t.Error("generate 响应无 Metadata.Usage (token 缺失)")
	} else {
		u := resp.Metadata.Usage
		t.Logf("generate token: in=%d out=%d total=%d", u.InputTokens, u.OutputTokens, u.TotalTokens)
		if u.TotalTokens == 0 {
			t.Error("generate token total=0")
		}
	}
	// 验 gen_ai.provider.name (provider, gap 2) + response model (gap 1) 透传到 Metadata
	if resp.Metadata != nil {
		if resp.Metadata.System != "dashscope" {
			t.Errorf("generate Metadata.System = %q; want dashscope (WithSystem 透传)", resp.Metadata.System)
		}
		if resp.Metadata.Model == "" {
			t.Error("generate Metadata.Model 空 (response model 缺失)")
		}
		t.Logf("generate response model=%q system=%q", resp.Metadata.Model, resp.Metadata.System)
	}
}

func TestQwenTestClient_DashScopeStream(t *testing.T) {
	envloader.Load()
	m := qwen.NewTestClient("")
	reader, err := m.Stream(context.Background(), []*schema.Message{
		schema.UserMessage("用一句话介绍上海"),
	})
	if err != nil {
		t.Fatalf("dashscope stream: %v", err)
	}
	defer reader.Close()

	var content string
	var usage *schema.MessageUsage
	var system, respModel string
	for {
		chunk, recvErr := reader.Recv(context.Background())
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				break
			}
			t.Fatalf("stream recv: %v", recvErr)
		}
		if chunk == nil {
			continue
		}
		content += chunk.Content
		// Round 8 fix: stream 末尾 usage chunk (include_usage) 填 Metadata.Usage
		if chunk.Metadata != nil && chunk.Metadata.Usage != nil {
			usage = chunk.Metadata.Usage
		}
		// gap 1/2: chunk Metadata 带 response model + provider (WithSystem 透传)
		if chunk.Metadata != nil {
			if chunk.Metadata.System != "" {
				system = chunk.Metadata.System
			}
			if chunk.Metadata.Model != "" {
				respModel = chunk.Metadata.Model
			}
		}
	}
	if content == "" {
		t.Fatal("dashscope stream 返空内容")
	}
	t.Logf("qwen dashscope stream 响应: %s", content)
	// 验 stream include_usage 真返 token (Round 8 集成验证核心)
	if usage == nil {
		t.Error("stream 无 usage — include_usage 未生效 / dashscope 不返 stream usage")
	} else {
		t.Logf("stream token: in=%d out=%d total=%d", usage.InputTokens, usage.OutputTokens, usage.TotalTokens)
		if usage.TotalTokens == 0 {
			t.Error("stream token total=0")
		}
	}
	// gap 1/2: stream chunk 也透传 provider (gen_ai.provider.name) + response model
	if system != "dashscope" {
		t.Errorf("stream Metadata.System = %q; want dashscope", system)
	}
	if respModel == "" {
		t.Error("stream response model 空")
	}
	t.Logf("stream response model=%q system=%q", respModel, system)
}
