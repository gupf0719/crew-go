package openai

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"crew-go/model"
	"crew-go/schema"
)

// ResponsesClient 用 OpenAI Responses API 实现 [model.ChatModel]。
type ResponsesClient struct {
	base *Client
}

var _ model.ChatModel = (*ResponsesClient)(nil)
var _ model.StructuredCapable = (*ResponsesClient)(nil)

// NewResponsesClient 构造由调用方持有上下文的客户端。请求使用 store=false，
// 续轮从消息历史恢复工具配对与同端点的加密推理项。
func NewResponsesClient(apiKey, modelName string, opts ...ClientOption) *ResponsesClient {
	return &ResponsesClient{base: NewClient(apiKey, modelName, append([]ClientOption{WithSystem("openai")}, opts...)...)}
}

// ModelName 返回默认模型名。
func (c *ResponsesClient) ModelName() string { return c.base.ModelName() }

// StructuredCapability 返回原生 JSON schema 和工具结构化输出能力。
func (c *ResponsesClient) StructuredCapability(name string) model.StructuredCapability {
	return c.base.StructuredCapability(name)
}

// WithTools 返回绑定本地函数工具的新客户端。
func (c *ResponsesClient) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	if err := validateResponsesTools(tools); err != nil {
		return nil, err
	}
	return &ResponsesClient{base: c.base.withTools(tools)}, nil
}

func validateResponsesTools(tools []*schema.ToolInfo) error {
	for _, t := range tools {
		if t == nil || t.Name == "" {
			return errors.New("openai responses: function tool requires a name")
		}
	}
	return nil
}

// Generate 发送非流式请求，映射 Responses 的文本、拒答、工具调用和用量。
func (c *ResponsesClient) Generate(ctx context.Context, messages []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	req, source, err := c.request(ctx, messages, model.Apply(opts...), false)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() // 响应体读完即归还连接。
	var response responsesResult
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, c.base.streamError(fmt.Errorf("decode responses result: %w", err))
	}
	return c.convert(&response, source)
}

func (c *ResponsesClient) request(ctx context.Context, messages []*schema.Message, opts *model.Options, stream bool) (*http.Request, string, error) {
	if len(opts.Stop) > 0 {
		return nil, "", errors.New("openai responses: stop sequences are unsupported")
	}
	if err := validateResponsesTools(opts.Tools); err != nil {
		return nil, "", err
	}
	name := c.base.model
	if opts.Model != nil && *opts.Model != "" {
		name = *opts.Model
	}
	source := fmt.Sprintf("%x", sha256.Sum256(fmt.Appendf(nil, "%q %q %q %q", c.base.baseURL, c.base.providerName(), name, c.base.apiKey)))
	input, err := responsesInput(messages, c.base.providerName(), source)
	if err != nil {
		return nil, "", err
	}
	extra := c.base.requestExtra(opts)
	for _, key := range []string{"input", "stream", "store", "model", "previous_response_id", "conversation", "background"} {
		if _, ok := extra[key]; ok {
			return nil, "", fmt.Errorf("openai responses: extra body cannot set %q", key)
		}
	}
	include := []string{"reasoning.encrypted_content"}
	if values, ok := extra["include"]; ok {
		raw, err := json.Marshal(values)
		if err != nil {
			return nil, "", fmt.Errorf("openai responses: encode include: %w", err)
		}
		var entries []string
		if err := json.Unmarshal(raw, &entries); err != nil {
			return nil, "", fmt.Errorf("openai responses: include requires strings: %w", err)
		}
		for _, value := range entries {
			if value != include[0] {
				include = append(include, value)
			}
		}
		delete(extra, "include")
	}
	body := map[string]any{"model": name, "input": input, "stream": stream, "store": false, "include": include}
	if opts.MaxTokens != nil {
		body["max_output_tokens"] = *opts.MaxTokens
	}
	if opts.Temperature != nil {
		body["temperature"] = *opts.Temperature
	}
	if opts.TopP != nil {
		body["top_p"] = *opts.TopP
	}
	tools := c.base.toolsFor(opts)
	if len(tools) > 0 {
		wire := make([]map[string]any, 0, len(tools))
		for _, t := range tools {
			f := t.Function
			params := f.Parameters
			if params == nil {
				params = map[string]any{"type": "object", "properties": map[string]any{}}
			}
			// Responses 的缺省严格策略会改写业务 schema 的可选参数。工具的本地校验仍生效。
			wire = append(wire, map[string]any{"type": "function", "name": f.Name, "description": f.Description,
				"parameters": params, "strict": false})
		}
		body["tools"] = wire
	}
	if opts.ToolChoice != nil && wireHasTools(tools, extra) {
		body["tool_choice"] = string(*opts.ToolChoice)
	}
	if format := opts.ResponseFormat; format != nil {
		fields := map[string]any{"type": format.Type}
		if s := format.JSONSchema; s != nil {
			fields["name"], fields["schema"], fields["strict"] = s.Name, s.Schema, s.Strict
		}
		body["text"] = map[string]any{"format": fields}
	}
	for key, value := range extra {
		body[key] = value
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, "", fmt.Errorf("openai responses: encode request: %w", err)
	}
	req, err := c.base.newJSONRequest(ctx, "/responses", data)
	return req, source, err
}

func (c *ResponsesClient) do(req *http.Request) (*http.Response, error) {
	resp, err := c.base.client.Do(req)
	if err != nil {
		return nil, c.base.streamError(fmt.Errorf("request failed: %w", err))
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()                                  // 错误路径也归还连接。
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10)) // 已知 HTTP 状态，读到多少就保留多少错误上下文。
		return nil, &model.APIError{Provider: c.base.providerName(), StatusCode: resp.StatusCode, Body: string(data),
			RetryAfter: model.ParseRetryAfter(resp.Header, time.Now())}
	}
	return resp, nil
}

func responsesInput(messages []*schema.Message, provider, source string) ([]any, error) {
	input := make([]any, 0, len(messages))
	for _, m := range messages {
		if m == nil {
			continue
		}
		switch m.Role {
		case schema.RoleAssistant:
			if replay := responsesReplay(m, source); len(replay) > 0 {
				for _, item := range replay {
					input = append(input, item)
				}
				continue
			}
			text := m.Content
			if m.Refusal != "" {
				if text != "" {
					text += "\n"
				}
				text += m.Refusal
			}
			if text != "" {
				input = append(input, map[string]any{"role": "assistant", "content": text})
			}
			for _, call := range schema.GetToolCalls(m) {
				input = append(input, map[string]any{"type": "function_call", "call_id": call.ID,
					"name": call.Function.Name, "arguments": call.Function.Arguments})
			}
		case schema.RoleTool:
			id := schema.GetToolResultID(m)
			if id == "" {
				return nil, errors.New("openai responses: tool result requires a call id")
			}
			input = append(input, map[string]any{"type": "function_call_output", "call_id": id, "output": m.Content})
		case schema.RoleUser:
			content, err := responsesUserContent(m, provider)
			if err != nil {
				return nil, err
			}
			input = append(input, map[string]any{"role": "user", "content": content})
		case schema.RoleSystem, "developer":
			input = append(input, map[string]any{"role": m.Role, "content": m.Content})
		default:
			return nil, fmt.Errorf("openai responses: unsupported message role %q", m.Role)
		}
	}
	return input, nil
}

func responsesUserContent(m *schema.Message, provider string) (any, error) {
	if len(m.ContentBlocks) == 0 {
		return m.Content, nil
	}
	content := make([]map[string]any, 0, len(m.ContentBlocks)+1)
	if m.Content != "" {
		content = append(content, map[string]any{"type": "input_text", "text": m.Content})
	}
	for _, block := range m.ContentBlocks {
		if block == nil {
			continue
		}
		if block.File != nil && block.File.File != nil && block.MediaType == "application/pdf" {
			file := block.File.File
			if file.ID == "" && file.FileData == "" && file.URL != "" {
				content = append(content, map[string]any{"type": "input_file", "file_url": file.URL})
				continue
			}
		}
		part, err := userContentPart(block, provider)
		if err != nil {
			return nil, fmt.Errorf("openai responses: %w", err)
		}
		switch part.Type {
		case "text":
			content = append(content, map[string]any{"type": "input_text", "text": part.Text})
		case "image_url":
			content = append(content, map[string]any{"type": "input_image", "image_url": part.ImageURL.URL, "detail": "auto"})
		case "file":
			file := map[string]any{"type": "input_file"}
			if part.File.ID != "" {
				file["file_id"] = part.File.ID
			} else {
				file["file_data"], file["filename"] = part.File.Data, part.File.Filename
			}
			content = append(content, file)
		case "":
			continue
		default:
			return nil, fmt.Errorf("openai responses: unsupported input part %q", part.Type)
		}
	}
	if len(content) == 0 {
		return m.Content, nil
	}
	return content, nil
}

const responsesReplayKey = "openai_responses_items"

type responsesEnvelope struct {
	Source string            `json:"source"`
	Digest string            `json:"digest"`
	Items  []json.RawMessage `json:"items"`
}

func responseDigest(m *schema.Message) string {
	var input strings.Builder
	fmt.Fprintf(&input, "%q %q %q", m.Content, m.ReasoningContent, m.Refusal)
	for _, call := range schema.GetToolCalls(m) {
		fmt.Fprintf(&input, " %q %q %q", call.ID, call.Function.Name, call.Function.Arguments)
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(input.String())))
}

func responsesReplay(m *schema.Message, source string) []json.RawMessage {
	raw, _ := m.Annotations[responsesReplayKey].(string)
	if raw == "" {
		return nil
	}
	var envelope responsesEnvelope
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || envelope.Source != source || envelope.Digest != responseDigest(m) {
		return nil
	}
	return envelope.Items
}
