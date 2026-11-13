package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"crew-go/internal/panicerr"
	"crew-go/model"
	"crew-go/schema"
)

type responsesResult struct {
	Model  string            `json:"model"`
	Status string            `json:"status"`
	Output []json.RawMessage `json:"output"`
	Error  responsesError    `json:"error"`
	Usage  *struct {
		Input     int                            `json:"input_tokens"`
		Output    int                            `json:"output_tokens"`
		Total     int                            `json:"total_tokens"`
		Cached    *openaiPromptTokensDetails     `json:"input_tokens_details"`
		Reasoning *openaiCompletionTokensDetails `json:"output_tokens_details"`
	} `json:"usage"`
	Incomplete struct {
		Reason string `json:"reason"`
	} `json:"incomplete_details"`
}

type responsesError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type responsesItem struct {
	ID               string `json:"id"`
	Type             string `json:"type"`
	CallID           string `json:"call_id"`
	Name             string `json:"name"`
	Arguments        string `json:"arguments"`
	EncryptedContent string `json:"encrypted_content"`
	Content          []struct {
		Type    string `json:"type"`
		Text    string `json:"text"`
		Refusal string `json:"refusal"`
	} `json:"content"`
	Summary []struct {
		Text string `json:"text"`
	} `json:"summary"`
}

func (c *ResponsesClient) resultError(api responsesError, status string) error {
	switch api.Code {
	case "context_length_exceeded":
		return c.base.streamError(model.ErrContextWindowExceeded)
	case "content_filter":
		return c.base.streamError(model.ErrContentFiltered)
	}
	if api.Message != "" {
		return c.base.streamError(fmt.Errorf("responses %s: %s", status, clip(api.Message)))
	}
	return c.base.streamError(fmt.Errorf("responses ended with status %q", status))
}

func (c *ResponsesClient) convert(result *responsesResult, source string) (*schema.Message, error) {
	if result.Status != "completed" {
		if result.Status == "incomplete" && result.Incomplete.Reason != "" {
			return nil, c.resultError(responsesError{Code: result.Incomplete.Reason}, result.Incomplete.Reason)
		}
		return nil, c.resultError(result.Error, result.Status)
	}
	if result.Output == nil {
		return nil, c.base.streamError(errors.New("completed responses result has no output"))
	}
	msg := &schema.Message{Role: schema.RoleAssistant, Metadata: &schema.MessageMetadata{
		Model: result.Model, System: c.base.system, FinishReason: "stop",
	}}
	if u := result.Usage; u != nil {
		usage := openaiUsage{PromptTokens: u.Input, CompletionTokens: u.Output, TotalTokens: u.Total,
			PromptTokensDetails: u.Cached, CompletionTokensDetails: u.Reasoning}
		msg.Metadata.Usage = usage.toSchema()
	}
	items := make([]json.RawMessage, 0, len(result.Output))
	seenCalls := make(map[string]bool)
	var text, reasoning, refusal strings.Builder
	for _, raw := range result.Output {
		var item responsesItem
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, c.base.streamError(fmt.Errorf("decode responses output item: %w", err))
		}
		switch item.Type {
		case "message":
			for _, part := range item.Content {
				switch part.Type {
				case "output_text":
					text.WriteString(part.Text)
				case "refusal":
					refusal.WriteString(part.Refusal)
				}
			}
		case "function_call":
			if item.CallID == "" || item.Name == "" || seenCalls[item.CallID] {
				return nil, c.base.streamError(errors.New("responses returned an invalid function call identity"))
			}
			seenCalls[item.CallID] = true
			msg.ContentBlocks = append(msg.ContentBlocks, &schema.ContentBlock{Type: "tool_call",
				Toolcall: &schema.ToolcallContentBlock{Id: item.CallID, Name: item.Name, Arguments: item.Arguments}})
			msg.Metadata.FinishReason = "tool_calls"
		case "reasoning":
			for _, part := range item.Summary {
				reasoning.WriteString(part.Text)
			}
			if item.EncryptedContent == "" {
				continue
			}
		default:
			continue
		}
		items = append(items, raw)
	}
	msg.Content, msg.ReasoningContent, msg.Refusal = text.String(), reasoning.String(), refusal.String()
	if len(items) > 0 {
		raw, err := json.Marshal(responsesEnvelope{Source: source, Digest: responseDigest(msg), Items: items})
		if err != nil {
			return nil, c.base.streamError(fmt.Errorf("encode responses replay: %w", err))
		}
		msg.Annotations = map[string]any{responsesReplayKey: string(raw)}
	}
	return msg, nil
}

// Stream 逐片交付 Responses 的文本、推理摘要和拒答。终帧提交工具调用、用量和续轮所需的项。
func (c *ResponsesClient) Stream(ctx context.Context, messages []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	if c.base.streamUnsupported {
		return nil, model.ErrStreamUnsupported
	}
	streamCtx, cancel := context.WithCancel(ctx)
	req, source, err := c.request(streamCtx, messages, model.Apply(opts...), true)
	if err != nil {
		cancel()
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	body := resp.Body
	if c.base.streamIdleTimeout > 0 {
		body = newIdleReader(body, c.base.streamIdleTimeout)
	}
	sr, send := schema.NewStreamReaderWithCancel[*schema.Message](8, cancel)
	go c.consume(ctx, cancel, body, source, sr, send)
	return sr, nil
}

func (c *ResponsesClient) consume(ctx context.Context, cancel context.CancelFunc, body io.ReadCloser, source string,
	sr *schema.StreamReader[*schema.Message], send func(*schema.Message)) {
	defer cancel()
	defer body.Close() // 消费停止时关闭流式响应。
	defer schema.CloseSend(sr)
	defer func() {
		if rec := recover(); rec != nil {
			pe := panicerr.New(rec)
			schema.SendError(sr, panicerr.WrapOpaque("openai responses stream panic", pe))
			panicerr.Log(ctx, slog.LevelError, "openai: Responses 流解析异常", pe)
		}
	}()
	if err := c.readStream(ctx, body, source, send); err != nil {
		select {
		case <-sr.Done():
			return
		default:
			schema.SendError(sr, err)
		}
	}
}

type responsesPart struct {
	output, part int
}

func (p responsesPart) before(other responsesPart) bool {
	return p.output < other.output || p.output == other.output && p.part < other.part
}

// responsesDeltas 只实时发送已确认顺序的前缀。交错到来的后续部分从最终快照补齐。
type responsesDeltas struct {
	text     strings.Builder
	active   responsesPart
	pending  map[responsesPart]bool
	deferred *responsesPart
	started  bool
	done     bool
}

func (d *responsesDeltas) delta(part responsesPart, text string, earlierItem bool) string {
	if text == "" {
		return ""
	}
	blocked := earlierItem || (d.deferred != nil && !part.before(*d.deferred))
	for prior := range d.pending {
		blocked = blocked || prior.before(part)
	}
	if !blocked {
		switch {
		case !d.started:
			d.active, d.started = part, true
		case d.active == part && !d.done:
		case d.done && d.active.before(part):
			d.active, d.done = part, false
		default:
			blocked = true
		}
	}
	if blocked {
		if d.deferred == nil || part.before(*d.deferred) {
			d.deferred = &part
		}
		return ""
	}
	d.text.WriteString(text)
	return text
}

func (d *responsesDeltas) announce(part responsesPart) {
	if d.pending == nil {
		d.pending = make(map[responsesPart]bool)
	}
	d.pending[part] = true
}

func (d *responsesDeltas) finish(part responsesPart) {
	delete(d.pending, part)
	if d.started && d.active == part {
		d.done = true
	}
}

func (d *responsesDeltas) finishItem(output int) {
	for part := range d.pending {
		if part.output == output {
			delete(d.pending, part)
		}
	}
	if d.started && d.active.output == output {
		d.done = true
	}
}

func earlierResponsesItem(pending map[int]string, index int, kind string) bool {
	for earlier, itemType := range pending {
		if earlier < index && itemType == kind {
			return true
		}
	}
	return false
}

func finalizeResponsesOutput(output []json.RawMessage, done map[int]json.RawMessage) []json.RawMessage {
	if output == nil && len(done) > 0 {
		output = make([]json.RawMessage, 0, len(done))
		for _, index := range slices.Sorted(maps.Keys(done)) {
			output = append(output, done[index])
		}
		return output
	}
	for index, raw := range output {
		if done[index] == nil {
			continue
		}
		var final, saved responsesItem
		if json.Unmarshal(raw, &final) == nil && final.Type == "reasoning" && final.EncryptedContent == "" &&
			json.Unmarshal(done[index], &saved) == nil && saved.Type == final.Type && saved.ID == final.ID &&
			saved.EncryptedContent != "" {
			output[index] = done[index]
		}
	}
	return output
}

func (c *ResponsesClient) readStream(ctx context.Context, body io.Reader, source string, send func(*schema.Message)) error {
	decoder := newSSEDecoder(body)
	var text, reasoning, refusal responsesDeltas
	items := make(map[int]json.RawMessage)
	pending := make(map[int]string)
	for {
		frame, err := decoder.next()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, io.EOF) || strings.TrimSpace(frame.data) == "[DONE]" {
			return c.base.streamError(errors.New("responses stream ended without response.completed"))
		}
		if err != nil {
			return c.base.streamError(fmt.Errorf("read responses stream: %w", err))
		}
		if frame.data == "" {
			continue
		}
		var ev struct {
			Type         string          `json:"type"`
			Delta        string          `json:"delta"`
			Response     responsesResult `json:"response"`
			Item         json.RawMessage `json:"item"`
			OutputIndex  int             `json:"output_index"`
			ContentIndex int             `json:"content_index"`
			SummaryIndex int             `json:"summary_index"`
			Part         struct {
				Type string `json:"type"`
			} `json:"part"`
			responsesError
		}
		if err := json.Unmarshal([]byte(frame.data), &ev); err != nil {
			return c.base.streamError(fmt.Errorf("decode responses stream event: %w", err))
		}
		if frame.event == "error" || ev.Type == "error" {
			return c.resultError(ev.responsesError, "stream error")
		}
		chunk := &schema.Message{Role: schema.RoleAssistant}
		part := responsesPart{ev.OutputIndex, ev.ContentIndex}
		summary := responsesPart{ev.OutputIndex, ev.SummaryIndex}
		switch ev.Type {
		case "response.output_text.delta":
			chunk.Content = text.delta(part, ev.Delta, earlierResponsesItem(pending, ev.OutputIndex, "message"))
		case "response.reasoning_summary_text.delta":
			chunk.ReasoningContent = reasoning.delta(summary, ev.Delta, earlierResponsesItem(pending, ev.OutputIndex, "reasoning"))
		case "response.refusal.delta":
			chunk.Refusal = refusal.delta(part, ev.Delta, earlierResponsesItem(pending, ev.OutputIndex, "message"))
		case "response.content_part.added":
			switch ev.Part.Type {
			case "output_text":
				text.announce(part)
			case "refusal":
				refusal.announce(part)
			}
		case "response.content_part.done":
			text.finish(part)
			refusal.finish(part)
		case "response.reasoning_summary_part.added":
			reasoning.announce(summary)
		case "response.reasoning_summary_part.done":
			reasoning.finish(summary)
		case "response.output_text.done":
			text.finish(part)
		case "response.reasoning_summary_text.done":
			reasoning.finish(summary)
		case "response.refusal.done":
			refusal.finish(part)
		case "response.output_item.added":
			var item responsesItem
			if len(ev.Item) != 0 {
				if err := json.Unmarshal(ev.Item, &item); err != nil {
					return c.base.streamError(fmt.Errorf("decode responses added item: %w", err))
				}
				if item.Type != "" {
					pending[ev.OutputIndex] = item.Type
				}
			}
		case "response.output_item.done":
			if !isErrorValue(ev.Item) {
				return c.base.streamError(errors.New("responses output_item.done has no item"))
			}
			delete(pending, ev.OutputIndex)
			items[ev.OutputIndex] = ev.Item
			text.finishItem(ev.OutputIndex)
			reasoning.finishItem(ev.OutputIndex)
			refusal.finishItem(ev.OutputIndex)
		case "response.completed":
			if ev.Response.Status == "" {
				ev.Response.Status = "completed"
			}
			if ev.Response.Output == nil && len(pending) != 0 {
				return c.base.streamError(errors.New("responses stream completed with unfinished output items"))
			}
			ev.Response.Output = finalizeResponsesOutput(ev.Response.Output, items)
			msg, err := c.convert(&ev.Response, source)
			if err != nil {
				return err
			}
			if !strings.HasPrefix(msg.Content, text.text.String()) || !strings.HasPrefix(msg.ReasoningContent, reasoning.text.String()) ||
				!strings.HasPrefix(msg.Refusal, refusal.text.String()) {
				return c.base.streamError(errors.New("responses stream deltas disagree with the completed output"))
			}
			msg.Content = msg.Content[text.text.Len():]
			msg.ReasoningContent = msg.ReasoningContent[reasoning.text.Len():]
			msg.Refusal = msg.Refusal[refusal.text.Len():]
			send(msg)
			return nil
		case "response.failed", "response.incomplete", "response.cancelled":
			if ev.Response.Status == "completed" {
				return c.base.streamError(errors.New("responses stream reported conflicting terminal status"))
			}
			_, err := c.convert(&ev.Response, source)
			return err
		default:
			continue
		}
		if chunk.Content != "" || chunk.ReasoningContent != "" || chunk.Refusal != "" {
			send(chunk)
		}
	}
}
