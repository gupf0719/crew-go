package openai

import (
	"encoding/json"

	"crew-go/schema"
)

type chatTokenLogprob struct {
	Token   string   `json:"token"`
	Bytes   []int32  `json:"bytes"`
	Logprob *float64 `json:"logprob"`
}

func (t chatTokenLogprob) toSchema() schema.LogProb {
	return schema.LogProb{TokenText: t.Token, Bytes: t.Bytes, Logprob: t.Logprob}
}

// decodeChatLogprobs 读取非流式 Chat Completions 的可见输出概率，保留顺序与缺失位置。
func decodeChatLogprobs(raw json.RawMessage) []*schema.LogProbContent {
	if len(raw) == 0 {
		return nil
	}
	var wire struct {
		Content []*struct {
			chatTokenLogprob
			TopLogprobs []*chatTokenLogprob `json:"top_logprobs"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil || wire.Content == nil {
		return nil
	}
	out := make([]*schema.LogProbContent, len(wire.Content))
	for i, token := range wire.Content {
		if token == nil {
			continue
		}
		record := &schema.LogProbContent{LogProb: token.chatTokenLogprob.toSchema()}
		if token.TopLogprobs != nil {
			record.TopLogprobs = make([]*schema.LogProb, len(token.TopLogprobs))
			for j, candidate := range token.TopLogprobs {
				if candidate != nil {
					record.TopLogprobs[j] = new(candidate.toSchema())
				}
			}
		}
		out[i] = record
	}
	return out
}
