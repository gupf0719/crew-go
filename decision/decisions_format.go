package decision

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
)

// 本文件是 OpenAI Decisions 格式（[ProtocolOpenAIDecisions]）的请求编码与响应解码。请求与响应的
// Go 类型跟 System One 共用，校验也在编码之前按同一套规则做过，这里只换线上格式。

type decisionsRequest struct {
	Model     string              `json:"model"`
	Input     string              `json:"input"`
	Questions []decisionsQuestion `json:"questions"`
}

type decisionsQuestion struct {
	Type         string            `json:"type"`
	Name         string            `json:"name"`
	Instructions string            `json:"instructions"`
	Choices      []decisionsChoice `json:"choices,omitempty"`
	Levels       []decisionsLevel  `json:"levels,omitempty"`
}

type decisionsChoice struct {
	Value       string `json:"value"`
	Description string `json:"description,omitempty"`
}

type decisionsLevel struct {
	Label string `json:"label"`
}

// encodeDecisionsRequest 把已校验过的请求编成 OpenAI Decisions 格式，换法见 [ProtocolOpenAIDecisions]。
func encodeDecisionsRequest(req Request) ([]byte, error) {
	state, err := marshalText(req.State)
	if err != nil {
		return nil, err
	}
	input, err := jsonText(state)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(req.Questions))
	for name := range req.Questions {
		names = append(names, name)
	}
	slices.Sort(names)
	wire := decisionsRequest{Model: req.Model, Input: input, Questions: make([]decisionsQuestion, 0, len(names))}
	for _, name := range names {
		dq, err := decisionsQuestionOf(name, req.Questions[name])
		if err != nil {
			return nil, err
		}
		wire.Questions = append(wire.Questions, dq)
	}
	return json.Marshal(wire)
}

func decisionsQuestionOf(name string, q Question) (decisionsQuestion, error) {
	dq := decisionsQuestion{Name: name, Instructions: q.Instructions}
	if strings.TrimSpace(dq.Instructions) == "" {
		dq.Instructions = name
	}
	criteria, err := marshalText(q.Criteria)
	if err != nil {
		return dq, err
	}
	switch q.Type {
	case TypeChoice:
		dq.Type = "choice"
		entries, err := objectEntries(criteria)
		if err != nil {
			return dq, err
		}
		for _, e := range entries {
			c := decisionsChoice{Value: e.key}
			if !isNull(e.value) {
				if c.Description, err = jsonText(e.value); err != nil {
					return dq, err
				}
			}
			dq.Choices = append(dq.Choices, c)
		}
	case TypeScore:
		dq.Type = "score"
		var levels []json.RawMessage
		if err := json.Unmarshal(criteria, &levels); err != nil {
			return dq, err
		}
		for _, level := range levels {
			label, err := jsonText(level)
			if err != nil {
				return dq, err
			}
			dq.Levels = append(dq.Levels, decisionsLevel{Label: label})
		}
	case TypeNoul:
		dq.Type = "predicate"
		if isNull(criteria) {
			return dq, nil
		}
		var outcomes map[string]json.RawMessage
		if err := json.Unmarshal(criteria, &outcomes); err != nil {
			return dq, err
		}
		// 这个格式的是非题没有 criteria，两种结果的说明拼在 instructions 后面，先是后否。
		for _, key := range []string{"true", "false"} {
			desc, ok := outcomes[key]
			if !ok || isNull(desc) {
				continue
			}
			text, err := jsonText(desc)
			if err != nil {
				return dq, err
			}
			dq.Instructions += "\n" + strings.ToUpper(key[:1]) + key[1:] + ": " + text
		}
	}
	return dq, nil
}

type objectEntry struct {
	key   string
	value json.RawMessage
}

// objectEntries 按出现顺序取出一个 JSON 对象的键值对。map 编出来的对象键已排好序，自己编码的类型
// 按它给的顺序，选项的次序因此与 System One 格式发出去的一致。
func objectEntries(raw []byte) ([]objectEntry, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, errors.New("decision: criteria is not a JSON object")
	}
	var out []objectEntry
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := tok.(string)
		if !ok {
			return nil, errors.New("decision: criteria object has a non-string key")
		}
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		out = append(out, objectEntry{key: key, value: v})
	}
	return out, nil
}

// marshalText 把要转成文本交给模型读的值编成 JSON，不做 HTML 转义。encoding/json 默认把 < > & 编成
// < 这样的转义：放在 JSON 线上无妨，服务端解析时会还原；可这里编出来的 JSON 本身就是 input 或说明
// 的文本，转义会原样到达模型，模型读到的是 < 字样。
func marshalText(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// jsonText 把一段 JSON 值转成文本：字符串取它的内容，别的值取紧凑的 JSON 文本。
func jsonText(raw []byte) (string, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", err
		}
		return s, nil
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// decisionsProbability 是 choice 与 score 答案里概率数组的一项。value 是候选值，服务端按请求里的原始
// 类型还原（打分题的档位编号是数字）。
type decisionsProbability struct {
	Value       json.RawMessage `json:"value"`
	Label       json.RawMessage `json:"label"`
	Probability *float64        `json:"probability"`
}

// decodeDecisionsResponse 解析 OpenAI Decisions 格式的响应，按 name 放回 [Response] 的 Answers。
// model 与 usage 必须有；answers 须是数组，每个答案须是带 type 与 name 的对象，name 须是这次请求
// 问过的问题、不能重复。认不得的答案类型跳过并记日志，与 System One 格式一致；拒答记为 Refused。
func decodeDecisionsResponse(ctx context.Context, raw []byte, requestID string, questions map[string]Question) (*Response, error) {
	var top struct {
		Model   *string         `json:"model"`
		Usage   *Usage          `json:"usage"`
		Answers json.RawMessage `json:"answers"`
	}
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	switch {
	case top.Model == nil:
		return nil, errors.New("invalid response: missing model")
	case top.Usage == nil:
		return nil, errors.New("invalid response: missing usage")
	case isNull(top.Answers):
		return nil, errors.New("invalid response: answers is null")
	}
	out := &Response{Model: *top.Model, Usage: *top.Usage, Answers: map[string]Answer{}}
	if len(top.Answers) == 0 {
		return out, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(top.Answers, &items); err != nil {
		return nil, fmt.Errorf("decode response answers: %w", err)
	}
	for i, item := range items {
		var probe struct {
			Type *string `json:"type"`
			Name *string `json:"name"`
		}
		if err := json.Unmarshal(item, &probe); err != nil || probe.Type == nil {
			return nil, fmt.Errorf("invalid response: answer %d has no type", i)
		}
		if probe.Name == nil || *probe.Name == "" {
			return nil, fmt.Errorf("invalid response: answer %d has no name", i)
		}
		name := *probe.Name
		q, asked := questions[name]
		if !asked {
			return nil, fmt.Errorf("invalid response: answer %d is for question %q, which was not asked", i, name)
		}
		if _, dup := out.Answers[name]; dup {
			return nil, fmt.Errorf("invalid response: more than one answer for question %q", name)
		}
		var (
			a   Answer
			err error
		)
		switch *probe.Type {
		case "predicate":
			a, err = decodeDecisionsPredicate(name, item)
		case "choice":
			a, err = decodeDecisionsChoice(name, item)
		case "score":
			a, err = decodeDecisionsScore(name, item)
		case "refusal":
			a = Answer{Type: q.Type, Refused: true}
		default:
			slog.WarnContext(ctx, "decision: ignoring answer with unrecognized type",
				"answer", name, "type", *probe.Type, "request_id", requestID)
			continue
		}
		if err != nil {
			return nil, err
		}
		out.Answers[name] = a
	}
	return out, nil
}

func decodeDecisionsPredicate(name string, raw json.RawMessage) (Answer, error) {
	a := Answer{Type: TypeNoul}
	var p struct {
		Probability *float64 `json:"probability"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return a, fmt.Errorf("decode answer %q: %w", name, err)
	}
	if p.Probability == nil {
		return a, fmt.Errorf("invalid response: predicate answer %q is missing probability", name)
	}
	a.Noul = p.Probability
	return a, nil
}

func decodeDecisionsChoice(name string, raw json.RawMessage) (Answer, error) {
	a := Answer{Type: TypeChoice}
	var c struct {
		Choice        json.RawMessage        `json:"choice"`
		Confidence    *float64               `json:"confidence"`
		Probabilities []decisionsProbability `json:"probabilities"`
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return a, fmt.Errorf("decode answer %q: %w", name, err)
	}
	missing := func(field string) error {
		return fmt.Errorf("invalid response: choice answer %q is missing %s", name, field)
	}
	if len(c.Choice) == 0 || isNull(c.Choice) {
		return a, missing("choice")
	}
	choice, err := jsonText(c.Choice)
	if err != nil || choice == "" {
		return a, missing("choice")
	}
	switch {
	case c.Confidence == nil:
		return a, missing("confidence")
	case c.Probabilities == nil:
		return a, missing("probabilities")
	}
	probs, err := decisionsProbabilities(name, TypeChoice, c.Probabilities)
	if err != nil {
		return a, err
	}
	a.Choice, a.Confidence, a.Probabilities = choice, c.Confidence, probs
	return a, nil
}

func decodeDecisionsScore(name string, raw json.RawMessage) (Answer, error) {
	a := Answer{Type: TypeScore}
	var s struct {
		Score         *float64                   `json:"score"`
		Confidence    *float64                   `json:"confidence"`
		Legend        map[string]json.RawMessage `json:"legend"`
		Probabilities []decisionsProbability     `json:"probabilities"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return a, fmt.Errorf("decode answer %q: %w", name, err)
	}
	missing := func(field string) error {
		return fmt.Errorf("invalid response: score answer %q is missing %s", name, field)
	}
	switch {
	case s.Score == nil:
		return a, missing("score")
	case s.Confidence == nil:
		return a, missing("confidence")
	case s.Probabilities == nil:
		return a, missing("probabilities")
	}
	probs, err := decisionsProbabilities(name, TypeScore, s.Probabilities)
	if err != nil {
		return a, err
	}
	// 档位说明优先取 legend，没有时按概率数组里每档的 label 补，键与概率表一样是档位编号。
	legend := map[string]any{}
	if s.Legend != nil {
		for level, desc := range s.Legend {
			v, err := legendValue(name, level, desc)
			if err != nil {
				return a, err
			}
			legend[level] = v
		}
	} else {
		for _, p := range s.Probabilities {
			if len(p.Label) == 0 || isNull(p.Label) {
				continue
			}
			level, err := jsonText(p.Value)
			if err != nil {
				return a, fmt.Errorf("decode answer %q: %w", name, err)
			}
			v, err := legendValue(name, level, p.Label)
			if err != nil {
				return a, err
			}
			legend[level] = v
		}
	}
	if len(legend) == 0 {
		return a, missing("legend")
	}
	a.Score, a.Confidence, a.Legend, a.Probabilities = s.Score, s.Confidence, legend, probs
	return a, nil
}

// legendValue 取一档的说明，须是文本、对象或数组，与 System One 格式的规则相同。
func legendValue(name, level string, desc json.RawMessage) (any, error) {
	if !isContent(desc) {
		return nil, fmt.Errorf("invalid response: score answer %q legend %q must be text, an object or an array", name, level)
	}
	var v any
	if err := json.Unmarshal(desc, &v); err != nil {
		return nil, fmt.Errorf("decode answer %q legend %q: %w", name, level, err)
	}
	return v, nil
}

// decisionsProbabilities 把概率数组换成「候选值 → 概率」：候选值是字符串时取内容，是数字时取它的
// 文本（打分题档位 0 记为 "0"）。概率为 null、候选值缺失或重复时报错。
func decisionsProbabilities(name string, t QuestionType, in []decisionsProbability) (map[string]float64, error) {
	out := make(map[string]float64, len(in))
	for _, p := range in {
		if len(p.Value) == 0 || isNull(p.Value) {
			return nil, fmt.Errorf("invalid response: %s answer %q has a probability without a value", t, name)
		}
		key, err := jsonText(p.Value)
		if err != nil {
			return nil, fmt.Errorf("decode answer %q: %w", name, err)
		}
		if p.Probability == nil {
			return nil, fmt.Errorf("invalid response: %s answer %q has a null probability for %q", t, name, key)
		}
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("invalid response: %s answer %q lists %q more than once", t, name, key)
		}
		out[key] = *p.Probability
	}
	return out, nil
}
