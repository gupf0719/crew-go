package agui

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

var errInvalidPending = errors.New("agui: invalid pending interrupt")

type answerSchemas map[string]*jsonschema.Schema

type interruptBatch struct {
	epoch   string
	events  []RunInterrupt
	open    map[string]bool
	schemas answerSchemas
}

// projectInterrupts 统一 SSE 与重连查询的投影，校验待展示的结构化答复约束。
func projectInterrupts(queries []*compose_v2.AskQuery, fallbackReason string) (interruptBatch, error) {
	batch := interruptBatch{
		events: make([]RunInterrupt, 0, len(queries)),
		open:   make(map[string]bool, len(queries)),
	}
	for _, q := range queries {
		if q == nil || q.InterruptID == "" {
			return interruptBatch{}, errInvalidPending
		}
		if _, exists := batch.open[q.InterruptID]; exists {
			return interruptBatch{}, fmt.Errorf("%w: duplicate id %q", errInvalidPending, q.InterruptID)
		}
		reason := q.Reason
		if reason == "" {
			reason = fallbackReason
		}
		isApproval := reason == agent_v2.ApprovalInterruptReason
		batch.open[q.InterruptID] = isApproval
		if isApproval {
			batch.events = append(batch.events, approvalInterrupt(q))
			continue
		}
		out := questionInterrupt(q, reason)
		if len(q.ResponseSchema) > 0 {
			var wire map[string]any
			dec := json.NewDecoder(bytes.NewReader(q.ResponseSchema))
			dec.UseNumber()
			if err := dec.Decode(&wire); err != nil {
				return interruptBatch{}, fmt.Errorf("%w: interrupt %q response schema: %w", errInvalidPending, q.InterruptID, err)
			}
			if wire == nil {
				return interruptBatch{}, fmt.Errorf("%w: interrupt %q response schema must be an object", errInvalidPending, q.InterruptID)
			}
			compiled, err := compileResponseSchema(wire)
			if err != nil {
				return interruptBatch{}, fmt.Errorf("%w: interrupt %q response schema: %w", errInvalidPending, q.InterruptID, err)
			}
			out.ResponseSchema = wire
			if batch.schemas == nil {
				batch.schemas = make(answerSchemas)
			}
			batch.schemas[q.InterruptID] = compiled
		}
		batch.events = append(batch.events, out)
	}
	return batch, nil
}

func compileResponseSchema(wire map[string]any) (*jsonschema.Schema, error) {
	const id = "https://crew-go.invalid/agui/answer.json"
	c := jsonschema.NewCompiler()
	c.UseLoader(localSchemaLoader{})
	if err := c.AddResource(id, wire); err != nil {
		return nil, err
	}
	return c.Compile(id)
}

type localSchemaLoader struct{}

func (localSchemaLoader) Load(string) (any, error) {
	return nil, errors.New("external response schema references are unsupported")
}
