package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"crew-go/agui"
)

func TestSOP_ValidateThenApproveExactlyOnce(t *testing.T) {
	var executions []string
	h, err := newHandler(func(plan string) { executions = append(executions, plan) })
	if err != nil {
		t.Fatal(err)
	}
	q := startSOP(t, h, "approve")
	for _, payload := range []string{`{"approved":"yes","reviewer":"Alice"}`, `{"approved":true}`} {
		r := answerSOP(h, "approve", q.ID, payload)
		if r.Code != http.StatusBadRequest || !strings.Contains(r.Body.String(), "invalid_resume_payload") {
			t.Fatalf("结构化答复应拒绝 %s，得到 %d %s", payload, r.Code, r.Body)
		}
	}
	if len(executions) != 0 {
		t.Fatalf("非法答复已推进执行: %v", executions)
	}
	payload := `{"approved":true,"reviewer":"Alice"}`
	r := answerSOP(h, "approve", q.ID, payload)
	if !strings.Contains(r.Body.String(), `"type":"success"`) || len(executions) != 1 ||
		!strings.Contains(executions[0], "验收：") {
		t.Fatalf("应执行校验后的方案一次: %v, %s", executions, r.Body)
	}
	r = answerSOP(h, "approve", q.ID, payload)
	if r.Code != http.StatusNotFound || len(executions) != 1 {
		t.Fatalf("重发不得再次执行: %v, %d %s", executions, r.Code, r.Body)
	}
}

func TestSOP_RejectEndsWithoutExecuting(t *testing.T) {
	calls := 0
	h, err := newHandler(func(string) { calls++ })
	if err != nil {
		t.Fatal(err)
	}
	q := startSOP(t, h, "deny")
	r := answerSOP(h, "deny", q.ID, `{"approved":false,"reviewer":"Alice"}`)
	if !strings.Contains(r.Body.String(), `"type":"success"`) || calls != 0 {
		t.Fatalf("审核拒绝应正常结束且不执行: calls=%d, %s", calls, r.Body)
	}
}

func startSOP(t *testing.T, h http.Handler, thread string) agui.RunInterrupt {
	t.Helper()
	r := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/agui", strings.NewReader(
		`{"threadId":"`+thread+`","runId":"start","messages":[{"role":"user","content":"v1.2"}]}`))
	h.ServeHTTP(r, req)
	for line := range strings.SplitSeq(r.Body.String(), "\n") {
		if data, ok := strings.CutPrefix(line, "data: "); ok && strings.Contains(data, `"type":"RUN_FINISHED"`) {
			var ev agui.RunFinishedEvent
			if err := json.Unmarshal([]byte(data), &ev); err != nil {
				t.Fatal(err)
			}
			if ev.Outcome != nil && len(ev.Outcome.Interrupts) == 1 {
				q := ev.Outcome.Interrupts[0]
				if !strings.Contains(q.Message, "第 2 版") || q.ResponseSchema["type"] != "object" {
					t.Fatalf("应先回退再展示结构化审核: %+v", q)
				}
				return q
			}
		}
	}
	t.Fatalf("没有审核中断: %s", r.Body)
	return agui.RunInterrupt{}
}

func answerSOP(h http.Handler, thread, id, payload string) *httptest.ResponseRecorder {
	r := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/agui", strings.NewReader(`{"threadId":"`+thread+
		`","runId":"resume","resume":[{"interruptId":"`+id+`","status":"resolved","payload":`+payload+`}]}`))
	h.ServeHTTP(r, req)
	return r
}
