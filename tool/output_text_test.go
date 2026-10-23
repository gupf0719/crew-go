package tool_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"crew-go/tool"
)

type item struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestOutputText(t *testing.T) {
	ch := make(chan int)
	for _, tc := range []struct {
		name string
		in   any
		want string
	}{
		{"string", "原样", "原样"},
		{"utf8 bytes", []byte("文本"), "文本"},
		{"binary bytes as base64", []byte{0xff, 0xfe}, `"//4="`},
		{"map as JSON", map[string]any{"items": []string{"a"}, "total": 1, "html": "<b>"}, `{"html":"<b>","items":["a"],"total":1}`},
		{"struct", item{1, "x"}, `{"id":1,"name":"x"}`},
		{"pointer to struct", &item{2, "y"}, `{"id":2,"name":"y"}`},
		{"slice", []item{{3, "z"}}, `[{"id":3,"name":"z"}]`},
		{"int", 42, "42"},
		{"bool", true, "true"},
		{"nil", nil, "<nil>"},
		{"stringer", 1500 * time.Millisecond, "1.5s"},
		{"error", errors.New("坏了"), "坏了"},
		{"not JSON-able falls back", map[string]any{"c": ch}, "map[c:" + fmtPtr(ch) + "]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tool.OutputText(tc.in); got != tc.want {
				t.Errorf("OutputText(%#v) = %q，期望 %q", tc.in, got, tc.want)
			}
		})
	}
}

func fmtPtr(ch chan int) string { return fmt.Sprintf("%v", ch) }
