package decision

import (
	"encoding/json"
	"errors"
	"math"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

	"crew-go/model"
)

// TestParseRetryAfter：先认 retry-after-ms，再认 retry-after 的秒数或 HTTP-date；
// 负数、非法值、过去的时间都当没给。
func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		hdr  map[string]string
		want time.Duration
	}{
		{"none", nil, 0},
		{"ms", map[string]string{"retry-after-ms": "250"}, 250 * time.Millisecond},
		{"ms wins over seconds", map[string]string{"retry-after-ms": "100", "retry-after": "5"}, 100 * time.Millisecond},
		{"invalid ms falls back to seconds", map[string]string{"retry-after-ms": "abc", "retry-after": "2"}, 2 * time.Second},
		{"seconds", map[string]string{"retry-after": "1.5"}, 1500 * time.Millisecond},
		{"negative seconds", map[string]string{"retry-after": "-3"}, 0},
		{"http date", map[string]string{"retry-after": now.Add(3 * time.Second).Format(http.TimeFormat)}, 3 * time.Second},
		{"past http date", map[string]string{"retry-after": now.Add(-time.Minute).Format(http.TimeFormat)}, 0},
		{"garbage", map[string]string{"retry-after": "soon"}, 0},
		// 越界的浮点转整数由平台决定：arm64 自己会饱和，amd64 会变成负数，所以这两条
		// 只在 amd64 这类平台上抓得到回归。
		{"huge seconds saturate", map[string]string{"retry-after": "1e300"}, time.Duration(math.MaxInt64)},
		{"huge ms saturate", map[string]string{"retry-after-ms": "1e300"}, time.Duration(math.MaxInt64)},
		{"infinite ms falls back to seconds", map[string]string{"retry-after-ms": "Inf", "retry-after": "2"}, 2 * time.Second},
		{"negative ms falls back to seconds", map[string]string{"retry-after-ms": "-5", "retry-after": "1"}, time.Second},
	}
	for _, tc := range cases {
		h := http.Header{}
		for k, v := range tc.hdr {
			h.Set(k, v)
		}
		if got := parseRetryAfter(h, now); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestBackoff：第 k 次重试等首次等待的 2^k 倍、不超过上限，再随机减去至多四分之一；
// 重试次数再大也不溢出。
func TestBackoff(t *testing.T) {
	c := &Client{backoffInitial: 500 * time.Millisecond, backoffMax: 5 * time.Second}
	cases := []struct {
		retry int
		full  time.Duration
	}{
		{0, 500 * time.Millisecond},
		{1, time.Second},
		{3, 4 * time.Second},
		{4, 5 * time.Second},
		{1000, 5 * time.Second},
	}
	for _, tc := range cases {
		for range 200 {
			d := c.backoff(tc.retry)
			if d > tc.full || d < tc.full*3/4 {
				t.Fatalf("retry %d: backoff %v outside [%v, %v]", tc.retry, d, tc.full*3/4, tc.full)
			}
		}
	}
	if d := (&Client{}).backoff(3); d != 0 {
		t.Fatalf("zero backoff config should not wait, got %v", d)
	}
}

// TestRetryable：对齐官方 SDK 的可重试集合。
func TestRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"no http response", &model.APIError{Provider: "typesafe", Err: errors.New("connection reset")}, true},
		{"408", &model.APIError{StatusCode: 408}, true},
		{"429", &model.APIError{StatusCode: 429}, true},
		{"500", &model.APIError{StatusCode: 500}, true},
		{"529", &model.APIError{StatusCode: 529}, true},
		{"400", &model.APIError{StatusCode: 400}, false},
		{"401", &model.APIError{StatusCode: 401}, false},
		{"422", &model.APIError{StatusCode: 422}, false},
		{"600", &model.APIError{StatusCode: 600}, false},
		{"malformed response", &responseError{err: errors.New("decode")}, false},
		{"plain error", errors.New("build request"), false},
	}
	for _, tc := range cases {
		if got := retryable(tc.err); got != tc.want {
			t.Errorf("%s: retryable = %v, want %v", tc.name, got, tc.want)
		}
	}
}

type plainBytes []byte

type valueJSONBytes []byte

func (valueJSONBytes) MarshalJSON() ([]byte, error) { return []byte(`"v"`), nil }

type ptrJSONBytes []byte

func (*ptrJSONBytes) MarshalJSON() ([]byte, error) { return []byte(`"p"`), nil }

type ptrTextBytes []byte

func (*ptrTextBytes) MarshalText() ([]byte, error) { return []byte("t"), nil }

type textByte uint8

func (*textByte) MarshalText() ([]byte, error) { return []byte("x"), nil }

type selfPtr *selfPtr

// TestIsRawBytes：以 encoding/json 的实际输出为准——编出来是 "hello" 的 base64 时才算。
func TestIsRawBytes(t *testing.T) {
	hello := []byte("hello")
	pj := ptrJSONBytes(hello)
	pt := ptrTextBytes(hello)
	var anyBytes any = hello
	// 经接口拿到的值不可寻址，指针接收者的 MarshalJSON 用不上——这正是本判据的立论。
	var anyPJ any = ptrJSONBytes(hello)
	// 任意深的指针链：encoding/json 照样编成 base64。
	deep := any(hello)
	for range 17 {
		p := reflect.New(reflect.TypeOf(deep))
		p.Elem().Set(reflect.ValueOf(deep))
		deep = p.Interface()
	}
	cases := map[string]any{
		"bytes":                      hello,
		"pointer to bytes":           &hello,
		"pointer to pointer":         func() **[]byte { p := &hello; return &p }(),
		"raw message":                json.RawMessage(`"hello"`),
		"ip":                         net.ParseIP("10.0.0.1"),
		"named plain":                plainBytes(hello),
		"value receiver json":        valueJSONBytes(hello),
		"pointer receiver json":      ptrJSONBytes(hello),
		"pointer to ptr-json":        &pj,
		"pointer receiver text":      ptrTextBytes(hello),
		"pointer to ptr-text":        &pt,
		"element with text methods":  []textByte{'h', 'e', 'l', 'l', 'o'},
		"array":                      [5]byte{'h', 'e', 'l', 'l', 'o'},
		"interface":                  anyBytes,
		"pointer to interface":       &anyBytes,
		"nil self pointer":           selfPtr(nil),
		"interface holding ptr-json": &anyPJ,
		"interface holding pointer":  any(&pj),
		"deep pointer chain":         deep,
		"string":                     "hello",
	}
	for name, v := range cases {
		raw, err := json.Marshal(v)
		want := err == nil && string(raw) == `"aGVsbG8="`
		if got := isRawBytes(v); got != want {
			t.Errorf("%s: isRawBytes = %v, but encoding/json gives %s", name, got, raw)
		}
	}
}

// TestIsRawBytes_PointerCycleDoesNotHang：自引用指针编不出 base64，判据要走得完。
// 这里不调 json.Marshal 当 oracle——环形输入在 encoding/json 的新实现下会撑爆栈，
// 连带整个测试进程。
func TestIsRawBytes_PointerCycleDoesNotHang(t *testing.T) {
	var x selfPtr
	x = &x
	done := make(chan bool, 1)
	go func() { done <- isRawBytes(x) }()
	select {
	case got := <-done:
		if got {
			t.Fatal("a self-referencing pointer never encodes as base64")
		}
	case <-time.After(time.Second):
		t.Fatal("isRawBytes did not return")
	}
}
