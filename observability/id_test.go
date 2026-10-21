package observability

import (
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// CS-OSS spec OTHER_LANGUAGE_SDK_INTEGRATION.md §Trace ID / Span ID 正则:
//
//	Trace ID: {appname}^^{32hex}|{epoch_millis}
//	Span ID:  root: {index}|{16hex}   sync: {cat}.{idx}|{16hex}   async: {cat}^{idx}|{16hex}
//
// 跟 monitor SDK span.go:22-23 同款正则.
//
// review P0 audit MED-2 fix: `([\w-]+\:)*` (0-N) 跟 monitor SDK 同款而非 `?` (0-1),
// 让 multi-hop RPC 透传 parent (e.g., "svc1:svc2:1.2|hex") 测试也能覆盖.
var (
	traceIDRe = regexp.MustCompile(`^[\w-]+\^\^[0-9a-f]{32}\|\d{13}$`)
	spanIDRe  = regexp.MustCompile(`^([\w-]+\:)*\d+([.^]\d+)*\|[0-9a-f]{16}$`)
)

func TestGenTraceID_FormatMatchesSpec(t *testing.T) {
	id := GenTraceID("my-app")
	if !traceIDRe.MatchString(id) {
		t.Errorf("GenTraceID 格式不匹配 spec 正则\n  got = %q\n  want pattern = %s", id, traceIDRe.String())
	}
	// 头部 appname 真在
	if !strings.HasPrefix(id, "my-app^^") {
		t.Errorf("GenTraceID 应以 appname^^ 开头, got = %q", id)
	}
	// 尾部 epoch_millis 真是当前时间附近 (允许 5s 偏差)
	parts := strings.Split(id, "|")
	if len(parts) != 2 {
		t.Fatalf("GenTraceID 应以 | 分两段, got = %q", id)
	}
	millis, err := parseInt64(parts[1])
	if err != nil {
		t.Fatalf("epoch_millis 段无法解析: %v", err)
	}
	now := time.Now().UnixMilli()
	if abs(now-millis) > 5000 {
		t.Errorf("epoch_millis 跟当前时间偏差 > 5s, got = %d, now = %d", millis, now)
	}
}

func TestGenTraceID_AppnameWithDashes(t *testing.T) {
	// spec line 116-127 没限制 appname 字符集, 但 monitor SDK 正则用 \w- 允许 dash
	id := GenTraceID("multi-word-app-name")
	if !traceIDRe.MatchString(id) {
		t.Errorf("dash appname 不匹配 spec 正则: %q", id)
	}
}

func TestGenRootSpanID_FormatMatchesSpec(t *testing.T) {
	id := GenRootSpanID()
	if !spanIDRe.MatchString(id) {
		t.Errorf("GenRootSpanID 格式不匹配 spec 正则: %q", id)
	}
	if !strings.HasPrefix(id, "1|") {
		t.Errorf("GenRootSpanID 应以 '1|' 开头 (spec line 224), got = %q", id)
	}
}

func TestGenChildSpanID_SyncAndAsync(t *testing.T) {
	parent := "1|abcdef0123456789"

	sync := GenChildSpanID(parent, 2, true)
	if !spanIDRe.MatchString(sync) {
		t.Errorf("sync child 格式错: %q", sync)
	}
	if !strings.HasPrefix(sync, "1.2|") {
		t.Errorf("sync child 应是 '1.2|...', got = %q", sync)
	}

	async := GenChildSpanID(parent, 3, false)
	if !spanIDRe.MatchString(async) {
		t.Errorf("async child 格式错: %q", async)
	}
	if !strings.HasPrefix(async, "1^3|") {
		t.Errorf("async child 应是 '1^3|...', got = %q", async)
	}
}

func TestGenChildSpanID_DeepNesting(t *testing.T) {
	// 多层 catalogue 透传: "1.2^3" 作 parent 派生孙子级
	grandparent := "1.2^3|deadbeef00001111"
	child := GenChildSpanID(grandparent, 4, true)
	if !strings.HasPrefix(child, "1.2^3.4|") {
		t.Errorf("deep nested child 应保留父 catalogue, got = %q", child)
	}
}

func TestGenChildSpanID_DegradeOnInvalidParent(t *testing.T) {
	// parent 无 "|" 分隔符 → 退化成 root 不抛 panic
	got := GenChildSpanID("invalid_no_pipe", 5, true)
	if !spanIDRe.MatchString(got) {
		t.Errorf("退化生成的 child 应仍合法 span ID, got = %q", got)
	}
	if !strings.HasPrefix(got, "5|") {
		t.Errorf("退化 child 应以 index '5|' 开头, got = %q", got)
	}
}

func TestGenRPCChildSpanID_HasServicePrefix(t *testing.T) {
	parent := "1.2|abc0000000000000"
	got := GenRPCChildSpanID(parent, "order-service", 1)
	if !spanIDRe.MatchString(got) {
		t.Errorf("RPC child 格式错: %q", got)
	}
	if !strings.HasPrefix(got, "order-service:1.2.1|") {
		t.Errorf("RPC child 应是 'service:cat.idx|...', got = %q", got)
	}
}

func TestParseTraceIDApp(t *testing.T) {
	tests := []struct {
		id   string
		want string
	}{
		{"order-service^^4bf92f3577b34da6a3ce929d0e0e4736|1715750400123", "order-service"},
		{"multi-word-app^^abcdef0123456789abcdef0123456789|1234567890123", "multi-word-app"},
		{"", ""},
		{"no_separator", ""},
		{"^^missing_appname|123", ""},
	}
	for _, tc := range tests {
		got := ParseTraceIDApp(tc.id)
		if got != tc.want {
			t.Errorf("ParseTraceIDApp(%q) = %q, want %q", tc.id, got, tc.want)
		}
	}
}

func TestExtractCatalogue(t *testing.T) {
	tests := []struct {
		spanID string
		want   string
	}{
		{"1|abc", "1"},
		{"1.2|abc", "1.2"},
		{"1.2^3|abc", "1.2^3"},
		{"order-service:1.2|abc", "order-service:1.2"},
		{"|abc", ""},
		{"no_pipe", ""},
	}
	for _, tc := range tests {
		got := extractCatalogue(tc.spanID)
		if got != tc.want {
			t.Errorf("extractCatalogue(%q) = %q, want %q", tc.spanID, got, tc.want)
		}
	}
}

func TestRandHex_Uniqueness(t *testing.T) {
	// crypto/rand 跨调用应几乎全唯一 (1000 次无碰撞)
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		h := randHex(16)
		if len(h) != 16 {
			t.Fatalf("randHex(16) 应返 16 char, got len = %d", len(h))
		}
		if seen[h] {
			t.Fatalf("randHex 重复了 %q (1000 次 within 测试 = crypto/rand 异常)", h)
		}
		seen[h] = true
	}
}

func TestRandHex_OddLengthPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("randHex(odd) 应 panic")
		}
	}()
	randHex(15)
}

func TestConfig_ApplyDefaults(t *testing.T) {
	c := Config{}
	c.applyDefaults()
	if c.MaxSpanAttributes != 128 {
		t.Errorf("MaxSpanAttributes 默认应 128, got %d", c.MaxSpanAttributes)
	}
	if c.MaxAttributeKeyChars != 255 {
		t.Errorf("MaxAttributeKeyChars 默认应 255 (spec line 166), got %d", c.MaxAttributeKeyChars)
	}
	if c.MaxAttributeValueChars != 512 {
		t.Errorf("MaxAttributeValueChars 默认应 512 (spec line 169), got %d", c.MaxAttributeValueChars)
	}
	if c.MaxInlineEventBytes != 1024*1024 {
		t.Errorf("MaxInlineEventBytes 默认应 1MB (spec line 141), got %d", c.MaxInlineEventBytes)
	}
	if c.TraceExportInterval != 5*time.Second {
		t.Errorf("TraceExportInterval 默认应 5s (spec line 419), got %v", c.TraceExportInterval)
	}
	if c.SamplingRatio != 1.0 {
		t.Errorf("SamplingRatio 默认应 1.0, got %v", c.SamplingRatio)
	}
}

func TestConfig_ApplyDefaults_DoesNotOverrideExplicit(t *testing.T) {
	c := Config{MaxSpanAttributes: 64, SamplingRatio: 0.5}
	c.applyDefaults()
	if c.MaxSpanAttributes != 64 {
		t.Errorf("业务显式设的 MaxSpanAttributes 不应被默认覆盖, got %d", c.MaxSpanAttributes)
	}
	if c.SamplingRatio != 0.5 {
		t.Errorf("业务显式设的 SamplingRatio 不应被默认覆盖, got %v", c.SamplingRatio)
	}
}

func TestResource_Validate(t *testing.T) {
	tests := []struct {
		name    string
		r       Resource
		wantErr bool
		wantSub string
	}{
		{"all required ok", Resource{AppName: "a", EnvName: "p", IP: "1.1.1.1", SDKVersion: "v1"}, false, ""},
		{"app with dash ok", Resource{AppName: "multi-word-app", EnvName: "p", IP: "1.1.1.1", SDKVersion: "v1"}, false, ""},
		{"app with underscore ok", Resource{AppName: "my_app_1", EnvName: "p", IP: "1.1.1.1", SDKVersion: "v1"}, false, ""},
		{"missing AppName", Resource{EnvName: "p", IP: "1.1.1.1", SDKVersion: "v1"}, true, "AppName"},
		{"missing EnvName", Resource{AppName: "a", IP: "1.1.1.1", SDKVersion: "v1"}, true, "EnvName"},
		{"missing IP", Resource{AppName: "a", EnvName: "p", SDKVersion: "v1"}, true, "IP"},
		{"missing SDKVersion", Resource{AppName: "a", EnvName: "p", IP: "1.1.1.1"}, true, "SDKVersion"},
		// review P0 audit HIGH-2 fix: AppName 字符集 fail-fast
		{"AppName 含中文", Resource{AppName: "中文应用", EnvName: "p", IP: "1.1.1.1", SDKVersion: "v1"}, true, "invalid"},
		{"AppName 含空格", Resource{AppName: "my app", EnvName: "p", IP: "1.1.1.1", SDKVersion: "v1"}, true, "invalid"},
		{"AppName 含 ^^", Resource{AppName: "app^^extra", EnvName: "p", IP: "1.1.1.1", SDKVersion: "v1"}, true, "invalid"},
		{"AppName 含 /", Resource{AppName: "app/v1", EnvName: "p", IP: "1.1.1.1", SDKVersion: "v1"}, true, "invalid"},
		{"AppName 含 |", Resource{AppName: "app|x", EnvName: "p", IP: "1.1.1.1", SDKVersion: "v1"}, true, "invalid"},
		// review P0 audit MED-3 fix: AppName 长度上限
		{"AppName 超长 > 100", Resource{AppName: strings.Repeat("a", 101), EnvName: "p", IP: "1.1.1.1", SDKVersion: "v1"}, true, "too long"},
		{"AppName 边界 100", Resource{AppName: strings.Repeat("a", 100), EnvName: "p", IP: "1.1.1.1", SDKVersion: "v1"}, false, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.r.validate()
			if tc.wantErr && err == nil {
				t.Errorf("应返 err, 实际 nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("不应返 err, 实际 %v", err)
			}
			if tc.wantSub != "" && err != nil && !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("err 应含 %q, 实际 %q", tc.wantSub, err.Error())
			}
		})
	}
}

// TestConfig_Validate_SamplingRatioNegative 验 review P0 audit HIGH-3 fix —
// SamplingRatio < 0 fail-fast 不静默改 1.0.
func TestConfig_Validate_SamplingRatioNegative(t *testing.T) {
	c := Config{
		Resource:      Resource{AppName: "a", EnvName: "p", IP: "1.1.1.1", SDKVersion: "v1"},
		SamplingRatio: -0.5,
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("SamplingRatio=-0.5 应 fail-fast (业务 typo 反预期), 实际 nil")
	}
	if !strings.Contains(err.Error(), "SamplingRatio") {
		t.Errorf("err 应含 'SamplingRatio', got %q", err.Error())
	}
}

func TestConfig_Validate_SamplingRatioZero(t *testing.T) {
	// == 0 仍走默认 1.0 (跟 < 0 fail-fast 区分)
	c := Config{Resource: Resource{AppName: "a", EnvName: "p", IP: "1.1.1.1", SDKVersion: "v1"}, SamplingRatio: 0}
	if err := c.Validate(); err != nil {
		t.Errorf("SamplingRatio=0 应 OK (走默认 1.0), got err = %v", err)
	}
	c.applyDefaults()
	if c.SamplingRatio != 1.0 {
		t.Errorf("SamplingRatio=0 applyDefaults 后应是 1.0, got %v", c.SamplingRatio)
	}
}

// TestGenTraceID_ConcurrentNoRace 验 crypto/rand + GenTraceID 并发安全.
// CLAUDE.md 规约: 改 sess/channel/map/sync 相关代码必含 -race 验证.
// 跑: go test -race ./observability/
func TestGenTraceID_ConcurrentNoRace(t *testing.T) {
	const N = 1000
	var wg sync.WaitGroup
	wg.Add(N)
	ids := make([]string, N)
	for i := 0; i < N; i++ {
		go func(idx int) {
			defer wg.Done()
			ids[idx] = GenTraceID("my-app")
		}(i)
	}
	wg.Wait()

	// 全格式合法 + 全唯一
	seen := make(map[string]bool, N)
	for _, id := range ids {
		if !traceIDRe.MatchString(id) {
			t.Errorf("并发 GenTraceID 产出格式异常: %q", id)
		}
		if seen[id] {
			t.Errorf("并发 GenTraceID 撞: %q (crypto/rand 异常?)", id)
		}
		seen[id] = true
	}
}

// TestGenRPCChildSpanID_InvalidServiceDegrades 验 review P0 audit HIGH-1 fix —
// parentService 含特殊字符时退化本进程 sync child 不带 service 前缀.
func TestGenRPCChildSpanID_InvalidServiceDegrades(t *testing.T) {
	parent := "1.2|abc0000000000000"
	for _, badSvc := range []string{"", "中文service", "svc with space", "svc:other", "svc|x"} {
		got := GenRPCChildSpanID(parent, badSvc, 1)
		if !spanIDRe.MatchString(got) {
			t.Errorf("退化 spanID 应仍合法 spec 正则, badSvc=%q got=%q", badSvc, got)
		}
		// 退化产出不应含非法 service 前缀
		if strings.Contains(got, badSvc) && badSvc != "" {
			t.Errorf("退化 spanID 不应含非法 service %q, got %q", badSvc, got)
		}
	}
}

// TestGenChildSpanID_MultiHopRPC 验 multi-hop RPC parent (含多 `:`) catalogue
// 抽取正确. spec line 207 `rpc child: {parent_service}:{parent_catalogue}.{idx}|{hex}`,
// 业务跨多服务时 parent 形如 "svc1:1.2|hex" 或更深.
func TestGenChildSpanID_MultiHopRPC(t *testing.T) {
	parent := "order-service:1.2|abc0000000000000"
	got := GenChildSpanID(parent, 3, true)
	if !spanIDRe.MatchString(got) {
		t.Errorf("multi-hop child 格式错: %q", got)
	}
	if !strings.HasPrefix(got, "order-service:1.2.3|") {
		t.Errorf("multi-hop child 应保留 service+catalogue 全前缀, got = %q", got)
	}
}

// helper

func parseInt64(s string) (int64, error) {
	var v int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, &strconv2Err{s}
		}
		v = v*10 + int64(c-'0')
	}
	return v, nil
}

type strconv2Err struct{ s string }

func (e *strconv2Err) Error() string { return "invalid int: " + e.s }

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
