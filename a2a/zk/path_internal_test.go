package zk

// 白盒单测路径 / 编码纯逻辑——无需真 ZK。§4.1 编码示例是跨语言互通的硬约束。

import (
	"strings"
	"testing"
)

func TestEncodeInstanceID(t *testing.T) {
	// §4.1 示例：instanceId "10.0.0.12:8080" → "MTAuMC4wLjEyOjgwODA"。
	// 跨语言（Java Base64.getUrlEncoder().withoutPadding() / Python urlsafe_b64encode().rstrip("=")）
	// 必须算出同一结果，否则 Go 写的节点 Java 找不到。
	if got := encodeInstanceID("10.0.0.12:8080"); got != "MTAuMC4wLjEyOjgwODA" {
		t.Fatalf("encodeInstanceID(\"10.0.0.12:8080\") = %q, want MTAuMC4wLjEyOjgwODA (§4.1 示例)", got)
	}
	// no-padding：不得出现 '='。
	for _, id := range []string{"10.0.0.12:8080", "host/path", "[::1]:8080", "a"} {
		if enc := encodeInstanceID(id); strings.Contains(enc, "=") {
			t.Errorf("encodeInstanceID(%q)=%q 含 padding（应 RawURLEncoding 无 padding）", id, enc)
		}
	}
}

func TestValidateSegment(t *testing.T) {
	for _, ok := range []string{"prod", "research-agent", "a.b_c-1", "default"} {
		if err := validateSegment("ns", ok); err != nil {
			t.Errorf("%q 应合法: %v", ok, err)
		}
	}
	for _, bad := range []string{"a/b", "a b", "a:b", "", "a@b", "a#b"} {
		if err := validateSegment("ns", bad); err == nil {
			t.Errorf("%q 应非法（§4.1 只允许字母数字 . _ -）", bad)
		}
	}
}

func TestPaths(t *testing.T) {
	if got := servicePath(DefaultRootPath, "prod", "research-agent"); got != "/crew/a2a/prod/research-agent" {
		t.Fatalf("servicePath = %q", got)
	}
	// agentName 中间层（§ZK 节点模型 4 层）
	if got := agentPath(DefaultRootPath, "prod", "commerce-service", "research"); got != "/crew/a2a/prod/commerce-service/research" {
		t.Fatalf("agentPath = %q", got)
	}
	// §ZK 节点模型 4 层完整路径：service/{agentName}/{encodedInstanceId}（§4.1 编码规则不变）
	if got := instancePath(DefaultRootPath, "prod", "commerce-service", "research", "10.0.0.12:8080"); got != "/crew/a2a/prod/commerce-service/research/MTAuMC4wLjEyOjgwODA" {
		t.Fatalf("instancePath = %q（应匹配 4 层路径 + §4.1 编码示例）", got)
	}
	anc := ancestorPaths(DefaultRootPath, "prod", "svc", "agent")
	want := []string{"/crew", "/crew/a2a", "/crew/a2a/prod", "/crew/a2a/prod/svc", "/crew/a2a/prod/svc/agent"}
	if len(anc) != len(want) {
		t.Fatalf("ancestorPaths = %v, want %v", anc, want)
	}
	for i := range want {
		if anc[i] != want[i] {
			t.Errorf("ancestorPaths[%d] = %q, want %q", i, anc[i], want[i])
		}
	}
}
