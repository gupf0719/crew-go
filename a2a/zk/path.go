package zk

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
)

// DefaultRootPath 是协议规定的 ZK 根路径（§4.1）。生产必须用它；测试可经 WithRootPath 隔离。
const DefaultRootPath = "/crew/a2a"

// segmentRe 是 §4.1 对 namespace / serviceName 的路径字符约束（字母数字 . _ -）。
var segmentRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// validateSegment 校验 namespace / serviceName 满足 §4.1 路径字符约束。
func validateSegment(kind, seg string) error {
	if !segmentRe.MatchString(seg) {
		return fmt.Errorf("zk: %s %q must match [A-Za-z0-9._-]+ (§4.1 path char constraint)", kind, seg)
	}
	return nil
}

// encodeInstanceID 按 §4.1 用 Base64 URL-safe no-padding 编码 instanceId（不同语言必须等价：
// Java Base64.getUrlEncoder().withoutPadding() / Python base64.urlsafe_b64encode().rstrip(b"=")）。
func encodeInstanceID(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}

// servicePath 返回服务路径 {root}/{namespace}/{serviceName}。
func servicePath(root, namespace, serviceName string) string {
	return root + "/" + namespace + "/" + serviceName
}

// agentPath 返回 agent 中间层路径 {root}/{namespace}/{serviceName}/{agentName}（§ZK 节点模型）
// ——同一 serviceName 下多个 Agent Card 各占一个 agentName 子树（持久父节点，instance 叶子挂其下）。
func agentPath(root, namespace, serviceName, agentName string) string {
	return servicePath(root, namespace, serviceName) + "/" + agentName
}

// instancePath 返回实例节点路径 {root}/{namespace}/{serviceName}/{agentName}/{encodedInstanceId}
// （§ZK 节点模型，4 层）。
func instancePath(root, namespace, serviceName, agentName, instanceID string) string {
	return agentPath(root, namespace, serviceName, agentName) + "/" + encodeInstanceID(instanceID)
}

// ancestorPaths 返回从根到 agent 路径的全部祖先（含自身），由浅到深——用于建持久父节点
// （含 agentName 中间层）。如 agentPath="/crew/a2a/prod/svc/agent" →
// ["/crew","/crew/a2a","/crew/a2a/prod","/crew/a2a/prod/svc","/crew/a2a/prod/svc/agent"]。
func ancestorPaths(root, namespace, serviceName, agentName string) []string {
	full := agentPath(root, namespace, serviceName, agentName)
	var out []string
	cur := ""
	for _, seg := range strings.Split(strings.Trim(full, "/"), "/") {
		if seg == "" {
			continue
		}
		cur += "/" + seg
		out = append(out, cur)
	}
	return out
}
