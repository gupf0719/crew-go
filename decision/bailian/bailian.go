// Package bailian 接入阿里云百炼的决策模型 decision-model-preview。百炼用的是 TypeSafe 的
// System One 协议，构造出的是 decision.Client，请求、答案与路由决策方都在 decision 包里：
//
//	c := bailian.NewClient(os.Getenv("DASHSCOPE_API_KEY"), bailian.Beijing, "llm-xxxx")
//	decider := decision.NewRouteDecider(c)
package bailian

import (
	"fmt"
	"strings"

	"crew-go/decision"
)

// Model 是百炼上决策模型的模型名，[NewClient] 默认用它。
const Model = "decision-model-preview"

// Region 是决策模型所在的地域，拼进业务空间地址的域名。
type Region string

const (
	// Beijing 是华北 2（北京）。
	Beijing Region = "cn-beijing"
	// Singapore 是新加坡。
	Singapore Region = "ap-southeast-1"
)

// NewClient 构造调用百炼决策模型的客户端：
//
//   - 请求发到业务空间自己的地址
//     https://{workspaceID}.{region}.maas.aliyuncs.com/compatible-mode/v1/systemone；
//   - 默认模型是 [Model]；
//   - apiKey 是百炼的 API key（DASHSCOPE_API_KEY）。
//
// workspaceID 是百炼控制台「业务空间管理」里的业务空间 ID，形如 llm-xxxx。region 与
// workspaceID 前后的空白会去掉；两者都要拼进域名，只能由字母、数字和连字符组成，否则
// panic：混进去的点、斜杠或 @ 会让请求连同 key 发到别的主机。key 为空或含控制字符时
// 同样 panic。
//
// opts 是 decision 包的选项。经别的地址访问（比如自己的反向代理）就用 decision.WithBaseURL
// 覆盖，地址写到 /compatible-mode 为止；错误里的服务方标识与请求 ID 的读取方式不随地址变。
func NewClient(apiKey string, region Region, workspaceID string, opts ...decision.Option) *decision.Client {
	r := strings.TrimSpace(string(region))
	ws := strings.TrimSpace(workspaceID)
	if !isHostLabel(r) {
		panic(fmt.Sprintf("bailian.NewClient: region %q must consist of letters, digits and hyphens", region))
	}
	if !isHostLabel(ws) {
		panic(fmt.Sprintf("bailian.NewClient: workspace ID %q must consist of letters, digits and hyphens", workspaceID))
	}
	return decision.NewClient(apiKey, decision.Preset{
		Provider:     "bailian",
		BaseURL:      "https://" + ws + "." + r + ".maas.aliyuncs.com/compatible-mode",
		DefaultModel: Model,
		// 成功响应与各种错误响应都带 x-request-id。x-dashscope-requestid 只在请求到了模型
		// 服务时才有，网关直接拒掉的（key 无效、模型不存在）没有。
		RequestIDHeader: "x-request-id",
	}, opts...)
}

// isHostLabel 报告 s 能不能原样作为域名里的一段：1 到 63 个 ASCII 字母、数字或连字符，
// 不以连字符开头或结尾。
func isHostLabel(s string) bool {
	if s == "" || len(s) > 63 || s[0] == '-' || s[len(s)-1] == '-' {
		return false
	}
	for i := 0; i < len(s); i++ {
		b := s[i]
		if !('a' <= b && b <= 'z' || 'A' <= b && b <= 'Z' || '0' <= b && b <= '9' || b == '-') {
			return false
		}
	}
	return true
}
