package observability

import "encoding/json"

// MetricsMappingAttrKey 是 `metrics_mapping` span attribute 的 key (spec
// AI_METRICS_SPEC.md line 38-40 固定 key).
//
// 业务在监控控制台从 metric 图点击数据点反查 trace 时, framework 后端按此 key
// 找 span, 解析 JSON 找匹配 metric name + labels → 跳到对应 trace 详情.
const MetricsMappingAttrKey = "metrics_mapping"

// metricsMappingPayload 是 `metrics_mapping` 的 JSON 结构.
//
// 协议 (spec AI_METRICS_SPEC.md line 44-53):
//
//	{
//	  "metricsMapping": {
//	    "gen_ai_agent_runs": {
//	      "gen_ai_agent_name": "chat_entry",
//	      "gen_ai_status": "success"
//	    }
//	  }
//	}
//
// **value 内字段必须跟实际 metric labels 完全一致** (spec line 59-60), 否则监控
// 后端反查不到对应 metric 点.
type metricsMappingPayload struct {
	MetricsMapping map[string]map[string]string `json:"metricsMapping"`
}

// buildMetricsMapping 把 `metric name → labels map` 序列化为 spec 要求的 JSON
// 字符串 (业务 caller 加到 span extraAttrs).
//
// **空 map / nil 返空 string**: caller 无 metric 关联时调用方应跳过加 attribute.
//
// **数量上限** (spec line 61): 一个 span 最多关联 5 个 metric (默认
// `monitor.spanMetricsMappingMaxNum=5`). 超量 caller 应自选最关键 metric.
func buildMetricsMapping(mapping map[string]map[string]string) string {
	if len(mapping) == 0 {
		return ""
	}
	payload := metricsMappingPayload{MetricsMapping: mapping}
	b, err := json.Marshal(payload)
	if err != nil {
		// json.Marshal 对 map[string]map[string]string 不会 err — defensive 兜底
		return ""
	}
	return string(b)
}
