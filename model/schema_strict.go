package model

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// 结构化输出 schema 规范化器（纯函数，无状态外泄）。业务用手写 map[string]any 表达
// output schema（零依赖、不引 jsonschema 反射库），大概率不合 provider 的 strict 约束
// （缺 additionalProperties:false、required 不全、带 minLength 等）。本文件把普通 schema
// 改成 provider 兼容形态，否则 strict 路径必 400。见 doc/v2/structured-output.md §4.3。

// SchemaProfile 是 schema 规范化档位（不同 provider 对 strict 要求不同）。
type SchemaProfile int

const (
	// ProfileStrict 是 OpenAI/xAI strict json_schema 档：每个 object 强制
	// additionalProperties:false + 全 property 进 required；strip strict 不支持的校验
	// 关键词（minLength/pattern/format… 转写进 description，不无声丢）；oneOf→anyOf；
	// strip title/$schema/discriminator/default；内联 $defs/$ref；缺 type 的 object 补
	// type:object。根必 object，否则 strictCompatible=false。
	ProfileStrict SchemaProfile = iota
	// ProfileInlineDefs 是 qwen/deepseek 弱档：只内联 $defs/$ref，保留 required 原样、
	// 保留全部校验关键词（provider 尽力而为，无 strict 硬保证）。strictCompatible 恒 true。
	ProfileInlineDefs
)

// strictUnsupportedKeys 是 OpenAI strict json_schema 不支持的校验关键词。ProfileStrict
// 下 strip 掉并把语义转写进 description（模型仍看得到约束提示，不无声丢语义）。
var strictUnsupportedKeys = map[string]bool{
	"minLength": true, "maxLength": true, "pattern": true, "format": true,
	"minimum": true, "maximum": true, "exclusiveMinimum": true, "exclusiveMaximum": true,
	"multipleOf": true, "minItems": true, "maxItems": true, "uniqueItems": true,
	"minProperties": true, "maxProperties": true, "minContains": true, "maxContains": true,
}

// JSON Schema 结构感知的 key 分类——决定递归时把 value 当"子 schema"还是"原始数据"。
// 盲目递归会把 enum/const 里的对象当 schema 规范化（污染业务数据），必须按语义走。
var (
	// schemaMapKeys：value 是 name→子 schema 的 map（properties 等）。
	schemaMapKeys = map[string]bool{
		"properties": true, "patternProperties": true, "dependentSchemas": true,
	}
	// schemaListKeys：value 是 []子 schema（allOf 等）。
	schemaListKeys = map[string]bool{
		"allOf": true, "anyOf": true, "oneOf": true, "prefixItems": true,
	}
	// schemaOrListKeys：value 是单个子 schema / []子 schema / bool（items 等）。
	schemaOrListKeys = map[string]bool{
		"items": true, "additionalItems": true, "contains": true, "not": true,
		"if": true, "then": true, "else": true, "propertyNames": true,
		"additionalProperties": true, "unevaluatedProperties": true, "unevaluatedItems": true,
	}
)

// NormalizeJSONSchema 按 profile 规范化业务给的 JSONSchema（纯函数，深拷贝、不改原 map）。
//
// 返回规范化 schema + strictCompatible。strictCompatible 仅 ProfileStrict 有意义：报告
// 这份 schema 能否满足 provider 的 strict 硬约束（根非 object、含无法解析或循环的 $ref
// → false）。agent 层据此 fail-fast 或降级（见 doc/v2/structured-output.md §4.7）。
// ProfileInlineDefs 恒返 true（弱档本不保证 strict）。
func NormalizeJSONSchema(s map[string]any, profile SchemaProfile) (map[string]any, bool) {
	if s == nil {
		return nil, false
	}
	defs := map[string]map[string]any{}
	collectDefs(s, "$defs", defs)
	collectDefs(s, "definitions", defs)
	n := &schemaNormalizer{profile: profile, defs: defs, compatible: true}
	out := n.node(s, map[string]bool{})
	if profile == ProfileInlineDefs {
		return out, true
	}
	if !hasObjectShape(out) {
		n.compatible = false
	}
	return out, n.compatible
}

type schemaNormalizer struct {
	profile    SchemaProfile
	defs       map[string]map[string]any
	compatible bool
}

// node 深拷贝 + 规范化一个 schema 节点。visiting 记录当前 $ref 解析栈（环检测）。
func (n *schemaNormalizer) node(s map[string]any, visiting map[string]bool) map[string]any {
	if ref, ok := s["$ref"].(string); ok {
		return n.inlineRef(s, ref, visiting)
	}

	out := map[string]any{}
	var strippedNotes []string

	for k, v := range s {
		// $defs/definitions 已提取供内联，输出不保留（嵌套 $defs P0 不处理）。
		if k == "$defs" || k == "definitions" {
			continue
		}
		if n.profile == ProfileStrict {
			if k == "title" || k == "$schema" || k == "discriminator" || k == "default" {
				continue // strict 不支持 / 不需要
			}
			if strictUnsupportedKeys[k] {
				strippedNotes = append(strippedNotes, fmt.Sprintf("%s=%v", k, v))
				continue
			}
			if k == "oneOf" {
				out["anyOf"] = n.schemaList(v, visiting) // strict 无 oneOf，改 anyOf
				continue
			}
		}
		switch {
		case schemaMapKeys[k]:
			out[k] = n.schemaMap(v, visiting)
		case schemaListKeys[k]:
			out[k] = n.schemaList(v, visiting)
		case schemaOrListKeys[k]:
			out[k] = n.schemaOrList(v, visiting)
		default:
			out[k] = deepCopyValue(v) // 原始数据：type/enum/required/const/description…
		}
	}

	if n.profile == ProfileStrict {
		n.applyStrictObject(out, strippedNotes)
	}
	return out
}

// applyStrictObject 对含 properties 的 object 节点施加 strict 约束：补 type:object、
// additionalProperties:false、全 property 进 required；并把 strip 的约束写进 description。
func (n *schemaNormalizer) applyStrictObject(out map[string]any, strippedNotes []string) {
	if len(strippedNotes) > 0 {
		sort.Strings(strippedNotes)
		note := "constraints: " + strings.Join(strippedNotes, "; ")
		if d, ok := out["description"].(string); ok && d != "" {
			out["description"] = d + " (" + note + ")"
		} else {
			out["description"] = note
		}
	}
	props, ok := out["properties"].(map[string]any)
	if !ok {
		return
	}
	if _, hasType := out["type"]; !hasType {
		out["type"] = "object"
	}
	out["additionalProperties"] = false
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys) // 稳定序，便于测试断言 + wire 缓存
	req := make([]any, len(keys))
	for i, k := range keys {
		req[i] = k
	}
	out["required"] = req
}

// inlineRef 把 $ref 替换为其 $defs 目标的规范化副本（环 / 未解析 → strict 不兼容）。
func (n *schemaNormalizer) inlineRef(s map[string]any, ref string, visiting map[string]bool) map[string]any {
	name, ok := refName(ref)
	if !ok {
		n.compatible = false
		return copyMap(s)
	}
	target, found := n.defs[name]
	if !found {
		n.compatible = false
		return copyMap(s)
	}
	if visiting[name] {
		n.compatible = false // 循环 $ref：strict 无法内联
		return copyMap(s)
	}
	visiting[name] = true
	resolved := n.node(target, visiting)
	delete(visiting, name)
	// $ref 兄弟键（description 等）合并进解析结果；$ref 自身丢弃。ProfileStrict 下兄弟键
	// 同样须走 node() 的 per-key strict 过滤（strip minLength/pattern、oneOf→anyOf、strip
	// title/default）——否则未过滤的兄弟键（如 {"$ref":..,"minLength":10}）泄漏进 strict
	// 输出、且假报 strictCompatible=true，strict provider 收到会 400。把兄弟键收成子 schema
	// 走 node()：ProfileInlineDefs 下 node 不 strip 任何键，等价原来的深拷贝，无行为变化。
	sibling := map[string]any{}
	for k, v := range s {
		if k == "$ref" {
			continue
		}
		sibling[k] = v
	}
	if len(sibling) > 0 {
		for k, v := range n.node(sibling, visiting) {
			resolved[k] = v
		}
	}
	return resolved
}

// schemaMap 规范化 name→子 schema 的 map（properties 等）。
func (n *schemaNormalizer) schemaMap(v any, visiting map[string]bool) any {
	m, ok := v.(map[string]any)
	if !ok {
		return deepCopyValue(v)
	}
	out := make(map[string]any, len(m))
	for name, sub := range m {
		if subMap, ok := sub.(map[string]any); ok {
			out[name] = n.node(subMap, visiting)
		} else {
			out[name] = deepCopyValue(sub)
		}
	}
	return out
}

// schemaList 规范化 []子 schema（allOf/anyOf/oneOf/prefixItems）。
func (n *schemaNormalizer) schemaList(v any, visiting map[string]bool) any {
	list, ok := v.([]any)
	if !ok {
		return deepCopyValue(v)
	}
	out := make([]any, len(list))
	for i, e := range list {
		if m, ok := e.(map[string]any); ok {
			out[i] = n.node(m, visiting)
		} else {
			out[i] = deepCopyValue(e)
		}
	}
	return out
}

// schemaOrList 规范化 items/additionalProperties 等（单 schema / []schema / bool）。
func (n *schemaNormalizer) schemaOrList(v any, visiting map[string]bool) any {
	switch t := v.(type) {
	case map[string]any:
		return n.node(t, visiting)
	case []any:
		return n.schemaList(v, visiting)
	default:
		return v // bool（additionalProperties:false）或标量
	}
}

// collectDefs 提取 root[key]（$defs / definitions）里的顶层定义供 $ref 内联。
func collectDefs(root map[string]any, key string, out map[string]map[string]any) {
	raw, ok := root[key].(map[string]any)
	if !ok {
		return
	}
	for name, v := range raw {
		if m, ok := v.(map[string]any); ok {
			out[name] = m
		}
	}
}

// refName 解析 "#/$defs/Name" / "#/definitions/Name" → "Name"。其他形式返 false。
func refName(ref string) (string, bool) {
	for _, p := range []string{"#/$defs/", "#/definitions/"} {
		if strings.HasPrefix(ref, p) {
			name := ref[len(p):]
			if name != "" && !strings.Contains(name, "/") {
				return name, true
			}
		}
	}
	return "", false
}

// hasObjectShape 判定根 schema 是否是 object（strict 要求根必 object）。
func hasObjectShape(s map[string]any) bool {
	if t, ok := s["type"].(string); ok {
		return t == "object"
	}
	_, ok := s["properties"]
	return ok
}

// deepCopyValue 深拷贝任意 JSON 值（避免规范化结果别名输入 map）。
func deepCopyValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return copyMap(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = deepCopyValue(e)
		}
		return out
	default:
		// typed slice（业务手写 []string{"name"} 作 required、[]int 作 enum 等 Go 原生
		// 字面量）用 reflect 拷一层，避免规范化结果与输入共享底层数组——否则改输出会污染
		// 业务原 map，违反 godoc「纯函数、深拷贝、不改原 map」契约。标量（string/int/bool/
		// nil）Kind != Slice 原样返（不可变，别名无害）。nil typed slice 保留 nil 不漂成空片。
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice && !rv.IsNil() {
			cp := reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
			reflect.Copy(cp, rv)
			return cp.Interface()
		}
		return v
	}
}

// copyMap 深拷贝一个 map[string]any。
func copyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = deepCopyValue(v)
	}
	return out
}
