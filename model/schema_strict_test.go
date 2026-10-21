package model_test

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"crew-go/model"
)

// TestNormalize_StrictObject 验 strict 档给 object 补 additionalProperties:false +
// 全 property 进 required（排序稳定）+ 缺 type 补 object。
func TestNormalize_StrictObject(t *testing.T) {
	in := map[string]any{
		"properties": map[string]any{ // 故意不写 type
			"b": map[string]any{"type": "number"},
			"a": map[string]any{"type": "string"},
		},
	}
	out, compat := model.NormalizeJSONSchema(in, model.ProfileStrict)
	if !compat {
		t.Fatal("strictCompatible = false; want true")
	}
	if out["type"] != "object" {
		t.Errorf("缺 type 应补 object, got %v", out["type"])
	}
	if out["additionalProperties"] != false {
		t.Errorf("additionalProperties = %v; want false", out["additionalProperties"])
	}
	req := toStrings(t, out["required"])
	sort.Strings(req)
	if !reflect.DeepEqual(req, []string{"a", "b"}) {
		t.Errorf("required = %v; want [a b]", req)
	}
}

// TestNormalize_StrictStripsKeywords 验 strict 不支持的校验关键词被 strip 且转写进
// description（不无声丢），保留的 type 不动。
func TestNormalize_StrictStripsKeywords(t *testing.T) {
	in := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{
				"type":        "string",
				"minLength":   float64(3),
				"pattern":     "^[a-z]+$",
				"format":      "email",
				"description": "用户名",
			},
		},
	}
	out, _ := model.NormalizeJSONSchema(in, model.ProfileStrict)
	name := asMap(t, asMap(t, out["properties"])["name"])
	for _, k := range []string{"minLength", "pattern", "format"} {
		if _, has := name[k]; has {
			t.Errorf("strict 应 strip %q, 仍在: %v", k, name)
		}
	}
	if name["type"] != "string" {
		t.Errorf("type 不应被动, got %v", name["type"])
	}
	desc, _ := name["description"].(string)
	for _, want := range []string{"用户名", "minLength", "pattern", "format"} {
		if !strings.Contains(desc, want) {
			t.Errorf("description 应含 %q（strip 转写不无声丢）, got %q", want, desc)
		}
	}
}

// TestNormalize_StrictOneOfToAnyOf 验 strict 把 oneOf 改 anyOf、strip title/$schema/default。
func TestNormalize_StrictOneOfToAnyOf(t *testing.T) {
	in := map[string]any{
		"type":    "object",
		"title":   "Wrapper",
		"$schema": "http://json-schema.org/draft-07/schema#",
		"default": map[string]any{"x": 1},
		"properties": map[string]any{
			"choice": map[string]any{
				"oneOf": []any{
					map[string]any{"type": "string"},
					map[string]any{"type": "number"},
				},
			},
		},
	}
	out, _ := model.NormalizeJSONSchema(in, model.ProfileStrict)
	for _, k := range []string{"title", "$schema", "default"} {
		if _, has := out[k]; has {
			t.Errorf("strict 应 strip 顶层 %q", k)
		}
	}
	choice := asMap(t, asMap(t, out["properties"])["choice"])
	if _, has := choice["oneOf"]; has {
		t.Error("oneOf 应改名 anyOf")
	}
	if _, has := choice["anyOf"]; !has {
		t.Error("缺 anyOf")
	}
}

// TestNormalize_StrictRootNonObject 验根非 object（array / 裸 anyOf）→ strictCompatible=false。
func TestNormalize_StrictRootNonObject(t *testing.T) {
	for _, in := range []map[string]any{
		{"type": "array", "items": map[string]any{"type": "string"}},
		{"anyOf": []any{map[string]any{"type": "object"}}},
	} {
		if _, compat := model.NormalizeJSONSchema(in, model.ProfileStrict); compat {
			t.Errorf("根非 object 应 strictCompatible=false: %v", in)
		}
	}
}

// TestNormalize_StrictNestedObject 验嵌套 object 也施加 strict（additionalProperties + required）。
func TestNormalize_StrictNestedObject(t *testing.T) {
	in := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"address": map[string]any{
				"type":       "object",
				"properties": map[string]any{"city": map[string]any{"type": "string"}},
			},
		},
	}
	out, _ := model.NormalizeJSONSchema(in, model.ProfileStrict)
	addr := asMap(t, asMap(t, out["properties"])["address"])
	if addr["additionalProperties"] != false {
		t.Errorf("嵌套 object 缺 additionalProperties:false, got %v", addr["additionalProperties"])
	}
	if req := toStrings(t, addr["required"]); !reflect.DeepEqual(req, []string{"city"}) {
		t.Errorf("嵌套 required = %v; want [city]", req)
	}
}

// TestNormalize_PreservesEnumConstData 是最关键的陷阱测：enum / const 里的对象是"数据"
// 不是"子 schema"，绝不能被规范化——盲目递归会污染业务数据。故意让这些对象带 schema
// 关键词键（minLength/title/pattern/default/properties），若被当 schema 处理会被 strip /
// 加 additionalProperties → DeepEqual 失败；正确的结构感知遍历原样保留（sanity-revert 已验）。
func TestNormalize_PreservesEnumConstData(t *testing.T) {
	enumObj := map[string]any{
		"op": "eq", "minLength": float64(5), "title": "keep-me",
		"properties": map[string]any{"x": float64(1)},
	}
	constObj := map[string]any{"pattern": "^x$", "default": "raw", "nested": map[string]any{"deep": true}}
	in := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"filter": map[string]any{"enum": []any{enumObj, "raw-string"}},
			"tag":    map[string]any{"const": constObj},
		},
	}
	out, _ := model.NormalizeJSONSchema(in, model.ProfileStrict)
	props := asMap(t, out["properties"])
	gotEnum := asMap(t, props["filter"])["enum"].([]any)
	if !reflect.DeepEqual(gotEnum[0], enumObj) {
		t.Errorf("enum 对象被污染: got %#v; want %#v", gotEnum[0], enumObj)
	}
	if gotEnum[1] != "raw-string" {
		t.Errorf("enum 标量被动: %v", gotEnum[1])
	}
	if got := asMap(t, props["tag"])["const"]; !reflect.DeepEqual(got, constObj) {
		t.Errorf("const 对象被污染: got %#v; want %#v", got, constObj)
	}
}

// TestNormalize_InlineRef 验 $ref 内联进 $defs 目标副本、$defs 从输出移除、兄弟 description 保留。
func TestNormalize_InlineRef(t *testing.T) {
	in := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"child": map[string]any{"$ref": "#/$defs/Node", "description": "子节点"},
		},
		"$defs": map[string]any{
			"Node": map[string]any{
				"type":       "object",
				"properties": map[string]any{"v": map[string]any{"type": "string"}},
			},
		},
	}
	out, compat := model.NormalizeJSONSchema(in, model.ProfileStrict)
	if !compat {
		t.Fatal("可解析 $ref 不应判 incompatible")
	}
	if _, has := out["$defs"]; has {
		t.Error("$defs 内联后应从输出移除")
	}
	child := asMap(t, asMap(t, out["properties"])["child"])
	if child["type"] != "object" {
		t.Errorf("$ref 未内联成 object, got %v", child["type"])
	}
	if child["additionalProperties"] != false {
		t.Error("内联的 object 未施加 strict")
	}
	if child["description"] != "子节点" {
		t.Errorf("$ref 兄弟 description 丢失, got %v", child["description"])
	}
}

// TestNormalize_CyclicRef 验循环 $ref → strict 判 incompatible（无法内联）。
func TestNormalize_CyclicRef(t *testing.T) {
	in := map[string]any{
		"type":       "object",
		"properties": map[string]any{"self": map[string]any{"$ref": "#/$defs/Loop"}},
		"$defs": map[string]any{
			"Loop": map[string]any{
				"type":       "object",
				"properties": map[string]any{"next": map[string]any{"$ref": "#/$defs/Loop"}},
			},
		},
	}
	if _, compat := model.NormalizeJSONSchema(in, model.ProfileStrict); compat {
		t.Error("循环 $ref 应 strictCompatible=false")
	}
}

// TestNormalize_InlineDefsProfile 验弱档：内联 $ref 但不强制 required、不 strip 关键词、恒 true。
func TestNormalize_InlineDefsProfile(t *testing.T) {
	in := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string", "minLength": float64(2)}, // 弱档保留
			"opt":  map[string]any{"type": "number"},
		},
		"required": []any{"name"}, // 弱档保留原样（不强制全 required）
	}
	out, compat := model.NormalizeJSONSchema(in, model.ProfileInlineDefs)
	if !compat {
		t.Error("ProfileInlineDefs 恒 true")
	}
	name := asMap(t, asMap(t, out["properties"])["name"])
	if name["minLength"] == nil {
		t.Error("弱档不应 strip minLength")
	}
	if req := toStrings(t, out["required"]); !reflect.DeepEqual(req, []string{"name"}) {
		t.Errorf("弱档 required 应保留原样 [name], got %v", req)
	}
	if _, has := out["additionalProperties"]; has {
		t.Error("弱档不应强加 additionalProperties")
	}
}

// TestNormalize_NoMutation 验纯函数不改原 map（深拷贝）。
func TestNormalize_NoMutation(t *testing.T) {
	in := map[string]any{
		"type":       "object",
		"properties": map[string]any{"a": map[string]any{"type": "string", "minLength": float64(1)}},
	}
	_, _ = model.NormalizeJSONSchema(in, model.ProfileStrict)
	a := in["properties"].(map[string]any)["a"].(map[string]any)
	if a["minLength"] == nil {
		t.Error("原 map 被 strip（应深拷贝不改原）")
	}
	if _, has := in["additionalProperties"]; has {
		t.Error("原 map 被加 additionalProperties（应深拷贝不改原）")
	}
	if _, has := in["required"]; has {
		t.Error("原 map 被加 required（应深拷贝不改原）")
	}
}

// TestNormalize_Nil 验 nil 输入 → (nil, false)。
func TestNormalize_Nil(t *testing.T) {
	out, compat := model.NormalizeJSONSchema(nil, model.ProfileStrict)
	if out != nil || compat {
		t.Errorf("nil 输入应返 (nil, false), got (%v, %v)", out, compat)
	}
}

// TestNormalize_StrictRefSiblingFiltered 验 $ref 兄弟键在 ProfileStrict 下同样走 strict
// 过滤：minLength 兄弟键须被 strip（转写进 description），不能泄漏进 strict 输出。
// 回归——修前 inlineRef 兄弟键用裸 deepCopyValue 不过滤，minLength 泄漏且假报 compatible=true。
func TestNormalize_StrictRefSiblingFiltered(t *testing.T) {
	in := map[string]any{
		"type": "object",
		"properties": map[string]any{
			// $ref + minLength 兄弟键：strict 无 minLength，须 strip
			"child": map[string]any{"$ref": "#/$defs/Node", "minLength": float64(10)},
		},
		"$defs": map[string]any{
			"Node": map[string]any{
				"type":       "object",
				"properties": map[string]any{"v": map[string]any{"type": "string"}},
			},
		},
	}
	out, _ := model.NormalizeJSONSchema(in, model.ProfileStrict)
	child := asMap(t, asMap(t, out["properties"])["child"])
	if child["minLength"] != nil {
		t.Errorf("$ref 兄弟键 minLength 应被 strict strip，却泄漏进输出: %v", child["minLength"])
	}
	// strip 不无声丢——语义须转写进 description
	if d, _ := child["description"].(string); !strings.Contains(d, "minLength") {
		t.Errorf("strip 的 minLength 未转写进 description: %q", d)
	}
}

// TestNormalize_NoMutationTypedSlice 验业务用 Go 原生 typed slice（[]string 作 enum）时
// 规范化不别名输入——改规范化输出不污染业务原 map。
// 回归——修前 deepCopyValue default 分支对 []string 裸 return v 别名，改输出即改原 map。
func TestNormalize_NoMutationTypedSlice(t *testing.T) {
	origEnum := []string{"admin", "user"} // Go 原生字面量，非 []any
	in := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"role": map[string]any{"type": "string", "enum": origEnum},
		},
	}
	out, _ := model.NormalizeJSONSchema(in, model.ProfileStrict)
	outEnum, ok := asMap(t, asMap(t, out["properties"])["role"])["enum"].([]string)
	if !ok {
		t.Fatalf("enum 应保持 []string 类型")
	}
	outEnum[0] = "MUTATED" // 改规范化输出
	if origEnum[0] != "admin" {
		t.Errorf("改输出污染了业务原 []string enum（typed slice 别名）: %v", origEnum)
	}
}

// --- helpers ---

func asMap(t *testing.T, v any) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("不是 map: %#v", v)
	}
	return m
}

func toStrings(t *testing.T, v any) []string {
	t.Helper()
	list, ok := v.([]any)
	if !ok {
		t.Fatalf("不是 []any: %#v", v)
	}
	out := make([]string, len(list))
	for i, e := range list {
		s, ok := e.(string)
		if !ok {
			t.Fatalf("元素非 string: %#v", e)
		}
		out[i] = s
	}
	return out
}
