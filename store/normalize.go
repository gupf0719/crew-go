package store

import (
	"bytes"
	"encoding/json"
	"unicode/utf8"
)

// NormalizeValue 把 Put 进来的值换成它的 JSON 形状：编码成 JSON，再按 UseNumber 解回来——对象成
// `map[string]any`、数组成 `[]any`、数字成 `json.Number`，字符串、布尔、null 原样；其余按 encoding/json 的编码规则
// （`[]byte` 读回是 base64 字符串、`time.Time` 是 RFC 3339 字符串、实现了 json.Marshaler 的按它自己的输出）。
//
// 这就是 [Item].Value 的往返契约：后端存的、Get / Search 读回的都是这个形状，filter 两侧比的也是（[WithFilter]
// 把比较值同样换成 JSON 形状）。持久后端本来就经 JSON 落盘，读回自然是它；内存后端在 Put 时调它，跟它们一致。于是
// 业务传 struct、`map[string]string`、`*schema.Message`，在每个后端上读回来都是 `map[string]any`、按同一套 filter
// 规则命中（曾经内存后端原样存业务的值：`map[string]string{"z":"1"}` 配 filter `{"z":"1"}` 在内存后端上 0 条、在
// SQLite 上 1 条，两边 `err == nil`）。编不成 JSON 的值（chan、func、NaN、±Inf）返回错误——「Value 必须能编成
// JSON」在每个后端上都强制。
//
// 代价是一次编码加一次解码（字符串与布尔的往返是恒等，直接返回）；内存后端存的是这份新形状，不共享业务传进来的对象，
// 大结构体（比如 1MB 的工具结果消息）Put 一次多花毫秒级的时间、多占一份内存。
//
// 自研后端：落盘前调它（sql 方言把它的结果再编码一遍落盘：编码 map 时键已排序，整对象相等按 JSON 文本比的
// 方言因此不受 struct 字段声明序影响），或者经 JSON 落盘、读回时按 UseNumber 解码——两种做法读回的形状相同。
func NormalizeValue(v any) (any, error) {
	switch x := v.(type) {
	case string:
		// 合法 UTF-8 的字符串编码后再解回来是它自己；不合法的字节会被换成 U+FFFD，走下面的通用路径
		if utf8.ValidString(x) {
			return x, nil
		}
	case bool:
		return x, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}
