package agent_v2

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"crew-go/schema"
)

// TestCloneCalleeInput_FieldCoverage：消息结构里每一个引用类型的字段（指针 / 切片 / map / 接口）都要
// 在 cloneCalleeInput 里有个说法——拷，或者明确共享。schema 加了新字段而拷贝没跟上时，被调方的钩子
// 改那个字段就会顺着指针写回调用方、随调用方的存档落盘（这正是要消灭的那个漂移）。
func TestCloneCalleeInput_FieldCoverage(t *testing.T) {
	handled := map[string]string{
		"Message.ContentBlocks":             "逐块拷",
		"Message.Annotations":               "cloneAnyMap",
		"Message.Metadata":                  "结构体拷",
		"MessageMetadata.Usage":             "clonePtr",
		"MessageMetadata.Logprobs":          "切片拷、元素共享（模型输出的附属，钩子没有理由改）",
		"ContentBlock.Text":                 "clonePtr",
		"ContentBlock.ImageUrl":             "结构体拷",
		"ImageUrlContentBlock.ImageUrl":     "clonePtr",
		"ContentBlock.InputAudio":           "结构体拷",
		"InputAudioContentBlock.InputAudio": "clonePtr",
		"ContentBlock.File":                 "结构体拷",
		"FileContentBlock.File":             "clonePtr",
		"ContentBlock.Toolcall":             "clonePtr",
		"ContentBlock.Toolresult":           "clonePtr",
		"ContentBlock.Data":                 "结构体拷",
		"DataContentBlock.Data":             "cloneAny",
	}
	// 共享的子树不往下走
	shared := map[string]bool{"MessageMetadata.Logprobs": true}
	seen := map[reflect.Type]bool{}
	var walk func(t reflect.Type)
	walk = func(typ reflect.Type) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Struct || seen[typ] {
			return
		}
		seen[typ] = true
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			path := typ.Name() + "." + f.Name
			switch f.Type.Kind() {
			case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
				if _, ok := handled[path]; !ok {
					t.Errorf("%s（%s）在 cloneCalleeInput 里没有说法：拷还是共享？", path, f.Type)
					continue
				}
				if !shared[path] {
					walk(f.Type)
				}
			case reflect.Struct:
				walk(f.Type)
			}
		}
	}
	walk(reflect.TypeOf(schema.Message{}))
}

// TestCloneCalleeInput_EditsDoNotReachOriginal：把拷贝的每个可变部位都改一遍，原件逐字节不变。
func TestCloneCalleeInput_EditsDoNotReachOriginal(t *testing.T) {
	orig := schema.UserMessage("原文")
	orig.Annotations = map[string]any{
		"k":      "v",
		"nested": map[string]any{"deep": []any{"a", map[string]any{"x": 1.0}}},
	}
	orig.ContentBlocks = []*schema.ContentBlock{
		{Type: "text", Text: &schema.TextContentBlock{Type: schema.ContentBlockTypeText, Text: "块"}},
		{Type: "image_url", ImageUrl: &schema.ImageUrlContentBlock{Type: schema.ContentBlockTypeImageUrl,
			ImageUrl: &schema.ImageUrl{Url: "http://a"}}},
		{Type: "file", File: &schema.FileContentBlock{File: &schema.FileData{FileData: "AAAA", Filename: "f"}}},
		{Type: "input_audio", InputAudio: &schema.InputAudioContentBlock{InputAudio: &schema.InputAudio{Data: "au"}}},
		{Type: "toolcall", Toolcall: &schema.ToolcallContentBlock{Id: "c1", Name: "n", Arguments: "{}"}},
		{Type: "toolresult", Toolresult: &schema.ToolresultContentBlock{ToolCallId: "c1", Content: "r"}},
		{Type: "data", Data: &schema.DataContentBlock{Type: schema.ContentBlockTypeData,
			Data: map[string]any{"list": []any{"x"}}}},
	}
	orig.Metadata = &schema.MessageMetadata{Model: "m", Usage: &schema.MessageUsage{TotalTokens: 3}}
	before, err := json.Marshal(orig)
	if err != nil {
		t.Fatal(err)
	}

	c := cloneCalleeInput(orig)
	c.Content = "改了"
	c.Annotations["k"] = "改了"
	c.Annotations["新增"] = 1
	c.Annotations["nested"].(map[string]any)["deep"].([]any)[1].(map[string]any)["x"] = 2.0
	c.ContentBlocks[0].Text.Text = "改了"
	c.ContentBlocks[1].ImageUrl.ImageUrl.Url = "http://b"
	c.ContentBlocks[2].File.File.FileData = "BBBB"
	c.ContentBlocks[3].InputAudio.InputAudio.Data = "改了"
	c.ContentBlocks[4].Toolcall.Arguments = `{"x":1}`
	c.ContentBlocks[5].Toolresult.Content = "改了"
	c.ContentBlocks[6].Data.Data.(map[string]any)["list"].([]any)[0] = "y"
	c.ContentBlocks = append(c.ContentBlocks, &schema.ContentBlock{Type: "text"})
	c.Metadata.Model = "改了"
	c.Metadata.Usage.TotalTokens = 99

	after, err := json.Marshal(orig)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("改拷贝改到了原件：\n前 %s\n后 %s", before, after)
	}
	if cloneCalleeInput(nil) != nil {
		t.Error("nil 输入应拷成 nil")
	}
}

type cloneHandle struct{ conn int }

type cloneRef struct{ ID int64 }

type cloneLevel int

type clonePanicMarshaler struct{}

func (clonePanicMarshaler) MarshalJSON() ([]byte, error) { panic("业务 MarshalJSON 的 bug") }

// TestCloneAny_KeepsTypesAndPrecision：拷贝之后类型不变、内容不丢。曾经把业务类型按存档往返一遍，
// 实测首跑就丢东西：订单号 int64 丢精度、json.Number 成 float64、只有未导出字段的句柄与 error 成
// 空 map、[]byte 成 base64 字符串。
func TestCloneAny_KeepsTypesAndPrecision(t *testing.T) {
	h := &cloneHandle{conn: 7}
	boom := errors.New("boom")
	orig := map[string]any{
		"handle": h,
		"err":    boom,
		"num":    json.Number("1234567890123456789"),
		"ref":    cloneRef{ID: 1234567890123456789},
		"level":  cloneLevel(3),
		"raw":    []byte("raw"),
		"tags":   []string{"a", "b"},
		"counts": map[string]int{"x": 1},
	}
	c := cloneAnyMap(orig)
	if got, ok := c["handle"].(*cloneHandle); !ok || got != h {
		t.Errorf("进程内句柄应原样共享，got %T %v", c["handle"], c["handle"])
	}
	if c["err"] != boom {
		t.Errorf("error 应原样共享，got %T %v", c["err"], c["err"])
	}
	if got, ok := c["num"].(json.Number); !ok || got != "1234567890123456789" {
		t.Errorf("json.Number 应原样保留，got %T %v", c["num"], c["num"])
	}
	if got, ok := c["ref"].(cloneRef); !ok || got.ID != 1234567890123456789 {
		t.Errorf("结构体里的 int64 不能丢精度，got %T %v", c["ref"], c["ref"])
	}
	if got, ok := c["level"].(cloneLevel); !ok || got != 3 {
		t.Errorf("具名枚举应保留类型，got %T %v", c["level"], c["level"])
	}
	raw, ok := c["raw"].([]byte)
	if !ok || string(raw) != "raw" {
		t.Fatalf("[]byte 应保留类型，got %T %v", c["raw"], c["raw"])
	}
	tags, ok := c["tags"].([]string)
	if !ok || len(tags) != 2 {
		t.Fatalf("[]string 应保留类型，got %T %v", c["tags"], c["tags"])
	}
	counts, ok := c["counts"].(map[string]int)
	if !ok || counts["x"] != 1 {
		t.Fatalf("map[string]int 应保留类型，got %T %v", c["counts"], c["counts"])
	}
	// 元素是标量的容器是独立的一份：被调方原地改写不写回调用方
	raw[0] = 'X'
	tags[0] = "改了"
	counts["x"] = 99
	if string(orig["raw"].([]byte)) != "raw" || orig["tags"].([]string)[0] != "a" || orig["counts"].(map[string]int)["x"] != 1 {
		t.Errorf("改拷贝改到了原件：%v", orig)
	}
}

// TestCloneAny_PanickingMarshalerDoesNotEscape：业务值的 MarshalJSON 会 panic 时，拷贝这一步不能把它
// 冒出去（曾经按存档往返时会：节点里调是 node panic、图外调直接崩到业务代码）。
func TestCloneAny_PanickingMarshalerDoesNotEscape(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("拷贝把业务值的 panic 冒了出来：%v", r)
		}
	}()
	c := cloneAnyMap(map[string]any{"bad": clonePanicMarshaler{}})
	if _, ok := c["bad"].(clonePanicMarshaler); !ok {
		t.Errorf("序列化不了的值应原样共享，got %T", c["bad"])
	}
}

// TestCloneAny_SelfReferencingContainerTerminates：标注里自己套自己的 map / 切片。递归拷下去不会停，
// Go 的栈溢出 recover 不住、整个进程直接退出（实测）。拷到路径上已经出现过的容器时原样共享它。
func TestCloneAny_SelfReferencingContainerTerminates(t *testing.T) {
	m := map[string]any{"k": "v"}
	m["self"] = m
	s := make([]any, 2)
	s[0] = "x"
	s[1] = s
	c := cloneAnyMap(map[string]any{"m": m, "s": s})
	cm, ok := c["m"].(map[string]any)
	if !ok || cm["k"] != "v" {
		t.Fatalf("自引用 map 的其余内容应照常拷，got %v", c["m"])
	}
	if cs, ok := c["s"].([]any); !ok || len(cs) != 2 || cs[0] != "x" {
		t.Fatalf("自引用切片的其余内容应照常拷，got %v", c["s"])
	}
}

type cloneTicket struct {
	ID   string
	Tags []string
	Meta map[string]any
}

type cloneJSON map[string]any

type cloneList []any

// TestCloneAny_DeepCopiesNestedContainers：切片、数组、map、值结构体的导出字段不管元素是什么都一路拷下去。
// 曾经只拷到「元素是标量」那一层：`[]map[string]any` 这类常见的 JSON 形业务容器照样共享，被调方钩子
// 原地改一个元素就写回了调用方——续跑时换身份重问、已完成的副作用重放（实测扣款两次），并发扇出时
// 框架读摘要与兄弟被调方的钩子写同一个 map，进程 fatal。
func TestCloneAny_DeepCopiesNestedContainers(t *testing.T) {
	forms := map[string]struct {
		make   func() any
		mutate func(c any)
	}{
		"[]map[string]any": {
			func() any { return []map[string]any{{"phone": "138"}} },
			func(c any) { c.([]map[string]any)[0]["phone"] = "***" }},
		"map[string][]string": {
			func() any { return map[string][]string{"to": {"a"}} },
			func(c any) { c.(map[string][]string)["to"][0] = "***" }},
		"[][]string": {
			func() any { return [][]string{{"a"}} },
			func(c any) { c.([][]string)[0][0] = "***" }},
		"map[string]map[string]string": {
			func() any { return map[string]map[string]string{"x": {"k": "v"}} },
			func(c any) { c.(map[string]map[string]string)["x"]["k"] = "***" }},
		"[1][]string": {
			func() any { return [1][]string{{"a"}} },
			func(c any) { a := c.([1][]string); a[0][0] = "***" }},
		"值结构体的导出字段": {
			func() any { return cloneTicket{ID: "T-1", Tags: []string{"a"}, Meta: map[string]any{"k": "v"}} },
			func(c any) { tk := c.(cloneTicket); tk.Tags[0] = "***"; tk.Meta["k"] = "***" }},
		"具名 map[string]any": {
			func() any { return cloneJSON{"x": []any{"a"}} },
			func(c any) { c.(cloneJSON)["x"].([]any)[0] = "***" }},
		"具名 []any": {
			func() any { return cloneList{map[string]any{"k": "v"}} },
			func(c any) { c.(cloneList)[0].(map[string]any)["k"] = "***" }},
		"map 里套 []map[string]any": {
			func() any { return map[string]any{"contacts": []map[string]any{{"phone": "138"}}} },
			func(c any) { c.(map[string]any)["contacts"].([]map[string]any)[0]["phone"] = "***" }},
	}
	for name, f := range forms {
		orig := f.make()
		want, _ := json.Marshal(orig)
		c := cloneAny(orig)
		if reflect.TypeOf(c) != reflect.TypeOf(orig) {
			t.Errorf("%s：拷完类型变了 %T → %T", name, orig, c)
			continue
		}
		f.mutate(c)
		if got, _ := json.Marshal(orig); string(got) != string(want) {
			t.Errorf("%s：改拷贝改到了原件：%s → %s", name, want, got)
		}
	}
}

// TestCloneAny_PointersAreShared：指针原样共享（指针的身份本身可能就是意义：进程内句柄、客户端、锁）。
// 这是有意的，文档要求被调方的钩子别经指针原地改调用方交来的东西。
func TestCloneAny_PointersAreShared(t *testing.T) {
	p := &cloneTicket{ID: "T-1"}
	s := "x"
	c := cloneAnyMap(map[string]any{"p": p, "ps": []*string{&s}})
	if c["p"].(*cloneTicket) != p {
		t.Error("指针应原样共享")
	}
	if c["ps"].([]*string)[0] != &s {
		t.Error("切片里的指针应原样共享（切片本身是新的一份）")
	}
}

// TestCloneAny_PreservesNilVsEmpty：空与 nil 在 JSON 里一个是 []/{} 一个是 null。曾经把空的 []any{} 拷成
// nil：交给远程 A2A agent 时 wire 上发出 "data":null，整条消息被对端拒收（实测）。
func TestCloneAny_PreservesNilVsEmpty(t *testing.T) {
	cases := map[string]any{
		"[]any{}":                 []any{},
		"[]any(nil)":              []any(nil),
		"map{}":                   map[string]any{},
		"[]string{}":              []string{},
		"[]any{[]any{}}":          []any{[]any{}},
		`map{"x":[]any{}}`:        map[string]any{"x": []any{}},
		"cloneList{}":             cloneList{},
		"[]map[string]any{}":      []map[string]any{},
		`[]map{{"k":[]string{}}}`: []map[string]any{{"k": []string{}}},
	}
	for name, v := range cases {
		want, _ := json.Marshal(v)
		got, _ := json.Marshal(cloneAny(v))
		if string(got) != string(want) {
			t.Errorf("%s：拷前 %s 拷后 %s", name, want, got)
		}
	}
	m := &schema.Message{Metadata: &schema.MessageMetadata{Logprobs: []*schema.LogProbContent{}}}
	if c := cloneCalleeInput(m); c.Metadata.Logprobs == nil {
		t.Error("元数据里空的 logprobs 被拷成了 nil")
	}
}

// TestCloneAny_CyclesKeepShapeInCopy：自己套自己的容器拷完形状一样，回边指向拷贝而不是原件（指向原件
// 的话被调方沿着回边改就写回了调用方）。
func TestCloneAny_CyclesKeepShapeInCopy(t *testing.T) {
	m := map[string]any{"k": "v"}
	m["self"] = m
	s := make([]any, 2)
	s[0] = map[string]any{"k": "v"}
	s[1] = s
	c := cloneAnyMap(map[string]any{"m": m, "s": s})
	cm := c["m"].(map[string]any)
	if reflect.ValueOf(cm["self"]).Pointer() != reflect.ValueOf(cm).Pointer() {
		t.Error("map 的回边应指向拷贝自己")
	}
	cs := c["s"].([]any)
	back := cs[1].([]any)
	back[0].(map[string]any)["k"] = "改了"
	if s[0].(map[string]any)["k"] != "v" {
		t.Error("切片的回边指向了原件：沿着回边改写回了调用方")
	}
}

// TestCloneAny_SliceViewsOnSameArrayDoNotCollide：同一个底层数组上的两个切片（一长一短、互相嵌套）不能被
// 认成同一个。曾经按「数据指针 + 长度」相加认身份，A=arr[0:17]、B=arr[1:2] 两者相加相等，里面的 map
// 被当成回边、跟原件共享（实测）。
func TestCloneAny_SliceViewsOnSameArrayDoNotCollide(t *testing.T) {
	arr := make([]any, 17)
	a := arr[0:17]
	b := arr[1:2]
	b[0] = map[string]any{"k": "v"}
	a[0] = b
	c := cloneAny(a).([]any)
	inner := c[0].([]any)[0].(map[string]any)
	inner["k"] = "改了"
	if arr[1].(map[string]any)["k"] != "v" {
		t.Error("短切片里的 map 跟原件共享了")
	}
}
