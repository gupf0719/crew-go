package agent_v2

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"sort"
	"testing"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// nestedCallNS 与 inputDigest 的每一维单独钉一遍：哪一维被拆掉，这里对应的那一条就红。
// 端到端的后果（续上别人的存档、答案套到别的调用上、已完成的调用重跑）由
// nested_call_identity_test.go 与 nested_call_order_drift_test.go 钉。

// nsOf 取不带 call key 时的子 ns。
func nsOf(t *testing.T, ctx context.Context, name, digest string) string {
	t.Helper()
	ns, _, err := nestedCallNS(ctx, name, "", digest, false, false)
	if err != nil {
		t.Fatalf("不带 call key 的派生不该报错：%v", err)
	}
	return ns
}

// execCtx 造一个「在一次执行里」的 ctx：借 WithCurrentToolCallContext 装上计数器，再清掉
// tool_call ID 让位置走静态路径（图外没有计数器，数不出「第几次」）。
func execCtx(parentNS string) context.Context {
	ctx := compose_v2.WithGraphNS(context.Background(), parentNS)
	return compose_v2.WithCurrentToolCallContext(ctx, "", 0)
}

func TestNestedCallNS_ToolCallDimensions(t *testing.T) {
	base := compose_v2.WithGraphNS(context.Background(), "p")
	first := nsOf(t, compose_v2.WithCurrentToolCallContext(base, "call_0", 0), "sub", "d")
	// 批内下标：同一批两个 tool_call 同 ID
	second := nsOf(t, compose_v2.WithCurrentToolCallContext(base, "call_0", 1), "sub", "d")
	if second == first {
		t.Errorf("同批同 ID 的第 2 个 tool_call 与第 1 个落在同一个 ns %q——批内下标没进身份", first)
	}
	// 同一个 tool_call、同一份输入：两次执行里算出同一个 ns（续跑找得回来）
	again := nsOf(t, compose_v2.WithCurrentToolCallContext(base, "call_0", 0), "sub", "d")
	if again != first {
		t.Errorf("同一个 tool_call 同一份输入应算出同一个 ns：%q vs %q", first, again)
	}
	// 访问序号：下一轮复用同一个 ID。visit 由节点入口注入，这里直接比较 toolCallInstanceKey 的输出。
	if toolCallInstanceKey("call_0", 0, 1) == toolCallInstanceKey("call_0", 0, 0) {
		t.Errorf("跨轮复用同一个 ID 时两轮的身份相同——tools 节点访问序号没进身份")
	}
	if toolCallInstanceKey("call_0", 1, 0) == toolCallInstanceKey("call_0", 0, 1) {
		t.Errorf("批内下标 1 与访问序号 1 撞成同一个身份")
	}
}

// TestNestedCallNS_ContentIdentityIgnoresCallOrder：身份按内容，跟调用次序无关——两次执行里
// 调用的先后换了、中间多了或少了别的调用，同一份输入算出的 ns 不变。
func TestNestedCallNS_ContentIdentityIgnoresCallOrder(t *testing.T) {
	run := func(digests ...string) map[string]string {
		ctx := execCtx("p")
		out := map[string]string{}
		for _, d := range digests {
			out[d] = nsOf(t, ctx, "sub", d)
		}
		return out
	}
	first := run("A", "B", "C")
	reordered := run("C", "B", "A")
	skipped := run("B", "C")
	inserted := run("X", "A", "B", "C")
	for _, d := range []string{"A", "B", "C"} {
		if reordered[d] != first[d] {
			t.Errorf("输入 %s：次序换了之后 ns 变了 %q → %q", d, first[d], reordered[d])
		}
		if inserted[d] != first[d] {
			t.Errorf("输入 %s：前面插进一次调用之后 ns 变了 %q → %q", d, first[d], inserted[d])
		}
	}
	for _, d := range []string{"B", "C"} {
		if skipped[d] != first[d] {
			t.Errorf("输入 %s：跳过前一次调用之后 ns 变了 %q → %q", d, first[d], skipped[d])
		}
	}
	if first["A"] == first["B"] || first["B"] == first["C"] || first["A"] == first["C"] {
		t.Fatalf("输入不同的调用落在同一个 ns：%v", first)
	}
}

// TestNestedCallNS_SameInputCountsByOrder：同一份输入调多次（投票、冗余调用）时按次序区分，
// 各落一个槽；别的输入夹在中间不影响这份输入的编号。
func TestNestedCallNS_SameInputCountsByOrder(t *testing.T) {
	ctx := execCtx("p")
	a0 := nsOf(t, ctx, "sub", "A")
	_ = nsOf(t, ctx, "sub", "B")
	a1 := nsOf(t, ctx, "sub", "A")
	a2 := nsOf(t, ctx, "sub", "A")
	if a0 == a1 || a1 == a2 || a0 == a2 {
		t.Fatalf("同一份输入的三次调用应落在三个不同的 ns：%q %q %q", a0, a1, a2)
	}
	ctx2 := execCtx("p")
	b0 := nsOf(t, ctx2, "sub", "A")
	b1 := nsOf(t, ctx2, "sub", "A")
	if b0 != a0 || b1 != a1 {
		t.Errorf("同一份输入的第 k 次在另一次执行里应算出同一个 ns（中间夹的别的输入不影响编号）")
	}
	// 别的名字互不影响
	if nsOf(t, ctx, "other", "A") == nsOf(t, ctx, "sub", "Z") {
		t.Errorf("不同名字的调用撞在了一起")
	}
	for _, ns := range []string{a0, a1, a2} {
		segs, err := compose_v2.SplitNS(ns)
		if err != nil {
			t.Fatalf("派生出的 ns %q 不合语法：%v", ns, err)
		}
		last := segs[len(segs)-1]
		if last.NodeID != "sub" || last.Kind != "fanout" {
			t.Errorf("调用的 ns %q 末段应是 sub 的一个 # 实例，got %+v", ns, last)
		}
	}
}

func TestNestedCallNS_InstanceAndToolCallBothCount(t *testing.T) {
	// SendFrom 实例节点里，业务自写的 toolsFn 又起了两个 tool_call：两维都得进身份
	base := compose_v2.WithNodeInstanceID(compose_v2.WithGraphNS(context.Background(), "p"), "i5")
	a := nsOf(t, compose_v2.WithCurrentToolCallContext(base, "tc_a", 0), "sub", "d")
	b := nsOf(t, compose_v2.WithCurrentToolCallContext(base, "tc_b", 1), "sub", "d")
	if a == b {
		t.Fatalf("同一个实例里两个 tool_call 落在同一个 ns %q——只取了实例下标", a)
	}
	// 只有实例、不在 tool_call 里：跟别的实例区分开
	other := compose_v2.WithNodeInstanceID(compose_v2.WithGraphNS(context.Background(), "p"), "i6")
	if nsOf(t, base, "sub", "d") == nsOf(t, other, "sub", "d") {
		t.Errorf("两个实例同一份输入落在同一个 ns——实例下标没进身份")
	}
}

func TestNestedCallNS_CallKey(t *testing.T) {
	ctx := execCtx("p")
	a, _, err := nestedCallNS(ctx, "sub", "item-a", "dA", false, false)
	if err != nil {
		t.Fatalf("第一次用 key item-a 不该报错：%v", err)
	}
	b, _, err := nestedCallNS(ctx, "sub", "item-b", "dA", false, false)
	if err != nil {
		t.Fatalf("另一个 key 不该报错：%v", err)
	}
	if a == b {
		t.Fatalf("两个不同的 key（同一份输入）落在同一个 ns %q", a)
	}
	// key 决定身份：同一个 key 换了输入，ns 不变
	ctx2 := execCtx("p")
	a2, _, err := nestedCallNS(ctx2, "sub", "item-a", "d-changed", false, false)
	if err != nil || a2 != a {
		t.Errorf("同一个 key 输入变了应落同一个槽 %q，got %q err=%v", a, a2, err)
	}
	// 带 key 与不带 key 的身份不相交
	if nsOf(t, ctx, "sub", "dA") == a {
		t.Errorf("不带 key 的调用撞上了带 key 的槽")
	}
	// 同一次执行里同一个 key 再用一次 ⟹ 两次调用共用一个身份，终局失败
	_, _, err = nestedCallNS(ctx, "sub", "item-a", "dA", false, false)
	if !errors.Is(err, compose_v2.ErrRunAborted) {
		t.Fatalf("同一次执行里重复用 key 应报 ErrRunAborted，got %v", err)
	}
}

func TestNestedCallNS_RetryIsNotDuplicate(t *testing.T) {
	for _, key := range []string{"", "item-a"} {
		ctx := execCtx("p")
		var seen []string
		_, err := compose_v2.RunWithRetry(ctx, compose_v2.SimpleRetry(2), func(ctx context.Context) (struct{}, error) {
			ns, _, err := nestedCallNS(ctx, "sub", key, "dA", false, false)
			if err != nil {
				return struct{}{}, err
			}
			seen = append(seen, ns)
			if len(seen) < 3 {
				return struct{}{}, errors.New("瞬时失败")
			}
			return struct{}{}, nil
		})
		if err != nil {
			t.Fatalf("key=%q：RunWithRetry 的每次尝试是同一次调用，不该出错：%v", key, err)
		}
		if len(seen) != 3 || seen[0] != seen[1] || seen[1] != seen[2] {
			t.Errorf("key=%q：三次尝试应落同一个槽，got %v", key, seen)
		}
	}
}

func TestIdentityString_Unambiguous(t *testing.T) {
	if identityString("w", "a\x00b") == identityString("w\x00a", "b") {
		t.Fatalf("段边界可以挪动而拼出同一个串")
	}
	if identityString("ab", "c") == identityString("a", "bc") {
		t.Fatalf("段边界可以挪动而拼出同一个串")
	}
}

func TestInputDigest_IgnoresPerConstructionFields(t *testing.T) {
	a := schema.UserMessage("同一句话")
	b := schema.UserMessage("同一句话")
	if a.ID == b.ID {
		t.Fatalf("前提不成立：两次构造的消息 ID 应不同（否则这条用例测不到 ID 被排除）")
	}
	if inputDigest(a) != inputDigest(b) {
		t.Errorf("同一句话构造两次，摘要应相同（ID 每次都铸新的，不代表输入变了）")
	}
	b.Metadata = &schema.MessageMetadata{Created: 12345, Model: "m"}
	if inputDigest(a) != inputDigest(b) {
		t.Errorf("响应元数据不同不该影响摘要")
	}
	if inputDigest(a) == inputDigest(schema.UserMessage("另一句话")) {
		t.Errorf("内容不同，摘要应不同")
	}
	if inputDigest(nil) != "" {
		t.Errorf("nil 消息应返回空串")
	}
	if got := len(inputDigest(a)); got != 32 {
		t.Errorf("摘要长度 = %d，want 32 个十六进制字符", got)
	}
}

func TestInputDigest_IgnoresRunIDAnnotation(t *testing.T) {
	a := schema.AssistantMessage("固定的计划文本")
	a.Annotations = map[string]any{compose_v2.RunIDAnnotationKey: "run-1", "keep": "x"}
	b := schema.AssistantMessage("固定的计划文本")
	b.Annotations = map[string]any{compose_v2.RunIDAnnotationKey: "run-2", "keep": "x"}
	if inputDigest(a) != inputDigest(b) {
		t.Errorf("只有每次调模型铸的 run_id 不同，摘要应相同")
	}
	b.Annotations["keep"] = "y"
	if inputDigest(a) == inputDigest(b) {
		t.Errorf("其它标注不同，摘要应不同")
	}
}

// roundTrip 把消息按存档的方式往返一遍（tagged JSON，与 channel 值进出存档同一条路）。
func roundTrip(t *testing.T, m *schema.Message) *schema.Message {
	t.Helper()
	raw, err := compose_v2.MarshalTagged(m)
	if err != nil {
		t.Fatalf("序列化失败：%v", err)
	}
	v, err := compose_v2.UnmarshalTagged(raw)
	if err != nil {
		t.Fatalf("反序列化失败：%v", err)
	}
	decoded, ok := v.(*schema.Message)
	if !ok {
		t.Fatalf("往返后类型是 %T", v)
	}
	return decoded
}

// TestInputDigest_StableAcrossCheckpointRoundTrip：续跑时父节点交来的输入可能是从存档里解回来
// 的，首跑时是内存里的原值——两边算出同一个摘要，否则同一次调用在续跑时被当成新调用（子
// agent 从头跑、副作用重放、问过的问题换个身份重问）。
//
// 每一格都是存档往返会改变的一种形态；changed 钉「前提成立」——往返真的改了值，这一格才测得
// 到归一化。
func TestInputDigest_StableAcrossCheckpointRoundTrip(t *testing.T) {
	type payload struct {
		Zeta  string `json:"zeta"`
		Alpha int    `json:"alpha"`
	}
	cases := []struct {
		name    string
		build   func() *schema.Message
		changed func(orig, decoded *schema.Message) bool
	}{
		{"标注里的 struct 变成 map", func() *schema.Message {
			m := schema.UserMessage("x")
			m.Annotations = map[string]any{"p": payload{Zeta: "z", Alpha: 1}}
			return m
		}, func(_, d *schema.Message) bool { _, isMap := d.Annotations["p"].(map[string]any); return isMap }},
		{"结构化数据块里的 struct 变成 map", func() *schema.Message {
			m := schema.UserMessage("x")
			m.ContentBlocks = []*schema.ContentBlock{{Type: "data", Data: &schema.DataContentBlock{
				Type: schema.ContentBlockTypeData, Data: payload{Zeta: "z", Alpha: 2}}}}
			return m
		}, func(_, d *schema.Message) bool {
			_, isMap := d.ContentBlocks[0].Data.Data.(map[string]any)
			return isMap
		}},
		{"超过 2^53 的整数丢精度", func() *schema.Message {
			m := schema.UserMessage("处理这条记录")
			m.Annotations = map[string]any{"record_id": int64(1<<60 + 1)}
			return m
		}, func(_, d *schema.Message) bool {
			f, ok := d.Annotations["record_id"].(float64)
			return ok && int64(f) != 1<<60+1
		}},
		{"文本里的非法 UTF-8 被替换", func() *schema.Message {
			return schema.UserMessage("坏字节:\xff\xfe结尾")
		}, func(o, d *schema.Message) bool { return o.Content != d.Content }},
		{"标注键里的非法 UTF-8 被替换", func() *schema.Message {
			m := schema.UserMessage("x")
			m.Annotations = map[string]any{"k\xff": "v\xfe"}
			return m
		}, func(_, d *schema.Message) bool { _, same := d.Annotations["k\xff"]; return !same }},
		// 逐键按原始字节排序写入摘要时，下面两格首跑与续跑算出不同的摘要（实测）：
		// 键的替换、合并、排序必须跟存档往返完全一致，所以整张表一起归一化。
		{"两个含非法 UTF-8 的标注键：替换后相对顺序翻转", func() *schema.Message {
			m := schema.UserMessage("x")
			// 原始字节序 \xf0\x9f\x98\x80（合法 😀）< \xff；替换后 U+FFFD（EF BF BD）< F0…，顺序翻转
			m.Annotations = map[string]any{"a\xf0\x9f\x98\x80": "emoji", "a\xff": "bad"}
			return m
		}, func(_, d *schema.Message) bool { _, same := d.Annotations["a\xff"]; return !same }},
		{"两个含非法 UTF-8 的标注键：替换后撞成同一个键", func() *schema.Message {
			m := schema.UserMessage("x")
			m.Annotations = map[string]any{"k\xff": "v1", "k\xfe": "v2"}
			return m
		}, func(_, d *schema.Message) bool { return len(d.Annotations) == 1 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			orig := c.build()
			decoded := roundTrip(t, orig)
			if !c.changed(orig, decoded) {
				t.Fatalf("前提不成立：存档往返没有改变这个值，这一格测不到归一化")
			}
			if inputDigest(orig) != inputDigest(decoded) {
				t.Fatalf("同一份输入经存档往返后摘要变了")
			}
			// 往返一次之后再往返，摘要不再变
			if inputDigest(decoded) != inputDigest(roundTrip(t, decoded)) {
				t.Fatalf("第二次往返后摘要又变了")
			}
		})
	}
}

// TestInputDigest_ContentBlocksCount：内容块的每类内容都进摘要（多模态输入只在块里不同时，
// 不能算成同一份输入）。
func TestInputDigest_ContentBlocksCount(t *testing.T) {
	img := func(url string) *schema.Message {
		m := schema.UserMessage("看图")
		m.ContentBlocks = []*schema.ContentBlock{{Type: "image_url", ImageUrl: &schema.ImageUrlContentBlock{
			Type: schema.ContentBlockTypeImageUrl, ImageUrl: &schema.ImageUrl{Url: url}}}}
		return m
	}
	if inputDigest(img("https://a/1.png")) == inputDigest(img("https://a/2.png")) {
		t.Fatalf("两张不同的图算出了同一个摘要")
	}
	withBlock := img("https://a/1.png")
	if inputDigest(withBlock) == inputDigest(schema.UserMessage("看图")) {
		t.Fatalf("有图和没图算出了同一个摘要")
	}
}

// panicMarshaler 的 MarshalJSON 直接 panic（模拟业务值的 bug）。
type panicMarshaler struct{}

func (panicMarshaler) MarshalJSON() ([]byte, error) { panic("业务 MarshalJSON 的 bug") }

func TestInputDigest_MarshalPanicDoesNotEscape(t *testing.T) {
	m := schema.UserMessage("hi")
	m.Annotations = map[string]any{"bad": panicMarshaler{}}
	var got string
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("inputDigest 把业务值的 panic 冒了出来：%v——原本能跑完的调用会因此失败", r)
			}
		}()
		got = inputDigest(m)
	}()
	if got == "" {
		t.Fatalf("算不出的值按固定标记计入，摘要本身不该是空的")
	}
	if got == inputDigest(schema.UserMessage("hi")) {
		t.Errorf("「有一个算不出的值」不该跟「没有这个值」算成一样")
	}
}

// TestInputDigest_CoversEveryField 是字段覆盖哨兵：Message 与内容块的类型加了字段时这里就红，
// 逼改的人决定新字段算不算输入（算的话加进 inputDigest，不算的话加进下面的排除表）。
func TestInputDigest_CoversEveryField(t *testing.T) {
	covered := map[string][]string{
		"Message":                {"ID", "Role", "Content", "ContentBlocks", "Refusal", "ReasoningContent", "Annotations", "Metadata"},
		"ContentBlock":           {"Type", "Text", "ImageUrl", "InputAudio", "File", "Toolcall", "Toolresult", "Data", "MediaType"},
		"TextContentBlock":       {"Type", "Text"},
		"ImageUrlContentBlock":   {"Type", "ImageUrl"},
		"ImageUrl":               {"Url"},
		"InputAudioContentBlock": {"Type", "InputAudio"},
		"InputAudio":             {"Format", "Data"},
		"FileContentBlock":       {"Type", "File"},
		"FileData":               {"FileData", "Filename", "URL"},
		"ToolcallContentBlock":   {"Type", "Id", "Arguments", "Name"},
		"ToolresultContentBlock": {"Type", "ToolCallId", "Content", "IsError"},
		"DataContentBlock":       {"Type", "Data"},
	}
	types := []any{
		schema.Message{}, schema.ContentBlock{}, schema.TextContentBlock{}, schema.ImageUrlContentBlock{},
		schema.ImageUrl{}, schema.InputAudioContentBlock{}, schema.InputAudio{}, schema.FileContentBlock{},
		schema.FileData{}, schema.ToolcallContentBlock{}, schema.ToolresultContentBlock{}, schema.DataContentBlock{},
	}
	for _, v := range types {
		rt := reflect.TypeOf(v)
		var fields []string
		for i := 0; i < rt.NumField(); i++ {
			fields = append(fields, rt.Field(i).Name)
		}
		want := append([]string(nil), covered[rt.Name()]...)
		sort.Strings(fields)
		sort.Strings(want)
		if !reflect.DeepEqual(fields, want) {
			t.Errorf("%s 的字段变了：现在 %v，摘要覆盖表 %v——决定新字段算不算输入，改 inputDigest 与本表",
				rt.Name(), fields, want)
		}
	}
}

// TestInputDigest_UnmarshalableValueMarksOnlyItself：标注表里一个值序列化不了（NaN、函数……）时，只有
// 它自己算成「坏叶子」，别的标注照样算进身份。整张表一起换成一个标记的话，两份只在别的标注上不同的
// 输入算出同一个身份：父节点重跑时换了工单，用户对「处理 工单-1 吗」的批准被直接用在工单-2 上（实测）。
// 坏叶子按「在哪儿 + 怎么坏的」计入：跟正常值、跟任何合法的字符串都不同。
func TestInputDigest_UnmarshalableValueMarksOnlyItself(t *testing.T) {
	with := func(ticket string, bad any) *schema.Message {
		m := schema.UserMessage("处理工单")
		m.Annotations = map[string]any{"ticket": ticket, "score": bad}
		return m
	}
	if inputDigest(with("工单-1", math.NaN())) == inputDigest(with("工单-2", math.NaN())) {
		t.Errorf("只在工单上不同的两份输入算出了同一个身份——坏值把整张标注表抹掉了")
	}
	if inputDigest(with("工单-1", math.NaN())) != inputDigest(with("工单-1", math.NaN())) {
		t.Errorf("同一份输入两次算出的摘要不同")
	}
	if inputDigest(with("工单-1", math.NaN())) == inputDigest(with("工单-1", 0.5)) {
		t.Errorf("「算不出的值」不该跟一个正常值算成一样")
	}
	if inputDigest(with("工单-1", math.NaN())) == inputDigest(with("工单-1", nil)) {
		t.Errorf("坏值不该跟同一位置上的 null 算成一样")
	}
	if inputDigest(with("工单-1", math.NaN())) == inputDigest(with("工单-1", "\x00unmarshalable\x00")) {
		t.Errorf("坏值不该跟任何合法的字符串撞（曾经换成一个固定字符串标记）")
	}
}

// TestInputDigest_BadLeafAnywhere_OtherLeavesStillCount：坏值不在标注表的顶层时同样只标记它自己——嵌在
// 标注的 map 里、在数据块里、或是一个序列化得了却解不回来的值（json.Number("1e999")、超过 1e308 的
// 大数、嵌套超过一万层）。曾经这几种都让整个值成了同一个标记，工单-1 的批准照样兑付给工单-2（实测）。
func TestInputDigest_BadLeafAnywhere_OtherLeavesStillCount(t *testing.T) {
	deep := func() any {
		var v any = "底"
		for i := 0; i < 10001; i++ {
			v = []any{v}
		}
		return v
	}
	shapes := map[string]func(ticket string) *schema.Message{
		"嵌在标注的 map 里": func(ticket string) *schema.Message {
			m := schema.UserMessage("处理工单")
			m.Annotations = map[string]any{"meta": map[string]any{"ticket": ticket, "score": math.NaN()}}
			return m
		},
		"在数据块里": func(ticket string) *schema.Message {
			return &schema.Message{Role: schema.RoleUser, ContentBlocks: []*schema.ContentBlock{{
				Type: "data", Data: &schema.DataContentBlock{Data: map[string]any{"ticket": ticket, "score": math.NaN()}},
			}}}
		},
		"解不回来的大数": func(ticket string) *schema.Message {
			m := schema.UserMessage("处理工单")
			m.Annotations = map[string]any{"ticket": ticket, "amount": json.Number("1e999")}
			return m
		},
		"嵌套超过一万层": func(ticket string) *schema.Message {
			m := schema.UserMessage("处理工单")
			m.Annotations = map[string]any{"ticket": ticket, "tree": deep()}
			return m
		},
		"函数值": func(ticket string) *schema.Message {
			m := schema.UserMessage("处理工单")
			m.Annotations = map[string]any{"meta": []any{ticket, func() {}}}
			return m
		},
	}
	for name, mk := range shapes {
		if inputDigest(mk("工单-1")) == inputDigest(mk("工单-2")) {
			t.Errorf("%s：只在工单上不同的两份输入算出了同一个身份", name)
		}
		if inputDigest(mk("工单-1")) != inputDigest(mk("工单-1")) {
			t.Errorf("%s：同一份输入两次算出的摘要不同", name)
		}
	}
	// 往返得回来的值（不含坏叶子）摘要跟逐叶子处理之前逐字相同：存档往返之后的样子不变
	good := schema.UserMessage("x")
	good.Annotations = map[string]any{"a": 1, "b": []any{"c", map[string]any{"d": true}}}
	if got, want := inputDigest(good), inputDigest(good); got != want {
		t.Fatalf("不稳定：%s vs %s", got, want)
	}
}
