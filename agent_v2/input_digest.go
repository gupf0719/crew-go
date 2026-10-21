package agent_v2

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
	"unsafe"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// inputDigest 算一条输入消息的摘要，是嵌套调用身份的一维（见 nestedCallNS）：不带 call key
// 的嵌套调用按「位置 + 这份摘要」定身份，同一份输入在父节点重跑时算出同一个值。
//
// # 取什么
//
// 消息的内容：角色、文本、推理文本、拒答文本、每个内容块（文本 / 图片 / 音频 / 文件 / 工具
// 调用 / 工具结果 / 结构化数据 / 媒体类型）、标注。**不取每次构造、每次调用都会变却不代表
// 输入变了的东西**：
//
//   - `ID`：schema 的构造函数每次都铸一个新的。父节点重跑时把同一句话再构造一遍，ID 就
//     不同了——算进来的话同一次调用每次重跑都成了一次新调用。
//   - `Metadata`：模型响应的元数据（时间戳 / 用量 / 结束原因），描述的是那次响应怎么来的，
//     不是这次调用的输入。
//   - `run_id` 标注（compose_v2.RunIDAnnotationKey）：每次调模型都铸一个新的。把上游模型的
//     产出原样交给嵌套调用时，内容一字不差也会被它弄得不同。
//
// ⚠️ 其余标注都算输入。**业务往输入里放每次都变的东西（请求号、时间戳）时，这次调用每次重跑
// 都是一次新调用**——要么别放，要么给调用一个 call key（[WithCallKey]）。
//
// # 怎么算：按存档往返之后的样子算
//
// 续跑时父节点交来的输入可能是从存档里解回来的，首跑时则是内存里的原值。两边必须算出同一个
// 摘要，否则同一次调用在续跑时被当成新调用——子 agent 从头跑、已做的副作用重放、问过的问题
// 换个身份重问。所以每个字段都先变成「存档往返之后的样子」再计入：
//
//   - `any` 类型的字段（标注值、结构化数据块）按存档的 JSON 往返归一化：marshal → 按存档同样
//     的方式解码 → 再 marshal。struct 变成 map、字段顺序变成字母序；数字解成 float64——超过
//     2^53 的整数在存档里本来就会丢精度，按字面精确计入的话续跑时它就变了。代价是存档分不出
//     的两个值，这里也不区分（续跑时业务自己同样拿不回那个区别）。
//   - 字符串字段里的非法 UTF-8 按 JSON 编码同样的方式换成 U+FFFD（每个非法字节换一个）。
//
// 字符串直接流进哈希（多模态的大块内容是字符串，合法 UTF-8 时只过一遍、不复制——8 MiB 的文本
// 曾经每次摘要都先复制一份），每个字段带名字与长度前缀，拼接无歧义。
//
// nil 消息返回空串。序列化不了、或往返不回来的值（见 digestValue）不让调用失败，也不跟「没有
// 这个值」混为一谈：它们按「在哪儿 + 怎么坏的」单独计入。
func inputDigest(m *schema.Message) string {
	if m == nil {
		return ""
	}
	h := sha256.New()
	w := digestWriter{h: h}
	w.str("role", string(m.Role))
	w.str("content", m.Content)
	w.str("reasoning", m.ReasoningContent)
	w.str("refusal", m.Refusal)
	w.count("blocks", len(m.ContentBlocks))
	for _, b := range m.ContentBlocks {
		w.block(b)
	}
	w.annotations(m.Annotations)
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:16])
}

// digestWriter 把字段逐个写进哈希：每个字段带名字与长度前缀，拼接无歧义。
type digestWriter struct {
	h hash.Hash
}

func (w digestWriter) raw(name string, data []byte) {
	// hash.Hash 的 Write 按契约永不返回错误
	_, _ = w.h.Write([]byte(name))
	_, _ = w.h.Write([]byte{0})
	_, _ = w.h.Write([]byte(strconv.Itoa(len(data))))
	_, _ = w.h.Write([]byte{0})
	_, _ = w.h.Write(data)
}

func (w digestWriter) str(name, s string) {
	if !utf8.ValidString(s) {
		// 转成 []rune 时每个非法字节解成一个 U+FFFD，跟 encoding/json 编码时的替换逐字相同
		s = string([]rune(s))
	}
	// 按字节视图写入，不复制：io.Writer 的契约不许 Write 改动或留住传进来的切片，sha256 也实际
	// 如此，所以借用字符串的底层字节是安全的
	w.raw(name, unsafe.Slice(unsafe.StringData(s), len(s)))
}

func (w digestWriter) count(name string, n int) {
	w.str(name, strconv.Itoa(n))
}

// present 记一个可空字段有没有：nil 与空值是两回事。
func (w digestWriter) present(name string, ok bool) {
	if ok {
		w.str(name, "1")
		return
	}
	w.str(name, "0")
}

func (w digestWriter) block(b *schema.ContentBlock) {
	w.present("block", b != nil)
	if b == nil {
		return
	}
	w.str("type", b.Type)
	w.str("media_type", b.MediaType)
	w.present("text", b.Text != nil)
	if b.Text != nil {
		w.str("text.type", string(b.Text.Type))
		w.str("text.text", b.Text.Text)
	}
	w.present("image", b.ImageUrl != nil)
	if b.ImageUrl != nil {
		w.str("image.type", string(b.ImageUrl.Type))
		w.present("image.url", b.ImageUrl.ImageUrl != nil)
		if b.ImageUrl.ImageUrl != nil {
			w.str("image.url.url", b.ImageUrl.ImageUrl.Url)
		}
	}
	w.present("audio", b.InputAudio != nil)
	if b.InputAudio != nil {
		w.str("audio.type", string(b.InputAudio.Type))
		w.present("audio.data", b.InputAudio.InputAudio != nil)
		if a := b.InputAudio.InputAudio; a != nil {
			w.str("audio.format", a.Format)
			w.str("audio.data", a.Data)
		}
	}
	w.present("file", b.File != nil)
	if b.File != nil {
		w.str("file.type", string(b.File.Type))
		w.present("file.data", b.File.File != nil)
		if f := b.File.File; f != nil {
			w.str("file.bytes", f.FileData)
			w.str("file.name", f.Filename)
			w.str("file.url", f.URL)
		}
	}
	w.present("toolcall", b.Toolcall != nil)
	if b.Toolcall != nil {
		w.str("toolcall.type", string(b.Toolcall.Type))
		w.str("toolcall.id", b.Toolcall.Id)
		w.str("toolcall.name", b.Toolcall.Name)
		w.str("toolcall.args", b.Toolcall.Arguments)
	}
	w.present("toolresult", b.Toolresult != nil)
	if b.Toolresult != nil {
		w.str("toolresult.type", string(b.Toolresult.Type))
		w.str("toolresult.call_id", b.Toolresult.ToolCallId)
		w.str("toolresult.content", b.Toolresult.Content)
		// 失败标记只在置上时写入：加这个字段之前的存档（都是 false）算出的摘要不变
		if b.Toolresult.IsError {
			w.str("toolresult.is_error", "true")
		}
	}
	w.present("data", b.Data != nil)
	if b.Data != nil {
		w.str("data.type", string(b.Data.Type))
		w.value("data.data", b.Data.Data)
	}
}

// annotations 写入去掉 run_id（每次调模型都新铸）之后的整张标注表，**整张一起**按存档的
// JSON 往返归一化（见 digestValue）。
//
// 不逐键写：键里若有非法 UTF-8，JSON 往返会把它换成 U+FFFD——两个键换完之后相对顺序可能
// 翻转，也可能变成同一个键、合并成一条。逐键按原始字节排序写入的话，首跑与续跑（解回来的
// 已是换过的键）算出不同的摘要（实测）。整张表过一遍跟存档同样的往返，键的替换、合并、
// 排序就跟存档完全一致。空表与 nil 同值（omitempty 往返后空表变 nil）。
func (w digestWriter) annotations(ann map[string]any) {
	filtered := make(map[string]any, len(ann))
	for k, v := range ann {
		if k == compose_v2.RunIDAnnotationKey {
			continue
		}
		filtered[k] = v
	}
	if len(filtered) == 0 {
		w.raw("annotations", []byte("{}"))
		return
	}
	w.value("annotations", filtered)
}

// value 写入一个 `any` 值的归一形态（见 digestValue）；有坏叶子时另写一个字段列出它们。好值
// 不写那个字段，摘要跟逐叶子处理之前逐字相同。
func (w digestWriter) value(name string, v any) {
	canonical, bad := digestValue(v)
	w.raw(name, canonical)
	if len(bad) > 0 {
		w.raw(name+".bad", []byte(strings.Join(bad, "\x00")))
	}
}

// digestValue 把一个 `any` 值归一成「存档往返之后的样子」（marshal → 按存档同样的方式解码 →
// 再 marshal，数字成 float64、map 键按字母序），返回归一后的 JSON 与坏叶子清单。
//
// 整个值往返得回来时就是这一步。往返不回来时（序列化不了：NaN / Inf、复数、函数、channel、业务
// MarshalJSON 出错或 panic、自己套自己的容器；序列化得了但解不回来：`json.Number("1e999")`、超过
// 1e308 的大数、嵌套超过一万层）**逐叶子**往下走：容器拆开，每个叶子各自往返；往返不回来的叶子在
// 归一形态里记成 null，另把「路径 + 怎么坏的」列进坏叶子清单（同一路径上的 null 与坏值因此不同，
// 坏值也不会跟任何合法的字符串撞）。
//
// 不这样做的话，一个坏叶子让整个值都成了同一个标记：两份只在别的叶子上不同的输入算出同一个身份——
// 父节点重跑时换了工单，用户对「处理 工单-1 吗」的批准被直接用在工单-2 上（实测：坏值嵌在标注的
// map 里、在数据块里、是顶层一个解不回来的大数、是结构体的一个字段，四种都中）。
//
// 逐叶子只在整个值往返不回来时才走，那样的输入永远不会从存档里解回来跟首跑比对，所以这里要的只是
// 「确定、分得开」，不必跟 JSON 逐字一致：结构体按 Go 字段名展开导出字段（嵌入的结构体按类型名
// 另开一层，未导出的嵌入结构体里的导出字段也在），键不是字符串的 map 把键拼成带种类前缀的串再
// 展开。整体算一个叶子的只剩自定义编码（实现了 MarshalJSON / MarshalText）的值，与键拼不出确定串
// （指针、结构体作键，或两个键拼出同一个串）的 map。
//
// 走的时候每个容器只走一遍：同一个容器第二次遇到（共享的子容器、回边）记一个指向它第一次出现的
// 引用，不再往下走——不然共享的子容器每条路径都重走一遍，走一张深 14 的共享结构要 20 秒以上（实测），
// 回边让递归不停、Go 的栈溢出 recover 不住。
func digestValue(v any) (canonical []byte, bad []string) {
	if generic, ok := jsonRoundTrip(v); ok {
		out, err := json.Marshal(generic)
		if err == nil {
			return out, nil
		}
	}
	n := &digestNormalizer{bad: map[string]string{}}
	tree := n.walk(reflect.ValueOf(v))
	var buf bytes.Buffer
	if err := encodeTree(&buf, tree); err != nil {
		// 走过一遍之后树里只剩往返过的值，不会走到这里；万一走到，整个值记成一个坏叶子
		return []byte("null"), []string{"\x00root\x00unencodable"}
	}
	out := buf.Bytes()
	for p, why := range n.bad {
		bad = append(bad, p+"\x00"+why)
	}
	sort.Strings(bad)
	return out, bad
}

// encodeTree 按 json.Marshal 的规则编码 walk 产出的树（map 键按字节序排、键与标量交给 json.Marshal，
// 输出与 json.Marshal 逐字相同），但不受嵌套深度限制。
//
// 不能直接 json.Marshal 这棵树：Go 1.27 起它拒绝编码嵌套超过一万层的值（1.26 只在解码时拒），而超深
// 嵌套正是走到逐叶子这条路的原因之一——用它编码，整份值就落进「根上一个坏叶子」，只在别的叶子上不同的
// 两份输入算出同一个身份。树里只有 nil / bool / float64 / string / map[string]any / []any（walk 与
// leaf 的产物），深度来自业务值本身的结构，递归在 Go 的可增长栈上走得下去（walk 本身也是递归）。
func encodeTree(buf *bytes.Buffer, v any) error {
	switch t := v.(type) {
	case map[string]any:
		if t == nil {
			buf.WriteString("null")
			return nil
		}
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			kb, err := json.Marshal(k)
			if err != nil {
				return err
			}
			buf.Write(kb)
			buf.WriteByte(':')
			if err := encodeTree(buf, t[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	case []any:
		if t == nil {
			buf.WriteString("null")
			return nil
		}
		buf.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := encodeTree(buf, e); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return err
		}
		buf.Write(b)
	}
	return nil
}

// jsonRoundTrip 按存档的方式往返一次 v；序列化出错、panic 或解不回来都返回 false。
func jsonRoundTrip(v any) (generic any, ok bool) {
	raw, err := marshalRecovered(v)
	if err != nil {
		return nil, false
	}
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, false
	}
	return generic, true
}

// marshalRecovered 是 json.Marshal，业务值的 MarshalJSON panic 时转成 error——摘要是给调用定身份用的
// 附带计算，放任 panic 就是让一次原本能跑完的调用因此失败。
//
// 有意不挂 compose_v2.PanicError、不记日志：这里的 panic 只是把这个值归成「编码不了」（见 digestValue），
// 错误不交给任何人；而摘要每次嵌套调用都算，记栈只会刷屏。
func marshalRecovered(v any) (raw []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			raw, err = nil, fmt.Errorf("marshal panic: %v", r)
		}
	}()
	return json.Marshal(v)
}

var (
	jsonMarshalerType = reflect.TypeFor[json.Marshaler]()
	textMarshalerType = reflect.TypeFor[encoding.TextMarshaler]()
)

// digestNormalizer 逐叶子归一一个往返不回来的值（见 digestValue）。整个遍历是线性的：每个容器
// 只走一遍，路径按段压栈、只在记坏叶子时才拼成串（逐层拼串的话内存随深度平方长，十万层实测
// 10 GB）。
type digestNormalizer struct {
	bad  map[string]string
	path []string
	// visited 记走过的容器与它第一次被走到的序号。走的顺序是确定的（map 键排序、切片按下标、结构体
	// 按字段声明序），序号也就是确定的。
	visited map[cloneKey]int
}

func (n *digestNormalizer) push(seg string) { n.path = append(n.path, seg) }
func (n *digestNormalizer) pop()            { n.path = n.path[:len(n.path)-1] }

func (n *digestNormalizer) pathString() string { return strings.Join(n.path, "") }

func (n *digestNormalizer) markBad(why string) { n.bad[n.pathString()] = why }

// visit 记下容器 k；第一次遇到返回 true。走过的容器再遇到时在当前位置记一个指向它的引用、返回
// false（不再往下走）。
func (n *digestNormalizer) visit(k cloneKey) bool {
	if order, seen := n.visited[k]; seen {
		n.markBad("ref:" + strconv.Itoa(order))
		return false
	}
	if n.visited == nil {
		n.visited = map[cloneKey]int{}
	}
	n.visited[k] = len(n.visited)
	return true
}

// dropUnder 删掉 path 自己与它下面的坏叶子。只在两个键撞成同一个时调（见 stringMap）。
func (n *digestNormalizer) dropUnder(path string) {
	for p := range n.bad {
		if p == path || strings.HasPrefix(p, path+"/") {
			delete(n.bad, p)
		}
	}
}

func (n *digestNormalizer) walk(v reflect.Value) any {
	if !v.IsValid() {
		return nil
	}
	t := v.Type()
	if t.Implements(jsonMarshalerType) || t.Implements(textMarshalerType) {
		return n.leaf(v)
	}
	// 指针接收者的编码方法：encoding/json 对可寻址的值（切片元素、结构体字段）会调它，这里也调——
	// 不然同样的 []big.Int 在整体往返时分得开、旁边多一个坏叶子走到这里就分不开了（实测）
	if v.CanAddr() {
		if pt := reflect.PointerTo(t); pt.Implements(jsonMarshalerType) || pt.Implements(textMarshalerType) {
			return n.leaf(v.Addr())
		}
	}
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return n.walk(v.Elem())
	case reflect.Pointer:
		if v.IsNil() {
			return nil
		}
		if !n.visit(cloneKey{typ: t, ptr: v.Pointer()}) {
			return nil
		}
		return n.walk(v.Elem())
	case reflect.Map:
		if v.IsNil() {
			return nil
		}
		if !n.visit(cloneKey{typ: t, ptr: v.Pointer()}) {
			return nil
		}
		if t.Key().Kind() == reflect.String {
			return n.stringMap(v)
		}
		return n.otherMap(v)
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return n.leaf(v) // 字节串编码成一个 base64 字符串
		}
		if v.Kind() == reflect.Slice {
			if v.IsNil() {
				return nil
			}
			if !n.visit(cloneKey{typ: t, ptr: v.Pointer(), len: v.Len()}) {
				return nil
			}
		}
		out := make([]any, v.Len())
		for i := range out {
			n.push("/" + strconv.Itoa(i))
			out[i] = n.walk(v.Index(i))
			n.pop()
		}
		return out
	case reflect.Struct:
		return n.structFields(v)
	case reflect.Complex64, reflect.Complex128:
		n.markBad("complex:" + strconv.FormatComplex(v.Complex(), 'g', -1, 128))
		return nil
	}
	return n.leaf(v)
}

// stringMap 走键是字符串的 map：跟 encoding/json 同样按原始键排序、同样替换非法 UTF-8。
func (n *digestNormalizer) stringMap(v reflect.Value) any {
	keys := v.MapKeys()
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	out := make(map[string]any, len(keys))
	for _, k := range keys {
		jk := k.String()
		if !utf8.ValidString(jk) {
			jk = string([]rune(jk))
		}
		n.push("/" + strconv.Itoa(len(jk)) + ":" + jk)
		if _, dup := out[jk]; dup {
			// 替换之后撞成同一个键：解码留下的是排在后面的那个，前一个留下的坏叶子跟着它一起消失
			n.dropUnder(n.pathString())
		}
		out[jk] = n.walk(v.MapIndex(k))
		n.pop()
	}
	return out
}

// otherMap 走键不是字符串的 map：键拼成带种类前缀的串（1 与 "1" 不撞）再按它排序展开。键拼不出
// 确定的串、或两个键拼出同一个串时，整张表按一个叶子处理——那时的遍历顺序取决于 map 的随机
// 迭代序，展开的话同一份输入每次算出不同的摘要。
func (n *digestNormalizer) otherMap(v reflect.Value) any {
	type entry struct {
		key string
		val reflect.Value
	}
	entries := make([]entry, 0, v.Len())
	seen := make(map[string]bool, v.Len())
	for it := v.MapRange(); it.Next(); {
		k, ok := mapKeyString(it.Key())
		if !ok || seen[k] {
			return n.leaf(v)
		}
		seen[k] = true
		entries = append(entries, entry{key: k, val: it.Value()})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	out := make(map[string]any, len(entries))
	for _, e := range entries {
		n.push("/" + strconv.Itoa(len(e.key)) + ":" + e.key)
		out[e.key] = n.walk(e.val)
		n.pop()
	}
	return out
}

// mapKeyString 把一个 map 键拼成确定的、带种类前缀的串；拼不出（指针、结构体、接口里的这些）时
// ok=false。
func mapKeyString(k reflect.Value) (string, bool) {
	if k.Kind() == reflect.Interface {
		if k.IsNil() {
			return "nil", true
		}
		k = k.Elem()
	}
	switch k.Kind() {
	case reflect.String:
		return "s:" + k.String(), true
	case reflect.Bool:
		return "b:" + strconv.FormatBool(k.Bool()), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "i:" + strconv.FormatInt(k.Int(), 10), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return "u:" + strconv.FormatUint(k.Uint(), 10), true
	case reflect.Float32, reflect.Float64:
		return "f:" + strconv.FormatFloat(k.Float(), 'g', -1, 64), true
	}
	if k.CanInterface() && k.Type().Implements(textMarshalerType) {
		text, err := k.Interface().(encoding.TextMarshaler).MarshalText()
		if err == nil {
			return "t:" + string(text), true
		}
	}
	return "", false
}

// structFields 按字段声明序展开一个结构体的导出字段（键是 Go 字段名）；嵌入的结构体（含未导出的
// 嵌入类型——它提升上来的导出字段 encoding/json 照样编码）按类型名另开一层。
func (n *digestNormalizer) structFields(v reflect.Value) any {
	t := v.Type()
	out := make(map[string]any, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Anonymous {
			ft := f.Type
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				name := "embed:" + ft.String()
				n.push("/" + name)
				out[name] = n.walk(v.Field(i))
				n.pop()
				continue
			}
		}
		if !f.IsExported() {
			continue
		}
		n.push("/" + f.Name)
		out[f.Name] = n.walk(v.Field(i))
		n.pop()
	}
	return out
}

// leaf 往返一个叶子；往返不回来就记进坏叶子清单：序列化得了的记它序列化出来的样子（1e999 与
// 1e998 因此不同），序列化不了的记类型（浮点数另记 NaN / ±Inf 的值）。读不到的值（未导出的嵌入
// 类型自己带编码方法时）记类型。
func (n *digestNormalizer) leaf(v reflect.Value) any {
	if !v.CanInterface() {
		n.markBad("opaque:" + v.Type().String())
		return nil
	}
	raw, err := marshalRecovered(v.Interface())
	if err != nil {
		why := "type:" + v.Type().String()
		if k := v.Kind(); k == reflect.Float32 || k == reflect.Float64 {
			why += ":" + strconv.FormatFloat(v.Float(), 'g', -1, 64)
		}
		n.markBad(why)
		return nil
	}
	var generic any
	if err := json.Unmarshal(raw, &generic); err != nil {
		n.markBad("raw:" + string(raw))
		return nil
	}
	return generic
}

// identityHash 把一次调用的身份串压成定长的十六进制串（步骤记录的键用）。
func identityHash(identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:16])
}
