package agent_v2

import (
	"reflect"

	"crew-go/schema"
)

// cloneCalleeInput 给嵌套调用的被调方一份输入的深拷贝。
//
// 被调方的钩子可以原地改写它拿到的消息（`OnAgentStart` 的 `input.Messages` 是[消费]档，框架
// 把改写喂给下游）。交出去的若是调用方手里那条——最常见的写法就是把从 channel 里读出来的那条
// 原样交给 RunSubAgent——改写会顺着指针写进调用方的 channel、随调用方的存档落盘：续跑时调用方
// 读到的已经是改写过的消息，输入摘要跟着变，同一次调用被当成另一次。实测每次加后缀的钩子让
// 续跑永不收敛（每次换一个提问 ID 重问），连幂等的脱敏钩子也多问一次、多执行一次。
//
// 输入摘要仍按调用方手里的原件算（调用方交来的是什么，这次调用就是什么）。
//
// # 拷到哪一层
//
// 消息的结构逐层拷：内容块切片与每个块、块里的子结构、标注 map、元数据。`any` 类型的值
// （标注值、数据块的 Data）**按原类型递归深拷**，拷完类型不变、内容不丢、nil 与空不混：
//
//   - 切片、数组、map 不管元素是什么都一路拷下去（`[]map[string]any`、`map[string][]string`、
//     具名的 `map[string]any` 这类常见的 JSON 形业务容器都在内）；接口里的值拷它的动态值；
//   - 值结构体整体复制一份，再逐个拷**导出**字段，连同嵌入的结构体（含未导出的嵌入类型）里的
//     导出字段——encoding/json 把它们提升上来编码、摘要也计入它们，共享的话被调方钩子一改就写回
//     调用方、续跑换身份重问，扇出时跟兄弟调用的摘要读撞成数据竞争（均实测）。其余未导出字段随
//     整体复制，里面的切片 / map 仍共享；
//   - 标量原样（不可变）；
//   - **指针、函数、channel 原样共享**——指针的身份本身可能就是意义（进程内句柄、客户端、
//     锁），深拷会把它们拷坏。所以被调方的钩子**别经指针原地改**调用方交来的东西：那样改会
//     写回调用方，随它的存档落盘，续跑时摘要变了、同一次调用被当成另一次（换身份重问）。
//
// 只拷到「元素是标量」那一层是不够的：`[]map[string]any` 这类容器共享时，被调方钩子原地改一个
// 元素就写回了调用方，续跑时换身份重问、已完成的副作用重放（实测扣款两次），并发扇出时框架
// 读摘要与兄弟被调方的钩子写同一个 map，进程直接 fatal（recover 不住）。
//
// 已经拷过的容器按「类型 + 底层指针 + 长度」记下它的拷贝：自己套自己的容器（`m["self"] = m`）
// 递归到回边时拿到的是那份拷贝，拷完的形状跟原件一样、不会无限递归（Go 的栈溢出是 fatal，
// 实测整个进程退出）；同一个容器被引用两次也只拷一份。
//
// 曾经把业务类型按存档往返一遍（拿到的是 JSON 形状的值，跟被调方续跑时从存档读到的一样），
// 实测是**首跑就丢东西**：`int64` 超过 2^53 丢精度（订单号 …789 成了 …768）、`json.Number`
// 成了 `float64`、只有未导出字段的句柄与 error 成了空 map、`[]byte` 成了 base64 字符串，
// 业务 MarshalJSON panic 时直接冒出去。存档往返的这些损失是存档自己的限制，续跑时才有；不该
// 让每一次首跑都先吃一遍。
//
// 摘要照旧按「存档往返之后的样子」算（见 inputDigest），跟这里拷成什么样无关。
//
// 元数据里的 logprobs 切片拷一份、元素共享：那是模型输出的附属，出现在一条输入消息上已经很少见，
// 钩子更没有理由改它。
//
// 新增字段时 `TestCloneCalleeInput_FieldCoverage` 会红，逼着这里跟上。
func cloneCalleeInput(m *schema.Message) *schema.Message {
	if m == nil {
		return nil
	}
	c := *m
	if m.ContentBlocks != nil {
		c.ContentBlocks = make([]*schema.ContentBlock, len(m.ContentBlocks))
		for i, b := range m.ContentBlocks {
			c.ContentBlocks[i] = cloneContentBlock(b)
		}
	}
	if m.Annotations != nil {
		c.Annotations = cloneAnyMap(m.Annotations)
	}
	if m.Metadata != nil {
		md := *m.Metadata
		md.Usage = clonePtr(m.Metadata.Usage)
		if m.Metadata.Logprobs != nil {
			md.Logprobs = make([]*schema.LogProbContent, len(m.Metadata.Logprobs))
			copy(md.Logprobs, m.Metadata.Logprobs)
		}
		c.Metadata = &md
	}
	return &c
}

func cloneContentBlock(b *schema.ContentBlock) *schema.ContentBlock {
	if b == nil {
		return nil
	}
	c := *b
	c.Text = clonePtr(b.Text)
	if b.ImageUrl != nil {
		v := *b.ImageUrl
		v.ImageUrl = clonePtr(b.ImageUrl.ImageUrl)
		c.ImageUrl = &v
	}
	if b.InputAudio != nil {
		v := *b.InputAudio
		v.InputAudio = clonePtr(b.InputAudio.InputAudio)
		c.InputAudio = &v
	}
	if b.File != nil {
		v := *b.File
		v.File = clonePtr(b.File.File)
		c.File = &v
	}
	c.Toolcall = clonePtr(b.Toolcall)
	c.Toolresult = clonePtr(b.Toolresult)
	if b.Data != nil {
		v := *b.Data
		v.Data = cloneAny(b.Data.Data)
		c.Data = &v
	}
	return &c
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func cloneAnyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	return cloneAny(m).(map[string]any)
}

// cloneAny 拷一个 `any` 值（见 cloneCalleeInput 的「拷到哪一层」）。
func cloneAny(v any) any {
	if v == nil {
		return nil
	}
	return cloneValue(reflect.ValueOf(v), &cloneMemo{}).Interface()
}

// cloneKey 标识一个已经拷过的容器：同一个底层数组上的两个切片可以长度不同、互相包含，只按
// 指针（或把长度加到指针上）认会把它们认成一个。
type cloneKey struct {
	typ reflect.Type
	ptr uintptr
	len int
}

// cloneMemo 记下已经拷过的容器与它的拷贝（环与共享在拷贝里保持同样的形状）。
type cloneMemo struct {
	seen map[cloneKey]reflect.Value
}

func (m *cloneMemo) get(k cloneKey) (reflect.Value, bool) {
	c, ok := m.seen[k]
	return c, ok
}

func (m *cloneMemo) put(k cloneKey, c reflect.Value) {
	if m.seen == nil {
		m.seen = map[cloneKey]reflect.Value{}
	}
	m.seen[k] = c
}

// cloneValue 按原类型深拷 v（规则见 cloneCalleeInput）。返回值的类型与 v 相同。
func cloneValue(v reflect.Value, memo *cloneMemo) reflect.Value {
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return v
		}
		// 动态值拷完还是它自己（标量不可变、指针 / 函数 / channel 共享）时原样返回，免得每个
		// 标量都重新装箱一次（JSON 形的载荷里绝大多数叶子是它们）
		if k := v.Elem().Kind(); isScalarKind(k) || k == reflect.Pointer || k == reflect.Func ||
			k == reflect.Chan || k == reflect.UnsafePointer {
			return v
		}
		out := reflect.New(v.Type()).Elem()
		out.Set(cloneValue(v.Elem(), memo))
		return out
	case reflect.Slice:
		if v.IsNil() {
			return v
		}
		key := cloneKey{typ: v.Type(), ptr: v.Pointer(), len: v.Len()}
		if c, ok := memo.get(key); ok {
			return c
		}
		// 长度为 0 时 MakeSlice 给的也是非 nil 的空切片：空与 nil 在 JSON 里一个是 [] 一个是 null
		c := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		memo.put(key, c)
		if isScalarKind(v.Type().Elem().Kind()) {
			reflect.Copy(c, v)
			return c
		}
		for i := 0; i < v.Len(); i++ {
			c.Index(i).Set(cloneValue(v.Index(i), memo))
		}
		return c
	case reflect.Map:
		if v.IsNil() {
			return v
		}
		key := cloneKey{typ: v.Type(), ptr: v.Pointer()}
		if c, ok := memo.get(key); ok {
			return c
		}
		c := reflect.MakeMapWithSize(v.Type(), v.Len())
		memo.put(key, c)
		it := v.MapRange()
		for it.Next() {
			c.SetMapIndex(it.Key(), cloneValue(it.Value(), memo))
		}
		return c
	case reflect.Array:
		if isScalarKind(v.Type().Elem().Kind()) {
			return v // 数组是值，Set 时整体复制
		}
		c := reflect.New(v.Type()).Elem()
		for i := 0; i < v.Len(); i++ {
			c.Index(i).Set(cloneValue(v.Index(i), memo))
		}
		return c
	case reflect.Struct:
		c := reflect.New(v.Type()).Elem()
		c.Set(v)
		cloneExportedFields(c, v, memo)
		return c
	}
	// 标量原样；指针、函数、channel、unsafe 指针共享
	return v
}

// cloneExportedFields 把 src 的导出字段逐个深拷进 dst（dst 已是 src 的整体复制、可寻址）。嵌入的
// 结构体按值嵌入时递归进去：未导出的嵌入类型自己读写不了，但它下面的导出字段可以——reflect
// 给嵌入的未导出字段打的只读标记不往下传，跟 encoding/json 的可见性一致。
func cloneExportedFields(dst, src reflect.Value, memo *cloneMemo) {
	t := src.Type()
	for i := 0; i < src.NumField(); i++ {
		f := t.Field(i)
		switch {
		case f.IsExported():
			dst.Field(i).Set(cloneValue(src.Field(i), memo))
		case f.Anonymous && f.Type.Kind() == reflect.Struct:
			cloneExportedFields(dst.Field(i), src.Field(i), memo)
		}
	}
}

// isScalarKind 报这一种类的值是不是不可变的标量（原样共享即可）。
func isScalarKind(k reflect.Kind) bool {
	switch k {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return true
	}
	return false
}
