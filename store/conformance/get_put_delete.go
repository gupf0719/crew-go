package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"crew-go/store"
)

// portableAlphabets 是长度上限用例的两档字符：ASCII 每个 1 字节；4 字节字符按 MySQL 的
// 字节列宽算正好顶满可移植上限。
var portableAlphabets = []struct{ name, fill, last1, last2 string }{
	{"ASCII", "k", "1", "2"},
	{"4 字节字符", "😀", "😁", "😂"},
}

// testGetPutDelete 验证 Get/Put/Delete 基础 CRUD + 边界语义.
func testGetPutDelete(t *testing.T, factory StoreFactory) {
	t.Run("Put_Then_Get_Roundtrip", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("users", "u1", "memories")

		if err := st.Put(ctx, ns, "k1", "hello"); err != nil {
			t.Fatalf("Put: %v", err)
		}
		item, err := st.Get(ctx, ns, "k1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if item == nil {
			t.Fatal("expected item, got nil")
		}
		if item.Value != "hello" {
			t.Errorf("expected value 'hello', got %v", item.Value)
		}
		if !item.Namespace.Equal(ns) {
			t.Errorf("expected namespace %v, got %v", ns, item.Namespace)
		}
		if item.Key != "k1" {
			t.Errorf("expected key 'k1', got %q", item.Key)
		}
		if item.CreatedAt.IsZero() || item.UpdatedAt.IsZero() {
			t.Errorf("expected CreatedAt + UpdatedAt non-zero, got %v / %v",
				item.CreatedAt, item.UpdatedAt)
		}
	})

	t.Run("Get_NonExistent_Returns_Nil_Item_Nil_Err", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("nonexistent")

		item, err := st.Get(ctx, ns, "no_such_key")
		if err != nil {
			t.Fatalf("Get non-existent should return (nil, nil), got err: %v", err)
		}
		if item != nil {
			t.Errorf("Get non-existent should return nil item, got %+v", item)
		}
	})

	// audit iter-3 视角 3 H2: 业务 PG `TIMESTAMP WITHOUT TIME ZONE` 错时区也 PASS.
	// PG 应用 `TIMESTAMP WITH TIME ZONE` 避此坑. 业务 PG dev/prod TZ 不同 → 跨实例
	// 读 UpdatedAt 错 8h → TTL sweeper 提前/延后触发. 加 UTC location 协议测试.
	t.Run("Timestamps_Use_UTC_Location", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("tz_check")

		beforePut := time.Now().UTC()
		if err := st.Put(ctx, ns, "k", "v"); err != nil {
			t.Fatalf("Put: %v", err)
		}
		item, _ := st.Get(ctx, ns, "k")
		if item == nil {
			t.Fatal("expected item non-nil")
		}
		afterPut := time.Now().UTC()

		// CreatedAt / UpdatedAt 必须用 UTC location (业务 backend 用 PG
		// TIMESTAMP 无 TZ 时反序列化默认 Local → 跨机器时区错)
		if item.CreatedAt.Location().String() != "UTC" {
			t.Errorf("CreatedAt should be UTC location, got %q (业务 backend 用 PG TIMESTAMP 无 TZ 字段会反序列化为 Local)",
				item.CreatedAt.Location().String())
		}
		if item.UpdatedAt.Location().String() != "UTC" {
			t.Errorf("UpdatedAt should be UTC location, got %q",
				item.UpdatedAt.Location().String())
		}

		// 时间戳应该在 [beforePut, afterPut] 范围内 — 错时区 backend 会偏 N 小时
		if item.CreatedAt.Before(beforePut.Add(-1*time.Second)) ||
			item.CreatedAt.After(afterPut.Add(1*time.Second)) {
			t.Errorf("CreatedAt %v 偏出合理范围 [%v, %v] — 业务 backend 时区偏移 (e.g., dev UTC vs prod CST 差 8h)",
				item.CreatedAt, beforePut, afterPut)
		}
	})

	t.Run("Put_Overwrite_Updates_Value_And_UpdatedAt", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("a")

		if err := st.Put(ctx, ns, "k", "v1"); err != nil {
			t.Fatalf("Put v1: %v", err)
		}
		first, _ := st.Get(ctx, ns, "k")
		// audit 视角 3 H4: nil-deref panic vs t.Fatal — 业务 backend Put 后 Get
		// 返 nil 时 first.CreatedAt 直接 panic 信息含糊; t.Fatal 精确指向 bug
		if first == nil {
			t.Fatal("Put 后 Get 应非 nil (业务 backend Put 实现可能 silent skip)")
		}
		createdAt := first.CreatedAt

		// audit 视角 2 L4: UpdatedAt strict After — 业务 backend 时钟精度不足 (秒级)
		// 两次 Put 同 tick 内 UpdatedAt == CreatedAt 也 PASS. 加 1ms sleep 跨 tick.
		time.Sleep(time.Millisecond)

		if err := st.Put(ctx, ns, "k", "v2"); err != nil {
			t.Fatalf("Put v2: %v", err)
		}
		second, _ := st.Get(ctx, ns, "k")
		if second == nil {
			t.Fatal("Put v2 后 Get 应非 nil")
		}
		if second.Value != "v2" {
			t.Errorf("expected overwritten value 'v2', got %v", second.Value)
		}
		// CreatedAt 不变, UpdatedAt 严格 advance
		if !second.CreatedAt.Equal(createdAt) {
			t.Errorf("CreatedAt should be preserved after Put overwrite, got %v vs %v",
				second.CreatedAt, createdAt)
		}
		if !second.UpdatedAt.After(createdAt) {
			t.Errorf("UpdatedAt should strictly advance after Put (cross-tick), got %v vs initial %v",
				second.UpdatedAt, createdAt)
		}
	})

	t.Run("Delete_Existing_Removes_Item", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("a")

		_ = st.Put(ctx, ns, "k", "v")
		if err := st.Delete(ctx, ns, "k"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		item, _ := st.Get(ctx, ns, "k")
		if item != nil {
			t.Errorf("expected nil after Delete, got %+v", item)
		}
	})

	t.Run("Delete_NonExistent_Idempotent_No_Error", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("a")

		// Delete 不存在的 key 不报错 (幂等)
		if err := st.Delete(ctx, ns, "no_such_key"); err != nil {
			t.Errorf("Delete non-existent should be idempotent (no error), got: %v", err)
		}
	})

	t.Run("Put_Nil_Value_Equivalent_To_Delete", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("a")

		_ = st.Put(ctx, ns, "k", "v")
		// Put nil value 应等价 Delete
		if err := st.Put(ctx, ns, "k", nil); err != nil {
			t.Fatalf("Put nil: %v", err)
		}
		item, _ := st.Get(ctx, ns, "k")
		if item != nil {
			t.Errorf("expected nil after Put(nil), got %+v", item)
		}
	})

	t.Run("Value_Types_Roundtrip", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("types")

		// audit 视角 2 H4: 不能只验 `item != nil` (5 类炸弹 #1 变种 — 业务 backend
		// 把所有 value 都存空 string 也 PASS). 拆 2 类:
		// 1) primitive (string/bool/empty string) — 强制 deep equal, 业务 backend
		//    JSON 不转换, crew-go 协议要求保留
		// 2) numeric (int/float) — 用 fmt.Sprintf 比较, 业务 JSON unmarshal 可能
		//    全转 float64 (json.Number 默认行为), 字符串化对比兼容
		// 3) 复合 (slice/map/struct) — 读回的是值的 JSON 形状（store.NormalizeValue）：按 store.ValueEqual
		//    比内容，struct 读回是以 JSON 字段名为键的 map[string]any
		t.Run("Primitive_StrictEqual", func(t *testing.T) {
			primitives := map[string]any{
				"string":       "hello",
				"empty_string": "",
				"bool_true":    true,
				"bool_false":   false,
			}
			for key, want := range primitives {
				if err := st.Put(ctx, ns, key, want); err != nil {
					t.Errorf("Put(%s): %v", key, err)
					continue
				}
				item, err := st.Get(ctx, ns, key)
				if err != nil || item == nil {
					t.Errorf("Get(%s): item=%v err=%v", key, item, err)
					continue
				}
				if item.Value != want {
					t.Errorf("Get(%s): expected %v (%T), got %v (%T)",
						key, want, want, item.Value, item.Value)
				}
			}
		})
		t.Run("Numeric_StringifiedEqual", func(t *testing.T) {
			// 业务 JSON backend int → float64 转换合规, 用 Sprintf 比较容忍
			numerics := map[string]any{
				"int":   42,
				"float": 3.14,
				"zero":  0,
			}
			for key, want := range numerics {
				if err := st.Put(ctx, ns, key, want); err != nil {
					t.Errorf("Put(%s): %v", key, err)
					continue
				}
				item, _ := st.Get(ctx, ns, key)
				if item == nil {
					t.Errorf("Get(%s): nil", key)
					continue
				}
				// 通过 fmt.Sprintf("%v") 比较 — int(42) / float64(42) / 都
				// 字符串化成 "42"; float 浮点字符串可能差异但 3.14 不会
				// `%v` 本身**就是**那道容差：int(42) 与 float64(42) 与 json.Number("42")
				// 都字符串化成 "42"，所以「JSON backend 把 int 变 float64」不会误红。
				// ⚠️ 曾经这里把 t.Errorf 又套了一层 `if gotStr == ""`，于是只有「后端返空
				// 串」才报错——**存 42 读回 "hello" 照样 PASS**，而这条用例名里带 Equal。
				// 那是「断言选在不承重的维度上」的教科书形态，也是几条数值往返 bug 能活
				// 到今天的直接原因。
				gotStr := fmt.Sprintf("%v", item.Value)
				wantStr := fmt.Sprintf("%v", want)
				if gotStr != wantStr {
					t.Errorf("Get(%s): 值没原样返回——want %q, got %q（%T）", key, wantStr, gotStr, item.Value)
				}
			}
		})
		t.Run("Composite_ContentEqual", func(t *testing.T) {
			composites := map[string]any{
				"slice":      []any{1, 2, 3},
				"map":        map[string]any{"a": 1, "b": "two"},
				"map_string": map[string]string{"z": "1"},
				"nested":     map[string]any{"tags": []string{"x", "y"}, "meta": map[string]any{"n": 2.5}},
			}
			for key, val := range composites {
				if err := st.Put(ctx, ns, key, val); err != nil {
					t.Errorf("Put(%s): %v", key, err)
					continue
				}
				item, err := st.Get(ctx, ns, key)
				if err != nil || item == nil {
					t.Errorf("Get(%s): item=%v err=%v", key, item, err)
					continue
				}
				want, err := store.NormalizeValue(val)
				if err != nil {
					t.Fatalf("NormalizeValue(%s): %v", key, err)
				}
				if !store.ValueEqual(item.Value, want) {
					t.Errorf("Get(%s): 读回 %v（%T），want 它的 JSON 形状 %v", key, item.Value, item.Value, want)
				}
			}
		})
		// struct 是业务写长期记忆最自然的形状之一：读回是以 JSON 字段名为键的 map[string]any（跟持久后端读回的一样）。
		// 曾经内存后端原样存 struct、读回还是 struct，同一份数据换后端读回的类型就变了。
		t.Run("Struct_ReadBackAsJSONObject", func(t *testing.T) {
			type memo struct {
				Kind  string   `json:"kind"`
				Count int      `json:"count"`
				Tags  []string `json:"tags"`
				Note  string   // 没有 tag：键是字段名
			}
			if err := st.Put(ctx, ns, "struct", memo{Kind: "pref", Count: 3, Tags: []string{"a"}, Note: "n"}); err != nil {
				t.Fatalf("Put struct: %v", err)
			}
			item, err := st.Get(ctx, ns, "struct")
			if err != nil || item == nil {
				t.Fatalf("Get struct: item=%v err=%v", item, err)
			}
			obj, ok := item.Value.(map[string]any)
			if !ok {
				t.Fatalf("struct 读回应是 map[string]any，got %T", item.Value)
			}
			want := map[string]any{"kind": "pref", "count": 3, "tags": []any{"a"}, "Note": "n"}
			if !store.ValueEqual(obj, want) {
				t.Errorf("struct 读回 %v，want %v", obj, want)
			}
		})

		// 不合法的 UTF-8 字节编成 JSON 时换成 U+FFFD，读回的是换过的那份——持久后端经 JSON 落盘本来如此，内存后端
		// 归一化时同样换（合法字符串的往返是恒等、可以跳过编解码，不合法的不行）。
		t.Run("String_InvalidUTF8_ReadBackReplaced", func(t *testing.T) {
			if err := st.Put(ctx, ns, "bad_utf8", "a\xffb"); err != nil {
				t.Fatalf("Put: %v", err)
			}
			item, err := st.Get(ctx, ns, "bad_utf8")
			if err != nil || item == nil {
				t.Fatalf("Get: item=%v err=%v", item, err)
			}
			if got, want := item.Value, "a�b"; got != want {
				t.Errorf("读回 %q，want %q", got, want)
			}
		})
	})

	// 只有无类型的 nil 等于 Delete。编成 JSON 是 null 的值（typed nil 指针 / slice / map、json.RawMessage("null")、
	// MarshalJSON 返回 null 的）是一个值：存一条 Value 为 nil 的记录，覆盖写时旧值换成它、记录还在。曾经示例后端先
	// 归一化再判 nil，把它们当成 Delete、覆盖写时删掉了旧记录（别的后端存一条 nil 记录），两边 `err == nil`。
	t.Run("Put_Value_Encoding_To_JSON_Null_Stores_Null", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("jsonnull")
		for _, c := range []struct {
			key string
			val any
		}{
			{"typed_nil_ptr", (*int)(nil)},
			{"nil_slice", []int(nil)},
			{"nil_map", map[string]int(nil)},
			{"raw_null", json.RawMessage("null")},
			{"marshaler_null", nullMarshaler{}},
		} {
			if err := st.Put(ctx, ns, c.key, "v1"); err != nil {
				t.Fatalf("Put %s v1: %v", c.key, err)
			}
			if err := st.Put(ctx, ns, c.key, c.val); err != nil {
				t.Fatalf("Put %s: %v", c.key, err)
			}
			item, err := st.Get(ctx, ns, c.key)
			if err != nil {
				t.Fatalf("Get %s: %v", c.key, err)
			}
			if item == nil {
				t.Errorf("%s：覆盖写之后记录没了（被当成了 Delete）", c.key)
				continue
			}
			if item.Value != nil {
				t.Errorf("%s：Value 应读回 nil（JSON null），got %#v", c.key, item.Value)
			}
		}
	})

	// 「Value 必须能编成 JSON」在每个后端上都强制：编不成的值 Put 报错、什么都不存。曾经内存后端照收，换到持久后端
	// 才失败。
	t.Run("Put_Unencodable_Value_Rejected", func(t *testing.T) {
		st := factory()
		ctx := context.Background()
		ns := store.NS("unencodable")
		for _, c := range []struct {
			key string
			val any
		}{
			{"nan", math.NaN()},
			{"inf", math.Inf(1)},
			{"chan", map[string]any{"c": make(chan int)}},
			{"func", map[string]any{"f": func() {}}},
		} {
			if err := st.Put(ctx, ns, c.key, c.val); err == nil {
				t.Errorf("Put(%s) 应报错（编不成 JSON），got nil", c.key)
			}
			if item, err := st.Get(ctx, ns, c.key); err != nil || item != nil {
				t.Errorf("Put(%s) 失败后不该存下任何东西，Get 得 item=%v err=%v", c.key, item, err)
			}
		}
	})

	// key 的**可移植长度**是 255 字符——见 [store.Store] 的 Put godoc：最窄的后端是 sql 的
	// MySQL（`data_key VARBINARY(1020)`，按最宽的 4 字节字符算正好 255 个；与 ns 同进主键、
	// 贴着 InnoDB 3072 字节上限）。
	//
	// ⚠️ 这条钉的不是「能存多长」，是**「不许静默塌成一行」**：两个只在末尾不同的长 key
	// 必须各自读回各自的值。MySQL 非 STRICT 模式超限会静默截断，那时 Get 返回的是别人的
	// 数据、err 还是 nil——业务 key 来自外部输入时这是一条真实的串数据路径。
	//
	// 两档字符各跑一遍：ASCII 每个 1 字节；4 字节字符（😀）按 MySQL 的字节列宽算正好顶满——
	// 列宽照「255 字符」写成 255 字节的话，ASCII 那档照样绿，只有这一档会红。
	t.Run("Long_Keys_At_Portable_Limit_Do_Not_Collide", func(t *testing.T) {
		for _, a := range portableAlphabets {
			t.Run(a.name, func(t *testing.T) {
				st := factory()
				ctx := context.Background()
				ns := store.NS("longkey")

				const portableKeyLen = 255
				prefix := strings.Repeat(a.fill, portableKeyLen-1)
				k1, k2 := prefix+a.last1, prefix+a.last2 // 只有最后一个字符不同
				if n1, n2 := utf8.RuneCountInString(k1), utf8.RuneCountInString(k2); n1 != portableKeyLen || n2 != portableKeyLen {
					t.Fatalf("fixture 没搭对：key 长度应为 %d 字符，实际 %d/%d", portableKeyLen, n1, n2)
				}

				if err := st.Put(ctx, ns, k1, "v1"); err != nil {
					t.Fatalf("Put k1 (%d 字符, %d 字节): %v", portableKeyLen, len(k1), err)
				}
				if err := st.Put(ctx, ns, k2, "v2"); err != nil {
					t.Fatalf("Put k2 (%d 字符, %d 字节): %v", portableKeyLen, len(k2), err)
				}
				for _, c := range []struct{ label, key, want string }{{"k1", k1, "v1"}, {"k2", k2, "v2"}} {
					it, err := st.Get(ctx, ns, c.key)
					if err != nil {
						t.Fatalf("Get %s: %v", c.label, err)
					}
					if it == nil {
						t.Fatalf("Get %s 返 nil——%d 字符（%d 字节）的 key 存不住", c.label, portableKeyLen, len(c.key))
					}
					if it.Value != c.want {
						t.Errorf("Get %s 拿到 %v，期望 %q——两个 key 塌成一行了（静默截断）", c.label, it.Value, c.want)
					}
				}
			})
		}
	})

	// namespace 全路径的可移植长度是 512 字符（MySQL `ns VARBINARY(2048)`），钉法同上一条。
	t.Run("Long_Namespace_At_Portable_Limit_Do_Not_Collide", func(t *testing.T) {
		for _, a := range portableAlphabets {
			t.Run(a.name, func(t *testing.T) {
				st := factory()
				ctx := context.Background()

				const portableNSLen = 512
				seg := strings.Repeat(a.fill, portableNSLen-1)
				ns1, ns2 := store.NS(seg+a.last1), store.NS(seg+a.last2) // 只有最后一个字符不同
				if n1, n2 := utf8.RuneCountInString(ns1.String()), utf8.RuneCountInString(ns2.String()); n1 != portableNSLen || n2 != portableNSLen {
					t.Fatalf("fixture 没搭对：namespace 长度应为 %d 字符，实际 %d/%d", portableNSLen, n1, n2)
				}

				if err := st.Put(ctx, ns1, "k", "v1"); err != nil {
					t.Fatalf("Put ns1 (%d 字符, %d 字节): %v", portableNSLen, len(ns1.String()), err)
				}
				if err := st.Put(ctx, ns2, "k", "v2"); err != nil {
					t.Fatalf("Put ns2 (%d 字符, %d 字节): %v", portableNSLen, len(ns2.String()), err)
				}
				for _, c := range []struct {
					label string
					ns    store.Namespace
					want  string
				}{{"ns1", ns1, "v1"}, {"ns2", ns2, "v2"}} {
					it, err := st.Get(ctx, c.ns, "k")
					if err != nil {
						t.Fatalf("Get %s: %v", c.label, err)
					}
					if it == nil {
						t.Fatalf("Get %s 返 nil——%d 字符（%d 字节）的 namespace 存不住", c.label, portableNSLen, len(c.ns.String()))
					}
					if it.Value != c.want {
						t.Errorf("Get %s 拿到 %v，期望 %q——两个 namespace 塌成一行了（静默截断）", c.label, it.Value, c.want)
					}
				}
			})
		}
	})

	t.Run("Empty_Namespace_Returns_Error", func(t *testing.T) {
		st := factory()
		ctx := context.Background()

		err := st.Put(ctx, store.Namespace{}, "k", "v")
		if err == nil {
			t.Error("Put with empty namespace should return error")
		}
		if !errors.Is(err, store.ErrEmptyNamespace) && !errors.Is(err, store.ErrInvalidNamespace) {
			t.Errorf("expected ErrEmptyNamespace or ErrInvalidNamespace, got %v", err)
		}
	})
}

// nullMarshaler 的 JSON 编码是 null。
type nullMarshaler struct{}

func (nullMarshaler) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
