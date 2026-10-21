package sql_test

import (
	"fmt"
	"strings"
	"testing"

	agentSQL "crew-go/sql"
)

// recBinder 是测试用的 [agentSQL.ParamBinder]——记下每次 Bind 的实参并发占位符。
type recBinder struct {
	ph   func(n int) string
	args []any
}

func (r *recBinder) Bind(v any) string {
	r.args = append(r.args, v)
	return r.ph(len(r.args)) // 占位符编号 1-based（PG 的 $1 起）
}

func newRec(d agentSQL.Dialect) *recBinder { return &recBinder{ph: d.Placeholder} }

// TestDialects_JSONFieldEquals_Shapes 钉三个 dialect 的相等片段与**绑参顺序**。
// PG / MySQL 有原生 JSON 相等，六类同一形状；SQLite 没有，按类别三路（godoc 说明）。
//
// ⚠️ 字段名从「拼进 SQL 字面量」改成了「绑参」，所以这里同时钉 args：顺序错了就是静默
// 读错参数（`?` 是位置绑定）。
func TestDialects_JSONFieldEquals_Shapes(t *testing.T) {
	const val = `"x"`
	kinds := []agentSQL.JSONKind{
		agentSQL.JSONKindNull, agentSQL.JSONKindBool, agentSQL.JSONKindNumber,
		agentSQL.JSONKindString, agentSQL.JSONKindArray, agentSQL.JSONKindObject,
	}
	for _, k := range kinds {
		b := newRec(agentSQL.Postgres{})
		if got, want := (agentSQL.Postgres{}).JSONFieldEquals("value", "f", val, k, b), "value->$1 = $2::jsonb"; got != want {
			t.Errorf("PG %s:\n  want %q\n  got  %q", k, want, got)
		}
		assertArgs(t, "PG "+k.String(), b, "f", val)

		b = newRec(agentSQL.MySQL{})
		if got, want := (agentSQL.MySQL{}).JSONFieldEquals("value", "f", val, k, b), "JSON_EXTRACT(value, ?) = CAST(? AS JSON)"; got != want {
			t.Errorf("MySQL %s:\n  want %q\n  got  %q", k, want, got)
		}
		assertArgs(t, "MySQL "+k.String(), b, `$."f"`, val)
	}
	sqliteWant := map[agentSQL.JSONKind]struct {
		sql  string
		args []any
	}{
		agentSQL.JSONKindNumber: {"(json_type(value, ?) IN ('integer','real') AND json_extract(value, ?) = json_extract(?, '$'))", []any{`$."f"`, `$."f"`, val}},
		agentSQL.JSONKindBool:   {"json_type(value, ?) = json_type(?, '$')", []any{`$."f"`, val}},
		agentSQL.JSONKindNull:   {"json_type(value, ?) = json_type(?, '$')", []any{`$."f"`, val}},
		agentSQL.JSONKindString: {"json(value) -> ? = json(?)", []any{`$."f"`, val}},
		agentSQL.JSONKindArray:  {"json(value) -> ? = json(?)", []any{`$."f"`, val}},
		agentSQL.JSONKindObject: {"json(value) -> ? = json(?)", []any{`$."f"`, val}},
	}
	for k, w := range sqliteWant {
		b := newRec(agentSQL.SQLite{})
		if got := (agentSQL.SQLite{}).JSONFieldEquals("value", "f", val, k, b); got != w.sql {
			t.Errorf("SQLite %s:\n  want %q\n  got  %q", k, w.sql, got)
		}
		assertArgs(t, "SQLite "+k.String(), b, w.args...)
	}
}

// TestDialects_JSONFieldNumericCompare_Shapes 钉范围 op 片段：三个方言都带类型守卫，
// PG 必须是 CASE（planner 不保证 AND 短路），MySQL / SQLite 比较永不报错、AND 即可。
func TestDialects_JSONFieldNumericCompare_Shapes(t *testing.T) {
	cases := []struct {
		name string
		d    agentSQL.Dialect
		want string
		args []any
	}{
		{"pg", agentSQL.Postgres{},
			"(CASE WHEN jsonb_typeof(value->$1) = 'number' THEN (value->>$1)::numeric END) > $2::numeric",
			[]any{"age", int64(3)}},
		{"mysql", agentSQL.MySQL{},
			"(JSON_TYPE(JSON_EXTRACT(value, ?)) IN ('INTEGER','UNSIGNED INTEGER','DOUBLE','DECIMAL') AND JSON_EXTRACT(value, ?) > ?)",
			[]any{`$."age"`, `$."age"`, int64(3)}},
		{"sqlite", agentSQL.SQLite{},
			"(json_type(value, ?) IN ('integer','real') AND json_extract(value, ?) > ?)",
			[]any{`$."age"`, `$."age"`, int64(3)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := newRec(c.d)
			got := c.d.JSONFieldNumericCompare("value", "age", ">", int64(3), b)
			if got != c.want {
				t.Errorf("\n  want %q\n  got  %q", c.want, got)
			}
			assertArgs(t, c.name, b, c.args...)
			// 片段必须自带外层括号，才能安全地和别的条件 AND 在一起。
			if !strings.HasPrefix(got, "(") {
				t.Errorf("not embeddable: %q", got)
			}
		})
	}
	// PG 的守卫形态是 CASE 不是 AND——sanity: 把 CASE 改回 AND 这里红
	if got := (agentSQL.Postgres{}).JSONFieldNumericCompare("value", "age", "<", int64(1), newRec(agentSQL.Postgres{})); !strings.HasPrefix(got, "(CASE WHEN jsonb_typeof(") {
		t.Errorf("PG numeric compare must be a CASE guard, got %q", got)
	}
}

// TestDialects_PlaceholderCountMatchesBoundArgs 钉共享层与 dialect 之间的参数契约：
// **片段里的占位符个数必须等于它绑掉的实参个数**。
//
// 少绑 → driver 报 missing argument；多绑 → 后面所有条件的参数**整体错位**，而 `?` 是
// 位置绑定 ⟹ 静默比错行、err == nil。
//
// ⚠️ 这条比旧版的「恰好消费一次占位符」强：字段名改绑参之后，一个片段会消费 2~3 个
// 占位符（PG 复用 `$n` 时只绑一次、MySQL / SQLite 必须绑两次），旧断言已表达不了这个契约。
func TestDialects_PlaceholderCountMatchesBoundArgs(t *testing.T) {
	kinds := []agentSQL.JSONKind{
		agentSQL.JSONKindNull, agentSQL.JSONKindBool, agentSQL.JSONKindNumber,
		agentSQL.JSONKindString, agentSQL.JSONKindArray, agentSQL.JSONKindObject,
	}
	for _, dl := range []struct {
		name string
		d    agentSQL.Dialect
	}{
		{"pg", agentSQL.Postgres{}}, {"mysql", agentSQL.MySQL{}}, {"sqlite", agentSQL.SQLite{}},
	} {
		positional := dl.d.Placeholder(1) == dl.d.Placeholder(2) // `?` 型 vs `$n` 型
		check := func(what, frag string, b *recBinder) {
			t.Helper()
			if len(b.args) == 0 {
				t.Errorf("%s %s: 一个参数都没绑（字段名也该绑参）: %q", dl.name, what, frag)
				return
			}
			if positional {
				// `?` 是位置绑定：出现几次就得绑几个，多一个少一个都是整体错位。
				if n := strings.Count(frag, "?"); n != len(b.args) {
					t.Errorf("%s %s: SQL 里 %d 个 `?` 但绑了 %d 个参数（位置绑定，数量必须相等）: %q",
						dl.name, what, n, len(b.args), frag)
				}
				return
			}
			// `$n` 型：绑过的每个编号都必须在 SQL 里出现（PG 允许复用同一个 $n），
			// 且不许出现没绑过的下一个编号。
			for i := 1; i <= len(b.args); i++ {
				if pl := dl.d.Placeholder(i); !strings.Contains(frag, pl) {
					t.Errorf("%s %s: 绑了参数但 SQL 里没有它的占位符 %s: %q", dl.name, what, pl, frag)
				}
			}
			if extra := dl.d.Placeholder(len(b.args) + 1); strings.Contains(frag, extra) {
				t.Errorf("%s %s: SQL 用了没绑过的占位符 %s: %q", dl.name, what, extra, frag)
			}
		}
		for _, k := range kinds {
			b := newRec(dl.d)
			check("JSONFieldEquals/"+k.String(), dl.d.JSONFieldEquals("value", "f", `"x"`, k, b), b)
		}
		for _, op := range []string{">", ">=", "<", "<="} {
			b := newRec(dl.d)
			frag := dl.d.JSONFieldNumericCompare("value", "f", op, int64(1), b)
			check("JSONFieldNumericCompare/"+op, frag, b)
			if !strings.Contains(frag, " "+op+" ") {
				t.Errorf("%s JSONFieldNumericCompare(%s) lost the operator: %q", dl.name, op, frag)
			}
		}
		b := newRec(dl.d)
		check("JSONFieldExists", dl.d.JSONFieldExists("value", "f", b), b)
	}
}

// TestDialects_FieldNameIsBoundNotSpliced 钉「字段名绝不出现在 SQL 文本里」——这是
// 注入面消失的**结构性**保证，比「白名单拒掉了哪些坏字符」强得多。
//
// 旧实现把字段名 `fmt.Sprintf` 进字面量，靠一道 `^[a-zA-Z_][a-zA-Z0-9_]*$` 白名单兜底；
// 那道白名单确实承重（放宽成 `^.+$` 后三个引擎实测全部跨 namespace 越权读），但它把
// `user-id` / `a.b` / `用户` 这些合法 JSON 字段名一并拒了。绑参之后两个问题一起消失。
func TestDialects_FieldNameIsBoundNotSpliced(t *testing.T) {
	// 既有攻击串，也有此前被白名单误拒的合法字段名
	fields := []string{
		`x'; DROP TABLE crew_store; --`,
		`a" OR "1"="1`,
		"user-id", "a.b", "用户", "emoji🙂", "*", "", "has\"quote", `back\slash`,
	}
	for _, f := range fields {
		for _, dl := range []struct {
			name string
			d    agentSQL.Dialect
		}{
			{"pg", agentSQL.Postgres{}}, {"mysql", agentSQL.MySQL{}}, {"sqlite", agentSQL.SQLite{}},
		} {
			frags := []string{}
			b := newRec(dl.d)
			frags = append(frags, dl.d.JSONFieldExists("value", f, b))
			b2 := newRec(dl.d)
			frags = append(frags, dl.d.JSONFieldEquals("value", f, `"x"`, agentSQL.JSONKindString, b2))
			b3 := newRec(dl.d)
			frags = append(frags, dl.d.JSONFieldNumericCompare("value", f, ">", int64(1), b3))
			for _, frag := range frags {
				if f != "" && strings.Contains(frag, f) {
					t.Errorf("%s: 字段名 %q 出现在了 SQL 文本里（应当绑参）: %q", dl.name, f, frag)
				}
			}
		}
	}
}

// TestJSONFieldPath_QuotingIsLoadBearing 钉 MySQL / SQLite 侧 JSON path 的引号包裹。
//
// 这是字段名改绑参**换来的新承重件**：path 是 JSON path 语法，`*` 是通配符、`.` 是层级
// 分隔符。裸拼 `"$."+field` 时字段名 `*` 会匹配整行所有字段（真 MySQL 实测会把值全吐
// 出来）、`a.b` 会命中嵌套的 a→b 而不是字面字段。包上引号两者都失效（真 SQLite 实测：
// `$."*"` 返 0 行、`$."a.b"` 只命中字面字段）。
//
// 从绑定的实参上验，不从 SQL 文本上验——文本里现在只有 `?`。
func TestJSONFieldPath_QuotingIsLoadBearing(t *testing.T) {
	cases := map[string]string{
		"f":          `$."f"`,
		"a.b":        `$."a.b"`,
		"*":          `$."*"`,
		"user-id":    `$."user-id"`,
		"用户":         `$."用户"`,
		`has"quote`:  `$."has\"quote"`,
		`back\slash`: `$."back\\slash"`,
		"":           `$.""`,
	}
	for field, wantPath := range cases {
		for _, dl := range []struct {
			name string
			d    agentSQL.Dialect
		}{{"mysql", agentSQL.MySQL{}}, {"sqlite", agentSQL.SQLite{}}} {
			b := newRec(dl.d)
			dl.d.JSONFieldExists("value", field, b)
			if len(b.args) != 1 {
				t.Fatalf("%s JSONFieldExists(%q) 绑了 %d 个参数，应为 1", dl.name, field, len(b.args))
			}
			if got := b.args[0]; got != wantPath {
				t.Errorf("%s field %q:\n  want path %q\n  got       %q", dl.name, field, wantPath, got)
			}
		}
	}
	// PG 不走 path 语法（`->` 直接收字段名文本），绑的就是字段名本身
	b := newRec(agentSQL.Postgres{})
	(agentSQL.Postgres{}).JSONFieldExists("value", "a.b", b)
	if len(b.args) != 1 || b.args[0] != "a.b" {
		t.Errorf("PG 应当直接绑字段名，得到 %v", b.args)
	}
}

func assertArgs(t *testing.T, what string, b *recBinder, want ...any) {
	t.Helper()
	if len(b.args) != len(want) {
		t.Errorf("%s: 绑了 %d 个参数，应为 %d：%v", what, len(b.args), len(want), b.args)
		return
	}
	for i := range want {
		if fmt.Sprintf("%v", b.args[i]) != fmt.Sprintf("%v", want[i]) {
			t.Errorf("%s: 第 %d 个参数 = %v，应为 %v（顺序错了就是静默读错参数）", what, i, b.args[i], want[i])
		}
	}
}

// TestJSONKind_String 钉 JSONKind 的展示名（错误信息 / 日志用）。
func TestJSONKind_String(t *testing.T) {
	want := map[agentSQL.JSONKind]string{
		agentSQL.JSONKindNull: "null", agentSQL.JSONKindBool: "bool", agentSQL.JSONKindNumber: "number",
		agentSQL.JSONKindString: "string", agentSQL.JSONKindArray: "array", agentSQL.JSONKindObject: "object",
	}
	for k, w := range want {
		if k.String() != w {
			t.Errorf("JSONKind(%d).String() = %q, want %q", int(k), k.String(), w)
		}
	}
	if got := agentSQL.JSONKind(99).String(); got != "JSONKind(99)" {
		t.Errorf("unknown kind: %q", got)
	}
}
