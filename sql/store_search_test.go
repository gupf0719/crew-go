package sql_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	agentSQL "crew-go/sql"
	"crew-go/store"
)

// TestStore_Search_TypeMismatch_Returns_Sentinel 验业务 `$gt with string`
// 返 `store.ErrFilterTypeMismatch` sentinel (跟 InMemoryStore 协议对齐).
//
// **audit iter-5 视角 1 H3 + 视角 4 修**: 原 sql.Store $gt with string 跑 SQL
// 失败返 driver err (PG `cast numeric fail`), 业务 errors.Is(ErrFilterTypeMismatch)
// 失败. 跨 backend 行为漂移.
//
// **sanity-revert 验证**: sentinel 现在由 Search 入口的 `store.ValidateFilter` 静态返
// (buildFilterSQL 里 numericArg 那道只是兜底防御, 不是契约点). 删掉入口那次
// ValidateFilter 调用**并**去掉兜底 → 此测试 fail: 字符串参数被拿去绑 `$n::numeric`,
// sqlmock 返不匹配的 err 而不是 ErrFilterTypeMismatch.
func TestStore_Search_TypeMismatch_Returns_Sentinel(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	// 业务 $gt with string — 应在 buildFilterSQL 拦截返 ErrFilterTypeMismatch
	// 不走 SQL exec.
	_, err = st.Search(context.Background(), store.NS("test"),
		store.WithFilter(map[string]any{
			"x": map[string]any{"$gt": "abc"}, // string 不是 numeric
		}),
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, store.ErrFilterTypeMismatch) {
		t.Errorf("expected ErrFilterTypeMismatch, got %v", err)
	}
}

// TestStore_Search_TypeMismatch_All_Numeric_Ops 验 4 数值 op ($gt/$gte/$lt/$lte)
// with string 都返 ErrFilterTypeMismatch.
func TestStore_Search_TypeMismatch_All_Numeric_Ops(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	for _, op := range []string{"$gt", "$gte", "$lt", "$lte"} {
		t.Run(op, func(t *testing.T) {
			_, err := st.Search(context.Background(), store.NS("test"),
				store.WithFilter(map[string]any{
					"x": map[string]any{op: "string_val"},
				}),
			)
			if !errors.Is(err, store.ErrFilterTypeMismatch) {
				t.Errorf("%s with string should return ErrFilterTypeMismatch, got %v", op, err)
			}
		})
	}
}

// TestStore_Search_FieldName_IsBoundNeverSpliced 验 filter 字段名**一律绑参、绝不进
// SQL 文本**——注入面从「靠白名单拦坏字符」变成「结构上不存在」。
//
// # 这条替代了什么
//
// 原先字段名 `fmt.Sprintf` 进字面量（`col->'key'` / `'$.key'`），靠一道
// `^[a-zA-Z_][a-zA-Z0-9_]*$` 白名单兜底（那个 validateIdentifier 已随本次改动删除），本用例断言的是「返 invalid identifier」。
// 那道白名单确实承重（审计把它放宽成 `^.+$` 后三个引擎实测**全部跨 namespace 越权读**），
// 但它拦错了东西：字段名进 SQL 时是**字符串字面量 / JSON path 实参**不是标识符，于是
// `user-id` / `a.b` / `用户` 这些完全合法的 JSON 字段名被一并拒掉，而它们在 in-memory /
// redis 上照常匹配——同一个 filter 换个后端从「有结果」变成「报错」。
//
// 绑参之后两个问题一起消失，断言也随之变强：不再问「坏字符拦住没有」，而是问
// **「字段名有没有出现在 SQL 文本里」**——后者是结构性的，不依赖白名单的完备性。
//
// **sanity-revert 验证**：把任一 dialect 的 JSONField* 改回 `fmt.Sprintf` 拼字段名 →
// 此测试红在「字段名出现在了 SQL 文本里」，并打出拼好的完整恶意 SQL。
func TestStore_Search_FieldName_IsBoundNeverSpliced(t *testing.T) {
	// 既有攻击串，也有此前被白名单误拒、业务真会用的合法字段名
	fields := []string{
		"x'; DROP TABLE crew_store; --",
		"x' OR '1'='1",
		"x--comment",
		"x/*comment*/",
		"user-id", "a.b", "用户", "*",
	}
	// ⚠️ **两种 filter 形态都要测**：直值 `{k: v}` 与 op-map `{k: {$eq: v}}` 是两条独立
	// 的代码路径。曾经只测了 op-map，而当时直值那条同样把 key 拼进了 SQL。
	forms := map[string]func(string) map[string]any{
		"direct":   func(k string) map[string]any { return map[string]any{k: "value"} },
		"op_map":   func(k string) map[string]any { return map[string]any{k: map[string]any{"$eq": "value"}} },
		"ne":       func(k string) map[string]any { return map[string]any{k: map[string]any{"$ne": "value"}} },
		"range_op": func(k string) map[string]any { return map[string]any{k: map[string]any{"$gt": 1}} },
	}
	for _, field := range fields {
		for formName, mk := range forms {
			t.Run(field+"/"+formName, func(t *testing.T) {
				db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(
					sqlmock.QueryMatcherFunc(func(expectedSQL, actualSQL string) error {
						// 截下真正发出去的 SQL：字段名一个字都不许出现在里面
						if strings.Contains(actualSQL, field) {
							return fmt.Errorf("字段名 %q 出现在了 SQL 文本里（应当绑参）：%s", field, actualSQL)
						}
						return nil
					})))
				if err != nil {
					t.Fatalf("sqlmock: %v", err)
				}
				defer db.Close()
				mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows(
					[]string{"ns", "data_key", "value", "created_at", "updated_at"}))

				st := agentSQL.NewStore(db, agentSQL.Postgres{})
				if _, err := st.Search(context.Background(), store.NS("test"),
					store.WithFilter(mk(field))); err != nil {
					t.Errorf("字段名 %q（%s 形态）应当被正常接受（绑参），得到: %v", field, formName, err)
				}
			})
		}
	}
}

// TestStore_Search_Int64_Filter_Goes_To_Numeric_Path 验业务 int64 (Snowflake ID
// / timestamp_ms) $gt 走数值比较路径不返 ErrFilterTypeMismatch.
func TestStore_Search_Int64_Filter_Goes_To_Numeric_Path(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	mock.ExpectQuery(`SELECT ns, data_key, value, created_at, updated_at FROM`).
		WillReturnRows(sqlmock.NewRows([]string{"ns", "data_key", "value", "created_at", "updated_at"}))

	_, err := st.Search(context.Background(), store.NS("test"),
		store.WithFilter(map[string]any{
			"age": map[string]any{"$gt": int64(18)},
		}),
	)
	if errors.Is(err, store.ErrFilterTypeMismatch) {
		t.Error("int64 should go to numeric path, got ErrFilterTypeMismatch")
	}
}

// TestStore_Search_JSONNumber_Filter_Binds_Numeric 验 json.Number filter 值 (业务
// `json.Decoder.UseNumber()` 路径) 走数值路径且按 int64 绑定——它是 string 底层类型,
// 不转的话 driver 会按文本绑, `numeric > text` 在 PG 上直接报错; 之前 sql 后端对它返
// ErrFilterTypeMismatch 而 in-memory / redis 接受, 跨后端漂.
func TestStore_Search_JSONNumber_Filter_Binds_Numeric(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	mock.ExpectQuery(regexp.QuoteMeta(`> $4::numeric`)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "id", int64(9007199254740993)).
		WillReturnRows(sqlmock.NewRows([]string{"ns", "data_key", "value", "created_at", "updated_at"}))

	_, err := st.Search(context.Background(), store.NS("test"),
		store.WithFilter(map[string]any{
			"id": map[string]any{"$gt": json.Number("9007199254740993")},
		}),
	)
	if err != nil {
		t.Errorf("json.Number filter: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("param binding: %v", err)
	}
}

// TestStore_Search_Range_Op_Generates_Type_Guarded_Numeric_Compare 验范围 op 生成的
// PG SQL 是 CASE + jsonb_typeof 守卫 + numeric cast——不是裸 `(value->>'age')::numeric`
// (裸 cast 在字符串行上报 22P02, 数字字符串还被静默 cast), 也不是 AND 守卫 (PG 不保证
// 求值顺序, 真库实测 planner 换 cost 后翻车).
func TestStore_Search_Range_Op_Generates_Type_Guarded_Numeric_Compare(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	mock.ExpectQuery(regexp.QuoteMeta(`(CASE WHEN jsonb_typeof(value->$3) = 'number' THEN (value->>$3)::numeric END) > $4::numeric`)).
		WillReturnRows(sqlmock.NewRows([]string{"ns", "data_key", "value", "created_at", "updated_at"}))

	_, err := st.Search(context.Background(), store.NS("test"),
		store.WithFilter(map[string]any{
			"age": map[string]any{"$gt": int64(18)},
		}),
	)
	if err != nil {
		t.Errorf("Search: %v", err)
	}
}

// TestStore_Search_TopLevel_Exact_Match_Goes_JSONFieldEquals 验**顶级 key 直值**路径
// (`{"tag": "important"}`) 走 jsonb 相等 `value->$3 = $4::jsonb`, $3 = 字段名 / $4 = 值的 JSON 文本.
//
// 不再是 `@>` containment: 那是包含语义 (`{"tags": ["a"]}` 命中 `["a","b"]`), 与 in-memory
// 的相等语义不一致; 现在直值与 `$eq` 是同一条路.
func TestStore_Search_TopLevel_Exact_Match_Goes_JSONFieldEquals(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	mock.ExpectQuery(regexp.QuoteMeta(`value->$3 = $4::jsonb`)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "tag", `"important"`).
		WillReturnRows(sqlmock.NewRows([]string{"ns", "data_key", "value", "created_at", "updated_at"}))

	_, err := st.Search(context.Background(), store.NS("test"),
		store.WithFilter(map[string]any{
			"tag": "important",
		}),
	)
	if err != nil {
		t.Errorf("Search exact match: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("param must be the JSON text of the value: %v", err)
	}
}

// TestStore_Search_Eq_Op_Map_Same_As_Direct_Value 验 `{"tag": {"$eq": v}}` 与直值走同一
// 条路 (同一段 SQL、同一个参数形态)——之前 op-map `$eq` 走 `->>` 文本比较, `$eq "10"`
// 会命中数字 10, 数值 `$eq` 还会 cast 字符串行报错.
func TestStore_Search_Eq_Op_Map_Same_As_Direct_Value(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	mock.ExpectQuery(regexp.QuoteMeta(`value->$3 = $4::jsonb`)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "tag", `10`).
		WillReturnRows(sqlmock.NewRows([]string{"ns", "data_key", "value", "created_at", "updated_at"}))

	_, err := st.Search(context.Background(), store.NS("test"),
		store.WithFilter(map[string]any{
			"tag": map[string]any{"$eq": 10},
		}),
	)
	if err != nil {
		t.Errorf("Search $eq op map: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("numeric $eq must bind JSON text `10`, not cast the column: %v", err)
	}
}

// TestStore_Search_Ne_Op_Is_Exists_And_Not_Equals 验 `$ne` = 字段存在 AND NOT 相等——
// 字段缺失不命中 (与 in-memory `!exists → false` 一致), JSON null 算存在.
// 之前走 `value->>'status' != $3` 文本比较: null 行因 SQL NULL 被丢, 数字行被文本命中.
func TestStore_Search_Ne_Op_Is_Exists_And_Not_Equals(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	mock.ExpectQuery(regexp.QuoteMeta(`(value ? $3 AND NOT (value->$4 = $5::jsonb))`)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "status", "status", `"completed"`).
		WillReturnRows(sqlmock.NewRows([]string{"ns", "data_key", "value", "created_at", "updated_at"}))

	_, err := st.Search(context.Background(), store.NS("test"),
		store.WithFilter(map[string]any{
			"status": map[string]any{"$ne": "completed"},
		}),
	)
	if err != nil {
		t.Errorf("Search $ne: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("$ne shape: %v", err)
	}
}

// TestStore_Search_Nested_Filter_Rejected 验 op map 里出现非 `$` 键 (嵌套 filter
// `{"meta": {"sub": 1}}`) 返 ErrUnsupportedFilterOp——in-memory 会按嵌套部分匹配, sql 不
// 支持, 与其静默给不同结果不如拒绝 (capability 矩阵标 ❌).
func TestStore_Search_Nested_Filter_Rejected(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	_, err := st.Search(context.Background(), store.NS("test"),
		store.WithFilter(map[string]any{
			"meta": map[string]any{"sub": 1},
		}),
	)
	if !errors.Is(err, store.ErrUnsupportedFilterOp) {
		t.Errorf("nested filter: expected ErrUnsupportedFilterOp, got %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "nested filter") {
		t.Errorf("error should say it is the nested form that is unsupported, got %v", err)
	}
}

// TestStore_Search_Filter_SQL_Is_Deterministic 验多键 filter 生成的 SQL 与参数顺序按键
// 字典序恒定——map 遍历随机会让同一个 filter 每次生成不同 SQL (prepared statement cache
// 失效, 测试也钉不住).
func TestStore_Search_Filter_SQL_Is_Deterministic(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	for i := 0; i < 5; i++ {
		// ⚠️ key `b` 上**故意放两个 op**：`buildFilterSQL` 里有两处 sort——key 的和
		// 同一 key 内 op 名的。两处服务同一句承诺（同一 filter 恒定生成同一条 SQL，
		// prepared statement cache 才命中得了），但只有 key 那处曾被钉住：删掉 op 排序后
		// 全仓 + 三个真库 conformance 跑 5 遍全绿，而 sqlmock 截下的 SQL 实测有 4 种形状。
		// 排序后 op 名的序是 `$gt` < `$lte`。
		mock.ExpectQuery(regexp.QuoteMeta(`value->$3 = $4::jsonb AND (CASE WHEN jsonb_typeof(value->$5) = 'number' THEN (value->>$5)::numeric END) > $6::numeric AND (CASE WHEN jsonb_typeof(value->$7) = 'number' THEN (value->>$7)::numeric END) <= $8::numeric AND value->$9 = $10::jsonb`)).
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "a", `"x"`, "b", 1, "b", 9, "c", `true`).
			WillReturnRows(sqlmock.NewRows([]string{"ns", "data_key", "value", "created_at", "updated_at"}))
		_, err := st.Search(context.Background(), store.NS("test"),
			store.WithFilter(map[string]any{
				"c": true,
				"a": "x",
				"b": map[string]any{"$gt": 1, "$lte": 9},
			}),
		)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("SQL / arg order must be stable across calls: %v", err)
	}
}

// TestStore_Search_Preserves_Int64_Precision_In_Row_Scan 验 Search 路径 (跟 Get
// 路径一样) 走 unmarshalValue 让大 int64 round-trip 保 precision.
//
// **audit iter-5-F 视角 1 M1 修**: iter-4 H6 改 Get + Search 都用 unmarshalValue.
// Get 有完整测试 (`TestStore_Get_Int64_Preserves_Precision`), **Search 路径的
// Scan→unmarshalValue 0 大整数测试** — 业务 Search 返大 int64 是否保 precision
// 未验.
func TestStore_Search_Preserves_Int64_Precision_In_Row_Scan(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	const bigInt64 int64 = 1234567890123456789 // 大于 2^53 (跟 Get test 同款)
	// sql backend Search 返 row 含大整数
	rows := sqlmock.NewRows([]string{"ns", "data_key", "value", "created_at", "updated_at"}).
		AddRow("test", "snowflake", []byte(`1234567890123456789`), time.Now().UTC(), time.Now().UTC())
	mock.ExpectQuery("SELECT ns, data_key, value, created_at, updated_at").
		WillReturnRows(rows)

	items, err := st.Search(context.Background(), store.NS("test"))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}

	// 业务期望 json.Number 类型 (跨 backend safe 大整数保留)
	num, ok := items[0].Value.(json.Number)
	if !ok {
		t.Fatalf("expected json.Number, got %T: %v (Search 路径 unmarshalValue 不应丢精度)",
			items[0].Value, items[0].Value)
	}
	got, _ := num.Int64()
	if got != bigInt64 {
		t.Errorf("精度丢失: expected %d, got %d", bigInt64, got)
	}
}

// TestStore_Search_EntryValidation_Precedes_KeyShape 钉「入口那次 store.ValidateFilter
// 先于 key 形状校验」——filter 语义错要报语义的哨兵，不能被 "invalid identifier" 抢先。
//
// # 为什么需要它
//
// buildFilterSQL 内部有一层兜底（numericArg / default 分支）返同款 sentinel，把入口那次
// ValidateFilter **完全遮蔽**了：实测删掉入口调用后，全仓测试 + 三个真库的 conformance
// 无一变红。也就是「有一道守卫，但它坏了没人知道」。
//
// 两者唯一还有区别的地方，就是「非法 key 与非法 op / 非法值同时出现时谁先报」——入口在
// 拼 SQL 之前跑，所以报的是语义哨兵；没有它就先撞上 key 校验，返一个不带任何 store.*
// sentinel 的裸错，业务 errors.Is 归不了类。
//
// **sanity-revert**: 删掉 Search 入口的 store.ValidateFilter 调用 → 本测试红。
func TestStore_Search_EntryValidation_Precedes_KeyShape(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	st := agentSQL.NewStore(db, agentSQL.Postgres{})

	cases := []struct {
		name   string
		filter map[string]any
		want   error
	}{
		{"非法 key + 范围 op 值非数值", map[string]any{
			"bad-key": map[string]any{"$gt": "not-a-number"},
		}, store.ErrFilterTypeMismatch},
		{"非法 key + 不支持的 op", map[string]any{
			"bad-key": map[string]any{"$regex": "x"},
		}, store.ErrUnsupportedFilterOp},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := st.Search(context.Background(), store.NS("test"), store.WithFilter(c.filter))
			if !errors.Is(err, c.want) {
				t.Errorf("期望 %v（语义校验先跑），得到 %v", c.want, err)
			}
		})
	}
}
