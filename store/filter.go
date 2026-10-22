package store

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
)

// ── filter 语义（唯一实现，客户端过滤型后端共用）─────────────────────────────
//
// [InMemoryStore] 自用；redis 后端与业务自实现的客户端过滤后端直接调
// [ValidateFilter] + [MatchFilter]，不要各自抄一份——这段语义曾在 in-memory / redis /
// example 三处各有一份拷贝，改一处漏两处的形状。下推型后端（sql）翻译成 SQL 时
// 也以这里的行为为准，conformance 套件按它逐格钉。
//
// # 语义（MongoDB 风格子集，type bracketing）
//
//   - `{"key": v}` 与 `{"key": {"$eq": v}}` 等价：**JSON 语义相等**——数值 int / float /
//     json.Number 互通（10 == 10.0），bool 不是数（true != 1），数字字符串不是数
//     （"10" != 10），数组逐元素相等（不是包含），对象逐键相等。见 [ValueEqual]。
//   - `$ne v`：**字段存在且不等**。字段缺失不命中；字段是 JSON null 时算存在
//     （`{"f": null}` 命中 `$ne: 10`）。
//   - `$gt / $gte / $lt / $lte`：**filter 值必须是数值**，否则返 [ErrFilterTypeMismatch]
//     （静态判定，空 namespace 也报——那是调用方自己给的东西）；**存的值不是数值
//     （字符串 / bool / null / 数组 / 对象）或字段缺失 ⟹ 这一行不命中、不报错**。
//     一条异构行不会让整个 Search 失败。
//   - 多键 AND；不支持 `$or / $nor / $in / $nin / $and`（返 [ErrUnsupportedFilterOp]）。
//
// 「存的类型对不上就不命中」而不是报错，理由见 doc/description/three-way-comparison/store.md
// 「filter 类型不匹配语义」节：调用方无法知道 namespace 里每一行存的是什么类型，报错等于
// 让一条脏数据毒化整个查询；而 filter 值的类型是调用方手上就有的信息，报错合法。

// supportedFilterOps 是支持的 6 比较操作符。
var supportedFilterOps = map[string]struct{}{
	"$eq":  {},
	"$ne":  {},
	"$gt":  {},
	"$gte": {},
	"$lt":  {},
	"$lte": {},
}

// checkFilterValueSerializable 拒绝**不能过 JSON** 的 filter 值——NaN / ±Inf / chan /
// func 都在此列。
//
// # 为什么这道守卫合法
//
// 判的是**调用方自己给的东西**（filter 值的类型和取值他手上就有），不是「你的意图对上
// 我发现的存储状态」——后者该定义默认行为而不是报错（那正是范围 op 对**存的值**采用
// type bracketing 的理由）。
//
// # 不拒会怎样：四个后端三种答案，全部 err == nil
//
// 真库实测 `$gt NaN` / `$lt NaN` 打在同一行数值上：
//
//	客户端过滤（in-memory / redis / example）  两个方向都不命中（NaN 不可比）
//	MySQL                                     `$gt NaN` 命中
//	PG                                        `$lt NaN` 命中（PG 的 numeric 把 NaN 排在
//	                                          所有数之上、且自等，与 IEEE-754 不同）
//	SQLite                                    两个方向都不命中
//
// 而 `$eq NaN` / `$eq chan` 在 sql 侧是 `json.Marshal` 失败返的**裸错**（业务
// errors.Is 判不出来），在客户端侧压根不报错。静态拒掉之后四个后端同一时机、同一个
// 哨兵——这是唯一能让它们对齐的位置，因为「NaN 在引擎里怎么排序」我们改不动。
func checkFilterValueSerializable(key, opName string, v any) error {
	if _, err := json.Marshal(v); err != nil {
		where := fmt.Sprintf("field %q", key)
		if opName != "" {
			where = fmt.Sprintf("%s on field %q", opName, key)
		}
		return fmt.Errorf("%w: %s got a value that cannot be represented in JSON (%v); "+
			"NaN / ±Inf / chan / func compare differently on every backend",
			ErrFilterTypeMismatch, where, err)
	}
	return nil
}

// normalizeFilter 返回 filter 的一份拷贝，比较值换成它们的 JSON 形状（同 [NormalizeValue]）：存进去的值按 JSON 形状
// 比，filter 这一侧也得是。不换的话比较值是具体 Go 类型时各后端答案不同——`{"meta": {"$eq": map[string]string{...}}}`
// 在客户端过滤的后端上 0 条（`map[string]any` 跟 `map[string]string` 不相等）、在 SQLite 上 1 条（两侧都编成 JSON
// 文本）；struct 比较值按字段声明序编码，在 SQLite 上也对不上键已排序的存储。
//
// 直值编成 JSON 是对象（struct、`map[string]string`）时包成 `{"$eq": 对象}`：它的意思是「等于这个对象」，换成
// `map[string]any` 之后不能被当成 op map。编不成 JSON 的比较值（NaN、chan）留原样，由 [ValidateFilter] 报
// [ErrFilterTypeMismatch]。
func normalizeFilter(filter map[string]any) map[string]any {
	if len(filter) == 0 {
		return filter
	}
	out := make(map[string]any, len(filter))
	for key, v := range filter {
		if opMap, ok := v.(map[string]any); ok {
			ops := make(map[string]any, len(opMap))
			for op, operand := range opMap {
				ops[op] = normalizeOperand(operand)
			}
			out[key] = ops
			continue
		}
		norm := normalizeOperand(v)
		if obj, ok := norm.(map[string]any); ok {
			out[key] = map[string]any{"$eq": obj}
			continue
		}
		out[key] = norm
	}
	return out
}

// normalizeOperand 把一个比较值换成它的 JSON 形状；编不成 JSON 的原样返回。
func normalizeOperand(v any) any {
	norm, err := NormalizeValue(v)
	if err != nil {
		return v
	}
	return norm
}

// isRangeOp 判 op 是否 4 个数值范围操作符之一。
func isRangeOp(op string) bool {
	switch op {
	case "$gt", "$gte", "$lt", "$lte":
		return true
	}
	return false
}

// ValidateFilter 静态校验 filter：操作符必须在 6 个支持的比较 op 内（否则
// [ErrUnsupportedFilterOp]；顶级 `$` 键如 `{"$or": [...]}` 同样按不支持的操作符拒绝）；`$gt / $gte / $lt / $lte` 的 **filter 值必须是数值**
// （int 系 / uint 系 / float 系 / json.Number，否则 [ErrFilterTypeMismatch]）。
//
// 不读数据、不看 namespace——空 namespace 也一样报。后端在扫描前调一次，让「filter 写错」
// 在任何后端上都是同一个错误、在同一时机报。
func ValidateFilter(filter map[string]any) error {
	for key, v := range filter {
		if strings.HasPrefix(key, "$") {
			// 顶级 `$` 键是 MongoDB 的逻辑操作符形态（`{"$or": [...]}`）——不支持就要报，
			// 不能当成一个叫 "$or" 的字段做等值（那样零行、err == nil，业务以为没数据）。
			return fmt.Errorf("%w: top-level %s (only the field-level form {key: {op: v}} is supported; no logical operators)",
				ErrUnsupportedFilterOp, key)
		}
		opMap, ok := v.(map[string]any)
		if !ok {
			// 直接相等匹配（非 op 子 map）——op 名不用校验，但值本身得能过 JSON。
			if err := checkFilterValueSerializable(key, "", v); err != nil {
				return err
			}
			continue
		}
		if len(opMap) == 0 {
			// `{"f": {}}`：既不是 op map 也不是「等于空对象」。放行的话新规则被**空洞
			// 满足**——实测 in-memory 返「有 f 字段的行」、SQLite 返**全表**，正是这条
			// 规则本来要消灭的「一个写法两种语义」。想表达「等于空对象」写
			// `{"f": {"$eq": map[string]any{}}}`。
			return fmt.Errorf("%w: field %q got an empty map (write {%q: {$eq: {}}} to match an empty object)",
				ErrUnsupportedFilterOp, key, key)
		}
		for opName, opValue := range opMap {
			if !strings.HasPrefix(opName, "$") {
				// 嵌套 filter / op map 里混进非 $ 键——**拒**，别猜业务想要哪种语义。
				// 修前这里是 continue，代价是同一个写法有两种语义、且第二种静默：
				//   {"f": {"g": 1}}            客户端按「嵌套部分匹配」算，sql 直接报错
				//   {"f": {"$eq": 10, "g": 1}} 客户端把 $eq 整个丢掉、返零行且 err == nil
				// 后者尤其坏：业务明明写了 $eq，得到的是「没数据」而不是「你写错了」。
				return fmt.Errorf("%w: field %q got a nested filter / mixed map (only {%q: {$op: v}} "+
					"is supported; nested field paths are not)", ErrUnsupportedFilterOp, key, key)
			}
			if _, supported := supportedFilterOps[opName]; !supported {
				return fmt.Errorf("%w: %s", ErrUnsupportedFilterOp, opName)
			}
			if isRangeOp(opName) {
				if _, isNum := toFloat(opValue); !isNum {
					return fmt.Errorf("%w: %s on field %q requires a numeric filter value, got %T",
						ErrFilterTypeMismatch, opName, key, opValue)
				}
			}
			if err := checkFilterValueSerializable(key, opName, opValue); err != nil {
				return err
			}
		}
	}
	return nil
}

// MatchFilter 判 value 是否匹配 filter——客户端过滤的唯一实现，语义见 [WithFilter] godoc
// （本文件顶部注释是同一份语义的展开）。
//
// 调用方应先过 [ValidateFilter]（本函数对非法 op / 非数值范围值也会返错，但那是
// 「扫到才报」，静态校验才能保证空 namespace 也报）。
//
//   - {"key": value}              精确匹配 value.key == value（[ValueEqual]）
//   - {"key": {"$gt": 10}}        操作符匹配
//   - {"k1": v1, "k2": v2}        多键 AND（隐含）
func MatchFilter(value any, filter map[string]any) (bool, error) {
	if len(filter) == 0 {
		return true, nil
	}
	valueMap, ok := value.(map[string]any)
	if !ok {
		// filter 非空但 value 不是 map——无法匹配
		return false, nil
	}
	for key, want := range filter {
		if strings.HasPrefix(key, "$") {
			// 与 ValidateFilter 对称：顶级 `$` 键是逻辑操作符形态，不是字段名。
			// 不拒的话这里会去查一个叫 "$or" 的字段、查不到返 false ⟹ 直接调
			// MatchFilter 的自定义 backend 又回到「零行且 err == nil」那个坑。
			return false, fmt.Errorf("%w: top-level %s (only the field-level form {key: {op: v}} is supported)",
				ErrUnsupportedFilterOp, key)
		}
		got, exists := valueMap[key]
		if !exists {
			return false, nil
		}
		ok, err := matchValue(got, want)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// matchValue 检查单字段值是否匹配 want（标量精确 / op map）。
func matchValue(got, want any) (bool, error) {
	wantMap, isOpMap := want.(map[string]any)
	if !isOpMap {
		// 直接精确匹配
		return ValueEqual(got, want), nil
	}
	if len(wantMap) == 0 {
		// 与 ValidateFilter 同一判据，见那里的说明。
		return false, fmt.Errorf("%w: got an empty map (write {$eq: {}} to match an empty object)",
			ErrUnsupportedFilterOp)
	}
	// 先整体扫一遍再算：嵌套 / 混用一律拒（与 ValidateFilter 同一判据）。
	//
	// ⚠️ **必须先扫完**，不能边算边拒——map 迭代序不定，「先撞到 $op 还是先撞到裸键」
	// 会让同一个 filter 一会儿报错、一会儿返 false。判据本身确定，出口就得确定。
	for opName := range wantMap {
		if !strings.HasPrefix(opName, "$") {
			return false, fmt.Errorf("%w: got a nested filter / mixed map at %q (only {$op: v} is supported)",
				ErrUnsupportedFilterOp, opName)
		}
	}
	// op map：每个 $op 都必须满足
	for opName, opValue := range wantMap {
		ok, err := applyOp(opName, got, opValue)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// applyOp 应用单个 $op 比较——支持 6 比较操作符。
//
// 范围 op 的两侧分开对待：**filter 值**非数值是调用方的错（返 [ErrFilterTypeMismatch]，
// 正常情况下 [ValidateFilter] 已在扫描前拦住）；**存的值**非数值是数据的形状，这一行
// 不命中、不报错（type bracketing）。
func applyOp(op string, got, opValue any) (bool, error) {
	switch op {
	case "$eq":
		return ValueEqual(got, opValue), nil
	case "$ne":
		return !ValueEqual(got, opValue), nil
	case "$gt", "$gte", "$lt", "$lte":
		if _, ok := toFloat(opValue); !ok {
			return false, fmt.Errorf("%w: %s requires a numeric filter value, got %T",
				ErrFilterTypeMismatch, op, opValue)
		}
		cmp, ok := compareNumeric(got, opValue)
		if !ok {
			return false, nil // 存的不是数：类型对不上 = 不命中
		}
		switch op {
		case "$gt":
			return cmp > 0, nil
		case "$gte":
			return cmp >= 0, nil
		case "$lt":
			return cmp < 0, nil
		case "$lte":
			return cmp <= 0, nil
		}
	}
	// ValidateFilter 已预校验过——理论上不可达
	return false, fmt.Errorf("%w: %s", ErrUnsupportedFilterOp, op)
}

// ValueEqual 是 JSON 语义相等——filter 的 `$eq` / `$ne` / 直值匹配都用它。
//
//   - 数值互通：int 系 / uint 系 / float 系 / json.Number 按数值比（10 == 10.0），
//     **递归到数组元素与对象字段**（JSON 反序列化后整数常变 float64 或 json.Number，
//     不这样做的话 `[]any{1}` 与 `[]any{float64(1)}` 永不相等）
//   - bool 不是数（true != 1）；数字字符串不是数（"10" != 10）
//   - 数组：长度相等且逐元素 ValueEqual（**不是包含**）；任意 slice / array 类型互比
//     （`[]string` 与 `[]any` 元素相同即相等）
//   - 对象（map[string]any）：键集相等且逐键 ValueEqual
//   - 其余（string / bool / nil / 其他类型）reflect.DeepEqual——避免 `a == b` 对不可比较
//     类型 panic
func ValueEqual(a, b any) bool {
	_, aIsNum := toFloat(a)
	_, bIsNum := toFloat(b)
	if aIsNum || bIsNum {
		// 一侧是数就必须两侧都是数——不能落到 DeepEqual 去（"10" 不等于 10）。
		// 两侧都是整数时 compareNumeric 走精确路径，2^53 之外不再被 float64 舍成相等。
		if !aIsNum || !bIsNum {
			return false
		}
		cmp, ok := compareNumeric(a, b)
		return ok && cmp == 0
	}
	if am, ok := a.(map[string]any); ok {
		bm, ok := b.(map[string]any)
		if !ok || len(am) != len(bm) {
			return false
		}
		for k, av := range am {
			bv, ok := bm[k]
			if !ok || !ValueEqual(av, bv) {
				return false
			}
		}
		return true
	}
	ra, rb := reflect.ValueOf(a), reflect.ValueOf(b)
	if isSequence(ra) && isSequence(rb) {
		if ra.Len() != rb.Len() {
			return false
		}
		for i := 0; i < ra.Len(); i++ {
			if !ValueEqual(ra.Index(i).Interface(), rb.Index(i).Interface()) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(a, b)
}

// isSequence 判 reflect.Value 是否 slice / array（[]byte 也算——它与 string 永不相等，
// 与 DeepEqual 行为一致）。
func isSequence(v reflect.Value) bool {
	if !v.IsValid() {
		return false
	}
	k := v.Kind()
	return k == reflect.Slice || k == reflect.Array
}

// toInt64 是 [toBigInt] 的无分配快路——只在值精确落在 int64 内时返 ok。
//
// uint64 超过 MaxInt64 的那一段、以及超长 json.Number 都返 false，交给 big.Int 路径；
// 两条路的判定集合必须一致（都是「这是不是一个精确整数」），只是表示范围不同。
func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case json.Number:
		str := n.String()
		if strings.ContainsAny(str, ".eE") {
			return 0, false
		}
		i, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return 0, false // 超 int64：交给 big.Int 路径
		}
		return i, true
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint:
		if uint64(n) > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		if n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	}
	return 0, false
}

// toBigInt 把 v 解释成**精确整数**——只在两侧都是整数时才走这条路。
//
// # 为什么必须有它
//
// `toFloat` 一律转 float64，超出 2^53 的整数会被舍入：存 `9007199254740993` 时
// `$eq 9007199254740992` **反命中**、`$gt 9007199254740992` **漏命中**——两个方向都错，
// 而三个 SQL 引擎（PG numeric / MySQL BIGINT / SQLite INTEGER）都算得对。也就是说在整数
// 这一维上，客户端过滤是唯一错的那个。Snowflake ID、纳秒时间戳都落在这个区间。
//
// # 边界：只治整数，不治小数
//
// 小数那半治不了，也不该在这里治——三个引擎自己就不一致（PG 的 jsonb 是任意精度 numeric、
// MySQL JSON 是 DOUBLE、SQLite `json_extract` 出 REAL），没有一个「对」的答案可对齐。
// 小数继续走 float64（与 MySQL / SQLite 同域），差异写在 [WithFilter] 的已知不一致里。
//
// float32 / float64 一律不进这条路：它们到这儿时精度已经丢了，按整数比反而制造假精确。
func toBigInt(v any) (*big.Int, bool) {
	switch n := v.(type) {
	case json.Number:
		// 只接受纯整数字面量——带小数点或指数的交给 float64 路径
		str := n.String()
		if strings.ContainsAny(str, ".eE") {
			return nil, false
		}
		i, ok := new(big.Int).SetString(str, 10)
		return i, ok
	case int:
		return big.NewInt(int64(n)), true
	case int8:
		return big.NewInt(int64(n)), true
	case int16:
		return big.NewInt(int64(n)), true
	case int32:
		return big.NewInt(int64(n)), true
	case int64:
		return big.NewInt(n), true
	case uint:
		return new(big.Int).SetUint64(uint64(n)), true
	case uint8:
		return new(big.Int).SetUint64(uint64(n)), true
	case uint16:
		return new(big.Int).SetUint64(uint64(n)), true
	case uint32:
		return new(big.Int).SetUint64(uint64(n)), true
	case uint64:
		return new(big.Int).SetUint64(n), true
	}
	return nil, false
}

// compareNumeric 比较两个数值，返回 -1 / 0 / 1；ok=false 表示某一侧不是数。
//
// 两侧都是整数时走 [toBigInt] 精确比较，否则退 float64（见 toBigInt 的边界说明）。
func compareNumeric(a, b any) (int, bool) {
	// 快路：两侧都落在 int64 内就直接比，不分配。MatchFilter 是**逐行**调用的，
	// 绝大多数业务数值都是小整数，不该为了 2^53 那一档让每行都分配两个 big.Int。
	if ai, ok := toInt64(a); ok {
		if bi, ok2 := toInt64(b); ok2 {
			switch {
			case ai < bi:
				return -1, true
			case ai > bi:
				return 1, true
			}
			return 0, true
		}
	}
	// 慢路：超出 int64 才用 big.Int（实测 4 allocs/op，只有 2^53 之外的整数走到）。
	if ai, ok := toBigInt(a); ok {
		if bi, ok2 := toBigInt(b); ok2 {
			return ai.Cmp(bi), true
		}
	}
	af, ok := toFloat(a)
	if !ok {
		return 0, false
	}
	bf, ok := toFloat(b)
	if !ok {
		return 0, false
	}
	// ⚠️ **NaN 是不可比，不是相等**。IEEE-754 下 NaN 与任何值（含自己）的 `<` `>` `==`
	// 全为 false——所以「既不小于也不大于」**不能**兜底成 `return 0`（那是相等）。
	// 曾经就这么写过：一个 NaN 行会命中**任何**数值 filter（`$eq 10` 把它返回来、
	// `$ne 10` 反而不返），五个算子全翻，且 err == nil。
	//
	// 返 ok=false ⟹ 调用方按「这一侧不是可比的数」处理：范围 op 不命中、
	// ValueEqual 返 false、`$ne` 因此为 true。与 NaN 的数学语义一致。
	if math.IsNaN(af) || math.IsNaN(bf) {
		return 0, false
	}
	switch {
	case af < bf:
		return -1, true
	case af > bf:
		return 1, true
	}
	return 0, true
}

// toFloat 尝试把 v 转 float64——支持 int 系列 + uint 系列 + float 系列 + json.Number。
// bool / string / nil / 容器都不是数。
//
// ⚠️ 它只回答「是不是数」，**不保证可比**：NaN 也是合法的 float64。要比大小走
// [compareNumeric]，它会把 NaN 判成不可比。
func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}
