package sql

import "strings"

// likeEscapeChar 是本模块所有 LIKE 模式统一的转义符。
//
// **不用反斜杠**：MySQL 默认就把 `\` 当 LIKE 的转义符，而它同时是字符串字面量的
// 转义符——两层转义叠在一起时行为随 `NO_BACKSLASH_ESCAPES` 等会话变量漂移。
// 显式 `ESCAPE '!'` 在 PG 与 MySQL 上语义一字不差，不受会话设置影响。
const likeEscapeChar = "!"

// likeEscapeSuffix 是拼在 LIKE 之后的 ESCAPE 子句。
const likeEscapeSuffix = ` ESCAPE '` + likeEscapeChar + `'`

// escapeLikePattern 把用户/框架给的**字面量前缀**转义成可安全嵌进 LIKE 模式的形式。
//
// # 为什么必须转义
//
// `_` 在 SQL LIKE 里是**单字符通配符**、`%` 是任意长度通配符。把一段字面量原样拼进
// 模式，等于让它里面的这两个字符获得通配能力——而校验层只守着分隔符：
// `compose_v2.ValidateNodeIDForNS` 只拒 `/ # @`，`store.Namespace.Validate` 只拒
// `.` 与 NUL，两者都放行 `_` 和 `%`。
//
// 实测（sqlite3，PG/MySQL 同款 LIKE 语义）：
//   - **cp 层**：`DeleteSubtree(sid, "round_1")` 的 `LIKE 'round_1/%'` 会连
//     `roundX1/child` 一起删——而 `round_%d` 正是 LoopAgent 自己生成的 ns 段
//     （另有 PlanAgent 的 `%s_step_%d` / `tmp_step_%d`、HandoffAgent 的
//     `agent_%s_hop%d`），业务一个字都没写错也够得着。
//   - **store 层**（更重）：`NS("a_b")` 的子树查询 `LIKE 'a_b.%'` 命中
//     `a.b.secret`——`_` 匹配了命名空间分隔符 `.` ⟹ **跨 namespace 读**。
//     namespace 是多租户隔离边界，这不是误删是越权。
//
// 两类危害都 `err == nil`，静默。
//
// 转义符自身必须最先替换，否则后续插入的转义符会被二次转义。
func escapeLikePattern(s string) string {
	s = strings.ReplaceAll(s, likeEscapeChar, likeEscapeChar+likeEscapeChar)
	s = strings.ReplaceAll(s, "%", likeEscapeChar+"%")
	s = strings.ReplaceAll(s, "_", likeEscapeChar+"_")
	return s
}
