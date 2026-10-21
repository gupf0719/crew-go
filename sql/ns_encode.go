package sql

import (
	"strings"

	"crew-go/store"
)

// nsSeparator 是 SQL backend 内部 namespace 分隔符. 跟 InMemoryStore 用 `\x00`
// 不同, SQL backend 用 `.` — SQL `LIKE` 友好.
//
// **sibling false-positive 防御** (audit iter-3 视角 3 H3 关键 contract):
// crew-go `store.Namespace.Validate()` 强制段不含 `.` (`store/store.go:71`),
// 所以 `joinNS([users, u1]) = "users.u1"` 不可能跟 `joinNS([users_admin]) =
// "users_admin"` 混淆. SQL `WHERE ns = 'users.u1' OR ns LIKE 'users.u1.%'`
// 不会误命中 `users_admin`.
const nsSeparator = "."

// joinNS 将 namespace slice 编码为 SQL ns 列值 (单段) — `.` 分隔.
func joinNS(ns store.Namespace) string {
	return strings.Join(ns, nsSeparator)
}

// splitNS 将 SQL ns 列值解析回 namespace slice. 仅在 ListNamespaces / Search
// 返回 *Item 时把 db row 转换回 store.Item.Namespace 用.
//
// **设计**: **写入端 fail-fast** + **读出端 0 验证** (直接 `split(".")`).
// crew-go `store.Namespace.Validate()` (`store/store.go:60-76`) 在 Put/Search/Get
// 入口已 fail-fast 防御所有非法 ns (空段 / `.` / NUL byte) — DB 不可能存进非法
// ns. 读出端信任 DB 数据, 不做兜底验证.
//
// **业务自负的边界场景** (framework 不兜底):
//   - 业务绕过 framework 直接 `INSERT INTO store (ns, ...) VALUES ('a..b', ...)`
//   - 业务跨 backend backup/restore 数据格式不同
//
// 业务自查用 `ns.Validate()` (拿到 Item 后).
//
// **iter-5-I Option A 修**: 退回 iter-4 H4 加的段验证 + 5c35175 caller silent
// skip — 双层过度防御让业务 SQL `LIMIT N OFFSET M` 翻页漏数据 (LIMIT 100 拿 100
// 行 skip 5 行 → 业务下页 OFFSET 100 永远漏被 skip 行的后续 row); "防 panic"
// 注释空想威胁 (strings.Split 总返 len≥1 slice 不可能 panic).
func splitNS(joined string) store.Namespace {
	if joined == "" {
		return nil
	}
	return store.Namespace(strings.Split(joined, nsSeparator))
}

// nsExactOrDescendantSQL 返"匹配 ns 自身 + 全后代" SQL 条件 + 参数列表.
//
// 业务用例: Search(NS("users", "u1")) 应匹配 `users.u1` (自身) + `users.u1.*`
// (所有后代). 不能 false-positive 命中 `users.u1abc` (字符前缀 sibling 但段级
// 不是后代).
//
// SQL 形态:
//
//	(ns = $1 OR ns LIKE $2)
//	 ↑ 参数: prefix, prefix + "." + "%"
//
// 业务 caller 把返的 cond + 参数嵌入 WHERE 子句.
func nsExactOrDescendantSQL(d Dialect, prefix store.Namespace, paramOffset int) (cond string, args []any) {
	p := joinNS(prefix)
	// **前缀必须转义**：`_` 在 LIKE 里匹配任意单字符，而 `store.Namespace.Validate`
	// 只拒 `.` 与 NUL ——`NS("a_b")` 的子树查询会命中 `a.b.secret`（`_` 匹配了
	// 命名空间分隔符 `.`），那是**跨 namespace 读**，不是误删。见 escapeLikePattern。
	cond = "(ns = " + d.Placeholder(paramOffset) + " OR ns LIKE " + d.Placeholder(paramOffset+1) + likeEscapeSuffix + ")"
	args = []any{p, escapeLikePattern(p) + nsSeparator + "%"}
	return
}
