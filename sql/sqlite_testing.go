package sql

import (
	databasesql "database/sql"
	"fmt"
	"sync/atomic"
)

// sqliteDBSeq 给每次 [OpenSQLiteForTest] 发一个唯一库名。
//
// **必须每次一个新库**：`mode=memory` 配 `cache=shared` 时，同名库在**整个进程内
// 是同一个**——不换名的话，每个 factory() 拿到的是上一个用例留下的数据。实测症状是
// 计数类断言全线偏大（「expected 2, got 8」），而那看起来像 backend 在多返数据。
var sqliteDBSeq atomic.Uint64

// OpenSQLiteForTest 打开一个进程内 SQLite 连接，用于让本模块的 SQL **在真引擎上**
// 跑起来（conformance 套件、集成用例）。
//
// # 为什么需要它
//
// 本模块此前唯一的测试手段是 sqlmock——它钉的是「发出去的 SQL 长什么样」，不是
// 「那条 SQL 跑出来对不对」。代价实测过两次（ns 子树匹配先后漏掉重访实例段与 LIKE
// 元字符，两次都对测试全程隐形）。而同类缺陷在跑 conformance 的 in-memory / redis
// 上会当场变红——唯一的变量是「backend 能不能真跑起来」。
//
// # 三条必须的连接设置（缺一会让用例误红或漏判）
//
//  1. **`PRAGMA case_sensitive_like = ON`**：SQLite 的 LIKE 默认对 ASCII 忽略大小写，
//     而本模块的 PG/MySQL 都是大小写敏感的（MySQL 的身份列是二进制串）。
//     不置位时 ns 大小写相关的用例会**误红**——错的是替身不是被测代码。
//  2. **`PRAGMA foreign_keys = ON`**：与另两个方言的约束行为对齐。
//  3. **`_txlock=immediate` + 单连接**：SQLite 的写是库级串行的，默认 deferred 事务
//     在并发用例里会撞 `SQLITE_BUSY`。conformance 有并发子测试，所以这里把连接池
//     压到 1 并用 immediate 事务——**这是替身的实现细节，不代表被测代码的并发语义**
//     （真并发行为只有连真库才验得到）。
//
// # ⚠️ 它覆盖什么、不覆盖什么
//
// **覆盖**：跨方言共有的语义契约。本模块的 SQL 主体是**同一份字符串**，只有占位符
// 与 filter 的 JSONField* 片段经 dialect 渲染——ns 子树匹配、UPSERT 幂等、round-trip、批量，
// 以及 filter 语义（三个 JSONField* 方法按 conformance 逐格与另两个方言对齐）都在此列。
// TTL 只覆盖**拒绝路径**（不支持 → 返 `ErrTTLNotSupported`），过期那条子测试是 SKIP。
//
// **不覆盖**：方言私有部分——PG 的 JSONB 存储与 planner 行为（`JSONFieldNumericCompare`
// 的守卫为什么必须是 CASE 只有真 PG 能证明）、MySQL 的 JSON 类型优先级与主键 3072 字节
// 上限、两者各自的锁与并发语义、类型精度。**别把它当成 PG 覆盖**；那一档只能连真库
// （业务侧照 `Checkpointer` / `Store` 的 godoc 跑 conformance 即可；本仓 2026-09-03 用
// embedded-postgres 与官方 MySQL tarball 各跑过一遍，见 store 对照文档）。
func OpenSQLiteForTest() (*databasesql.DB, error) {
	// 每次一个**独立**的内存库（见 sqliteDBSeq）；cache=shared 让同一个库的多个
	// 连接看到同一份数据；_txlock=immediate 见上方第 3 条。
	dsn := fmt.Sprintf("file:crew_test_%d?mode=memory&cache=shared&_txlock=immediate",
		sqliteDBSeq.Add(1))
	db, err := databasesql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// 并发写串行化——替身的实现细节，见 godoc 第 3 条。
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA case_sensitive_like = ON;",
		"PRAGMA foreign_keys = ON;",
	} {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("sqlite %s: %w", pragma, err)
		}
	}
	return db, nil
}
