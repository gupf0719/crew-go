package sql

import (
	"strings"
	"testing"
)

// TestValidateTableName_Valid 验合法表名通过.
func TestValidateTableName_Valid(t *testing.T) {
	cases := []string{
		"crew_store",
		"my_table",
		"a",                     // 单字符
		"_prefix",               // 下划线开头
		"table123",              // 含数字
		strings.Repeat("a", 63), // 最长边界 63
	}
	for _, name := range cases {
		if err := validateTableName(name); err != nil {
			t.Errorf("validateTableName(%q) should pass, got: %v", name, err)
		}
	}
}

// TestValidateTableName_Invalid 验非法表名被拒 (SQL injection 防御).
//
// **audit iter-5 sanity-revert 验证**: 删 validateTableName regex 或改 `.+`
// 让所有名字通过 → 此测试 fail, 业务 SQL injection 攻击面打开.
func TestValidateTableName_Invalid(t *testing.T) {
	cases := []string{
		"",                       // 空
		"123_starts_with_digit",  // 数字开头
		"has spaces",             // 空格
		"has.dot",                // 含点
		"has-dash",               // 含连字符
		"; DROP TABLE users; --", // SQL injection 经典 pattern
		"table; DROP TABLE x",    // 含分号
		"table'or'1'='1",         // SQL 注入 quote
		"table--comment",         // 含 SQL 注释
		"table/*comment*/",       // 含块注释
		strings.Repeat("a", 64),  // 超长 64
		strings.Repeat("a", 100), // 超长 100
		"table\x00null",          // 含 NUL byte
		"中文表名",                   // 非 ASCII
	}
	for _, name := range cases {
		if err := validateTableName(name); err == nil {
			t.Errorf("validateTableName(%q) should fail (SQL injection 防御)", name)
		}
	}
}
