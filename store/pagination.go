package store

import (
	"errors"
	"fmt"
)

// ErrNegativeOffset 表分页偏移是负数（WithOffset / WithListOffset）。Search 与 ListNamespaces 在入口返回它，不读不写。
//
// 负的偏移几乎总是调用方算错了（页码减一减成了负数之类），当成 0 会把这个 bug 悄悄吞掉；值是调用方给的、本身非法，
// 调用方自己就能检查——所以报错。曾经各后端各答各的：in-memory 切片越界 panic、SQLite 当 0、PG 报
// `OFFSET must not be negative`、MySQL 报语法错误。
var ErrNegativeOffset = errors.New("store: offset must not be negative")

// ValidateOffset 校验分页偏移，负数返回 [ErrNegativeOffset]。业务自实现 Store 时在 Search / ListNamespaces 入口
// 调它（offset 取自 ParseSearchOptions / ParseListOptions），跟内置后端同一份判据。
func ValidateOffset(offset int) error {
	if offset < 0 {
		return fmt.Errorf("%w: %d", ErrNegativeOffset, offset)
	}
	return nil
}
