package schedule

import (
	"testing"
	"time"
)

func TestParseCron_Next(t *testing.T) {
	mustParse := func(expr string) *cronSchedule {
		c, err := parseCron(expr, time.UTC)
		if err != nil {
			t.Fatalf("parseCron(%q): %v", expr, err)
		}
		return c
	}
	at := func(s string) time.Time {
		tm, err := time.ParseInLocation("2006-01-02 15:04", s, time.UTC)
		if err != nil {
			t.Fatal(err)
		}
		return tm
	}
	tests := []struct {
		name string
		expr string
		from string
		want string
	}{
		{"每日9点", "0 9 * * *", "2026-07-07 08:30", "2026-07-07 09:00"},
		{"严格晚于from", "0 9 * * *", "2026-07-07 09:00", "2026-07-08 09:00"},
		{"每15分钟步进", "*/15 * * * *", "2026-07-07 09:07", "2026-07-07 09:15"},
		{"每月1号14:30跨月", "30 14 1 * *", "2026-07-07 00:00", "2026-08-01 14:30"},
		{"下一个周一", "0 0 * * 1", "2026-07-07 12:00", "2026-07-13 00:00"},  // 2026-07-13 是周一
		{"周日用7表示", "0 0 * * 7", "2026-07-07 12:00", "2026-07-12 00:00"}, // 2026-07-12 是周日
		{"单值步进首触发", "5/15 * * * *", "2026-07-07 10:00", "2026-07-07 10:05"},
		{"单值步进第二触发", "5/15 * * * *", "2026-07-07 10:05", "2026-07-07 10:20"}, // 步进生效才有 :20
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := mustParse(tt.expr).next(at(tt.from))
			if !ok {
				t.Fatalf("%s from %s: no next match", tt.expr, tt.from)
			}
			if !got.Equal(at(tt.want)) {
				t.Errorf("%s from %s: got %s want %s",
					tt.expr, tt.from, got.Format("2006-01-02 15:04"), tt.want)
			}
		})
	}
}

// TestParseCron_DomDowUnion 锁住经典 crontab 的日/周并集语义：两者都受限时命中其一即算。
func TestParseCron_DomDowUnion(t *testing.T) {
	c, err := parseCron("0 0 13 * 5", time.UTC) // 每月13号 或 每周五 的 00:00
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 7, 7, 0, 0, 0, 0, time.UTC) // 周二
	got, ok := c.next(from)
	if !ok {
		t.Fatal("no next match")
	}
	// 周五(07-10)先于13号(07-13)到达——并集语义取更早者；若误用交集则会晚得多。
	want := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("got %s want %s (union semantics)", got.Format("2006-01-02"), want.Format("2006-01-02"))
	}
}

func TestParseCron_Invalid(t *testing.T) {
	bad := []string{
		"",            // 空
		"* * * *",     // 4 字段
		"* * * * * *", // 6 字段
		"60 * * * *",  // 分钟越界
		"* 24 * * *",  // 小时越界
		"* * 0 * *",   // 日越界（<1）
		"* * * 13 *",  // 月越界
		"* * * * 8",   // 周越界（>7）
		"a * * * *",   // 非数字
		"*/0 * * * *", // 步进为 0
		"5-2 * * * *", // 范围逆序
	}
	for _, expr := range bad {
		if _, err := parseCron(expr, time.UTC); err == nil {
			t.Errorf("expected error for %q, got nil", expr)
		}
	}
}

// TestParseField_SingleValueStep 锁住 "N/step" 语义 = "N-max/step"，步进不能被丢弃。
func TestParseField_SingleValueStep(t *testing.T) {
	tests := []struct {
		field    string
		min, max int
		want     []int
	}{
		{"5/15", 0, 59, []int{5, 20, 35, 50}},
		{"10/15", 0, 59, []int{10, 25, 40, 55}},
		{"3/4", 0, 23, []int{3, 7, 11, 15, 19, 23}},
		{"5", 0, 59, []int{5}},                // 裸单值不受影响
		{"*/15", 0, 59, []int{0, 15, 30, 45}}, // */step 不受影响
	}
	for _, tt := range tests {
		mask, _, err := parseField(tt.field, tt.min, tt.max)
		if err != nil {
			t.Errorf("parseField(%q): %v", tt.field, err)
			continue
		}
		var got []int
		for v := tt.min; v <= tt.max; v++ {
			if bit(mask, v) {
				got = append(got, v)
			}
		}
		if !equalInts(got, tt.want) {
			t.Errorf("parseField(%q) = %v, want %v", tt.field, got, tt.want)
		}
	}
}

// TestParseCron_LeapCenturyBoundary 锁住 next 扫描窗口覆盖跨世纪闰日间隔（最大 8 年）。
func TestParseCron_LeapCenturyBoundary(t *testing.T) {
	c, err := parseCron("0 0 29 2 *", time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2096, 3, 1, 0, 0, 0, 0, time.UTC) // 刚过 2096-02-29
	got, ok := c.next(from)
	if !ok {
		t.Fatal("next 越过跨世纪闰日间隔返回 false（扫描窗口过小）")
	}
	want := time.Date(2104, 2, 29, 0, 0, 0, 0, time.UTC) // 2100 非闰，下一个闰日是 2104
	if !got.Equal(want) {
		t.Errorf("got %s want %s", got.Format("2006-01-02"), want.Format("2006-01-02"))
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
