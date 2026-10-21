package schedule

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// cronSchedule 是一条标准 crontab 表达式解析后的形态。
//
// 5 字段：分(0-59) 时(0-23) 日(1-31) 月(1-12) 周(0-7, 0/7=周日)。每个字段用位掩码
// 表示允许值，next 按分钟步进求下一个匹配时刻。只实现经典 crontab 语义（* , - /），
// 不含 @macro / L / W / # / 秒 等扩展——对"参考 crontab"够用，且零外部依赖。
type cronSchedule struct {
	minute uint64 // bit i 置位表示分钟 i 命中
	hour   uint64
	dom    uint64 // day-of-month
	month  uint64
	dow    uint64 // day-of-week（已把 7 归一到 0）
	// domRestricted / dowRestricted 记录字段是否为 "*"，用于经典 crontab 的
	// "日和周都受限时取并集"语义（见 dayMatch）。
	domRestricted bool
	dowRestricted bool
	loc           *time.Location
}

// parseCron 解析标准 crontab 表达式；loc 为空用 time.Local。
func parseCron(expr string, loc *time.Location) (*cronSchedule, error) {
	fields := strings.Fields(strings.TrimSpace(expr))
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron expression must have 5 fields, got %d: %q", len(fields), expr)
	}
	if loc == nil {
		loc = time.Local
	}
	minute, _, err := parseField(fields[0], 0, 59)
	if err != nil {
		return nil, fmt.Errorf("minute field: %w", err)
	}
	hour, _, err := parseField(fields[1], 0, 23)
	if err != nil {
		return nil, fmt.Errorf("hour field: %w", err)
	}
	dom, domRestricted, err := parseField(fields[2], 1, 31)
	if err != nil {
		return nil, fmt.Errorf("day-of-month field: %w", err)
	}
	month, _, err := parseField(fields[3], 1, 12)
	if err != nil {
		return nil, fmt.Errorf("month field: %w", err)
	}
	dow, dowRestricted, err := parseField(fields[4], 0, 7)
	if err != nil {
		return nil, fmt.Errorf("day-of-week field: %w", err)
	}
	// crontab 允许 0 或 7 表示周日；归一到 0 以对齐 time.Weekday。
	if dow&(1<<7) != 0 {
		dow = (dow &^ (1 << 7)) | 1
	}
	return &cronSchedule{
		minute:        minute,
		hour:          hour,
		dom:           dom,
		month:         month,
		dow:           dow,
		domRestricted: domRestricted,
		dowRestricted: dowRestricted,
		loc:           loc,
	}, nil
}

// parseField 解析单个字段为位掩码。支持逗号列表、a-b 范围、*/step 与 a-b/step 步进。
// 返回的 restricted 表示该字段不是纯 "*"（供日/周并集语义判定）。
func parseField(field string, min, max int) (mask uint64, restricted bool, err error) {
	restricted = field != "*"
	for _, item := range strings.Split(field, ",") {
		lo, hi, step := min, max, 1
		hasStep := false
		body := item
		if slash := strings.IndexByte(item, '/'); slash >= 0 {
			s, convErr := strconv.Atoi(item[slash+1:])
			if convErr != nil || s <= 0 {
				return 0, false, fmt.Errorf("invalid step in %q", item)
			}
			step = s
			hasStep = true
			body = item[:slash]
		}
		switch {
		case body == "*":
			// lo/hi 保持 min/max
		case strings.IndexByte(body, '-') >= 0:
			dash := strings.IndexByte(body, '-')
			a, err1 := strconv.Atoi(body[:dash])
			b, err2 := strconv.Atoi(body[dash+1:])
			if err1 != nil || err2 != nil {
				return 0, false, fmt.Errorf("invalid range in %q", item)
			}
			lo, hi = a, b
		default:
			v, convErr := strconv.Atoi(body)
			if convErr != nil {
				return 0, false, fmt.Errorf("invalid value %q", item)
			}
			// 裸单值 lo=hi=v；但 "N/step" 的标准语义是 "N-max/step"（从 N 到 max 步进），
			// 故带步进时 hi 取 max，否则步进被静默丢弃只剩单值。
			lo, hi = v, v
			if hasStep {
				hi = max
			}
		}
		if lo < min || hi > max || lo > hi {
			return 0, false, fmt.Errorf("value out of range [%d,%d] in %q", min, max, item)
		}
		for v := lo; v <= hi; v += step {
			mask |= 1 << uint(v)
		}
	}
	return mask, restricted, nil
}

// next 返回严格晚于 after 的下一个匹配时刻；未来 9 年内无匹配（如 2 月 30 日这类
// 不可能组合）返回零值 + false。
func (c *cronSchedule) next(after time.Time) (time.Time, bool) {
	// 从 after 的下一整分钟开始（清掉秒 / 纳秒）。
	t := after.In(c.loc).Truncate(time.Minute).Add(time.Minute)
	// 上限取 9 年：闰日（2 月 29 日）在跨世纪非闰年处最大间隔达 8 年（如 2096→2104，
	// 因 2100 非闰年），5 年窗口会把合法表达式误判成"永不触发"；9 年覆盖该最大间隔。
	limit := t.AddDate(9, 0, 0)
	for t.Before(limit) {
		if bit(c.month, int(t.Month())) &&
			c.dayMatch(t) &&
			bit(c.hour, t.Hour()) &&
			bit(c.minute, t.Minute()) {
			return t, true
		}
		t = t.Add(time.Minute)
	}
	return time.Time{}, false
}

// dayMatch 实现经典 crontab 的日 / 周匹配：两者都受限时取"并集"（命中其一即算），
// 否则只看受限的那个（都为 * 则恒真）。这是 Vixie cron 的历史行为，务必保留。
func (c *cronSchedule) dayMatch(t time.Time) bool {
	domHit := bit(c.dom, t.Day())
	dowHit := bit(c.dow, int(t.Weekday())) // time.Weekday: 0=Sunday，与 crontab 对齐
	switch {
	case c.domRestricted && c.dowRestricted:
		return domHit || dowHit
	case c.domRestricted:
		return domHit
	case c.dowRestricted:
		return dowHit
	default:
		return true
	}
}

func bit(mask uint64, v int) bool { return mask&(1<<uint(v)) != 0 }
