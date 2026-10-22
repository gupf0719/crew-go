package agent_v2

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	"crew-go/store"
)

func readSpilled(t *testing.T, content string, args map[string]any) string {
	t.Helper()
	st := store.NewInMemoryStore()
	cfg := &ToolResultSpillConfig{Store: st}
	ctx := context.Background()
	if err := st.Put(ctx, spillNamespace(ctx), "sp_x", content); err != nil {
		t.Fatal(err)
	}
	args["id"] = "sp_x"
	res := readToolOutput(ctx, cfg, args)
	if res.Error != nil {
		return "[" + res.Error.Code + "] " + res.Error.Message
	}
	return fmt.Sprintf("%v", res.Output)
}

// 按正则搜、扫到了末尾（只有第 10 行匹配）：结尾不该再说「后面还有 N 行；接着读用 offset=11」。
func TestReadToolOutput_PatternFooterAfterFullScan(t *testing.T) {
	var b strings.Builder
	for i := 1; i <= 3000; i++ {
		if i == 10 {
			b.WriteString("NEEDLE\n")
			continue
		}
		fmt.Fprintf(&b, "line %d\n", i)
	}
	got := readSpilled(t, b.String(), map[string]any{"pattern": "NEEDLE"})
	t.Logf("%s", got)
	if strings.Contains(got, "接着") || !strings.Contains(got, "已搜到末尾") {
		t.Fatalf("已经扫到末尾、没有更多匹配，结尾应说搜到末尾、不再让模型接着读：\n%s", got)
	}
}

// 「每次最多返回 16000 字节」：整段输出（抬头 + 行 + 结尾）不该超过它。
func TestReadToolOutput_CapStrict(t *testing.T) {
	line := strings.Repeat("x", 1990)
	content := strings.Repeat(line+"\n", 100)
	got := readSpilled(t, content, map[string]any{"limit": float64(2000)})
	if len(got) > readToolOutputMaxBytes {
		t.Fatalf("一次返回 %d 字节，超过声明的上限 %d", len(got), readToolOutputMaxBytes)
	}
}

// TestReadToolOutput_CapStrictWithEscapedPattern：按正则搜时抬头里显示的正则经 %q 转义，反斜杠多的正则转义之后长一倍，
// 整段输出照样不超过上限。曾经给抬头第二段与结尾留固定 400 字节，这种正则下一次返回 16014 字节（审计实测）。
func TestReadToolOutput_CapStrictWithEscapedPattern(t *testing.T) {
	pattern := strings.Repeat(`\d`, 100)
	for width := 1500; width <= 2000; width += 13 {
		content := strings.Repeat(strings.Repeat("7", width)+"\n", 100)
		got := readSpilled(t, content, map[string]any{"pattern": pattern, "limit": float64(2000)})
		if len(got) > readToolOutputMaxBytes {
			t.Fatalf("行长 %d：一次返回 %d 字节，超过声明的上限 %d", width, len(got), readToolOutputMaxBytes)
		}
	}
}

// TestReadToolOutput_NoticeCountsFoldedLines：转存说明里的「N 行」跟取回工具的「共 N 行」是同一个口径——超长的行
// 在两边都按折行算。按换行数的话，说明说 3 行、取回工具说共 5 行，模型拿说明里的行数去定 offset 就对不上。
func TestReadToolOutput_NoticeCountsFoldedLines(t *testing.T) {
	content := "LINE1\n" + strings.Repeat("y", 5000) + "\nLINE3"
	cfg := &ToolResultSpillConfig{Threshold: 100, PreviewBytes: 50, Store: store.NewInMemoryStore()}
	ctx := context.Background()
	kept, id, _ := spillToolResult(ctx, cfg, "dump", content)
	if id == "" {
		t.Fatal("fixture 没搭对：没转存")
	}
	got := fmt.Sprintf("%v", readToolOutput(ctx, cfg, map[string]any{"id": id}).Output)
	m1 := regexp.MustCompile(`（(\d+) 行）`).FindStringSubmatch(kept)
	m2 := regexp.MustCompile(`共 (\d+) 行`).FindStringSubmatch(got)
	if m1 == nil || m2 == nil || m1[1] != m2[1] {
		t.Fatalf("转存说明与取回工具的行数口径不一致：说明 %v，取回 %v", m1, m2)
	}
}

// 多字节字符折行不切出半个字符（期望绿）。
func TestReadToolOutput_FoldMultibyteValid(t *testing.T) {
	for _, s := range []string{strings.Repeat("中", 5000), "a" + strings.Repeat("😀", 2000), strings.Repeat("é", 3001)} {
		for i, l := range foldLines(s, readToolOutputLineBytes) {
			if !utf8.ValidString(l) || len(l) > readToolOutputLineBytes {
				t.Fatalf("折出的第 %d 行非法或超宽：len=%d valid=%v", i, len(l), utf8.ValidString(l))
			}
		}
	}
}

// 参数边界（期望绿）：负数 / 0 / 超大 / 非整数 / 非法正则 / pattern + offset。
func TestReadToolOutput_ArgEdges(t *testing.T) {
	var b strings.Builder
	for i := 1; i <= 50; i++ {
		fmt.Fprintf(&b, "row %d\n", i)
	}
	c := b.String()
	cases := []map[string]any{
		{"offset": float64(-5)}, {"offset": float64(0), "limit": float64(0)}, {"limit": float64(1e20)},
		{"offset": float64(1e20)}, {"offset": 2.7, "limit": 1.9}, {"pattern": "("}, {"pattern": "row 4", "offset": float64(41)},
		{"offset": "10"},
	}
	for _, a := range cases {
		t.Logf("%v => %.160q", a, readSpilled(t, c, a))
	}
}

// TestReadToolOutput_StringNumberArgs：模型偶尔把 offset / limit 写成字符串（"10"），照样认，不静默按默认值从第 1 行读。
func TestReadToolOutput_StringNumberArgs(t *testing.T) {
	var b strings.Builder
	for i := 1; i <= 50; i++ {
		fmt.Fprintf(&b, "row %d\n", i)
	}
	got := readSpilled(t, b.String(), map[string]any{"offset": "10", "limit": "2"})
	if !strings.Contains(got, "10| row 10") || strings.Contains(got, "12| row 12") || strings.Contains(got, "1| row 1\n") {
		t.Fatalf("offset / limit 写成字符串时应从第 10 行读 2 行：\n%s", got)
	}
}
