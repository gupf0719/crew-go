package agent_v2

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// TestTruncateMiddle_Invariants 钉住 [truncateMiddle] godoc 承诺的四条保证。
//
// 用**遍历预算**的方式跑，而不是挑几个手选的值：这个函数的分支（够不够挖空、
// 头尾怎么分、切点回退多少字节）全都由预算与内容的相对大小决定，手选几个值很容易
// 正好全落在同一条分支上——那样测了等于没测。
func TestTruncateMiddle_Invariants(t *testing.T) {
	inputs := map[string]string{
		"纯 ASCII":   strings.Repeat("abcdefghij", 200),
		"纯中文":       strings.Repeat("中文内容测试", 200),
		"中英混排":      strings.Repeat("mixed 混排 content 内容 ", 120),
		"含 emoji":   strings.Repeat("ok✅fail❌", 200),
		"单个超长 rune": strings.Repeat("𝕏", 500), // 4 字节 rune
	}
	for name, s := range inputs {
		for _, maxBytes := range []int{1, 2, 3, 8, 16, 32, 40, 64, 100, 256, 1000, len(s) - 1, len(s), len(s) + 1} {
			got := truncateMiddle(s, maxBytes)

			if len(got) > maxBytes && maxBytes > 0 {
				t.Errorf("[%s maxBytes=%d] 超预算：出 %d 字节", name, maxBytes, len(got))
			}
			if !utf8.ValidString(got) {
				t.Errorf("[%s maxBytes=%d] 切出了半个字符（非法 UTF-8）", name, maxBytes)
			}
			if len(s) <= maxBytes {
				// 本来就装得下 ⟹ 原样返回，**不许白加标记**。
				if got != s {
					t.Errorf("[%s maxBytes=%d] 装得下却动了它：%d → %d 字节",
						name, maxBytes, len(s), len(got))
				}
				continue
			}
			if maxBytes > 0 && len(got) >= len(s) {
				t.Errorf("[%s maxBytes=%d] 该截却没变短：%d → %d 字节",
					name, maxBytes, len(s), len(got))
			}
		}
	}
}

// TestTruncateMiddle_KeepsHeadAndTail 钉住「保留的是头**和**尾」——只留头是另一个函数
// （[clampPrefixToUTF8]）的行为，两者别混。
//
// 断言选在**位置**上而不是「长度变没变」：一个只留头的实现同样会让长度变短、同样是合法
// UTF-8、同样不超预算——前面那条不变量用例对它**全绿**。要抓「尾巴丢了」只能断言尾巴还在。
func TestTruncateMiddle_KeepsHeadAndTail(t *testing.T) {
	const head, tail = "HEAD-MARK", "TAIL-MARK"
	s := head + strings.Repeat("x", 5000) + tail

	got := truncateMiddle(s, 400)
	if !strings.HasPrefix(got, head) {
		t.Errorf("头没保住：%.60q", got)
	}
	if !strings.HasSuffix(got, tail) {
		t.Errorf("尾没保住（退化成只留头了？）：%.60q", got)
	}
	if !strings.Contains(got, "中间省略") {
		t.Errorf("接缝处没留痕——模型会把截短的内容当成完整内容用：%.80q", got)
	}
	// 头:尾 = 4:1（不是对半）——这个比例是有理由的选择，不是随手写的默认值。
	i := strings.Index(got, "\n…（中间省略")
	j := strings.LastIndex(got, "）…\n")
	if i < 0 || j < 0 {
		t.Fatalf("标记形态变了，用例的切分假设失效：%q", got)
	}
	gotHead, gotTail := i, len(got)-(j+len("）…\n"))
	if gotHead <= gotTail*2 {
		t.Errorf("头尾比例不对：头 %d 尾 %d，期望头明显多于尾（4:1）", gotHead, gotTail)
	}
}

// TestTruncateMiddle_TinyBudgetDegradesToPrefix 钉住「预算小到挖空不划算时退化成只留头」
// ——而不是产出一条几乎全是标记、只剩两三字节内容的垃圾。
func TestTruncateMiddle_TinyBudgetDegradesToPrefix(t *testing.T) {
	s := strings.Repeat("内容", 100)
	got := truncateMiddle(s, 20) // 远小于标记本身
	if strings.Contains(got, "中间省略") {
		t.Errorf("预算这么小还硬挖空，结果几乎全是标记：%q", got)
	}
	if len(got) > 20 {
		t.Errorf("超预算：%d 字节", len(got))
	}
	if !utf8.ValidString(got) {
		t.Errorf("切出了半个字符：%q", got)
	}
}

// TestClampSuffixToUTF8 单独钉后缀裁剪的方向——它跟前缀是**反的**：前缀往回退、
// 后缀必须往后推，两者都得往「更短」的方向走才不超预算。写反了会超预算且切出半个字符。
func TestClampSuffixToUTF8(t *testing.T) {
	s := "abc中文def" // 中/文 各 3 字节
	for n := 0; n <= len(s)+2; n++ {
		got := clampSuffixToUTF8(s, n)
		if len(got) > n && n < len(s) {
			t.Errorf("n=%d 超预算：出 %d 字节 %q", n, len(got), got)
		}
		if !utf8.ValidString(got) {
			t.Errorf("n=%d 切出半个字符：%q", n, got)
		}
		if !strings.HasSuffix(s, got) {
			t.Errorf("n=%d 取的不是后缀：%q", n, got)
		}
	}
}
