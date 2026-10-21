package skill

import (
	"math"
	"sort"
	"strings"
	"unicode"
)

// matchSkills 对所有 skill 执行关键词匹配，返回按分数降序排列的结果（分数 > 0）。
func matchSkills(skills []loadedSkill, query string) []SkillMatchResult {
	results := make([]SkillMatchResult, 0, len(skills))
	for _, s := range skills {
		score := keywordScore(query, s.matchText)
		if score > 0 {
			results = append(results, SkillMatchResult{
				Skill: s.descriptor,
				Score: score,
			})
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	return results
}

// KeywordScore 是 keywordScore 的导出包装——供包外复用同款 query↔text 关键词评分
// （如 agent_v2 工具级渐进披露的默认检索器）。返回 [0,1]，0 = 完全不匹配。
func KeywordScore(query, matchText string) float64 {
	return keywordScore(query, matchText)
}

// keywordScore 对 query 分词后与 skill matchText 做词频匹配，返回 [0,1] 归一化分数。
// matchText = name + description + whenToUse + tags + examples 的拼接（由 buildMatchText 生成）。
func keywordScore(query string, matchText string) float64 {
	tokens := tokenize(query)
	if len(tokens) == 0 {
		return 0
	}
	text := strings.ToLower(matchText)
	var matched int
	for _, tok := range tokens {
		if strings.Contains(text, tok) {
			matched++
		}
	}
	if matched == 0 {
		return 0
	}
	// 归一化：分母取 max(tokenCount, 2) 避免单词查询分数虚高
	denom := float64(max(len(tokens), 2))
	return math.Min(1.0, float64(matched)/denom)
}

// tokenize 将文本分割为小写 token，支持：
//   - 按空白符分割的英文单词（≥2 字符）
//   - CJK 字符逐字切分（每个汉字/日文/韩文字符单独成 token）
func tokenize(text string) []string {
	var tokens []string
	var buf strings.Builder

	flush := func() {
		if buf.Len() >= 2 {
			tokens = append(tokens, strings.ToLower(buf.String()))
		}
		buf.Reset()
	}

	for _, r := range text {
		if isCJK(r) {
			flush()
			tokens = append(tokens, string(r))
		} else if unicode.IsLetter(r) || unicode.IsDigit(r) {
			buf.WriteRune(r)
		} else {
			flush()
		}
	}
	flush()
	return tokens
}

// isCJK 判断字符是否属于 CJK（中日韩）字符范围。
func isCJK(r rune) bool {
	return (r >= 0x4E00 && r <= 0x9FFF) || // CJK 统一汉字
		(r >= 0x3400 && r <= 0x4DBF) || // CJK 扩展 A
		(r >= 0x20000 && r <= 0x2A6DF) || // CJK 扩展 B
		(r >= 0x3040 && r <= 0x309F) || // 平假名
		(r >= 0x30A0 && r <= 0x30FF) || // 片假名
		(r >= 0xAC00 && r <= 0xD7AF) // 韩文音节
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
