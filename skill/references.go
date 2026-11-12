package skill

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

const referenceMetadataLimit = 16 << 10

// ReferenceMatch 是单份参考文档对当前请求的匹配结果。
type ReferenceMatch struct {
	Resource SkillResource
	Score    float64
}

// MatchReferences 按 always_load、相关性及路径顺序返回可自动读取的参考文档。
func MatchReferences(query string, resources []SkillResource) []ReferenceMatch {
	var matches []ReferenceMatch
	for _, ref := range resources {
		if ref.ResourceType != "reference" {
			continue
		}
		text := strings.Join([]string{ref.Description, ref.When, strings.Join(ref.Topics, " ")}, " ")
		score := KeywordScore(query, text)
		if score > 0 || ref.AlwaysLoad {
			matches = append(matches, ReferenceMatch{Resource: ref, Score: score})
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		a, b := matches[i], matches[j]
		if a.Resource.AlwaysLoad != b.Resource.AlwaysLoad {
			return a.Resource.AlwaysLoad
		}
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		return a.Resource.Path < b.Resource.Path
	})
	return matches
}

func loadReferenceMetadata(dir string, ref *SkillResource) {
	raw, _, err := readReferenceFile(dir, ref.Path, referenceMetadataLimit)
	if err != nil {
		return // 文件可能在扫描之后已移走，后续读取会返回错误。
	}
	fm, _ := splitFrontmatter(raw)
	if fm == "" {
		return
	}
	var meta struct {
		Description string   `yaml:"description"`
		When        string   `yaml:"when"`
		Topics      []string `yaml:"topics"`
		AlwaysLoad  bool     `yaml:"always_load"`
	}
	if err := yaml.Unmarshal([]byte(fm), &meta); err != nil {
		slog.Warn("skill: 参考文档元数据解析失败", "path", ref.Path, "err", err)
		return
	}
	ref.Description, ref.When, ref.Topics, ref.AlwaysLoad = meta.Description, meta.When, meta.Topics, meta.AlwaysLoad
}

// ReadSkillReference 读取登记的文本参考文档，去掉 frontmatter，最多返回 maxBytes 字节。
// 截断时 complete 为 false。读取限制在该 skill 的根目录内。
func (b *DefaultSkillBox) ReadSkillReference(name, path string, maxBytes int) (body string, complete bool, err error) {
	if maxBytes <= 0 {
		return "", false, fmt.Errorf("reference size limit must be positive")
	}
	for _, s := range b.skills {
		if s.descriptor.Name != name {
			continue
		}
		if !slices.ContainsFunc(s.resources, func(ref SkillResource) bool {
			return ref.ResourceType == "reference" && ref.Path == path
		}) {
			break
		}
		// 元数据和正文分开计数。上限避免在读取层为大文件分配整份缓冲。
		limit := min(maxBytes, 1<<20)
		raw, complete, err := readReferenceFile(s.dir, path, limit+referenceMetadataLimit)
		if err != nil {
			return "", false, err
		}
		_, body = splitFrontmatter(raw)
		if len(body) > limit {
			n := limit
			for n > 0 && !utf8.RuneStart(body[n]) {
				n--
			}
			body, complete = body[:n], false
		}
		return strings.TrimSpace(body), complete, nil
	}
	return "", false, os.ErrNotExist
}

func readReferenceFile(dir, path string, limit int) (string, bool, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return "", false, err
	}
	defer root.Close() // 只读目录句柄。
	file, err := root.Open(path)
	if err != nil {
		return "", false, err
	}
	defer file.Close() // 只读文件，Close 失败不改变内容。
	info, err := file.Stat()
	if err != nil {
		return "", false, err
	}
	if !info.Mode().IsRegular() {
		return "", false, fmt.Errorf("reference is not a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil {
		return "", false, err
	}
	return string(data[:min(len(data), limit)]), len(data) <= limit, nil
}
