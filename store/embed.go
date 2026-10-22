package store

import (
	"context"
	"fmt"
	"sort"
)

// Embedder 把文本变成向量。**框架不绑任何 embedding 供应商**——业务传自己的实现进来
// （OpenAI / 本地模型 / 任意 HTTP 服务都行）。
//
// 批量入参是有意的：embedding 服务几乎都按批计费和限流，逐条调用既慢又贵。实现方
// 必须保证**返回顺序与入参一一对应**，长度不等视为错误。
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

// EmbedderFunc 让普通函数直接当 Embedder 用。
type EmbedderFunc func(ctx context.Context, texts []string) ([][]float32, error)

// Embed 实现 [Embedder]。
func (f EmbedderFunc) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	return f(ctx, texts)
}

// IndexConfig 声明「这个 store 要支持语义检索」。
//
// **不配就没有语义检索**——`Search(WithQuery(...))` 里的 query 被忽略，退化成
// filter + 字典序（那是 store 的基础语义，不是缺陷）。这跟业务接 mem0 / 向量数据库
// 后端是两条独立的路：那条路上语义检索由后端自己做，这里的配置不参与。
//
// # Fields：哪些内容参与检索
//
// 决定从一条记录里抽出什么文本去 embed。空 = 用默认抽取（见 [TextExtractor]）。
// 给了字段名时，对编成 JSON 是对象的 value 生效（struct、`map[string]string` 按它读回的
// `map[string]any` 取，跟 filter 按字段命中是同一个形状），按名取值再转文本；一条记录
// 可以产出多段文本，各自 embed，检索时取**分最高的那段**代表这条记录。
//
// # 代价
//
// `Put` 会**同步**调一次 Embedder（网络往返）。批量写入走 [Store.Batch] 能合并成
// 一次调用；单条 Put 就是一次。Embed 失败**不让 Put 失败**——记录照常存下，只是
// 这一条不参与语义检索（`Search` 有 query 时它排在所有打了分的后面）。
type IndexConfig struct {
	// Embedder 是必填的——[IndexConfig] 的全部意义就是它。
	Embedder Embedder
	// Fields 限定对象形态 value 的哪些字段参与检索。空 = 默认抽取。
	Fields []string
	// Extractor 覆盖默认的「从 value 抽文本」规则。空 = [DefaultTextExtractor]。它收到的是业务 Put 进来的原值
	// （按业务自己的类型写）；默认抽取器收到的是 value 的 JSON 形状（实现了 GetContent 的值除外，那一档要原值）。
	Extractor TextExtractor
}

// TextExtractor 从一条记录的 value 里抽出要参与语义检索的文本。
//
// 返回空切片 = 这条记录不参与语义检索（不是错误）。返回多段 = 各自 embed，检索时
// 取分最高的那段代表这条记录。
type TextExtractor func(value any, fields []string) []string

// Validate 检查配置自洽。
func (c *IndexConfig) Validate() error {
	if c == nil {
		return nil
	}
	if c.Embedder == nil {
		return fmt.Errorf("store: IndexConfig.Embedder 必填——不需要语义检索就别配 IndexConfig")
	}
	return nil
}

// extractor 返回生效的抽取器。
func (c *IndexConfig) extractor() TextExtractor {
	if c.Extractor != nil {
		return c.Extractor
	}
	return DefaultTextExtractor
}

// DefaultTextExtractor 是默认的文本抽取规则：
//
//   - `string` → 自己
//   - 实现了 `interface{ GetContent() string }`（框架的消息类型是这个形状）→ 那段内容
//   - `map[string]any` → 给了 fields 就按名取那几个，没给就取全部值里的字符串
//   - 其他 → 不参与语义检索（返回空）
//
// **「其他」这一档是有意不兜底的**：把任意结构 JSON 序列化后 embed，得到的是「这个
// 结构长什么样」的向量而不是「它说了什么」，检索质量比不参与还差。业务的 value 有自己
// 的形状时，给 [IndexConfig.Extractor] 传一个知道怎么读它的函数。
func DefaultTextExtractor(value any, fields []string) []string {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		if v == "" {
			return nil
		}
		return []string{v}
	case interface{ GetContent() string }:
		if c := v.GetContent(); c != "" {
			return []string{c}
		}
		return nil
	case map[string]any:
		return extractFromMap(v, fields)
	default:
		return nil
	}
}

// extractFromMap 从 map value 抽文本。
func extractFromMap(m map[string]any, fields []string) []string {
	if len(fields) > 0 {
		out := make([]string, 0, len(fields))
		for _, f := range fields {
			if s, ok := m[f].(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	// 没指定字段：取所有 string 值。**按 key 排序**——map 迭代顺序随机，不排的话
	// 同一条记录每次 Put 产出的向量顺序都不同，检索结果跟着抖。
	keys := make([]string, 0, len(m))
	for k := range m {
		if s, ok := m[k].(string); ok && s != "" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, m[k].(string))
	}
	return out
}
