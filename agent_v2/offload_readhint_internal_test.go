package agent_v2

import (
	"strings"
	"testing"

	"crew-go/schema"
)

// TestOffloadPlaceholder_ReadHint 钉住「卸载占位符只在**配了读回工具**时才告诉模型能读回来」。
//
// **why 要这句**：占位符本来只有 `[offloaded:key] preview: …`——模型看得见有东西被卸载、
// 也看得见 key，**但没人告诉它这东西能读回来**，于是它只会按残缺的预览作答。
// 对照的实现每个模板都以「用 X 工具查看」结尾。
//
// **why 必须配了才拼**：那个工具是**业务自己注册**的，框架不铸它。没配却写一句
// 「用 X 读回全文」等于给模型一条它执行不了的指令——**比不写更糟**。
//
// 两个方向都要断言：配了要有、没配**一定不能有**。只断言「配了要有」的话，
// 一个无脑恒拼的实现照样全绿。
func TestOffloadPlaceholder_ReadHint(t *testing.T) {
	src := schema.ToolResultMessage("c1", strings.Repeat("z", 500))

	t.Run("没配就不拼", func(t *testing.T) {
		got := cloneToolResultWithOffloadRef(src, "k1", 50, "")
		if strings.Contains(got.Content, "读回全文") {
			t.Errorf("没配读回工具却告诉模型能读回来——那是一条它执行不了的指令：%q", got.Content)
		}
		if !strings.Contains(got.Content, "[offloaded:k1]") {
			t.Errorf("key 丢了，业务与模型都定位不到卸载出去的内容：%q", got.Content)
		}
	})

	t.Run("配了就拼上工具名", func(t *testing.T) {
		got := cloneToolResultWithOffloadRef(src, "k1", 50, "read_offloaded")
		if !strings.Contains(got.Content, "read_offloaded") {
			t.Errorf("配了读回工具却没告诉模型，卸载出去的内容对它等于丢了：%q", got.Content)
		}
		if !strings.Contains(got.Content, "[offloaded:k1]") {
			t.Errorf("key 丢了：%q", got.Content)
		}
	})

	// **两份正文必须说同一句话**。`ToolResultMessage` 把正文同时存在 Content 和
	// ContentBlocks[].Toolresult.Content 里，而落盘与上 wire 各取一份。
	// 这里原先是两处各拼一遍同样的格式串，然后就漂了——加读回提示时只加进了 Content。
	// 断言选在「两份是否一致」上，不是「各自含不含某个子串」：后者在两份说不同话时
	// 照样能全绿。
	t.Run("两份正文一致", func(t *testing.T) {
		for _, name := range []string{"", "read_offloaded"} {
			got := cloneToolResultWithOffloadRef(src, "k1", 50, name)
			if len(got.ContentBlocks) == 0 || got.ContentBlocks[0] == nil || got.ContentBlocks[0].Toolresult == nil {
				t.Fatalf("前提没搭对：fixture 没有 Toolresult block，测不到第二份正文")
			}
			if got.ContentBlocks[0].Toolresult.Content != got.Content {
				t.Errorf("readToolName=%q 时两份正文不一致：\n  Content = %q\n  block   = %q",
					name, got.Content, got.ContentBlocks[0].Toolresult.Content)
			}
		}
	})
}
