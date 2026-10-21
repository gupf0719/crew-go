package agui

import (
	"crypto/rand"
	"fmt"
)

// newThreadID 铸一个 UUIDv4 形状的会话标识，给「客户端没带 threadId」的新一轮运行用。
//
// 为什么这一层要自己铸而不是复用别处：`agent_v2` 的同款生成器不导出，而这里只需要
// 一个「唯一 + 能回传给前端当续跑句柄」的字符串，不值得为它开一个公开 API。
// 形状对齐 `RunSession.RunID`（UUIDv4），前端和日志里看起来是同一类东西。
func newThreadID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
