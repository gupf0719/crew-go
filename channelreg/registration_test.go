// Package channelreg_test 守一条**跨包**的不变量：框架自己声明的每一个 channel，
// 它的值类型都必须注册进序列化类型表。
//
// # 为什么这个哨兵必须在 agent_v2 之外
//
// 它守的东西恰恰只在「没人替你注册」时才会失败，而同一个测试二进制里任何一个
// `RegisterValueType[T]` 都会把窟窿补上。真实案例：`[]string` 漏注册长期没被发现，
// 因为 `agent_v2` 的另一个测试文件里有一行无关的 `RegisterValueType[[]string]`——
// 那个 feature 自己的回归用例靠它才通过，`go test ./...` 对这条 bug 零分辨力。
//
// 后果不是「那个 channel 存不下来」，是**整份 checkpoint 存不下来**：Snapshot 逐个
// marshal channel 值，一个失败整份快照失败。实测表现是 ListPending=0、Resume 返
// ErrNoPendingCheckpoint，HITL 整条链死掉，而 Run 那次的错误里只有一句 snapshot failed。
package channelreg_test

import (
	"reflect"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
)

// 依赖 agent_v2 的 init 完成注册。
var _ = agent_v2.NewRunSession

// TestFrameworkChannelValueTypes_AllRegistered 逐个查框架 channel 的值类型。
//
// **新增 channel 时把它的值类型加进下表**——漏了就是一次「整份 cp 存不下来」。
func TestFrameworkChannelValueTypes_AllRegistered(t *testing.T) {
	cases := []struct {
		what string
		typ  reflect.Type
	}{
		{"[]*schema.Message（消息流）", reflect.TypeOf([]*schema.Message(nil))},
		{"int（轮次 / 计数）", reflect.TypeOf(0)},
		{"string（单值）", reflect.TypeOf("")},
		{"bool（标志位）", reflect.TypeOf(false)},
		{"*schema.Message（单条消息）", reflect.TypeOf((*schema.Message)(nil))},
		{"[]string（工具渐进披露的已解锁集）", reflect.TypeOf([]string(nil))},
	}
	for _, c := range cases {
		name, ok := compose_v2.LookupKey(c.typ)
		if !ok {
			t.Errorf("%s 的值类型没注册——它所在的图一旦要存档，**整份 cp 都存不下来**"+
				"（Snapshot 逐个 marshal，一个失败全份失败），业务侧表现是 ListPending=0 + "+
				"Resume 返 ErrNoPendingCheckpoint", c.what)
			continue
		}
		t.Logf("%s → wire tag %q", c.what, name)
	}
}
