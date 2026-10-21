package compose_v2_test

// `ErrCheckpointStale` 必须被当作**控制流信号**，不是普通业务错。
//
// # 为什么这件事比它看起来重要
//
// 守卫的全部价值是"把最坏的静默变成可见"。但如果上层把 stale 当普通失败处理，
// 价值不仅归零、还会变成负的——实测过两条链路：
//
//   - **编排 retry**：7 个编排都用 `IsRetryable` 决定要不要重跑子 agent。stale 可重试
//     ⟹ 重跑已经成功的子 agent ⟹ 副作用再来一遍，而最终 `err=nil`（stale 被吞）
//   - **AgentAsTool**：把 stale 转成 `tool.Failure` 喂回父 LLM ⟹ LLM 当普通工具错误
//     重试 ⟹ 扣款做了两次，业务侧 `err=nil` 完全无从察觉
//
// 根因是同一个：stale 的语义是「另一个执行者已经推进了这个会话，你这次的账没记上」。
// **重试不会让账记上**（对方还在那儿），只会把有副作用的节点再跑一遍。这跟
// `InterruptError`（要等人）、ctx 取消（要停）同属"重试没有意义"的那一类。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"crew-go/compose_v2"
)

func TestErrCheckpointStale_IsControlFlowSignal(t *testing.T) {
	stale := fmt.Errorf("%w (probe)", compose_v2.ErrCheckpointStale)

	t.Run("裸 stale 是控制流信号", func(t *testing.T) {
		if !compose_v2.IsControlFlowSignal(compose_v2.ErrCheckpointStale) {
			t.Error("ErrCheckpointStale 必须是控制流信号")
		}
	})

	t.Run("wrap 后仍是", func(t *testing.T) {
		if !compose_v2.IsControlFlowSignal(stale) {
			t.Error("wrap 后应仍被识别（errors.Is 穿透）")
		}
	})

	t.Run("不可重试——重试会让副作用再跑一遍", func(t *testing.T) {
		if compose_v2.IsRetryable(stale) {
			t.Error("stale 不该可重试：重试不会让账记上（对方还在），只会重造副作用。\n" +
				"  实测后果：编排 retry 静默重跑已成功的子 agent，扣款两次而业务拿到 err=nil")
		}
	})

	t.Run("守卫的双 %w 形态：两个语义都判得出、都不可重试", func(t *testing.T) {
		ie := &compose_v2.InterruptError{
			Reason: "q", Query: &compose_v2.AskQuery{InterruptID: "x", Question: "?"},
		}
		both := fmt.Errorf("%w (cp 未落盘: %w)", ie, stale)
		if _, isInt := compose_v2.IsInterrupt(both); !isInt {
			t.Error("原始 InterruptError 必须判得出——上层靠它决定是否等用户答案")
		}
		if !errors.Is(both, compose_v2.ErrCheckpointStale) {
			t.Error("stale 也必须判得出——业务靠它决定要不要补偿")
		}
		if compose_v2.IsRetryable(both) {
			t.Error("不可重试")
		}
	})

	t.Run("普通业务错仍可重试（别误伤）", func(t *testing.T) {
		if !compose_v2.IsRetryable(errors.New("model api 503")) {
			t.Error("普通错该照常重试——守卫的收紧不能波及正常的重试路径")
		}
		if compose_v2.IsControlFlowSignal(context.Canceled) != true {
			t.Error("ctx 取消仍应是控制流信号")
		}
	})
}

// TestStaleWriteError_StructuredForObservability：`StaleWriteError` 必须让观测层
// **不用 parse 字符串**就能拿到低基数的 `Op`。
//
// 观测要给 metric 打 label{stale_op}，若只能从错误文案里正则抠 `op=xxx`，改一次
// 文案就静默失效。同时 `SessionID` / `Baseline` / `Current` 是高基数字段，**绝不能
// 当 label**（炸 cardinality），也不该原样外发（agui / a2a 都做了脱敏）。
func TestStaleWriteError_StructuredForObservability(t *testing.T) {
	var err error = &compose_v2.StaleWriteError{
		Op: "purge-success", SessionID: "sid-1", NS: "sub#3",
		Baseline: "aaa", Current: "bbb", Detail: "槽已被推进",
	}
	// 包一层，模拟真实错误链
	wrapped := fmt.Errorf("%w (outer)", err)

	if !errors.Is(wrapped, compose_v2.ErrCheckpointStale) {
		t.Error("errors.Is(ErrCheckpointStale) 必须成立——业务只关心'是不是陈旧'时用它")
	}
	var sw *compose_v2.StaleWriteError
	if !errors.As(wrapped, &sw) {
		t.Fatal("errors.As 必须能取到结构化形态——否则观测层只能 parse 字符串，改文案就失效")
	}
	if sw.Op != "purge-success" {
		t.Errorf("Op 应可直接当 metric label, got %q", sw.Op)
	}
	if sw.Detail == "" {
		t.Error("Detail 是脱敏后可上报的说明（不含 sid/epoch），不该为空")
	}
	// Error() 里含高基数信息，那是给日志/排查的，不是给 label 或对外消息的
	if !strings.Contains(err.Error(), "sid-1") {
		t.Error("Error() 应含完整定位信息供排查")
	}
}
