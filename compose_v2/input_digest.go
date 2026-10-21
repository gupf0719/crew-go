package compose_v2

import (
	"context"
	"fmt"
	"log/slog"
)

// checkInputDigestOnAutoLoad 在 auto-load 捡起存档时比对输入摘要（见 [WithInputDigest] /
// [WithInputIdentity]）。存档里没有摘要、或本次没给时不比。
//
// 对不上时按摘要的角色分两种：
//
//   - 摘要就是这次调用的身份（[WithInputIdentity]，存档槽由它派生）：同一个槽上本该只有同一份
//     输入，对不上只可能是两次不同的调用撞进了同一个槽（哈希碰撞）。续下去就是拿别人的进度当
//     自己的结果，所以拒绝（[errInputIdentityCollision]：不重试、**不清档**）。
//   - 摘要只作核对（[WithInputDigest]，身份另有来源，如调用方给的 key）：同一次调用、输入变了。
//     照样续跑，打一条告警。
func checkInputDigestOnAutoLoad(ctx context.Context, cfg *runOptions, cp *Checkpoint) error {
	if cp.InputDigest == "" || cfg.inputDigest == "" || cfg.inputDigest == cp.InputDigest {
		return nil
	}
	if cfg.inputIsIdentity {
		return &errInputIdentityCollision{msg: fmt.Sprintf(
			"(sid=%q, ns=%q) 两次输入不同的调用落进了同一个存档槽（身份哈希碰撞，存档摘要 %s、本次 %s）"+
				"——给这两次调用各自一个 agent_v2.WithCallKey",
			cfg.sessionID, cfg.graphNS, cp.InputDigest, cfg.inputDigest)}
	}
	slog.WarnContext(ctx, "compose_v2: 这次调用的输入与存档建立时不同，仍按存档续跑，本次输入没有进图",
		"session", cfg.sessionID, "ns", cfg.graphNS, "graph", cp.GraphID,
		"saved_digest", cp.InputDigest, "input_digest", cfg.inputDigest,
		"hint", "这次调用的身份由调用方给定（如 agent_v2.WithCallKey），同一个身份下输入变了；"+
			"真是另一件事的话换一个身份。见 compose_v2.WithInputDigest")
	return nil
}

// errInputIdentityCollision 是「两次输入不同的调用落进了同一个存档槽」的错误类型（见
// [checkInputDigestOnAutoLoad]）。
//
// 实现 [NonRetryable]：碰撞是确定性的，重试原样再撞一遍。
//
// **不用 [ErrRunAborted]**：那捆绑「清 cp 子树」，而本错抛在图开始执行之前，本次一个字节都
// 没写过——槽里那份存档属于**另一次调用**，清掉它是替别人做了决定；错误再冒到父层时父层按
// 终局清整棵树，连兄弟分支还挂着的存档也一起删掉（实测续跑后剩余存档 0 份）。
type errInputIdentityCollision struct{ msg string }

func (e *errInputIdentityCollision) Error() string { return e.msg }

// NonRetryable 实现 [NonRetryable]——碰撞重试必然一样。
func (e *errInputIdentityCollision) NonRetryable() {}
