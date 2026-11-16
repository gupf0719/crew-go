package agent_v2

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"

	"crew-go/compose_v2"
	"crew-go/schema"
)

const (
	autoCompactPhase    = "auto_compact"
	autoCompactAsked    = "auto_compact_asked"
	autoCompactApproval = "auto_compact_approval"
)

// compactApproval 绑定摘要调用已经提出的问题与当时的请求。请求切换后即使回到原内容也要重新提问。
type compactApproval struct {
	Digest   string `json:"digest"`
	Revision int    `json:"revision"`
}

func init() {
	compose_v2.RegisterValueType[*compactApproval]("*agent_v2.compactApproval")
}

func saveAutoCompactApproval(ctx context.Context, approval *compactApproval) error {
	return compose_v2.SaveStepResult(ctx, autoCompactApproval, approval, compose_v2.Rerunnable(), compose_v2.WithStreams(nil))
}

func autoCompactHadQuestion(ctx context.Context) bool {
	if approval, _ := compose_v2.GetStepResult[*compactApproval](ctx, autoCompactApproval); approval != nil {
		return true
	}
	// 旧进度只用来让已挂起的摘要越过断路器。它没有请求指纹，不能确认任何旧答复。
	asked, _ := compose_v2.GetStepResult[bool](ctx, autoCompactAsked)
	return asked
}

func bindAutoCompactApproval(phaseCtx, stateCtx context.Context, input []*schema.Message) (context.Context, *compactApproval, error) {
	digest, err := autoCompactInputDigest(input)
	if err != nil {
		return nil, nil, fmt.Errorf("auto compact: fingerprint summary request: %w", err)
	}
	approval, _ := compose_v2.GetStepResult[*compactApproval](stateCtx, autoCompactApproval)
	if approval == nil {
		approval = &compactApproval{Digest: digest}
	} else if approval.Digest != digest {
		approval = &compactApproval{Digest: digest, Revision: approval.Revision + 1}
		// 先存新代际。摘要换了请求后可能在提问之前失败，下一次仍要记得旧答复已作废。
		if err := saveAutoCompactApproval(stateCtx, approval); err != nil {
			return nil, nil, fmt.Errorf("auto compact: record changed summary request: %w", err)
		}
	}
	// 摘要代码可以在内部再开相位；答复的身份仍包含原摘要调用及实际请求。
	scope := identityString(autoCompactPhase, compose_v2.SubCallScope(phaseCtx), strconv.Itoa(approval.Revision), digest)
	return compose_v2.WithResumeAnswerScope(phaseCtx, scope), approval, nil
}

// autoCompactInputDigest 按请求顺序计算消息内容、协议块和业务标注的指纹。
// 消息 ID 和模型调用观测信息不参与计算。用精确的 JSON 数字区分相邻大整数。
func autoCompactInputDigest(input []*schema.Message) (string, error) {
	view := make([]*schema.Message, len(input))
	for i, msg := range input {
		if msg == nil {
			continue
		}
		content := *msg
		content.ID, content.Metadata = "", nil
		_, hasRunID := content.Annotations[compose_v2.RunIDAnnotationKey]
		_, hasBatchID := content.Annotations[toolCallBatchIDAnnotation]
		if hasRunID || hasBatchID {
			content.Annotations = maps.Clone(content.Annotations)
			delete(content.Annotations, compose_v2.RunIDAnnotationKey)
			delete(content.Annotations, toolCallBatchIDAnnotation)
		}
		view[i] = &content
	}
	data, err := json.Marshal(view)
	if err != nil {
		return "", err
	}
	// 业务可能用 struct 或 map 表达同一个 JSON 对象。统一键顺序，但不经 float64 解释数字。
	var normalized []any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		return "", err
	}
	data, err = json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}
