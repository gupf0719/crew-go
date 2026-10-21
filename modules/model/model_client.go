package model

import (
	"context"
	crew_modules_common "crew-go/modules/common"
)

type ModelClient interface {
	Chat(ctx context.Context, messages []*crew_modules_common.Message, stream bool, modelConfig *crew_modules_common.ModelConfig) (*MessageVariant, error)
}
