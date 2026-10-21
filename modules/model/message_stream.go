package model

import (
	"context"
	crew_modules_common "crew-go/modules/common"
)

type MessageStream interface {
	Push(ctx context.Context, msg *crew_modules_common.Message) error
	Recv(ctx context.Context) (*crew_modules_common.Message, error)
	Close() error
}
