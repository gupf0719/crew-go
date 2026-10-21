package model

import (
	"context"
	crew_modules_common "crew-go/modules/common"
)

type MessageStreamReader interface {
	Recv(ctx context.Context) (*crew_modules_common.Message, error)
	Close() error
}
