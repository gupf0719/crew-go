package common

import (
	"context"
)

type ToolCallResultView interface {
	ToLLMString(ctx context.Context) (string, error)
}
