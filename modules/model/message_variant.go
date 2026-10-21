package model

import (
	crew_modules_common "crew-go/modules/common"
)

type MessageVariant struct {
	Streaming bool                         `json:"streaming"`
	Message   *crew_modules_common.Message `json:"message"`
	Stream    MessageStream                `json:"stream"`
}
