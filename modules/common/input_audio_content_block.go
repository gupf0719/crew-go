package common

type InputAudioContentBlock struct {
	Type       ContentBlockType `json:"type"`
	InputAudio *InputAudio      `json:"inputAudio"`
}
