package common

type LogProb struct {
	Token   int32   `json:"token"`
	Bytes   []int32 `json:"bytes"`
	Logprob int32   `json:"logprob"`
}
