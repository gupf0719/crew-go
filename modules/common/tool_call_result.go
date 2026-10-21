package common

type ToolCallResult struct {
	Success   bool   `json:"success"`
	Output    string `json:"output"`
	Error     string `json:"error"`
	ErrorCode string `json:"errorCode"`
}
