package common

type FileData struct {
	FileData string `json:"fileData"`
	Filename string `json:"filename"`
	// URL: A2A url part 对齐（crew-go 新增，spec 无）——文件以 URI 引用（非 inline bytes）。
	// FileData=base64 inline bytes 映射 A2A raw part；URL=URI 引用映射 A2A url part（见
	// a2a/convert.go）。omitempty 向后兼容，现有走 FileData inline 路径不受影响。
	URL string `json:"url,omitempty"`
}
