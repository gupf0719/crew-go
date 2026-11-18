package model

// SupportsToolResultImage 报原生工具结果适配共用的内嵌图片格式。
func SupportsToolResultImage(mediaType string) bool {
	switch mediaType {
	case "image/jpeg", "image/png", "image/gif", "image/webp":
		return true
	}
	return false
}
