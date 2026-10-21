package redis

import "bytes"

// jsonBytesReader 返 json bytes 的 io.Reader (供 json.Decoder 用).
//
// 直接用 *bytes.Reader 而非 strings.NewReader — bytes.Reader 跟 json.RawMessage
// 一致避免 byte→string→byte 转换.
func jsonBytesReader(b []byte) *bytes.Reader {
	return bytes.NewReader(b)
}
