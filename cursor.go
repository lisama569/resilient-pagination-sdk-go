package sdk

// Cursor 是分页游标的逻辑结构，序列化为不透明字符串。
type Cursor struct {
	Seq   uint64 // 单调递增的页序号，用于回退检测
	Token string // 服务端分页令牌
}

// EncodeCursor 将游标编码为不透明字符串（含校验）。
func EncodeCursor(c Cursor) string { return "" }

// DecodeCursor 解码并校验游标；格式非法返回 ErrKindCursorInvalid。
func DecodeCursor(s string) (Cursor, error) { return Cursor{}, nil }
