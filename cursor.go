package sdk

import (
	"encoding/base64"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
)

// 游标格式（不透明字符串）：
//
//	sdk1.<base64url("<seq>:<token>")>.<crc32 校验和（8 位十六进制）>
//
// Seq 为单调递增的页序号，用于回退检测；Token 为服务端分页令牌。
// 校验和覆盖编码前 payload，任何篡改或截断都会导致解码失败。
const cursorPrefix = "sdk1"

// Cursor 是分页游标的逻辑结构，序列化为不透明字符串。
type Cursor struct {
	Seq   uint64 // 单调递增的页序号，用于回退检测
	Token string // 服务端分页令牌
}

// EncodeCursor 将游标编码为不透明字符串（含校验）。
func EncodeCursor(c Cursor) string {
	payload := fmt.Sprintf("%d:%s", c.Seq, c.Token)
	sum := crc32.ChecksumIEEE([]byte(payload))
	return fmt.Sprintf("%s.%s.%08x",
		cursorPrefix,
		base64.RawURLEncoding.EncodeToString([]byte(payload)),
		sum)
}

// DecodeCursor 解码并校验游标；格式非法或校验失败返回 ErrKindCursorInvalid。
func DecodeCursor(s string) (Cursor, error) {
	const op = "DecodeCursor"
	parts := strings.Split(s, ".")
	if len(parts) != 3 || parts[0] != cursorPrefix {
		return Cursor{}, wrap(ErrKindCursorInvalid, op,
			fmt.Sprintf("游标结构非法: %q", s), nil)
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Cursor{}, wrap(ErrKindCursorInvalid, op, "游标编码非法", err)
	}
	want, err := strconv.ParseUint(parts[2], 16, 32)
	if err != nil {
		return Cursor{}, wrap(ErrKindCursorInvalid, op, "游标校验和非法", err)
	}
	if got := uint64(crc32.ChecksumIEEE(raw)); got != want {
		return Cursor{}, wrap(ErrKindCursorInvalid, op, "游标校验和不匹配", nil)
	}
	body := strings.SplitN(string(raw), ":", 2)
	if len(body) != 2 {
		return Cursor{}, wrap(ErrKindCursorInvalid, op, "游标载荷非法", nil)
	}
	seq, err := strconv.ParseUint(body[0], 10, 64)
	if err != nil {
		return Cursor{}, wrap(ErrKindCursorInvalid, op, "游标序号非法", err)
	}
	return Cursor{Seq: seq, Token: body[1]}, nil
}
