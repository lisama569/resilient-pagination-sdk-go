package syncsdk

import (
	"context"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
)

// Checkpoint 记录已确认的同步进度。
type Checkpoint struct {
	Cursor string // 下一页游标；空串表示从头开始
	Seq    uint64 // 已提交的条目数，用于顺序语义校验
}

// CheckpointStore 抽象检查点持久化，测试中用本地替身实现。
type CheckpointStore interface {
	Load(ctx context.Context) (*Checkpoint, error)
	// Save 仅在 store 内游标与 cp 的前置状态一致时推进（乐观并发）。
	Save(ctx context.Context, cp *Checkpoint) error
}

// 游标格式：v<version>.<offset>.<crc32 校验和>，对调用方不透明。
func cursorPayload(version, offset int) string {
	return fmt.Sprintf("v%d.%d", version, offset)
}

// EncodeCursor 将内部游标状态编码为带校验和的不透明字符串。
func EncodeCursor(offset int, version int) string {
	p := cursorPayload(version, offset)
	sum := crc32.ChecksumIEEE([]byte(p))
	return fmt.Sprintf("%s.%08x", p, sum)
}

// DecodeCursor 解码并校验游标；格式非法或校验和不匹配返回 KindCursorInvalid。
func DecodeCursor(s string) (offset int, version int, err error) {
	bad := func(msg string) (int, int, error) {
		return 0, 0, &Error{Kind: KindCursorInvalid, Message: msg}
	}
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return bad("cursor must have 3 segments: v<version>.<offset>.<checksum>")
	}
	if !strings.HasPrefix(parts[0], "v") {
		return bad("cursor version segment malformed")
	}
	version, err = strconv.Atoi(parts[0][1:])
	if err != nil {
		return bad("cursor version is not an integer")
	}
	offset, err = strconv.Atoi(parts[1])
	if err != nil || offset < 0 {
		return bad("cursor offset is not a non-negative integer")
	}
	want := fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(cursorPayload(version, offset))))
	if parts[2] != want {
		return bad("cursor checksum mismatch (tampered or corrupted)")
	}
	return offset, version, nil
}

// ValidateResume 校验 next 相对 last 是否为合法推进：
// 序列号回退、游标回退、游标失效（不可解码）都必须显式拒绝，
// 不得从中间继续。
func ValidateResume(last *Checkpoint, next *Checkpoint) error {
	if last == nil || last.Cursor == "" {
		return nil // 首次运行，任何起点都合法
	}
	if next == nil || next.Cursor == "" {
		return &Error{Kind: KindCursorInvalid, Message: "resume regresses to empty cursor"}
	}
	if next.Seq < last.Seq {
		return &Error{Kind: KindCursorInvalid,
			Message: fmt.Sprintf("sequence regression: %d < %d", next.Seq, last.Seq)}
	}
	lastOff, _, err := DecodeCursor(last.Cursor)
	if err != nil {
		return err
	}
	nextOff, _, err := DecodeCursor(next.Cursor)
	if err != nil {
		return err
	}
	if nextOff < lastOff {
		return &Error{Kind: KindCursorInvalid,
			Message: fmt.Sprintf("cursor regression: offset %d < %d", nextOff, lastOff)}
	}
	return nil
}
