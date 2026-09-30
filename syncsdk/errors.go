// Package syncsdk 提供带版本协商、限流退避与断点恢复的分页同步客户端。
package syncsdk

import "errors"

// Kind 对错误进行可区分的归类，调用方可用 KindOf 判断失败原因。
type Kind string

const (
	// KindUnknownVersion 响应声明的协议版本不被支持。
	KindUnknownVersion Kind = "unknown_version"
	// KindMissingField 响应缺失必填字段。
	KindMissingField Kind = "missing_field"
	// KindMalformed 响应结构非法（JSON 解析失败、字段类型错误等）。
	KindMalformed Kind = "malformed"
	// KindCursorInvalid 游标回退、失效或格式非法，被显式拒绝。
	KindCursorInvalid Kind = "cursor_invalid"
	// KindRetryExhausted 可重试的临时失败在退避重试后仍未成功。
	KindRetryExhausted Kind = "retry_exhausted"
	// KindCanceled 上下文取消或超时导致的中断。
	KindCanceled Kind = "canceled"
	// KindTransport 底层传输失败（不直接向调用方透出原始错误）。
	KindTransport Kind = "transport"
	// KindCheckpointCorrupt 持久化进度损坏、被篡改或与此前内容对不上。
	KindCheckpointCorrupt Kind = "checkpoint_corrupt"
)

// Error 是 SDK 归一化后的错误，底层解析/传输异常被包装在 Cause 中，
// 不会以原始形式透出给调用方。
type Error struct {
	Kind    Kind
	Message string
	Cause   error
	// Stream 标识多流场景下出错的数据流；单流场景为空。
	Stream StreamID
}

func (e *Error) Error() string {
	prefix := string(e.Kind)
	if e.Stream != "" {
		prefix += "[" + string(e.Stream) + "]"
	}
	if e.Cause != nil {
		return prefix + ": " + e.Message + ": " + e.Cause.Error()
	}
	return prefix + ": " + e.Message
}

func (e *Error) Unwrap() error { return e.Cause }

// KindOf 返回 err 归一化后的 Kind；非 SDK 错误返回空串。
func KindOf(err error) Kind {
	var se *Error
	if errors.As(err, &se) {
		return se.Kind
	}
	return ""
}

// IsKind 判断 err 是否属于指定类别。
func IsKind(err error, k Kind) bool { return KindOf(err) == k }
