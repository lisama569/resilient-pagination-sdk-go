// Package sdk 提供带版本协商、限流退避与断点恢复的分页同步客户端。
package sdk

// ErrorKind 对 SDK 归一化后的错误进行分类，调用方可据此区分失败原因。
type ErrorKind int

const (
	ErrKindUnknown ErrorKind = iota
	// ErrKindMalformed 响应结构非法（JSON 无法解析或类型不符）。
	ErrKindMalformed
	// ErrKindMissingField 响应缺失必填字段。
	ErrKindMissingField
	// ErrKindVersionUnknown 协议版本未知（主版本不受支持）。
	ErrKindVersionUnknown
	// ErrKindVersionMismatch 协商后版本不兼容。
	ErrKindVersionMismatch
	// ErrKindCursorInvalid 游标格式非法或已失效。
	ErrKindCursorInvalid
	// ErrKindCursorRegression 游标回退（序号小于已确认进度）。
	ErrKindCursorRegression
	// ErrKindTransient 可重试的临时失败。
	ErrKindTransient
	// ErrKindRateLimited 触发限流，可退避重试。
	ErrKindRateLimited
	// ErrKindCanceled 上下文取消或超时。
	ErrKindCanceled
	// ErrKindCheckpoint 检查点持久化失败。
	ErrKindCheckpoint
)

// Error 是 SDK 对外暴露的归一化错误，底层解析/传输异常一律包装在 Err 中，
// 不向调用方直接透出原始异常类型。
type Error struct {
	Kind ErrorKind
	Op   string
	Msg  string
	Err  error
}

func (e *Error) Error() string { return "" }

func (e *Error) Unwrap() error { return e.Err }

// KindOf 返回错误归一化后的分类；非 SDK 错误归为 ErrKindUnknown。
func KindOf(err error) ErrorKind { return ErrKindUnknown }

// Retryable 报告错误是否可重试。
func Retryable(err error) bool { return false }
