// Package sdk 提供带版本协商、限流退避与断点恢复的分页同步客户端。
package sdk

import (
	"errors"
	"fmt"
)

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

// String 返回错误分类的稳定字符串表示，便于日志与测试断言。
func (k ErrorKind) String() string {
	switch k {
	case ErrKindMalformed:
		return "malformed"
	case ErrKindMissingField:
		return "missing_field"
	case ErrKindVersionUnknown:
		return "version_unknown"
	case ErrKindVersionMismatch:
		return "version_mismatch"
	case ErrKindCursorInvalid:
		return "cursor_invalid"
	case ErrKindCursorRegression:
		return "cursor_regression"
	case ErrKindTransient:
		return "transient"
	case ErrKindRateLimited:
		return "rate_limited"
	case ErrKindCanceled:
		return "canceled"
	case ErrKindCheckpoint:
		return "checkpoint"
	default:
		return "unknown"
	}
}

// Error 是 SDK 对外暴露的归一化错误，底层解析/传输异常一律包装在 Err 中，
// 不向调用方直接透出原始异常类型。
type Error struct {
	Kind ErrorKind
	Op   string // 出错的操作，如 "DecodePage"、"FetchPage"
	Msg  string // 面向调用方的说明
	Err  error  // 被包装的底层错误（可为 nil）
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("sdk: %s: %s [%s]: %v", e.Op, e.Msg, e.Kind, e.Err)
	}
	return fmt.Sprintf("sdk: %s: %s [%s]", e.Op, e.Msg, e.Kind)
}

func (e *Error) Unwrap() error { return e.Err }

// wrap 构造归一化错误，底层异常仅作为被包装原因保存。
func wrap(kind ErrorKind, op, msg string, err error) *Error {
	return &Error{Kind: kind, Op: op, Msg: msg, Err: err}
}

// KindOf 返回错误归一化后的分类；非 SDK 错误归为 ErrKindUnknown。
func KindOf(err error) ErrorKind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return ErrKindUnknown
}

// Retryable 报告错误是否可重试：临时失败与限流可重试，
// 版本、结构、游标类错误不可重试。
func Retryable(err error) bool {
	switch KindOf(err) {
	case ErrKindTransient, ErrKindRateLimited:
		return true
	default:
		return false
	}
}
