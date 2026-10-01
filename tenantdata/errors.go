// Package tenantdata 提供可本地嵌入的多租户数据访问 SDK。
//
// 它把身份校验、策略判定、数据读取与审计留痕串联成一条确定的执行链路：
// 每个请求都在某个固定的策略版本下完成判定，读取支持游标续读，
// 暂时性故障按策略有限重试，且错误原因按类别稳定归一化。
package tenantdata

import (
	"errors"
	"fmt"
)

// ErrorKind 是归一化后的错误类别，便于调用方稳定分类处理。
type ErrorKind int

const (
	// KindOK 表示无错误。
	KindOK ErrorKind = iota
	// KindUnauthenticated 身份缺失或身份本身无效（如已过期）。
	KindUnauthenticated
	// KindTenantMismatch 请求访问了不属于身份所属租户的资源。
	KindTenantMismatch
	// KindPermissionDenied 租户匹配，但权限范围不包含所需操作。
	KindPermissionDenied
	// KindPolicyStale 判定所依据的策略版本已失效且无法续读。
	KindPolicyStale
	// KindNotFound 资源（数据集/记录）不存在。
	KindNotFound
	// KindInvalidCursor 游标无效、过期或与其所属读取会话不一致。
	KindInvalidCursor
	// KindCapacityExceeded 请求超过并发/批量等容量限制，被主动拒绝。
	KindCapacityExceeded
	// KindUnavailable 暂时性内部故障（重试耗尽后仍失败）。
	KindUnavailable
	// KindInvalidArgument SDK 入参本身非法（编程错误，不参与重试）。
	KindInvalidArgument
)

// String 返回错误类别的稳定字符串标识，用于审计与日志归一化。
func (k ErrorKind) String() string {
	switch k {
	case KindOK:
		return "ok"
	case KindUnauthenticated:
		return "unauthenticated"
	case KindTenantMismatch:
		return "tenant_mismatch"
	case KindPermissionDenied:
		return "permission_denied"
	case KindPolicyStale:
		return "policy_stale"
	case KindNotFound:
		return "not_found"
	case KindInvalidCursor:
		return "invalid_cursor"
	case KindCapacityExceeded:
		return "capacity_exceeded"
	case KindUnavailable:
		return "unavailable"
	case KindInvalidArgument:
		return "invalid_argument"
	default:
		return "unknown"
	}
}

// Error 是 SDK 返回的结构化错误，携带归一化类别与请求上下文。
type Error struct {
	// Kind 是归一化错误类别。
	Kind ErrorKind
	// Message 是人类可读的细节。
	Message string
	// RequestID 是触发错误的请求标识。
	RequestID string
	// PolicyVersion 是判定时使用的策略版本（若尚未解析到策略则为 0）。
	PolicyVersion uint64
}

// Error 实现 error 接口，输出稳定、可解析的单行信息。
func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.RequestID != "" {
		return fmt.Sprintf("tenantdata: kind=%s request=%s policy_version=%d: %s",
			e.Kind, e.RequestID, e.PolicyVersion, e.Message)
	}
	return fmt.Sprintf("tenantdata: kind=%s: %s", e.Kind, e.Message)
}

// Is 支持 errors.Is：同类别即视为同类错误，
// 因此调用方可以直接拿哨兵错误与携带上下文的具体错误比较。
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return e.Kind == t.Kind
}

// newError 构造一条携带请求上下文的结构化错误。
func newError(kind ErrorKind, requestID string, policyVersion uint64, format string, args ...any) *Error {
	return &Error{
		Kind:          kind,
		Message:       fmt.Sprintf(format, args...),
		RequestID:     requestID,
		PolicyVersion: policyVersion,
	}
}

// Normalize 返回错误对应的归一化类别；nil 返回 KindOK，
// 非 SDK 错误保守归为 unavailable。
func Normalize(err error) ErrorKind {
	if err == nil {
		return KindOK
	}
	var target *Error
	if errors.As(err, &target) {
		return target.Kind
	}
	return KindUnavailable
}

// AsError 尝试把 err 转为结构化 *Error。
func AsError(err error) (*Error, bool) {
	var target *Error
	if errors.As(err, &target) {
		return target, true
	}
	return nil, false
}

// 以下哨兵错误便于调用方使用 errors.Is 做快速判断。
var (
	// ErrUnauthenticated 身份缺失或无效。
	ErrUnauthenticated = &Error{Kind: KindUnauthenticated, Message: "unauthenticated request"}
	// ErrTenantMismatch 租户不匹配。
	ErrTenantMismatch = &Error{Kind: KindTenantMismatch, Message: "tenant mismatch"}
	// ErrPermissionDenied 权限不足。
	ErrPermissionDenied = &Error{Kind: KindPermissionDenied, Message: "permission denied"}
)

var (
	// ErrPolicyStale 策略版本失效。
	ErrPolicyStale = &Error{Kind: KindPolicyStale, Message: "policy version is stale"}
	// ErrNotFound 资源不存在。
	ErrNotFound = &Error{Kind: KindNotFound, Message: "resource not found"}
	// ErrInvalidCursor 游标无效。
	ErrInvalidCursor = &Error{Kind: KindInvalidCursor, Message: "invalid cursor"}
	// ErrCapacityExceeded 容量超限被拒绝。
	ErrCapacityExceeded = &Error{Kind: KindCapacityExceeded, Message: "capacity exceeded"}
	// ErrUnavailable 暂时不可用。
	ErrUnavailable = &Error{Kind: KindUnavailable, Message: "service temporarily unavailable"}
)
