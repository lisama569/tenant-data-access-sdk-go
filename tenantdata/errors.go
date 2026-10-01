package tenantdata

import "fmt"

// ErrCode 对错误做稳定分类，调用方可据此区分处理。
type ErrCode string

const (
	ErrCodeTenantMismatch   ErrCode = "TENANT_MISMATCH"   // 身份租户与目标租户不一致
	ErrCodeIdentityExpired  ErrCode = "IDENTITY_EXPIRED"  // 请求身份已过有效期
	ErrCodePermissionDenied ErrCode = "PERMISSION_DENIED" // 权限范围不足或策略未放行
	ErrCodePolicyExpired    ErrCode = "POLICY_EXPIRED"    // 策略已过有效期
	ErrCodeNotFound         ErrCode = "NOT_FOUND"         // 资源不存在
	ErrCodeCapacityExceeded ErrCode = "CAPACITY_EXCEEDED" // 并发容量不足，被拒绝
	ErrCodeUnavailable      ErrCode = "UNAVAILABLE"       // 暂时性内部故障，可有限重试
	ErrCodeCursorInvalid    ErrCode = "CURSOR_INVALID"    // 游标失效，无法继续续读
)

// Error 是 SDK 的统一错误类型，携带归一化后的分类与上下文。
type Error struct {
	Code          ErrCode
	Reason        string
	RequestID     string
	PolicyVersion uint64
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s (request=%s policy_version=%d)", e.Code, e.Reason, e.RequestID, e.PolicyVersion)
}

// Retryable 报告该错误是否属于可重试的暂时性故障。
// 只有暂时不可用可重试；容量不足、权限拒绝、数据不存在等均不可重试。
func (e *Error) Retryable() bool { return e.Code == ErrCodeUnavailable }

// newError 构造统一错误。
func newError(code ErrCode, requestID string, policyVersion uint64, format string, args ...any) *Error {
	return &Error{
		Code:          code,
		Reason:        fmt.Sprintf(format, args...),
		RequestID:     requestID,
		PolicyVersion: policyVersion,
	}
}
