package tenantdata

import (
	"sync"
	"time"
)

// 审计判定结果。
const (
	DecisionAllow = "ALLOW"
	DecisionDeny  = "DENY"
)

// AuditEvent 记录一次请求的最终判定结果。
// 每个逻辑请求（含其内部重试）只产生一条最终事件，
// 重试不会重复产生成功审计。
type AuditEvent struct {
	RequestID     string
	TenantID      string
	Caller        string
	Decision      string // DecisionAllow 或 DecisionDeny
	PolicyVersion uint64
	Cursor        string
	ErrCode       ErrCode // 成功时为空
	Records       int
	Attempts      int
	At            time.Time
}

// AuditLog 是线程安全的审计日志。
type AuditLog struct {
	mu     sync.Mutex
	events []AuditEvent
}

func NewAuditLog() *AuditLog { return &AuditLog{} }

// Append 追加一条审计事件。
func (l *AuditLog) Append(e AuditEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

// Events 返回全部审计事件的副本。
func (l *AuditLog) Events() []AuditEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]AuditEvent, len(l.events))
	copy(out, l.events)
	return out
}
