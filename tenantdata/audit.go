package tenantdata

import (
	"context"
	"sync"
	"time"
)

// AuditEvent 是一次请求终态的审计记录。
//
// 重试不会产生多条成功事件：每个请求的成功审计只写一次，
// 失败审计也只在最终结论确定后写入一条。
type AuditEvent struct {
	Time          time.Time `json:"time"`
	RequestID     string    `json:"request_id"`
	ReadID        string    `json:"read_id,omitempty"`
	TenantID      string    `json:"tenant_id"`
	CallerID      string    `json:"caller_id"`
	Dataset       string    `json:"dataset,omitempty"`
	PolicyVersion uint64    `json:"policy_version"`
	Action        string    `json:"action"`
	// CursorPos 记录本次请求后的游标位置（本页最后一条记录 ID）。
	CursorPos string `json:"cursor_pos,omitempty"`
	Page      int    `json:"page,omitempty"`
	// Allowed 为最终授权结论。
	Allowed bool `json:"allowed"`
	// ErrorKind 是归一化错误原因；成功为 "ok"。
	ErrorKind string `json:"error_kind"`
	Attempts  int    `json:"attempts"`
}

// AuditSink 接收审计事件。
type AuditSink interface {
	Write(ctx context.Context, e AuditEvent) error
}

// nopAuditSink 丢弃全部事件，在调用方未提供 Sink 时使用。
type nopAuditSink struct{}

func (nopAuditSink) Write(context.Context, AuditEvent) error { return nil }

// MemoryAuditSink 是线程安全的内存审计留痕实现。
type MemoryAuditSink struct {
	mu     sync.Mutex
	events []AuditEvent
}

// NewMemoryAuditSink 创建内存审计 Sink。
func NewMemoryAuditSink() *MemoryAuditSink {
	return &MemoryAuditSink{}
}

// Write 追加一条审计事件。
func (s *MemoryAuditSink) Write(_ context.Context, e AuditEvent) error {
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
	return nil
}

// Events 返回审计事件快照副本。
func (s *MemoryAuditSink) Events() []AuditEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]AuditEvent, len(s.events))
	copy(out, s.events)
	return out
}

// EventsByRequest 按请求标识检索审计事件。
func (s *MemoryAuditSink) EventsByRequest(requestID string) []AuditEvent {
	var out []AuditEvent
	for _, e := range s.Events() {
		if e.RequestID == requestID {
			out = append(out, e)
		}
	}
	return out
}
