package tenantdata

import (
	"context"
	"errors"
	"testing"
	"time"
)

// 前两次 Scan 暂时性故障、第三次成功：重试后结论成功，审计只写一条且 attempts=3。
func TestRetryTransientThenSuccessSingleAudit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRetry = 3
	cfg.RetryBaseDelay = time.Millisecond
	f := newFixture(t, cfg)
	seedPolicy(t, f, "acme", 1, ScopeRead)
	seedRecords(t, f, "orders", "acme", 2)
	f.data.FailNextScans(2)

	res, err := f.client.Read(context.Background(), validIdentity("acme", "req-retry-ok", ScopeRead),
		ReadOptions{Dataset: "orders", TenantID: "acme"})
	if err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}
	if len(res.Records) != 2 {
		t.Fatalf("records = %d", len(res.Records))
	}
	events := f.audit.EventsByRequest("req-retry-ok")
	if len(events) != 1 {
		t.Fatalf("want exactly 1 audit event despite retries, got %d: %+v", len(events), events)
	}
	if events[0].Attempts != 3 || !events[0].Allowed || events[0].ErrorKind != "ok" {
		t.Fatalf("audit event wrong: %+v", events[0])
	}
}

// 故障次数超过重试预算：结论必须是 unavailable，且与权限/不存在/容量错误可区分。
func TestRetryExhaustedIsUnavailable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRetry = 2
	cfg.RetryBaseDelay = time.Millisecond
	f := newFixture(t, cfg)
	seedPolicy(t, f, "acme", 1, ScopeRead)
	seedRecords(t, f, "orders", "acme", 1)
	f.data.FailNextScans(10)

	_, err := f.client.Read(context.Background(), validIdentity("acme", "req-retry-fail", ScopeRead),
		ReadOptions{Dataset: "orders", TenantID: "acme"})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("want unavailable, got %v", err)
	}
	for _, other := range []error{ErrPermissionDenied, ErrNotFound, ErrCapacityExceeded} {
		if errors.Is(err, other) {
			t.Fatalf("unavailable must not be confused with %v", other)
		}
	}
	events := f.audit.EventsByRequest("req-retry-fail")
	if len(events) != 1 || events[0].Attempts != 2 || events[0].ErrorKind != "unavailable" {
		t.Fatalf("failure audit wrong: %+v", events)
	}
}

// 确定性失败（权限拒绝）绝不重试，即使存在故障钩子。
func TestDeterministicErrorsAreNotRetried(t *testing.T) {
	f := newFixture(t, DefaultConfig())
	seedPolicy(t, f, "acme", 1, ScopeRead)
	seedRecords(t, f, "orders", "acme", 1)

	// 跨租户访问：在触达存储前就确定失败，Scan 故障钩子不应被调用。
	called := 0
	f.data.SetFaultHook(func() error { called++; return nil })
	_, err := f.client.Read(context.Background(), validIdentity("other", "req-denied", ScopeRead),
		ReadOptions{Dataset: "orders", TenantID: "acme"})
	if !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("want tenant_mismatch, got %v", err)
	}
	if called != 0 {
		t.Fatalf("storage must not be touched on deterministic failure, called=%d", called)
	}
	events := f.audit.EventsByRequest("req-denied")
	if len(events) != 1 || events[0].Attempts != 1 {
		t.Fatalf("deny audit should record exactly 1 attempt: %+v", events)
	}
}

// 超过并发上限的请求立即被容量保护拒绝，错误类别稳定，且不进入读取链路。
func TestConcurrencyCapacityProtection(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxConcurrent = 2
	cfg.RetryBaseDelay = time.Millisecond
	f := newFixture(t, cfg)
	seedPolicy(t, f, "acme", 1, ScopeRead)
	seedRecords(t, f, "orders", "acme", 1)

	release := make(chan struct{})
	started := make(chan struct{}, 2)
	block := func(i int) {
		f.data.SetFaultHook(func() error {
			started <- struct{}{}
			<-release
			return nil
		})
	}
	block(0)

	errCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			_, err := f.client.Read(context.Background(), validIdentity("acme", reqID("busy", i), ScopeRead),
				ReadOptions{Dataset: "orders", TenantID: "acme"})
			errCh <- err
		}(i)
	}
	<-started
	<-started

	// 第三个请求应立即被拒绝。
	_, err := f.client.Read(context.Background(), validIdentity("acme", "req-overflow", ScopeRead),
		ReadOptions{Dataset: "orders", TenantID: "acme"})
	if !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("want capacity_exceeded, got %v", err)
	}
	events := f.audit.EventsByRequest("req-overflow")
	if len(events) != 1 || events[0].ErrorKind != "capacity_exceeded" {
		t.Fatalf("capacity audit wrong: %+v", events)
	}

	close(release)
	for i := 0; i < 2; i++ {
		if e := <-errCh; e != nil {
			t.Fatalf("in-flight reads should fail with injected fault path handled: %v", e)
		}
	}
}
