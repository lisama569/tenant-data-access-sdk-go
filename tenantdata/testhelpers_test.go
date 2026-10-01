package tenantdata

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// testLogger 收集 SDK 日志，便于断言日志中必须出现的上下文字段。
type testLogger struct {
	mu    sync.Mutex
	lines []string
	t     *testing.T
}

func newTestLogger(t *testing.T) *testLogger {
	return &testLogger{t: t}
}

func (l *testLogger) Printf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	l.mu.Lock()
	l.lines = append(l.lines, line)
	l.mu.Unlock()
	l.t.Log(line)
}

func (l *testLogger) Lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.lines))
	copy(out, l.lines)
	return out
}

// fixture 聚合一次测试需要的全套本地组件。
type fixture struct {
	client *Client
	policy *MemoryPolicyStore
	data   *MemoryDataStore
	audit  *MemoryAuditSink
	now    func() time.Time
	log    *testLogger
}

func newFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	clock := &mutableClock{t: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)}
	policy := NewMemoryPolicyStore()
	data := NewMemoryDataStore()
	audit := NewMemoryAuditSink()
	if cfg.MaxRetry <= 0 {
		cfg = DefaultConfig()
	}
	cfg.Now = clock.get
	c := NewClient(cfg, policy, data, audit)
	log := newTestLogger(t)
	c.SetLogger(log)
	return &fixture{client: c, policy: policy, data: data, audit: audit, now: clock.get, log: log}
}

type mutableClock struct {
	mu sync.Mutex
	t  time.Time
}

func (m *mutableClock) get() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.t
}

func (m *mutableClock) advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.t = m.t.Add(d)
}

func seedPolicy(t *testing.T, f *fixture, tenant string, version uint64, scopes ...string) {
	t.Helper()
	err := f.policy.Put(Policy{
		Version:       version,
		TenantID:      tenant,
		AllowedScopes: scopes,
		EffectiveAt:   f.now().Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("seed policy: %v", err)
	}
}

func seedRecords(t *testing.T, f *fixture, dataset, tenant string, n int) {
	t.Helper()
	if err := f.data.CreateDataset(nil, dataset); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	for i := 0; i < n; i++ {
		r := Record{
			ID:        fmt.Sprintf("rec-%04d", i),
			TenantID:  tenant,
			UpdatedAt: f.now(),
			Payload:   fmt.Sprintf("payload-%d", i),
		}
		if err := f.data.Upsert(nil, dataset, r); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}
}

func validIdentity(tenant, requestID string, scopes ...string) Identity {
	return Identity{
		RequestID: requestID,
		TenantID:  tenant,
		CallerID:  "caller-" + tenant,
		Scopes:    scopes,
		ExpiresAt: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func reqID(prefix string, i int) string {
	return fmt.Sprintf("req-%s-%d", prefix, i)
}

func seedRecordsOnly(t *testing.T, f *fixture, dataset, tenant string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		r := Record{
			ID:        recID(i),
			TenantID:  tenant,
			UpdatedAt: f.now(),
			Payload:   "payload",
		}
		if err := f.data.Upsert(nil, dataset, r); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}
}
