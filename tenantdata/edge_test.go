package tenantdata

import (
	"context"
	"errors"
	"testing"
	"time"
)

// 策略带过期时间，时钟越过过期点后的新请求必须得到 KindPolicyStale。
func TestExpiredPolicyRejectedThroughClient(t *testing.T) {
	cfg := DefaultConfig()
	clock := &mutableClock{t: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)}
	cfg.Now = clock.get
	policy := NewMemoryPolicyStore()
	data := NewMemoryDataStore()
	audit := NewMemoryAuditSink()
	c := NewClient(cfg, policy, data, audit)
	c.SetLogger(newTestLogger(t))
	ctx := context.Background()

	if err := data.CreateDataset(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := policy.Put(Policy{
		Version:       1,
		TenantID:      "acme",
		AllowedScopes: []string{ScopeRead},
		EffectiveAt:   clock.t.Add(-time.Minute),
		ExpiresAt:     clock.t.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if err := data.Upsert(ctx, "orders", Record{ID: "r1", TenantID: "acme"}); err != nil {
		t.Fatal(err)
	}

	id := validIdentity("acme", "req-exp", ScopeRead)
	if _, err := c.Read(ctx, id, ReadOptions{Dataset: "orders", TenantID: "acme"}); err != nil {
		t.Fatalf("before expiry should succeed: %v", err)
	}

	clock.advance(2 * time.Hour)
	_, err := c.Read(ctx, id, ReadOptions{Dataset: "orders", TenantID: "acme"})
	if !errors.Is(err, ErrPolicyStale) {
		t.Fatalf("want policy_stale after expiry, got %v", err)
	}
	if e, _ := AsError(err); e.PolicyVersion != 1 {
		t.Fatalf("error should cite version 1, got %d", e.PolicyVersion)
	}
}

// 身份过期 -> unauthenticated。
func TestExpiredIdentityRejected(t *testing.T) {
	f := newFixture(t, DefaultConfig())
	seedPolicy(t, f, "acme", 1, ScopeRead)
	seedRecords(t, f, "orders", "acme", 1)
	id := validIdentity("acme", "req-idexp", ScopeRead)
	id.ExpiresAt = f.now().Add(-time.Second)
	_, err := f.client.Read(context.Background(), id, ReadOptions{Dataset: "orders", TenantID: "acme"})
	if !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("want unauthenticated, got %v", err)
	}
}

// 旧游标重放：必须从上次位置之后继续，不重复已读记录。
func TestCursorReplayDoesNotRepeat(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DefaultPageSize = 2
	cfg.MaxPageSize = 2
	f := newFixture(t, cfg)
	seedPolicy(t, f, "acme", 1, ScopeRead)
	seedRecords(t, f, "orders", "acme", 5)
	ctx := context.Background()
	id := validIdentity("acme", "req-replay", ScopeRead)

	p1, err := f.client.Read(ctx, id, ReadOptions{Dataset: "orders", TenantID: "acme", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	p2, err := f.client.ContinueRead(ctx, id, p1.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	// 用同一个游标（p1.NextCursor）再发一次，必须得到与 p2 相同的页面，而非 p1。
	replay, err := f.client.ContinueRead(ctx, id, p1.NextCursor)
	if err != nil {
		t.Fatalf("replaying a still-valid cursor should serve the same page: %v", err)
	}
	if !sameStrings(recordIDs(p2.Records), recordIDs(replay.Records)) {
		t.Fatalf("replay changed page: p2=%v replay=%v", recordIDs(p2.Records), recordIDs(replay.Records))
	}
	if overlap(recordIDs(p1.Records), recordIDs(replay.Records)) {
		t.Fatalf("replay repeated already-read records: %v vs %v",
			recordIDs(p1.Records), recordIDs(replay.Records))
	}
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func overlap(a, b []string) bool {
	m := map[string]bool{}
	for _, s := range a {
		m[s] = true
	}
	for _, s := range b {
		if m[s] {
			return true
		}
	}
	return false
}
