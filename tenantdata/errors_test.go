package tenantdata

import (
	"errors"
	"testing"
	"time"
)

func TestErrorKindsDistinctAndStable(t *testing.T) {
	cases := []struct {
		err  *Error
		kind ErrorKind
		text string
	}{
		{ErrUnauthenticated, KindUnauthenticated, "unauthenticated"},
		{ErrTenantMismatch, KindTenantMismatch, "tenant_mismatch"},
		{ErrPermissionDenied, KindPermissionDenied, "permission_denied"},
		{ErrPolicyStale, KindPolicyStale, "policy_stale"},
		{ErrNotFound, KindNotFound, "not_found"},
		{ErrInvalidCursor, KindInvalidCursor, "invalid_cursor"},
		{ErrCapacityExceeded, KindCapacityExceeded, "capacity_exceeded"},
		{ErrUnavailable, KindUnavailable, "unavailable"},
	}
	seen := map[ErrorKind]bool{}
	for _, c := range cases {
		if c.err.Kind != c.kind {
			t.Fatalf("kind mismatch: got %d want %d", c.err.Kind, c.kind)
		}
		if c.kind.String() != c.text {
			t.Fatalf("normalized text = %q, want %q", c.kind.String(), c.text)
		}
		if seen[c.kind] {
			t.Fatalf("duplicate kind %d", c.kind)
		}
		seen[c.kind] = true

		// 携带上下文的具体错误必须能用 errors.Is 匹配到同类别哨兵。
		specific := newError(c.kind, "req-1", 7, "detail")
		if !errors.Is(specific, c.err) {
			t.Fatalf("errors.Is(%s, sentinel) = false", c.text)
		}
		if Normalize(specific) != c.kind {
			t.Fatalf("Normalize = %s, want %s", Normalize(specific), c.text)
		}
	}

	// 四类关键失败绝不能互相混淆。
	distinct := []error{ErrPermissionDenied, ErrNotFound, ErrCapacityExceeded, ErrUnavailable}
	for i := range distinct {
		for j := range distinct {
			if i != j && errors.Is(distinct[i], distinct[j]) {
				t.Fatalf("distinct errors %v and %v matched each other", distinct[i], distinct[j])
			}
		}
	}
}

func TestIdentityValidAt(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	base := validIdentity("t1", "req-1", ScopeRead)

	missingTenant := base
	missingTenant.TenantID = ""
	if k := Normalize(missingTenant.ValidAt(now)); k != KindUnauthenticated {
		t.Fatalf("missing tenant: %s", k)
	}

	expired := base
	expired.ExpiresAt = now.Add(-time.Second)
	if k := Normalize(expired.ValidAt(now)); k != KindUnauthenticated {
		t.Fatalf("expired identity: %s", k)
	}

	future := base
	future.NotBefore = now.Add(time.Minute)
	if k := Normalize(future.ValidAt(now)); k != KindUnauthenticated {
		t.Fatalf("future identity: %s", k)
	}

	if err := base.ValidAt(now); err != nil {
		t.Fatalf("valid identity rejected: %v", err)
	}
	if !base.HasScope(ScopeRead) || base.HasScope("write") {
		t.Fatal("HasScope behavior wrong")
	}
}
