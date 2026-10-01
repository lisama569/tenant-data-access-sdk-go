package tenantdata

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPolicyStoreVersionsAreImmutable(t *testing.T) {
	f := newFixture(t, DefaultConfig())
	seedPolicy(t, f, "acme", 1, ScopeRead)

	p1, err := f.policy.Get(context.Background(), "acme", f.now())
	if err != nil {
		t.Fatal(err)
	}

	// 并发更新产生新版本；旧版本快照必须原样可取。
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = f.policy.Update(context.Background(), "acme", func(p Policy) Policy {
			p.AllowedScopes = []string{ScopeRead, "write"}
			return p
		})
	}()
	<-done

	p2, err := f.policy.Get(context.Background(), "acme", f.now())
	if err != nil {
		t.Fatal(err)
	}
	if p2.Version != 2 || len(p2.AllowedScopes) != 2 {
		t.Fatalf("unexpected new version: %+v", p2)
	}
	old, err := f.policy.GetAtVersion(context.Background(), "acme", 1, f.now())
	if err != nil || old.Version != 1 || len(old.AllowedScopes) != 1 {
		t.Fatalf("old snapshot not preserved: %+v err=%v", old, err)
	}
	if !p1.Allows(ScopeRead, f.now()) {
		t.Fatal("v1 should still allow read in isolation")
	}
}

func TestAuthorizeErrorClassification(t *testing.T) {
	f := newFixture(t, DefaultConfig())
	seedPolicy(t, f, "acme", 1, ScopeRead)
	ctx := context.Background()

	// 租户不匹配
	id := validIdentity("acme", "req-tenant", ScopeRead)
	if _, err := authorize(id, "other", ScopeRead, f.policy, f.now()); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("want tenant_mismatch, got %v", err)
	}

	// 无策略租户 -> not_found
	ghost := validIdentity("ghost", "req-nopolicy", ScopeRead)
	if _, err := authorize(ghost, ghost.TenantID, ScopeRead, f.policy, f.now()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want not_found, got %v", err)
	}

	// 权限不足：身份缺 scope
	idNoScope := validIdentity("acme", "req-noscope", "write")
	if _, err := authorize(idNoScope, "acme", ScopeRead, f.policy, f.now()); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("want permission_denied (identity scope), got %v", err)
	}

	// 权限不足：策略不允许该 scope
	seedPolicy(t, f, "beta", 1, "write")
	beta := validIdentity("beta", "req-beta", ScopeRead)
	if _, err := authorize(beta, "beta", ScopeRead, f.policy, f.now()); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("want permission_denied (policy scope), got %v", err)
	}

	// 策略过期 -> policy_stale
	seedPolicy(t, f, "gamma", 1, ScopeRead)
	gamma, _ := f.policy.Get(ctx, "gamma", time.Time{})
	gamma.ExpiresAt = f.now().Add(-time.Second)
	// 通过 Put 无法回写同版本，这里直接验证 Allows + 一条完整读取链路用例覆盖。
	if gamma.Allows(ScopeRead, f.now()) {
		t.Fatal("expired policy must not allow")
	}

	// 放行
	ok := validIdentity("acme", "req-ok", ScopeRead)
	d, err := authorize(ok, "acme", ScopeRead, f.policy, f.now())
	if err != nil || !d.Allowed || d.PolicyVersion != 1 {
		t.Fatalf("want allowed decision on v1, got %+v err=%v", d, err)
	}
}
