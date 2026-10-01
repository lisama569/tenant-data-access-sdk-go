package tenantdata

import (
	"context"
	"errors"
	"testing"
)

// 读取会话进行中策略更新为新版本：游标仍固定 v1。
// v1 仍有效 -> 续读继续按 v1 完成，结果不变。
func TestContinuePinnedToOldPolicyVersion(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DefaultPageSize = 2
	cfg.MaxPageSize = 2
	f := newFixture(t, cfg)
	seedPolicy(t, f, "acme", 1, ScopeRead)
	seedRecords(t, f, "orders", "acme", 5)
	ctx := context.Background()
	id := validIdentity("acme", "req-switch-ok", ScopeRead)

	res, err := f.client.Read(ctx, id, ReadOptions{Dataset: "orders", TenantID: "acme", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	// 策略升级：新版本移除 read 权限，但旧请求必须不受影响。
	updated, err := f.policy.Update(ctx, "acme", func(p Policy) Policy {
		p.AllowedScopes = []string{"write"}
		return p
	})
	if err != nil || updated.Version != 2 {
		t.Fatalf("update policy: %+v %v", updated, err)
	}

	page := 1
	var all []string
	all = append(all, recordIDs(res.Records)...)
	for res.NextCursor != "" {
		page++
		res, err = f.client.ContinueRead(ctx, id, res.NextCursor)
		if err != nil {
			t.Fatalf("old session must finish on v1 even after policy update, page %d: %v", page, err)
		}
		if res.PolicyVersion != 1 {
			t.Fatalf("continuation silently switched policy to %d", res.PolicyVersion)
		}
		all = append(all, recordIDs(res.Records)...)
	}
	assertUniqueAndContiguous(t, all, 0, 5)
}

// 旧版本被显式废止：续读必须返回 KindPolicyStale，且不会产生成功审计。
func TestContinueFailsWhenPinnedPolicyRevoked(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DefaultPageSize = 2
	cfg.MaxPageSize = 2
	f := newFixture(t, cfg)
	seedPolicy(t, f, "acme", 1, ScopeRead)
	seedRecords(t, f, "orders", "acme", 5)
	ctx := context.Background()
	id := validIdentity("acme", "req-revoked", ScopeRead)

	res, err := f.client.Read(ctx, id, ReadOptions{Dataset: "orders", TenantID: "acme", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}

	// 废止当前版本（v1），并发布不含 read 权限的 v2。
	if _, err := f.policy.Revoke("acme"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.policy.Update(ctx, "acme", func(p Policy) Policy {
		p.AllowedScopes = []string{"write"}
		return p
	}); err != nil {
		t.Fatal(err)
	}

	_, err = f.client.ContinueRead(ctx, id, res.NextCursor)
	if !errors.Is(err, ErrPolicyStale) {
		t.Fatalf("want policy_stale, got %v", err)
	}
	if e, ok := AsError(err); !ok || e.PolicyVersion != 1 {
		t.Fatalf("stale error must reference pinned version 1, got %+v", e)
	}

	// 该请求的审计中不得出现成功结论。
	for _, e := range f.audit.EventsByRequest("req-revoked") {
		if e.ErrorKind == "ok" && e.Page > 1 {
			t.Fatalf("successful audit written despite stale policy: %+v", e)
		}
	}
}

// 并发进行多次策略更新时，每个读取请求固定在恰好一个版本上完成。
func TestConcurrentPolicyUpdatesEachRequestPinnedToOneVersion(t *testing.T) {
	f := newFixture(t, DefaultConfig())
	seedPolicy(t, f, "acme", 1, ScopeRead)
	seedRecords(t, f, "orders", "acme", 3)
	ctx := context.Background()

	stop := make(chan struct{})
	go func() {
		v := uint64(1)
		for {
			select {
			case <-stop:
				return
			default:
				v++
				_, _ = f.policy.Update(ctx, "acme", func(p Policy) Policy {
					p.AllowedScopes = []string{ScopeRead}
					return p
				})
			}
		}
	}()

	const n = 40
	errs := make(chan error, n)
	versions := make(chan uint64, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			id := validIdentity("acme", reqID("race", i), ScopeRead)
			res, err := f.client.Read(ctx, id, ReadOptions{Dataset: "orders", TenantID: "acme", Limit: 1})
			if err != nil {
				errs <- err
				return
			}
			versions <- res.PolicyVersion
		}(i)
	}
	close(stop)

	for i := 0; i < n; i++ {
		select {
		case err := <-errs:
			t.Fatalf("read under policy churn failed: %v", err)
		case v := <-versions:
			if v == 0 {
				t.Fatal("request completed without a pinned policy version")
			}
		}
	}
}
