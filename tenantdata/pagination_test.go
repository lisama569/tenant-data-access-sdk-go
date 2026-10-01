package tenantdata

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestPaginationIsStableAcrossWrites(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DefaultPageSize = 3
	cfg.MaxPageSize = 3
	f := newFixture(t, cfg)
	seedPolicy(t, f, "acme", 1, ScopeRead)
	seedRecords(t, f, "orders", "acme", 10)
	ctx := context.Background()
	id := validIdentity("acme", "req-page", ScopeRead)

	var gotIDs []string
	res, err := f.client.Read(ctx, id, ReadOptions{Dataset: "orders", TenantID: "acme", Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if res.Watermark != 10 || res.PolicyVersion != 1 || res.ReadID == "" {
		t.Fatalf("unexpected first page header: %+v", res)
	}
	gotIDs = append(gotIDs, recordIDs(res.Records)...)

	// 读取进行期间：既有记录更新、新记录插入，都不得影响已固定的快照。
	if err := f.data.Upsert(ctx, "orders", Record{ID: "rec-0001", TenantID: "acme", Payload: "changed"}); err != nil {
		t.Fatal(err)
	}
	for i := 10; i < 15; i++ {
		if err := f.data.Upsert(ctx, "orders", Record{ID: recID(i), TenantID: "acme"}); err != nil {
			t.Fatal(err)
		}
	}

	page := 1
	for res.NextCursor != "" {
		page++
		res, err = f.client.ContinueRead(ctx, id, res.NextCursor)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		if res.ReadID == "" {
			t.Fatal("continuation lost read id")
		}
		gotIDs = append(gotIDs, recordIDs(res.Records)...)
	}
	if page != 4 {
		t.Fatalf("pages = %d, want 4", page)
	}
	if len(gotIDs) != 10 {
		t.Fatalf("got %d records, want 10: %v", len(gotIDs), gotIDs)
	}
	assertUniqueAndContiguous(t, gotIDs, 0, 10)
}

func TestPaginationFilterPrefixAndMinVersion(t *testing.T) {
	f := newFixture(t, DefaultConfig())
	seedPolicy(t, f, "acme", 1, ScopeRead)
	seedRecords(t, f, "orders", "acme", 6)
	if err := f.data.Upsert(context.Background(), "orders", Record{ID: "other-1", TenantID: "acme"}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Prefix 只返回 rec- 前缀的 6 条，排除 other-1
	res, err := f.client.Read(ctx, validIdentity("acme", "req-prefix", ScopeRead),
		ReadOptions{Dataset: "orders", TenantID: "acme", Filter: Filter{Prefix: "rec-"}, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if ids := recordIDs(res.Records); len(ids) != 6 {
		t.Fatalf("prefix filter got %v", ids)
	}

	// MinVersion: 记录版本等于其首次提交水位 1..6，>=4 应返回 rec-0003..rec-0005
	res2, err := f.client.Read(ctx, validIdentity("acme", "req-minv", ScopeRead),
		ReadOptions{Dataset: "orders", TenantID: "acme", Filter: Filter{MinVersion: 4}, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if ids := recordIDs(res2.Records); len(ids) != 4 {
		t.Fatalf("minversion filter got %v", ids)
	}
}

func TestCursorRejectsGarbageAndCrossTenant(t *testing.T) {
	f := newFixture(t, DefaultConfig())
	seedPolicy(t, f, "acme", 1, ScopeRead)
	seedPolicy(t, f, "beta", 1, ScopeRead)
	seedRecords(t, f, "orders", "acme", 5)
	ctx := context.Background()

	res, err := f.client.Read(ctx, validIdentity("acme", "req-cur", ScopeRead),
		ReadOptions{Dataset: "orders", TenantID: "acme", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if res.NextCursor == "" {
		t.Fatal("expected next cursor")
	}

	// 损坏游标
	if _, err := f.client.ContinueRead(ctx, validIdentity("acme", "req-cur", ScopeRead), "not-a-cursor"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("garbage cursor: %v", err)
	}

	// 跨租户续读 -> tenant_mismatch
	if _, err := f.client.ContinueRead(ctx, validIdentity("beta", "req-cur", ScopeRead), res.NextCursor); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("cross-tenant continuation: %v", err)
	}

	// 正常续读完成
	res2, err := f.client.ContinueRead(ctx, validIdentity("acme", "req-cur", ScopeRead), res.NextCursor)
	if err != nil {
		t.Fatalf("continue: %v", err)
	}
	if len(res2.Records) != 2 {
		t.Fatalf("page2 len = %d", len(res2.Records))
	}
}

func TestDatasetNotFound(t *testing.T) {
	f := newFixture(t, DefaultConfig())
	seedPolicy(t, f, "acme", 1, ScopeRead)
	_, err := f.client.Read(context.Background(), validIdentity("acme", "req-404", ScopeRead),
		ReadOptions{Dataset: "missing", TenantID: "acme"})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want not_found, got %v", err)
	}
}

func recordIDs(rs []Record) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.ID
	}
	return out
}

func recID(i int) string {
	return fmt.Sprintf("rec-%04d", i)
}

func assertUniqueAndContiguous(t *testing.T, ids []string, from, n int) {
	t.Helper()
	seen := map[string]bool{}
	want := map[string]bool{}
	for i := from; i < from+n; i++ {
		want[recID(i)] = true
	}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate record returned: %s (full=%v)", id, ids)
		}
		seen[id] = true
		if !want[id] {
			t.Fatalf("unexpected record %s (snapshot should hide new writes)", id)
		}
	}
	for w := range want {
		if !seen[w] {
			t.Fatalf("record silently skipped: %s (got %v)", w, ids)
		}
	}
}
