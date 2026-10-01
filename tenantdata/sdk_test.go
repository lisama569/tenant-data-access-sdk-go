package tenantdata

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

var future = time.Now().Add(time.Hour)

func newEnv(t *testing.T, opts ...Option) (*Client, *PolicyStore, *Store, *AuditLog) {
	t.Helper()
	ps := NewPolicyStore()
	st := NewStore()
	al := NewAuditLog()
	return NewClient(ps, st, al, opts...), ps, st, al
}

func identity(id, tenant string, scopes ...string) Identity {
	return Identity{RequestID: id, TenantID: tenant, Caller: "caller-" + id, Scopes: scopes, ExpiresAt: future}
}

func allowPolicy(tenant string, version uint64, scopes ...string) Policy {
	return Policy{TenantID: tenant, Version: version, AllowedScopes: scopes, ValidUntil: future}
}

// logErr 按测试要求打印请求标识、策略版本、游标位置与归一化错误原因。
func logErr(t *testing.T, reqID string, policyVersion uint64, cursor string, err error) {
	t.Helper()
	var e *Error
	if errors.As(err, &e) {
		t.Logf("request=%s policy_version=%d cursor=%q err_code=%s reason=%q",
			e.RequestID, e.PolicyVersion, cursor, e.Code, e.Reason)
	} else {
		t.Logf("request=%s policy_version=%d cursor=%q err=%v", reqID, policyVersion, cursor, err)
	}
}

func logPage(t *testing.T, reqID string, p Page) {
	t.Helper()
	t.Logf("request=%s policy_version=%d cursor=%q records=%d has_more=%v",
		reqID, p.PolicyVersion, p.NextCursor, len(p.Records), p.HasMore)
}

func TestReadAllPages_ConsistentUnderConcurrentWrites(t *testing.T) {
	c, ps, st, _ := newEnv(t)
	if err := ps.Set(allowPolicy("t1", 1, "read")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		st.Put("t1", fmt.Sprintf("k%02d", i), "v")
	}

	// 首页之后并发写入新数据：续读不得包含新数据，也不得重复或遗漏快照内记录。
	ctx := context.Background()
	id := identity("r-page", "t1", "read")
	page1, err := c.Read(ctx, id, ReadRequest{TenantID: "t1", Scope: "read", Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	logPage(t, "r-page", page1)
	for i := 10; i < 15; i++ {
		st.Put("t1", fmt.Sprintf("k%02d", i), "new")
	}

	seen := map[string]int{}
	for _, r := range page1.Records {
		seen[r.Key]++
	}
	cursor := page1.NextCursor
	for page1.HasMore {
		p, err := c.Read(ctx, id, ReadRequest{TenantID: "t1", Scope: "read", Limit: 4, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		logPage(t, "r-page", p)
		for _, r := range p.Records {
			seen[r.Key]++
		}
		cursor = p.NextCursor
		page1 = p
	}
	if len(seen) != 10 {
		t.Fatalf("want 10 distinct records, got %d: %v", len(seen), seen)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("record %s returned %d times", k, n)
		}
	}
}

func TestConcurrentReads_DeterministicResults(t *testing.T) {
	c, ps, st, al := newEnv(t, WithMaxConcurrent(64))
	if err := ps.Set(allowPolicy("t1", 1, "read")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		st.Put("t1", fmt.Sprintf("k%02d", i), "v")
	}

	var wg sync.WaitGroup
	results := make([][]string, 32)
	errs := make([]error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := identity(fmt.Sprintf("r-conc-%02d", i), "t1", "read")
			var keys []string
			cursor := ""
			for {
				p, err := c.Read(context.Background(), id, ReadRequest{TenantID: "t1", Scope: "read", Limit: 7, Cursor: cursor})
				if err != nil {
					errs[i] = err
					return
				}
				for _, r := range p.Records {
					keys = append(keys, r.Key)
				}
				if !p.HasMore {
					break
				}
				cursor = p.NextCursor
			}
			results[i] = keys
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("reader %d failed: %v", i, err)
		}
	}
	if len(results[0]) != 50 {
		t.Fatalf("want 50 records, got %d", len(results[0]))
	}
	for i := 1; i < 32; i++ {
		if len(results[i]) != 50 {
			t.Fatalf("reader %d got %d records", i, len(results[i]))
		}
		for j := range results[i] {
			if results[i][j] != results[0][j] {
				t.Fatalf("reader %d diverges at %d: %s != %s", i, j, results[i][j], results[0][j])
			}
		}
	}
	allow := 0
	for _, e := range al.Events() {
		if e.Decision == DecisionAllow {
			allow++
		}
	}
	t.Logf("concurrent reads done: allow_events=%d total_events=%d", allow, len(al.Events()))
}

func TestPolicySwitch_CursorInvalidated(t *testing.T) {
	c, ps, st, _ := newEnv(t)
	if err := ps.Set(allowPolicy("t1", 1, "read")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		st.Put("t1", fmt.Sprintf("k%d", i), "v")
	}
	ctx := context.Background()
	id := identity("r-switch", "t1", "read")

	p1, err := c.Read(ctx, id, ReadRequest{TenantID: "t1", Scope: "read", Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	logPage(t, "r-switch", p1)

	// 策略切换到 v2：旧游标必须明确失败，不得静默按新策略续读。
	if err := ps.Set(allowPolicy("t1", 2, "read")); err != nil {
		t.Fatal(err)
	}
	_, err = c.Read(ctx, id, ReadRequest{TenantID: "t1", Scope: "read", Limit: 3, Cursor: p1.NextCursor})
	logErr(t, "r-switch", 2, p1.NextCursor, err)
	var e *Error
	if !errors.As(err, &e) || e.Code != ErrCodeCursorInvalid {
		t.Fatalf("want CURSOR_INVALID, got %v", err)
	}

	// 新请求按 v2 判定并正常返回。
	p2, err := c.Read(ctx, id, ReadRequest{TenantID: "t1", Scope: "read", Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if p2.PolicyVersion != 2 {
		t.Fatalf("want policy version 2, got %d", p2.PolicyVersion)
	}
	logPage(t, "r-switch", p2)
}

func TestMalformedCursor(t *testing.T) {
	c, ps, st, _ := newEnv(t)
	if err := ps.Set(allowPolicy("t1", 1, "read")); err != nil {
		t.Fatal(err)
	}
	st.Put("t1", "k", "v")
	_, err := c.Read(context.Background(), identity("r-badcur", "t1", "read"),
		ReadRequest{TenantID: "t1", Scope: "read", Cursor: "!!!not-a-cursor"})
	logErr(t, "r-badcur", 1, "!!!not-a-cursor", err)
	var e *Error
	if !errors.As(err, &e) || e.Code != ErrCodeCursorInvalid {
		t.Fatalf("want CURSOR_INVALID, got %v", err)
	}
}

func TestAccessDenied_ErrorClassification(t *testing.T) {
	c, ps, st, al := newEnv(t)
	if err := ps.Set(allowPolicy("t1", 1, "read")); err != nil {
		t.Fatal(err)
	}
	if err := ps.Set(Policy{TenantID: "t-expired", Version: 1, AllowedScopes: []string{"read"}, ValidUntil: time.Now().Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}
	st.Put("t1", "k", "v")
	ctx := context.Background()

	cases := []struct {
		name string
		id   Identity
		req  ReadRequest
		want ErrCode
	}{
		{"tenant mismatch", identity("r-mismatch", "t2", "read"), ReadRequest{TenantID: "t1", Scope: "read"}, ErrCodeTenantMismatch},
		{"identity expired", Identity{RequestID: "r-idexp", TenantID: "t1", Caller: "c", Scopes: []string{"read"}, ExpiresAt: time.Now().Add(-time.Minute)}, ReadRequest{TenantID: "t1", Scope: "read"}, ErrCodeIdentityExpired},
		{"scope not in identity", identity("r-noscope", "t1", "write"), ReadRequest{TenantID: "t1", Scope: "read"}, ErrCodePermissionDenied},
		{"scope not in policy", identity("r-policynoscope", "t1", "admin"), ReadRequest{TenantID: "t1", Scope: "admin"}, ErrCodePermissionDenied},
		{"no policy for tenant", identity("r-nopolicy", "t9", "read"), ReadRequest{TenantID: "t9", Scope: "read"}, ErrCodePermissionDenied},
		{"policy expired", identity("r-polexp", "t-expired", "read"), ReadRequest{TenantID: "t-expired", Scope: "read"}, ErrCodePolicyExpired},
		{"dataset not found", identity("r-notfound", "t1", "read"), ReadRequest{TenantID: "t1", Scope: "read", KeyPrefix: ""}, ErrCodeNotFound},
	}
	// dataset not found 用例需要无数据租户：改用存在策略但无数据集的租户。
	if err := ps.Set(allowPolicy("t-empty", 1, "read")); err != nil {
		t.Fatal(err)
	}
	cases[len(cases)-1].id = identity("r-notfound", "t-empty", "read")
	cases[len(cases)-1].req = ReadRequest{TenantID: "t-empty", Scope: "read"}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.Read(ctx, tc.id, tc.req)
			logErr(t, tc.id.RequestID, 0, tc.req.Cursor, err)
			var e *Error
			if !errors.As(err, &e) {
				t.Fatalf("want *Error, got %T", err)
			}
			if e.Code != tc.want {
				t.Fatalf("want %s, got %s", tc.want, e.Code)
			}
			if e.Retryable() {
				t.Fatalf("%s must not be retryable", e.Code)
			}
		})
	}
	// 每个失败请求恰好一条 DENY 审计。
	deny := 0
	for _, ev := range al.Events() {
		if ev.Decision == DecisionDeny {
			deny++
		}
	}
	if deny != len(cases) {
		t.Fatalf("want %d deny events, got %d", len(cases), deny)
	}
}

func TestRetry_TransientFailureEventuallySucceeds(t *testing.T) {
	c, ps, st, al := newEnv(t, WithMaxRetries(3), WithRetryBackoff(time.Nanosecond))
	if err := ps.Set(allowPolicy("t1", 1, "read")); err != nil {
		t.Fatal(err)
	}
	st.Put("t1", "k", "v")

	var mu sync.Mutex
	calls := 0
	st.SetFaultHook(func() error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls <= 2 {
			return errors.New("transient store error")
		}
		return nil
	})

	p, err := c.Read(context.Background(), identity("r-retry", "t1", "read"), ReadRequest{TenantID: "t1", Scope: "read"})
	if err != nil {
		t.Fatal(err)
	}
	logPage(t, "r-retry", p)
	if len(p.Records) != 1 {
		t.Fatalf("want 1 record, got %d", len(p.Records))
	}

	// 重试不得重复产生成功审计：整个逻辑请求只有一条 ALLOW。
	events := al.Events()
	if len(events) != 1 || events[0].Decision != DecisionAllow {
		t.Fatalf("want exactly one ALLOW event, got %+v", events)
	}
	if events[0].Attempts != 3 {
		t.Fatalf("want 3 attempts recorded, got %d", events[0].Attempts)
	}
	t.Logf("request=r-retry attempts=%d decision=%s", events[0].Attempts, events[0].Decision)
}

func TestRetry_ExhaustedReturnsUnavailable(t *testing.T) {
	c, ps, st, al := newEnv(t, WithMaxRetries(2), WithRetryBackoff(time.Nanosecond))
	if err := ps.Set(allowPolicy("t1", 1, "read")); err != nil {
		t.Fatal(err)
	}
	st.Put("t1", "k", "v")
	st.SetFaultHook(func() error { return errors.New("always failing") })

	_, err := c.Read(context.Background(), identity("r-exhaust", "t1", "read"), ReadRequest{TenantID: "t1", Scope: "read"})
	logErr(t, "r-exhaust", 1, "", err)
	var e *Error
	if !errors.As(err, &e) || e.Code != ErrCodeUnavailable {
		t.Fatalf("want UNAVAILABLE, got %v", err)
	}
	if !e.Retryable() {
		t.Fatal("UNAVAILABLE must be retryable classification")
	}
	events := al.Events()
	if len(events) != 1 || events[0].Decision != DecisionDeny || events[0].Attempts != 3 {
		t.Fatalf("want one DENY with 3 attempts, got %+v", events)
	}
}

func TestCapacityExceeded(t *testing.T) {
	c, ps, st, al := newEnv(t, WithMaxConcurrent(1))
	if err := ps.Set(allowPolicy("t1", 1, "read")); err != nil {
		t.Fatal(err)
	}
	st.Put("t1", "k", "v")

	// 用一个阻塞中的读取占满容量。
	release := make(chan struct{})
	st.SetFaultHook(func() error {
		<-release
		return nil
	})
	done := make(chan error, 1)
	go func() {
		_, err := c.Read(context.Background(), identity("r-block", "t1", "read"), ReadRequest{TenantID: "t1", Scope: "read"})
		done <- err
	}()
	// 等占满容量的请求进入 Scan。
	time.Sleep(50 * time.Millisecond)

	_, err := c.Read(context.Background(), identity("r-overflow", "t1", "read"), ReadRequest{TenantID: "t1", Scope: "read"})
	logErr(t, "r-overflow", 1, "", err)
	var e *Error
	if !errors.As(err, &e) || e.Code != ErrCodeCapacityExceeded {
		t.Fatalf("want CAPACITY_EXCEEDED, got %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// 容量拒绝与权限拒绝不能混为同类。
	for _, ev := range al.Events() {
		if ev.RequestID == "r-overflow" && ev.ErrCode != ErrCodeCapacityExceeded {
			t.Fatalf("unexpected err code for overflow request: %s", ev.ErrCode)
		}
	}
}

func TestKeyPrefixFilter(t *testing.T) {
	c, ps, st, _ := newEnv(t)
	if err := ps.Set(allowPolicy("t1", 1, "read")); err != nil {
		t.Fatal(err)
	}
	st.Put("t1", "user/1", "a")
	st.Put("t1", "user/2", "b")
	st.Put("t1", "order/1", "c")

	p, err := c.Read(context.Background(), identity("r-filter", "t1", "read"),
		ReadRequest{TenantID: "t1", Scope: "read", KeyPrefix: "user/"})
	if err != nil {
		t.Fatal(err)
	}
	logPage(t, "r-filter", p)
	if len(p.Records) != 2 {
		t.Fatalf("want 2 records, got %d", len(p.Records))
	}
	keys := []string{p.Records[0].Key, p.Records[1].Key}
	sort.Strings(keys)
	if keys[0] != "user/1" || keys[1] != "user/2" {
		t.Fatalf("unexpected keys: %v", keys)
	}
}
