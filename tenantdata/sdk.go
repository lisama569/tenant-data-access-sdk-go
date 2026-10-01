package tenantdata

import (
	"context"
	"time"
)

// ReadRequest 描述一次读取：目标租户、权限范围、过滤条件、游标与页大小。
type ReadRequest struct {
	TenantID  string
	Scope     string
	KeyPrefix string
	Cursor    string
	Limit     int
}

// Page 是一页读取结果。
type Page struct {
	Records       []Record
	NextCursor    string
	HasMore       bool
	PolicyVersion uint64
}

// Client 是可嵌入的多租户数据访问客户端。
type Client struct {
	policies     *PolicyStore
	store        *Store
	audit        *AuditLog
	sem          chan struct{}
	maxRetries   int
	retryBackoff time.Duration
	now          func() time.Time
}

// Option 配置 Client。
type Option func(*Client)

// WithMaxConcurrent 限制并发读取数，超出即拒绝（CAPACITY_EXCEEDED）。
func WithMaxConcurrent(n int) Option {
	return func(c *Client) {
		if n > 0 {
			c.sem = make(chan struct{}, n)
		}
	}
}

// WithMaxRetries 设置暂时性故障的最大重试次数（不含首次尝试）。
func WithMaxRetries(n int) Option {
	return func(c *Client) {
		if n >= 0 {
			c.maxRetries = n
		}
	}
}

// WithRetryBackoff 设置重试间隔。
func WithRetryBackoff(d time.Duration) Option {
	return func(c *Client) { c.retryBackoff = d }
}

// WithClock 注入时钟（仅用于测试）。
func WithClock(now func() time.Time) Option {
	return func(c *Client) { c.now = now }
}

func NewClient(policies *PolicyStore, store *Store, audit *AuditLog, opts ...Option) *Client {
	c := &Client{
		policies:     policies,
		store:        store,
		audit:        audit,
		sem:          make(chan struct{}, 64),
		maxRetries:   2,
		retryBackoff: time.Millisecond,
		now:          time.Now,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Read 执行一次带身份校验、策略判定、并发限制与有限重试的分页读取。
//
// 判定顺序固定：身份有效期 → 租户匹配 → 策略快照（一次请求只用一个版本）
// → 策略有效期 → 权限范围 → 游标校验 → 并发准入 → 数据读取（有限重试）。
// 无论成败，每个逻辑请求只写一条最终审计事件。
func (c *Client) Read(ctx context.Context, id Identity, req ReadRequest) (Page, error) {
	now := c.now()

	// 1. 身份有效期。
	if !id.ExpiresAt.IsZero() && !now.Before(id.ExpiresAt) {
		return Page{}, c.deny(id, req, 0, 0, newError(ErrCodeIdentityExpired, id.RequestID, 0, "identity expired at %s", id.ExpiresAt))
	}

	// 2. 租户匹配。
	if id.TenantID != req.TenantID {
		return Page{}, c.deny(id, req, 0, 0, newError(ErrCodeTenantMismatch, id.RequestID, 0, "identity tenant %q cannot access tenant %q", id.TenantID, req.TenantID))
	}

	// 3. 策略快照：一次请求只按取到的这一个版本判定。
	policy, ok := c.policies.Get(req.TenantID)
	if !ok {
		return Page{}, c.deny(id, req, 0, 0, newError(ErrCodePermissionDenied, id.RequestID, 0, "no policy for tenant %q", req.TenantID))
	}

	// 4. 策略有效期。
	if policy.Expired(now) {
		return Page{}, c.deny(id, req, policy.Version, 0, newError(ErrCodePolicyExpired, id.RequestID, policy.Version, "policy v%d expired at %s", policy.Version, policy.ValidUntil))
	}

	// 5. 权限范围：身份持有且策略放行。
	if !id.HasScope(req.Scope) || !policy.Allows(req.Scope) {
		return Page{}, c.deny(id, req, policy.Version, 0, newError(ErrCodePermissionDenied, id.RequestID, policy.Version, "scope %q not granted", req.Scope))
	}

	// 6. 游标校验：续读必须沿用签发时的策略版本，否则明确失败。
	var cur Cursor
	if req.Cursor != "" {
		decoded, err := DecodeCursor(req.Cursor)
		if err != nil {
			return Page{}, c.deny(id, req, policy.Version, 0, newError(ErrCodeCursorInvalid, id.RequestID, policy.Version, "malformed cursor: %v", err))
		}
		if decoded.PolicyVersion != policy.Version {
			return Page{}, c.deny(id, req, policy.Version, 0, newError(ErrCodeCursorInvalid, id.RequestID, policy.Version, "cursor issued under policy v%d, current is v%d", decoded.PolicyVersion, policy.Version))
		}
		cur = decoded
	} else {
		// 首页：以当前写入水位线为快照上限，之后的新写入不进入本次分页。
		wm, exists := c.store.Watermark(req.TenantID)
		if !exists {
			return Page{}, c.deny(id, req, policy.Version, 0, newError(ErrCodeNotFound, id.RequestID, policy.Version, "dataset for tenant %q not found", req.TenantID))
		}
		cur = Cursor{SnapshotSeq: wm, PolicyVersion: policy.Version}
	}

	// 7. 并发准入：超出容量立即拒绝，不排队。
	select {
	case c.sem <- struct{}{}:
		defer func() { <-c.sem }()
	default:
		return Page{}, c.deny(id, req, policy.Version, 0, newError(ErrCodeCapacityExceeded, id.RequestID, policy.Version, "too many concurrent reads"))
	case <-ctx.Done():
		return Page{}, c.deny(id, req, policy.Version, 0, newError(ErrCodeUnavailable, id.RequestID, policy.Version, "context cancelled: %v", ctx.Err()))
	}

	// 8. 数据读取：仅对暂时性故障做有限重试，审计只记最终结果。
	var lastErr *Error
	attempts := 0
	for {
		attempts++
		records, exists, err := c.store.Scan(req.TenantID, cur.LastSeq, cur.SnapshotSeq, req.KeyPrefix, req.Limit)
		if err == nil {
			if !exists {
				return Page{}, c.deny(id, req, policy.Version, attempts, newError(ErrCodeNotFound, id.RequestID, policy.Version, "dataset for tenant %q not found", req.TenantID))
			}
			page := Page{Records: records, PolicyVersion: policy.Version}
			if len(records) > 0 {
				next := Cursor{SnapshotSeq: cur.SnapshotSeq, LastSeq: records[len(records)-1].Seq, PolicyVersion: policy.Version}
				page.NextCursor = next.Encode()
				page.HasMore = records[len(records)-1].Seq < cur.SnapshotSeq
			}
			c.allow(id, req, policy.Version, attempts, len(records), page.NextCursor)
			return page, nil
		}
		lastErr = newError(ErrCodeUnavailable, id.RequestID, policy.Version, "store scan failed: %v", err)
		if attempts > c.maxRetries {
			return Page{}, c.deny(id, req, policy.Version, attempts, lastErr)
		}
		select {
		case <-ctx.Done():
			return Page{}, c.deny(id, req, policy.Version, attempts, newError(ErrCodeUnavailable, id.RequestID, policy.Version, "context cancelled during retry: %v", ctx.Err()))
		case <-time.After(c.retryBackoff):
		}
	}
}

// allow 写入一条成功审计事件。
func (c *Client) allow(id Identity, req ReadRequest, policyVersion uint64, attempts, n int, nextCursor string) {
	c.audit.Append(AuditEvent{
		RequestID:     id.RequestID,
		TenantID:      req.TenantID,
		Caller:        id.Caller,
		Decision:      DecisionAllow,
		PolicyVersion: policyVersion,
		Cursor:        nextCursor,
		Records:       n,
		Attempts:      attempts,
		At:            c.now(),
	})
}

// deny 写入一条失败审计事件并返回错误本身。
func (c *Client) deny(id Identity, req ReadRequest, policyVersion uint64, attempts int, err *Error) error {
	c.audit.Append(AuditEvent{
		RequestID:     id.RequestID,
		TenantID:      req.TenantID,
		Caller:        id.Caller,
		Decision:      DecisionDeny,
		PolicyVersion: policyVersion,
		Cursor:        req.Cursor,
		ErrCode:       err.Code,
		Attempts:      attempts,
		At:            c.now(),
	})
	return err
}
