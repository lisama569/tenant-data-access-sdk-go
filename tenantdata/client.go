package tenantdata

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// Logger 是 SDK 接受的最小日志接口，调用方可桥接到自身日志系统。
//
// 每条日志都包含请求标识、策略版本、游标位置与归一化错误原因。
type Logger interface {
	Printf(format string, args ...any)
}

// discardLogger 默认丢弃日志。
type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// Config 是 SDK 客户端配置。
type Config struct {
	// MaxConcurrent 是允许的最大并发读取数；<=0 使用默认值。
	MaxConcurrent int
	// DefaultPageSize 是未指定 Limit 时的每页大小。
	DefaultPageSize int
	// MaxPageSize 是单页允许的最大记录数。
	MaxPageSize int
	// MaxRetry 是暂时性故障的最大尝试次数（含首次）。
	MaxRetry int
	// RetryBaseDelay 是重试退避的基础延迟。
	RetryBaseDelay time.Duration
	// Now 注入时钟；nil 使用 time.Now。
	Now func() time.Time
}

// DefaultConfig 返回带默认容量与重试参数的配置。
func DefaultConfig() Config {
	return Config{
		MaxConcurrent:   32,
		DefaultPageSize: 50,
		MaxPageSize:     500,
		MaxRetry:        3,
		RetryBaseDelay:  5 * time.Millisecond,
	}
}

// Client 是可被其他调用方直接嵌入使用的多租户数据访问客户端。
//
// 它串联身份校验 -> 固定版本策略判定 -> 快照读取/游标续读 ->
// 有限重试 -> 并发保护 -> 审计留痕，保证结果一致、可解释、可审计。
type Client struct {
	cfg    Config
	policy PolicyStore
	data   DataStore
	audit  AuditSink
	log    Logger
	now    func() time.Time

	sem chan struct{}

	readSeq atomic.Uint64
}

// NewClient 创建客户端；audit 为 nil 时审计事件被丢弃。
func NewClient(cfg Config, policy PolicyStore, data DataStore, audit AuditSink) *Client {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 32
	}
	if cfg.DefaultPageSize <= 0 {
		cfg.DefaultPageSize = 50
	}
	if cfg.MaxPageSize <= 0 {
		cfg.MaxPageSize = 500
	}
	if cfg.MaxRetry <= 0 {
		cfg.MaxRetry = 3
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = 5 * time.Millisecond
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if audit == nil {
		audit = nopAuditSink{}
	}
	return &Client{
		cfg:    cfg,
		policy: policy,
		data:   data,
		audit:  audit,
		log:    discardLogger{},
		now:    cfg.Now,
		sem:    make(chan struct{}, cfg.MaxConcurrent),
	}
}

// SetLogger 注入日志器；传 nil 恢复为丢弃日志。
func (c *Client) SetLogger(l Logger) {
	if l == nil {
		c.log = discardLogger{}
		return
	}
	c.log = l
}

// ReadResult 是一次读取（首页或续页）的结果。
type ReadResult struct {
	// Records 是本页记录。
	Records []Record
	// NextCursor 是续读游标；空串表示已读到末尾。
	NextCursor string
	// More 表示是否还有后续记录。
	More bool
	// ReadID 是整个读取会话标识，首页与续页一致。
	ReadID string
	// PolicyVersion 是本次判定固定的策略版本。
	PolicyVersion uint64
	// Watermark 是本次读取固定的快照水位。
	Watermark int64
}

// Read 在鉴权后读取数据集首页，返回的游标用于 ContinueRead 续读。
func (c *Client) Read(ctx context.Context, id Identity, opts ReadOptions) (ReadResult, error) {
	acquired := c.tryAcquire(id)
	if !acquired {
		err := newError(KindCapacityExceeded, id.RequestID, 0,
			"max concurrent reads %d reached, request rejected", c.cfg.MaxConcurrent)
		c.finish(ctx, auditBase(id, opts.Dataset, 1), err, 1)
		return ReadResult{}, err
	}
	defer c.release()

	if opts.Dataset == "" {
		err := newError(KindInvalidArgument, id.RequestID, 0, "dataset is required")
		c.finish(ctx, auditBase(id, "", 1), err, 1)
		return ReadResult{}, err
	}
	if opts.TenantID == "" {
		err := newError(KindInvalidArgument, id.RequestID, 0, "resource tenant is required")
		c.finish(ctx, auditBase(id, opts.Dataset, 1), err, 1)
		return ReadResult{}, err
	}

	// 鉴权在重试之前完成，整个请求只按这一个策略版本判定。
	dec, err := authorize(id, opts.TenantID, ScopeRead, c.policy, c.now())
	if err != nil {
		err = decorate(err, id)
		c.finish(ctx, auditBase(id, opts.Dataset, 1), err, 1)
		return ReadResult{}, err
	}

	exists, dserr := c.data.DatasetExists(ctx, opts.Dataset)
	if dserr != nil {
		nerr := decorate(dserr, id)
		c.finish(ctx, auditBase(id, opts.Dataset, 1), nerr, 1)
		return ReadResult{}, nerr
	}
	if !exists {
		nerr := newError(KindNotFound, id.RequestID, dec.PolicyVersion, "dataset %q not found", opts.Dataset)
		c.finish(ctx, auditBase(id, opts.Dataset, 1), nerr, 1)
		return ReadResult{}, nerr
	}

	limit := c.normalizeLimit(opts.Limit)
	wm, werr := c.data.SnapshotWatermark(ctx, opts.Dataset)
	if werr != nil {
		werr = decorate(werr, id)
		c.finish(ctx, auditBase(id, opts.Dataset, 1), werr, 1)
		return ReadResult{}, werr
	}

	cur := Cursor{
		ReadID:        c.newReadID(id),
		RequestID:     id.RequestID,
		TenantID:      opts.TenantID,
		Dataset:       opts.Dataset,
		Watermark:     wm,
		PolicyVersion: dec.PolicyVersion,
		Filter:        opts.Filter,
		PageSize:      limit,
		LastID:        "",
		Page:          1,
	}

	res, rerr, attempts := c.scanWithRetry(ctx, id, cur, limit)
	c.terminal(ctx, id, res, rerr, attempts)
	return res.result, rerr
}

// ContinueRead 使用首页返回的游标续读下一页。
//
// 游标所固定的策略版本失效、会话不匹配或游标损坏时返回 KindInvalidCursor /
// KindPolicyStale；不会重复返回已读记录，也不会静默跳过记录。
func (c *Client) ContinueRead(ctx context.Context, id Identity, token string) (ReadResult, error) {
	acquired := c.tryAcquire(id)
	if !acquired {
		err := newError(KindCapacityExceeded, id.RequestID, 0,
			"max concurrent reads %d reached, request rejected", c.cfg.MaxConcurrent)
		c.finish(ctx, auditBase(id, "", 0), err, 1)
		return ReadResult{}, err
	}
	defer c.release()

	cur, derr := DecodeCursor(token)
	if derr != nil {
		derr = decorate(derr, id)
		c.finish(ctx, auditBase(id, "", 0), derr, 1)
		return ReadResult{}, derr
	}

	// 身份本身必须仍然有效。
	if verr := id.ValidAt(c.now()); verr != nil {
		verr = decorate(verr, id)
		c.finish(ctx, auditBase(id, cur.Dataset, cur.Page+1), verr, 1)
		return ReadResult{}, verr
	}
	// 续读身份必须与首页属于同一租户（跨租户拿游标续读是明确的越权）。
	if id.TenantID != cur.TenantID {
		err := newError(KindTenantMismatch, id.RequestID, cur.PolicyVersion,
			"identity tenant %q cannot continue read owned by %q", id.TenantID, cur.TenantID)
		c.finish(ctx, auditBase(id, cur.Dataset, cur.Page+1), err, 1)
		return ReadResult{}, err
	}

	// 按游标固定的版本重新校验策略；旧版本被废止/过期则明确失败，
	// 绝不静默切换到新版本（那会改变可见性与结论）。
	policy, perr := c.policy.GetAtVersion(ctx, cur.TenantID, cur.PolicyVersion, c.now())
	if perr != nil {
		err := decorate(perr, id)
		if Normalize(err) == KindNotFound {
			err = newError(KindPolicyStale, id.RequestID, cur.PolicyVersion,
				"pinned policy version %d no longer exists", cur.PolicyVersion)
		}
		c.finish(ctx, auditBase(id, cur.Dataset, cur.Page+1), err, 1)
		return ReadResult{}, err
	}
	if !policy.Allows(ScopeRead, c.now()) {
		err := newError(KindPolicyStale, id.RequestID, cur.PolicyVersion,
			"pinned policy version %d is no longer valid", cur.PolicyVersion)
		c.finish(ctx, auditBase(id, cur.Dataset, cur.Page+1), err, 1)
		return ReadResult{}, err
	}
	if !id.HasScope(ScopeRead) {
		err := newError(KindPermissionDenied, id.RequestID, cur.PolicyVersion,
			"identity no longer carries scope %q on continuation", ScopeRead)
		c.finish(ctx, auditBase(id, cur.Dataset, cur.Page+1), err, 1)
		return ReadResult{}, err
	}

	cur.Page++
	limit := cur.PageSize // 续读沿用首页固定的页大小。

	res, rerr, attempts := c.scanWithRetry(ctx, id, cur, limit)
	c.terminal(ctx, id, res, rerr, attempts)
	return res.result, rerr
}

// scanResult 是内部扫描产物。
type scanResult struct {
	result ReadResult
	page   int
	cursor Cursor
}

// scanWithRetry 仅对 Scan 的暂时性故障重试；游标、水位、策略版本在重试间保持不变，
// 因而重试要么返回同一确定页面，要么带着明确错误失败。
func (c *Client) scanWithRetry(ctx context.Context, id Identity, cur Cursor, limit int) (scanResult, error, int) {
	var (
		recs   []Record
		lastID string
		more   bool
	)
	rerr, attempts := withRetry(ctx, c.retryCfg(), func(_ int) error {
		r, l, m, err := c.data.Scan(ctx, cur.Dataset, cur.TenantID, cur.Watermark, cur.Filter, cur.LastID, limit)
		if err != nil {
			return decorate(err, id)
		}
		recs, lastID, more = r, l, m
		return nil
	})

	if rerr != nil {
		c.log.Printf("tenantdata read failed request=%s read=%s policy_version=%d cursor=%s error=%s",
			id.RequestID, cur.ReadID, cur.PolicyVersion, cur.String(), Normalize(rerr))
		return scanResult{result: ReadResult{}, page: cur.Page, cursor: cur}, rerr, attempts
	}

	out := ReadResult{
		Records:       recs,
		More:          more,
		ReadID:        cur.ReadID,
		PolicyVersion: cur.PolicyVersion,
		Watermark:     cur.Watermark,
	}
	if more {
		next := cur
		next.LastID = lastID
		out.NextCursor = next.Encode()
	}
	finished := cur
	finished.LastID = lastID
	c.log.Printf("tenantdata read ok request=%s read=%s policy_version=%d cursor_pos=%q page=%d returned=%d more=%t error=ok",
		id.RequestID, cur.ReadID, cur.PolicyVersion, lastID, cur.Page, len(recs), more)
	return scanResult{result: out, page: cur.Page, cursor: finished}, nil, attempts
}

// terminal 基于读取会话上下文构造并写入终态审计事件。
func (c *Client) terminal(ctx context.Context, id Identity, sr scanResult, err error, attempts int) {
	e := auditBase(id, sr.cursor.Dataset, sr.page)
	e.ReadID = sr.cursor.ReadID
	e.PolicyVersion = sr.cursor.PolicyVersion
	e.CursorPos = sr.cursor.LastID
	c.finish(ctx, e, err, attempts)
}

// finish 在请求终态确定后写入唯一一条审计事件。
func (c *Client) finish(ctx context.Context, e AuditEvent, err error, attempts int) {
	if attempts < 1 {
		attempts = 1
	}
	e.Time = c.now()
	e.Attempts = attempts
	if err == nil {
		e.Allowed = true
		e.ErrorKind = KindOK.String()
	} else {
		e.Allowed = false
		e.ErrorKind = Normalize(err).String()
	}
	_ = c.audit.Write(ctx, e)
	if err != nil {
		c.log.Printf("tenantdata terminal request=%s policy_version=%d cursor_pos=%q page=%d attempts=%d error=%s detail=%q",
			e.RequestID, e.PolicyVersion, e.CursorPos, e.Page, attempts, e.ErrorKind, err.Error())
	}
}

// tryAcquire 非阻塞地申请并发槽位；失败表示被容量保护拒绝。
func (c *Client) tryAcquire(_ Identity) bool {
	select {
	case c.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (c *Client) release() { <-c.sem }

func (c *Client) normalizeLimit(limit int) int {
	switch {
	case limit <= 0:
		return c.cfg.DefaultPageSize
	case limit > c.cfg.MaxPageSize:
		return c.cfg.MaxPageSize
	default:
		return limit
	}
}

func (c *Client) retryCfg() retryConfig {
	return retryConfig{maxAttempts: c.cfg.MaxRetry, baseDelay: c.cfg.RetryBaseDelay}
}

func (c *Client) newReadID(id Identity) string {
	n := c.readSeq.Add(1)
	return fmt.Sprintf("read-%d-%s-%d", n, id.RequestID, c.now().UnixNano())
}

func auditBase(id Identity, dataset string, page int) AuditEvent {
	return AuditEvent{
		RequestID: id.RequestID,
		TenantID:  id.TenantID,
		CallerID:  id.CallerID,
		Dataset:   dataset,
		Action:    ScopeRead,
		Page:      page,
	}
}

// decorate 确保从存储层冒出的错误也携带请求标识；
// 非 SDK 错误包装为 KindUnavailable。
func decorate(err error, id Identity) error {
	if err == nil {
		return nil
	}
	if e, ok := AsError(err); ok {
		if e.RequestID == "" {
			cp := *e
			cp.RequestID = id.RequestID
			return &cp
		}
		return err
	}
	return newError(KindUnavailable, id.RequestID, 0, "internal error: %v", err)
}
