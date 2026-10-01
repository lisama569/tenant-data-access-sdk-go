package tenantdata

import (
	"context"
	"time"
)

// ScopeRead 是读取操作对应的权限范围约定。
const ScopeRead = "read"

// Policy 是某个租户在某个版本下的访问策略快照。
type Policy struct {
	// Version 是单调递增的策略版本号。
	Version uint64
	// TenantID 是策略所属租户。
	TenantID string
	// AllowedScopes 是该租户允许使用的权限范围集合。
	AllowedScopes []string
	// EffectiveAt 策略生效时间。
	EffectiveAt time.Time
	// ExpiresAt 策略失效时间；零值表示不过期。
	ExpiresAt time.Time
	// Revoked 为 true 时表示该版本已被显式废止。
	Revoked bool
}

// allowsScope 判断策略是否包含指定权限范围。
func (p Policy) allowsScope(scope string) bool {
	for _, s := range p.AllowedScopes {
		if s == scope {
			return true
		}
	}
	return false
}

// Allows 判断策略在 now 时刻是否允许指定 scope：
// 未废止、已生效、未过期且权限范围包含。
func (p Policy) Allows(scope string, now time.Time) bool {
	if p.Revoked {
		return false
	}
	if !p.EffectiveAt.IsZero() && now.Before(p.EffectiveAt) {
		return false
	}
	if !p.ExpiresAt.IsZero() && !now.Before(p.ExpiresAt) {
		return false
	}
	return p.allowsScope(scope)
}

// Decision 是一次策略判定的确定结果。
type Decision struct {
	// Allowed 表示是否放行。
	Allowed bool
	// TenantID 与 PolicyVersion 记录判定依据，保证可解释、可审计。
	TenantID      string
	PolicyVersion uint64
	// Reason 是归一化后的判定说明（含拒绝类别）。
	Reason string
}

// PolicyStore 是版本化策略存储的抽象。
//
// 实现必须保证：Get 在并发 Update 下，要么返回某个完整的旧版本快照，
// 要么返回完整的新版本快照；每个请求只能按一个确定版本完成判定。
type PolicyStore interface {
	// Get 返回租户在指定时刻有效的最新策略快照。
	Get(ctx context.Context, tenantID string, now time.Time) (Policy, error)
	// GetAtVersion 返回指定版本的策略快照；版本缺失或已废止返回可区分错误。
	GetAtVersion(ctx context.Context, tenantID string, version uint64, now time.Time) (Policy, error)
	// Update 用变更函数原子更新租户策略，返回新版本快照。
	Update(ctx context.Context, tenantID string, mut func(Policy) Policy) (Policy, error)
}

// authorize 在固定策略版本下完成一次完整访问判定。
//
// 判定顺序固定，错误类别互不混淆：
//  1. 身份有效性         -> KindUnauthenticated
//  2. 租户匹配           -> KindTenantMismatch
//  3. 策略存在且有效      -> KindNotFound / KindPolicyStale
//  4. 权限范围           -> KindPermissionDenied
//
// 一旦取到策略快照，整个请求只按该 Policy.Version 判定，
// 并发更新不会影响进行中的请求。
func authorize(id Identity, resourceTenant string, scope string, store PolicyStore, now time.Time) (Decision, error) {
	if err := id.ValidAt(now); err != nil {
		return Decision{}, err
	}

	d := Decision{TenantID: id.TenantID}

	if resourceTenant != id.TenantID {
		return d, newError(KindTenantMismatch, id.RequestID, 0,
			"identity tenant %q cannot access tenant %q resource", id.TenantID, resourceTenant)
	}

	policy, err := store.Get(context.Background(), id.TenantID, now)
	if err != nil {
		return d, err
	}
	d.PolicyVersion = policy.Version

	if policy.Revoked {
		return d, newError(KindPolicyStale, id.RequestID, policy.Version,
			"policy version %d has been revoked", policy.Version)
	}
	if !policy.EffectiveAt.IsZero() && now.Before(policy.EffectiveAt) {
		return d, newError(KindPolicyStale, id.RequestID, policy.Version,
			"policy version %d is not effective until %s", policy.Version, policy.EffectiveAt.Format(time.RFC3339Nano))
	}
	if !policy.ExpiresAt.IsZero() && !now.Before(policy.ExpiresAt) {
		return d, newError(KindPolicyStale, id.RequestID, policy.Version,
			"policy version %d expired at %s", policy.Version, policy.ExpiresAt.Format(time.RFC3339Nano))
	}
	if !id.HasScope(scope) || !policy.allowsScope(scope) {
		return d, newError(KindPermissionDenied, id.RequestID, policy.Version,
			"scope %q not granted or not allowed by policy version %d", scope, policy.Version)
	}

	d.Allowed = true
	d.Reason = "ok"
	return d, nil
}
