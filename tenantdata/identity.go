package tenantdata

import "time"

// Identity 描述一次请求携带的访问身份。
type Identity struct {
	// RequestID 是请求的唯一标识，贯穿判定、读取与审计。
	RequestID string
	// TenantID 是调用方所属租户。
	TenantID string
	// CallerID 是调用方（应用/用户/服务账号）标识。
	CallerID string
	// Scopes 是被授予的权限范围集合，如 "read:orders"。
	Scopes []string
	// NotBefore 是身份生效起点（零值表示立即生效）。
	NotBefore time.Time
	// ExpiresAt 是身份有效期终点。
	ExpiresAt time.Time
}

// ValidAt 校验身份在给定时刻是否有效（字段完整性与有效期）。
// 返回的错误类别为 KindUnauthenticated。
func (id Identity) ValidAt(now time.Time) error {
	switch {
	case id.RequestID == "":
		return newError(KindUnauthenticated, id.RequestID, 0, "missing request id")
	case id.TenantID == "":
		return newError(KindUnauthenticated, id.RequestID, 0, "missing tenant id")
	case id.CallerID == "":
		return newError(KindUnauthenticated, id.RequestID, 0, "missing caller id")
	case !id.NotBefore.IsZero() && now.Before(id.NotBefore):
		return newError(KindUnauthenticated, id.RequestID, 0,
			"identity not effective until %s", id.NotBefore.Format(time.RFC3339Nano))
	case !id.ExpiresAt.IsZero() && !now.Before(id.ExpiresAt):
		return newError(KindUnauthenticated, id.RequestID, 0,
			"identity expired at %s", id.ExpiresAt.Format(time.RFC3339Nano))
	default:
		return nil
	}
}

// HasScope 判断身份是否包含指定权限范围。
func (id Identity) HasScope(scope string) bool {
	for _, s := range id.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}
