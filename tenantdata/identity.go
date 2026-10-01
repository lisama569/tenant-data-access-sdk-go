package tenantdata

import "time"

// Identity 描述一次请求的身份：租户、调用方、权限范围与有效期。
type Identity struct {
	RequestID string
	TenantID  string
	Caller    string
	Scopes    []string
	ExpiresAt time.Time
}

// HasScope 判断身份是否持有指定权限范围。
func (id Identity) HasScope(scope string) bool {
	for _, s := range id.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}
