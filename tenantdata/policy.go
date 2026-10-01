package tenantdata

import (
	"fmt"
	"sync"
	"time"
)

// Policy 是某一版本的租户访问策略。
type Policy struct {
	TenantID      string
	Version       uint64
	AllowedScopes []string
	ValidUntil    time.Time
}

// Allows 判断策略是否放行指定权限范围。
func (p Policy) Allows(scope string) bool {
	for _, s := range p.AllowedScopes {
		if s == scope {
			return true
		}
	}
	return false
}

// Expired 判断策略在指定时刻是否已失效。
func (p Policy) Expired(now time.Time) bool {
	return !p.ValidUntil.IsZero() && !now.Before(p.ValidUntil)
}

// PolicyStore 保存各租户的最新策略版本，读取时返回确定快照。
// 并发更新与读取通过互斥保证：每个请求只能拿到一个确定的版本。
type PolicyStore struct {
	mu       sync.RWMutex
	policies map[string]Policy
}

func NewPolicyStore() *PolicyStore { return &PolicyStore{policies: make(map[string]Policy)} }

// Set 发布新版本策略；版本号必须严格递增，防止旧版本覆盖新版本。
func (s *PolicyStore) Set(p Policy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.policies[p.TenantID]
	if ok && p.Version <= cur.Version {
		return fmt.Errorf("policy version must increase: tenant=%s current=%d got=%d", p.TenantID, cur.Version, p.Version)
	}
	s.policies[p.TenantID] = p
	return nil
}

// Get 返回租户当前策略版本的快照（值拷贝，之后更新不影响已取出的版本）。
func (s *PolicyStore) Get(tenantID string) (Policy, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.policies[tenantID]
	return p, ok
}
