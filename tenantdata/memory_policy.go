package tenantdata

import (
	"context"
	"sync"
	"time"
)

// MemoryPolicyStore 是 PolicyStore 的并发安全本地实现。
//
// 每次 Update 都生成一个不可变的新版本快照并追加保存；
// 旧版本快照在保留期内仍可被 GetAtVersion 精确读取，
// 因此进行中的请求可以始终按其开始时的版本完成判定。
type MemoryPolicyStore struct {
	mu        sync.RWMutex
	snapshots map[string][]Policy // tenantID -> 按版本升序的全部快照
	versions  map[string]uint64   // tenantID -> 当前版本号
}

// NewMemoryPolicyStore 创建空的本地策略存储。
func NewMemoryPolicyStore() *MemoryPolicyStore {
	return &MemoryPolicyStore{
		snapshots: make(map[string][]Policy),
		versions:  make(map[string]uint64),
	}
}

// Put 以显式快照形式植入一条策略；其 Version 必须大于该租户的现有版本。
// 用于初始化测试/引导数据。
func (s *MemoryPolicyStore) Put(p Policy) error {
	if p.TenantID == "" {
		return newError(KindInvalidArgument, "", 0, "policy tenant id is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.Version <= s.versions[p.TenantID] {
		return newError(KindInvalidArgument, "", p.Version,
			"policy version %d is not greater than current %d", p.Version, s.versions[p.TenantID])
	}
	s.snapshots[p.TenantID] = append(s.snapshots[p.TenantID], p)
	s.versions[p.TenantID] = p.Version
	return nil
}

// Get 返回当前版本号指向的策略快照（不按有效期过滤——
// 策略是否有效属于判定环节，由 authorize 归一化错误类别）。
func (s *MemoryPolicyStore) Get(_ context.Context, tenantID string, _ time.Time) (Policy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v := s.versions[tenantID]
	if v == 0 {
		return Policy{}, newError(KindNotFound, "", 0, "no policy for tenant %q", tenantID)
	}
	return s.snapshotLocked(tenantID, v)
}

// GetAtVersion 返回指定版本快照；版本不存在返回 KindNotFound。
// 是否废止/过期同样交由判定环节处理（返回 KindPolicyStale）。
func (s *MemoryPolicyStore) GetAtVersion(_ context.Context, tenantID string, version uint64, _ time.Time) (Policy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if version == 0 || version > s.versions[tenantID] {
		return Policy{}, newError(KindNotFound, "", version,
			"policy version %d not found for tenant %q", version, tenantID)
	}
	return s.snapshotLocked(tenantID, version)
}

// snapshotLocked 返回指定版本快照；调用方须持有读锁。
func (s *MemoryPolicyStore) snapshotLocked(tenantID string, version uint64) (Policy, error) {
	for _, p := range s.snapshots[tenantID] {
		if p.Version == version {
			// 返回副本，避免调用方修改不可变快照。
			cp := p
			cp.AllowedScopes = append([]string(nil), p.AllowedScopes...)
			return cp, nil
		}
	}
	return Policy{}, newError(KindNotFound, "", version,
		"policy version %d not found for tenant %q", version, tenantID)
}

// Update 以 mut 基于当前快照原子生成新版本。
// mut 返回的新版本不可设置 Revoked（废止应通过 Revoke 表达），
// Version 由存储统一分配，保证全局单调。
func (s *MemoryPolicyStore) Update(_ context.Context, tenantID string, mut func(Policy) Policy) (Policy, error) {
	if tenantID == "" {
		return Policy{}, newError(KindInvalidArgument, "", 0, "tenant id is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	cur := Policy{TenantID: tenantID}
	if v := s.versions[tenantID]; v != 0 {
		found, err := s.snapshotLocked(tenantID, v)
		if err != nil {
			return Policy{}, err
		}
		cur = found
	}

	next := mut(cur)
	next.TenantID = tenantID
	next.Version = s.versions[tenantID] + 1
	next.Revoked = false // 更新即产生一个有效的新版本。
	next.AllowedScopes = append([]string(nil), next.AllowedScopes...)

	s.snapshots[tenantID] = append(s.snapshots[tenantID], next)
	s.versions[tenantID] = next.Version
	return s.cloneSnapshot(next), nil
}

// Revoke 把租户当前版本标记为废止，不递增版本号；
// 进行中按旧版本判定的请求随后用 GetAtVersion 校验时会得到 KindPolicyStale。
func (s *MemoryPolicyStore) Revoke(tenantID string) (Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.versions[tenantID]
	if v == 0 {
		return Policy{}, newError(KindNotFound, "", 0, "no policy for tenant %q", tenantID)
	}
	for i := range s.snapshots[tenantID] {
		if s.snapshots[tenantID][i].Version == v {
			s.snapshots[tenantID][i].Revoked = true
			return s.snapshots[tenantID][i], nil
		}
	}
	return Policy{}, newError(KindNotFound, "", v, "policy snapshot missing")
}

// CurrentVersion 返回租户当前策略版本号（0 表示尚无策略）。
func (s *MemoryPolicyStore) CurrentVersion(tenantID string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.versions[tenantID]
}

func (s *MemoryPolicyStore) cloneSnapshot(p Policy) Policy {
	cp := p
	cp.AllowedScopes = append([]string(nil), p.AllowedScopes...)
	return cp
}
