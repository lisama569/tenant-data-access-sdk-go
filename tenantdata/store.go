package tenantdata

import (
	"strings"
	"sync"
)

// Record 是一条租户数据记录，Seq 为租户内单调递增的写入序号。
type Record struct {
	TenantID string
	Key      string
	Value    string
	Seq      uint64
}

// Store 是本地模拟的租户数据集，按租户隔离，记录按 Seq 排序。
//
// 可见性规则：读取以首次请求时的 Watermark 为快照上限，
// 续读只返回 Seq <= 快照上限的记录，因此读取期间的新写入
// 不会混入本次分页结果；记录只增不删，续读不会静默跳过记录。
type Store struct {
	mu       sync.RWMutex
	datasets map[string]*dataset
	fault    func() error
}

type dataset struct {
	nextSeq uint64
	records []Record // 按 Seq 升序
}

func NewStore() *Store { return &Store{datasets: make(map[string]*dataset)} }

// Put 写入或更新一条记录，返回其写入序号。
// 同 key 更新保留原 Seq（值原地更新），新 key 分配新 Seq。
func (s *Store) Put(tenantID, key, value string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	ds, ok := s.datasets[tenantID]
	if !ok {
		ds = &dataset{}
		s.datasets[tenantID] = ds
	}
	for i := range ds.records {
		if ds.records[i].Key == key {
			ds.records[i].Value = value
			return ds.records[i].Seq
		}
	}
	ds.nextSeq++
	ds.records = append(ds.records, Record{TenantID: tenantID, Key: key, Value: value, Seq: ds.nextSeq})
	return ds.nextSeq
}

// SetFaultHook 注入暂时性故障（仅用于测试），返回非 nil 错误时本次读取失败。
func (s *Store) SetFaultHook(hook func() error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fault = hook
}

// Scan 返回租户数据集中 seq > afterSeq 且 seq <= watermark 的记录（按 Seq 升序），
// 最多 limit 条；limit<=0 表示不限制。ok=false 表示数据集不存在。
func (s *Store) Scan(tenantID string, afterSeq, watermark uint64, keyPrefix string, limit int) ([]Record, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.fault != nil {
		if err := s.fault(); err != nil {
			return nil, false, err
		}
	}
	ds, ok := s.datasets[tenantID]
	if !ok {
		return nil, false, nil
	}
	var out []Record
	for _, r := range ds.records {
		if r.Seq <= afterSeq || r.Seq > watermark {
			continue
		}
		if keyPrefix != "" && !strings.HasPrefix(r.Key, keyPrefix) {
			continue
		}
		out = append(out, r)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, true, nil
}

// Watermark 返回租户当前写入序号上限；数据集不存在时 ok=false。
func (s *Store) Watermark(tenantID string) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ds, ok := s.datasets[tenantID]
	if !ok {
		return 0, false
	}
	return ds.nextSeq, true
}
