package tenantdata

import (
	"context"
	"sort"
	"sync"
)

// recordRevision 保存一条记录的一个已提交版本。
//
// commit 是该次写入对应的全局提交水位；读取快照 W 下，
// 一条记录可见的取值是 commit <= W 的最后一个修订。
type recordRevision struct {
	commit int64
	record Record
}

// MemoryDataStore 是 DataStore 的并发安全本地实现。
//
// 每次 Upsert 产生一个新修订并把全库水位 +1；Scan 只看到
// commit <= 请求快照水位 的最新记录修订，实现快照隔离：
// 读取期间新到达的写入既不会混入结果，也不会改变顺序。
type MemoryDataStore struct {
	mu        sync.RWMutex
	watermark int64
	// dataset -> tenantID -> recordID -> 按 commit 升序的修订
	data map[string]map[string]map[string][]recordRevision
	// datasets 记录已创建的数据集（即使尚无记录）。
	datasets map[string]struct{}

	// fault 用于测试注入暂时性故障。
	fault       func() error
	failTimesMu sync.Mutex
}

// NewMemoryDataStore 创建空的本地数据存储。
func NewMemoryDataStore() *MemoryDataStore {
	return &MemoryDataStore{
		data:     make(map[string]map[string]map[string][]recordRevision),
		datasets: make(map[string]struct{}),
	}
}

// CreateDataset 创建一个空数据集；重复创建返回 KindInvalidArgument。
func (s *MemoryDataStore) CreateDataset(_ context.Context, dataset string) error {
	if dataset == "" {
		return newError(KindInvalidArgument, "", 0, "dataset name is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.datasets[dataset]; ok {
		return newError(KindInvalidArgument, "", 0, "dataset %q already exists", dataset)
	}
	s.datasets[dataset] = struct{}{}
	return nil
}

// DatasetExists 判断数据集是否已创建。
func (s *MemoryDataStore) DatasetExists(_ context.Context, dataset string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.datasets[dataset]
	return ok, nil
}

// SetFaultHook 注入故障钩子：每次 Scan 前调用，返回非 nil 错误时中断扫描。
// 传 nil 清除钩子。供 SDK 自身测试使用。
func (s *MemoryDataStore) SetFaultHook(fn func() error) {
	s.mu.Lock()
	s.fault = fn
	s.mu.Unlock()
}

// FailNextScans 令接下来 n 次 Scan 返回 KindUnavailable，之后自动恢复。
// 供 SDK 自身测试有限重试使用。
func (s *MemoryDataStore) FailNextScans(n int) {
	s.failTimesMu.Lock()
	remaining := int32(n)
	s.failTimesMu.Unlock()
	s.SetFaultHook(func() error {
		s.failTimesMu.Lock()
		defer s.failTimesMu.Unlock()
		if remaining <= 0 {
			return nil
		}
		remaining--
		return newError(KindUnavailable, "", 0, "injected transient scan fault (%d left)", remaining)
	})
}

// ClearFault 清除故障钩子。
func (s *MemoryDataStore) ClearFault() { s.SetFaultHook(nil) }

// Upsert 写入记录的新修订，提交水位原子 +1。
// Record.Version 若为 0 则由存储按提交水位赋值。
func (s *MemoryDataStore) Upsert(_ context.Context, dataset string, r Record) error {
	if dataset == "" || r.TenantID == "" || r.ID == "" {
		return newError(KindInvalidArgument, "", 0, "upsert requires dataset, tenant id and record id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.datasets[dataset] = struct{}{}
	s.watermark++
	if r.Version == 0 {
		r.Version = s.watermark
	}
	tenants := s.data[dataset]
	if tenants == nil {
		tenants = make(map[string]map[string][]recordRevision)
		s.data[dataset] = tenants
	}
	recs := tenants[r.TenantID]
	if recs == nil {
		recs = make(map[string][]recordRevision)
		tenants[r.TenantID] = recs
	}
	recs[r.ID] = append(recs[r.ID], recordRevision{commit: s.watermark, record: r})
	return nil
}

// SnapshotWatermark 返回当前已提交水位。
// 数据集尚未创建也返回 (0, nil)：空数据集是合法快照。
func (s *MemoryDataStore) SnapshotWatermark(_ context.Context, _ string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.watermark, nil
}

// Scan 在快照水位 watermark 下，按 ID 字典序扫描 afterID 之后、
// 至多 limit 条满足过滤条件且属于 tenantID 的记录。
//
// 返回本页记录、最后一条记录 ID、以及快照内是否仍有后续记录。
// 由于遍历的是快照下的确定集合，续读不会重复或跳过记录。
func (s *MemoryDataStore) Scan(_ context.Context, dataset, tenantID string, watermark int64, f Filter, afterID string, limit int) ([]Record, string, bool, error) {
	s.mu.RLock()
	fault := s.fault
	s.mu.RUnlock()

	if fault != nil {
		if err := fault(); err != nil {
			return nil, "", false, err
		}
	}

	if limit <= 0 {
		return nil, "", false, newError(KindInvalidArgument, "", 0, "scan limit must be positive")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if watermark > s.watermark {
		// 调用方持有比当前已提交数据更高的水位，无法给出确定结果。
		return nil, "", false, newError(KindInvalidCursor, "", 0,
			"snapshot watermark %d is ahead of committed %d", watermark, s.watermark)
	}

	tenantRecs := s.data[dataset][tenantID]
	ids := make([]string, 0, len(tenantRecs))
	visible := make(map[string]Record, len(tenantRecs))
	for id, revs := range tenantRecs {
		// 找到 commit <= watermark 的最后一个修订（revs 按 commit 升序）。
		idx := sort.Search(len(revs), func(i int) bool { return revs[i].commit > watermark })
		if idx == 0 {
			continue // 该记录在快照时点尚不存在。
		}
		rec := revs[idx-1].record
		if !f.Match(rec) {
			continue
		}
		ids = append(ids, id)
		visible[id] = rec
	}
	sort.Strings(ids)

	start := 0
	if afterID != "" {
		start = sort.SearchStrings(ids, afterID)
		// 严格“之后”：跳过等于 afterID 的位置。
		if start < len(ids) && ids[start] == afterID {
			start++
		}
	}

	end := start + limit
	more := false
	if end < len(ids) {
		more = true
	} else {
		end = len(ids)
	}

	out := make([]Record, 0, end-start)
	lastID := afterID
	for i := start; i < end; i++ {
		out = append(out, visible[ids[i]])
		lastID = ids[i]
	}
	if len(out) == 0 {
		lastID = afterID
	}
	return out, lastID, more, nil
}
