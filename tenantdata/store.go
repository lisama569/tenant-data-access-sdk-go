package tenantdata

import (
	"context"
)

// DataStore 是租户数据集的本地存储抽象。
//
// 实现必须支持多版本快照读：Scan 依据 readAt（逻辑读取时间戳/快照水位）
// 决定记录可见性，从而在读取期间发生写入时，结果仍然确定。
type DataStore interface {
	// DatasetExists 判断数据集是否已创建。
	DatasetExists(ctx context.Context, dataset string) (bool, error)
	// Upsert 写入或更新指定数据集中的一条记录。
	Upsert(ctx context.Context, dataset string, r Record) error
	// SnapshotWatermark 返回当前数据集的最新提交水位（版本序号）。
	SnapshotWatermark(ctx context.Context, dataset string) (int64, error)
	// Scan 在给定快照水位下，按游标之后的顺序扫描至多 limit 条满足过滤条件的记录。
	//
	// afterID 为空表示从头开始。返回记录、本页最后一条记录 ID 以及是否还有更多数据。
	Scan(ctx context.Context, dataset, tenantID string, watermark int64, f Filter, afterID string, limit int) (records []Record, lastID string, more bool, err error)
}
