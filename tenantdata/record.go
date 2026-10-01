package tenantdata

import (
	"strings"
	"time"
)

// Record 是租户数据集中的一条记录。
type Record struct {
	// ID 是记录在数据集内的唯一标识。
	ID string
	// TenantID 是记录所属租户，用于强制租户隔离。
	TenantID string
	// Version 是记录版本，单调递增，用于多版本可见性判定。
	Version int64
	// UpdatedAt 是记录最后写入时间。
	UpdatedAt time.Time
	// Payload 是记录数据。
	Payload string
}

// Filter 描述读取时的条件过滤。
type Filter struct {
	// Prefix 按记录 ID 前缀过滤；空串表示不过滤。
	Prefix string
	// MinVersion 仅返回 Version >= MinVersion 的记录；0 表示不过滤。
	MinVersion int64
}

// Match 判断记录是否满足过滤条件。
func (f Filter) Match(r Record) bool {
	if f.Prefix != "" && !strings.HasPrefix(r.ID, f.Prefix) {
		return false
	}
	if f.MinVersion != 0 && r.Version < f.MinVersion {
		return false
	}
	return true
}

// ReadOptions 控制单次读取行为。
type ReadOptions struct {
	// Dataset 是要读取的本地数据集名称。
	Dataset string
	// TenantID 是目标资源所属租户；与身份租户不一致时返回 KindTenantMismatch。
	TenantID string
	// Filter 是可选的条件过滤。
	Filter Filter
	// Limit 是本页最多返回记录数；<=0 时使用默认值。
	Limit int
}
