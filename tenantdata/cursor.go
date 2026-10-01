package tenantdata

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// cursorMagic 是游标令牌的固定前缀，用于快速识别损坏/伪造的令牌。
const cursorMagic = "tdc1:"

// Cursor 是一次续读会话的不透明游标。
//
// 游标固定住读取会话的全部上下文（快照水位、策略版本、数据集、过滤条件、
// 起始位置），保证续读不会因写入或策略变化而重复/跳过记录。
type Cursor struct {
	// ReadID 是一次完整读取会话（首页 + 后续续页）的标识。
	ReadID string `json:"read_id"`
	// RequestID 是首页请求标识，便于审计串联。
	RequestID string `json:"request_id"`
	// TenantID 与 Dataset 固定数据集范围。
	TenantID string `json:"tenant_id"`
	Dataset  string `json:"dataset"`
	// Watermark 固定快照水位。
	Watermark int64 `json:"watermark"`
	// PolicyVersion 固定判定所用策略版本。
	PolicyVersion uint64 `json:"policy_version"`
	// Filter 固定过滤条件。
	Filter Filter `json:"filter"`
	// PageSize 固定每页大小，续读必须沿用首页设置。
	PageSize int `json:"page_size"`
	// LastID 是已返回的最后一条记录 ID；空串表示尚未读取。
	LastID string `json:"last_id"`
	// Page 是页码（从 1 开始），用于审计可读性。
	Page int `json:"page"`
}

// Encode 把游标编码为带固定前缀的 base64 不透明字符串。
func (c Cursor) Encode() string {
	raw, err := json.Marshal(c)
	if err != nil {
		// Cursor 字段均为可 JSON 序列化的基础类型，此处理论上不可达。
		return ""
	}
	return cursorMagic + base64.RawURLEncoding.EncodeToString(raw)
}

// DecodeCursor 解析不透明游标；格式非法、字段缺失时返回 KindInvalidCursor。
func DecodeCursor(token string) (Cursor, error) {
	var c Cursor
	if len(token) <= len(cursorMagic) || token[:len(cursorMagic)] != cursorMagic {
		return c, newError(KindInvalidCursor, "", 0, "cursor has invalid prefix")
	}
	raw, err := base64.RawURLEncoding.DecodeString(token[len(cursorMagic):])
	if err != nil {
		return c, newError(KindInvalidCursor, "", 0, "cursor is not valid base64: %v", err)
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return c, newError(KindInvalidCursor, "", 0, "cursor payload is corrupt: %v", err)
	}
	if c.ReadID == "" || c.TenantID == "" || c.Dataset == "" {
		return c, newError(KindInvalidCursor, "", c.PolicyVersion,
			"cursor is missing required session fields")
	}
	return c, nil
}

// String 用于日志，打印游标位置而非全部内部字段。
func (c Cursor) String() string {
	return fmt.Sprintf("read=%s page=%d last=%q watermark=%d policy_version=%d",
		c.ReadID, c.Page, c.LastID, c.Watermark, c.PolicyVersion)
}
