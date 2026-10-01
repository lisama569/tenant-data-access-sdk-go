package tenantdata

import (
	"encoding/base64"
	"encoding/json"
)

// Cursor 标识一次分页读取的续读位置，绑定签发时的策略版本。
type Cursor struct {
	SnapshotSeq   uint64 `json:"s"`
	LastSeq       uint64 `json:"l"`
	PolicyVersion uint64 `json:"p"`
}

// Encode 序列化游标为不透明字符串。
func (c Cursor) Encode() string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor 解析游标字符串，格式非法时返回错误。
func DecodeCursor(s string) (Cursor, error) {
	var c Cursor
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return c, err
	}
	return c, json.Unmarshal(b, &c)
}
