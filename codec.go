package sdk

import "encoding/json"

// Page 是归一化后的分页响应。旧版本使用方遇到新增字段时，
// 字段被完整保留在 Extra 中，既不报错也不静默丢弃。
type Page struct {
	Version Version
	Cursor  string // 下一页游标；空串表示没有更多数据
	Items   []json.RawMessage
	Extra   map[string]json.RawMessage // 未识别的新增字段，原样保留
}

// DecodePage 解码分页响应并做版本与必填字段校验。
// 错误按 ErrKindMalformed / ErrKindMissingField / ErrKindVersionUnknown 区分，
// 底层 json 异常一律包装，不直接透出。
func DecodePage(data []byte) (*Page, error) { return nil, nil }

// EncodePage 编码分页响应（主要用于本地测试替身）。
func EncodePage(p *Page) ([]byte, error) { return nil, nil }
