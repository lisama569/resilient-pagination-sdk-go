package sdk

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// SupportedVersions 是 SDK 支持的协议版本列表，用于响应校验与协商。
var SupportedVersions = []Version{
	{Major: 1, Minor: 0},
	{Major: 1, Minor: 1},
	{Major: 1, Minor: 2},
}

// 分页响应的必填字段。
const (
	fieldVersion = "version"
	fieldCursor  = "cursor"
	fieldItems   = "items"
)

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
func DecodePage(data []byte) (*Page, error) {
	const op = "DecodePage"

	// 先解析为通用结构，区分“结构非法”与“缺失必填字段”。
	var raw map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&raw); err != nil {
		return nil, wrap(ErrKindMalformed, op, "响应不是合法的 JSON 对象", err)
	}

	// 必填字段：缺失与类型非法分别归类。
	versionRaw, ok := raw[fieldVersion]
	if !ok {
		return nil, wrap(ErrKindMissingField, op, "缺失必填字段 version", nil)
	}
	var versionStr string
	if err := json.Unmarshal(versionRaw, &versionStr); err != nil {
		return nil, wrap(ErrKindMalformed, op, "字段 version 类型非法", err)
	}
	version, err := ParseVersion(versionStr)
	if err != nil {
		return nil, wrap(ErrKindMalformed, op, "字段 version 取值非法", err)
	}
	if _, err := Negotiate(SupportedVersions, version); err != nil {
		return nil, err // 已是归一化的 version_unknown / version_mismatch
	}

	cursorRaw, ok := raw[fieldCursor]
	if !ok {
		return nil, wrap(ErrKindMissingField, op, "缺失必填字段 cursor", nil)
	}
	var cursor string
	if err := json.Unmarshal(cursorRaw, &cursor); err != nil {
		return nil, wrap(ErrKindMalformed, op, "字段 cursor 类型非法", err)
	}

	itemsRaw, ok := raw[fieldItems]
	if !ok {
		return nil, wrap(ErrKindMissingField, op, "缺失必填字段 items", nil)
	}
	var items []json.RawMessage
	if err := json.Unmarshal(itemsRaw, &items); err != nil {
		return nil, wrap(ErrKindMalformed, op, "字段 items 类型非法", err)
	}

	// 新增字段原样保留，旧版本使用方不报错、不丢弃。
	extra := make(map[string]json.RawMessage)
	for k, v := range raw {
		switch k {
		case fieldVersion, fieldCursor, fieldItems:
		default:
			extra[k] = v
		}
	}

	return &Page{
		Version: version,
		Cursor:  cursor,
		Items:   items,
		Extra:   extra,
	}, nil
}

// EncodePage 编码分页响应（主要用于本地测试替身）。
func EncodePage(p *Page) ([]byte, error) {
	const op = "EncodePage"
	out := make(map[string]json.RawMessage, len(p.Extra)+3)
	for k, v := range p.Extra {
		out[k] = v
	}
	var err error
	if out[fieldVersion], err = json.Marshal(p.Version.String()); err != nil {
		return nil, wrap(ErrKindMalformed, op, "编码 version 失败", err)
	}
	if out[fieldCursor], err = json.Marshal(p.Cursor); err != nil {
		return nil, wrap(ErrKindMalformed, op, "编码 cursor 失败", err)
	}
	items := p.Items
	if items == nil {
		items = []json.RawMessage{}
	}
	if out[fieldItems], err = json.Marshal(items); err != nil {
		return nil, wrap(ErrKindMalformed, op, fmt.Sprintf("编码 items 失败: %v", err), err)
	}
	data, err := json.Marshal(out)
	if err != nil {
		return nil, wrap(ErrKindMalformed, op, "编码响应失败", err)
	}
	return data, nil
}
