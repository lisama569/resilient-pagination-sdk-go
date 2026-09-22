package syncsdk

import (
	"encoding/json"
	"fmt"
)

// SupportedVersions 是客户端可协商的协议版本（降序）。
var SupportedVersions = []int{2, 1}

// Page 是归一化后的分页响应。
type Page struct {
	Version    int
	Items      []Item
	NextCursor string // 空串表示没有下一页
	// Extra 保留旧版本使用方不认识的新增字段：既不报错也不静默丢弃。
	Extra map[string]any
}

// Item 是同步的数据记录。
type Item struct {
	ID   string
	Data map[string]any
}

// 响应信封中的必填字段。
var requiredFields = []string{"version", "items", "next_cursor"}

// negotiateVersion 在客户端支持版本与服务端声明版本间协商。
// 服务端版本必须落在支持集合内，否则返回 KindUnknownVersion。
func negotiateVersion(serverVersion int) (int, error) {
	for _, v := range SupportedVersions {
		if v == serverVersion {
			return serverVersion, nil
		}
	}
	return 0, &Error{
		Kind:    KindUnknownVersion,
		Message: fmt.Sprintf("unsupported protocol version %d (supported: %v)", serverVersion, SupportedVersions),
	}
}

// parsePage 解析并校验原始响应体。
// 未知字段被保留进 Page.Extra；缺失必填字段、未知版本、结构非法
// 分别返回 KindMissingField / KindUnknownVersion / KindMalformed，
// 底层 json 异常一律包装进 *Error.Cause，不直接透出。
func parsePage(body []byte) (*Page, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, &Error{Kind: KindMalformed, Message: "response is not a valid JSON object", Cause: err}
	}
	for _, f := range requiredFields {
		if _, ok := raw[f]; !ok {
			return nil, &Error{Kind: KindMissingField, Message: "missing required field: " + f}
		}
	}

	var version int
	if err := json.Unmarshal(raw["version"], &version); err != nil {
		return nil, &Error{Kind: KindMalformed, Message: "field version is not an integer", Cause: err}
	}
	if _, err := negotiateVersion(version); err != nil {
		return nil, err
	}

	var rawItems []json.RawMessage
	if err := json.Unmarshal(raw["items"], &rawItems); err != nil {
		return nil, &Error{Kind: KindMalformed, Message: "field items is not an array", Cause: err}
	}
	items := make([]Item, 0, len(rawItems))
	for i, ri := range rawItems {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(ri, &m); err != nil {
			return nil, &Error{Kind: KindMalformed, Message: fmt.Sprintf("items[%d] is not an object", i), Cause: err}
		}
		idRaw, ok := m["id"]
		if !ok {
			return nil, &Error{Kind: KindMissingField, Message: fmt.Sprintf("items[%d] missing required field: id", i)}
		}
		var id string
		if err := json.Unmarshal(idRaw, &id); err != nil {
			return nil, &Error{Kind: KindMalformed, Message: fmt.Sprintf("items[%d].id is not a string", i), Cause: err}
		}
		data := map[string]any{}
		if d, ok := m["data"]; ok {
			if err := json.Unmarshal(d, &data); err != nil {
				return nil, &Error{Kind: KindMalformed, Message: fmt.Sprintf("items[%d].data is not an object", i), Cause: err}
			}
		}
		items = append(items, Item{ID: id, Data: data})
	}

	var nextCursor string
	if err := json.Unmarshal(raw["next_cursor"], &nextCursor); err != nil {
		return nil, &Error{Kind: KindMalformed, Message: "field next_cursor is not a string", Cause: err}
	}

	// 保留未知字段，旧版本使用方不报错也不静默丢弃。
	extra := map[string]any{}
	known := map[string]bool{"version": true, "items": true, "next_cursor": true}
	for k, v := range raw {
		if known[k] {
			continue
		}
		var anyVal any
		if err := json.Unmarshal(v, &anyVal); err != nil {
			return nil, &Error{Kind: KindMalformed, Message: "unknown field " + k + " is not valid JSON", Cause: err}
		}
		extra[k] = anyVal
	}

	return &Page{Version: version, Items: items, NextCursor: nextCursor, Extra: extra}, nil
}
