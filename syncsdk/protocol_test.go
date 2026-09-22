package syncsdk

import (
	"strings"
	"testing"
)

func TestParsePageVersionMismatch(t *testing.T) {
	input := `{"version":99,"items":[],"next_cursor":""}`
	t.Logf("input: %s", input)
	_, err := parsePage([]byte(input))
	t.Logf("normalized error: %v (kind=%s)", err, KindOf(err))
	if !IsKind(err, KindUnknownVersion) {
		t.Fatalf("want kind %s, got %v", KindUnknownVersion, err)
	}
}

func TestParsePageMissingField(t *testing.T) {
	cases := []string{
		`{"items":[],"next_cursor":""}`,          // 缺 version
		`{"version":2,"next_cursor":""}`,        // 缺 items
		`{"version":2,"items":[]}`,              // 缺 next_cursor
		`{"version":2,"items":[{"data":{}}],"next_cursor":""}`, // item 缺 id
	}
	for _, input := range cases {
		t.Logf("input: %s", input)
		_, err := parsePage([]byte(input))
		t.Logf("normalized error: %v (kind=%s)", err, KindOf(err))
		if !IsKind(err, KindMissingField) {
			t.Fatalf("want kind %s, got %v", KindMissingField, err)
		}
	}
}

func TestParsePageMalformed(t *testing.T) {
	cases := []string{
		`not json at all`,
		`{"version":"two","items":[],"next_cursor":""}`,
		`{"version":2,"items":{},"next_cursor":""}`,
		`{"version":2,"items":[{"id":7}],"next_cursor":""}`,
		`{"version":2,"items":[],"next_cursor":42}`,
	}
	for _, input := range cases {
		t.Logf("input: %s", input)
		_, err := parsePage([]byte(input))
		t.Logf("normalized error: %v (kind=%s)", err, KindOf(err))
		if !IsKind(err, KindMalformed) {
			t.Fatalf("want kind %s, got %v", KindMalformed, err)
		}
		// 底层解析异常不得直接透出：必须包装在 *Error 内。
		var se *Error
		if !asError(err, &se) || se.Cause == nil {
			t.Fatalf("malformed error must wrap the underlying cause: %v", err)
		}
	}
}

func TestParsePageUnknownFieldsPreserved(t *testing.T) {
	// v2 响应带新增字段，旧版本使用方不得报错或静默丢弃。
	input := `{"version":2,"items":[{"id":"a","data":{}}],"next_cursor":"","trace_id":"t-1","extra_meta":{"x":1}}`
	t.Logf("input: %s", input)
	page, err := parsePage([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Logf("extra fields: %v", page.Extra)
	if page.Extra["trace_id"] != "t-1" {
		t.Fatalf("unknown field trace_id dropped: %v", page.Extra)
	}
	if _, ok := page.Extra["extra_meta"]; !ok {
		t.Fatalf("unknown field extra_meta dropped: %v", page.Extra)
	}
	if len(page.Items) != 1 || page.Items[0].ID != "a" {
		t.Fatalf("items parsed wrong: %+v", page.Items)
	}
}

func TestNegotiateVersion(t *testing.T) {
	for _, v := range SupportedVersions {
		got, err := negotiateVersion(v)
		t.Logf("input version=%d -> negotiated=%d err=%v", v, got, err)
		if err != nil || got != v {
			t.Fatalf("supported version %d rejected: %v", v, err)
		}
	}
	if _, err := negotiateVersion(0); !IsKind(err, KindUnknownVersion) {
		t.Fatalf("version 0 must be rejected, got %v", err)
	}
	if _, err := negotiateVersion(99); !IsKind(err, KindUnknownVersion) {
		t.Fatalf("version 99 must be rejected, got %v", err)
	}
}

func asError(err error, target **Error) bool {
	for err != nil {
		if se, ok := err.(*Error); ok {
			*target = se
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestErrorMessageContainsKind(t *testing.T) {
	err := &Error{Kind: KindMalformed, Message: "x"}
	if !strings.Contains(err.Error(), string(KindMalformed)) {
		t.Fatalf("error string should contain kind: %s", err.Error())
	}
}
