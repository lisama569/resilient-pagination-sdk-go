package sdk

import (
	"encoding/json"
	"testing"
)

func TestDecodePage_Malformed(t *testing.T) {
	cases := []struct {
		name  string
		input string
		kind  ErrorKind
	}{
		{name: "非JSON", input: `not-json`, kind: ErrKindMalformed},
		{name: "JSON数组而非对象", input: `[1,2,3]`, kind: ErrKindMalformed},
		{name: "version类型非法", input: `{"version":123,"cursor":"","items":[]}`, kind: ErrKindMalformed},
		{name: "version取值非法", input: `{"version":"x.y","cursor":"","items":[]}`, kind: ErrKindMalformed},
		{name: "cursor类型非法", input: `{"version":"1.0","cursor":{},"items":[]}`, kind: ErrKindMalformed},
		{name: "items类型非法", input: `{"version":"1.0","cursor":"","items":"nope"}`, kind: ErrKindMalformed},
		{name: "缺失version", input: `{"cursor":"","items":[]}`, kind: ErrKindMissingField},
		{name: "缺失cursor", input: `{"version":"1.0","items":[]}`, kind: ErrKindMissingField},
		{name: "缺失items", input: `{"version":"1.0","cursor":""}`, kind: ErrKindMissingField},
		{name: "未知主版本", input: `{"version":"7.0","cursor":"","items":[]}`, kind: ErrKindVersionUnknown},
	}
	for _, tc := range cases {
		_, err := DecodePage([]byte(tc.input))
		logErr(t, tc.name+" body="+tc.input, err)
		if KindOf(err) != tc.kind {
			t.Fatalf("%s: kind = %s; want %s", tc.name, KindOf(err), tc.kind)
		}
		if Retryable(err) {
			t.Fatalf("%s: 结构/版本类错误不应可重试", tc.name)
		}
	}
}

func TestDecodePage_UnknownFieldsPreserved(t *testing.T) {
	// 模拟 1.2 服务端向 1.0 客户端返回新增字段：旧端不报错、不丢弃。
	input := `{"version":"1.2","cursor":"","items":["a","b"],` +
		`"total_count":2,"next_hint":{"prefetch":true}}`
	page, err := DecodePage([]byte(input))
	logErr(t, "带新增字段的响应", err)
	if err != nil {
		t.Fatalf("新增字段不应导致报错: %v", err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("items 数量 = %d; want 2", len(page.Items))
	}
	if len(page.Extra) != 2 {
		t.Fatalf("Extra 应保留 2 个新增字段, got %v", page.Extra)
	}
	var total int
	if err := json.Unmarshal(page.Extra["total_count"], &total); err != nil || total != 2 {
		t.Fatalf("新增字段 total_count 未完整保留: %v, %v", total, err)
	}
	// 往返编码后新增字段仍在。
	out, err := EncodePage(page)
	if err != nil {
		t.Fatalf("EncodePage: %v", err)
	}
	again, err := DecodePage(out)
	if err != nil {
		t.Fatalf("往返解码失败: %v", err)
	}
	if len(again.Extra) != 2 {
		t.Fatalf("往返后新增字段丢失: %v", again.Extra)
	}
	t.Logf("input=%s -> extra 字段保留 %d 个，往返一致", input, len(again.Extra))
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	orig := &Page{
		Version: Version{1, 1},
		Cursor:  EncodeCursor(Cursor{Seq: 3, Token: "tok-3"}),
		Items:   []json.RawMessage{json.RawMessage(`"x"`)},
	}
	data, err := EncodePage(orig)
	if err != nil {
		t.Fatalf("EncodePage: %v", err)
	}
	got, err := DecodePage(data)
	if err != nil {
		t.Fatalf("DecodePage: %v", err)
	}
	if got.Version != orig.Version || got.Cursor != orig.Cursor || len(got.Items) != 1 {
		t.Fatalf("往返不一致: %+v", got)
	}
}
