package sdk

import (
	"testing"
)

func TestParseVersion(t *testing.T) {
	cases := []struct {
		input string
		want  Version
		kind  ErrorKind // ErrKindUnknown 之外的零值表示期望成功；用 -1 表示成功
	}{
		{input: "1.0", want: Version{1, 0}, kind: ErrorKind(-1)},
		{input: "2.15", want: Version{2, 15}, kind: ErrorKind(-1)},
		{input: "1", kind: ErrKindMalformed},
		{input: "1.2.3", kind: ErrKindMalformed},
		{input: "a.b", kind: ErrKindMalformed},
		{input: "-1.0", kind: ErrKindMalformed},
		{input: "", kind: ErrKindMalformed},
	}
	for _, tc := range cases {
		v, err := ParseVersion(tc.input)
		logErr(t, tc.input, err)
		if tc.kind == ErrorKind(-1) {
			if err != nil || v != tc.want {
				t.Fatalf("ParseVersion(%q) = %v, %v; want %v, nil", tc.input, v, err, tc.want)
			}
			continue
		}
		if KindOf(err) != tc.kind {
			t.Fatalf("ParseVersion(%q) kind = %s; want %s", tc.input, KindOf(err), tc.kind)
		}
	}
}

func TestNegotiate(t *testing.T) {
	cases := []struct {
		name    string
		offered string
		want    Version
		kind    ErrorKind
	}{
		{name: "精确匹配", offered: "1.2", want: Version{1, 2}, kind: ErrorKind(-1)},
		{name: "对端次版本更高_降级到已知最高", offered: "1.9", want: Version{1, 2}, kind: ErrorKind(-1)},
		{name: "对端次版本较低_取不超过的最高", offered: "1.1", want: Version{1, 1}, kind: ErrorKind(-1)},
		{name: "未知主版本", offered: "9.0", kind: ErrKindVersionUnknown},
	}
	for _, tc := range cases {
		offered, _ := ParseVersion(tc.offered)
		got, err := Negotiate(SupportedVersions, offered)
		logErr(t, tc.name+" offered="+tc.offered, err)
		if tc.kind == ErrorKind(-1) {
			if err != nil || got != tc.want {
				t.Fatalf("Negotiate(%s) = %v, %v; want %v, nil", tc.offered, got, err, tc.want)
			}
			continue
		}
		if KindOf(err) != tc.kind {
			t.Fatalf("Negotiate(%s) kind = %s; want %s", tc.offered, KindOf(err), tc.kind)
		}
	}
}

func TestCompatibleWith(t *testing.T) {
	old := Version{1, 0}
	if !old.CompatibleWith(Version{1, 7}) {
		t.Fatal("同主版本应兼容（新增字段由旧端保留）")
	}
	if old.CompatibleWith(Version{2, 0}) {
		t.Fatal("跨主版本不应兼容")
	}
}
