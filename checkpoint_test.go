package sdk

import (
	"context"
	"path/filepath"
	"testing"
)

func TestCursorRoundTrip(t *testing.T) {
	orig := Cursor{Seq: 42, Token: "tok-xyz"}
	s := EncodeCursor(orig)
	got, err := DecodeCursor(s)
	logErr(t, "roundtrip "+s, err)
	if err != nil || got != orig {
		t.Fatalf("游标往返不一致: %+v, %v", got, err)
	}
}

func TestDecodeCursor_Invalid(t *testing.T) {
	valid := EncodeCursor(Cursor{Seq: 1, Token: "a"})
	cases := []string{
		"",
		"not-a-cursor",
		"sdk1.!!!.deadbeef",
		valid[:len(valid)-1] + "0", // 篡改校验和
		"sdk2." + valid[5:],        // 未知前缀
	}
	for _, input := range cases {
		_, err := DecodeCursor(input)
		logErr(t, "decode "+input, err)
		if KindOf(err) != ErrKindCursorInvalid {
			t.Fatalf("DecodeCursor(%q) kind = %s; want cursor_invalid", input, KindOf(err))
		}
		if Retryable(err) {
			t.Fatalf("DecodeCursor(%q) 不应可重试", input)
		}
	}
}

func TestMemoryStore_RejectRegression(t *testing.T) {
	m := NewMemoryStore()
	ctx := context.Background()
	cp5 := Checkpoint{Cursor: EncodeCursor(Cursor{Seq: 5, Token: "t5"}), Seq: 5}
	if err := m.Save(ctx, cp5); err != nil {
		t.Fatalf("保存前进游标应成功: %v", err)
	}
	// 回退：序号更小，必须被显式拒绝。
	cp3 := Checkpoint{Cursor: EncodeCursor(Cursor{Seq: 3, Token: "t3"}), Seq: 3}
	err := m.Save(ctx, cp3)
	logErr(t, "已确认 seq=5 后保存 seq=3", err)
	if KindOf(err) != ErrKindCursorRegression {
		t.Fatalf("kind = %s; want cursor_regression", KindOf(err))
	}
	// 存储内容未被破坏。
	got, _ := m.Load(ctx)
	if got != cp5 {
		t.Fatalf("回退被拒绝后检查点被污染: %+v", got)
	}
}

func TestMemoryStore_RejectInvalidCursor(t *testing.T) {
	m := NewMemoryStore()
	err := m.Save(context.Background(), Checkpoint{Cursor: "garbage", Seq: 1})
	logErr(t, "保存非法游标", err)
	if KindOf(err) != ErrKindCursorInvalid {
		t.Fatalf("kind = %s; want cursor_invalid", KindOf(err))
	}
	// 游标序号与检查点序号不一致同样拒绝。
	err = m.Save(context.Background(), Checkpoint{
		Cursor: EncodeCursor(Cursor{Seq: 9, Token: "t"}), Seq: 2})
	logErr(t, "游标序号与检查点序号不一致", err)
	if KindOf(err) != ErrKindCursorInvalid {
		t.Fatalf("kind = %s; want cursor_invalid", KindOf(err))
	}
}

func TestFileStore_PersistAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	ctx := context.Background()

	fs1 := NewFileStore(path)
	// 首次运行：无文件，从零开始。
	cp, err := fs1.Load(ctx)
	if err != nil || cp != (Checkpoint{}) {
		t.Fatalf("首次 Load = %+v, %v", cp, err)
	}
	want := Checkpoint{Cursor: EncodeCursor(Cursor{Seq: 7, Token: "t7"}), Seq: 7}
	if err := fs1.Save(ctx, want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// 模拟重启：新建 FileStore 实例从磁盘恢复。
	fs2 := NewFileStore(path)
	got, err := fs2.Load(ctx)
	logErr(t, "重启后恢复检查点", err)
	if err != nil || got != want {
		t.Fatalf("重启恢复不一致: %+v, %v", got, err)
	}
	t.Logf("重启恢复: seq=%d cursor=%s", got.Seq, got.Cursor)

	// 重启后回退同样被拒绝。
	err = fs2.Save(ctx, Checkpoint{Cursor: EncodeCursor(Cursor{Seq: 1, Token: "t1"}), Seq: 1})
	logErr(t, "重启后保存回退游标", err)
	if KindOf(err) != ErrKindCursorRegression {
		t.Fatalf("kind = %s; want cursor_regression", KindOf(err))
	}
}
