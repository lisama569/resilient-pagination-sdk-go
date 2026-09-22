package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// failAtSink 在第 n 次 Commit 时注入失败（模拟提交中途崩溃）。
type failAtSink struct {
	inner *memSink
	n     int
	count int
}

func (f *failAtSink) Commit(ctx context.Context, seq uint64, items []json.RawMessage) error {
	f.count++
	if f.count == f.n {
		return errors.New("注入的提交失败")
	}
	return f.inner.Commit(ctx, seq, items)
}

func TestSync_FullChain(t *testing.T) {
	pages, want := buildChain(5, 2)
	ft := newFakeTransport(pages)
	c := NewClient(ft, WithBackoff(testBackoff(2)))
	sink := &memSink{}
	s := NewSyncer(c, NewMemoryStore(), sink)

	cp, err := s.Run(context.Background())
	logErr(t, "全量同步 5 页", err)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !cp.Done {
		t.Fatalf("同步应完成: %+v", cp)
	}
	got := sink.snapshot()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("数据不一致:\n got=%v\nwant=%v", got, want)
	}
	t.Logf("同步完成: %d 条数据，顺序一致，无遗漏无重复", len(got))
}

func TestSync_RestartNoLossNoDup(t *testing.T) {
	pages, want := buildChain(6, 2)
	path := filepath.Join(t.TempDir(), "checkpoint.json")

	// 第一次运行：第 4 页提交时崩溃。
	ft1 := newFakeTransport(pages)
	c1 := NewClient(ft1, WithBackoff(testBackoff(2)))
	sink1 := &memSink{}
	s1 := NewSyncer(c1, NewFileStore(path), &failAtSink{inner: sink1, n: 4})
	cp1, err := s1.Run(context.Background())
	logErr(t, "首次运行在第 4 页提交时崩溃", err)
	if err == nil {
		t.Fatal("注入失败应使 Run 返回错误")
	}
	t.Logf("崩溃时已确认进度: seq=%d 已提交=%d 条", cp1.Seq, len(sink1.snapshot()))

	// 重启：新客户端、新同步器、新 sink，从文件检查点恢复。
	ft2 := newFakeTransport(pages)
	c2 := NewClient(ft2, WithBackoff(testBackoff(2)))
	sink2 := &memSink{}
	s2 := NewSyncer(c2, NewFileStore(path), sink2)
	cp2, err := s2.Run(context.Background())
	logErr(t, "重启后从检查点恢复", err)
	if err != nil {
		t.Fatalf("恢复运行失败: %v", err)
	}
	if !cp2.Done {
		t.Fatalf("恢复后应同步完成: %+v", cp2)
	}

	// 两次运行的提交拼接 = 完整数据，不漏不重。
	got := append(sink1.snapshot(), sink2.snapshot()...)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("重启后数据不一致:\n got=%v\nwant=%v", got, want)
	}
	t.Logf("重启恢复: 第一段 %d 条 + 第二段 %d 条 = %d 条，与期望完全一致",
		len(sink1.snapshot()), len(sink2.snapshot()), len(got))
}

func TestSync_RepeatAfterDoneIsNoop(t *testing.T) {
	pages, want := buildChain(3, 1)
	ft := newFakeTransport(pages)
	c := NewClient(ft, WithBackoff(testBackoff(2)))
	store := NewMemoryStore()
	sink := &memSink{}
	s := NewSyncer(c, store, sink)

	if _, err := s.Run(context.Background()); err != nil {
		t.Fatalf("首次 Run: %v", err)
	}
	callsBefore := len(ft.calls)
	cp, err := s.Run(context.Background())
	logErr(t, "完成后重复消费", err)
	if err != nil || !cp.Done {
		t.Fatalf("重复 Run = %+v, %v", cp, err)
	}
	if len(ft.calls) != callsBefore {
		t.Fatalf("完成后重复消费不应再发起请求: %d -> %d", callsBefore, len(ft.calls))
	}
	if got := sink.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("重复消费产生重复数据: %v", got)
	}
	t.Logf("重复消费为 no-op：请求数 %d 不变，数据 %d 条无重复", len(ft.calls), len(want))
}

func TestSync_CorruptedCheckpointRejected(t *testing.T) {
	pages, _ := buildChain(3, 1)
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	// 写入游标被篡改的检查点（模拟游标失效）。
	tampered := Checkpoint{Cursor: "sdk1.AAAA.00000000", Seq: 2}
	data, _ := json.Marshal(tampered)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	ft := newFakeTransport(pages)
	c := NewClient(ft, WithBackoff(testBackoff(2)))
	s := NewSyncer(c, NewFileStore(path), &memSink{})
	_, err := s.Run(context.Background())
	logErr(t, "检查点游标被篡改", err)
	if KindOf(err) != ErrKindCursorInvalid {
		t.Fatalf("kind = %s; want cursor_invalid", KindOf(err))
	}
	if len(ft.calls) != 0 {
		t.Fatalf("游标失效应被拒绝而不是从中间继续，实际发起 %d 次请求", len(ft.calls))
	}
}

func TestSync_ServerCursorRegressionRejected(t *testing.T) {
	// 服务端在第二页返回不前进的游标（序号原地踏步）。
	cur1 := EncodeCursor(Cursor{Seq: 1, Token: "t1"})
	pages := map[string]*Page{
		"":    {Cursor: cur1, Items: []json.RawMessage{json.RawMessage(`"a"`)}},
		cur1:  {Cursor: cur1, Items: []json.RawMessage{json.RawMessage(`"b"`)}},
	}
	ft := newFakeTransport(pages)
	c := NewClient(ft, WithBackoff(testBackoff(2)))
	s := NewSyncer(c, NewMemoryStore(), &memSink{})
	_, err := s.Run(context.Background())
	logErr(t, "服务端返回回退游标", err)
	if KindOf(err) != ErrKindCursorRegression {
		t.Fatalf("kind = %s; want cursor_regression", KindOf(err))
	}
}

func TestSync_CommitBeforeCheckpointOrdering(t *testing.T) {
	// 验证顺序语义：检查点推进必须发生在数据提交之后。
	pages, _ := buildChain(4, 1)
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	ft := newFakeTransport(pages)
	c := NewClient(ft, WithBackoff(testBackoff(2)))
	sink := &memSink{}
	store := NewFileStore(path)
	s := NewSyncer(c, store, sink)

	if _, err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 每个已保存的检查点序号都必须不超过已提交的数据页数。
	cp, _ := store.Load(context.Background())
	committed := len(sink.snapshot())
	t.Logf("最终检查点 seq=%d（Done=%v）已提交数据=%d 条", cp.Seq, cp.Done, committed)
	if !cp.Done || committed != 4 {
		t.Fatalf("顺序语义破坏: checkpoint=%+v committed=%d", cp, committed)
	}
	// 再验证中间态：崩溃场景下检查点序号 == 已提交页数（见 Restart 测试），
	// 此处验证 sink 收到的序号严格递增。
	for i, seq := range sink.seqs {
		if seq != uint64(i+1) {
			t.Fatalf("提交序号不连续: seqs=%v", sink.seqs)
		}
	}
}

func TestSync_RetryDuringSyncNoDup(t *testing.T) {
	// 同步过程中叠加限流与临时失败：重试不得产生重复数据。
	pages, want := buildChain(4, 2)
	ft := newFakeTransport(pages)
	ft.rateLimitN = 2
	ft.failN = 2
	c := NewClient(ft, WithBackoff(testBackoff(8)))
	sink := &memSink{}
	s := NewSyncer(c, NewMemoryStore(), sink)

	cp, err := s.Run(context.Background())
	totalRetries := 0
	for _, n := range ft.idemSeen {
		totalRetries += n - 1
	}
	t.Logf("input=限流x2+临时失败x2 -> 总重试=%d err=%v", totalRetries, err)
	if err != nil {
		t.Fatalf("可重试失败应被吸收: %v", err)
	}
	if !cp.Done {
		t.Fatalf("应同步完成: %+v", cp)
	}
	if got := sink.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("重试后数据不一致:\n got=%v\nwant=%v", got, want)
	}
	t.Logf("重试 %d 次后数据 %d 条无重复", totalRetries, len(want))
}
