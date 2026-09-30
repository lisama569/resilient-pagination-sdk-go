package syncsdk

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestFileStoreRoundTrip 基础落盘/读回。
func TestFileStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	fs, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	cp := &Checkpoint{Cursor: EncodeCursor(10, 2), Seq: 10}
	if err := fs.SaveStream(ctx, "orders", cp); err != nil {
		t.Fatalf("save: %v", err)
	}

	// 用“新进程”视角重新打开同一目录。
	fs2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fs2.LoadStream(ctx, "orders")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got.Seq != 10 || got.Cursor != cp.Cursor {
		t.Fatalf("round trip mismatch: %+v", got)
	}

	ids, err := fs2.ListStreams(ctx)
	if err != nil || len(ids) != 1 || ids[0] != "orders" {
		t.Fatalf("ListStreams=%v err=%v", ids, err)
	}
	t.Logf("persisted file content:\n%s", readFile(t, filepath.Join(dir, "orders.ckpt.json")))
}

// TestFileStoreStreamIsolation 各流独立文件、互不串用。
func TestFileStoreStreamIsolation(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	fs, _ := NewFileStore(dir)
	if err := fs.SaveStream(ctx, "a", &Checkpoint{Cursor: EncodeCursor(4, 2), Seq: 4}); err != nil {
		t.Fatal(err)
	}
	if err := fs.SaveStream(ctx, "b", &Checkpoint{Cursor: EncodeCursor(8, 2), Seq: 8}); err != nil {
		t.Fatal(err)
	}
	a, _ := fs.LoadStream(ctx, "a")
	b, _ := fs.LoadStream(ctx, "b")
	if a.Seq != 4 || b.Seq != 8 {
		t.Fatalf("stream checkpoints leaked: a=%+v b=%+v", a, b)
	}
	missing, err := fs.LoadStream(ctx, "nope")
	if err != nil || missing != nil {
		t.Fatalf("missing stream should be (nil,nil), got %v %v", missing, err)
	}
}

// TestFileStoreRejectsRegression 读回来的进度倒退必须显式拒绝。
func TestFileStoreRejectsRegression(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	fs, _ := NewFileStore(dir)
	if err := fs.SaveStream(ctx, "s", &Checkpoint{Cursor: EncodeCursor(10, 2), Seq: 10}); err != nil {
		t.Fatal(err)
	}
	// 游标偏移倒退。
	if err := fs.SaveStream(ctx, "s", &Checkpoint{Cursor: EncodeCursor(4, 2), Seq: 10}); !IsKind(err, KindCursorInvalid) {
		t.Fatalf("offset regression must be rejected, got %v", err)
	}
	// 序列号倒退（游标前进但 seq 变小）。
	if err := fs.SaveStream(ctx, "s", &Checkpoint{Cursor: EncodeCursor(12, 2), Seq: 5}); !IsKind(err, KindCursorInvalid) {
		t.Fatalf("seq regression must be rejected, got %v", err)
	}
	// 合法推进应被接受。
	if err := fs.SaveStream(ctx, "s", &Checkpoint{Cursor: EncodeCursor(12, 2), Seq: 12}); err != nil {
		t.Fatalf("forward progress should be accepted, got %v", err)
	}
	// 完成终态（空游标、seq 不倒退）应被接受，且重启读回视为已完成。
	if err := fs.SaveStream(ctx, "done", &Checkpoint{Cursor: "", Seq: 6}); err != nil {
		t.Fatalf("completion terminal save rejected: %v", err)
	}
}

// TestFileStoreCorruption 内容对不上（被篡改/损坏）必须报 checkpoint_corrupt 拒绝继续。
func TestFileStoreCorruption(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	fs, _ := NewFileStore(dir)
	if err := fs.SaveStream(ctx, "s", &Checkpoint{Cursor: EncodeCursor(10, 2), Seq: 10}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "s.ckpt.json")

	t.Run("tampered_seq", func(t *testing.T) {
		raw := readFile(t, path)
		tampered := strings.Replace(string(raw), `"seq": 10`, `"seq": 99`, 1)
		if tampered == string(raw) {
			t.Fatal("test setup failed to patch seq")
		}
		os.WriteFile(path, []byte(tampered), 0o600)
		_, err := fs.LoadStream(ctx, "s")
		t.Logf("tampered seq -> %v (kind=%s)", err, KindOf(err))
		if !IsKind(err, KindCheckpointCorrupt) {
			t.Fatalf("tampered checkpoint must be rejected as checkpoint_corrupt, got %v", err)
		}
	})

	t.Run("stream_name_mismatch", func(t *testing.T) {
		// 把 a 流的文件内容复制成 s 流文件（checksum 有效但流名对不上）。
		fs2, _ := NewFileStore(t.TempDir())
		fs2.SaveStream(ctx, "a", &Checkpoint{Cursor: EncodeCursor(10, 2), Seq: 10})
		src := filepath.Join(fs2.dir, "a.ckpt.json")
		data := readFile(t, src)
		os.WriteFile(filepath.Join(dir, "mismatch.ckpt.json"), data, 0o600)
		_, err := fs.LoadStream(ctx, "mismatch")
		t.Logf("stream name mismatch -> %v (kind=%s)", err, KindOf(err))
		if !IsKind(err, KindCheckpointCorrupt) {
			t.Fatalf("stream-name mismatch must be rejected, got %v", err)
		}
	})

	t.Run("garbage_json", func(t *testing.T) {
		os.WriteFile(path, []byte("not json"), 0o600)
		_, err := fs.LoadStream(ctx, "s")
		if !IsKind(err, KindCheckpointCorrupt) {
			t.Fatalf("garbage checkpoint must be rejected, got %v", err)
		}
	})
}

// TestFileStoreUnsafeStreamName 路径穿越/非法流名拒绝。
func TestFileStoreUnsafeStreamName(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewFileStore(dir)
	ctx := context.Background()
	for _, bad := range []StreamID{"", "../escape", "a/b", "x:y"} {
		err := fs.SaveStream(ctx, bad, &Checkpoint{Seq: 1})
		t.Logf("stream id=%q -> %v (kind=%s)", bad, err, KindOf(err))
		if !IsKind(err, KindMalformed) {
			t.Fatalf("unsafe stream id %q must be rejected, got %v", bad, err)
		}
	}
}

// TestEngineRestartAcrossProcesses 真正跨“进程”（新 FileStore、新 Client、新 Manager）
// 从落盘进度恢复并跑完，无丢无重。
func TestEngineRestartAcrossProcesses(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	const pageSize, total = 2, 8
	id := StreamID("orders")

	runOnce := func(failPage int) (*Result, error, *multiCommitSink) {
		disp := newDispatchingTransport()
		disp.add(string(id))
		first := disp.setupStream(string(id), streamBase(0), pageSize, total)
		if failPage >= 0 {
			disp.injectFailureAt(string(id), streamBase(0), failPage, pageSize,
				&RetryableError{Reason: "503"}) // 非重试配置下立即失败
		}
		fs, _ := NewFileStore(dir) // 重新打开目录，模拟新进程
		c := NewClient(disp, Config{RatePerSecond: 1000, MaxRetries: 0})
		sink := newMultiCommitSink()
		out := NewManager(c, fs).RunAll(ctx, []StreamSpec{{
			ID: id, StartCursor: first, MaxInFlight: 1, Commit: sink.commit,
		}})
		o := out[id]
		return o.Result, o.Err, sink
	}

	// 第一次：第 4 页（pageIndex=3）失败，前 3 页共 6 条已确认并落盘。
	res1, err1, _ := runOnce(3)
	t.Logf("run1: seq=%d stop=%s err=%v(%s)", res1.Checkpoint.Seq, res1.Stop, err1, KindOf(err1))
	if !IsKind(err1, KindRetryExhausted) {
		t.Fatalf("run1 want retry_exhausted, got %v", err1)
	}
	if res1.Checkpoint.Seq != 6 {
		t.Fatalf("run1 confirmed seq=%d want 6", res1.Checkpoint.Seq)
	}

	// 第二次：全新替身（无失败）+ 全新 FileStore/Client/Manager，从磁盘恢复跑完。
	res2, err2, _ := runOnce(-1)
	t.Logf("run2: seq=%d stop=%s completed=%v err=%v", res2.Checkpoint.Seq, res2.Stop, res2.Completed, err2)
	if err2 != nil || !res2.Completed {
		t.Fatalf("run2 should complete, got %v", err2)
	}
	if res2.Checkpoint.Seq != total {
		// 第二次只应补抓最后一页 2 条（8-6=2），累计序号为 total。
		t.Fatalf("run2 total seq=%d want %d", res2.Checkpoint.Seq, total)
	}
	if len(res2.Items) != 2 {
		t.Fatalf("run2 newly fetched items=%d want 2 (resume must not re-fetch committed pages)", len(res2.Items))
	}

	// 跨进程幂等 sink 视角：用一个聚合 sink 重新统计并不容易（两个 sink），
	// 这里验证落盘总进度 == total，且第二次没有回退/重复抓取已确认页。
	fs3, _ := NewFileStore(dir)
	final, _ := fs3.LoadStream(ctx, id)
	if final.Seq != total || final.Cursor != "" {
		t.Fatalf("final checkpoint=%+v want seq=%d empty cursor", final, total)
	}
	// 再“重启”一次：已是完成终态，不应再抓取。
	res3, err3, sink3 := runOnce(-1)
	if err3 != nil || !res3.Completed || res3.Checkpoint.Seq != total {
		t.Fatalf("third run after completion should short-circuit, got %+v err=%v", res3, err3)
	}
	if len(sink3.ids(id)) != 0 {
		t.Fatalf("completed stream should not be re-fetched, got %d items", len(sink3.ids(id)))
	}
}

func readFile(t *testing.T, p string) []byte {
	t.Helper()
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
