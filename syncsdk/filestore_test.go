package syncsdk

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeEnvelopeFile 手工构造一个检查点文件（测试用），可指定任意字段用于异常注入。
func writeEnvelopeFile(t *testing.T, path, streamID, cursor string, seq, gen uint64, mutate func(*checkpointEnvelope)) {
	t.Helper()
	env := &checkpointEnvelope{Format: checkpointFormatV1, Stream: streamID, Cursor: cursor, Seq: seq, Gen: gen}
	sum, err := envelopeChecksum(env)
	if err != nil {
		t.Fatal(err)
	}
	env.Checksum = sum
	if mutate != nil {
		mutate(env)
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestFileStoreMissingFileIsFreshStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	s, err := NewFileStore(path, "orders")
	if err != nil {
		t.Fatal(err)
	}
	cp, err := s.Load(context.Background())
	t.Logf("missing file -> cp=%v err=%v", cp, err)
	if err != nil || cp != nil {
		t.Fatalf("want (nil,nil) on fresh start, got (%v,%v)", cp, err)
	}
}

func TestFileStoreRoundTripAcrossProcesses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "cp.json")
	want := &Checkpoint{Cursor: EncodeCursor(4, 2), Seq: 4}

	// “进程 A”写入。
	s1, _ := NewFileStore(path, "orders")
	if err := s1.Save(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	// “进程 B”是另一个实例，仅靠磁盘文件恢复。
	s2, _ := NewFileStore(path, "orders")
	got, err := s2.Load(context.Background())
	if err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	t.Logf("reloaded checkpoint: %+v (file=%s)", got, path)
	if got.Cursor != want.Cursor || got.Seq != want.Seq {
		t.Fatalf("round trip mismatch: %+v vs %+v", got, want)
	}
}

func TestFileStoreEndToEndResumeAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "orders.json")

	// 第一次运行：第二页永久失败，第一页进度必须已经落盘。
	ft1 := newFakeTransport()
	setupPages(ft1, 2, 6)
	ft1.injectFailure(EncodeCursor(2, 2), fmt.Errorf("boom"), fmt.Errorf("boom"))
	sink := newCommitLog()
	cfg := Config{RatePerSecond: 1000, MaxRetries: 1, InitialBackoff: time.Millisecond}

	store1, _ := NewFileStore(path, "orders")
	syn1 := NewSyncer(NewClient(ft1, cfg), store1, sink.commit)
	res1, err1 := syn1.Run(context.Background())
	t.Logf("run1 items=%d err=%v (kind=%s) checkpoint=%+v", len(res1.Items), err1, KindOf(err1), res1.Checkpoint)
	if err1 == nil {
		t.Fatal("run1 should fail")
	}

	// “重启”：新 transport、新 client、新 FileStore 实例，只共享文件与 sink。
	ft2 := newFakeTransport()
	setupPages(ft2, 2, 6)
	store2, _ := NewFileStore(path, "orders")
	syn2 := NewSyncer(NewClient(ft2, cfg), store2, sink.commit)
	res2, err2 := syn2.Run(context.Background())
	if err2 != nil || !res2.Completed {
		t.Fatalf("run2 failed: %v", err2)
	}
	t.Logf("run2 completed items=%d unique committed=%d", len(res2.Items), len(sink.ids))
	if len(sink.ids) != 6 {
		t.Fatalf("want exactly 6 unique items after restart, got %d (%v)", len(sink.ids), sink.ids)
	}
	if res2.Checkpoint.Seq != 6 {
		t.Fatalf("want seq 6, got %d", res2.Checkpoint.Seq)
	}
}

func TestFileStoreRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	writeEnvelopeFile(t, path, "orders", EncodeCursor(2, 2), 2, 1, nil)

	// 篡改游标字节但保持 JSON 合法 -> 校验和不匹配。
	raw, _ := os.ReadFile(path)
	tampered := strings.Replace(string(raw), EncodeCursor(2, 2), EncodeCursor(4, 2), 1)
	if err := os.WriteFile(path, []byte(tampered), 0o600); err != nil {
		t.Fatal(err)
	}
	s, _ := NewFileStore(path, "orders")
	_, err := s.Load(context.Background())
	t.Logf("tampered file -> %v (kind=%s)", err, KindOf(err))
	if !IsKind(err, KindCheckpointCorrupt) {
		t.Fatalf("tampered checkpoint must be rejected, got %v", err)
	}

	// 截断成半截 JSON。
	if err := os.WriteFile(path, []byte(`{"format":1,"stre`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = s.Load(context.Background())
	t.Logf("truncated file -> %v (kind=%s)", err, KindOf(err))
	if !IsKind(err, KindCheckpointCorrupt) {
		t.Fatalf("truncated checkpoint must be rejected, got %v", err)
	}
}

func TestFileStoreRejectsForeignStream(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	writeEnvelopeFile(t, path, "orders", EncodeCursor(2, 2), 2, 1, nil)

	// 文件属于 orders，billing 不得把它当成自己的进度。
	s, _ := NewFileStore(path, "billing")
	_, err := s.Load(context.Background())
	t.Logf("foreign stream -> %v (kind=%s)", err, KindOf(err))
	if !IsKind(err, KindCheckpointCorrupt) {
		t.Fatalf("foreign-stream checkpoint must be rejected, got %v", err)
	}
}

func TestFileStoreRejectsRegression(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	s, _ := NewFileStore(path, "orders")

	cp2 := &Checkpoint{Cursor: EncodeCursor(2, 2), Seq: 2}
	cp6 := &Checkpoint{Cursor: EncodeCursor(6, 2), Seq: 6}
	if err := s.Save(context.Background(), cp2); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(context.Background(), cp6); err != nil {
		t.Fatal(err)
	}
	// 同进程内 Save 回退 -> 拒绝。
	if err := s.Save(context.Background(), cp2); !IsKind(err, KindCheckpointCorrupt) {
		t.Fatalf("save regression must be rejected, got %v", err)
	}

	// 已观测到 gen=2 后，磁盘被换成更旧的 gen=1 文件 -> Load 拒绝。
	writeEnvelopeFile(t, path, "orders", EncodeCursor(2, 2), 2, 1, nil)
	_, err := s.Load(context.Background())
	t.Logf("stale generation -> %v (kind=%s)", err, KindOf(err))
	if !IsKind(err, KindCheckpointCorrupt) {
		t.Fatalf("stale checkpoint generation must be rejected, got %v", err)
	}
}

func TestFileStoreRejectsInvalidCursor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	s, _ := NewFileStore(path, "orders")
	if err := s.Save(context.Background(), &Checkpoint{Cursor: "garbage", Seq: 1}); !IsKind(err, KindCursorInvalid) {
		t.Fatalf("saving invalid cursor must fail as cursor_invalid, got %v", err)
	}
}

func TestFileStoreDoneCheckpointRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	s, _ := NewFileStore(path, "orders")

	// 完成态落盘 -> 读回仍是完成态，进度不变。
	done := &Checkpoint{Cursor: "", Seq: 6, Done: true}
	if err := s.Save(context.Background(), done); err != nil {
		t.Fatalf("save done checkpoint: %v", err)
	}
	got, err := s.Load(context.Background())
	if err != nil {
		t.Fatalf("load done checkpoint: %v", err)
	}
	t.Logf("done checkpoint round trip: seq=%d done=%v", got.Seq, got.Done)
	if !got.Done || got.Seq != 6 || got.Cursor != "" {
		t.Fatalf("done checkpoint not preserved: %+v", got)
	}

	// 完成态是终态：同进程内任何改动（重抓、覆盖、回退）都被拒绝。
	for _, cp := range []*Checkpoint{
		{Cursor: EncodeCursor(2, 2), Seq: 2}, // 回退到中途进度
		{Cursor: "", Seq: 6},                 // 丢掉完成标记
		{Cursor: "", Seq: 12, Done: true},    // 完成态上再叠加进度
	} {
		if err := s.Save(context.Background(), cp); !IsKind(err, KindCheckpointCorrupt) {
			t.Fatalf("overwriting completed checkpoint must be rejected, got %v", err)
		}
	}
	// 读回同一个完成检查点（重启后新实例的场景）是合法的。
	s2, _ := NewFileStore(path, "orders")
	got2, err := s2.Load(context.Background())
	if err != nil || !got2.Done || got2.Seq != 6 {
		t.Fatalf("fresh instance must read completed checkpoint as-is, got %+v err=%v", got2, err)
	}
}

func TestFileStoreRejectsInconsistentDone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	s, _ := NewFileStore(path, "orders")

	// 完成态却携带下一页游标：自相矛盾，保存即拒绝。
	bad := &Checkpoint{Cursor: EncodeCursor(2, 2), Seq: 2, Done: true}
	if err := s.Save(context.Background(), bad); !IsKind(err, KindCheckpointCorrupt) {
		t.Fatalf("done checkpoint with next cursor must be rejected, got %v", err)
	}

	// 磁盘上被篡改成“完成态+游标”：读回同样拒绝。
	writeEnvelopeFile(t, path, "orders", EncodeCursor(2, 2), 2, 1, func(env *checkpointEnvelope) {
		env.Done = true
		sum, err := envelopeChecksum(env)
		if err != nil {
			t.Fatal(err)
		}
		env.Checksum = sum
	})
	s2, _ := NewFileStore(path, "orders")
	_, err := s2.Load(context.Background())
	t.Logf("inconsistent done checkpoint -> %v (kind=%s)", err, KindOf(err))
	if !IsKind(err, KindCheckpointCorrupt) {
		t.Fatalf("tampered done checkpoint must be rejected, got %v", err)
	}
}
