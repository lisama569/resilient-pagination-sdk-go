package syncsdk

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// commitLog 记录已提交的条目，模拟幂等 sink（按 ID 去重）。
type commitLog struct {
	mu    sync.Mutex
	ids   []string
	seen  map[string]bool
	batch int
}

func newCommitLog() *commitLog { return &commitLog{seen: map[string]bool{}} }

func (c *commitLog) commit(ctx context.Context, items []Item) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batch++
	for _, it := range items {
		if !c.seen[it.ID] {
			c.seen[it.ID] = true
			c.ids = append(c.ids, it.ID)
		}
	}
	return nil
}

func setupPages(ft *fakeTransport, pageSize, total int) string {
	// 构造 total 条记录、每页 pageSize 条的游标链，返回首页游标（""）。
	cursor := ""
	cursors := []string{""}
	for off := pageSize; off < total; off += pageSize {
		cursors = append(cursors, EncodeCursor(off, 2))
	}
	for i := len(cursors) - 1; i >= 0; i-- {
		next := ""
		if i+1 < len(cursors) {
			next = cursors[i+1]
		}
		ids := []string{}
		for j := i * pageSize; j < (i+1)*pageSize && j < total; j++ {
			ids = append(ids, fmt.Sprintf("id-%03d", j))
		}
		ft.addPage(cursors[i], 2, next, ids...)
	}
	return cursor
}

func TestSyncerFullRun(t *testing.T) {
	ft := newFakeTransport()
	setupPages(ft, 3, 10) // 4 页：3+3+3+1
	store := &memStore{}
	sink := newCommitLog()

	c := NewClient(ft, Config{RatePerSecond: 1000})
	s := NewSyncer(c, store, sink.commit)
	res, err := s.Run(context.Background())
	t.Logf("items=%d completed=%v checkpoint=%+v err=%v", len(res.Items), res.Completed, res.Checkpoint, err)
	if err != nil || !res.Completed {
		t.Fatalf("run failed: %v", err)
	}
	if len(sink.ids) != 10 {
		t.Fatalf("want 10 committed items, got %d", len(sink.ids))
	}
	if res.Checkpoint.Seq != 10 {
		t.Fatalf("checkpoint seq=%d, want 10", res.Checkpoint.Seq)
	}
}

func TestSyncerResumeAfterRestart(t *testing.T) {
	ft := newFakeTransport()
	setupPages(ft, 2, 6) // 3 页
	store := &memStore{}
	sink := newCommitLog()
	cfg := Config{RatePerSecond: 1000, MaxRetries: 1, InitialBackoff: time.Millisecond}

	// 第一次运行：第二页永久失败，同步中断但首页进度已确认。
	ft.injectFailure(EncodeCursor(2, 2), fmt.Errorf("disk on fire"), fmt.Errorf("disk on fire"))
	s1 := NewSyncer(NewClient(ft, cfg), store, sink.commit)
	res1, err1 := s1.Run(context.Background())
	t.Logf("run1: items=%d err=%v (kind=%s) checkpoint=%+v", len(res1.Items), err1, KindOf(err1), res1.Checkpoint)
	if err1 == nil {
		t.Fatal("run1 should fail")
	}
	committedAfterRun1 := len(sink.ids)

	// 模拟重启：新客户端、新 Syncer，同一 store 与 sink。
	ft2 := newFakeTransport()
	setupPages(ft2, 2, 6)
	s2 := NewSyncer(NewClient(ft2, cfg), store, sink.commit)
	res2, err2 := s2.Run(context.Background())
	t.Logf("run2: items=%d completed=%v err=%v", len(res2.Items), res2.Completed, err2)
	if err2 != nil {
		t.Fatalf("run2 failed: %v", err2)
	}
	// 无遗漏无重复：总共恰好 6 条唯一记录。
	if len(sink.ids) != 6 {
		t.Fatalf("want 6 unique committed items, got %d (%v)", len(sink.ids), sink.ids)
	}
	if committedAfterRun1 == 0 {
		t.Fatal("run1 should have committed first page before failing")
	}
	t.Logf("unique committed ids=%v", sink.ids)
}

func TestSyncerCursorRegressionRejected(t *testing.T) {
	last := &Checkpoint{Cursor: EncodeCursor(6, 2), Seq: 6}
	next := &Checkpoint{Cursor: EncodeCursor(2, 2), Seq: 6}
	err := ValidateResume(last, next)
	t.Logf("input last=%+v next=%+v normalized error: %v (kind=%s)", last, next, err, KindOf(err))
	if !IsKind(err, KindCursorInvalid) {
		t.Fatalf("cursor regression must be rejected, got %v", err)
	}
}

func TestSyncerCursorInvalidRejected(t *testing.T) {
	cases := []string{
		"garbage",
		"v2.10.deadbeef",                 // 校验和错误
		"v2.-1.00000000",                 // 负偏移
		EncodeCursor(4, 2)[:5] + "xxxxx", // 截断
	}
	for _, cur := range cases {
		_, _, err := DecodeCursor(cur)
		t.Logf("input cursor=%q normalized error: %v (kind=%s)", cur, err, KindOf(err))
		if !IsKind(err, KindCursorInvalid) {
			t.Fatalf("cursor %q must be rejected as cursor_invalid, got %v", cur, err)
		}
	}
	// 持久化了失效游标时，恢复必须显式失败而不是从中间继续。
	store := &memStore{cp: &Checkpoint{Cursor: "garbage", Seq: 3}}
	ft := newFakeTransport()
	setupPages(ft, 2, 6)
	s := NewSyncer(NewClient(ft, Config{RatePerSecond: 1000}), store, nil)
	_, err := s.Run(context.Background())
	t.Logf("resume with corrupted checkpoint -> %v (kind=%s)", err, KindOf(err))
	if !IsKind(err, KindCursorInvalid) {
		t.Fatalf("corrupted checkpoint must be rejected, got %v", err)
	}
}

func TestSyncerCancelPreservesProgress(t *testing.T) {
	ft := newFakeTransport()
	setupPages(ft, 2, 10) // 5 页
	ft.delay = 10 * time.Millisecond
	store := &memStore{}
	sink := newCommitLog()

	c := NewClient(ft, Config{RatePerSecond: 1000})
	s := NewSyncer(c, store, sink.commit)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	res, err := s.Run(ctx)
	t.Logf("canceled run: items=%d completed=%v err=%v (kind=%s) checkpoint=%+v",
		len(res.Items), res.Completed, err, KindOf(err), res.Checkpoint)
	if !IsKind(err, KindCanceled) {
		t.Fatalf("want canceled, got %v", err)
	}
	// 已确认进度被保留：checkpoint 与已提交数据一致。
	cp, _ := store.Load(context.Background())
	if cp.Seq != uint64(len(sink.ids)) {
		t.Fatalf("checkpoint seq=%d but committed=%d: inconsistent", cp.Seq, len(sink.ids))
	}

	// 恢复后继续，最终无遗漏无重复。
	ft2 := newFakeTransport()
	setupPages(ft2, 2, 10)
	s2 := NewSyncer(NewClient(ft2, Config{RatePerSecond: 1000}), store, sink.commit)
	res2, err2 := s2.Run(context.Background())
	if err2 != nil || !res2.Completed {
		t.Fatalf("resume run failed: %v", err2)
	}
	if len(sink.ids) != 10 {
		t.Fatalf("want 10 unique items after resume, got %d", len(sink.ids))
	}
	t.Logf("after resume: unique ids=%d completed=%v", len(sink.ids), res2.Completed)
}

func TestSyncerConcurrentSharedClient(t *testing.T) {
	// 多个协程共享同一客户端，各自独立同步，互不干扰且不超限。
	ft := newFakeTransport()
	setupPages(ft, 2, 6)
	ft.delay = 5 * time.Millisecond
	c := NewClient(ft, Config{MaxConcurrency: 3, RatePerSecond: 200, MaxRetries: 1, InitialBackoff: time.Millisecond})

	const workers = 4
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			store := &memStore{}
			sink := newCommitLog()
			s := NewSyncer(c, store, sink.commit)
			res, err := s.Run(context.Background())
			if err != nil {
				errs <- fmt.Errorf("worker %d: %w", w, err)
				return
			}
			if len(sink.ids) != 6 || !res.Completed {
				errs <- fmt.Errorf("worker %d: got %d items completed=%v", w, len(sink.ids), res.Completed)
				return
			}
			t.Logf("worker %d done: items=%d retries_observed=%d", w, len(sink.ids), ft.totalCalls())
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	t.Logf("max observed concurrency=%d (limit=3)", ft.maxConc)
	if ft.maxConc > 3 {
		t.Fatalf("concurrency exceeded: %d", ft.maxConc)
	}
}
