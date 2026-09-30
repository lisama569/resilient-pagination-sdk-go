package syncsdk

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// TestManagerMultipleIndependentStreams 验证多条流并行、进度互相隔离、
// 全局并发不突破共享 Client 上限。
func TestManagerMultipleIndependentStreams(t *testing.T) {
	disp := newDispatchingTransport()
	disp.delay = 5 * time.Millisecond
	// (名字, 每页条数, 总条数)
	defs := []struct {
		name     string
		pageSize int
		total    int
	}{
		{"orders", 2, 7},
		{"users", 3, 5},
		{"events", 2, 6},
	}
	firsts := map[string]string{}
	for i, df := range defs {
		disp.add(df.name)
		firsts[df.name] = disp.setupStream(df.name, streamBase(i), df.pageSize, df.total)
	}

	// 全局并发上限 2，所有流共享。
	c := NewClient(disp, Config{MaxConcurrency: 2, RatePerSecond: 1000, MaxRetries: 1, InitialBackoff: time.Millisecond})
	store := newMultiMemStore()
	sink := newMultiCommitSink()

	specs := make([]StreamSpec, 0, len(defs))
	for _, df := range defs {
		specs = append(specs, StreamSpec{
			ID:          StreamID(df.name),
			StartCursor: firsts[df.name],
			MaxInFlight: 2,
			Commit:      sink.commit,
		})
	}

	out := NewManager(c, store).RunAll(context.Background(), specs)
	if len(out) != len(defs) {
		t.Fatalf("want %d outcomes, got %d", len(defs), len(out))
	}
	for _, df := range defs {
		o := out[StreamID(df.name)]
		if o == nil {
			t.Fatalf("missing outcome for %s", df.name)
		}
		if o.Err != nil {
			t.Fatalf("stream %s failed: %v (kind=%s)", df.name, o.Err, KindOf(o.Err))
		}
		if !o.Result.Completed || o.Result.Stop != StopCompleted {
			t.Fatalf("stream %s not completed: %+v", df.name, o.Result)
		}
		if o.Result.Checkpoint.Seq != uint64(df.total) {
			t.Fatalf("stream %s seq=%d want %d", df.name, o.Result.Checkpoint.Seq, df.total)
		}
		if len(sink.ids(StreamID(df.name))) != df.total {
			t.Fatalf("stream %s committed=%d want %d", df.name, len(sink.ids(StreamID(df.name))), df.total)
		}
	}

	// 进度互相隔离：每条流只提交自己的 ID，不串流。
	for _, df := range defs {
		prefix := df.name + "-id-"
		for _, id := range sink.ids(StreamID(df.name)) {
			if !hasPrefix(id, prefix) {
				t.Fatalf("stream %s got foreign item %s (progress leaked across streams)", df.name, id)
			}
		}
	}
	t.Logf("max global concurrency=%d (limit=2)", disp.maxConc)
	if disp.maxConc > 2 {
		t.Fatalf("global concurrency exceeded: %d", disp.maxConc)
	}
}

// TestManagerOneStreamFailureDoesNotAffectOthers 验证限流耗尽只让该流失败，
// 其他流正常完成且已确认进度不受影响；错误能区分流与类别。
func TestManagerOneStreamFailureDoesNotAffectOthers(t *testing.T) {
	disp := newDispatchingTransport()
	disp.add("good")
	disp.add("bad")
	goodFirst := disp.setupStream("good", streamBase(0), 2, 4)
	badFirst := disp.setupStream("bad", streamBase(1), 2, 4)
	// bad 流第二页（pageIndex=1）持续 429，重试耗尽。
	disp.injectFailureAt("bad", streamBase(1), 1, 2,
		&RetryableError{Reason: "429"}, &RetryableError{Reason: "429"}, &RetryableError{Reason: "429"})

	c := NewClient(disp, Config{MaxConcurrency: 4, RatePerSecond: 1000, MaxRetries: 2,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond})
	store := newMultiMemStore()
	sink := newMultiCommitSink()
	specs := []StreamSpec{
		{ID: "good", StartCursor: goodFirst, Commit: sink.commit},
		{ID: "bad", StartCursor: badFirst, Commit: sink.commit},
	}

	out := NewManager(c, store).RunAll(context.Background(), specs)
	good := out["good"]
	bad := out["bad"]
	if good.Err != nil || !good.Result.Completed {
		t.Fatalf("good stream should complete unaffected, got %v", good.Err)
	}
	if !IsKind(bad.Err, KindRetryExhausted) {
		t.Fatalf("bad stream want retry_exhausted, got %v", bad.Err)
	}
	if se := asSDKError(bad.Err); se == nil || se.Stream != "bad" {
		t.Fatalf("error must identify stream bad, got %+v", se)
	}
	if bad.Result.Stop != StopFailed {
		t.Fatalf("bad stream stop=%s want failed", bad.Result.Stop)
	}
	// bad 流首页 2 条已确认落盘，未受失败影响而回滚。
	if bad.Result.Checkpoint.Seq != 2 {
		t.Fatalf("bad stream confirmed seq=%d want 2", bad.Result.Checkpoint.Seq)
	}
	persisted, _ := store.LoadStream(context.Background(), "bad")
	if persisted == nil || persisted.Seq != 2 {
		t.Fatalf("bad stream persisted progress lost: %+v", persisted)
	}
	t.Logf("good=%v bad.seq=%d", good.Result.Checkpoint, persisted.Seq)
}

// TestManagerCommitFailureIsolatesStream 验证某条流提交失败不影响另一条流。
func TestManagerCommitFailureIsolatesStream(t *testing.T) {
	disp := newDispatchingTransport()
	disp.add("a")
	disp.add("b")
	aFirst := disp.setupStream("a", streamBase(0), 2, 6)
	bFirst := disp.setupStream("b", streamBase(1), 2, 6)

	c := NewClient(disp, Config{RatePerSecond: 1000})
	store := newMultiMemStore()
	sink := newMultiCommitSink()
	sink.failAfter["a"] = 2 // a 流第 2 批提交失败

	out := NewManager(c, store).RunAll(context.Background(), []StreamSpec{
		{ID: "a", StartCursor: aFirst, Commit: sink.commit},
		{ID: "b", StartCursor: bFirst, Commit: sink.commit},
	})
	if out["b"].Err != nil || !out["b"].Result.Completed {
		t.Fatalf("stream b should complete, got %v", out["b"].Err)
	}
	if !IsKind(out["a"].Err, KindTransport) {
		t.Fatalf("stream a want transport, got %v", out["a"].Err)
	}
	if out["a"].Result.Stop != StopFailed || out["b"].Result.Stop != StopCompleted {
		t.Fatalf("stop reasons wrong: a=%s b=%s", out["a"].Result.Stop, out["b"].Result.Stop)
	}
	// a 流第 1 批 2 条已确认并落盘。
	if out["a"].Result.Checkpoint.Seq != 2 {
		t.Fatalf("stream a confirmed seq=%d want 2", out["a"].Result.Checkpoint.Seq)
	}
}

func hasPrefix(s, p string) bool {
	return len(s) >= len(p) && s[:len(p)] == p
}

func asSDKError(err error) *Error {
	if se, ok := err.(*Error); ok {
		return se
	}
	return nil
}

// TestManagerSharedRateLimit 多条流并行共享一个低速率 Client，
// 聚合请求速率仍不得突破上限。
func TestManagerSharedRateLimit(t *testing.T) {
	disp := newDispatchingTransport()
	const streams, pageSize, total = 3, 2, 10 // 每流 5 页，共 15 次请求
	specs := make([]StreamSpec, 0, streams)
	for i := 0; i < streams; i++ {
		name := streamName(i)
		disp.add(name)
		first := disp.setupStream(name, streamBase(i), pageSize, total)
		specs = append(specs, StreamSpec{
			ID: StreamID(name), StartCursor: first, MaxInFlight: 4,
			Commit: newMultiCommitSink().commit, // 每流各自独立 sink
		})
	}
	// 50/s => 至少 20ms 间隔；15 个请求理论下限约 14*20ms=280ms。
	c := NewClient(disp, Config{MaxConcurrency: 8, RatePerSecond: 50, MaxRetries: 0})
	start := time.Now()
	out := NewManager(c, newMultiMemStore()).RunAll(context.Background(), specs)
	elapsed := time.Since(start)
	for i := 0; i < streams; i++ {
		if o := out[StreamID(streamName(i))]; o.Err != nil || !o.Result.Completed {
			t.Fatalf("stream %d failed: %v", i, o.Err)
		}
	}
	if got := disp.totalFetches(); got != streams*total/pageSize {
		t.Fatalf("fetches=%d want %d", got, streams*total/pageSize)
	}
	t.Logf("%d streams, %d aggregated requests in %v (rate=50/s)", streams, disp.totalFetches(), elapsed)
	// 允许一定调度误差，取理论下限的 80%。
	if elapsed < 280*time.Millisecond*8/10 {
		t.Fatalf("aggregate rate limit not enforced: %d requests in %v", disp.totalFetches(), elapsed)
	}
}

func streamName(i int) string { return "st" + strconv.Itoa(i) }
