package syncsdk

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestEngineBackpressureInFlight 慢提交导致在途配额占满时，抓取必须背压停下，
// 不能无限把页抓进内存；放开提交后能正常跑完且不丢不重。
func TestEngineBackpressureInFlight(t *testing.T) {
	disp := newDispatchingTransport()
	disp.add("s")
	const pageSize, total = 2, 10
	first := disp.setupStream("s", streamBase(0), pageSize, total)

	c := NewClient(disp, Config{MaxConcurrency: 8, RatePerSecond: 1000})
	store := newMultiMemStore()

	gate := make(chan struct{})
	var doneFirst sync.WaitGroup
	doneFirst.Add(1)
	commits := 0
	var commitMu sync.Mutex
	commit := func(ctx context.Context, id StreamID, items []Item) error {
		commitMu.Lock()
		commits++
		n := commits
		commitMu.Unlock()
		if n == 1 {
			doneFirst.Done() // 第一批已进入提交
			<-gate           // 阻塞提交，模拟下游慢：在途页配额很快被占满
		}
		return nil
	}

	r := &streamRunner{client: c, store: store, spec: StreamSpec{
		ID: "s", StartCursor: first, MaxInFlight: 1, MaxBuffered: 0, Commit: commit,
	}}
	done := make(chan struct{})
	var res *Result
	var err error
	go func() {
		res, err = r.run(context.Background())
		close(done)
	}()

	doneFirst.Wait()
	// 提交被卡住：MaxInFlight=1，抓取最多比确认领先 1 页。
	time.Sleep(40 * time.Millisecond)
	fetchedWhileBlocked := disp.totalFetches()
	t.Logf("while commit blocked: fetched pages=%d (inflight cap=1)", fetchedWhileBlocked)
	if fetchedWhileBlocked > 2 { // 第1批确认的1页 + 最多1页在途
		t.Fatalf("backpressure failed: %d pages fetched while commit blocked", fetchedWhileBlocked)
	}

	close(gate) // 放开下游
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not finish after releasing backpressure")
	}
	if err != nil || !res.Completed {
		t.Fatalf("run failed after release: %v completed=%v", err, res.Completed)
	}
	if res.Checkpoint.Seq != total {
		t.Fatalf("seq=%d want %d", res.Checkpoint.Seq, total)
	}
}

// TestEngineBackpressureBufferedItems MaxBuffered 限制本地缓冲条目数，
// 缓冲满时抓取同样背压，不无限堆积。
func TestEngineBackpressureBufferedItems(t *testing.T) {
	disp := newDispatchingTransport()
	disp.add("s")
	const pageSize, total = 2, 20 // 10 页
	first := disp.setupStream("s", streamBase(0), pageSize, total)

	c := NewClient(disp, Config{MaxConcurrency: 8, RatePerSecond: 1000})
	store := newMultiMemStore()
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	commit := func(ctx context.Context, id StreamID, items []Item) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-gate // 一直拖住提交，缓冲会被占满
		return nil
	}
	r := &streamRunner{client: c, store: store, spec: StreamSpec{
		ID: "s", StartCursor: first, MaxInFlight: 8, MaxBuffered: 4, Commit: commit,
	}}
	runDone := make(chan struct{})
	go func() { _, _ = r.run(context.Background()); close(runDone) }()

	<-started
	time.Sleep(40 * time.Millisecond)
	fetched := disp.totalFetches()
	t.Logf("with MaxBuffered=4 and stalled commit, fetched pages=%d", fetched)
	// 4 个条目缓冲 + 在途限制：最多缓冲 4 条（2 页），不应把 10 页全部抓回。
	if fetched > 3 {
		t.Fatalf("item buffer backpressure failed: %d pages buffered", fetched)
	}
	close(gate)
	<-runDone
}

// TestEngineCancelStopsFetchingAndReturnsPartial 取消后不再发起新抓取，
// 尽快收尾并返回已确认的部分进度，结束原因为 canceled。
func TestEngineCancelStopsFetchingAndReturnsPartial(t *testing.T) {
	disp := newDispatchingTransport()
	disp.delay = 8 * time.Millisecond
	disp.add("s")
	const pageSize, total = 2, 20
	first := disp.setupStream("s", streamBase(0), pageSize, total)
	sink := newMultiCommitSink()
	store := newMultiMemStore()

	c := NewClient(disp, Config{MaxConcurrency: 4, RatePerSecond: 1000})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	r := &streamRunner{client: c, store: store, spec: StreamSpec{
		ID: "s", StartCursor: first, MaxInFlight: 1, Commit: sink.commit,
	}}
	res, err := r.run(ctx)
	t.Logf("canceled: seq=%d stop=%s err=%v(%s) items=%d",
		res.Checkpoint.Seq, res.Stop, err, KindOf(err), len(res.Items))
	if res.Stop != StopCanceled || !IsKind(err, KindCanceled) {
		t.Fatalf("want stop=canceled / kind=canceled, got stop=%s err=%v", res.Stop, err)
	}
	callsAtStop := disp.totalFetches()
	// 已确认进度落盘，且与提交一致。
	persisted, _ := store.LoadStream(context.Background(), "s")
	if persisted == nil || persisted.Seq != uint64(len(sink.ids("s"))) {
		t.Fatalf("partial progress not persisted consistently: %+v committed=%d",
			persisted, len(sink.ids("s")))
	}
	// 取消后不再发起新抓取。
	time.Sleep(30 * time.Millisecond)
	if calls := disp.totalFetches(); calls > callsAtStop {
		t.Fatalf("new fetches issued after cancel: %d > %d", calls, callsAtStop)
	}

	// 恢复后跑完，最终不丢不重。
	disp2 := newDispatchingTransport()
	disp2.add("s")
	disp2.setupStream("s", streamBase(0), pageSize, total)
	c2 := NewClient(disp2, Config{RatePerSecond: 1000})
	fs2 := store // 同一内存存储模拟重启
	r2 := &streamRunner{client: c2, store: fs2, spec: StreamSpec{
		ID: "s", StartCursor: first, MaxInFlight: 1, Commit: sink.commit,
	}}
	res2, err2 := r2.run(context.Background())
	if err2 != nil || !res2.Completed {
		t.Fatalf("resume run failed: %v completed=%v", err2, res2.Completed)
	}
	if res2.Checkpoint.Seq != total {
		t.Fatalf("after resume seq=%d want %d", res2.Checkpoint.Seq, total)
	}
	if len(sink.ids("s")) != total {
		t.Fatalf("after resume unique items=%d want %d (loss or duplication)", len(sink.ids("s")), total)
	}
}

// TestEngineTimeoutReason 超时（context deadline）同样归类为 canceled。
func TestEngineTimeoutReason(t *testing.T) {
	disp := newDispatchingTransport()
	disp.delay = 50 * time.Millisecond
	disp.add("s")
	first := disp.setupStream("s", streamBase(0), 2, 8)
	c := NewClient(disp, Config{RatePerSecond: 1000})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	r := &streamRunner{client: c, store: newMultiMemStore(), spec: StreamSpec{
		ID: "s", StartCursor: first, MaxInFlight: 1,
	}}
	res, err := r.run(ctx)
	if res.Stop != StopCanceled || !IsKind(err, KindCanceled) {
		t.Fatalf("timeout want canceled, got stop=%s err=%v", res.Stop, err)
	}
}
