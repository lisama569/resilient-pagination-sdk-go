package syncsdk

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// streamServer 是多数据流的本地服务替身：按 (stream, cursor) 提供页面，
// 条目 ID 带流前缀，并统计跨流的整体并发与调用情况。
type streamServer struct {
	mu       sync.Mutex
	pages    map[string]string // key: streamID + "\x00" + cursor
	failures map[string][]error
	calls    map[string]int
	curConc  int
	maxConc  int
	delay    time.Duration
	notify   chan struct{} // 每次实际发送一页时通知（可选）
}

func newStreamServer() *streamServer {
	return &streamServer{
		pages:    map[string]string{},
		failures: map[string][]error{},
		calls:    map[string]int{},
	}
}

func (s *streamServer) key(stream StreamID, cursor string) string {
	return string(stream) + "\x00" + cursor
}

// setupStream 构造一条流：total 条、每页 pageSize 条，ID 形如 <stream>-id-000。
func (s *streamServer) setupStream(stream StreamID, pageSize, total int) {
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
			ids = append(ids, fmt.Sprintf("%s-id-%03d", stream, j))
		}
		items := make([]string, len(ids))
		for k, id := range ids {
			items[k] = fmt.Sprintf(`{"id":%q}`, id)
		}
		s.pages[s.key(stream, cursors[i])] = fmt.Sprintf(
			`{"version":2,"items":[%s],"next_cursor":%q}`, strings.Join(items, ","), next)
	}
}

func (s *streamServer) injectFailure(stream StreamID, cursor string, errs ...error) {
	k := s.key(stream, cursor)
	s.failures[k] = append(s.failures[k], errs...)
}

func (s *streamServer) Fetch(ctx context.Context, stream StreamID, cursor string) ([]byte, error) {
	k := s.key(stream, cursor)
	s.mu.Lock()
	s.curConc++
	if s.curConc > s.maxConc {
		s.maxConc = s.curConc
	}
	s.calls[k]++
	n := s.calls[k]
	var fail error
	if fs := s.failures[k]; len(fs) >= n {
		fail = fs[n-1]
	}
	s.mu.Unlock()

	if s.delay > 0 {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.curConc--
			s.mu.Unlock()
			return nil, ctx.Err()
		case <-time.After(s.delay):
		}
	}

	s.mu.Lock()
	s.curConc--
	body, ok := s.pages[k]
	if s.notify != nil {
		s.notify <- struct{}{}
	}
	s.mu.Unlock()
	if fail != nil {
		return nil, fail
	}
	if !ok {
		return nil, fmt.Errorf("stream %q: no page for cursor %q", stream, cursor)
	}
	return []byte(body), nil
}

func (s *streamServer) callsFor(stream StreamID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k, c := range s.calls {
		if strings.HasPrefix(k, string(stream)+"\x00") {
			n += c
		}
	}
	return n
}

func mustRunner(t *testing.T, c *Client, streams []StreamConfig) *Runner {
	t.Helper()
	r, err := NewRunner(c, streams)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	return r
}

// unusedSharedTransport 是“所有流都自带 StreamTransport”场景下的共享占位，
// 正常路径绝不应被调用。
type unusedSharedTransport struct{}

func (unusedSharedTransport) Fetch(ctx context.Context, cursor string) ([]byte, error) {
	return nil, fmt.Errorf("shared transport unexpectedly used for cursor %q", cursor)
}

// withStreamTransport 给每个配置注入同一个流专属服务。
func withStreamTransport(cfgs []StreamConfig, st StreamTransport) []StreamConfig {
	out := make([]StreamConfig, len(cfgs))
	copy(out, cfgs)
	for i := range out {
		out[i].Transport = st
	}
	return out
}

func TestRunnerConfigValidation(t *testing.T) {
	store := &memStore{}
	good := StreamConfig{ID: "a", Store: store}
	if _, err := NewRunner(nil, []StreamConfig{good}); err == nil {
		t.Fatal("nil client must be rejected")
	}
	c := NewClient(newFakeTransport(), Config{RatePerSecond: 1000})
	if _, err := NewRunner(c, nil); err == nil {
		t.Fatal("empty streams must be rejected")
	}
	if _, err := NewRunner(c, []StreamConfig{{ID: "", Store: store}}); err == nil {
		t.Fatal("empty stream id must be rejected")
	}
	if _, err := NewRunner(c, []StreamConfig{good, good}); err == nil {
		t.Fatal("duplicate stream ids must be rejected")
	}
	if _, err := NewRunner(c, []StreamConfig{{ID: "b", Store: nil}}); err == nil {
		t.Fatal("nil store must be rejected")
	}
}

func TestRunnerMultiStreamIsolation(t *testing.T) {
	srv := newStreamServer()
	srv.setupStream("orders", 3, 10) // 4 页
	srv.setupStream("billing", 2, 6) // 3 页
	srv.setupStream("users", 5, 5)   // 1 页

	sinks := map[StreamID]*commitLog{}
	var cfgs []StreamConfig
	for _, id := range []StreamID{"orders", "billing", "users"} {
		sink := newCommitLog()
		sinks[id] = sink
		cfgs = append(cfgs, StreamConfig{ID: id, Store: &memStore{}, Commit: sink.commit})
	}
	c := NewClient(unusedSharedTransport{}, Config{MaxConcurrency: 4, RatePerSecond: 1000})
	r := mustRunner(t, c, withStreamTransport(cfgs, srv))

	results := r.Run(context.Background())
	want := map[StreamID]int{"orders": 10, "billing": 6, "users": 5}
	for _, res := range results {
		t.Logf("stream=%s reason=%s items=%d seq=%d err=%v",
			res.Stream, res.Reason, len(res.Items), res.Checkpoint.Seq, res.Err)
		if res.Reason != StopCompleted || res.Err != nil {
			t.Fatalf("stream %s not completed: reason=%s err=%v", res.Stream, res.Reason, res.Err)
		}
		if len(sinks[res.Stream].ids) != want[res.Stream] {
			t.Fatalf("stream %s committed=%d want %d", res.Stream, len(sinks[res.Stream].ids), want[res.Stream])
		}
		if res.Checkpoint.Seq != uint64(want[res.Stream]) {
			t.Fatalf("stream %s seq=%d want %d", res.Stream, res.Checkpoint.Seq, want[res.Stream])
		}
		// 进度互不串：每条流只提交带自己前缀的 ID。
		for _, id := range sinks[res.Stream].ids {
			if !strings.HasPrefix(id, string(res.Stream)+"-") {
				t.Fatalf("stream %s received foreign item %s", res.Stream, id)
			}
		}
	}
}

func TestRunnerGlobalConcurrencyAndRate(t *testing.T) {
	srv := newStreamServer()
	srv.delay = 20 * time.Millisecond
	for _, id := range []StreamID{"a", "b", "c"} {
		srv.setupStream(id, 2, 6) // 每流 3 页
	}
	var cfgs []StreamConfig
	for _, id := range []StreamID{"a", "b", "c"} {
		cfgs = append(cfgs, StreamConfig{ID: id, Store: &memStore{}})
	}
	// 全局并发上限 2、速率 50/s（20ms 间隔）。
	c := NewClient(unusedSharedTransport{}, Config{MaxConcurrency: 2, RatePerSecond: 50, InitialBackoff: time.Millisecond})
	r := mustRunner(t, c, withStreamTransport(cfgs, srv))

	start := time.Now()
	results := r.Run(context.Background())
	elapsed := time.Since(start)
	for _, res := range results {
		if res.Reason != StopCompleted {
			t.Fatalf("stream %s: %v", res.Stream, res.Err)
		}
	}
	t.Logf("9 requests across 3 streams: maxConc=%d elapsed=%v", srv.maxConc, elapsed)
	if srv.maxConc > 2 {
		t.Fatalf("global concurrency exceeded: %d > 2", srv.maxConc)
	}
	// 9 个请求至少需要 8 个速率间隔 ≈ 160ms（留 10% 余量）。
	if elapsed < 8*20*time.Millisecond*9/10 {
		t.Fatalf("global rate limit not enforced: 9 requests in %v", elapsed)
	}
}

func TestRunnerFailureIsolatedPerStream(t *testing.T) {
	srv := newStreamServer()
	srv.setupStream("good", 2, 6)
	srv.setupStream("bad", 2, 6)
	// bad 流第二页永久失败（超过重试次数后耗尽）。
	srv.injectFailure("bad", EncodeCursor(2, 2),
		fmt.Errorf("disk on fire"), fmt.Errorf("disk on fire"), fmt.Errorf("disk on fire"))

	goodSink := newCommitLog()
	badSink := newCommitLog()
	cfgs := []StreamConfig{
		{ID: "good", Store: &memStore{}, Commit: goodSink.commit},
		{ID: "bad", Store: &memStore{rejectRegress: true}, Commit: badSink.commit},
	}
	cfg := Config{MaxConcurrency: 4, RatePerSecond: 1000, MaxRetries: 2,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}
	c := NewClient(unusedSharedTransport{}, cfg)
	r := mustRunner(t, c, withStreamTransport(cfgs, srv))

	results := r.Run(context.Background())
	byID := map[StreamID]StreamResult{}
	for _, res := range results {
		byID[res.Stream] = res
		t.Logf("stream=%s reason=%s kind=%s items=%d",
			res.Stream, res.Reason, KindOf(res.Err), len(res.Items))
	}

	// bad 流失败，但要能分清是哪条流、哪类问题。
	bad := byID["bad"]
	if bad.Reason != StopFailed {
		t.Fatalf("bad stream reason=%s want failed", bad.Reason)
	}
	if !IsKind(bad.Err, KindTransport) {
		t.Fatalf("bad stream want transport, got %v", bad.Err)
	}
	var se *Error
	if !asError(bad.Err, &se) || se.Stream != "bad" {
		t.Fatalf("error must be attributed to stream bad, got %+v", bad.Err)
	}
	// good 流不受影响，完整完成且进度互不串。
	good := byID["good"]
	if good.Reason != StopCompleted {
		t.Fatalf("good stream must complete despite bad stream: %v", good.Err)
	}
	if len(goodSink.ids) != 6 {
		t.Fatalf("good stream want 6 items, got %d", len(goodSink.ids))
	}
	// bad 流第一页已确认的进度保留在它自己的检查点里。
	if len(badSink.ids) != 2 || bad.Checkpoint == nil || bad.Checkpoint.Seq != 2 {
		t.Fatalf("bad stream confirmed progress lost: committed=%d cp=%+v", len(badSink.ids), bad.Checkpoint)
	}
}

func TestRunnerRetryExhaustedKind(t *testing.T) {
	srv := newStreamServer()
	srv.setupStream("r", 2, 6)
	srv.injectFailure("r", "", &RetryableError{Reason: "429"}, &RetryableError{Reason: "429"}, &RetryableError{Reason: "429"})
	cfgs := []StreamConfig{{ID: "r", Store: &memStore{}}}
	c := NewClient(unusedSharedTransport{}, Config{RatePerSecond: 1000, MaxRetries: 2,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond})
	r := mustRunner(t, c, withStreamTransport(cfgs, srv))
	res := r.Run(context.Background())[0]
	t.Logf("retry stream reason=%s kind=%s err=%v", res.Reason, KindOf(res.Err), res.Err)
	if res.Reason != StopFailed || !IsKind(res.Err, KindRetryExhausted) {
		t.Fatalf("want failed/retry_exhausted, got reason=%s err=%v", res.Reason, res.Err)
	}
}

func TestRunnerCancelStopsAllStreams(t *testing.T) {
	srv := newStreamServer()
	srv.delay = 15 * time.Millisecond
	srv.setupStream("a", 2, 40)
	srv.setupStream("b", 2, 40)
	sinks := map[StreamID]*commitLog{}
	cfgs := []StreamConfig{}
	for _, id := range []StreamID{"a", "b"} {
		sink := newCommitLog()
		sinks[id] = sink
		cfgs = append(cfgs, StreamConfig{ID: id, Store: &memStore{}, Commit: sink.commit})
	}
	c := NewClient(unusedSharedTransport{}, Config{MaxConcurrency: 4, RatePerSecond: 1000})
	r := mustRunner(t, c, withStreamTransport(cfgs, srv))

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	results := r.Run(ctx)
	for _, res := range results {
		t.Logf("stream=%s reason=%s kind=%s committed=%d cpSeq=%d",
			res.Stream, res.Reason, KindOf(res.Err), len(sinks[res.Stream].ids), res.Checkpoint.Seq)
		if res.Reason != StopCanceled {
			t.Fatalf("stream %s reason=%s want canceled", res.Stream, res.Reason)
		}
		if !IsKind(res.Err, KindCanceled) {
			t.Fatalf("stream %s want canceled kind, got %v", res.Stream, res.Err)
		}
		// 已确认进度：返回的检查点与提交数一致。
		if res.Checkpoint.Seq != uint64(len(sinks[res.Stream].ids)) {
			t.Fatalf("stream %s cp seq %d != committed %d", res.Stream, res.Checkpoint.Seq, len(sinks[res.Stream].ids))
		}
	}
	// 取消后不得再发起新抓取：稍等确认服务侧调用数不再增长。
	before := srv.callsFor("a") + srv.callsFor("b")
	time.Sleep(50 * time.Millisecond)
	after := srv.callsFor("a") + srv.callsFor("b")
	if after != before {
		t.Fatalf("fetches continued after cancel: %d -> %d", before, after)
	}
}

// gatedCommit 在释放前阻塞提交，用于把缓冲区稳定压在上限处观察背压。
type gatedCommit struct {
	mu      sync.Mutex
	commits int
	release chan struct{}
}

func (g *gatedCommit) commit(ctx context.Context, items []Item) error {
	g.mu.Lock()
	g.commits++
	n := g.commits
	g.mu.Unlock()
	if n == 1 {
		select {
		case <-g.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func TestRunnerBackpressureBounded(t *testing.T) {
	// 第一页提交被闸门挡住时，已抓取页数必须稳定在容量上界：
	//   总抓取页 = 1（提交中，仍计入条目缓冲）+ 在途页数
	// 两个上限各自独立生效，分别构造用例验证。
	cases := []struct {
		name        string
		maxInFlight int
		maxItems    int
		minFetched  int // 至少已发起的抓取数（背压不应过度饥饿）
		maxFetched  int // 已完成抓取的硬上界（容量约束）
	}{
		// 在途页上限先生效：1 提交 + 2 在途 = 3 个槽位占满；
		// 第 3 次抓取可能还在网络耗时中，观测到 2 或 3 均合法，但绝不超过 3。
		{"inflight_caps", 2, 100, 2, 3},
		// 条目缓冲上限先生效：每页 2 条，1 提交(2)+2 在途(4)=6 条超过上限 4。
		{"buffer_caps", 4, 4, 3, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newStreamServer()
			srv.delay = 5 * time.Millisecond
			srv.setupStream("s", 2, 40)
			gate := &gatedCommit{release: make(chan struct{})}
			cfgs := []StreamConfig{{
				ID: "s", Store: &memStore{}, Commit: gate.commit,
				MaxInFlight: tc.maxInFlight, MaxBufferedItems: tc.maxItems,
			}}
			c := NewClient(unusedSharedTransport{}, Config{MaxConcurrency: 8, RatePerSecond: 1000})
			r := mustRunner(t, c, withStreamTransport(cfgs, srv))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan StreamResult, 1)
			go func() { done <- r.Run(ctx)[0] }()

			// 等抓取页数稳定：连续 60ms 不再增长，且至少达到期望上界。
			last, stable := -1, 0
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
				n := srv.callsFor("s")
				if n == last {
					if stable++; stable >= 3 {
						break
					}
				} else {
					stable = 0
					last = n
				}
			}
			fetched := srv.callsFor("s")
			t.Logf("%s: while first commit blocked, fetched pages=%d (ok range %d..%d)", tc.name, fetched, tc.minFetched, tc.maxFetched)
			if fetched < tc.minFetched || fetched > tc.maxFetched {
				t.Fatalf("%s: backpressure bound wrong: fetched=%d want [%d,%d]", tc.name, fetched, tc.minFetched, tc.maxFetched)
			}
			// 再等一拍，确认不会继续上涨（背压真正生效而非仅时序巧合）。
			time.Sleep(40 * time.Millisecond)
			if again := srv.callsFor("s"); again > tc.maxFetched {
				t.Fatalf("%s: unbounded buffering: fetched grew to %d", tc.name, again)
			}

			close(gate.release)
			res := <-done
			if res.Reason != StopCompleted || res.Checkpoint.Seq != 40 {
				t.Fatalf("want 40 items completed, got reason=%s seq=%d err=%v", res.Reason, res.Checkpoint.Seq, res.Err)
			}
		})
	}
}

func TestRunnerResumeAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	sink := newCommitLog()
	cfg := Config{MaxConcurrency: 4, RatePerSecond: 1000, MaxRetries: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}

	makeCfg := func() []StreamConfig {
		stA, _ := NewFileStore(dir+"/orders.json", "orders")
		stB, _ := NewFileStore(dir+"/billing.json", "billing")
		return []StreamConfig{
			{ID: "orders", Store: stA, Commit: sink.commit, MaxInFlight: 2, MaxBufferedItems: 8},
			{ID: "billing", Store: stB, Commit: sink.commit, MaxInFlight: 1},
		}
	}

	// 第一次：orders 流在第 2 页永久失败，billing 流完整完成。
	srv1 := newStreamServer()
	srv1.setupStream("orders", 2, 8)
	srv1.setupStream("billing", 3, 6)
	srv1.injectFailure("orders", EncodeCursor(2, 2),
		fmt.Errorf("boom"), fmt.Errorf("boom"))
	r1 := mustRunner(t, NewClient(unusedSharedTransport{}, cfg), withStreamTransport(makeCfg(), srv1))
	res1 := r1.Run(context.Background())
	partialOrders := 0
	for _, res := range res1 {
		t.Logf("run1 stream=%s reason=%s seq=%d", res.Stream, res.Reason, res.Checkpoint.Seq)
		if res.Stream == "orders" {
			if res.Reason != StopFailed {
				t.Fatalf("orders should fail, got %s", res.Reason)
			}
			partialOrders = len(res.Items)
		}
		if res.Stream == "billing" && res.Reason != StopCompleted {
			t.Fatalf("billing should complete, got %v", res.Err)
		}
	}
	if partialOrders == 0 {
		t.Fatal("orders should have confirmed first page before failing")
	}

	// “重启”：全新 Runner / 服务实例，仅靠磁盘文件与幂等 sink 接着跑。
	srv2 := newStreamServer()
	srv2.setupStream("orders", 2, 8)
	srv2.setupStream("billing", 3, 6)
	r2 := mustRunner(t, NewClient(unusedSharedTransport{}, cfg), withStreamTransport(makeCfg(), srv2))
	res2 := r2.Run(context.Background())
	for _, res := range res2 {
		t.Logf("run2 stream=%s reason=%s seq=%d", res.Stream, res.Reason, res.Checkpoint.Seq)
		if res.Reason != StopCompleted {
			t.Fatalf("stream %s should complete after restart, got %v", res.Stream, res.Err)
		}
	}
	// 无遗漏无重复：orders 8 条 + billing 6 条，全部唯一。
	if len(sink.ids) != 14 {
		t.Fatalf("want 14 unique items across restart, got %d (%v)", len(sink.ids), sink.ids)
	}
	for _, want := range []string{"orders-id-000", "orders-id-007", "billing-id-000", "billing-id-005"} {
		if !sink.seen[want] {
			t.Fatalf("missing expected item %s", want)
		}
	}
}

func TestRunnerCorruptCheckpointRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "orders.json")
	// 写入一个被篡改的检查点文件（校验和对不上）。
	writeEnvelopeFile(t, path, "orders", EncodeCursor(2, 2), 2, 1, func(env *checkpointEnvelope) {
		env.Seq = 99
	})
	store, _ := NewFileStore(path, "orders")
	srv := newStreamServer()
	srv.setupStream("orders", 2, 8)
	cfgs := []StreamConfig{{ID: "orders", Store: store}}
	c := NewClient(unusedSharedTransport{}, Config{RatePerSecond: 1000})
	r := mustRunner(t, c, withStreamTransport(cfgs, srv))

	res := r.Run(context.Background())[0]
	t.Logf("corrupt checkpoint -> reason=%s kind=%s err=%v", res.Reason, KindOf(res.Err), res.Err)
	if res.Reason != StopFailed || !IsKind(res.Err, KindCheckpointCorrupt) {
		t.Fatalf("corrupt checkpoint must yield failed/checkpoint_corrupt, got %s/%v", res.Reason, res.Err)
	}
	if srv.callsFor("orders") != 0 {
		t.Fatal("no fetch may happen when resume progress is corrupt")
	}
}

func TestRunnerStreamFilesDoNotCross(t *testing.T) {
	// orders 的进度文件被错误地交给 billing 打开：必须按流归属拒绝，不得串进度。
	path := filepath.Join(t.TempDir(), "cp.json")
	writeEnvelopeFile(t, path, "orders", EncodeCursor(4, 2), 4, 1, nil)
	store, _ := NewFileStore(path, "billing")
	srv := newStreamServer()
	srv.setupStream("billing", 2, 4)
	cfgs := []StreamConfig{{ID: "billing", Store: store}}
	c := NewClient(unusedSharedTransport{}, Config{RatePerSecond: 1000})
	r := mustRunner(t, c, withStreamTransport(cfgs, srv))
	res := r.Run(context.Background())[0]
	t.Logf("foreign checkpoint file -> reason=%s kind=%s err=%v", res.Reason, KindOf(res.Err), res.Err)
	if res.Reason != StopFailed || !IsKind(res.Err, KindCheckpointCorrupt) {
		t.Fatalf("foreign checkpoint must be rejected, got %s/%v", res.Reason, res.Err)
	}
}

// TestRunnerCommitFailureDoesNotHang 覆盖“提交侧失败、而抓取侧已预取填满
// 在途窗口”的情形：Runner 必须在有限时间内收尾返回，不泄漏协程/不死锁。
func TestRunnerCommitFailureDoesNotHang(t *testing.T) {
	srv := newStreamServer()
	srv.delay = 2 * time.Millisecond
	srv.setupStream("s", 2, 40)

	var once sync.Once
	failCommit := func(ctx context.Context, items []Item) error {
		var err error
		once.Do(func() { err = fmt.Errorf("sink unavailable") })
		return err
	}
	cfgs := []StreamConfig{{
		ID: "s", Store: &memStore{}, Commit: failCommit,
		MaxInFlight: 3, MaxBufferedItems: 100,
	}}
	c := NewClient(unusedSharedTransport{}, Config{MaxConcurrency: 8, RatePerSecond: 1000})
	r := mustRunner(t, c, withStreamTransport(cfgs, srv))

	before := runtime.NumGoroutine()
	done := make(chan StreamResult, 1)
	go func() { done <- r.Run(context.Background())[0] }()
	select {
	case res := <-done:
		t.Logf("commit failure -> reason=%s kind=%s err=%v", res.Reason, KindOf(res.Err), res.Err)
		if res.Reason != StopFailed || !IsKind(res.Err, KindTransport) {
			t.Fatalf("want failed/transport, got %s/%v", res.Reason, res.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner hung after commit failure with full inflight window")
	}
	// 等待并确认内部抓取/看门狗协程全部退出，不泄漏。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && runtime.NumGoroutine() > before {
		time.Sleep(10 * time.Millisecond)
	}
	if leaked := runtime.NumGoroutine() - before; leaked > 0 {
		t.Fatalf("leaked %d goroutines after stream failure", leaked)
	}

	// ctx 取消收尾同理：所有内部协程应已退出，再次 Run 行为正常。
	srv2 := newStreamServer()
	srv2.setupStream("ok", 2, 4)
	okSink := newCommitLog()
	okCfg := []StreamConfig{{ID: "ok", Store: &memStore{}, Commit: okSink.commit}}
	c2 := NewClient(unusedSharedTransport{}, Config{RatePerSecond: 1000})
	r2 := mustRunner(t, c2, withStreamTransport(okCfg, srv2))
	res := r2.Run(context.Background())[0]
	if res.Reason != StopCompleted || res.Checkpoint.Seq != 4 {
		t.Fatalf("subsequent run after failure broken: %+v", res)
	}
}

// TestRunnerTimeoutReturnsPartialProgress 用例区分“超时”这一结束原因：
// 超时后不再发起新抓取，并把已确认进度与部分结果返回。
func TestRunnerTimeoutReturnsPartialProgress(t *testing.T) {
	srv := newStreamServer()
	srv.delay = 20 * time.Millisecond
	srv.setupStream("slow", 5, 100) // 慢而长的流
	sink := newCommitLog()
	cfgs := []StreamConfig{{ID: "slow", Store: &memStore{}, Commit: sink.commit, MaxInFlight: 2}}
	c := NewClient(unusedSharedTransport{}, Config{MaxConcurrency: 4, RatePerSecond: 1000})
	r := mustRunner(t, c, withStreamTransport(cfgs, srv))

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Millisecond)
	defer cancel()
	res := r.Run(ctx)[0]
	t.Logf("timeout -> reason=%s kind=%s items=%d cpSeq=%d", res.Reason, KindOf(res.Err), len(res.Items), res.Checkpoint.Seq)
	if res.Reason != StopCanceled || !IsKind(res.Err, KindCanceled) {
		t.Fatalf("timeout must surface as canceled, got %s/%v", res.Reason, res.Err)
	}
	if res.Checkpoint.Seq == 0 || res.Checkpoint.Seq != uint64(len(sink.ids)) {
		t.Fatalf("partial progress inconsistent: cpSeq=%d committed=%d", res.Checkpoint.Seq, len(sink.ids))
	}
}

// readFileBytes 读取落盘检查点的原始字节，用于断言“已完成流的进度文件未被重写”。
func readFileBytes(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read checkpoint file %s: %v", path, err)
	}
	return b
}

// TestRunnerRestartReusesCompletedStreams 稳定覆盖“部分完成后重启”场景：
// 重启前同一批流分别处于——users 已完整跑完成、orders 跑到一半、billing
// 失败退出；随后用全新 client/runner（只共享磁盘检查点与幂等 sink）重启。
// 断言：已完成流不再被抓取、不重复提交、落盘进度与文件字节完全不变；
// 未完成/失败流各按各自落盘进度继续；整体不丢不重。
func TestRunnerRestartReusesCompletedStreams(t *testing.T) {
	dir := t.TempDir()
	ordersPath := dir + "/orders.json"
	billingPath := dir + "/billing.json"
	usersPath := dir + "/users.json"

	sink := newCommitLog() // 跨“重启”共享的幂等提交端
	cfg := Config{MaxConcurrency: 4, RatePerSecond: 1000, MaxRetries: 1,
		InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}

	makeCfgs := func() []StreamConfig {
		stOrders, _ := NewFileStore(ordersPath, "orders")
		stBilling, _ := NewFileStore(billingPath, "billing")
		stUsers, _ := NewFileStore(usersPath, "users")
		return []StreamConfig{
			{ID: "orders", Store: stOrders, Commit: sink.commit, MaxInFlight: 1},
			{ID: "billing", Store: stBilling, Commit: sink.commit, MaxInFlight: 1},
			{ID: "users", Store: stUsers, Commit: sink.commit, MaxInFlight: 1},
		}
	}

	// ---- 阶段 1：users 流完整跑到尾页（orders/billing 未参与）。----
	srvA := newStreamServer()
	srvA.setupStream("users", 2, 6) // 3 页，共 6 条
	rA := mustRunner(t, NewClient(unusedSharedTransport{}, cfg),
		withStreamTransport([]StreamConfig{makeCfgs()[2]}, srvA))
	resA := rA.Run(context.Background())
	if resA[0].Reason != StopCompleted {
		t.Fatalf("[phase1] users should complete, got %s/%v", resA[0].Reason, resA[0].Err)
	}
	usersCompletedBytes := readFileBytes(t, usersPath)
	t.Logf("[phase1] users completed: seq=%d checkpointFile=%s", resA[0].Checkpoint.Seq, usersCompletedBytes)

	// ---- 阶段 2：orders 跑到第二页失败，仅首页(4 条)确认落盘；billing 失败，仅首页(2 条)确认。----
	srvB := newStreamServer()
	srvB.setupStream("orders", 4, 10) // 3 页：4 + 4 + 2
	srvB.setupStream("billing", 2, 8) // 4 页：2 x 4
	srvB.injectFailure("orders", EncodeCursor(4, 2), fmt.Errorf("boom"), fmt.Errorf("boom"))
	srvB.injectFailure("billing", EncodeCursor(2, 2), fmt.Errorf("boom"), fmt.Errorf("boom"))
	cfgsB := makeCfgs()
	rB := mustRunner(t, NewClient(unusedSharedTransport{}, cfg), withStreamTransport(cfgsB, srvB))
	resB := rB.Run(context.Background())
	byID := map[StreamID]StreamResult{}
	for _, res := range resB {
		byID[res.Stream] = res
		t.Logf("[phase2] stream=%s reason=%s kind=%s committedItems=%d cpSeq=%d",
			res.Stream, res.Reason, KindOf(res.Err), len(res.Items), res.Checkpoint.Seq)
	}
	if byID["orders"].Reason != StopFailed || byID["orders"].Checkpoint.Seq != 4 {
		t.Fatalf("[phase2] orders should fail with seq=4, got reason=%s seq=%d",
			byID["orders"].Reason, byID["orders"].Checkpoint.Seq)
	}
	if byID["billing"].Reason != StopFailed || byID["billing"].Checkpoint.Seq != 2 {
		t.Fatalf("[phase2] billing should fail with seq=2, got reason=%s seq=%d",
			byID["billing"].Reason, byID["billing"].Checkpoint.Seq)
	}
	if byID["users"].Reason != StopCompleted {
		t.Fatalf("[phase2] users (already on disk as completed) must stay completed, got %s/%v",
			byID["users"].Reason, byID["users"].Err)
	}
	// 阶段 2 已经是一次“同进程内复用”：users 不应被抓取，进度文件不变。
	if srvB.callsFor("users") != 0 {
		t.Fatalf("[phase2] completed users stream was re-fetched: %d calls", srvB.callsFor("users"))
	}
	if got := readFileBytes(t, usersPath); !bytesEqual(got, usersCompletedBytes) {
		t.Fatalf("[phase2] users checkpoint file was rewritten:\n before=%s\n after =%s", usersCompletedBytes, got)
	}

	// ---- 阶段 3：真正的“进程重启”——全新服务、全新 client、全新 runner/FileStore，
	// 只靠磁盘文件与幂等 sink。三条流一起跑。----
	srvC := newStreamServer()
	srvC.setupStream("orders", 4, 10)
	srvC.setupStream("billing", 2, 8)
	srvC.setupStream("users", 2, 6)
	rC := mustRunner(t, NewClient(unusedSharedTransport{}, cfg), withStreamTransport(makeCfgs(), srvC))
	resC := rC.Run(context.Background())
	for _, res := range resC {
		t.Logf("[restart] stream=%s reason=%s kind=%s committedItems(this run)=%d cpSeq=%d",
			res.Stream, res.Reason, KindOf(res.Err), len(res.Items), res.Checkpoint.Seq)
		if res.Reason != StopCompleted {
			t.Fatalf("[restart] stream %s should complete, got %s/%v", res.Stream, res.Reason, res.Err)
		}
	}

	// 已完成流：重启后零抓取、零本次提交、返回进度与重启前完全一致。
	if n := srvC.callsFor("users"); n != 0 {
		t.Fatalf("[restart] completed users stream must not be fetched, got %d calls", n)
	}
	usersRes := resC[2]
	if usersRes.Checkpoint == nil || usersRes.Checkpoint.Cursor != "" || usersRes.Checkpoint.Seq != 6 {
		t.Fatalf("[restart] users checkpoint must be the preserved completed one, got %+v", usersRes.Checkpoint)
	}
	if len(usersRes.Items) != 0 {
		t.Fatalf("[restart] completed users stream must not re-commit items, got %d", len(usersRes.Items))
	}
	if got := readFileBytes(t, usersPath); !bytesEqual(got, usersCompletedBytes) {
		t.Fatalf("[restart] users checkpoint file changed despite being completed:\n before=%s\n after =%s",
			usersCompletedBytes, got)
	}

	// 未完成/失败流：各按各的落盘进度继续，服务侧只从恢复游标开始抓。
	// orders 首页 4 条已确认 -> 只剩 2 次抓取（offset4 页 + 尾页）。
	if n := srvC.callsFor("orders"); n != 2 {
		t.Fatalf("[restart] orders should resume with 2 fetches, got %d", n)
	}
	// billing 首页 2 条已确认 -> 只剩 3 次抓取（offset2/4/6）。
	if n := srvC.callsFor("billing"); n != 3 {
		t.Fatalf("[restart] billing should resume with 3 fetches, got %d", n)
	}

	// 整体不丢不重：orders 10 + billing 8 + users 6 = 24 个唯一 ID。
	if len(sink.ids) != 24 {
		t.Fatalf("[restart] want 24 unique committed items (no dup/no loss), got %d: %v", len(sink.ids), sink.ids)
	}
	for _, want := range []string{
		"orders-id-000", "orders-id-009",
		"billing-id-000", "billing-id-007",
		"users-id-000", "users-id-005",
	} {
		if !sink.seen[want] {
			t.Fatalf("[restart] missing expected item %s", want)
		}
	}
	t.Logf("[restart] all streams done; total unique committed=%d; users untouched (0 fetch, file unchanged)",
		len(sink.ids))
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
