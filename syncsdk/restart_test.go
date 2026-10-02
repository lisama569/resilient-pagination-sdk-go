package syncsdk

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

// restartFixture 把“一批流跑到部分完成后模拟重启”的公共搭建收敛到一处：
// 三条流分别覆盖 已完成 / 未完成(中途失败) / 失败退出 三种落盘状态。
type restartFixture struct {
	dir   string
	sinks map[StreamID]*commitLog
	cfg   Config
}

func newRestartFixture(t *testing.T) *restartFixture {
	t.Helper()
	return &restartFixture{
		dir:   t.TempDir(),
		sinks: map[StreamID]*commitLog{},
		cfg: Config{MaxConcurrency: 4, RatePerSecond: 1000, MaxRetries: 1,
			InitialBackoff: 1, MaxBackoff: 2},
	}
}

// streamConfigs 为每条流建全新的 FileStore（模拟重启后的新实例），
// 但路径与幂等 sink 跨“重启”共享。
func (f *restartFixture) streamConfigs(t *testing.T, ids ...StreamID) []StreamConfig {
	t.Helper()
	var cfgs []StreamConfig
	for _, id := range ids {
		if f.sinks[id] == nil {
			f.sinks[id] = newCommitLog()
		}
		st, err := NewFileStore(filepath.Join(f.dir, string(id)+".json"), string(id))
		if err != nil {
			t.Fatalf("new file store for %s: %v", id, err)
		}
		cfgs = append(cfgs, StreamConfig{ID: id, Store: st, Commit: f.sinks[id].commit})
	}
	return cfgs
}

func (f *restartFixture) sink(id StreamID) *commitLog { return f.sinks[id] }

// batches 返回该流已执行的提交批次数（含被幂等去重的重复提交），
// 用于断言“已完成流重启后不再重复提交”。
func (c *commitLog) batches() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.batch
}

// TestRunnerRestartCompletedStreamNotRefetched 覆盖核心场景：
// 一批流跑到部分完成（done 已完成、partial 中途失败、failed 首页即失败）后
// 模拟重启（全新 client/runner/store 实例，仅靠落盘文件与幂等提交续跑），
// 已完成流必须直接复用落盘进度，不再抓取、不再重复提交、进度不变。
func TestRunnerRestartCompletedStreamNotRefetched(t *testing.T) {
	fx := newRestartFixture(t)
	ids := []StreamID{"done", "partial", "failed"}

	// ---- 第 1 轮：三条流分别落到 完成 / 中途失败 / 首页失败 ----
	srv1 := newStreamServer()
	srv1.setupStream("done", 2, 6)    // 3 页，完整跑完
	srv1.setupStream("partial", 2, 6) // 3 页，第 2 页永久失败 -> 只确认第 1 页
	srv1.setupStream("failed", 2, 4)  // 2 页，首页即永久失败 -> 0 条确认
	srv1.injectFailure("partial", EncodeCursor(2, 2),
		fmt.Errorf("page2 down"), fmt.Errorf("page2 down"), fmt.Errorf("page2 down"))
	srv1.injectFailure("failed", "",
		fmt.Errorf("first page down"), fmt.Errorf("first page down"), fmt.Errorf("first page down"))

	r1 := mustRunner(t, NewClient(unusedSharedTransport{}, fx.cfg),
		withStreamTransport(fx.streamConfigs(t, ids...), srv1))
	res1 := map[StreamID]StreamResult{}
	for _, res := range r1.Run(context.Background()) {
		res1[res.Stream] = res
		t.Logf("run1 stream=%s reason=%s kind=%s seq=%d done=%v fetched=%d committed=%d",
			res.Stream, res.Reason, KindOf(res.Err), res.Checkpoint.Seq, res.Checkpoint.Done,
			srv1.callsFor(res.Stream), len(fx.sink(res.Stream).ids))
	}

	// 第 1 轮结果：三种状态各自落盘。
	if res1["done"].Reason != StopCompleted || res1["done"].Checkpoint.Seq != 6 {
		t.Fatalf("run1 done stream: want completed seq=6, got reason=%s seq=%d err=%v",
			res1["done"].Reason, res1["done"].Checkpoint.Seq, res1["done"].Err)
	}
	if res1["partial"].Reason != StopFailed || res1["partial"].Checkpoint.Seq != 2 {
		t.Fatalf("run1 partial stream: want failed seq=2, got reason=%s seq=%d",
			res1["partial"].Reason, res1["partial"].Checkpoint.Seq)
	}
	if res1["failed"].Reason != StopFailed || res1["failed"].Checkpoint.Seq != 0 {
		t.Fatalf("run1 failed stream: want failed seq=0, got reason=%s seq=%d",
			res1["failed"].Reason, res1["failed"].Checkpoint.Seq)
	}
	// 失败归属与类别必须可区分。
	var se *Error
	if !asError(res1["failed"].Err, &se) || se.Stream != "failed" || !IsKind(res1["failed"].Err, KindTransport) {
		t.Fatalf("run1 failed stream error must carry stream attribution and kind, got %+v", res1["failed"].Err)
	}
	doneSeqBefore := res1["done"].Checkpoint.Seq
	doneBatchesBefore := fx.sink("done").batches()

	// ---- 模拟重启：全新 server / client / runner / FileStore，只共享落盘文件与幂等 sink ----
	srv2 := newStreamServer()
	srv2.setupStream("done", 2, 6)
	srv2.setupStream("partial", 2, 6)
	srv2.setupStream("failed", 2, 4) // 故障已移除

	r2 := mustRunner(t, NewClient(unusedSharedTransport{}, fx.cfg),
		withStreamTransport(fx.streamConfigs(t, ids...), srv2))
	res2 := map[StreamID]StreamResult{}
	for _, res := range r2.Run(context.Background()) {
		res2[res.Stream] = res
		t.Logf("run2 stream=%s reason=%s kind=%s seq=%d done=%v fetched=%d committed_total=%d",
			res.Stream, res.Reason, KindOf(res.Err), res.Checkpoint.Seq, res.Checkpoint.Done,
			srv2.callsFor(res.Stream), len(fx.sink(res.Stream).ids))
	}

	// 已完成流：不再抓取、不再重复提交、进度与重启前完全一致。
	done2 := res2["done"]
	if done2.Reason != StopCompleted || done2.Err != nil {
		t.Fatalf("run2 done stream must complete cleanly, got reason=%s err=%v", done2.Reason, done2.Err)
	}
	if n := srv2.callsFor("done"); n != 0 {
		t.Fatalf("completed stream refetched after restart: %d fetches", n)
	}
	if got := fx.sink("done").batches(); got != doneBatchesBefore {
		t.Fatalf("completed stream recommitted after restart: batches %d -> %d", doneBatchesBefore, got)
	}
	if done2.Checkpoint.Seq != doneSeqBefore || !done2.Checkpoint.Done {
		t.Fatalf("completed stream progress changed: seq %d -> %d done=%v",
			doneSeqBefore, done2.Checkpoint.Seq, done2.Checkpoint.Done)
	}
	if len(done2.Items) != 0 {
		t.Fatalf("completed stream must not confirm new items, got %d", len(done2.Items))
	}

	// 未完成流：从断点续抓（不重抓已确认的第 1 页），最终完成。
	if res2["partial"].Reason != StopCompleted || res2["partial"].Checkpoint.Seq != 6 {
		t.Fatalf("run2 partial stream: want completed seq=6, got reason=%s seq=%d err=%v",
			res2["partial"].Reason, res2["partial"].Checkpoint.Seq, res2["partial"].Err)
	}
	if n := srv2.callsFor("partial"); n != 2 {
		t.Fatalf("partial stream must resume from checkpoint (2 remaining pages), fetched %d", n)
	}
	if srv2.calls["partial\x00"] != 0 {
		t.Fatal("partial stream refetched from the first page instead of resuming")
	}

	// 上一轮失败的流：故障移除后从头补齐，最终完成。
	if res2["failed"].Reason != StopCompleted || res2["failed"].Checkpoint.Seq != 4 {
		t.Fatalf("run2 failed stream: want completed seq=4, got reason=%s seq=%d err=%v",
			res2["failed"].Reason, res2["failed"].Checkpoint.Seq, res2["failed"].Err)
	}

	// 整体条目不丢不重：6 + 6 + 4 = 16 条，全部唯一。
	total := 0
	for _, id := range ids {
		n := len(fx.sink(id).ids)
		t.Logf("final stream=%s unique_committed=%d", id, n)
		total += n
	}
	if total != 16 {
		t.Fatalf("want 16 unique committed items across restart, got %d", total)
	}
}

// TestSyncerRestartCompletedCheckpointReused 覆盖单流 Syncer 的同一规则：
// 落盘为完成态的检查点被正确识别为“已完成”，不抓取、不报错、进度不变。
func TestSyncerRestartCompletedCheckpointReused(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "orders.json")

	// 第 1 轮：完整同步到尾页。
	ft1 := newFakeTransport()
	setupPages(ft1, 2, 4)
	st1, err := NewFileStore(path, "orders")
	if err != nil {
		t.Fatal(err)
	}
	sink := newCommitLog()
	res1, err := NewSyncer(NewClient(ft1, Config{RatePerSecond: 1000}), st1, sink.commit).Run(context.Background())
	if err != nil || !res1.Completed {
		t.Fatalf("run1: want completed, got res=%+v err=%v", res1, err)
	}
	t.Logf("run1 completed seq=%d done=%v committed=%d", res1.Checkpoint.Seq, res1.Checkpoint.Done, len(sink.ids))

	// “重启”：全新 transport/client/store，只共享落盘文件与幂等 sink。
	ft2 := newFakeTransport()
	setupPages(ft2, 2, 4)
	st2, err := NewFileStore(path, "orders")
	if err != nil {
		t.Fatal(err)
	}
	res2, err := NewSyncer(NewClient(ft2, Config{RatePerSecond: 1000}), st2, sink.commit).Run(context.Background())
	if err != nil {
		t.Fatalf("run2: completed checkpoint must not error, got %v", err)
	}
	t.Logf("run2 completed=%v seq=%d fetched=%d committed=%d",
		res2.Completed, res2.Checkpoint.Seq, ft2.totalCalls(), len(sink.ids))
	if !res2.Completed {
		t.Fatal("run2: completed checkpoint must be recognized as completed")
	}
	if ft2.totalCalls() != 0 {
		t.Fatalf("run2: completed stream refetched %d pages", ft2.totalCalls())
	}
	if res2.Checkpoint.Seq != res1.Checkpoint.Seq {
		t.Fatalf("run2: progress changed across restart: %d -> %d", res1.Checkpoint.Seq, res2.Checkpoint.Seq)
	}
	if len(sink.ids) != 4 {
		t.Fatalf("run2: items lost or duplicated: %d unique", len(sink.ids))
	}
}
