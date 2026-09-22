package sdk

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// testBackoff 使用确定性随机源与极小等待，保证测试可复现且快速。
func testBackoff(maxRetries int) Backoff {
	return Backoff{
		Base:       time.Millisecond,
		Max:        5 * time.Millisecond,
		Factor:     2,
		MaxRetries: maxRetries,
		Jitter:     0.1,
	}.WithRand(rand.New(rand.NewSource(42)))
}

func TestFetchPage_RateLimitRetry(t *testing.T) {
	pages, _ := buildChain(2, 1)
	ft := newFakeTransport(pages)
	ft.rateLimitN = 3 // 前 3 次请求被限流
	c := NewClient(ft, WithBackoff(testBackoff(5)))

	page, err := c.FetchPage(context.Background(), "")
	key := idempotencyKey("")
	t.Logf("input=首页重试场景 retries=%d err=%v", ft.retries(key), err)
	if err != nil {
		t.Fatalf("限流应被退避重试吸收: %v", err)
	}
	if ft.retries(key) != 3 {
		t.Fatalf("重试次数 = %d; want 3", ft.retries(key))
	}
	// 幂等性：同一逻辑页的所有重试共用同一幂等键。
	for k, n := range ft.idemSeen {
		if k != key || n != 4 {
			t.Fatalf("出现额外幂等键 %s x%d，可能产生重复副作用", k, n)
		}
	}
	if page.Cursor == "" {
		t.Fatal("应返回下一页游标")
	}
}

func TestFetchPage_RetryExhausted(t *testing.T) {
	pages, _ := buildChain(1, 1)
	ft := newFakeTransport(pages)
	ft.failN = 10 // 持续 500
	c := NewClient(ft, WithBackoff(testBackoff(2)))

	_, err := c.FetchPage(context.Background(), "")
	logErr(t, "持续 500，MaxRetries=2", err)
	if KindOf(err) != ErrKindTransient {
		t.Fatalf("kind = %s; want transient", KindOf(err))
	}
	if got := ft.retries(idempotencyKey("")); got != 2 {
		t.Fatalf("重试次数 = %d; want 2", got)
	}
}

func TestFetchPage_VersionMismatch(t *testing.T) {
	pages, _ := buildChain(1, 1)
	ft := newFakeTransport(pages)
	ft.version = Version{Major: 2, Minor: 0} // 对端主版本更高
	c := NewClient(ft, WithBackoff(testBackoff(1)), WithVersion(Version{1, 2}))

	_, err := c.FetchPage(context.Background(), "")
	logErr(t, "响应主版本 2.x，客户端 1.2", err)
	if KindOf(err) != ErrKindVersionUnknown {
		t.Fatalf("kind = %s; want version_unknown", KindOf(err))
	}
}

func TestFetchAll_ConcurrentClientsBounded(t *testing.T) {
	// 多个协程共享同一客户端：聚合并发与速率不得超过配置上限。
	pages, _ := buildChain(8, 2)
	ft := newFakeTransport(pages)
	ft.delay = 10 * time.Millisecond // 制造并发重叠
	const maxConc = 3
	c := NewClient(ft,
		WithConcurrency(maxConc),
		WithRateLimit(1000, maxConc),
		WithBackoff(testBackoff(2)))

	// 每个协程从不同的已存在游标开始抓取一页。
	cursors := []string{""}
	for cur := pages[""].Cursor; cur != ""; cur = pages[cur].Cursor {
		cursors = append(cursors, cur)
	}
	var wg sync.WaitGroup
	errs := make([]error, len(cursors))
	for i, cur := range cursors {
		wg.Add(1)
		go func(i int, cur string) {
			defer wg.Done()
			_, errs[i] = c.FetchPage(context.Background(), cur)
		}(i, cur)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("协程 %d 抓取失败: %v", i, err)
		}
	}
	t.Logf("input=%d 协程共享客户端 maxConc=%d -> 观测最大并发=%d 总请求=%d",
		len(cursors), maxConc, ft.maxInFlight, len(ft.calls))
	if ft.maxInFlight > maxConc {
		t.Fatalf("观测最大并发 %d 超过上限 %d", ft.maxInFlight, maxConc)
	}
}

func TestFetchAll_RateLimitBounded(t *testing.T) {
	pages, _ := buildChain(6, 1)
	ft := newFakeTransport(pages)
	const rate = 200 // 每秒 200 次
	c := NewClient(ft,
		WithRateLimit(rate, 1),
		WithBackoff(testBackoff(2)))

	var n int
	sink := func(ctx context.Context, items []json.RawMessage) error {
		n += len(items)
		return nil
	}
	start := time.Now()
	if _, err := c.FetchAll(context.Background(), "", sink); err != nil {
		t.Fatalf("FetchAll: %v", err)
	}
	elapsed := time.Since(start)
	// 6 页、burst=1：至少需等待 5 个令牌间隔。
	minElapsed := 5 * time.Second / rate
	t.Logf("input=6页 rate=%d/s burst=1 -> 页数=%d 耗时=%v 下限=%v", rate, n, elapsed, minElapsed)
	if n != 6 {
		t.Fatalf("抓取页数 = %d; want 6", n)
	}
	if elapsed < minElapsed {
		t.Fatalf("耗时 %v 低于限流下限 %v，速率超限", elapsed, minElapsed)
	}
}

func TestFetchAll_CancelPreservesProgress(t *testing.T) {
	pages, _ := buildChain(10, 1)
	ft := newFakeTransport(pages)
	c := NewClient(ft, WithBackoff(testBackoff(2)))

	ctx, cancel := context.WithCancel(context.Background())
	confirmed := 0
	sink := func(ctx context.Context, items []json.RawMessage) error {
		confirmed++
		if confirmed == 3 {
			cancel() // 确认 3 页后取消
		}
		return nil
	}
	progress, err := c.FetchAll(ctx, "", sink)
	logErr(t, "确认 3 页后取消", err)
	if KindOf(err) != ErrKindCanceled {
		t.Fatalf("kind = %s; want canceled", KindOf(err))
	}
	// 已确认的进度：第 3 页提交后游标推进到 seq=3。
	cur, decErr := DecodeCursor(progress)
	if decErr != nil {
		t.Fatalf("进度游标非法: %v", decErr)
	}
	t.Logf("已确认页数=%d 保留进度游标序号=%d", confirmed, cur.Seq)
	if cur.Seq != 3 {
		t.Fatalf("保留进度序号 = %d; want 3", cur.Seq)
	}
	if confirmed != 3 {
		t.Fatalf("确认页数 = %d; want 3", confirmed)
	}
}

func TestFetchPage_CancelWhileRateLimited(t *testing.T) {
	pages, _ := buildChain(1, 1)
	ft := newFakeTransport(pages)
	c := NewClient(ft, WithRateLimit(0.5, 1)) // 每 2 秒一个令牌

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	// 第一个请求占掉突发令牌，第二个必须等待并被超时终止。
	if _, err := c.FetchPage(ctx, ""); err != nil {
		t.Fatalf("首次请求应成功: %v", err)
	}
	_, err := c.FetchPage(ctx, EncodeCursor(Cursor{Seq: 1, Token: "tok-1"}))
	logErr(t, "限流等待中超时", err)
	if KindOf(err) != ErrKindCanceled {
		t.Fatalf("kind = %s; want canceled", KindOf(err))
	}
}
