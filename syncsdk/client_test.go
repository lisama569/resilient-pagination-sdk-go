package syncsdk

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestClientRetryOnRetryableFailure(t *testing.T) {
	ft := newFakeTransport()
	ft.addPage("", 2, "", "a", "b")
	// 前两次临时失败，第三次成功。
	ft.injectFailure("", &RetryableError{Reason: "429 rate limited"}, &RetryableError{Reason: "503 unavailable"})

	c := NewClient(ft, Config{MaxRetries: 3, InitialBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond, RatePerSecond: 1000})
	page, err := c.FetchPage(context.Background(), "")
	t.Logf("input cursor=%q retries_used=%d err=%v", "", ft.calls[""]-1, err)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ft.calls[""] != 3 {
		t.Fatalf("want 3 attempts, got %d", ft.calls[""])
	}
	if len(page.Items) != 2 {
		t.Fatalf("want 2 items, got %d", len(page.Items))
	}
}

func TestClientRetryExhausted(t *testing.T) {
	ft := newFakeTransport()
	ft.addPage("", 2, "", "a")
	ft.injectFailure("", &RetryableError{Reason: "429"}, &RetryableError{Reason: "429"}, &RetryableError{Reason: "429"})

	c := NewClient(ft, Config{MaxRetries: 2, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, RatePerSecond: 1000})
	_, err := c.FetchPage(context.Background(), "")
	t.Logf("input cursor=%q retries_used=%d normalized error: %v (kind=%s)", "", ft.calls[""]-1, err, KindOf(err))
	if !IsKind(err, KindRetryExhausted) {
		t.Fatalf("want kind %s, got %v", KindRetryExhausted, err)
	}
	if ft.calls[""] != 3 { // 1 + 2 retries
		t.Fatalf("want 3 attempts, got %d", ft.calls[""])
	}
}

func TestClientNonRetryableFailure(t *testing.T) {
	ft := newFakeTransport()
	ft.addPage("", 2, "", "a")
	ft.injectFailure("", errors.New("connection refused"))

	c := NewClient(ft, Config{MaxRetries: 3, InitialBackoff: time.Millisecond, RatePerSecond: 1000})
	_, err := c.FetchPage(context.Background(), "")
	t.Logf("normalized error: %v (kind=%s)", err, KindOf(err))
	if !IsKind(err, KindTransport) {
		t.Fatalf("want kind %s, got %v", KindTransport, err)
	}
	if ft.calls[""] != 1 {
		t.Fatalf("non-retryable error must not be retried, got %d attempts", ft.calls[""])
	}
}

func TestClientConcurrencyLimit(t *testing.T) {
	ft := newFakeTransport()
	ft.delay = 20 * time.Millisecond
	ft.addPage("p1", 2, "")
	ft.addPage("p2", 2, "")
	ft.addPage("p3", 2, "")
	ft.addPage("p4", 2, "")

	c := NewClient(ft, Config{MaxConcurrency: 2, RatePerSecond: 1000})
	var wg sync.WaitGroup
	for _, cur := range []string{"p1", "p2", "p3", "p4"} {
		wg.Add(1)
		go func(cur string) {
			defer wg.Done()
			if _, err := c.FetchPage(context.Background(), cur); err != nil {
				t.Errorf("fetch %s: %v", cur, err)
			}
		}(cur)
	}
	wg.Wait()
	t.Logf("max observed concurrency=%d (limit=2)", ft.maxConc)
	if ft.maxConc > 2 {
		t.Fatalf("concurrency exceeded limit: %d", ft.maxConc)
	}
}

func TestClientRateLimit(t *testing.T) {
	ft := newFakeTransport()
	ft.addPage("p", 2, "")

	c := NewClient(ft, Config{MaxConcurrency: 8, RatePerSecond: 50}) // 20ms 间隔
	const n = 5
	start := time.Now()
	for i := 0; i < n; i++ {
		if _, err := c.FetchPage(context.Background(), "p"); err != nil {
			t.Fatalf("fetch: %v", err)
		}
	}
	elapsed := time.Since(start)
	t.Logf("input requests=%d rate=50/s elapsed=%v", n, elapsed)
	if elapsed < 4*20*time.Millisecond*9/10 { // 允许 10% 误差
		t.Fatalf("rate limit not enforced: %d requests in %v", n, elapsed)
	}
}

func TestClientCancelDuringBackoff(t *testing.T) {
	ft := newFakeTransport()
	ft.addPage("", 2, "", "a")
	ft.injectFailure("", &RetryableError{Reason: "429"}, &RetryableError{Reason: "429"}, &RetryableError{Reason: "429"})

	c := NewClient(ft, Config{MaxRetries: 5, InitialBackoff: time.Hour, MaxBackoff: time.Hour, RatePerSecond: 1000})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.FetchPage(ctx, "")
	t.Logf("retries_used=%d elapsed=%v normalized error: %v (kind=%s)", ft.calls[""]-1, time.Since(start), err, KindOf(err))
	if !IsKind(err, KindCanceled) {
		t.Fatalf("want kind %s, got %v", KindCanceled, err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("cancellation not timely: %v", time.Since(start))
	}
}
