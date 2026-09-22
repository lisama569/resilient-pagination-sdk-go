package syncsdk

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Transport 抽象底层分页请求，测试中用本地替身实现。
type Transport interface {
	// Fetch 按游标抓取一页；cursor 为空表示首页。
	Fetch(ctx context.Context, cursor string) (body []byte, err error)
}

// RetryableError 表示可重试的临时失败（如限流 429、临时 5xx）。
type RetryableError struct{ Reason string }

func (e *RetryableError) Error() string { return "retryable: " + e.Reason }

// Config 控制并发、速率与退避。
type Config struct {
	MaxConcurrency int           // 并发抓取上限，<=0 取 1
	RatePerSecond  float64       // 每秒请求上限，<=0 取 1
	MaxRetries     int           // 可重试失败的最大重试次数
	InitialBackoff time.Duration // 首次退避时长
	MaxBackoff     time.Duration // 退避上限
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.MaxConcurrency <= 0 {
		out.MaxConcurrency = 1
	}
	if out.RatePerSecond <= 0 {
		out.RatePerSecond = 1
	}
	if out.MaxRetries < 0 {
		out.MaxRetries = 0
	}
	if out.InitialBackoff <= 0 {
		out.InitialBackoff = 10 * time.Millisecond
	}
	if out.MaxBackoff <= 0 {
		out.MaxBackoff = time.Second
	}
	return out
}

// Client 是可被多个协程安全共享的分页客户端。
// 并发数与请求速率均不超过 Config 上限。
type Client struct {
	t   Transport
	cfg Config

	sem chan struct{} // 并发信号量

	mu       sync.Mutex
	nextSlot time.Time // 限流：下一个允许发送的时刻
}

// NewClient 构造客户端。
func NewClient(t Transport, cfg Config) *Client {
	c := cfg.withDefaults()
	return &Client{t: t, cfg: c, sem: make(chan struct{}, c.MaxConcurrency)}
}

// waitRate 预留一个发送时隙并等到该时刻；取消时返回 KindCanceled。
func (c *Client) waitRate(ctx context.Context) error {
	interval := time.Duration(float64(time.Second) / c.cfg.RatePerSecond)
	c.mu.Lock()
	now := time.Now()
	slot := c.nextSlot
	if slot.Before(now) {
		slot = now
	}
	c.nextSlot = slot.Add(interval)
	c.mu.Unlock()

	if d := time.Until(slot); d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return &Error{Kind: KindCanceled, Message: "canceled while rate limiting", Cause: ctx.Err()}
		case <-t.C:
		}
	}
	return nil
}

// backoff 按指数退避等待，可被 ctx 中断。
func (c *Client) backoff(ctx context.Context, attempt int) error {
	d := c.cfg.InitialBackoff << attempt
	if d > c.cfg.MaxBackoff {
		d = c.cfg.MaxBackoff
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return &Error{Kind: KindCanceled, Message: "canceled during backoff", Cause: ctx.Err()}
	case <-t.C:
		return nil
	}
}

// FetchPage 抓取一页：限流、并发上限、退避重试、错误归一化。
// 重试使用同一游标，幂等，不产生重复数据或重复副作用。
func (c *Client) FetchPage(ctx context.Context, cursor string) (*Page, error) {
	select {
	case c.sem <- struct{}{}:
		defer func() { <-c.sem }()
	case <-ctx.Done():
		return nil, &Error{Kind: KindCanceled, Message: "canceled while waiting for concurrency slot", Cause: ctx.Err()}
	}

	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			if err := c.backoff(ctx, attempt-1); err != nil {
				return nil, err
			}
		}
		if err := c.waitRate(ctx); err != nil {
			return nil, err
		}
		body, err := c.t.Fetch(ctx, cursor)
		if err != nil {
			var re *RetryableError
			if errors.As(err, &re) {
				lastErr = err
				continue // 同一游标重试，幂等
			}
			if ctx.Err() != nil {
				return nil, &Error{Kind: KindCanceled, Message: "canceled during fetch", Cause: ctx.Err()}
			}
			return nil, &Error{Kind: KindTransport, Message: "transport failure", Cause: err}
		}
		return parsePage(body)
	}
	return nil, &Error{Kind: KindRetryExhausted, Message: "retryable failure exhausted retries", Cause: lastErr}
}
