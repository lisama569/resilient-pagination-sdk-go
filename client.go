package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"time"
)

// Transport 是底层传输抽象。生产可实现 HTTP，测试使用本地替身，
// SDK 本身不依赖任何外部网络。
type Transport interface {
	Do(ctx context.Context, req *Request) (*Response, error)
}

// Request 是一次分页抓取请求。
type Request struct {
	Cursor         string // 上一页返回的游标；空串表示首页
	IdempotencyKey string // 幂等键：同一逻辑页的重试共用同一键
	Version        string // 客户端协议版本
}

// Response 是传输层原始响应。
type Response struct {
	Status     int    // 类 HTTP 状态码：200 成功，429 限流，5xx 临时失败
	Body       []byte // 分页响应体
	RetryAfter int    // 限流时服务端建议的等待毫秒数
}

// Client 是可在多协程间共享的分页客户端。
// 并发槽与限流器为全客户端共享，保证聚合并发数与速率不超过配置上限。
type Client struct {
	transport Transport
	limiter   *Limiter
	backoff   Backoff
	sem       chan struct{} // 并发上限
	version   Version
}

// Option 配置 Client。
type Option func(*Client)

// WithConcurrency 设置最大并发抓取数（<=1 表示串行）。
func WithConcurrency(n int) Option {
	return func(c *Client) {
		if n < 1 {
			n = 1
		}
		c.sem = make(chan struct{}, n)
	}
}

// WithRateLimit 设置每秒请求速率与突发上限。
func WithRateLimit(rate float64, burst int) Option {
	return func(c *Client) { c.limiter = NewLimiter(rate, burst) }
}

// WithBackoff 设置重试退避策略。
func WithBackoff(b Backoff) Option {
	return func(c *Client) { c.backoff = b }
}

// WithVersion 设置客户端协议版本。
func WithVersion(v Version) Option {
	return func(c *Client) { c.version = v }
}

// NewClient 创建分页客户端。
func NewClient(t Transport, opts ...Option) *Client {
	c := &Client{
		transport: t,
		backoff:   DefaultBackoff(),
		version:   SupportedVersions[len(SupportedVersions)-1],
		sem:       make(chan struct{}, 4),
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.sem == nil {
		c.sem = make(chan struct{}, 1)
	}
	return c
}

// idempotencyKey 为逻辑页（由游标唯一确定）生成稳定的幂等键：
// 同一页的任意次重试共用同一键，传输层可据此去重，避免重复副作用。
func idempotencyKey(cursor string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(cursor))
	return fmt.Sprintf("page-%016x", h.Sum64())
}

// FetchPage 抓取一页：先获取并发槽与限流令牌，再按退避策略重试临时失败。
// 同一页的重试复用同一幂等键，不产生重复数据或重复副作用。
func (c *Client) FetchPage(ctx context.Context, cursor string) (*Page, error) {
	const op = "FetchPage"

	// 并发槽：超限的协程在此等待，取消时及时退出。
	select {
	case c.sem <- struct{}{}:
		defer func() { <-c.sem }()
	case <-ctx.Done():
		return nil, wrap(ErrKindCanceled, op, "等待并发槽时被取消", ctx.Err())
	}

	// 限流：聚合速率不超过配置上限。
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, err
	}

	req := &Request{
		Cursor:         cursor,
		IdempotencyKey: idempotencyKey(cursor),
		Version:        c.version.String(),
	}

	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, wrap(ErrKindCanceled, op, "已取消", err)
		}

		resp, err := c.transport.Do(ctx, req)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				// 由取消/超时导致的传输失败归为 canceled。
				return nil, wrap(ErrKindCanceled, op, "传输期间被取消", ctxErr)
			}
			// 传输层异常统一归为可重试的临时失败，不向调用方透出原始类型。
			lastErr = wrap(ErrKindTransient, op, "传输失败", err)
		} else {
			var page *Page
			page, lastErr = c.handleResponse(resp)
			if lastErr == nil {
				return page, nil
			}
		}

		if !Retryable(lastErr) || attempt >= c.backoff.MaxRetries {
			return nil, lastErr
		}

		delay := c.backoff.Delay(attempt)
		if resp != nil && resp.RetryAfter > 0 {
			if ra := time.Duration(resp.RetryAfter) * time.Millisecond; ra > delay {
				delay = ra
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, wrap(ErrKindCanceled, op, "退避等待期间被取消", ctx.Err())
		case <-timer.C:
		}
	}
}

// handleResponse 将原始响应归一化为 Page 或可区分的错误。
func (c *Client) handleResponse(resp *Response) (*Page, error) {
	const op = "FetchPage"
	switch {
	case resp.Status >= 200 && resp.Status < 300:
		page, err := DecodePage(resp.Body)
		if err != nil {
			return nil, err
		}
		// 协商：响应主版本必须与客户端一致。
		if !c.version.CompatibleWith(page.Version) {
			return nil, wrap(ErrKindVersionMismatch, op,
				fmt.Sprintf("响应版本 %s 与客户端版本 %s 不兼容", page.Version, c.version), nil)
		}
		return page, nil
	case resp.Status == 429:
		return nil, wrap(ErrKindRateLimited, op, "触发限流", nil)
	case resp.Status >= 500:
		return nil, wrap(ErrKindTransient, op,
			fmt.Sprintf("服务端临时失败（状态码 %d）", resp.Status), nil)
	default:
		return nil, wrap(ErrKindUnknown, op,
			fmt.Sprintf("未预期的状态码 %d", resp.Status), nil)
	}
}

// FetchAll 从起始游标顺序抓取直到没有更多页，每页确认后回调 sink。
// 取消或超时及时终止，返回最后一次已确认提交的下一游标（已确认的进度）。
func (c *Client) FetchAll(ctx context.Context, start string, sink func(ctx context.Context, items []json.RawMessage) error) (string, error) {
	const op = "FetchAll"
	cursor := start
	for {
		page, err := c.FetchPage(ctx, cursor)
		if err != nil {
			return cursor, err
		}
		if err := sink(ctx, page.Items); err != nil {
			// 数据未确认提交，进度停留在当前游标。
			return cursor, wrap(KindOf(err), op, "提交数据失败", err)
		}
		// 先提交数据，再推进游标。
		cursor = page.Cursor
		if cursor == "" {
			return cursor, nil
		}
	}
}
