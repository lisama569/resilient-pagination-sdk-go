package sdk

import (
	"context"
	"encoding/json"
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
type Client struct {
	transport Transport
	limiter   *Limiter
	backoff   Backoff
	sem       chan struct{} // 并发上限
	version   Version
}

// Option 配置 Client。
type Option func(*Client)

// WithConcurrency 设置最大并发抓取数。
func WithConcurrency(n int) Option { return func(c *Client) {} }

// WithRateLimit 设置每秒请求速率与突发上限。
func WithRateLimit(rate float64, burst int) Option { return func(c *Client) {} }

// WithBackoff 设置重试退避策略。
func WithBackoff(b Backoff) Option { return func(c *Client) {} }

// WithVersion 设置客户端协议版本。
func WithVersion(v Version) Option { return func(c *Client) {} }

// NewClient 创建分页客户端。
func NewClient(t Transport, opts ...Option) *Client { return &Client{transport: t} }

// FetchPage 抓取一页：先获取并发槽与限流令牌，再按退避策略重试临时失败。
// 同一页的重试复用同一幂等键，不产生重复数据或重复副作用。
func (c *Client) FetchPage(ctx context.Context, cursor string) (*Page, error) {
	return nil, nil
}

// FetchAll 以配置的并发度从起始游标抓取直到没有更多页，
// 每页确认后回调 sink。取消或超时及时终止并返回已确认进度。
func (c *Client) FetchAll(ctx context.Context, start string, sink func(ctx context.Context, items []json.RawMessage) error) (string, error) {
	return "", nil
}
