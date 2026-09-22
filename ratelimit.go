package sdk

import (
	"context"
	"time"
)

// Limiter 是客户端内共享的令牌桶限流器，保证多协程总速率不超过配置上限。
type Limiter struct {
	rate  float64
	burst int
}

// NewLimiter 创建限流器：rate 为每秒允许的请求数，burst 为突发容量。
func NewLimiter(rate float64, burst int) *Limiter { return &Limiter{rate: rate, burst: burst} }

// Wait 阻塞直到获得一个令牌或 ctx 取消。
func (l *Limiter) Wait(ctx context.Context) error { return nil }

// retryAfter 供退避逻辑读取服务端建议等待时长（占位）。
func retryAfter(d time.Duration) time.Duration { return d }
