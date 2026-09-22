package sdk

import (
	"context"
	"sync"
	"time"
)

// Limiter 是客户端内共享的令牌桶限流器，保证多协程总速率不超过配置上限。
// 零值或 rate<=0 表示不限流。
type Limiter struct {
	mu     sync.Mutex
	rate   float64   // 每秒补充的令牌数
	burst  float64   // 桶容量
	tokens float64   // 当前令牌数
	last   time.Time // 上次补充时间
	now    func() time.Time
}

// NewLimiter 创建限流器：rate 为每秒允许的请求数，burst 为突发容量。
// rate<=0 时不限流（Wait 立即返回）。
func NewLimiter(rate float64, burst int) *Limiter {
	if burst < 1 {
		burst = 1
	}
	return &Limiter{
		rate:   rate,
		burst:  float64(burst),
		tokens: float64(burst),
		now:    time.Now,
	}
}

// Wait 阻塞直到获得一个令牌或 ctx 取消。
func (l *Limiter) Wait(ctx context.Context) error {
	if l == nil || l.rate <= 0 {
		return ctx.Err()
	}
	for {
		l.mu.Lock()
		now := l.now()
		elapsed := now.Sub(l.last).Seconds()
		l.last = now
		l.tokens += elapsed * l.rate
		if l.tokens > l.burst {
			l.tokens = l.burst
		}
		if l.tokens >= 1 {
			l.tokens--
			l.mu.Unlock()
			return nil
		}
		// 计算凑齐一个令牌所需的等待时长。
		deficit := 1 - l.tokens
		wait := time.Duration(deficit / l.rate * float64(time.Second))
		l.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return wrap(ErrKindCanceled, "Limiter.Wait", "等待限流令牌时被取消", ctx.Err())
		case <-timer.C:
		}
	}
}
