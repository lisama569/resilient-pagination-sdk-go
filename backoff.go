package sdk

import (
	"math"
	"math/rand"
	"time"
)

// Backoff 定义指数退避策略（带抖动）。
type Backoff struct {
	Base       time.Duration // 首次重试等待
	Max        time.Duration // 单次等待上限
	Factor     float64       // 增长倍数
	MaxRetries int           // 最大重试次数（不含首次请求）
	Jitter     float64       // 抖动比例 [0,1]

	rand *rand.Rand // 可注入的随机源；nil 时使用默认源
}

// DefaultBackoff 返回默认退避策略。
func DefaultBackoff() Backoff {
	return Backoff{
		Base:       50 * time.Millisecond,
		Max:        2 * time.Second,
		Factor:     2.0,
		MaxRetries: 5,
		Jitter:     0.2,
	}
}

// WithRand 返回使用指定随机源的策略副本（测试可注入确定性源）。
func (b Backoff) WithRand(r *rand.Rand) Backoff {
	b.rand = r
	return b
}

// Delay 计算第 attempt 次重试（从 0 开始）的等待时长：
// Base * Factor^attempt，封顶 Max，再叠加 ±Jitter 比例的对称抖动。
func (b Backoff) Delay(attempt int) time.Duration {
	base := b.Base
	if base <= 0 {
		base = 50 * time.Millisecond
	}
	max := b.Max
	if max <= 0 {
		max = 2 * time.Second
	}
	factor := b.Factor
	if factor < 1 {
		factor = 2.0
	}
	d := float64(base) * math.Pow(factor, float64(attempt))
	if d > float64(max) {
		d = float64(max)
	}
	jitter := b.Jitter
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	if jitter > 0 {
		var r float64
		if b.rand != nil {
			r = b.rand.Float64()
		} else {
			r = rand.Float64()
		}
		// 在 [1-jitter, 1+jitter] 区间内缩放。
		d *= 1 + jitter*(2*r-1)
	}
	if d < 0 {
		d = 0
	}
	return time.Duration(d)
}
