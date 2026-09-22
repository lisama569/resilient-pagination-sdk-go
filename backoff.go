package sdk

import "time"

// Backoff 定义指数退避策略（带抖动）。
type Backoff struct {
	Base       time.Duration // 首次重试等待
	Max        time.Duration // 单次等待上限
	Factor     float64       // 增长倍数
	MaxRetries int           // 最大重试次数（不含首次请求）
	Jitter     float64       // 抖动比例 [0,1]
}

// DefaultBackoff 返回默认退避策略。
func DefaultBackoff() Backoff { return Backoff{} }

// Delay 计算第 attempt 次重试（从 0 开始）的等待时长。
func (b Backoff) Delay(attempt int) time.Duration { return 0 }
