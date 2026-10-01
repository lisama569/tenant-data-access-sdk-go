package tenantdata

import (
	"context"
	"math"
	"time"
)

// retryable 判断错误是否属于可重试的暂时性故障。
//
// 只有 KindUnavailable 才重试；权限拒绝、租户不匹配、不存在、
// 容量不足、游标损坏、策略失效都是确定性结论，绝不重试。
func retryable(err error) bool {
	return Normalize(err) == KindUnavailable
}

// retryConfig 控制有限重试行为。
type retryConfig struct {
	maxAttempts int
	baseDelay   time.Duration
}

// withRetry 对暂时性故障执行有限次重试，采用指数退避。
//
// attemptFn 接收从 1 开始的尝试序号；返回最终错误与实际尝试次数。
// 上下文取消时立即返回取消错误（归一化为 unavailable，不再重试）。
func withRetry(ctx context.Context, cfg retryConfig, attemptFn func(attempt int) error) (error, int) {
	if cfg.maxAttempts < 1 {
		cfg.maxAttempts = 1
	}
	var err error
	for attempt := 1; attempt <= cfg.maxAttempts; attempt++ {
		if err = ctx.Err(); err != nil {
			return newError(KindUnavailable, "", 0, "context cancelled before attempt %d: %v", attempt, err), attempt - 1
		}
		err = attemptFn(attempt)
		if err == nil {
			return nil, attempt
		}
		if !retryable(err) || attempt == cfg.maxAttempts {
			return err, attempt
		}
		// 指数退避：base * 2^(attempt-1)，封顶避免溢出。
		delay := cfg.baseDelay * time.Duration(math.Pow(2, float64(attempt-1)))
		if delay < 0 {
			delay = cfg.baseDelay
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return newError(KindUnavailable, "", 0, "context cancelled during backoff: %v", ctx.Err()), attempt
		case <-timer.C:
		}
	}
	return err, cfg.maxAttempts
}
