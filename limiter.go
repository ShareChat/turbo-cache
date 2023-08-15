package turbocache

import "sync/atomic"

type limiter struct {
	limit    int32
	onFlight int32
}

func newLimiter(limit int32) *limiter {
	return &limiter{limit: limit, onFlight: 0}
}

func (l *limiter) Acquire(count int32) bool {
	if l.limit <= 0 {
		return true
	}
	if atomic.LoadInt32(&l.onFlight)+count > l.limit {
		return false
	}
	var result = atomic.AddInt32(&l.onFlight, count) <= l.limit
	if !result {
		atomic.AddInt32(&l.onFlight, -1*count)
	}
	return result
}

func (l *limiter) Release(count int32) {
	if l.limit > 0 {
		atomic.AddInt32(&l.onFlight, -1*count)
	}
}
