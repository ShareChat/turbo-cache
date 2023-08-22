package turbocache

import "sync/atomic"

type limiter struct {
	limit    atomic.Int32
	onFlight atomic.Int32
}

func newLimiter(limit int32) *limiter {
	flight := atomic.Int32{}
	flight.Store(0)
	lim := atomic.Int32{}
	lim.Store(limit)
	return &limiter{limit: lim, onFlight: flight}
}

func (l *limiter) Acquire(count int32) bool {
	if l.limit.Load() <= 0 {
		return true
	}

	if l.onFlight.Load()+count > l.limit.Load() {
		return false
	}
	var result = l.onFlight.Add(count) <= l.limit.Load()
	if !result {
		l.onFlight.Add(-1 * count)
	}
	return result
}

func (l *limiter) Release(count int32) {
	if l.limit.Load() > 0 {
		l.onFlight.Add(-1 * count)
	}
}
