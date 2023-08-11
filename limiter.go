package turbocache

import "sync/atomic"

type limiter struct {
	limit    int32
	onFlight int32
}

func newLimiter(limit int32) *limiter {
	return &limiter{limit: limit, onFlight: 0}
}

func (l *limiter) Do(limitingAction func(), number int32) bool {
	if l == nil {
		limitingAction()
		return true
	}

	return l.do(limitingAction, number)
}

func (l *limiter) do(limitingAction func(), number int32) bool {
	defer atomic.AddInt32(&l.onFlight, -1*number)
	if atomic.AddInt32(&l.onFlight, number) < l.limit {
		limitingAction()
		return true
	} else {
		return false
	}
}
