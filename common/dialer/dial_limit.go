package dialer

import (
	"context"
	"sync"
)

const defaultDialConcurrencyLimit = 128

type defaultDialLimiterState struct {
	slots chan struct{}
}

var (
	defaultDialLimiterMu sync.RWMutex
	defaultDialLimiter   = newDefaultDialLimiter(defaultDialConcurrencyLimit)
)

func newDefaultDialLimiter(limit int) *defaultDialLimiterState {
	if limit < 1 {
		limit = 1
	}
	return &defaultDialLimiterState{
		slots: make(chan struct{}, limit),
	}
}

func acquireDefaultDialSlot(ctx context.Context) (func(), error) {
	defaultDialLimiterMu.RLock()
	limiter := defaultDialLimiter
	defaultDialLimiterMu.RUnlock()
	return limiter.acquire(ctx)
}

func (l *defaultDialLimiterState) acquire(ctx context.Context) (func(), error) {
	select {
	case l.slots <- struct{}{}:
		var releaseOnce sync.Once
		return func() {
			releaseOnce.Do(func() {
				<-l.slots
			})
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
