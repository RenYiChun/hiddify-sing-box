package route

import (
	"sync"
	"sync/atomic"

	C "github.com/sagernet/sing-box/constant"
	E "github.com/sagernet/sing/common/exceptions"
)

const (
	DefaultDirectRouteConnectionAdmissionLimit = 1024
	DefaultProxyRouteConnectionAdmissionLimit  = 256
)

var (
	routeConnectionAdmissionMu sync.RWMutex
	routeConnectionAdmission   = newRouteConnectionAdmissionSet(
		DefaultDirectRouteConnectionAdmissionLimit,
		DefaultProxyRouteConnectionAdmissionLimit,
	)
)

func SetRouteConnectionAdmissionLimits(directLimit int, proxyLimit int) {
	routeConnectionAdmissionMu.Lock()
	defer routeConnectionAdmissionMu.Unlock()
	routeConnectionAdmission = newRouteConnectionAdmissionSet(directLimit, proxyLimit)
}

func RouteConnectionAdmissionLimits() (directLimit int, proxyLimit int) {
	routeConnectionAdmissionMu.RLock()
	defer routeConnectionAdmissionMu.RUnlock()
	return routeConnectionAdmission.direct.limit(), routeConnectionAdmission.proxy.limit()
}

func acquireRouteConnectionAdmissionForOutboundType(outboundType string) (func(error), error) {
	routeConnectionAdmissionMu.RLock()
	admission := routeConnectionAdmission
	routeConnectionAdmissionMu.RUnlock()
	return admission.acquireForOutboundType(outboundType)
}

type routeConnectionAdmissionSet struct {
	direct *routeConnectionAdmissionLimiter
	proxy  *routeConnectionAdmissionLimiter
}

func newRouteConnectionAdmissionSet(directLimit int, proxyLimit int) *routeConnectionAdmissionSet {
	return &routeConnectionAdmissionSet{
		direct: newRouteConnectionAdmissionLimiter(directLimit),
		proxy:  newRouteConnectionAdmissionLimiter(proxyLimit),
	}
}

func (a *routeConnectionAdmissionSet) acquireForOutboundType(outboundType string) (func(error), error) {
	if outboundType == C.TypeDirect {
		return a.direct.acquire("direct")
	}
	return a.proxy.acquire("proxy")
}

type routeConnectionAdmissionLimiter struct {
	slots     chan struct{}
	active    atomic.Int64
	highWater atomic.Int64
	rejected  atomic.Uint64
}

func newRouteConnectionAdmissionLimiter(limit int) *routeConnectionAdmissionLimiter {
	if limit < 1 {
		limit = 1
	}
	return &routeConnectionAdmissionLimiter{
		slots: make(chan struct{}, limit),
	}
}

func (l *routeConnectionAdmissionLimiter) limit() int {
	return cap(l.slots)
}

func (l *routeConnectionAdmissionLimiter) acquire(name string) (func(error), error) {
	select {
	case l.slots <- struct{}{}:
		active := l.active.Add(1)
		l.recordHighWater(active)
		var releaseOnce sync.Once
		return func(error) {
			releaseOnce.Do(func() {
				<-l.slots
				l.active.Add(-1)
			})
		}, nil
	default:
		rejected := l.rejected.Add(1)
		return nil, E.New(
			"route ", name, " connection limit exceeded: active=",
			l.active.Load(), "/", l.limit(),
			", high_water=", l.highWater.Load(),
			", rejected=", rejected,
		)
	}
}

func (l *routeConnectionAdmissionLimiter) recordHighWater(active int64) {
	for {
		current := l.highWater.Load()
		if active <= current || l.highWater.CompareAndSwap(current, active) {
			return
		}
	}
}
