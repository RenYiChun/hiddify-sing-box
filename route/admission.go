package route

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	E "github.com/sagernet/sing/common/exceptions"
)

const (
	DefaultDirectRouteConnectionAdmissionLimit = 1024
	DefaultProxyRouteConnectionAdmissionLimit  = 512
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

func acquireRouteConnectionAdmissionForOutbound(outboundType string, metadata adapter.InboundContext) (func(error), error) {
	routeConnectionAdmissionMu.RLock()
	admission := routeConnectionAdmission
	routeConnectionAdmissionMu.RUnlock()
	return admission.acquireForOutbound(outboundType, metadata)
}

type routeConnectionAdmissionSet struct {
	direct       *routeConnectionAdmissionLimiter
	proxy        *routeConnectionAdmissionLimiter
	directPerKey *keyedRouteConnectionAdmissionLimiter
}

func newRouteConnectionAdmissionSet(directLimit int, proxyLimit int) *routeConnectionAdmissionSet {
	return newRouteConnectionAdmissionSetWithDirectKeyLimit(
		directLimit,
		proxyLimit,
		defaultDirectRouteConnectionAdmissionKeyLimit(directLimit),
	)
}

func newRouteConnectionAdmissionSetWithDirectKeyLimit(directLimit int, proxyLimit int, directKeyLimit int) *routeConnectionAdmissionSet {
	return &routeConnectionAdmissionSet{
		direct:       newRouteConnectionAdmissionLimiter(directLimit),
		proxy:        newRouteConnectionAdmissionLimiter(proxyLimit),
		directPerKey: newKeyedRouteConnectionAdmissionLimiter(directKeyLimit),
	}
}

func (a *routeConnectionAdmissionSet) acquireForOutboundType(outboundType string) (func(error), error) {
	if outboundType == C.TypeDirect {
		return a.direct.acquire("direct")
	}
	return a.proxy.acquire("proxy")
}

func (a *routeConnectionAdmissionSet) acquireForOutbound(outboundType string, metadata adapter.InboundContext) (func(error), error) {
	if outboundType == C.TypeDirect {
		releaseGlobal, err := a.direct.acquire("direct")
		if err != nil {
			return nil, err
		}
		releaseKey, err := a.directPerKey.acquire(routeConnectionAdmissionKey(metadata))
		if err != nil {
			releaseGlobal(err)
			return nil, err
		}
		var releaseOnce sync.Once
		return func(err error) {
			releaseOnce.Do(func() {
				releaseKey(err)
				releaseGlobal(err)
			})
		}, nil
	}
	return a.proxy.acquire("proxy")
}

func defaultDirectRouteConnectionAdmissionKeyLimit(directLimit int) int {
	if directLimit < 1 {
		directLimit = DefaultDirectRouteConnectionAdmissionLimit
	}
	limit := directLimit / 2
	if limit < 128 {
		limit = 128
	}
	if limit > 1024 {
		limit = 1024
	}
	if limit > directLimit {
		return directLimit
	}
	return limit
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

type keyedRouteConnectionAdmissionLimiter struct {
	limit    int
	access   sync.Mutex
	active   map[string]int
	rejected atomic.Uint64
}

func newKeyedRouteConnectionAdmissionLimiter(limit int) *keyedRouteConnectionAdmissionLimiter {
	if limit < 1 {
		limit = 1
	}
	return &keyedRouteConnectionAdmissionLimiter{
		limit:  limit,
		active: map[string]int{},
	}
}

func (l *keyedRouteConnectionAdmissionLimiter) acquire(key string) (func(error), error) {
	if key == "" {
		key = "unknown"
	}
	l.access.Lock()
	active := l.active[key]
	if active >= l.limit {
		rejected := l.rejected.Add(1)
		l.access.Unlock()
		return nil, E.New(
			"route direct destination connection limit exceeded: key=",
			key,
			", active=", active, "/", l.limit,
			", rejected=", rejected,
		)
	}
	l.active[key] = active + 1
	l.access.Unlock()
	var releaseOnce sync.Once
	return func(error) {
		releaseOnce.Do(func() {
			l.access.Lock()
			defer l.access.Unlock()
			next := l.active[key] - 1
			if next <= 0 {
				delete(l.active, key)
				return
			}
			l.active[key] = next
		})
	}, nil
}

func routeConnectionAdmissionKey(metadata adapter.InboundContext) string {
	if metadata.Domain != "" {
		return "domain:" + strings.ToLower(strings.TrimSuffix(metadata.Domain, "."))
	}
	if metadata.Destination.Fqdn != "" {
		return "domain:" + strings.ToLower(strings.TrimSuffix(metadata.Destination.Fqdn, "."))
	}
	if metadata.Destination.Addr.IsValid() {
		return "ip:" + metadata.Destination.Addr.String()
	}
	return "unknown"
}
