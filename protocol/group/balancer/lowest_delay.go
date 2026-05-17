package balancer

import (
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	N "github.com/sagernet/sing/common/network"
)

type LowestDelay struct {
	outbounds        map[string][]adapter.Outbound
	udpOutbounds     []adapter.Outbound
	selectedOutbound map[string]adapter.Outbound

	mu sync.Mutex
}

func NewLowestDelay(outbounds []adapter.Outbound, options option.BalancerOutboundOptions) *LowestDelay {
	couts := convertOutbounds(outbounds)
	selected := map[string]adapter.Outbound{}
	for net, outs := range couts {
		if len(outs) > 0 {
			selected[net] = outs[0]
		}
	}
	return &LowestDelay{
		outbounds:        couts,
		selectedOutbound: selected,
	}
}

var _ Strategy = (*LowestDelay)(nil)

func (s *LowestDelay) Now() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	outbound := s.selectedOutbound[N.NetworkTCP]
	if outbound == nil {
		return ""
	}
	return outbound.Tag()
}
func (s *LowestDelay) UpdateOutboundsInfo(history map[string]*adapter.URLTestHistory) bool {
	min, _ := getMinDelay(s.outbounds, history)

	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	for net, out := range min {
		if out == nil {
			continue
		}
		current := s.selectedOutbound[net]
		if current == nil || out.Tag() != current.Tag() {
			changed = true
			s.selectedOutbound[net] = out
		}
	}
	return changed
}
func (s *LowestDelay) Select(metadata adapter.InboundContext, net string, touch bool) adapter.Outbound {
	s.mu.Lock()
	defer s.mu.Unlock()
	if net != N.NetworkTCP && net != N.NetworkUDP {
		net = N.NetworkTCP
	}
	return s.selectedOutbound[net]

}
