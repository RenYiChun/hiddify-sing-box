package balancer

import (
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/monitoring"
	"github.com/sagernet/sing-box/option"
	N "github.com/sagernet/sing/common/network"
)

func TestConsistentHashingOnlyInterruptsWhenEligibilityChanges(t *testing.T) {
	strategy := NewConsistentHashing([]adapter.Outbound{
		testOutbound{tag: "first", networks: []string{N.NetworkTCP}},
		testOutbound{tag: "second", networks: []string{N.NetworkTCP}},
	}, option.BalancerOutboundOptions{DelayAcceptableRatio: 2})
	if !strategy.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{
		"first": {Delay: 120}, "second": {Delay: 180},
	}) {
		t.Fatal("expected initial healthy eligibility to change selection")
	}
	if strategy.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{
		"first": {Delay: 130}, "second": {Delay: 190},
	}) {
		t.Fatal("latency-only changes must not interrupt existing connections")
	}
	if !strategy.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{
		"first": {Delay: 130}, "second": {Delay: monitoring.TimeoutDelay},
	}) {
		t.Fatal("a failed candidate must change eligibility")
	}
	if strategy.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{
		"first": {Delay: 131}, "second": {Delay: monitoring.TimeoutDelay},
	}) {
		t.Fatal("unchanged failed candidate must not interrupt again")
	}
}
