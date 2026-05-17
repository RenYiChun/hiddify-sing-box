package balancer

import (
	"context"
	"net"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/monitoring"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type testOutbound struct {
	tag      string
	networks []string
}

func (o testOutbound) Type() string           { return "test" }
func (o testOutbound) Tag() string            { return o.tag }
func (o testOutbound) Network() []string      { return o.networks }
func (o testOutbound) Dependencies() []string { return nil }
func (o testOutbound) DisplayType() string    { return "test" }
func (o testOutbound) IsReady() bool          { return true }
func (o testOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, nil
}
func (o testOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, nil
}

func TestLowestDelayBootstrapsBeforeValidHistory(t *testing.T) {
	strategy := NewLowestDelay([]adapter.Outbound{
		testOutbound{tag: "first", networks: []string{N.NetworkTCP, N.NetworkUDP}},
		testOutbound{tag: "second", networks: []string{N.NetworkTCP, N.NetworkUDP}},
	}, option.BalancerOutboundOptions{})

	if got := strategy.Now(); got != "first" {
		t.Fatalf("expected initial bootstrap selection to be first, got %q", got)
	}
	if got := strategy.Select(adapter.InboundContext{}, N.NetworkTCP, true); got == nil || got.Tag() != "first" {
		t.Fatalf("expected first bootstrap outbound before URL test succeeds, got %#v", got)
	}

	changed := strategy.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{
		"first":  {Delay: monitoring.TimeoutDelay},
		"second": {Delay: monitoring.TimeoutDelay},
	})

	if changed {
		t.Fatal("expected unchanged selection when every history is invalid")
	}
	if got := strategy.Now(); got != "first" {
		t.Fatalf("expected bootstrap selection when every history is invalid, got %q", got)
	}
}

func TestLowestDelayUsesValidHistory(t *testing.T) {
	strategy := NewLowestDelay([]adapter.Outbound{
		testOutbound{tag: "first", networks: []string{N.NetworkTCP, N.NetworkUDP}},
		testOutbound{tag: "second", networks: []string{N.NetworkTCP, N.NetworkUDP}},
	}, option.BalancerOutboundOptions{})

	changed := strategy.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{
		"first":  {Delay: 500},
		"second": {Delay: 100},
	})

	if !changed {
		t.Fatal("expected valid lower delay history to change selection")
	}
	if got := strategy.Now(); got != "second" {
		t.Fatalf("expected second to be selected, got %q", got)
	}
}

func TestRoundRobinReportsCurrentSelectionAfterValidHistory(t *testing.T) {
	strategy := NewRoundRobin([]adapter.Outbound{
		testOutbound{tag: "first", networks: []string{N.NetworkTCP, N.NetworkUDP}},
		testOutbound{tag: "second", networks: []string{N.NetworkTCP, N.NetworkUDP}},
	}, option.BalancerOutboundOptions{DelayAcceptableRatio: 2})

	strategy.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{
		"first":  {Delay: 120},
		"second": {Delay: 180},
	})

	selected := strategy.Select(adapter.InboundContext{}, N.NetworkTCP, false)
	if selected == nil {
		t.Fatal("expected round-robin to select a visible outbound after valid history")
	}
	if got := strategy.Now(); got != selected.Tag() {
		t.Fatalf("expected Now to report %q, got %q", selected.Tag(), got)
	}
}

func TestRoundRobinBootstrapsBeforeValidHistory(t *testing.T) {
	strategy := NewRoundRobin([]adapter.Outbound{
		testOutbound{tag: "first", networks: []string{N.NetworkTCP, N.NetworkUDP}},
		testOutbound{tag: "second", networks: []string{N.NetworkTCP, N.NetworkUDP}},
	}, option.BalancerOutboundOptions{DelayAcceptableRatio: 2})

	strategy.UpdateOutboundsInfo(map[string]*adapter.URLTestHistory{
		"first":  {Delay: monitoring.TimeoutDelay},
		"second": {Delay: monitoring.TimeoutDelay},
	})

	if got := strategy.Now(); got == "" {
		t.Fatal("expected visible bootstrap round-robin selection when every history is invalid")
	}
	if got := strategy.Select(adapter.InboundContext{}, N.NetworkTCP, false); got == nil {
		t.Fatal("expected selectable bootstrap round-robin outbound when every history is invalid")
	}
}
