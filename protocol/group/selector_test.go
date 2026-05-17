package group

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

type selectorTestOutbound struct {
	tag string
}

func (o selectorTestOutbound) Type() string           { return "test" }
func (o selectorTestOutbound) Tag() string            { return o.tag }
func (o selectorTestOutbound) Network() []string      { return []string{N.NetworkTCP, N.NetworkUDP} }
func (o selectorTestOutbound) Dependencies() []string { return nil }
func (o selectorTestOutbound) DisplayType() string    { return "test" }
func (o selectorTestOutbound) IsReady() bool          { return true }
func (o selectorTestOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, nil
}
func (o selectorTestOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, nil
}

type selectorTestGroup struct {
	selectorTestOutbound
	now string
	all []string
}

func (g selectorTestGroup) Now() string   { return g.now }
func (g selectorTestGroup) All() []string { return g.all }

var _ adapter.OutboundGroup = selectorTestGroup{}

type selectorTestOutboundManager struct {
	outbounds map[string]adapter.Outbound
}

func (m selectorTestOutboundManager) Start(adapter.StartStage) error { return nil }
func (m selectorTestOutboundManager) Close() error                   { return nil }
func (m selectorTestOutboundManager) Outbounds() []adapter.Outbound  { return nil }
func (m selectorTestOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	outbound, loaded := m.outbounds[tag]
	return outbound, loaded
}
func (m selectorTestOutboundManager) Default() adapter.Outbound { return nil }
func (m selectorTestOutboundManager) Remove(string) error       { return nil }
func (m selectorTestOutboundManager) Create(context.Context, adapter.Router, log.ContextLogger, string, string, any) error {
	return nil
}

type selectorTestCacheFile struct {
	selected map[string]string
}

func (c *selectorTestCacheFile) Name() string                            { return "selector-test-cache" }
func (c *selectorTestCacheFile) Start(adapter.StartStage) error          { return nil }
func (c *selectorTestCacheFile) Close() error                            { return nil }
func (c *selectorTestCacheFile) StoreFakeIP() bool                       { return false }
func (c *selectorTestCacheFile) StoreRDRC() bool                         { return false }
func (c *selectorTestCacheFile) StoreWARPConfig() bool                   { return false }
func (c *selectorTestCacheFile) StoreDNS() bool                          { return false }
func (c *selectorTestCacheFile) FakeIPMetadata() *adapter.FakeIPMetadata { return nil }
func (c *selectorTestCacheFile) FakeIPSaveMetadata(*adapter.FakeIPMetadata) error {
	return nil
}
func (c *selectorTestCacheFile) FakeIPSaveMetadataAsync(*adapter.FakeIPMetadata) {}
func (c *selectorTestCacheFile) FakeIPStore(netip.Addr, string) error            { return nil }
func (c *selectorTestCacheFile) FakeIPStoreAsync(netip.Addr, string, logger.Logger) {
}
func (c *selectorTestCacheFile) FakeIPLoad(netip.Addr) (string, bool) {
	return "", false
}
func (c *selectorTestCacheFile) FakeIPLoadDomain(string, bool) (netip.Addr, bool) {
	return netip.Addr{}, false
}
func (c *selectorTestCacheFile) FakeIPReset() error { return nil }
func (c *selectorTestCacheFile) LoadRDRC(string, string, uint16) bool {
	return false
}
func (c *selectorTestCacheFile) SaveRDRC(string, string, uint16) error { return nil }
func (c *selectorTestCacheFile) SaveRDRCAsync(string, string, uint16, logger.Logger) {
}
func (c *selectorTestCacheFile) LoadDNSCache(string, string, uint16) ([]byte, time.Time, bool) {
	return nil, time.Time{}, false
}
func (c *selectorTestCacheFile) SaveDNSCache(string, string, uint16, []byte, time.Time) error {
	return nil
}
func (c *selectorTestCacheFile) SaveDNSCacheAsync(string, string, uint16, []byte, time.Time, logger.Logger) {
}
func (c *selectorTestCacheFile) ClearDNSCache() error               { return nil }
func (c *selectorTestCacheFile) SetDisableExpire(bool)              {}
func (c *selectorTestCacheFile) SetOptimisticTimeout(time.Duration) {}
func (c *selectorTestCacheFile) LoadMode() string                   { return "" }
func (c *selectorTestCacheFile) StoreMode(string) error {
	return nil
}
func (c *selectorTestCacheFile) LoadSelected(group string) string {
	return c.selected[group]
}
func (c *selectorTestCacheFile) StoreSelected(group string, selected string) error {
	if c.selected == nil {
		c.selected = make(map[string]string)
	}
	c.selected[group] = selected
	return nil
}
func (c *selectorTestCacheFile) LoadGroupExpand(string) (bool, bool) { return false, false }
func (c *selectorTestCacheFile) StoreGroupExpand(string, bool) error { return nil }
func (c *selectorTestCacheFile) LoadRuleSet(string) *adapter.SavedBinary {
	return nil
}
func (c *selectorTestCacheFile) SaveRuleSet(string, *adapter.SavedBinary) error {
	return nil
}
func (c *selectorTestCacheFile) LoadBinary(string) *adapter.SavedBinary {
	return nil
}
func (c *selectorTestCacheFile) SaveBinary(string, *adapter.SavedBinary) error {
	return nil
}

func newSelectorForInitialSelectionTest(t *testing.T, cached string, defaultTag string) *Selector {
	t.Helper()

	outbounds := map[string]adapter.Outbound{
		"lowest":  selectorTestGroup{selectorTestOutbound: selectorTestOutbound{tag: "lowest"}, now: "leaf-lowest"},
		"balance": selectorTestGroup{selectorTestOutbound: selectorTestOutbound{tag: "balance"}, now: "leaf-balance"},
		"leaf":    selectorTestOutbound{tag: "leaf"},
	}
	ctx := service.ContextWith[adapter.OutboundManager](context.Background(), selectorTestOutboundManager{outbounds: outbounds})
	ctx = service.ContextWith[adapter.CacheFile](ctx, &selectorTestCacheFile{
		selected: map[string]string{"select": cached},
	})

	rawSelector, err := NewSelector(ctx, nil, nil, "select", option.SelectorOutboundOptions{
		Outbounds: []string{"lowest", "balance", "leaf"},
		Default:   defaultTag,
	})
	if err != nil {
		t.Fatal(err)
	}
	selector := rawSelector.(*Selector)
	if err := selector.Start(); err != nil {
		t.Fatal(err)
	}
	return selector
}

func TestSelectorStartPrefersDefaultOverCachedFirstTag(t *testing.T) {
	selector := newSelectorForInitialSelectionTest(t, "lowest", "balance")

	if got := selector.Now(); got != "balance" {
		t.Fatalf("expected selector to use default balance instead of stale cached lowest, got %q", got)
	}
}

func TestSelectorStartPrefersDefaultOverCachedGeneratedGroup(t *testing.T) {
	selector := newSelectorForInitialSelectionTest(t, "balance", "lowest")

	if got := selector.Now(); got != "lowest" {
		t.Fatalf("expected selector to use default lowest instead of stale cached balance, got %q", got)
	}
}

func TestSelectorStartRestoresCachedLeafSelection(t *testing.T) {
	selector := newSelectorForInitialSelectionTest(t, "leaf", "balance")

	if got := selector.Now(); got != "leaf" {
		t.Fatalf("expected selector to restore cached leaf selection, got %q", got)
	}
}

func TestMonitoringTargetForSelectedGroupUsesGroupTag(t *testing.T) {
	tag, isGroup := monitoringTargetForSelected(selectorTestGroup{
		selectorTestOutbound: selectorTestOutbound{tag: "lowest"},
		now:                  "first",
		all:                  []string{"first", "second"},
	})

	if !isGroup {
		t.Fatal("expected selected outbound to be recognized as a group")
	}
	if tag != "lowest" {
		t.Fatalf("expected selected group tag to be monitored, got %q", tag)
	}
}

func TestMonitoringTargetForSelectedGroupFallsBackToGroupTag(t *testing.T) {
	tag, isGroup := monitoringTargetForSelected(selectorTestGroup{
		selectorTestOutbound: selectorTestOutbound{tag: "lowest"},
		now:                  "",
		all:                  []string{"first", "second"},
	})

	if !isGroup {
		t.Fatal("expected selected outbound to be recognized as a group")
	}
	if tag != "lowest" {
		t.Fatalf("expected selected group tag fallback to be monitored, got %q", tag)
	}
}

func TestMonitoringTargetForSelectedLeafUsesRealTag(t *testing.T) {
	tag, isGroup := monitoringTargetForSelected(selectorTestOutbound{tag: "first"})

	if isGroup {
		t.Fatal("expected leaf outbound to not be recognized as a group")
	}
	if tag != "first" {
		t.Fatalf("expected selected leaf tag to be monitored, got %q", tag)
	}
}
