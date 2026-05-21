package route

import (
	"net/netip"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

func TestOverrideSniffDestinationUsesSniffedDomain(t *testing.T) {
	metadata := &adapter.InboundContext{
		Destination: M.Socksaddr{
			Addr: netip.MustParseAddr("108.160.162.104"),
			Port: 443,
		},
		Domain: "chatgpt.com",
	}

	if !overrideSniffDestination(metadata) {
		t.Fatal("expected destination override")
	}
	if metadata.Destination.Fqdn != "chatgpt.com" {
		t.Fatalf("expected destination fqdn chatgpt.com, got %q", metadata.Destination.Fqdn)
	}
	if metadata.Destination.Port != 443 {
		t.Fatalf("expected destination port 443, got %d", metadata.Destination.Port)
	}
	if len(metadata.DestinationAddresses) != 0 {
		t.Fatalf("expected destination addresses to be cleared, got %#v", metadata.DestinationAddresses)
	}
}

func TestOverrideSniffDestinationKeepsExistingDomainDestination(t *testing.T) {
	metadata := &adapter.InboundContext{
		Destination: M.Socksaddr{
			Fqdn: "example.com",
			Port: 443,
		},
		Domain: "chatgpt.com",
	}

	if overrideSniffDestination(metadata) {
		t.Fatal("expected no override for existing domain destination")
	}
	if metadata.Destination.Fqdn != "example.com" {
		t.Fatalf("expected existing destination to be kept, got %q", metadata.Destination.Fqdn)
	}
}
