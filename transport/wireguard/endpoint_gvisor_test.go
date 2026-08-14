//go:build with_gvisor

package wireguard

import (
	"context"
	"encoding/base64"
	"net/netip"
	"runtime"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
)

func TestEndpointCloseBeforeStartStopsGVisorProcessors(t *testing.T) {
	processorCountBefore := gVisorTCPProcessorCount()
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	endpoint, err := NewEndpoint(EndpointOptions{
		Context:    context.Background(),
		Logger:     log.NewNOPFactory().NewLogger("wireguard-close-test"),
		Address:    []netip.Prefix{netip.MustParsePrefix("10.0.0.2/32")},
		PrivateKey: key,
		Peers: []PeerOptions{{
			Endpoint:   M.SocksaddrFrom(netip.MustParseAddr("127.0.0.1"), 51820),
			PublicKey:  key,
			AllowedIPs: []netip.Prefix{netip.MustParsePrefix("0.0.0.0/0")},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if processorCountAfterCreate := gVisorTCPProcessorCount(); processorCountAfterCreate <= processorCountBefore {
		t.Fatalf(
			"expected creating an unstarted WireGuard endpoint to add gVisor processors: before=%d after=%d",
			processorCountBefore,
			processorCountAfterCreate,
		)
	}

	if err := endpoint.Close(); err != nil {
		t.Fatal(err)
	}
	if processorCountAfterClose := gVisorTCPProcessorCount(); processorCountAfterClose != processorCountBefore {
		t.Fatalf(
			"expected closing an unstarted WireGuard endpoint to release gVisor processors: before=%d after=%d",
			processorCountBefore,
			processorCountAfterClose,
		)
	}
}

func gVisorTCPProcessorCount() int {
	buffer := make([]byte, 4<<20)
	length := runtime.Stack(buffer, true)
	return strings.Count(
		string(buffer[:length]),
		"github.com/sagernet/gvisor/pkg/tcpip/transport/tcp.(*processor).start",
	)
}
