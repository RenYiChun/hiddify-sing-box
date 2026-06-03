package wireguard

import (
	"encoding/json"
	"net/netip"
	"testing"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
)

func TestBuildWARPAwgEndpointOptionsUsesProfileAndAwgOptions(t *testing.T) {
	config := testWARPConfig(t)
	options := option.WARPEndpointOptions{
		ListenPort: 51820,
		MTU:        1280,
		AWG: &option.AwgOptions{
			Jc: 4,
			H1: "host",
		},
	}

	awgOptions, ok := buildWARPAwgEndpointOptions(options, config, "198.51.100.7", 2408)
	if !ok {
		t.Fatal("expected WARP AWG options to be built")
	}

	if awgOptions.PrivateKey != "private-key" {
		t.Fatalf("unexpected private key: %s", awgOptions.PrivateKey)
	}
	if awgOptions.ListenPort != 51820 || awgOptions.MTU != 1280 {
		t.Fatalf("unexpected listen/mtu: %d/%d", awgOptions.ListenPort, awgOptions.MTU)
	}
	if awgOptions.Awg.Jc != 4 || awgOptions.Awg.H1 != "host" {
		t.Fatalf("unexpected AWG options: %+v", awgOptions.Awg)
	}
	if len(awgOptions.Address) != 2 ||
		awgOptions.Address[0] != netip.MustParsePrefix("172.16.0.2/32") ||
		awgOptions.Address[1] != netip.MustParsePrefix("2606:4700:110:8765::2/128") {
		t.Fatalf("unexpected addresses: %+v", awgOptions.Address)
	}
	if len(awgOptions.Peers) != 1 {
		t.Fatalf("expected one peer, got %d", len(awgOptions.Peers))
	}
	peer := awgOptions.Peers[0]
	if peer.Address != "198.51.100.7" || peer.Port != 2408 || peer.PublicKey != "peer-key" {
		t.Fatalf("unexpected peer: %+v", peer)
	}
	if len(peer.AllowedIPs) != 2 ||
		peer.AllowedIPs[0] != netip.MustParsePrefix("0.0.0.0/0") ||
		peer.AllowedIPs[1] != netip.MustParsePrefix("::/0") {
		t.Fatalf("unexpected allowed IPs: %+v", peer.AllowedIPs)
	}
}

func TestBuildWARPAwgEndpointOptionsSkipsMissingAwgOptions(t *testing.T) {
	_, ok := buildWARPAwgEndpointOptions(option.WARPEndpointOptions{}, testWARPConfig(t), "198.51.100.7", 2408)
	if ok {
		t.Fatal("empty AWG options should use the regular WireGuard WARP path")
	}
}

func TestBuildWARPWireGuardEndpointOptionsPreservesExistingBehavior(t *testing.T) {
	config := testWARPConfig(t)
	options := option.WARPEndpointOptions{
		MTU: 1280,
	}

	wireGuardOptions := buildWARPWireGuardEndpointOptions(options, config, "198.51.100.7", 2408)
	if wireGuardOptions.PrivateKey != "private-key" || wireGuardOptions.MTU != 1280 {
		t.Fatalf("unexpected WireGuard options: %+v", wireGuardOptions)
	}
	if len(wireGuardOptions.Peers) != 1 ||
		wireGuardOptions.Peers[0].Address != "198.51.100.7" ||
		wireGuardOptions.Peers[0].Port != 2408 ||
		wireGuardOptions.Peers[0].PublicKey != "peer-key" {
		t.Fatalf("unexpected WireGuard peer: %+v", wireGuardOptions.Peers)
	}
}

func testWARPConfig(t *testing.T) C.WARPConfig {
	t.Helper()

	var config C.WARPConfig
	content := []byte(`{
		"private_key": "private-key",
		"interface": {
			"addresses": {
				"v4": "172.16.0.2",
				"v6": "2606:4700:110:8765::2"
			}
		},
		"peers": [{
			"public_key": "peer-key",
			"endpoint": {
				"host": "engage.cloudflareclient.com:2408",
				"ports": [2408]
			}
		}]
	}`)
	if err := json.Unmarshal(content, &config); err != nil {
		t.Fatal(err)
	}
	return config
}
