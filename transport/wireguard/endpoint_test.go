package wireguard

import (
	"context"
	"net"
	"net/netip"
	"os"
	"testing"

	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/wireguard-go/device"
	wgTun "github.com/sagernet/wireguard-go/tun"
)

func TestEndpointCloseClosesTunDeviceBeforeWireGuardStarts(t *testing.T) {
	tunDevice := &closeTrackingDevice{}
	endpoint := &Endpoint{tunDevice: tunDevice}

	if err := endpoint.Close(); err != nil {
		t.Fatal(err)
	}
	if tunDevice.closeCalls != 1 {
		t.Fatalf("expected unstarted TUN device to be closed once, got %d", tunDevice.closeCalls)
	}
}

type closeTrackingDevice struct {
	closeCalls int
}

func (d *closeTrackingDevice) File() *os.File { return nil }

func (d *closeTrackingDevice) Read(_ [][]byte, _ []int, _ int) (int, error) {
	return 0, os.ErrClosed
}

func (d *closeTrackingDevice) Write(_ [][]byte, _ int) (int, error) {
	return 0, os.ErrClosed
}

func (d *closeTrackingDevice) MTU() (int, error) { return 0, nil }

func (d *closeTrackingDevice) Name() (string, error) { return "test", nil }

func (d *closeTrackingDevice) Events() <-chan wgTun.Event { return nil }

func (d *closeTrackingDevice) Close() error {
	d.closeCalls++
	return nil
}

func (d *closeTrackingDevice) BatchSize() int { return 1 }

func (d *closeTrackingDevice) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, os.ErrClosed
}

func (d *closeTrackingDevice) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, os.ErrClosed
}

func (d *closeTrackingDevice) Start() error { return nil }

func (d *closeTrackingDevice) SetDevice(*device.Device) {}

func (d *closeTrackingDevice) Inet4Address() netip.Addr { return netip.Addr{} }

func (d *closeTrackingDevice) Inet6Address() netip.Addr { return netip.Addr{} }
