package route

import (
	"net"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/common/control"
)

func TestSelectOutboundDefaultInterfaceSkipsOwnTun(t *testing.T) {
	selected := selectOutboundDefaultInterface(
		&control.Interface{Name: "sing-tun", Index: 64},
		[]adapter.NetworkInterface{
			{Interface: control.Interface{Name: "sing-tun", Index: 64}, Type: C.InterfaceTypeOther},
			{Interface: control.Interface{Name: "Wi-Fi", Index: 22}, Type: C.InterfaceTypeWIFI},
			{Interface: control.Interface{Name: "Ethernet", Index: 30}, Type: C.InterfaceTypeEthernet},
		},
		"sing-tun",
	)

	if selected == nil {
		t.Fatal("expected a non-tun interface")
	}
	if selected.Name != "Wi-Fi" || selected.Index != 22 {
		t.Fatalf("expected Wi-Fi to be selected, got %s/%d", selected.Name, selected.Index)
	}
}

func TestNetworkInterfacesFromControlInterfacesAllowsFallbackWithoutPlatformInterfaces(t *testing.T) {
	interfaces := networkInterfacesFromControlInterfaces([]control.Interface{
		{Name: "sing-tun", Index: 64, Flags: net.FlagUp},
		{Name: "WLAN", Index: 22, Flags: net.FlagUp},
	})
	selected := selectOutboundDefaultInterface(
		&control.Interface{Name: "sing-tun", Index: 64},
		interfaces,
		"sing-tun",
	)

	if selected == nil {
		t.Fatal("expected WLAN fallback interface")
	}
	if selected.Name != "WLAN" || selected.Index != 22 {
		t.Fatalf("expected WLAN to be selected, got %s/%d", selected.Name, selected.Index)
	}
}

func TestSelectOutboundDefaultInterfaceKeepsNonTunDefault(t *testing.T) {
	selected := selectOutboundDefaultInterface(
		&control.Interface{Name: "Wi-Fi", Index: 22},
		[]adapter.NetworkInterface{
			{Interface: control.Interface{Name: "Ethernet", Index: 30}, Type: C.InterfaceTypeEthernet},
		},
		"sing-tun",
	)

	if selected == nil {
		t.Fatal("expected default interface")
	}
	if selected.Name != "Wi-Fi" || selected.Index != 22 {
		t.Fatalf("expected default Wi-Fi to be kept, got %s/%d", selected.Name, selected.Index)
	}
}

func TestSelectOutboundDefaultInterfaceReturnsNilForOnlyOwnTun(t *testing.T) {
	selected := selectOutboundDefaultInterface(
		&control.Interface{Name: "sing-tun", Index: 64},
		[]adapter.NetworkInterface{
			{Interface: control.Interface{Name: "sing-tun", Index: 64}, Type: C.InterfaceTypeOther},
		},
		"sing-tun",
	)

	if selected != nil {
		t.Fatalf("expected no fallback interface, got %s/%d", selected.Name, selected.Index)
	}
}
