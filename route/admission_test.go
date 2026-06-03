package route

import (
	"testing"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	M "github.com/sagernet/sing/common/metadata"
)

func TestDefaultDirectRouteAdmissionLimitSupportsTunBursts(t *testing.T) {
	const expectedDirectBurst = DefaultDirectRouteConnectionAdmissionLimit

	admission := newRouteConnectionAdmissionSet(
		DefaultDirectRouteConnectionAdmissionLimit,
		DefaultProxyRouteConnectionAdmissionLimit,
	)
	releases := make([]func(error), 0, expectedDirectBurst)
	defer func() {
		for _, release := range releases {
			release(nil)
		}
	}()

	for i := 0; i < expectedDirectBurst; i++ {
		release, err := admission.acquireForOutboundType(C.TypeDirect)
		if err != nil {
			t.Fatalf("direct route admission rejected connection %d/%d: %v", i+1, expectedDirectBurst, err)
		}
		releases = append(releases, release)
	}
}

func TestDefaultProxyRouteAdmissionLimitSupportsStoreDownloadBursts(t *testing.T) {
	const expectedProxyBurst = DefaultProxyRouteConnectionAdmissionLimit

	admission := newRouteConnectionAdmissionSet(
		DefaultDirectRouteConnectionAdmissionLimit,
		DefaultProxyRouteConnectionAdmissionLimit,
	)
	releases := make([]func(error), 0, expectedProxyBurst)
	defer func() {
		for _, release := range releases {
			release(nil)
		}
	}()

	for i := 0; i < expectedProxyBurst; i++ {
		release, err := admission.acquireForOutboundType(C.TypeVLESS)
		if err != nil {
			t.Fatalf("proxy route admission rejected connection %d/%d: %v", i+1, expectedProxyBurst, err)
		}
		releases = append(releases, release)
	}
}

func TestSetRouteConnectionAdmissionLimits(t *testing.T) {
	SetRouteConnectionAdmissionLimits(DefaultDirectRouteConnectionAdmissionLimit, DefaultProxyRouteConnectionAdmissionLimit)
	defer SetRouteConnectionAdmissionLimits(DefaultDirectRouteConnectionAdmissionLimit, DefaultProxyRouteConnectionAdmissionLimit)

	SetRouteConnectionAdmissionLimits(2, 1)
	directLimit, proxyLimit := RouteConnectionAdmissionLimits()
	if directLimit != 2 || proxyLimit != 1 {
		t.Fatalf("unexpected route admission limits: direct=%d proxy=%d", directLimit, proxyLimit)
	}

	directRelease1, err := acquireRouteConnectionAdmissionForOutboundType(C.TypeDirect)
	if err != nil {
		t.Fatal(err)
	}
	defer directRelease1(nil)
	directRelease2, err := acquireRouteConnectionAdmissionForOutboundType(C.TypeDirect)
	if err != nil {
		t.Fatal(err)
	}
	defer directRelease2(nil)
	if _, err := acquireRouteConnectionAdmissionForOutboundType(C.TypeDirect); err == nil {
		t.Fatal("expected direct route admission to reject over limit")
	}

	proxyRelease, err := acquireRouteConnectionAdmissionForOutboundType(C.TypeVLESS)
	if err != nil {
		t.Fatal(err)
	}
	defer proxyRelease(nil)
	if _, err := acquireRouteConnectionAdmissionForOutboundType(C.TypeVLESS); err == nil {
		t.Fatal("expected proxy route admission to reject over limit")
	}
}

func TestDirectRouteAdmissionLimitsSingleDestination(t *testing.T) {
	admission := newRouteConnectionAdmissionSetWithDirectKeyLimit(10, 10, 2)
	metadata := adapter.InboundContext{
		Domain: "cube.weixinbridge.com",
		Destination: M.Socksaddr{
			Fqdn: "cube.weixinbridge.com",
			Port: 443,
		},
	}

	release1, err := admission.acquireForOutbound(C.TypeDirect, metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer release1(nil)
	release2, err := admission.acquireForOutbound(C.TypeDirect, metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer release2(nil)
	if _, err := admission.acquireForOutbound(C.TypeDirect, metadata); err == nil {
		t.Fatal("expected same direct destination to be rejected over per-destination limit")
	}

	otherRelease, err := admission.acquireForOutbound(C.TypeDirect, adapter.InboundContext{
		Domain: "work.weixin.qq.com",
		Destination: M.Socksaddr{
			Fqdn: "work.weixin.qq.com",
			Port: 443,
		},
	})
	if err != nil {
		t.Fatalf("expected a different direct destination to be admitted: %v", err)
	}
	defer otherRelease(nil)
}

func TestDirectRouteAdmissionKeyLimitScalesWithTotalLimit(t *testing.T) {
	tests := []struct {
		name        string
		directLimit int
		want        int
	}{
		{name: "default", directLimit: DefaultDirectRouteConnectionAdmissionLimit, want: 512},
		{name: "configured high", directLimit: 2048, want: 1024},
		{name: "configured low", directLimit: 64, want: 64},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := defaultDirectRouteConnectionAdmissionKeyLimit(testCase.directLimit); got != testCase.want {
				t.Fatalf("unexpected direct route per-destination limit: got %d want %d", got, testCase.want)
			}
		})
	}
}
