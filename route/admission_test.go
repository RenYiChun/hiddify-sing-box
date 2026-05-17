package route

import (
	"testing"

	C "github.com/sagernet/sing-box/constant"
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
