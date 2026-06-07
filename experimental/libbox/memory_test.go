package libbox

import (
	"runtime"
	"testing"
)

func TestDefaultMemoryLimitUsesDesktopBudgetOutsideMobile(t *testing.T) {
	switch runtime.GOOS {
	case "android":
		if got := defaultMemoryLimit(); got != desktopMemoryLimit {
			t.Fatalf("expected android memory limit %d, got %d", desktopMemoryLimit, got)
		}
	case "ios":
		if got := defaultMemoryLimit(); got != iosMemoryLimit {
			t.Fatalf("expected ios memory limit %d, got %d", iosMemoryLimit, got)
		}
	default:
		if got := defaultMemoryLimit(); got != desktopMemoryLimit {
			t.Fatalf("expected desktop memory limit %d, got %d", desktopMemoryLimit, got)
		}
	}
}

func TestMemoryLimitForGOOSUsesDesktopBudgetOnAndroid(t *testing.T) {
	if got := memoryLimitForGOOS("android"); got != desktopMemoryLimit {
		t.Fatalf("expected android memory limit %d, got %d", desktopMemoryLimit, got)
	}
	if got := memoryLimitForGOOS("ios"); got != iosMemoryLimit {
		t.Fatalf("expected ios memory limit %d, got %d", iosMemoryLimit, got)
	}
	if androidMemoryLimit != desktopMemoryLimit {
		t.Fatalf("expected android memory limit %d to match desktop limit %d", androidMemoryLimit, desktopMemoryLimit)
	}
}
