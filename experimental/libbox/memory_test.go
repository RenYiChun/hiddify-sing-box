package libbox

import (
	"runtime"
	"testing"
)

func TestDefaultMemoryLimitUsesDesktopBudgetOutsideMobile(t *testing.T) {
	switch runtime.GOOS {
	case "android", "ios":
		if got := defaultMemoryLimit(); got != mobileMemoryLimit {
			t.Fatalf("expected mobile memory limit %d, got %d", mobileMemoryLimit, got)
		}
	default:
		if got := defaultMemoryLimit(); got != desktopMemoryLimit {
			t.Fatalf("expected desktop memory limit %d, got %d", desktopMemoryLimit, got)
		}
	}
}
