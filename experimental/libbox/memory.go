package libbox

import (
	"math"
	"runtime"
	runtimeDebug "runtime/debug"

	"github.com/sagernet/sing-box/common/conntrack"
)

const (
	desktopMemoryLimit = 768 * 1024 * 1024
	iosMemoryLimit     = 45 * 1024 * 1024
	androidMemoryLimit = desktopMemoryLimit
)

func SetMemoryLimit(enabled bool) {
	if enabled {
		memoryLimit := defaultMemoryLimit()
		memoryLimitGo := int64(float64(memoryLimit) / 1.5)
		runtimeDebug.SetGCPercent(10)
		runtimeDebug.SetMemoryLimit(memoryLimitGo)
		conntrack.KillerEnabled = true
		conntrack.MemoryLimit = memoryLimit
	} else {
		runtimeDebug.SetGCPercent(100)
		runtimeDebug.SetMemoryLimit(math.MaxInt64)
		conntrack.KillerEnabled = false
	}
}

func defaultMemoryLimit() uint64 {
	return memoryLimitForGOOS(runtime.GOOS)
}

func memoryLimitForGOOS(goos string) uint64 {
	switch goos {
	case "android":
		return androidMemoryLimit
	case "ios":
		return iosMemoryLimit
	default:
		return desktopMemoryLimit
	}
}
