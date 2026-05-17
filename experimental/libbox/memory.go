package libbox

import (
	"math"
	"runtime"
	runtimeDebug "runtime/debug"

	"github.com/sagernet/sing-box/common/conntrack"
)

const (
	mobileMemoryLimit  = 45 * 1024 * 1024
	desktopMemoryLimit = 768 * 1024 * 1024
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
	switch runtime.GOOS {
	case "android", "ios":
		return mobileMemoryLimit
	default:
		return desktopMemoryLimit
	}
}
