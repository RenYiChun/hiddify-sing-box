package libbox

import (
	"path/filepath"
	"testing"
)

func TestCrashReportPathUsesDataDirectory(t *testing.T) {
	workingPath := filepath.Join("C:", "Users", "test", "Hiddify")

	got := crashReportPath(workingPath, "")
	want := filepath.Join(workingPath, "crash_reports", "pending", "CrashReport-.log")
	if got != want {
		t.Fatalf("expected crash report path %q, got %q", want, got)
	}
}
