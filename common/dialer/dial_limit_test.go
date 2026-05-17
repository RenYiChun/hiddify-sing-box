package dialer

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDefaultDialLimiterRejectsWhenAllSlotsAreBusy(t *testing.T) {
	restore := setDefaultDialLimitForTest(1)
	defer restore()

	release, err := acquireDefaultDialSlot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err = acquireDefaultDialSlot(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected second dial slot acquisition to respect context deadline, got %v", err)
	}
}

func TestDefaultDialLimiterReleasesSlot(t *testing.T) {
	restore := setDefaultDialLimitForTest(1)
	defer restore()

	release, err := acquireDefaultDialSlot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()

	secondRelease, err := acquireDefaultDialSlot(context.Background())
	if err != nil {
		t.Fatalf("expected released slot to be reusable, got %v", err)
	}
	secondRelease()
}

func setDefaultDialLimitForTest(limit int) func() {
	defaultDialLimiterMu.Lock()
	previous := defaultDialLimiter
	defaultDialLimiter = newDefaultDialLimiter(limit)
	defaultDialLimiterMu.Unlock()
	return func() {
		defaultDialLimiterMu.Lock()
		defaultDialLimiter = previous
		defaultDialLimiterMu.Unlock()
	}
}
