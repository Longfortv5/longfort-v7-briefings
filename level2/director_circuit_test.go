package level2

import (
	"sync/atomic"
	"testing"
)

func TestCircuitBreakerClearsOnNormalScore(t *testing.T) {
	cb := NewCircuitBreaker(nil)
	cb.InspectSonarSignal(SonarEmergencySentinel)
	if cb.AssertSafeToTrade() {
		t.Fatal("expected breaker to be tripped after sentinel")
	}
	cb.InspectSonarSignal(50)
	if !cb.AssertSafeToTrade() {
		t.Fatal("expected breaker to clear after normal score")
	}
}

func TestCircuitBreakerTripEdgeFiresOnce(t *testing.T) {
	var callCount int32
	notify := func(TripEvent) { atomic.AddInt32(&callCount, 1) }

	cb := NewCircuitBreaker(notify)

	// Send sentinel 100 times — notification must fire exactly once
	for i := 0; i < 100; i++ {
		cb.InspectSonarSignal(SonarEmergencySentinel)
	}

	// Allow goroutine to complete
	for atomic.LoadInt32(&callCount) == 0 {
		// spin — notification is async but should resolve immediately in tests
	}
	if got := atomic.LoadInt32(&callCount); got != 1 {
		t.Errorf("notify called %d times, want exactly 1", got)
	}
}

func TestCircuitBreakerExternalTrip(t *testing.T) {
	var reason string
	notify := func(e TripEvent) { reason = e.Reason }

	cb := NewCircuitBreaker(notify)
	cb.Trip("regime=PAUSED")

	if cb.AssertSafeToTrade() {
		t.Fatal("expected breaker tripped after external Trip()")
	}

	for reason == "" {
	} // wait for async notify
	if reason != "regime=PAUSED" {
		t.Errorf("TripEvent.Reason = %q, want %q", reason, "regime=PAUSED")
	}
}

func TestCircuitBreakerReset(t *testing.T) {
	cb := NewCircuitBreaker(nil)
	cb.Trip("test")
	cb.Reset()
	if !cb.AssertSafeToTrade() {
		t.Fatal("expected breaker clear after Reset()")
	}
}

func TestCircuitBreakerResetAllowsRetripNotification(t *testing.T) {
	var callCount int32
	notify := func(TripEvent) { atomic.AddInt32(&callCount, 1) }

	cb := NewCircuitBreaker(notify)

	cb.Trip("first")
	for atomic.LoadInt32(&callCount) < 1 {
	}

	cb.Reset()
	cb.Trip("second")
	for atomic.LoadInt32(&callCount) < 2 {
	}

	if got := atomic.LoadInt32(&callCount); got != 2 {
		t.Errorf("expected 2 notifications after trip-reset-trip cycle, got %d", got)
	}
}
