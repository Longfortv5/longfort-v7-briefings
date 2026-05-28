package level2

// Reference implementation for director.CircuitBreaker.
// Copy into your private bot repo under package director.

import (
	"log/slog"
	"sync/atomic"
	"time"
)

// TripEvent is the payload delivered to NotifyFn when the breaker trips.
type TripEvent struct {
	Time   time.Time
	Reason string
	Score  int // raw sonar score if triggered by InspectSonarSignal; 0 for manual trips
}

// NotifyFn is called asynchronously when the circuit breaker trips.
// Wire in your Discord poster, QuestDB writer, PagerDuty call, etc.
type NotifyFn func(TripEvent)

// CircuitBreaker is a thread-safe execution gate.
// isPaused == 1 means all trade execution must halt.
type CircuitBreaker struct {
	isPaused int32    // accessed only via atomic ops
	notify   NotifyFn // called async on trip edge; nil = log-only
}

// NewCircuitBreaker constructs a breaker with an optional notification hook.
// Pass nil for notify to get log-only behaviour during testing.
func NewCircuitBreaker(notify NotifyFn) *CircuitBreaker {
	return &CircuitBreaker{notify: notify}
}

// InspectSonarSignal trips the breaker on the -999 emergency sentinel
// or clears it for any normal score.
func (cb *CircuitBreaker) InspectSonarSignal(score int) {
	if score == SonarEmergencySentinel {
		cb.trip("sonar emergency sentinel", score)
	} else {
		atomic.StoreInt32(&cb.isPaused, 0)
	}
}

// Trip arms the breaker externally with a named reason.
// Called by LLMDirector when regime evaluates to RegimeEmergency.
func (cb *CircuitBreaker) Trip(reason string) {
	cb.trip(reason, 0)
}

// Reset clears the breaker. Desk Governor use only — never called by the bot autonomously.
func (cb *CircuitBreaker) Reset() {
	atomic.StoreInt32(&cb.isPaused, 0)
}

// AssertSafeToTrade returns true when the circuit is clear.
// Call this at every execution gate before submitting an order.
func (cb *CircuitBreaker) AssertSafeToTrade() bool {
	return atomic.LoadInt32(&cb.isPaused) == 0
}

// SonarEmergencySentinel is the magic score value that triggers an instant halt.
const SonarEmergencySentinel = -999

// trip is the internal edge-triggered trip path.
// CAS ensures the notification fires exactly once per 0→1 transition,
// even if InspectSonarSignal is called in a tight loop with -999.
func (cb *CircuitBreaker) trip(reason string, score int) {
	if !atomic.CompareAndSwapInt32(&cb.isPaused, 0, 1) {
		return // already tripped — suppress duplicate notifications
	}

	event := TripEvent{
		Time:   time.Now().UTC(),
		Reason: reason,
		Score:  score,
	}
	slog.Error("circuit breaker tripped",
		"reason", reason,
		"score", score,
		"ts", event.Time.Format(time.RFC3339),
	)

	if cb.notify != nil {
		go cb.notify(event) // async — never block the trading hot path
	}
}
