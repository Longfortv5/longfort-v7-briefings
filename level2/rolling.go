package level2

import "sync"

// RollingLambda maintains EWMA-smoothed lambda estimates over a stream of Snapshots.
// It tracks two windows — short (~1 s) and long (~10 s) — to distinguish transient
// spikes from structural thinning.
type RollingLambda struct {
	mu          sync.RWMutex
	short       Lambda
	long        Lambda
	alphaShort  float64 // EWMA decay for the short window
	alphaLong   float64 // EWMA decay for the long window
	initialized bool
}

// NewRollingLambda creates a tracker for a feed running at ticksPerSecond updates/s.
// EWMA decay: α = 2 / (N + 1), where N is the window expressed in ticks.
func NewRollingLambda(ticksPerSecond float64) *RollingLambda {
	return &RollingLambda{
		alphaShort: 2.0 / (ticksPerSecond*1 + 1),
		alphaLong:  2.0 / (ticksPerSecond*10 + 1),
	}
}

// Update ingests snap and returns the current short and long EWMA Lambda values.
func (r *RollingLambda) Update(snap Snapshot, k int) (short, long Lambda, err error) {
	l, err := Compute(snap, k)
	if err != nil {
		return Lambda{}, Lambda{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.initialized {
		r.short = l
		r.long = l
		r.initialized = true
	} else {
		r.short = ewmaStep(r.short, l, r.alphaShort)
		r.long = ewmaStep(r.long, l, r.alphaLong)
	}

	return r.short, r.long, nil
}

// Values returns the current short and long EWMA snapshots without updating.
func (r *RollingLambda) Values() (short, long Lambda) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.short, r.long
}

// AskSpike returns true when current.Ask >= multiplier × the long EWMA baseline.
// Use multiplier = 2.0 as the standard alert threshold.
func (r *RollingLambda) AskSpike(current Lambda, multiplier float64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.initialized || r.long.Ask == 0 {
		return false
	}
	return current.Ask >= r.long.Ask*multiplier
}

func ewmaStep(prev, cur Lambda, alpha float64) Lambda {
	inv := 1 - alpha
	return Lambda{
		Ask:   alpha*cur.Ask + inv*prev.Ask,
		Bid:   alpha*cur.Bid + inv*prev.Bid,
		Ratio: alpha*cur.Ratio + inv*prev.Ratio,
	}
}
