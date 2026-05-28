package level2

// Signal is the output of the OFI + lambda confluence check.
type Signal struct {
	Armed    bool    // true = structural weakness confirmed, ready to fire
	OFI      float64 // order flow imbalance at signal time
	Lambda   Lambda  // instantaneous lambda at signal time
	LongEWMA Lambda  // 10 s EWMA baseline at signal time
}

// SignalConfig holds the thresholds for the armed-signal gate.
type SignalConfig struct {
	OFIThreshold    float64 // minimum positive OFI to qualify, e.g. 0.3
	SpikeMultiplier float64 // λ_ask must be ≥ this × 10 s EWMA, e.g. 2.0
	RatioThreshold  float64 // Λ_ratio must exceed this to confirm ask thinness, e.g. 1.0
}

// DefaultConfig returns the standard signal gate configuration.
func DefaultConfig() SignalConfig {
	return SignalConfig{
		OFIThreshold:    0.3,
		SpikeMultiplier: 2.0,
		RatioThreshold:  1.0,
	}
}

// Evaluate checks whether OFI and lambda conditions jointly arm the signal.
// ofi is a normalised [-1, +1] order flow imbalance; positive = net aggressive buying.
func Evaluate(ofi float64, snap Snapshot, k int, rolling *RollingLambda, cfg SignalConfig) (Signal, error) {
	l, err := Compute(snap, k)
	if err != nil {
		return Signal{}, err
	}
	_, longEWMA := rolling.Values()

	armed := ofi >= cfg.OFIThreshold &&
		l.Ratio > cfg.RatioThreshold &&
		rolling.AskSpike(l, cfg.SpikeMultiplier)

	return Signal{
		Armed:    armed,
		OFI:      ofi,
		Lambda:   l,
		LongEWMA: longEWMA,
	}, nil
}
