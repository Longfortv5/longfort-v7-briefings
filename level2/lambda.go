package level2

import "fmt"

// Lambda holds the computed ask and bid impact coefficients for one Snapshot.
type Lambda struct {
	Ask   float64 // λ_ask = (P_{a,k} − P_{a,1}) / Σ V_{a,i}   i=1→k
	Bid   float64 // λ_bid = (P_{b,1} − P_{b,k}) / Σ V_{b,i}   i=1→k
	Ratio float64 // Λ_ratio = λ_ask / λ_bid; >1 means ask is thinner → path of least resistance UP
}

// Compute calculates Kyle's ex-ante Lambda from the top k levels of snap.
// k must not exceed the actual depth of the snapshot.
func Compute(snap Snapshot, k int) (Lambda, error) {
	if k <= 0 {
		return Lambda{}, fmt.Errorf("k must be >= 1, got %d", k)
	}
	if len(snap.Asks) < k {
		return Lambda{}, fmt.Errorf("ask depth %d insufficient for k=%d", len(snap.Asks), k)
	}
	if len(snap.Bids) < k {
		return Lambda{}, fmt.Errorf("bid depth %d insufficient for k=%d", len(snap.Bids), k)
	}

	ask, err := computeAsk(snap.Asks, k)
	if err != nil {
		return Lambda{}, fmt.Errorf("λ_ask: %w", err)
	}
	bid, err := computeBid(snap.Bids, k)
	if err != nil {
		return Lambda{}, fmt.Errorf("λ_bid: %w", err)
	}

	ratio := 0.0
	if bid > 0 {
		ratio = ask / bid
	}
	return Lambda{Ask: ask, Bid: bid, Ratio: ratio}, nil
}

// λ_ask = (P_{a,k} − P_{a,1}) / Σ V_{a,i}
func computeAsk(levels []PriceLevel, k int) (float64, error) {
	vol := cumulativeVolume(levels, k)
	if vol == 0 {
		return 0, fmt.Errorf("zero cumulative volume at depth %d", k)
	}
	return (levels[k-1].Price - levels[0].Price) / vol, nil
}

// λ_bid = (P_{b,1} − P_{b,k}) / Σ V_{b,i}
func computeBid(levels []PriceLevel, k int) (float64, error) {
	vol := cumulativeVolume(levels, k)
	if vol == 0 {
		return 0, fmt.Errorf("zero cumulative volume at depth %d", k)
	}
	return (levels[0].Price - levels[k-1].Price) / vol, nil
}

func cumulativeVolume(levels []PriceLevel, k int) float64 {
	total := 0.0
	for i := 0; i < k; i++ {
		total += levels[i].Volume
	}
	return total
}
