package level2

import (
	"math"
	"testing"
	"time"
)

func TestComputeAskLambda(t *testing.T) {
	snap := Snapshot{
		Timestamp: time.Now(),
		Symbol:    "NQM5",
		Asks: []PriceLevel{
			{Price: 19000.00, Volume: 10},
			{Price: 19000.25, Volume: 15},
			{Price: 19000.50, Volume: 8},
		},
		Bids: []PriceLevel{
			{Price: 18999.75, Volume: 12},
			{Price: 18999.50, Volume: 20},
			{Price: 18999.25, Volume: 5},
		},
	}

	l, err := Compute(snap, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// λ_ask = (19000.50 − 19000.00) / (10+15+8) = 0.50/33
	wantAsk := 0.50 / 33.0
	if math.Abs(l.Ask-wantAsk) > 1e-9 {
		t.Errorf("λ_ask = %.10f, want %.10f", l.Ask, wantAsk)
	}

	// λ_bid = (18999.75 − 18999.25) / (12+20+5) = 0.50/37
	wantBid := 0.50 / 37.0
	if math.Abs(l.Bid-wantBid) > 1e-9 {
		t.Errorf("λ_bid = %.10f, want %.10f", l.Bid, wantBid)
	}

	// ask thinner than bid → Λ_ratio > 1 → path of least resistance UP
	if l.Ratio <= 1.0 {
		t.Errorf("expected Λ_ratio > 1 (ask thinner), got %f", l.Ratio)
	}
}

func TestComputeInsufficientDepth(t *testing.T) {
	snap := Snapshot{
		Asks: []PriceLevel{{Price: 100, Volume: 5}},
		Bids: []PriceLevel{{Price: 99, Volume: 5}},
	}
	if _, err := Compute(snap, 5); err == nil {
		t.Fatal("expected error for insufficient depth, got nil")
	}
}

func TestComputeZeroVolume(t *testing.T) {
	snap := Snapshot{
		Asks: []PriceLevel{{Price: 100, Volume: 0}, {Price: 101, Volume: 0}},
		Bids: []PriceLevel{{Price: 99, Volume: 5}, {Price: 98, Volume: 5}},
	}
	if _, err := Compute(snap, 2); err == nil {
		t.Fatal("expected error for zero ask volume, got nil")
	}
}

func TestSymmetricBook(t *testing.T) {
	snap := Snapshot{
		Timestamp: time.Now(),
		Symbol:    "ESM5",
		Asks: []PriceLevel{
			{Price: 5000.25, Volume: 50},
			{Price: 5000.50, Volume: 50},
		},
		Bids: []PriceLevel{
			{Price: 5000.00, Volume: 50},
			{Price: 4999.75, Volume: 50},
		},
	}

	l, err := Compute(snap, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// symmetric book → Λ_ratio ≈ 1.0
	if math.Abs(l.Ratio-1.0) > 1e-9 {
		t.Errorf("symmetric book: expected Λ_ratio=1.0, got %f", l.Ratio)
	}
}

func TestRollingLambdaAskSpike(t *testing.T) {
	makeSnap := func(askSpread, bidSpread float64) Snapshot {
		return Snapshot{
			Timestamp: time.Now(),
			Symbol:    "NQM5",
			Asks: []PriceLevel{
				{Price: 19000.00, Volume: 10},
				{Price: 19000.00 + askSpread, Volume: 10},
			},
			Bids: []PriceLevel{
				{Price: 18999.75, Volume: 10},
				{Price: 18999.75 - bidSpread, Volume: 10},
			},
		}
	}

	r := NewRollingLambda(100)
	for i := 0; i < 300; i++ {
		r.Update(makeSnap(0.25, 0.25), 2)
	}

	// Normal book must not trigger
	normal, _ := Compute(makeSnap(0.25, 0.25), 2)
	if r.AskSpike(normal, 2.0) {
		t.Error("normal book should not trigger ask spike")
	}

	// Ask widened 8× should spike
	thin, _ := Compute(makeSnap(2.0, 0.25), 2)
	if !r.AskSpike(thin, 2.0) {
		t.Error("thinned ask book (8× spread) should trigger ask spike")
	}
}

func TestEvaluateSignalArmed(t *testing.T) {
	thinAskSnap := Snapshot{
		Timestamp: time.Now(),
		Symbol:    "NQM5",
		Asks: []PriceLevel{
			{Price: 19000.00, Volume: 1},
			{Price: 19001.00, Volume: 1},
		},
		Bids: []PriceLevel{
			{Price: 18999.75, Volume: 200},
			{Price: 18999.50, Volume: 200},
		},
	}

	r := NewRollingLambda(100)
	normalSnap := Snapshot{
		Timestamp: time.Now(),
		Symbol:    "NQM5",
		Asks: []PriceLevel{{Price: 19000, Volume: 50}, {Price: 19000.25, Volume: 50}},
		Bids: []PriceLevel{{Price: 18999.75, Volume: 50}, {Price: 18999.50, Volume: 50}},
	}
	for i := 0; i < 300; i++ {
		r.Update(normalSnap, 2)
	}

	sig, err := Evaluate(0.8, thinAskSnap, 2, r, DefaultConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !sig.Armed {
		t.Errorf("expected signal armed; λ_ask=%f λ_bid=%f Λ_ratio=%f",
			sig.Lambda.Ask, sig.Lambda.Bid, sig.Lambda.Ratio)
	}
}

func TestEvaluateSignalNotArmedLowOFI(t *testing.T) {
	snap := Snapshot{
		Timestamp: time.Now(),
		Symbol:    "NQM5",
		Asks: []PriceLevel{{Price: 19000, Volume: 1}, {Price: 19001, Volume: 1}},
		Bids: []PriceLevel{{Price: 18999.75, Volume: 200}, {Price: 18999.50, Volume: 200}},
	}

	r := NewRollingLambda(100)
	normalSnap := Snapshot{
		Asks: []PriceLevel{{Price: 19000, Volume: 50}, {Price: 19000.25, Volume: 50}},
		Bids: []PriceLevel{{Price: 18999.75, Volume: 50}, {Price: 18999.50, Volume: 50}},
	}
	for i := 0; i < 300; i++ {
		r.Update(normalSnap, 2)
	}

	// OFI below threshold — should not arm regardless of book structure
	sig, err := Evaluate(0.1, snap, 2, r, DefaultConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sig.Armed {
		t.Error("signal should not arm when OFI is below threshold")
	}
}
