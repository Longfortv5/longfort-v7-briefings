package level2

// Tick is the normalised Level 1 top-of-book unit produced by the Databento
// MBP-1 stream (and any other L1 provider). It carries enough data to:
//   - Write to QuestDB (mid price, spread, imbalance)
//   - Feed RollingLambda (k=1, lambda will be 0 — L1 limitation documented below)
//   - Compute top-of-book order flow imbalance (a valid L1 signal)
//
// LAMBDA NOTE: Kyle's ex-ante Lambda requires k≥2 price levels to produce
// a non-zero result. With mbp-1 (k=1), Compute() always returns Lambda{0,0,0}
// because P_{a,k} - P_{a,1} = 0. The Tick pipeline is fully wired so that
// switching to the MBP-10 schema (and k=10) in DatabentoCfg.Schema is the
// only change needed to get non-trivial lambda values.

import (
	"context"
	"log/slog"
	"time"
)

// Tick is a single top-of-book event from an L1 data source.
type Tick struct {
	Timestamp time.Time
	Symbol    string
	BidPrice  float64
	BidSize   float64
	AskPrice  float64
	AskSize   float64
}

// Mid returns the bid-ask midpoint.
func (t Tick) Mid() float64 { return (t.BidPrice + t.AskPrice) / 2.0 }

// Spread returns the absolute bid-ask spread.
func (t Tick) Spread() float64 { return t.AskPrice - t.BidPrice }

// Imbalance returns top-of-book order flow imbalance in [-1, +1].
// +1 = all size on bid, -1 = all size on ask.
func (t Tick) Imbalance() float64 {
	total := t.BidSize + t.AskSize
	if total == 0 {
		return 0
	}
	return (t.BidSize - t.AskSize) / total
}

// ToSnapshot wraps a Tick into a 1-level Snapshot for the Lambda pipeline.
func (t Tick) ToSnapshot() Snapshot {
	return Snapshot{
		Timestamp: t.Timestamp,
		Symbol:    t.Symbol,
		Bids:      []PriceLevel{{Price: t.BidPrice, Volume: t.BidSize}},
		Asks:      []PriceLevel{{Price: t.AskPrice, Volume: t.AskSize}},
	}
}

// SnapshotToTick extracts a Tick from the top level of a Snapshot.
func SnapshotToTick(s Snapshot) Tick {
	t := Tick{Timestamp: s.Timestamp, Symbol: s.Symbol}
	if len(s.Bids) > 0 {
		t.BidPrice = s.Bids[0].Price
		t.BidSize = s.Bids[0].Volume
	}
	if len(s.Asks) > 0 {
		t.AskPrice = s.Asks[0].Price
		t.AskSize = s.Asks[0].Volume
	}
	return t
}

// TickSignal is the output of RunTickLambdaPipeline for each incoming Tick.
type TickSignal struct {
	Tick       Tick
	Lambda     Lambda  // always {0,0,0} for mbp-1; non-zero when MBP-10 wired
	ShortEWMA  Lambda  // 1s EWMA
	LongEWMA   Lambda  // 10s EWMA
	Imbalance  float64 // top-of-book OFI proxy
	Signal     Signal  // OFI + lambda armed gate
}

// TickSignalCh is a read-only channel of TickSignals.
type TickSignalCh <-chan TickSignal

// RunTickLambdaPipeline consumes Snapshots from the Databento stream,
// runs them through RollingLambda and the signal gate, and emits TickSignals.
// It also writes each row to QuestDB and optionally merges GEX data from gexCh.
//
// Use depth=1 for mbp-1 (lambda=0), depth=10 for mbp-10 (full lambda).
func RunTickLambdaPipeline(
	ctx context.Context,
	snapCh <-chan Snapshot,
	gexCh <-chan GEXData,
	writer *QuestDBWriter,
	depth int,
	ticksPerSecond float64,
	cfg SignalConfig,
) TickSignalCh {
	out := make(chan TickSignal, 512)
	rolling := NewRollingLambda(ticksPerSecond)

	go func() {
		defer close(out)
		var lastGEX GEXData

		for {
			select {
			case <-ctx.Done():
				return

			case gex, ok := <-gexCh:
				if ok {
					lastGEX = gex
				}

			case snap, ok := <-snapCh:
				if !ok {
					return
				}
				if !IsGlobalSystemAvailable(snap.Timestamp) {
					continue
				}

				short, long, err := rolling.Update(snap, depth)
				if err != nil {
					continue // depth < k; skip until book has enough levels
				}
				l, err := Compute(snap, depth)
				if err != nil {
					continue
				}

				tick := SnapshotToTick(snap)
				ofi := tick.Imbalance() // top-of-book imbalance as OFI proxy

				sig, err := Evaluate(ofi, snap, depth, rolling, cfg)
				if err != nil {
					continue
				}

				state := GetAssetSessionState(snap.Symbol, snap.Timestamp)

				// Write full row to QuestDB
				if writer != nil {
					if err := writer.WriteSnapshot(
						snap.Symbol, snap, l, short, long, sig, state, depth, lastGEX,
					); err != nil {
						slog.Warn("tick qdb write error", "symbol", snap.Symbol, "err", err)
					}
				}

				ts := TickSignal{
					Tick:      tick,
					Lambda:    l,
					ShortEWMA: short,
					LongEWMA:  long,
					Imbalance: ofi,
					Signal:    sig,
				}

				select {
				case out <- ts:
				default: // drop if downstream is blocked
				}
			}
		}
	}()

	return out
}
