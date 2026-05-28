package level2

import (
	"context"
	"time"
)

// PriceLevel is a single resting price level in the order book.
type PriceLevel struct {
	Price  float64
	Volume float64
}

// Snapshot is a full L2 order book snapshot at a given microsecond.
type Snapshot struct {
	Timestamp time.Time
	Symbol    string
	Asks      []PriceLevel // index 0 = best ask (level 1), ascending price
	Bids      []PriceLevel // index 0 = best bid (level 1), descending price
}

// Provider is the interface any market data source must satisfy.
// Subscribe returns a channel of Snapshots and runs until ctx is cancelled.
type Provider interface {
	Subscribe(ctx context.Context, symbol string, depth int) (<-chan Snapshot, error)
	Close() error
}
