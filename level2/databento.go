package level2

import (
	"context"
	"fmt"
)

// DatabentoCfg holds connection parameters for the Databento streaming API.
type DatabentoCfg struct {
	APIKey  string // Databento API key
	Dataset string // e.g. "GLBX.MDP3" for CME Globex
	Schema  string // use "mbp-10" for 10-level market-by-price
}

// DatabentoProvider adapts a Databento MBP-10 stream to the Provider interface.
// Wire up with the official Databento Go client: github.com/databento/dbn-go
type DatabentoProvider struct {
	cfg DatabentoCfg
	// client *dbn.LiveClient  // uncomment once databento/dbn-go is a module dependency
}

// NewDatabentoProvider creates a Databento provider with the given config.
func NewDatabentoProvider(cfg DatabentoCfg) *DatabentoProvider {
	return &DatabentoProvider{cfg: cfg}
}

// Subscribe opens an MBP-10 stream for symbol and emits Snapshots on the returned channel.
// Replace the stub body with the real dbn.LiveClient subscription loop.
//
// Databento MBP-10 field mapping:
//
//	rec.BidPx[i]  (int64, fixed-point ×1e-9) → Bids[i].Price
//	rec.BidSz[i]  (uint32)                    → Bids[i].Volume
//	rec.AskPx[i]  (int64, fixed-point ×1e-9)  → Asks[i].Price
//	rec.AskSz[i]  (uint32)                    → Asks[i].Volume
//	rec.TsEvent   (uint64 ns UNIX)             → Snapshot.Timestamp
func (d *DatabentoProvider) Subscribe(ctx context.Context, symbol string, depth int) (<-chan Snapshot, error) {
	if depth > 10 {
		return nil, fmt.Errorf("Databento MBP-10 caps depth at 10; requested %d", depth)
	}
	ch := make(chan Snapshot, 256)

	go func() {
		defer close(ch)
		// TODO: initialise dbn.LiveClient, subscribe to symbol with schema "mbp-10",
		// then loop: decode each MBP10Msg, map fields above → Snapshot, send on ch.
		<-ctx.Done()
	}()

	return ch, nil
}

func (d *DatabentoProvider) Close() error { return nil }
