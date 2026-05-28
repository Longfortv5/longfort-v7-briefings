package level2

import (
	"context"
	"fmt"
)

// RithmicCfg holds connection parameters for the Rithmic R|API+ gateway.
type RithmicCfg struct {
	GatewayHost string // e.g. "rituz00100.01.rithmic.com:443"
	SystemName  string // e.g. "Rithmic 01 Chicago"
	UserID      string
	Password    string
	FCMID       string
	IBID        string
}

// RithmicProvider adapts a Rithmic R|API+ depth feed to the Provider interface.
//
// R|API+ is a C++ library. This adapter assumes a sidecar gRPC bridge
// translates SBestBidOfferCallback events into the RithmicDepthEvent type below.
// A minimal bridge can be built with the Rithmic protobuf definitions and cgo,
// or as a separate process communicating over a local Unix socket.
type RithmicProvider struct {
	cfg RithmicCfg
	// bridge RithmicBridge  // inject your gRPC / cgo bridge implementation
}

// NewRithmicProvider creates a Rithmic provider with the given config.
func NewRithmicProvider(cfg RithmicCfg) *RithmicProvider {
	return &RithmicProvider{cfg: cfg}
}

// RithmicDepthEvent is the normalised form your bridge should emit.
// Maps directly from SBestBidOfferCallback fields.
//
// Rithmic field mapping:
//
//	oBidInfoList[i].dPrice  → BidLevels[i].Price
//	oBidInfoList[i].iSize   → BidLevels[i].Volume
//	oOfferInfoList[i].dPrice → AskLevels[i].Price
//	oOfferInfoList[i].iSize  → AskLevels[i].Volume
//	iSsboe + iUsecs          → Timestamp (seconds + microseconds UNIX)
type RithmicDepthEvent struct {
	TimestampUs int64        // microseconds since UNIX epoch
	Symbol      string
	BidLevels   []PriceLevel // index 0 = best bid, descending price
	AskLevels   []PriceLevel // index 0 = best ask, ascending price
}

// Subscribe opens a best-bid-offer + depth stream for symbol and emits Snapshots.
// Replace the stub body with real bridge event consumption.
func (r *RithmicProvider) Subscribe(ctx context.Context, symbol string, depth int) (<-chan Snapshot, error) {
	if depth > 10 {
		return nil, fmt.Errorf("configure Rithmic depth subscription to at least %d levels", depth)
	}
	ch := make(chan Snapshot, 256)

	go func() {
		defer close(ch)
		// TODO: connect to bridge, subscribe symbol at depth,
		// then loop: receive RithmicDepthEvent → Snapshot, send on ch.
		// Example conversion:
		//   snap := Snapshot{
		//       Timestamp: time.UnixMicro(evt.TimestampUs),
		//       Symbol:    evt.Symbol,
		//       Asks:      evt.AskLevels,
		//       Bids:      evt.BidLevels,
		//   }
		<-ctx.Done()
	}()

	return ch, nil
}

func (r *RithmicProvider) Close() error { return nil }
