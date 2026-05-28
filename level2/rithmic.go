package level2

import (
	"context"
	"io"
	"log/slog"
	"time"

	pb "github.com/Longfortv5/longfort-v7-briefings/level2/proto/rithmic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const rithmicUDS = "unix:///tmp/rithmic_bridge.sock"

// RithmicCfg holds connection parameters for the Rithmic R|API+ gateway.
// GatewayHost / credentials are used by the C++ sidecar; the Go adapter
// dials the sidecar over a Unix Domain Socket (rithmicUDS) to avoid
// TCP/IP loopback overhead on GB10.
type RithmicCfg struct {
	GatewayHost string // e.g. "rituz00100.01.rithmic.com:443"
	SystemName  string // e.g. "Rithmic 01 Chicago"
	UserID      string
	Password    string
	FCMID       string
	IBID        string
}

// RithmicProvider streams MBP-10 depth from the C++ sidecar over UDS gRPC
// and emits Snapshots on the channel returned by Subscribe.
type RithmicProvider struct {
	cfg  RithmicCfg
	conn *grpc.ClientConn
}

// NewRithmicProvider creates a provider. Call Subscribe to open the connection.
func NewRithmicProvider(cfg RithmicCfg) *RithmicProvider {
	return &RithmicProvider{cfg: cfg}
}

// Subscribe dials the sidecar UDS, opens the SubscribeDepth stream, and emits
// Snapshots until ctx is cancelled or the sidecar disconnects.
// If the socket does not exist yet, grpc.WithBlock() causes this to wait —
// the goroutine will not emit any ticks until the sidecar comes up.
func (r *RithmicProvider) Subscribe(ctx context.Context, symbol string, depth int) (<-chan Snapshot, error) {
	conn, err := grpc.DialContext(ctx, rithmicUDS,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}
	r.conn = conn

	client := pb.NewRithmicSidecarClient(conn)
	stream, err := client.SubscribeDepth(ctx, &pb.DepthRequest{Symbol: symbol})
	if err != nil {
		conn.Close()
		return nil, err
	}
	slog.Info("rithmic sidecar connected", "symbol", symbol, "depth", depth, "uds", rithmicUDS)

	ch := make(chan Snapshot, 256)
	go func() {
		defer close(ch)
		defer conn.Close()
		r.readLoop(ctx, stream, symbol, depth, ch)
	}()
	return ch, nil
}

func (r *RithmicProvider) readLoop(
	ctx context.Context,
	stream pb.RithmicSidecar_SubscribeDepthClient,
	symbol string,
	depth int,
	ch chan<- Snapshot,
) {
	for {
		update, err := stream.Recv()
		if err == io.EOF {
			slog.Info("rithmic stream closed by sidecar", "symbol", symbol)
			return
		}
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("rithmic stream error", "symbol", symbol, "err", err)
			}
			return
		}

		snap := depthUpdateToSnapshot(update, symbol, depth)

		select {
		case ch <- snap:
		case <-ctx.Done():
			return
		default: // drop if pipeline is backed up
		}
	}
}

// depthUpdateToSnapshot maps a sidecar DepthUpdate into a level2.Snapshot.
// Caps levels at depth (max 10) to match the Lambda pipeline expectation.
func depthUpdateToSnapshot(u *pb.DepthUpdate, symbol string, depth int) Snapshot {
	bids := make([]PriceLevel, 0, depth)
	for i, pl := range u.Bids {
		if i >= depth {
			break
		}
		bids = append(bids, PriceLevel{Price: pl.Price, Volume: float64(pl.Size)})
	}
	asks := make([]PriceLevel, 0, depth)
	for i, pl := range u.Asks {
		if i >= depth {
			break
		}
		asks = append(asks, PriceLevel{Price: pl.Price, Volume: float64(pl.Size)})
	}
	return Snapshot{
		Timestamp: time.Unix(0, u.ExchangeTsNs).UTC(),
		Symbol:    symbol,
		Bids:      bids,
		Asks:      asks,
	}
}

func (r *RithmicProvider) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}
