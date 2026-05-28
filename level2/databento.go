package level2

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	dbn "github.com/NimbleMarkets/dbn-go"
	dbn_live "github.com/NimbleMarkets/dbn-go/live"
)

// DatabentoCfg holds connection parameters for the Databento streaming API.
type DatabentoCfg struct {
	APIKey  string // DATABENTO_API_KEY env var
	Dataset string // e.g. "GLBX.MDP3" for CME Globex
	Schema  string // "mbp-1" for Level 1 top-of-book
}

// DatabentoProvider streams MBP-1 records via the NimbleMarkets dbn-go SDK
// and emits Snapshots on the channel returned by Subscribe.
//
// With mbp-1 (k=1), Kyle's Lambda is always 0 — meaningful lambda requires
// MBP-10. Switch DatabentoCfg.Schema to "mbp-10" and depth to 10 in
// LoadCfgFromEnv / OrchestratorCfg to activate non-trivial lambda values.
type DatabentoProvider struct {
	cfg    DatabentoCfg
	client *dbn_live.LiveClient
}

// NewDatabentoProvider creates a provider. Call Subscribe to open the connection.
func NewDatabentoProvider(cfg DatabentoCfg) *DatabentoProvider {
	return &DatabentoProvider{cfg: cfg}
}

// Subscribe authenticates, subscribes to symbol with cfg.Schema, and streams
// Snapshots on the returned channel until ctx is cancelled.
func (d *DatabentoProvider) Subscribe(ctx context.Context, symbol string, depth int) (<-chan Snapshot, error) {
	liveCfg := dbn_live.LiveConfig{
		ApiKey:  d.cfg.APIKey,
		Dataset: d.cfg.Dataset,
		Verbose: false,
	}

	client, err := dbn_live.NewLiveClient(liveCfg)
	if err != nil {
		return nil, fmt.Errorf("databento connect: %w", err)
	}
	d.client = client

	if _, err := client.Authenticate(d.cfg.APIKey); err != nil {
		client.Stop()
		return nil, fmt.Errorf("databento auth: %w", err)
	}

	stype, err := dbn.STypeFromString("continuous")
	if err != nil {
		client.Stop()
		return nil, fmt.Errorf("databento stype: %w", err)
	}

	sub := dbn_live.SubscriptionRequestMsg{
		Schema:  d.cfg.Schema,
		StypeIn: stype,
		Symbols: []string{symbol},
	}
	if err := client.Subscribe(sub); err != nil {
		client.Stop()
		return nil, fmt.Errorf("databento subscribe: %w", err)
	}

	if err := client.Start(); err != nil {
		client.Stop()
		return nil, fmt.Errorf("databento start: %w", err)
	}
	slog.Info("databento live stream started", "symbol", symbol, "schema", d.cfg.Schema)

	ch := make(chan Snapshot, 512)
	go func() {
		defer close(ch)
		defer client.Stop()
		d.readLoop(ctx, client, symbol, ch)
	}()

	return ch, nil
}

// readLoop drives the DbnScanner and dispatches each record to the Visitor.
func (d *DatabentoProvider) readLoop(ctx context.Context, client *dbn_live.LiveClient, symbol string, ch chan<- Snapshot) {
	scanner := client.GetDbnScanner()
	if scanner == nil {
		slog.Error("databento: DbnScanner is nil after Start()")
		return
	}

	v := &mbp1Visitor{symbol: symbol, ch: ch, ctx: ctx}

	for scanner.Next() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := scanner.Visit(v); err != nil {
			slog.Warn("databento visit error", "err", err)
		}
	}
	if err := scanner.Error(); err != nil {
		if ctx.Err() == nil {
			slog.Error("databento scanner error", "err", err)
		}
	}
}

func (d *DatabentoProvider) Close() error {
	if d.client != nil {
		return d.client.Stop()
	}
	return nil
}

// ── Visitor ───────────────────────────────────────────────────────────────────

// mbp1Visitor implements dbn.Visitor, converting MBP-1 records to Snapshots.
// All non-MBP1 record types are silently dropped via no-op methods.
type mbp1Visitor struct {
	symbol string
	ch     chan<- Snapshot
	ctx    context.Context
}

// OnMbp1 converts a top-of-book record to a 1-level Snapshot.
// Prices are int64 fixed-point (÷ 1e9); dbn.Fixed9ToFloat64 handles the conversion.
func (v *mbp1Visitor) OnMbp1(rec *dbn.Mbp1Msg) error {
	snap := Snapshot{
		Timestamp: time.Unix(0, int64(rec.Header.TsEvent)).UTC(),
		Symbol:    v.symbol,
		Bids: []PriceLevel{{
			Price:  dbn.Fixed9ToFloat64(rec.Level.BidPx),
			Volume: float64(rec.Level.BidSz),
		}},
		Asks: []PriceLevel{{
			Price:  dbn.Fixed9ToFloat64(rec.Level.AskPx),
			Volume: float64(rec.Level.AskSz),
		}},
	}
	select {
	case v.ch <- snap:
	case <-v.ctx.Done():
		return v.ctx.Err()
	default: // drop if pipeline is backed up
	}
	return nil
}

// ── dbn.Visitor no-ops for unused record types ────────────────────────────────

func (v *mbp1Visitor) OnMbp0(r *dbn.Mbp0Msg) error               { return nil }
func (v *mbp1Visitor) OnMbp10(r *dbn.Mbp10Msg) error              { return nil }
func (v *mbp1Visitor) OnMbo(r *dbn.MboMsg) error                   { return nil }
func (v *mbp1Visitor) OnOhlcv(r *dbn.OhlcvMsg) error               { return nil }
func (v *mbp1Visitor) OnCmbp1(r *dbn.Cmbp1Msg) error               { return nil }
func (v *mbp1Visitor) OnBbo(r *dbn.BboMsg) error                   { return nil }
func (v *mbp1Visitor) OnImbalance(r *dbn.ImbalanceMsg) error        { return nil }
func (v *mbp1Visitor) OnStatMsg(r *dbn.StatMsg) error               { return nil }
func (v *mbp1Visitor) OnStatusMsg(r *dbn.StatusMsg) error           { return nil }
func (v *mbp1Visitor) OnInstrumentDefMsg(r *dbn.InstrumentDefMsg) error { return nil }
func (v *mbp1Visitor) OnSymbolMappingMsg(r *dbn.SymbolMappingMsg) error { return nil }
func (v *mbp1Visitor) OnStreamEnd() error                           { return nil }

func (v *mbp1Visitor) OnErrorMsg(r *dbn.ErrorMsg) error {
	slog.Error("databento error record", "err", string(r.Error[:]))
	return nil
}

func (v *mbp1Visitor) OnSystemMsg(r *dbn.SystemMsg) error {
	slog.Debug("databento system msg", "msg", string(r.Message[:]))
	return nil
}
