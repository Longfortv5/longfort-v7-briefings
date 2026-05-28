package level2

// FlashAlpha Growth tier REST client.
// Polls /v1/gex/snapshot?ticker=X on a configurable interval.
//
// Wire: NewFlashAlphaPoller(FlashAlphaCfg{
//     APIKey:  os.Getenv("FLASHALPHA_API_KEY"),
//     BaseURL: "https://api.flashalpha.com/v1",   // confirm with FlashAlpha docs
// })
//
// NOTE: FlashAlpha's exact endpoint paths and JSON field names must be
// verified against their live API docs — the mappings below are based on
// their published data dictionary. Adjust if fields differ.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// FlashAlphaCfg holds credentials for the FlashAlpha API.
type FlashAlphaCfg struct {
	APIKey  string
	BaseURL string // e.g. "https://api.flashalpha.com/v1"
}

// FlashAlphaPoller polls FlashAlpha for GEX snapshots and emits GEXData.
type FlashAlphaPoller struct {
	cfg        FlashAlphaCfg
	httpClient *http.Client
}

// NewFlashAlphaPoller creates a poller with a 10-second HTTP timeout.
func NewFlashAlphaPoller(cfg FlashAlphaCfg) *FlashAlphaPoller {
	return &FlashAlphaPoller{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// flashAlphaGEXResponse — verify these field names against FlashAlpha's live docs.
type flashAlphaGEXResponse struct {
	Ticker    string  `json:"ticker"`
	Timestamp string  `json:"timestamp"`
	ZeroGamma float64 `json:"zero_gamma"`
	CallWall  float64 `json:"call_wall"`
	PutWall   float64 `json:"put_wall"`
	NetGEX    float64 `json:"net_gex"`
	NetDEX    float64 `json:"net_dex"`
}

// FetchGEX makes a single synchronous GEX snapshot request.
func (p *FlashAlphaPoller) FetchGEX(ctx context.Context, ticker string) (GEXData, error) {
	url := fmt.Sprintf("%s/gex/snapshot?ticker=%s", p.cfg.BaseURL, ticker)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return GEXData{}, err
	}
	req.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)
	req.Header.Set("Accept", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return GEXData{}, fmt.Errorf("flashalpha fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return GEXData{}, fmt.Errorf("flashalpha HTTP %d for %s", resp.StatusCode, ticker)
	}

	var raw flashAlphaGEXResponse
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return GEXData{}, fmt.Errorf("flashalpha decode: %w", err)
	}

	ts, err := time.Parse(time.RFC3339, raw.Timestamp)
	if err != nil {
		ts = time.Now().UTC()
	}

	return GEXData{
		Timestamp: ts,
		Symbol:    raw.Ticker,
		ZeroGamma: raw.ZeroGamma,
		NetGEX:    raw.NetGEX,
		CallWall:  raw.CallWall,
		PutWall:   raw.PutWall,
		DEX:       raw.NetDEX,
	}, nil
}

// Poll continuously fetches GEX snapshots at interval and sends on the returned channel.
// Runs until ctx is cancelled. Transient errors are logged and skipped — the channel
// will simply not receive a value for that tick.
func (p *FlashAlphaPoller) Poll(ctx context.Context, ticker string, interval time.Duration) <-chan GEXData {
	ch := make(chan GEXData, 8)
	go func() {
		defer close(ch)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if !IsGlobalSystemAvailable(time.Now()) {
					continue
				}
				gex, err := p.FetchGEX(ctx, ticker)
				if err != nil {
					slog.Warn("flashalpha poll error", "ticker", ticker, "err", err)
					continue
				}
				select {
				case ch <- gex:
				default: // drop if QuestDB writer is backed up
				}
			}
		}
	}()
	return ch
}
