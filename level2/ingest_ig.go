package level2

// IG REST API snapshot client (v2 session auth + v3 markets endpoint).
// Authenticates once, auto-renews on 401, polls for DFB mid prices.
//
// Live endpoint:  "https://api.ig.com/gateway/deal"
// Demo endpoint:  "https://demo-api.ig.com/gateway/deal"
//
// Epic examples (confirm in IG account):
//   NQ DFB:     "IX.D.NASDAQ.IFD.IP"
//   DAX DFB:    "IX.D.DAX.IFD.IP"
//   Nikkei DFB: "IX.D.NIKKEI.IFD.IP"
//
// Wire: NewIGClient(IGConfig{
//     APIKey:     os.Getenv("IG_API_KEY"),
//     Identifier: os.Getenv("IG_IDENTIFIER"),
//     Password:   os.Getenv("IG_PASSWORD"),
//     BaseURL:    "https://api.ig.com/gateway/deal",
// })

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// IGConfig holds IG REST API credentials.
type IGConfig struct {
	APIKey     string
	Identifier string
	Password   string
	BaseURL    string // live or demo endpoint
}

// IGSpotSnapshot is a single IG price snapshot.
type IGSpotSnapshot struct {
	Epic      string
	Bid       float64
	Offer     float64
	Mid       float64
	Timestamp time.Time
}

// IGClient is a thread-safe IG REST client with automatic session renewal.
type IGClient struct {
	cfg        IGConfig
	httpClient *http.Client

	mu          sync.Mutex
	cst         string    // Client-Session-Token header
	xst         string    // X-SECURITY-TOKEN header
	tokenExpiry time.Time
}

// NewIGClient creates an IG client. Call Authenticate before first FetchSpot.
func NewIGClient(cfg IGConfig) *IGClient {
	return &IGClient{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

type igSessionReq struct {
	Identifier        string `json:"identifier"`
	Password          string `json:"password"`
	EncryptedPassword bool   `json:"encryptedPassword"`
}

// Authenticate opens an IG session and stores the CST and XST tokens.
func (c *IGClient) Authenticate(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.authenticate(ctx)
}

func (c *IGClient) authenticate(ctx context.Context) error {
	body, _ := json.Marshal(igSessionReq{
		Identifier:        c.cfg.Identifier,
		Password:          c.cfg.Password,
		EncryptedPassword: false,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.cfg.BaseURL+"/session", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-IG-API-KEY", c.cfg.APIKey)
	req.Header.Set("Version", "2")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("IG auth: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("IG auth HTTP %d", resp.StatusCode)
	}

	c.cst = resp.Header.Get("CST")
	c.xst = resp.Header.Get("X-SECURITY-TOKEN")
	c.tokenExpiry = time.Now().Add(6 * time.Hour)
	return nil
}

type igMarketResp struct {
	Snapshot struct {
		Bid   float64 `json:"bid"`
		Offer float64 `json:"offer"`
	} `json:"snapshot"`
}

// FetchSpot returns the current bid/offer/mid for the given IG epic.
func (c *IGClient) FetchSpot(ctx context.Context, epic string) (IGSpotSnapshot, error) {
	c.mu.Lock()
	if time.Now().After(c.tokenExpiry) {
		if err := c.authenticate(ctx); err != nil {
			c.mu.Unlock()
			return IGSpotSnapshot{}, fmt.Errorf("IG re-auth: %w", err)
		}
	}
	cst, xst := c.cst, c.xst
	c.mu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.cfg.BaseURL+"/markets/"+epic, nil)
	if err != nil {
		return IGSpotSnapshot{}, err
	}
	req.Header.Set("X-IG-API-KEY", c.cfg.APIKey)
	req.Header.Set("CST", cst)
	req.Header.Set("X-SECURITY-TOKEN", xst)
	req.Header.Set("Version", "3")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return IGSpotSnapshot{}, fmt.Errorf("IG market fetch: %w", err)
	}
	defer resp.Body.Close()

	// Token expired mid-session — re-auth and retry once
	if resp.StatusCode == http.StatusUnauthorized {
		c.mu.Lock()
		authErr := c.authenticate(ctx)
		c.mu.Unlock()
		if authErr != nil {
			return IGSpotSnapshot{}, authErr
		}
		return c.FetchSpot(ctx, epic)
	}

	if resp.StatusCode != http.StatusOK {
		return IGSpotSnapshot{}, fmt.Errorf("IG market HTTP %d for %s", resp.StatusCode, epic)
	}

	var mkt igMarketResp
	if err := json.NewDecoder(resp.Body).Decode(&mkt); err != nil {
		return IGSpotSnapshot{}, fmt.Errorf("IG decode: %w", err)
	}

	mid := (mkt.Snapshot.Bid + mkt.Snapshot.Offer) / 2.0
	return IGSpotSnapshot{
		Epic:      epic,
		Bid:       mkt.Snapshot.Bid,
		Offer:     mkt.Snapshot.Offer,
		Mid:       mid,
		Timestamp: time.Now().UTC(),
	}, nil
}

// Poll continuously fetches spot snapshots at interval for the given epic.
func (c *IGClient) Poll(ctx context.Context, epic, asset string, interval time.Duration, writer *QuestDBWriter) {
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
			snap, err := c.FetchSpot(ctx, epic)
			if err != nil {
				slog.Warn("IG poll error", "epic", epic, "err", err)
				continue
			}
			if err := writer.WriteIGSpot(asset, snap.Mid, snap.Timestamp); err != nil {
				slog.Warn("IG QDB write error", "epic", epic, "err", err)
			}
		}
	}
}
