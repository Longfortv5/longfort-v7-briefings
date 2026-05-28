package level2

// This file provides the EvaluateRegime implementation reference for the
// director package. Copy into your private bot repo under package director
// and adjust the import paths accordingly.
//
// Dependencies to add to your go.mod:
//   (none beyond stdlib — database/sql driver for QuestDB uses the standard
//    postgres wire protocol; add github.com/lib/pq or pgx as your driver)

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ── Types ────────────────────────────────────────────────────────────────────

type MarketRegime string

const (
	RegimePinned     MarketRegime = "PINNED"
	RegimeLongTrend  MarketRegime = "LONG_TREND"
	RegimeShortTrend MarketRegime = "SHORT_TREND"
	RegimeEmergency  MarketRegime = "PAUSED"
)

type SessionState int

const (
	StateClosed SessionState = iota
	StateShadow
	StateOpen
)

type LLMDirector struct {
	db           *sql.DB
	vllmEndpoint string
	httpClient   *http.Client

	mu           sync.RWMutex
	CurrentState MarketRegime
	ManualForced bool
}

// NewLLMDirector constructs a director with a sensible HTTP timeout.
func NewLLMDirector(db *sql.DB, vllmEndpoint string) *LLMDirector {
	return &LLMDirector{
		db:           db,
		vllmEndpoint: vllmEndpoint,
		httpClient:   &http.Client{Timeout: 10 * time.Second},
		CurrentState: RegimePinned,
	}
}

// CurrentRegime returns the current regime safely for concurrent readers.
func (d *LLMDirector) CurrentRegime() MarketRegime {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.CurrentState
}

// ForceRegime sets a manual override that EvaluateRegime will not overwrite.
func (d *LLMDirector) ForceRegime(r MarketRegime) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.CurrentState = r
	d.ManualForced = true
}

// ClearForce releases a manual override and returns control to EvaluateRegime.
func (d *LLMDirector) ClearForce() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ManualForced = false
}

// ── EvaluateRegime ───────────────────────────────────────────────────────────

func (d *LLMDirector) EvaluateRegime(ctx context.Context) error {
	d.mu.RLock()
	forced := d.ManualForced
	d.mu.RUnlock()
	if forced {
		return nil // Desk Governor override active — LLM cannot overwrite
	}

	// 1. Pull latest snapshot row per asset from QuestDB
	rows, err := d.queryLatestSnapshots(ctx)
	if err != nil {
		return fmt.Errorf("snapshot query: %w", err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("no snapshot rows available")
	}

	// 2. Build structured prompt
	prompt := buildRegimePrompt(rows)

	// 3. Call local vLLM endpoint
	regime, err := d.callVLLM(ctx, prompt)
	if err != nil {
		// Fail-safe: do not corrupt CurrentState on vLLM error
		return fmt.Errorf("vllm call: %w", err)
	}

	// 4. Persist deterministic single-word result
	d.mu.Lock()
	d.CurrentState = regime
	d.mu.Unlock()

	return nil
}

// ── QuestDB query ────────────────────────────────────────────────────────────

type snapshotRow struct {
	TS             time.Time
	Asset          string
	SpotPrice      float64
	ZeroGammaLevel float64
	NetGEX         float64
	CallWall       float64
	PutWall        float64
	OFI            float64
	LambdaAsk      float64
	LambdaBid      float64
	LambdaRatio    float64
	LambdaSpike    bool
	SignalArmed    bool
	SonarScore     int16
	SessionState   int8
}

// LATEST ON ts PARTITION BY asset returns exactly one row per asset symbol —
// the row with the most recent ts for that asset. No subquery needed.
const latestSnapshotQuery = `
SELECT
    ts, asset, spot_price, zero_gamma_level, net_gex,
    call_wall, put_wall, ofi_metric,
    lambda_ask, lambda_bid, lambda_ratio,
    lambda_spike, signal_armed,
    sonar_score, session_state
FROM market_snapshots
LATEST ON ts PARTITION BY asset
`

func (d *LLMDirector) queryLatestSnapshots(ctx context.Context) ([]snapshotRow, error) {
	dbRows, err := d.db.QueryContext(ctx, latestSnapshotQuery)
	if err != nil {
		return nil, err
	}
	defer dbRows.Close()

	var result []snapshotRow
	for dbRows.Next() {
		var s snapshotRow
		if err := dbRows.Scan(
			&s.TS, &s.Asset, &s.SpotPrice, &s.ZeroGammaLevel, &s.NetGEX,
			&s.CallWall, &s.PutWall, &s.OFI,
			&s.LambdaAsk, &s.LambdaBid, &s.LambdaRatio,
			&s.LambdaSpike, &s.SignalArmed,
			&s.SonarScore, &s.SessionState,
		); err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, dbRows.Err()
}

// ── Prompt builder ───────────────────────────────────────────────────────────

const systemPrompt = `You are a market regime classifier for an automated trading system.
Analyse the structured market snapshot and classify the current regime as exactly one of:

  PINNED       — Spot near zero-gamma. Dealers long gamma. Price range-bound. Large positive GEX.
  LONG_TREND   — Spot above zero-gamma or above call wall. Dealers short gamma. Upward momentum self-reinforcing.
  SHORT_TREND  — Spot below zero-gamma or below put wall. Dealers short gamma. Downward momentum self-reinforcing.
  PAUSED       — Abnormal conditions: lambda spike, extreme OFI, shadow/closed session, or conflicting signals.

Rules:
- Respond with ONLY the single classification token. No explanation. No punctuation. No newline.
- If any asset is in SHADOW or CLOSED session state, bias toward PAUSED.
- A lambda_spike=true with signal_armed=true on any asset is a strong PAUSED or trend confirmation signal.
- Net GEX sign is the primary regime indicator; spot vs zero-gamma level confirms direction.`

func buildRegimePrompt(rows []snapshotRow) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Snapshot time: %s\n\n", rows[0].TS.UTC().Format("2006-01-02 15:04:05 UTC")))

	for _, s := range rows {
		fmt.Fprintf(&b, "=== %s ===\n", s.Asset)
		fmt.Fprintf(&b, "  Spot:            %.2f\n", s.SpotPrice)
		fmt.Fprintf(&b, "  Zero Gamma:      %.2f  (offset %+.2f)\n", s.ZeroGammaLevel, s.SpotPrice-s.ZeroGammaLevel)
		fmt.Fprintf(&b, "  Net GEX:         %.2f  (%s)\n", s.NetGEX, gexLabel(s.NetGEX))
		fmt.Fprintf(&b, "  Call Wall:       %.2f  (spot %+.2f from wall)\n", s.CallWall, s.SpotPrice-s.CallWall)
		fmt.Fprintf(&b, "  Put Wall:        %.2f  (spot %+.2f from wall)\n", s.PutWall, s.SpotPrice-s.PutWall)
		fmt.Fprintf(&b, "  OFI:             %+.4f\n", s.OFI)
		fmt.Fprintf(&b, "  λ_ask/bid/ratio: %.6f / %.6f / %.4f\n", s.LambdaAsk, s.LambdaBid, s.LambdaRatio)
		fmt.Fprintf(&b, "  Lambda spike:    %t  |  Signal armed: %t\n", s.LambdaSpike, s.SignalArmed)
		fmt.Fprintf(&b, "  Sonar score:     %d/100\n", s.SonarScore)
		fmt.Fprintf(&b, "  Session state:   %s\n\n", sessionLabel(SessionState(s.SessionState)))
	}

	b.WriteString("Regime classification:")
	return b.String()
}

func gexLabel(netGEX float64) string {
	switch {
	case netGEX > 1e9:
		return "strongly long / suppressive"
	case netGEX > 0:
		return "long / suppressive"
	case netGEX < -1e9:
		return "strongly short / amplifying"
	default:
		return "short / amplifying"
	}
}

func sessionLabel(s SessionState) string {
	switch s {
	case StateOpen:
		return "OPEN"
	case StateShadow:
		return "SHADOW"
	default:
		return "CLOSED"
	}
}

// ── vLLM call ────────────────────────────────────────────────────────────────

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	MaxTokens   int           `json:"max_tokens"`
	Temperature float64       `json:"temperature"`
	Stop        []string      `json:"stop"`
}

type chatChoice struct {
	Message chatMessage `json:"message"`
}

type chatResponse struct {
	Choices []chatChoice `json:"choices"`
}

func (d *LLMDirector) callVLLM(ctx context.Context, userPrompt string) (MarketRegime, error) {
	reqBody := chatRequest{
		Model: "local",
		Messages: []chatMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: userPrompt},
		},
		MaxTokens:   8,   // longest token is SHORT_TREND (11 chars) — 8 tokens is sufficient
		Temperature: 0.0, // fully deterministic
		Stop:        []string{"\n"},
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		d.vllmEndpoint+"/v1/chat/completions",
		bytes.NewReader(body),
	)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("vllm unreachable at %s: %w", d.vllmEndpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("vllm HTTP %d: %s", resp.StatusCode, b)
	}

	var chatResp chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return "", fmt.Errorf("decode vllm response: %w", err)
	}
	if len(chatResp.Choices) == 0 {
		return "", fmt.Errorf("vllm returned zero choices")
	}

	return parseRegime(chatResp.Choices[0].Message.Content)
}

// parseRegime maps the raw LLM token to a MarketRegime.
// Returns an error (and no update) if the model returns an unrecognised token.
func parseRegime(raw string) (MarketRegime, error) {
	switch strings.TrimSpace(strings.ToUpper(raw)) {
	case "PINNED":
		return RegimePinned, nil
	case "LONG_TREND":
		return RegimeLongTrend, nil
	case "SHORT_TREND":
		return RegimeShortTrend, nil
	case "PAUSED":
		return RegimeEmergency, nil
	default:
		return "", fmt.Errorf("unrecognised regime token %q — CurrentState unchanged", raw)
	}
}
