package level2

// QuestDB ILP (InfluxDB Line Protocol) writer over raw TCP — no external
// dependency. QuestDB listens on port 9009 by default.
//
// Wire: NewQuestDBWriter("gb10.local:9009")

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// QuestDBWriter sends ILP lines to QuestDB over a persistent TCP connection.
// Safe for concurrent use from multiple goroutines.
type QuestDBWriter struct {
	addr string
	conn net.Conn
	mu   sync.Mutex
}

// NewQuestDBWriter opens a TCP connection to QuestDB ILP on addr (e.g. "gb10.local:9009").
func NewQuestDBWriter(addr string) (*QuestDBWriter, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("questdb connect %s: %w", addr, err)
	}
	return &QuestDBWriter{addr: addr, conn: conn}, nil
}

// WriteSnapshot persists a full market snapshot to market_snapshots.
//
// Depth levels are written as explicit flat columns (bid_p1..bid_pK,
// bid_s1..bid_sK, ask_p1..ask_pK, ask_s1..ask_sK) so Qwen can scan
// them without JSON parsing. K = min(len(snap.Bids|Asks), k).
//
// Example row (k=10):
//
//	market_snapshots,asset=NQ \
//	  bid_p1=18500.00,bid_s1=28.0,...,ask_p1=18500.25,ask_s1=12.0,...,\
//	  spot_price=18500.125,lambda_ask=0.043,... 1748572190000000000
func (w *QuestDBWriter) WriteSnapshot(
	asset string,
	snap Snapshot,
	l Lambda,
	short, long Lambda,
	sig Signal,
	state SessionState,
	k int,
	gex GEXData,
) error {
	mid := 0.0
	if len(snap.Bids) > 0 && len(snap.Asks) > 0 {
		mid = (snap.Bids[0].Price + snap.Asks[0].Price) / 2.0
	}
	spike := l.Ask >= long.Ask*2.0

	var sb strings.Builder
	sb.WriteString("market_snapshots,asset=")
	sb.WriteString(ilpEscape(asset))
	sb.WriteByte(' ')

	// ── 10-level depth columns ────────────────────────────────────────────────
	for i, pl := range snap.Bids {
		if i >= k {
			break
		}
		fmt.Fprintf(&sb, "bid_p%d=%f,bid_s%d=%f,", i+1, pl.Price, i+1, pl.Volume)
	}
	for i, pl := range snap.Asks {
		if i >= k {
			break
		}
		fmt.Fprintf(&sb, "ask_p%d=%f,ask_s%d=%f,", i+1, pl.Price, i+1, pl.Volume)
	}

	// ── Derived, lambda, GEX, and signal columns ──────────────────────────────
	fmt.Fprintf(&sb,
		"spot_price=%f,"+
			"zero_gamma_level=%f,net_gex=%f,call_wall=%f,put_wall=%f,"+
			"ofi_metric=%f,"+
			"lambda_ask=%f,lambda_bid=%f,lambda_ratio=%f,"+
			"lambda_ask_s1=%f,lambda_ask_l10=%f,"+
			"lambda_spike=%t,signal_armed=%t,"+
			"session_state=%di,book_depth_k=%di",
		mid,
		gex.ZeroGamma, gex.NetGEX, gex.CallWall, gex.PutWall,
		sig.OFI,
		l.Ask, l.Bid, l.Ratio,
		short.Ask, long.Ask,
		spike, sig.Armed,
		int(state), k,
	)

	fmt.Fprintf(&sb, " %d\n", snap.Timestamp.UnixNano())

	w.mu.Lock()
	_, err := fmt.Fprint(w.conn, sb.String())
	w.mu.Unlock()
	return err
}

// WriteGEX persists a FlashAlpha GEX-only snapshot (used between L2 ticks).
func (w *QuestDBWriter) WriteGEX(asset string, gex GEXData) error {
	line := fmt.Sprintf(
		"gex_snapshots,asset=%s "+
			"zero_gamma_level=%f,net_gex=%f,call_wall=%f,put_wall=%f,dex=%f"+
			" %d\n",
		ilpEscape(asset),
		gex.ZeroGamma, gex.NetGEX, gex.CallWall, gex.PutWall, gex.DEX,
		gex.Timestamp.UnixNano(),
	)
	w.mu.Lock()
	_, err := fmt.Fprint(w.conn, line)
	w.mu.Unlock()
	return err
}

// WriteIGSpot persists an IG DFB mid price snapshot.
func (w *QuestDBWriter) WriteIGSpot(asset string, mid float64, ts time.Time) error {
	line := fmt.Sprintf(
		"ig_snapshots,asset=%s spot_price=%f %d\n",
		ilpEscape(asset), mid, ts.UnixNano(),
	)
	w.mu.Lock()
	_, err := fmt.Fprint(w.conn, line)
	w.mu.Unlock()
	return err
}

// Close shuts down the TCP connection.
func (w *QuestDBWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conn.Close()
}

// ilpEscape replaces spaces and commas in ILP tag values.
func ilpEscape(s string) string {
	s = strings.ReplaceAll(s, " ", "\\ ")
	s = strings.ReplaceAll(s, ",", "\\,")
	return s
}

// GEXData holds the options dealer gamma exposure snapshot from FlashAlpha.
type GEXData struct {
	Timestamp time.Time
	Symbol    string
	ZeroGamma float64
	NetGEX    float64
	CallWall  float64
	PutWall   float64
	DEX       float64
}
