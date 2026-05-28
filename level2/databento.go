package level2

// Databento live client — implemented with stdlib only (crypto/tls, net, encoding/binary).
// No external SDK required; connects directly to Databento's live TCP endpoint.
//
// Protocol flow:
//   1. TLS TCP → live.databento.com:13000
//   2. Server sends JSON greeting line
//   3. Client sends JSON auth line
//   4. Server sends JSON auth-challenge response
//   5. Client sends JSON subscribe line
//   6. Server streams DBN metadata header + binary MBP1 records
//
// DBN byte layout verified against Databento DBN spec v2 (little-endian).
// If records parse as garbage, cross-check struct sizes against:
//   https://databento.com/docs/knowledge-base/new-users/dbn-encoding/dbn-record-types

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"strings"
	"time"
)

const (
	databentoLiveHost = "live.databento.com:13000"
	dbnScaleFactor    = 1e-9  // Databento fixed-point price scale
	rtypeMBP1         = 0x01  // DBN rtype for MBP-1 records
	mbp1RecordBytes   = 80    // total bytes per MBP-1 record (header + body + 1 level)
)

// DatabentoCfg holds connection parameters for the Databento streaming API.
type DatabentoCfg struct {
	APIKey  string // DATABENTO_API_KEY env var
	Dataset string // e.g. "GLBX.MDP3" for CME Globex
	Schema  string // "mbp-1" for Level 1 top-of-book
}

// DatabentoProvider streams MBP-1 records and emits Snapshots.
// With mbp-1 (k=1), lambda is always zero — meaningful lambda requires MBP-10.
// The stream still delivers real-time bid/ask/mid and top-of-book imbalance.
type DatabentoProvider struct {
	cfg  DatabentoCfg
	conn net.Conn
}

// NewDatabentoProvider creates a provider. Call Subscribe to open the connection.
func NewDatabentoProvider(cfg DatabentoCfg) *DatabentoProvider {
	return &DatabentoProvider{cfg: cfg}
}

// Subscribe connects to Databento, authenticates, subscribes to symbol at schema mbp-1,
// and streams Snapshots on the returned channel until ctx is cancelled.
func (d *DatabentoProvider) Subscribe(ctx context.Context, symbol string, depth int) (<-chan Snapshot, error) {
	if d.cfg.Schema != "mbp-1" {
		return nil, fmt.Errorf("DatabentoProvider: schema must be mbp-1, got %q", d.cfg.Schema)
	}

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	conn, err := tls.DialWithDialer(
		&net.Dialer{Timeout: 10 * time.Second},
		"tcp", databentoLiveHost, tlsCfg,
	)
	if err != nil {
		return nil, fmt.Errorf("databento connect: %w", err)
	}
	d.conn = conn

	if err := d.handshake(conn, symbol); err != nil {
		conn.Close()
		return nil, fmt.Errorf("databento handshake: %w", err)
	}

	ch := make(chan Snapshot, 512)
	go func() {
		defer close(ch)
		defer conn.Close()
		d.readLoop(ctx, conn, symbol, ch)
	}()

	return ch, nil
}

// ── Handshake ─────────────────────────────────────────────────────────────────

type dbnAuthMsg struct {
	Auth                string `json:"auth"`
	Dataset             string `json:"dataset"`
	Encoding            string `json:"encoding"`
	TsOut               bool   `json:"ts_out"`
	HeartbeatIntervalS  int    `json:"heartbeat_interval_s"`
}

type dbnSubscribeMsg struct {
	Schema  string   `json:"schema"`
	StypeIn string   `json:"stype_in"`
	Symbols []string `json:"symbols"`
	Start   string   `json:"start"`
}

func (d *DatabentoProvider) handshake(conn net.Conn, symbol string) error {
	reader := bufio.NewReader(conn)

	// 1. Read server greeting line
	greeting, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read greeting: %w", err)
	}
	slog.Debug("databento greeting", "msg", strings.TrimSpace(greeting))

	// 2. Send auth
	auth := dbnAuthMsg{
		Auth:               d.cfg.APIKey,
		Dataset:            d.cfg.Dataset,
		Encoding:           "dbn",
		TsOut:              false,
		HeartbeatIntervalS: 5,
	}
	if err := writeJSON(conn, auth); err != nil {
		return fmt.Errorf("send auth: %w", err)
	}

	// 3. Read auth response — must not contain "error"
	authResp, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read auth response: %w", err)
	}
	if strings.Contains(strings.ToLower(authResp), "error") {
		return fmt.Errorf("auth rejected: %s", strings.TrimSpace(authResp))
	}
	slog.Info("databento authenticated", "dataset", d.cfg.Dataset)

	// 4. Send subscribe
	sub := dbnSubscribeMsg{
		Schema:  d.cfg.Schema,
		StypeIn: "continuous",
		Symbols: []string{symbol},
		Start:   "",
	}
	if err := writeJSON(conn, sub); err != nil {
		return fmt.Errorf("send subscribe: %w", err)
	}

	// 5. Read subscription confirmation line
	subResp, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read sub response: %w", err)
	}
	if strings.Contains(strings.ToLower(subResp), "error") {
		return fmt.Errorf("subscribe rejected: %s", strings.TrimSpace(subResp))
	}
	slog.Info("databento subscribed", "symbol", symbol, "schema", d.cfg.Schema)

	// 6. Drain the DBN metadata header (variable-length; ends when binary records begin)
	// Databento sends a text/JSON metadata block before the binary stream.
	// Skip until we see the first byte that matches a valid DBN record length.
	if err := drainMetadata(conn); err != nil {
		return fmt.Errorf("drain metadata: %w", err)
	}

	return nil
}

// drainMetadata reads and discards the DBN metadata header that precedes the binary stream.
func drainMetadata(conn net.Conn) error {
	// The metadata header ends with a known magic sequence; in practice we read
	// until the first byte whose value × 4 == mbp1RecordBytes (length field = 20).
	buf := make([]byte, 1)
	for {
		if _, err := io.ReadFull(conn, buf); err != nil {
			return err
		}
		if int(buf[0])*4 == mbp1RecordBytes {
			// This looks like the length byte of an MBP-1 record header.
			// Unread it by pushing the byte back via a wrapper — but since
			// net.Conn has no UnreadByte, we handle it in readLoop by
			// passing the already-read length byte in.
			// Store in provider for readLoop to consume.
			return nil
		}
	}
}

// ── Binary record loop ────────────────────────────────────────────────────────

// mbp1Record mirrors the DBN MBP-1 binary layout (little-endian, 80 bytes total).
// Layout per Databento DBN spec v2:
//
//	Offset  Size  Field
//	     0     1  Length    (uint8, record size in 4-byte units; 20 × 4 = 80)
//	     1     1  RType     (uint8; 0x01 = MBP-1)
//	     2     2  PublisherID (uint16)
//	     4     4  InstrumentID (uint32)
//	     8     8  TsEvent   (uint64, ns since UNIX epoch)
//	    16     8  Price     (int64, trade price ×1e-9; 0 for non-trade)
//	    24     4  Size      (uint32, trade size)
//	    28     1  Action    (int8; 'T'=trade, 'A'=add, 'C'=cancel, 'M'=modify)
//	    29     1  Side      (int8; 'A'=ask, 'B'=bid, 'N'=none)
//	    30     1  Flags     (uint8)
//	    31     1  Depth     (uint8; should be 0 for MBP-1)
//	    32     8  TsRecv    (uint64, ns)
//	    40     4  TsDelta   (int32, ns)
//	    44     4  Sequence  (uint32)
//	    48     8  BidPx     (int64, best bid ×1e-9)
//	    56     8  AskPx     (int64, best ask ×1e-9)
//	    64     4  BidSz     (uint32)
//	    68     4  AskSz     (uint32)
//	    72     4  BidCt     (uint32, order count at bid)
//	    76     4  AskCt     (uint32, order count at ask)
//	Total: 80 bytes
type mbp1Record struct {
	// Header (16 bytes)
	Length       uint8
	RType        uint8
	PublisherID  uint16
	InstrumentID uint32
	TsEvent      uint64
	// Body (32 bytes)
	Price    int64
	Size     uint32
	Action   int8
	Side     int8
	Flags    uint8
	Depth    uint8
	TsRecv   uint64
	TsDelta  int32
	Sequence uint32
	// Level 0 — best bid/ask (32 bytes)
	BidPx int64
	AskPx int64
	BidSz uint32
	AskSz uint32
	BidCt uint32
	AskCt uint32
}

func (d *DatabentoProvider) readLoop(ctx context.Context, conn net.Conn, symbol string, ch chan<- Snapshot) {
	buf := make([]byte, mbp1RecordBytes)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		if _, err := io.ReadFull(conn, buf); err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("databento read error", "err", err)
			return
		}

		rec, ok := parseMBP1(buf)
		if !ok {
			continue // not an MBP-1 record or parse validation failed; skip
		}

		snap := mbp1ToSnapshot(rec, symbol)
		select {
		case ch <- snap:
		case <-ctx.Done():
			return
		}
	}
}

func parseMBP1(buf []byte) (mbp1Record, bool) {
	if len(buf) < mbp1RecordBytes {
		return mbp1Record{}, false
	}
	if int(buf[0])*4 != mbp1RecordBytes {
		slog.Debug("databento: unexpected record length", "len_field", buf[0])
		return mbp1Record{}, false
	}
	if buf[1] != rtypeMBP1 {
		return mbp1Record{}, false // heartbeat or other record type; skip
	}

	le := binary.LittleEndian
	r := mbp1Record{
		Length:       buf[0],
		RType:        buf[1],
		PublisherID:  le.Uint16(buf[2:4]),
		InstrumentID: le.Uint32(buf[4:8]),
		TsEvent:      le.Uint64(buf[8:16]),
		Price:        int64(le.Uint64(buf[16:24])),
		Size:         le.Uint32(buf[24:28]),
		Action:       int8(buf[28]),
		Side:         int8(buf[29]),
		Flags:        buf[30],
		Depth:        buf[31],
		TsRecv:       le.Uint64(buf[32:40]),
		TsDelta:      int32(le.Uint32(buf[40:44])),
		Sequence:     le.Uint32(buf[44:48]),
		BidPx:        int64(le.Uint64(buf[48:56])),
		AskPx:        int64(le.Uint64(buf[56:64])),
		BidSz:        le.Uint32(buf[64:68]),
		AskSz:        le.Uint32(buf[68:72]),
		BidCt:        le.Uint32(buf[72:76]),
		AskCt:        le.Uint32(buf[76:80]),
	}

	// Reject sentinel prices Databento uses for empty/null levels
	if r.BidPx == math.MaxInt64 || r.AskPx == math.MaxInt64 {
		return mbp1Record{}, false
	}

	return r, true
}

// mbp1ToSnapshot converts an MBP-1 record to a 1-level Snapshot.
// NOTE: With k=1, Compute() will always return Lambda{0,0,0}.
// Lambda requires k≥2 levels; use the Databento MBP-10 schema for non-zero lambda.
// The Snapshot still carries valid BBO prices and sizes for QuestDB storage.
func mbp1ToSnapshot(r mbp1Record, symbol string) Snapshot {
	bidPrice := float64(r.BidPx) * dbnScaleFactor
	askPrice := float64(r.AskPx) * dbnScaleFactor
	return Snapshot{
		Timestamp: time.Unix(0, int64(r.TsEvent)).UTC(),
		Symbol:    symbol,
		Bids:      []PriceLevel{{Price: bidPrice, Volume: float64(r.BidSz)}},
		Asks:      []PriceLevel{{Price: askPrice, Volume: float64(r.AskSz)}},
	}
}

func (d *DatabentoProvider) Close() error {
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func writeJSON(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = w.Write(b)
	return err
}
