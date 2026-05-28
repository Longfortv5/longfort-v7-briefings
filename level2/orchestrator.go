package level2

// Orchestrator — wires all data sources into QuestDB and starts the
// Qwen director loop. This is the single entry point to run on GB10.
//
// Data flow:
//
//   Rithmic R|API+  ──► RithmicProvider  ──┐
//   Databento MBP10 ──► DatabentoProvider ──┤──► QuestDBWriter (ILP :9009)
//   FlashAlpha REST ──► FlashAlphaPoller  ──┤         │
//   IG REST         ──► IGClient          ──┘         │
//                                                      ▼
//                                          Qwen (vLLM :8000) ◄─── 1-min ticker
//                                          LLMDirector.EvaluateRegime()
//                                                      │
//                                                      ▼
//                                          CircuitBreaker / execution gate
//
// All credentials are injected via OrchestratorCfg — load from env vars,
// never hardcode. See LoadCfgFromEnv() below.

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq" // postgres wire driver for QuestDB reads
)

// OrchestratorCfg holds all credentials and addresses for the full stack.
type OrchestratorCfg struct {
	// QuestDB
	QuestDBILPAddr string // ILP write port, e.g. "gb10.local:9009"
	QuestDBPGAddr  string // Postgres wire read port, e.g. "gb10.local:8812"

	// Qwen / vLLM
	VLLMEndpoint string // e.g. "http://gb10.local:8000"
	QwenModel    string // e.g. "Qwen/Qwen3-Coder-30B-A3B-Instruct"

	// Rithmic R|API+ (needs gRPC bridge sidecar)
	Rithmic RithmicCfg

	// Databento (needs databento-go SDK added to go.mod)
	Databento DatabentoCfg

	// FlashAlpha GEX REST
	FlashAlpha FlashAlphaCfg

	// IG REST (DFB execution + spot snapshot)
	IG IGConfig

	// Instrument map: internal name → IG epic
	// e.g. {"NQ": "IX.D.NASDAQ.IFD.IP", "DAX": "IX.D.DAX.IFD.IP"}
	IGEpics map[string]string
}

// LoadCfgFromEnv builds an OrchestratorCfg from environment variables.
// Set these on GB10 before running — never put credentials in source.
func LoadCfgFromEnv() OrchestratorCfg {
	return OrchestratorCfg{
		QuestDBILPAddr: envOr("QUESTDB_ILP_ADDR", "localhost:9009"),
		QuestDBPGAddr:  envOr("QUESTDB_PG_ADDR", "localhost:8812"),
		VLLMEndpoint:   envOr("VLLM_ENDPOINT", "http://localhost:8000"),
		QwenModel:      envOr("QWEN_MODEL", "Qwen/Qwen3-Coder-30B-A3B-Instruct"),
		FlashAlpha: FlashAlphaCfg{
			APIKey:  os.Getenv("FLASHALPHA_API_KEY"),
			BaseURL: envOr("FLASHALPHA_BASE_URL", "https://api.flashalpha.com/v1"),
		},
		IG: IGConfig{
			APIKey:     os.Getenv("IG_API_KEY"),
			Identifier: os.Getenv("IG_IDENTIFIER"),
			Password:   os.Getenv("IG_PASSWORD"),
			BaseURL:    envOr("IG_BASE_URL", "https://api.ig.com/gateway/deal"),
		},
		IGEpics: map[string]string{
			"NQ":     "IX.D.NASDAQ.IFD.IP",
			"DAX":    "IX.D.DAX.IFD.IP",
			"NIKKEI": "IX.D.NIKKEI.IFD.IP",
		},
		Rithmic: RithmicCfg{
			GatewayHost: envOr("RITHMIC_GATEWAY", "rituz00100.01.rithmic.com:443"),
			SystemName:  envOr("RITHMIC_SYSTEM", "Rithmic 01 Chicago"),
			UserID:      os.Getenv("RITHMIC_USER"),
			Password:    os.Getenv("RITHMIC_PASSWORD"),
			FCMID:       os.Getenv("RITHMIC_FCMID"),
			IBID:        os.Getenv("RITHMIC_IBID"),
		},
		Databento: DatabentoCfg{
			APIKey:  os.Getenv("DATABENTO_API_KEY"),
			Dataset: envOr("DATABENTO_DATASET", "GLBX.MDP3"),
			Schema:  "mbp-10",
		},
	}
}

// RunOrchestrator is the main entry point. Call from main().
// Blocks until SIGINT/SIGTERM or ctx cancellation.
func RunOrchestrator(parentCtx context.Context, cfg OrchestratorCfg) error {
	ctx, cancel := signal.NotifyContext(parentCtx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// ── QuestDB ILP writer ────────────────────────────────────────────────────
	qdb, err := NewQuestDBWriter(cfg.QuestDBILPAddr)
	if err != nil {
		return fmt.Errorf("questdb writer: %w", err)
	}
	defer qdb.Close()
	slog.Info("questdb ILP connected", "addr", cfg.QuestDBILPAddr)

	// ── QuestDB postgres connection (director reads) ──────────────────────────
	pgDSN := fmt.Sprintf("host=%s port=8812 user=admin password=quest dbname=qdb sslmode=disable",
		cfg.QuestDBPGAddr)
	db, err := sql.Open("postgres", pgDSN)
	if err != nil {
		return fmt.Errorf("questdb pg: %w", err)
	}
	defer db.Close()

	// ── Circuit breaker ───────────────────────────────────────────────────────
	breaker := NewCircuitBreaker(func(e TripEvent) {
		slog.Error("CIRCUIT TRIPPED", "reason", e.Reason, "score", e.Score)
		// TODO: wire Discord alert here
	})

	// ── Qwen director ─────────────────────────────────────────────────────────
	director := NewLLMDirector(db, cfg.VLLMEndpoint, cfg.QwenModel)
	slog.Info("qwen director initialised", "model", cfg.QwenModel, "endpoint", cfg.VLLMEndpoint)

	// ── FlashAlpha GEX poller (60 s — GEX is options-based, not tick-by-tick) ─
	faPoller := NewFlashAlphaPoller(cfg.FlashAlpha)
	for _, asset := range []string{"NQ", "DAX", "NIKKEI"} {
		a := asset
		go func() {
			for gex := range faPoller.Poll(ctx, a, 60*time.Second) {
				if err := qdb.WriteGEX(a, gex); err != nil {
					slog.Warn("gex write error", "asset", a, "err", err)
				}
			}
		}()
	}
	slog.Info("flashalpha GEX pollers started")

	// ── IG spot poller (5 s) ──────────────────────────────────────────────────
	igClient := NewIGClient(cfg.IG)
	if err := igClient.Authenticate(ctx); err != nil {
		slog.Warn("IG initial auth failed — spot snapshots will be unavailable", "err", err)
	} else {
		for asset, epic := range cfg.IGEpics {
			a, e := asset, epic
			go igClient.Poll(ctx, e, a, 5*time.Second, qdb)
		}
		slog.Info("IG spot pollers started", "epics", cfg.IGEpics)
	}

	// ── Rithmic L2 stream ─────────────────────────────────────────────────────
	// Needs gRPC bridge sidecar running on GB10.
	// Once the bridge is running:
	//   rithmicProvider := NewRithmicProvider(cfg.Rithmic)
	//   go runL2Ingest(ctx, rithmicProvider, "NQ", 10, qdb, breaker)
	slog.Warn("rithmic L2 stream: STUB — wire gRPC bridge to activate")

	// ── Databento MBP-1 stream (live — stdlib TLS TCP, no external SDK) ───────
	// Schema is "mbp-1" (Level 1). Lambda values will be 0 at k=1.
	// Switch DatabentoCfg.Schema to "mbp-10" for full Kyle's Lambda.
	if cfg.Databento.APIKey != "" {
		gexFeedForNQ := faPoller.Poll(ctx, "NQ", 60*time.Second)
		databentoProvider := NewDatabentoProvider(cfg.Databento)
		go func() {
			snapCh, err := databentoProvider.Subscribe(ctx, "NQ.c.0", 1)
			if err != nil {
				slog.Error("databento subscribe failed", "err", err)
				return
			}
			sigCh := RunTickLambdaPipeline(ctx, snapCh, gexFeedForNQ, qdb, 1, 10, DefaultConfig())
			for ts := range sigCh {
				if ts.Signal.Armed {
					slog.Info("L1 signal armed",
						"symbol", ts.Tick.Symbol,
						"mid", ts.Tick.Mid(),
						"spread", ts.Tick.Spread(),
						"imbalance", ts.Imbalance,
					)
				}
				_ = breaker.AssertSafeToTrade() // checked at intake cycle level
			}
		}()
		slog.Info("databento MBP-1 stream started", "symbol", "NQ.c.0")
	} else {
		slog.Warn("databento stream: DATABENTO_API_KEY not set — skipped")
	}

	// ── 1-minute Qwen director loop (Sunday 21:00 – Friday 21:00 UTC) ─────────
	directorTicker := time.NewTicker(time.Minute)
	defer directorTicker.Stop()
	go func() {
		for {
			select {
			case t := <-directorTicker.C:
				if !IsGlobalSystemAvailable(t) {
					continue
				}
				if err := director.EvaluateRegime(ctx); err != nil {
					slog.Error("qwen regime eval failed", "err", err)
					continue
				}
				regime := director.CurrentRegime()
				slog.Info("regime updated", "regime", regime)
				if regime == RegimeEmergency {
					breaker.Trip("regime=PAUSED")
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// ── Master execution intake (100 ms) ─────────────────────────────────────
	intakeTicker := time.NewTicker(100 * time.Millisecond)
	defer intakeTicker.Stop()

	slog.Info("orchestrator running — session window Sunday 21:00 – Friday 21:00 UTC")
	for {
		select {
		case <-ctx.Done():
			slog.Info("shutdown — orchestrator halting")
			return nil
		case t := <-intakeTicker.C:
			runIntakeCycle(ctx, t, breaker, director)
		}
	}
}

// runL2Ingest consumes an L2 Provider stream and writes snapshots to QuestDB.
// Activate once Rithmic bridge or Databento SDK is wired.
func runL2Ingest(
	ctx context.Context,
	provider Provider,
	symbol string,
	depth int,
	qdb *QuestDBWriter,
	breaker *CircuitBreaker,
) {
	defer provider.Close()

	ch, err := provider.Subscribe(ctx, symbol, depth)
	if err != nil {
		slog.Error("L2 subscribe failed", "symbol", symbol, "err", err)
		return
	}

	rolling := NewRollingLambda(100) // assume ~100 ticks/s from exchange
	cfg := DefaultConfig()
	var lastGEX GEXData // populated from FlashAlpha channel; pass in for full wiring

	for snap := range ch {
		if !breaker.AssertSafeToTrade() {
			continue
		}

		short, long, err := rolling.Update(snap, depth)
		if err != nil {
			continue
		}
		l, err := Compute(snap, depth)
		if err != nil {
			continue
		}
		sig, err := Evaluate(0, snap, depth, rolling, cfg) // OFI=0 until OFI source wired
		if err != nil {
			continue
		}

		state := GetAssetSessionState(symbol, snap.Timestamp)
		if err := qdb.WriteSnapshot(symbol, snap, l, short, long, sig, state, depth, lastGEX); err != nil {
			slog.Warn("snapshot write error", "symbol", symbol, "err", err)
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
