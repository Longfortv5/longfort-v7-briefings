// Reference implementation for cmd/jack/main.go.
// Copy into your private bot repo; adjust import paths and DB wiring.

package level2

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// ── Session state (mirrors director.GetAssetSessionState) ────────────────────

func IsGlobalSystemAvailable(t time.Time) bool {
	utc := t.UTC()
	weekday := utc.Weekday()
	hr, _, _ := utc.Clock()
	if weekday == time.Friday && hr >= 20 {
		return false
	}
	if weekday == time.Saturday {
		return false
	}
	if weekday == time.Sunday && hr < 22 {
		return false
	}
	if hr == 21 {
		return false
	}
	return true
}

func GetAssetSessionState(asset string, t time.Time) SessionState {
	if !IsGlobalSystemAvailable(t) {
		return StateClosed
	}
	utc := t.UTC()
	hr, min, _ := utc.Clock()
	switch asset {
	case "DAX":
		if hr == 7 && min < 20 {
			return StateClosed
		}
		if hr >= 7 && hr < 21 {
			return StateOpen
		}
		return StateClosed
	case "NIKKEI":
		if hr == 1 {
			return StateClosed
		}
		if hr > 1 && hr < 21 {
			return StateOpen
		}
		return StateClosed
	case "NQ":
		nikkeiWindow := hr >= 1 && hr < 21
		daxWindow := hr >= 7 && hr < 21
		if nikkeiWindow || daxWindow {
			return StateShadow
		}
		return StateOpen
	}
	return StateClosed
}

// ── NY session window (UTC) ───────────────────────────────────────────────────
// 09:30–16:00 ET = 14:30–21:00 UTC (winter). Add DST offset if needed.
const (
	nyOpenUTCMinutes  = 14*60 + 30 // 14:30 UTC
	nyCloseUTCMinutes = 21 * 60    // 21:00 UTC
)

func isNYOpen(t time.Time) bool {
	utc := t.UTC()
	h, m, _ := utc.Clock()
	total := h*60 + m
	return total >= nyOpenUTCMinutes && total < nyCloseUTCMinutes
}

// ── Main ──────────────────────────────────────────────────────────────────────

func JackMain() {
	// Graceful shutdown on SIGINT / SIGTERM
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Circuit breaker — wire real Discord/QuestDB notifier here
	breaker := NewCircuitBreaker(func(e TripEvent) {
		slog.Error("CIRCUIT TRIPPED",
			"reason", e.Reason,
			"score", e.Score,
			"ts", e.Time.Format(time.RFC3339),
		)
		// TODO: discord.PostEmergency(e)
		// TODO: questdb.LogTripEvent(e)
	})

	// Director — wire real *sql.DB and vLLM endpoint from env/config
	// db := mustOpenQuestDB(os.Getenv("QUESTDB_DSN"))
	// directorEngine := NewLLMDirector(db, os.Getenv("VLLM_ENDPOINT"))
	// Below uses nil DB as placeholder — swap before live arming
	directorEngine := NewLLMDirector(nil, "http://gb10.local:8000")

	// 5-minute regime update goroutine
	regimeTicker := time.NewTicker(5 * time.Minute)
	defer regimeTicker.Stop()
	go func() {
		for {
			select {
			case <-regimeTicker.C:
				if err := directorEngine.EvaluateRegime(ctx); err != nil {
					slog.Error("regime evaluation failed", "err", err)
					// Do not trip the breaker on evaluation error — stale regime
					// is safer than a spurious halt. Log and continue.
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Master execution intake — gated by ticker, not a spin loop
	intakeTicker := time.NewTicker(100 * time.Millisecond)
	defer intakeTicker.Stop()

	slog.Info("jack execution loop started")
	for {
		select {
		case <-ctx.Done():
			slog.Info("shutdown signal — halting execution loop")
			return
		case t := <-intakeTicker.C:
			runIntakeCycle(ctx, t, breaker, directorEngine)
		}
	}
}

// ── Intake cycle ──────────────────────────────────────────────────────────────

func runIntakeCycle(
	ctx context.Context,
	now time.Time,
	breaker *CircuitBreaker,
	eng *LLMDirector,
) {
	// Rule 1: Circuit breaker + emergency regime — halt immediately
	if !breaker.AssertSafeToTrade() {
		return
	}
	if eng.CurrentRegime() == RegimeEmergency {
		// Regime emergency trips the breaker so Rule 1 catches it next cycle;
		// trip here ensures the notification fires and the state is consistent.
		breaker.Trip("regime=PAUSED")
		return
	}

	// Rule 2: Global and asset session gate
	if GetAssetSessionState("NQ", now) == StateClosed {
		return
	}

	// Rule 3: NY open vs off-hours execution path
	if isNYOpen(now) {
		runNYOpenStrategy(ctx, eng)
	} else {
		runOffHoursL2Strategy(ctx)
	}
}

// ── NY open: GEX-informed, regime-bounded execution ───────────────────────────

func runNYOpenStrategy(ctx context.Context, eng *LLMDirector) {
	// Use CurrentRegime() — never read CurrentState directly (mutex bypass).
	// Only trade in the direction the regime permits.
	switch eng.CurrentRegime() {
	case RegimeLongTrend:
		// Ask side is thin, dealers short gamma, upward momentum self-reinforcing.
		// Enter long. GEX walls act as profit targets, not stop zones.
		// [long entry logic]

	case RegimeShortTrend:
		// Bid side is thin, dealers short gamma, downward momentum self-reinforcing.
		// Enter short. Put wall is magnetic, not a floor.
		// [short entry logic]

	case RegimePinned:
		// Dealers long gamma, spot anchored between put wall and call wall.
		// Mean-reversion only. Fade moves back toward zero-gamma level.
		// Tight stops — any break of the walls flips to trend mode next cycle.
		// [mean-reversion logic]

	default:
		// PAUSED or unrecognised — no entries this cycle
	}
}

// ── Off-hours: raw L2 microstructure, options flow bypassed ──────────────────

func runOffHoursL2Strategy(ctx context.Context) {
	// GEX/dealer-gamma data is stale outside NY hours — ignore it entirely.
	// Entry condition is purely structural:
	//   signal_armed = true  (from level2.Evaluate)
	//   i.e. OFI spike + λ_ask spike (≥ 2× 10s EWMA) + Λ_ratio > 1
	//
	// Wire a *RollingLambda and call Evaluate() on each incoming Rithmic snapshot.
	// When Signal.Armed == true, fire the DFB order on IG before the
	// primary exchange reprices.
	//
	// [wire level2.RollingLambda + level2.Evaluate here]
}
