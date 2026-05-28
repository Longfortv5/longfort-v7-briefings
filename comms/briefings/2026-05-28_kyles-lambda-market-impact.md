# Kyle's Lambda (λ) — Market Impact & Liquidity Depth Briefing

**Prepared by:** Perplexity Computer (Intelligence Chief)
**For:** HS + Chief (Opus 4.7)
**Date:** 28 May 2026
**Topic:** Ex-Ante Lambda Calculation from Level 2 Order Book Data

---

## 1. Concept Overview

Kyle's Lambda (λ) is the fundamental metric for market impact and liquidity depth, originating from Albert Kyle's 1985 paper *"Continuous Auctions and Insider Trading."*

**The central question it answers:** How much does the price move for every unit of volume executed?

| Formulation | Method | Timing |
|---|---|---|
| **Kyle (1985) original** | Regression of price changes against net order flow (signed volume) | *Ex-post* — calculated after fills |
| **Modern ex-ante** | Direct measurement of resting liquidity at each price tier from L2 snapshot | *Ex-ante* — calculated before trade execution |

The bot uses the **ex-ante formulation**, streaming L2 snapshots from Databento (MBP-10) into QuestDB and computing λ continuously in real time.

---

## 2. Step 1 — Extract the Level 2 Vectors

At a given microsecond `t`, pull the top `k` levels of the limit order book snapshot.

**Ask side (resistance to aggressive buying):**

| Symbol | Description |
|---|---|
| `P_{a,i}` | Price of ask at level `i` |
| `V_{a,i}` | Volume (size) of ask at level `i` |

**Bid side (resistance to aggressive selling):**

| Symbol | Description |
|---|---|
| `P_{b,i}` | Price of bid at level `i` |
| `V_{b,i}` | Volume (size) of bid at level `i` |

---

## 3. Step 2 — The Ex-Ante Lambda Calculation

### Ask-side Lambda (λ_ask)

Measures the market's resistance to aggressive buying pressure.

Price deviation is measured from the best ask `P_{a,1}` to depth `k`, divided by cumulative volume required to sweep those levels:

```
λ_ask = (P_{a,k} − P_{a,1}) / Σ V_{a,i}   for i = 1 → k
```

### Bid-side Lambda (λ_bid)

Measures the market's resistance to aggressive selling pressure. Prices descend so the deviation is inverted:

```
λ_bid = (P_{b,1} − P_{b,k}) / Σ V_{b,i}   for i = 1 → k
```

### Units

Both lambdas express **price ticks per unit of volume**. A higher value means a thinner book — less volume is required to move price by one tick.

### Interpreting the Output

| Lambda Value | Book State | Implication |
|---|---|---|
| **Low λ** | Thick, deep book | Massive aggressive volume required to move price one tick |
| **High λ** | Thin, fragile book | Standard-sized market order chews through liquidity; price gaps |

---

## 4. Step 3 — Exploiting Asymmetric Liquidity

Markets are rarely symmetrical. Comparing `λ_ask` to `λ_bid` reveals the path of least resistance.

### Lambda Ratio

```
Λ_ratio = λ_ask / λ_bid
```

| Λ_ratio | Interpretation |
|---|---|
| `> 1` | Ask side is **thinner** than bid. Upward movement faces structural friction; **downward move is barricaded** by resting bids. Path of least resistance: **UP**. |
| `< 1` | Bid side is **thinner** than ask. Downward movement faces structural friction; **upward move is barricaded** by resting offers. Path of least resistance: **DOWN**. |
| `≈ 1` | Book is structurally balanced. No directional edge from lambda alone. |

---

## 5. Tactical Execution Application — OFI + Lambda Confluence

Lambda becomes offensive (not defensive) when combined with **Order Flow Imbalance (OFI)**.

### The Setup

```
OFI spike (aggressive market buying detected)
    AND
λ_ask spiking (ask liquidity vanishing in real time)
    →
Central order book is structurally primed to snap UPWARD
```

### The Edge (DFB / IG Execution)

Because execution occurs on **IG for DFB instruments**, a lambda-confirmed setup creates a micro-latency dislocation:

1. The underlying central exchange (e.g. CME NQ) is mathematically buckling under volume — λ_ask confirms the ask side has been swept thin.
2. The bot executes the DFB position on IG's platform **before** IG's synthetic pricing algorithms fully reprice to reflect the liquidity vacuum on the primary exchange.
3. The dealer's spread-widening or quote-shifting lags the central book by measurable milliseconds; lambda quantifies exactly how weak the opposing side is before the trigger is pulled.

**Lambda is not a risk manager here. It is a structural weakness detector.**

---

## 6. QuestDB Integration — Continuous Rolling λ Arrays

When streaming unadulterated exchange feeds into QuestDB, the lambda calculation transforms from a one-shot snapshot into a continuous time-series.

### Recommended Schema (illustrative)

```sql
CREATE TABLE lambda_rolling (
    ts         TIMESTAMP,
    symbol     SYMBOL,
    lambda_ask DOUBLE,
    lambda_bid DOUBLE,
    lambda_ratio DOUBLE,
    depth_k    INT,
    best_ask   DOUBLE,
    best_bid   DOUBLE
) TIMESTAMP(ts) PARTITION BY DAY;
```

### Rolling Calculation Window

- Recalculate on every L2 update (tick-by-tick) for real-time responsiveness.
- Maintain a rolling 1-second and 10-second EWMA of both lambdas to distinguish transient spikes from structural thinning.
- Alert threshold: if `λ_ask` crosses `2× its 10s EWMA` **and** OFI is positive, signal is armed.

---

## 7. Signal Hierarchy — Where Lambda Sits

| Layer | Metric | Role |
|---|---|---|
| Regime | VPIN, weather score | Is the market tradeable at all? |
| Direction | OFI | Which way is aggressive flow pushing? |
| Structural | **λ_ask / λ_bid** | **Is the book structurally weak on the side OFI is attacking?** |
| Execution | Λ_ratio | Confirm path of least resistance before firing |
| Risk | Position counter, max=3 gate | Hard stop regardless of signal strength |

Lambda gates the execution step. OFI without lambda confirmation is an incomplete signal — the book may look directional but be defended by thick resting orders that will absorb the move and snap back.

---

## 8. Known Constraints & Hazards

| Item | Detail |
|---|---|
| **Data source** | MBP-10 (10-level book) from Databento. `k` is capped at 10. Sweeps deeper than 10 levels are not captured; lambda will underestimate impact for very large size. |
| **Latency** | QuestDB ingest latency must be sub-millisecond for lambda to reflect true book state at signal time. Any lag means lambda is stale. |
| **Spoofing / layering** | Large resting orders that vanish on approach will cause lambda to underestimate ask/bid thinness. Lambda is a snapshot, not a prediction of order persistence. |
| **DFB spread widening** | IG can widen its DFB spread independently of the central book. Lambda on the primary exchange does not account for IG's own spread; slippage on IG can be higher than lambda implies. |
| **Dry-run lock** | Bot remains in DRY_RUN. Lambda signals are logged but **no live arming** until HS + Chief both approve. |

---

## 9. Routing

- **HS:** Review §5 (tactical execution application) and §8 (constraints). Confirm lambda integration is aligned with DFB execution approach.
- **Chief (Opus 4.7):** Ingest this briefing as the canonical lambda reference. When OFI fires, cross-check `λ_ask`, `λ_ratio`, and the rolling EWMA before classifying signal as armed.
- **CC (MacBook Pro):** Implement QuestDB schema in §6 and wire rolling lambda computation into the signal pipeline. Align with OFI gate logic.
- **PC (me):** Deliver updated lambda diagnostics in next pre-open briefing if live data is flowing.

---

*End of briefing. No fabrication. Mathematics is standard Kyle (1985) + modern L2 extension. No positions taken.*
