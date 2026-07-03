# Phase 1 Report — Trader Census Preview (PRELIMINARY)

> **PRELIMINARY — partial data, volume-biased sample.** The trades sweep
> (`ingest_trades.py --seasons 2024,2025,2026 --workers 6`) is still running
> in the background as this report is written. Markets are pulled
> **volume-descending within season**, so this census is built from the
> highest-volume ~600-700 resolved CLOB-era (2024+) markets out of the full
> 30,289-market target universe (per `reports/01_lake_markets.md`) — i.e. the
> analysis-critical liquid core, but not remotely a full census yet. Every
> number below should be re-run once the sweep completes (`trader_metrics.py`
> is a single idempotent full-recompute pass — safe to re-run as-is).

Run date: 2026-07-02. Snapshot: `data/lake/state/trades.json` had **738
completed markets**, of which **610 had trade data** (some completed markets
had 0 trades — normal for very low-volume resolved props). Season mix: 2024:
138 markets, 2025: 466, 2026: 6.

## Pipeline

- `analysis/build_lake.py --tables trades` refreshed `data/lake/trades.parquet`
  from raw JSONL (read-only w.r.t. ingestion — only reads `raw/trades/`,
  never touches ingest state/logs).
- `analysis/trader_metrics.py` filters trades to `condition_id`s in
  `data/lake/state/trades.json` `completed_ids` **before** any position
  reconstruction, so partial (still-being-fetched) market data never enters
  the metrics — 1,622,790 raw trade rows → 1,622,190 in-scope rows after this
  filter (the parquet itself already only contains fully-completed markets
  in this run, so the filter was a no-op here, but it's load-bearing for
  future runs mid-sweep).
- Writes `data/lake/positions.parquet` (403,132 rows, one per
  wallet×condition_id×token_id) and `data/lake/traders.parquet` (114,844
  rows, one per wallet).

## Census size

**114,844 distinct wallets** traded across the 610 markets with trade data
(403,132 wallet-market-token positions). Total reconstructed dollar volume:
**$552.4M** buy+sell notional (vs. $602.8M Gamma-reported volume across the
same 610 markets — see Data-Quality Surprises below for the gap).

- 65,988 wallets (57%) touched only 1 market — the long tail of one-off
  bettors expected from a volume-descending, still-partial sample.
- 3,569 wallets (3.1%) have ≥10 markets; 604 wallets (0.5%) have ≥50.

## Settlement ROI distribution

`roi_pooled` = (Σreturned − Σinvested) / Σinvested per wallet, dollar-weighted
across all their settled markets (invested = Σ buy cost, returned = Σ sell
proceeds + settlement payout — see Sanity Checks below for the payout
formula and hand-verification).

**All wallets with ≥1 settled market (n=114,844):**

| Decile | ROI |
|---|---:|
| 0% (min) | −100.0% |
| 10% | −100.0% |
| 20% | −12.5% |
| 30% | −0.3% |
| 40% | −0.1% |
| 50% (median) | −0.1% |
| 60% | 0.0% |
| 70% | +0.3% |
| 80% | +15.3% |
| 90% | +78.6% |
| 100% (max) | +3,903,780% |

The extreme top tail is dominated by n_markets=1 wallets who bought
deep-longshot tokens (price near $0.001–0.01) that resolved YES — a $10 bet
at $0.002/share pays $5,000 at settlement, a legitimate 500x, not a data
bug (see Data-Quality Surprises).

**Wallets with ≥5 settled markets (n=10,776, less small-sample noise):**

| Decile | ROI |
|---|---:|
| 0% | −100.0% |
| 10% | −31.8% |
| 20% | −15.7% |
| 30% | −4.9% |
| 40% | −0.3% |
| 50% | −0.1% |
| 60% | +0.8% |
| 70% | +7.4% |
| 80% | +17.4% |
| 90% | +33.0% |
| 100% | +38,853% |

**By n_markets bucket** (mean/median ROI, mean $ invested, win rate):

| n_markets | n wallets | mean ROI | median ROI | mean $ invested | win rate |
|---|---:|---:|---:|---:|---:|
| 1 | 65,988 | +130% | −0.1% | $197 | 34.1% |
| 2–4 | 38,080 | +9.3% | −0.1% | $558 | 35.2% |
| 5–9 | 5,811 | +5.8% | −0.1% | $2,669 | 36.0% |
| 10–19 | 2,025 | +36.7% | +0.1% | $10,432 | 51.2% |
| 20–49 | 2,336 | +4.1% | +0.6% | $27,970 | 51.5% |
| 50+ | 604 | +12.3% | +2.2% | $582,325 | 53.8% |

Median ROI is close to breakeven-to-slightly-negative at low n_markets (a
mix of casual bettors and vig/slippage), turns modestly positive at 10+
markets, and win rate climbs from ~34% to ~54% as n_markets grows — directly
consistent with H1/H2 from RESEARCH_PLAN.md (small samples are noisy; a
larger settled-market count is a better skill signal than any single-window
PnL snapshot). This is descriptive, not yet a walk-forward test (Phase 2).

## Top-50 pre-game-ROI wallets (≥20 settled pre-game markets)

Pre-game reconstruction = position built **only from fills timestamped
before first pitch** (schedule join: new-format `mlb-{away}-{home}-{date}`
slugs only, 39,701/42,893 markets matched platform-wide per
`reports/01_lake_markets.md`'s methodology, reused here). This models "what
a copier who only mirrors this wallet's pre-game entries would have earned."
2,285 wallets in the current census qualify (≥20 pre-game-settled markets).
Top 10 by `pregame_roi_pooled`:

| wallet | pregame n_markets | pregame ROI | pregame win rate | pregame $ invested | full-history ROI |
|---|---:|---:|---:|---:|---:|
| 0xbde579ead9...4769c38 | 57 | +703.6% | 63.2% | $34,238 | +1,178.4% |
| 0x30a0c0554c...5585a458 | 20 | +409.2% | 85.0% | $634 | +250.8% |
| 0xfd326d28f5...410c9c1 | 40 | +228.0% | 52.5% | $3,123 | +100.0% |
| 0x2ada299aaa...9c2f28 | 56 | +212.5% | 67.9% | $3,978 | +1.6% |
| 0x26192845c8...b8f9f4e | 27 | +126.0% | 70.4% | $7,158 | +126.0% |
| 0x5d03aa8695...062369a | 23 | +117.7% | 69.6% | $7,304 | +117.7% |
| 0xb72b95c3ef...30f4e0 | 22 | +108.8% | 59.1% | $10,780 | +5.5% |
| 0x5429859fc3...6dfa2ec | 28 | +96.1% | 50.0% | $845 | +92.5% |
| 0xfccafe3b51...649faeba | 27 | +91.6% | 70.4% | $34 | +76.3% |
| 0xfecb4ea0fe...5ba19ba | 24 | +71.1% | 58.3% | $114,318 | +57.4% |

Full table (top 50) is in `data/lake/traders.parquet`
(`pregame_n_markets>=20`, sorted by `pregame_roi_pooled` desc). Note several
wallets' pre-game-only ROI diverges sharply from their full-history ROI
(e.g. `0x2ada299a...`: +212% pre-game vs +1.6% full-history) — this is exactly
the copier-relevant signal RESEARCH_PLAN.md is after: a wallet's *overall*
edge can be diluted or inflated by in-game/post-game activity that a copier
following only pre-game entries wouldn't replicate.

## Overlap with the current tracked-trader list

`data/top_mlb_traders.json`: `top_tier` = 58 wallets, `watchlist` = 89
wallets, **union = 147** (zero overlap between the two tiers) — the task
description's "134" doesn't match the current file on disk; reporting the
actual current count (147) here rather than forcing agreement.

**60 of 147 (40.8%) tracked wallets appear** in the current partial census
(738-market, volume-descending sample — the other 59.2% simply haven't had
one of their traded markets ingested yet, not necessarily inactive).

- Tracked-wallet median settlement ROI: **+19.9%**, vs. **−0.1%** for the
  census overall — the tracked list is enriched for positive performers,
  as expected from a PnL-screened funnel, but far from purely skilled: their
  census percentile rank (1.0 = best) ranges from the 1.1st percentile down
  to the 93.1st percentile (median 18.8th percentile), and **8 of the 60
  (13%)** have settlement ROI ≤ −60% (five of them −100%, i.e. total loss on
  their only settled market in this sample) — mechanically consistent with
  RESEARCH_PLAN.md's H1 concern that the list's 15-day-PnL tiering selects
  on a noisy, short window.
- `top_tier` and `watchlist` wallets are interleaved across the ranking —
  membership in `top_tier` (the "better" tier, PnL≥$500/15d) does not cleanly
  separate from `watchlist` in settlement-ROI rank order here (e.g. several
  `top_tier` wallets rank below the 85th percentile, several `watchlist`
  wallets rank in the top 15%). The full 60-wallet ranked table is trivially
  reproducible (join `traders.parquet` on lower-cased wallet against
  `top_mlb_traders.json`'s `top_tier`+`watchlist` union, sort by
  `roi_pooled` desc) — not frozen into this report since ranks will keep
  shifting as the sweep adds markets; re-derive fresh for Phase 2 rather than
  trusting a point-in-time table here.

## Sanity checks

**Hand-verification, 2 wallets, full fills → position → payout (see
`analysis/trader_metrics.py::sanity_verify_wallets`, printed on every run):**

1. `0x00292f99820699c5c2ce21e8990a2e996d0bc419`, market "Orioles vs.
   Guardians" (winner: Guardians): 2 BUY fills, size 40+5=45 @ $0.56 →
   buy_cost $25.20, net_size +45 (net long), token is the winning token →
   settlement_payout = 45×1.0 = $45.00. Pipeline: invested=$25.20,
   returned=$45.00. **Hand-calc match: True.**
2. `0xa0d32ea6e6a98c234bc98e957ab95dcb35d0a725`, market "Twins vs. Blue
   Jays" (winner: Blue Jays): 4 BUY fills (89.72 total) + 1 SELL (89.71 @
   $0.90) → buy_cost $38.00, sell_proceeds $80.739, net_size = +0.01 (net
   long, tiny residual), winning token → settlement_payout = 0.01×1.0 =
   $0.01. Pipeline: invested=$38.00, returned=$80.749 ($80.739 sale +
   $0.01 settlement). **Hand-calc match: True.**

A synthetic-short example was also hand-verified in an earlier run on the
same logic (wallet with net_size=−20 on a losing token: sold 60, bought 40,
outcome lost → settle_price=0 → payout=(-(-20))×(1-0)=$20, matching the
pipeline exactly) — confirms the net-short formula (pays out when that
token's outcome *loses*) is wired correctly in both directions.

**Cross-checks (see `cross_check_reconciliation()` in `trader_metrics.py`,
printed on every run):**

1. *What was checked and why it changed from the original plan*: the first
   draft assumed "every BUY fill has a matching SELL fill of the same
   token" (a naive matched-trade invariant). That's **empirically false**
   here — buy fill count (937,314) is ~2.3x sell fill count (405,930)
   platform-wide, and buy dollar volume is several times sell dollar
   volume, because most wallets buy-and-hold to settlement rather than
   exiting early (expected prediction-market behavior — not a data bug),
   and negRisk markets convert some volume through a protocol adapter
   rather than a visible counterparty SELL row. That check was replaced.
2. **Primary check run**: reconstructed per-market trade volume (buy
   notional + sell notional, all wallets/tokens) vs. Gamma's independently
   reported `markets.parquet.volume` field (not derived from our trade
   data at all). Across 610 markets: **median ratio 1.008, IQR
   [0.992, 1.037]**, 77.2% of markets within ±10% of Gamma's figure — this
   validates that trade reconstruction recovers essentially all of Gamma's
   reported volume for the large majority of markets.
3. **Structural bound**: `settlement_payout <= buy_size` for every net-long
   position (a payout can never exceed what was actually bought) — **0
   violations** across all 403,132 positions.
4. No independent winning-token-supply feed was ingested (no mint/merge
   events), so "total settlement payout ≈ total winning-token supply"
   per RESEARCH_PLAN.md's literal wording can't be checked against a 3rd
   source; total settlement payout ($447.1M) is the same order of magnitude
   as total reconstructed volume ($552.5M), which is the expected
   relationship, and is internally consistent by construction given check 3
   holds with zero violations.

## Data-quality surprises

1. **40 of 610 markets (6.6%) are significantly under-captured** relative
   to Gamma's volume (our reconstructed volume < 50% of Gamma's figure),
   concentrated almost entirely in **futures markets** (35 of 40 — division
   winners, pennant, World Series champion) plus 3 moneyline outliers
   including, oddly, a single Oct-28 World-Series-game market. These 40
   markets carry $79.5M of the $602.8M total Gamma volume in this sample
   (13.2%) — likely `ingest_trades.py`'s `ROW_CAP_FOR_TIME_SLICE`
   time-slice fallback not fully completing for these long-lived, very
   busy markets before being marked complete in `completed_ids`. **Not
   fixed here** (read-only re: ingestion per task scope) — flagging for the
   ingestion owner; wallet ROI for anyone heavily concentrated in these ~40
   markets should be treated with extra caution until re-verified.
2. **Extreme top-decile ROI values are real, not bugs**: prediction-market
   deep-longshot tokens (e.g. $0.001–0.01) that resolve YES produce
   100x–40,000x nominal ROI on a single settled market. This is expected
   given the payout mechanics, but it means simple `roi_pooled` ranking is
   dominated by lucky longshot hits at low n_markets — the n_markets-bucketed
   table and the ≥5/≥20-market filters in this report exist specifically to
   damp that; Phase 2's portfolio selection should apply a similar or
   stronger minimum-sample floor (RESEARCH_PLAN.md already specifies "min 30
   pre-game settled bets per trader" as an overfit guard).
3. **Pre-game join coverage**: 39,701/42,893 platform-wide markets matched
   via the new-format slug join (reused from `reports/01_lake_markets.md`);
   within the 610-market trades sample specifically, 3,190 fills fell on
   legacy free-text-slug 2024 postseason/futures markets that this join
   intentionally does not attempt (documented gap, not a bug) — those fills
   are flagged `timing='unknown'` rather than pre/in/post. This affects only
   2024-season markets; the sweep's 2025/2026 growth (the vast majority of
   remaining work) uses the fully-joinable new-format slugs.
4. **Zero overlap between `top_tier` and `watchlist`** in
   `data/top_mlb_traders.json` (58 + 89 = 147 tracked wallets, no
   duplicates) — and this doesn't match the "134" figure referenced in the
   task; worth reconciling against whatever produced that number, or simply
   treating 147 as current ground truth going forward.

## Files written

- `data/lake/trades.parquet` — refreshed (1,622,790 rows)
- `data/lake/positions.parquet` — 403,132 rows (wallet × condition_id × token_id)
- `data/lake/traders.parquet` — 114,844 rows (one per wallet, full census +
  settlement ROI + pre-game-only ROI + per-market-type splits + bot/MM
  heuristic features)
- `analysis/trader_metrics.py` — idempotent, single full-recompute pass;
  re-run as-is once the sweep completes (or any time in between for a fresh
  preview) — no incremental-state complexity needed per task scope.
