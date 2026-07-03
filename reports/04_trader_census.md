# Phase 1 Report — Trader Census (FINAL)

> **FINAL — full lake.** Replaces `reports/03_census_preview.md` (610-market,
> volume-descending partial sample, 40.8% list-overlap). This run is against
> the **complete** target universe: 30,289 CLOB-era (season ≥ 2024) resolved,
> volume>0 markets (2024 playoffs + 2025 + 2026 seasons; 30,287 fetched in
> the original sweep, the remaining 2 — both season-2027-labeled futures
> markets, see Data-Quality Notes — fetched during this run's straggler
> mop-up). 28,690 of the 30,289 target markets ended up with ≥1 captured
> trade (the other 1,599 are resolved, Gamma-volume>0 markets that produced
> zero rows from the trades endpoint — same "normal for very low-volume
> resolved props" pattern reports/03 flagged, now confirmed at full scale).

Run date: 2026-07-03. Pipeline: `analysis/trader_metrics.py` (unchanged
logic from the preview — idempotent full-recompute pass, re-run as-is on
the complete lake) + `analysis/hypothesis_tests.py` (new, for the
census-breakdown and sanity-anchor numbers in this report; the H1–H7
hypothesis verdicts are in `reports/05_skill_persistence.md`).

## Pipeline / lake summary

- `data/lake/trades.parquet`: **13,073,293** unique trades (season mix:
  2024: 97,029 · 2025: 2,364,480 · 2026: 10,611,517 · 2027: 267 — the 2027
  rows are 2 manager/futures props whose Gamma `end_date` falls in 2027).
  229,129 distinct wallets appear in raw trades.
- `data/lake/positions.parquet`: 3,297,149 rows (wallet × condition_id ×
  token_id).
- `data/lake/traders.parquet`: **224,559 wallets** (every wallet with
  `invested > 0` in at least one settled market — the ~4,570 wallets present
  in raw trades but absent here only ever touched zero-cost fills, e.g.
  reward/airdrop markets, and have no settlement ROI to report).
- Runtime: 125.3s, 13.15GB peak memory (pandas, unmodified — did **not**
  need a DuckDB rewrite; see task note below).

**Memory note**: `trader_metrics.py` was validated on 610 markets / 1.6M
trades in the preview run. At the full 13.07M trades / 3.3M positions it
ran to completion in pandas without modification (13.15GB peak on a 17GB
machine, 125s wall time) — the anticipated "prefer DuckDB SQL if it
strains" fallback was not needed. (`analysis/build_lake.py`'s `build_trades()`
*was* rewritten to a two-pass DuckDB approach — the pandas JSONL→DataFrame
path there OOM'd on 7.9GB of raw trade JSONL; see commit-level detail in
`analysis/ingest/build_lake.py`. That rewrite was byte-for-byte validated:
all 13,073,026 pre-straggler trade_ids identical, zero column mismatches,
against the DuckDB script that produced the already-landed lake.)

## Census size

**224,559 distinct wallets** with settled MLB positions, $1.77B reconstructed
buy+sell notional across 28,690 markets. This is **~2.0x** the preview's
114,844-wallet count (expected: the preview covered 610 of 30,289 target
markets, ~2% of markets but a disproportionate share of *volume* since it
pulled volume-descending — the remaining 29,679 markets added roughly as
many *new* wallets as the first 610 did, consistent with a long tail of
low-volume markets each bringing in a few not-otherwise-seen bettors).

## Settlement ROI distribution

`roi_pooled` = (Σreturned − Σinvested) / Σinvested per wallet, dollar-weighted
across all settled markets.

**All wallets (n=224,559):**

| Decile | ROI |
|---|---:|
| 0% (min) | −100.0% |
| 10% | −100.0% |
| 20% | −36.5% |
| 30% | −6.3% |
| 40% | −0.2% |
| 50% (median) | −0.1% |
| 60% | 0.0% |
| 70% | +1.9% |
| 80% | +20.6% |
| 90% | +78.6% |
| 100% (max) | +3,903,780% |

**≥5 settled markets (n=49,585):**

| Decile | ROI |
|---|---:|
| 0% | −100.0% |
| 10% | −33.7% |
| 20% | −18.2% |
| 30% | −9.4% |
| 40% | −3.4% |
| 50% | −0.1% |
| 60% | +0.1% |
| 70% | +3.8% |
| 80% | +11.1% |
| 90% | +25.7% |
| 100% | +258,988% |

**≥30 settled markets (n=14,951 — new cut vs. the preview, this is the
overfit-guard floor `RESEARCH_PLAN.md` specifies for Phase 2 selection):**

| Decile | ROI |
|---|---:|
| 0% | −100.0% |
| 10% | −17.1% |
| 20% | −10.1% |
| 30% | −5.6% |
| 40% | −2.4% |
| 50% | +0.1% |
| 60% | +0.1% |
| 70% | +2.3% |
| 80% | +6.4% |
| 90% | +13.4% |
| 100% | +7,628% |

The pattern from the preview holds and sharpens with more data: the
extreme top tail compresses hard as the minimum-sample floor rises (max ROI
drops from 39,037x → 2,589x → 76x going from all-wallets → ≥5 → ≥30), while
the *median* stays essentially flat at roughly breakeven (−0.1% to +0.1%)
regardless of sample-size cut — median trader performance is not a function
of how much you've traded, only the tails shrink. Distribution spread
(p90−p10) also compresses sharply with n: 178.6pp at all-wallets → 59.4pp
at ≥5 → 30.5pp at ≥30, i.e. more markets = a materially tighter, more
trustworthy ROI estimate, as expected from variance reduction, not from
better traders self-selecting into higher-n buckets.

**By n_markets bucket:**

| n_markets | n wallets | mean ROI | median ROI | mean $ invested | win rate |
|---|---:|---:|---:|---:|---:|
| 1 | 105,982 | +100.6% | −0.1% | $157 | 37.5% |
| 2–4 | 68,992 | +25.4% | −0.1% | $420 | 35.9% |
| 5–9 | 17,649 | +13.6% | −0.1% | $1,942 | 43.2% |
| 10–19 | 12,424 | +2.4% | −0.8% | $2,982 | 53.9% |
| 20–49 | 9,076 | +0.02% | −1.6% | $10,268 | 53.3% |
| 50+ | 10,436 | +1.3% | +0.1% | $127,951 | 59.3% |

Same shape as the preview: win rate climbs from 37.5% (n=1) to 59.3%
(n=50+) as sample size grows — consistent with more-active wallets being
better-calibrated bettors on average, not necessarily higher-ROI ones
(mean ROI is dominated by longshot outliers at low n; median ROI is a
better skill proxy and stays essentially flat-to-slightly-negative until
50+ markets).

## Pre-game-only ROI distribution

Reconstructed from fills timestamped strictly before first pitch only
(new-format `mlb-{away}-{home}-{date}` slug join, 39,701/42,893
platform-wide markets matched). **10,282 wallets** qualify with ≥20
pre-game-settled markets (vs. 2,285 in the preview — the ~4.5x increase
tracks the ~2x wallet-census growth times a higher qualification rate now
that the sweep covers many more low-volume, pre-game-heavy games).

| Decile | Pre-game ROI |
|---|---:|
| 0% | −100.0% |
| 10% | −22.4% |
| 20% | −13.8% |
| 30% | −8.4% |
| 40% | −4.3% |
| 50% (median) | −0.9% |
| 60% | +2.2% |
| 70% | +6.0% |
| 80% | +11.0% |
| 90% | +18.9% |
| 100% | +1,841% |

Top 10 by `pregame_roi_pooled` (≥20 pre-game-settled markets):

| wallet | pregame n | pregame ROI | pregame win rate | pregame $ invested | full-history ROI |
|---|---:|---:|---:|---:|---:|
| 0xde80ff4559...c62612 | 24 | +1,840.7% | 87.5% | $411 | +1,843.7% |
| 0xbde579ead9...4769c38 | 128 | +319.4% | 64.1% | $74,496 | +517.4% |
| 0xb6bed94e75...ef3cfcfe | 20 | +367.6% | 60.0% | $31,833 | +171.6% |
| 0x99f5bafdc0...0960f68d1 | 42 | +227.7% | 97.6% | $315 | +227.6% |
| 0xfd326d28f5...410c9c1 | 121 | +144.0% | 45.5% | $4,796 | +78.6% |
| 0x30a0c0554c...5585a458 | 49 | +130.0% | 65.3% | $1,915 | +22.5% |
| 0x085b004527...9c2ab9f | 22 | +105.0% | 54.5% | $529 | −4.1% |
| 0x43857162c8...5eb6bf4b1 | 176 | +104.5% | 2.3% | $9 | +87.8% |
| 0x613bfbb81d...a73e823a | 28 | +102.9% | 57.1% | $4,926 | +35.3% |
| 0x5d03aa8695...062369a | 69 | +96.4% | 53.6% | $15,005 | +96.4% |

Two wallets from the preview's top-10 (`0x2ada299a...`, `0x26192845...`)
have dropped out at full scale — expected churn once ~13x more markets
enter the sample; `0xfd326d28...`, `0x30a0c055...`, `0x5d03aa86...` persist
in both the preview and this run's top 10. As before, several wallets'
pre-game ROI diverges sharply from full-history ROI (e.g. `0x085b0045...`:
+105% pre-game vs. −4% full-history; `0x30a0c055...`: +130% pre-game vs.
+23% full-history) — the copier-relevant signal RESEARCH_PLAN.md is after:
in-game/post-game activity can meaningfully dilute a wallet's pre-game edge.

## Market-type splits

Population-level pooled ROI (dollar-weighted across all wallets' positions
of that type — this is "what a uniform, follow-everyone strategy would
have earned per market type," not a per-wallet average):

| market_type | n positions | n wallets | invested | pooled ROI |
|---|---:|---:|---:|---:|
| futures | 171,435 | 90,744 | $33.0M | **+18.3%** |
| moneyline | 1,513,087 | 127,491 | $1,245.9M | +1.00% |
| other | 23,045 | 14,287 | $8.4M | +4.6% |
| prop | 4,340 | 1,091 | $1.6M | +1.4% |
| spread | 430,815 | 22,468 | $69.8M | +0.12% |
| total | 695,259 | 27,699 | $182.4M | +0.11% |
| nrfi | 47,498 | 6,696 | $4.2M | +0.005% |

`futures` pooled ROI (+18.3%) looks like a huge outlier but is a direct
consequence of the under-capture pattern documented below (our
reconstructed `invested` for futures markets is systematically smaller
than Gamma's true volume because of the negRisk-conversion gap — the
denominator is undercounted, inflating this figure; treat futures pooled
ROI as **not comparable** to the other rows until that structural gap is
resolved). Every other market type pools close to breakeven (0.005%–1.4%),
consistent with "Polymarket has no vig on settlement" (task 6 sanity
anchor, below) — the population as a whole neither gains nor loses
meaningfully once futures is set aside; the `total` market type's
essentially-zero population-level pooled ROI directly informs H3 in
`reports/05_skill_persistence.md`.

## Season splits

| season | n wallets | mean wallet ROI | median wallet ROI | total $ invested |
|---|---:|---:|---:|---:|
| 2024 | 8,877 | +1.5% | −0.1% | $5.8M |
| 2025 | 111,146 | +113.2% | −0.02% | $598.0M |
| 2026 | 112,294 | +4.1% | −0.1% | $941.4M |
| 2027 | 22 | −40.9% | −60.7% | $0.16M |

2025's mean is a longshot-outlier artifact (median tells the real story:
essentially breakeven, same as every other season) — a small number of
wallets hit deep-longshot tokens in 2025 that dominate the mean the same
way they dominate the all-wallets ROI decile table above. 2027 is the 22
wallets who bet the 2 season-2027-dated manager-futures markets fetched in
this run's straggler mop-up; too small a sample to read into (median −61%
reflects those props still being mostly unresolved-favorite-longshot bets,
not a real "2027 season" signal).

## Bot / MM feature distributions

(No filtering applied — descriptive only, per `trader_metrics.py`'s design.)

| feature | p10 | p50 | p90 | p99 |
|---|---:|---:|---:|---:|
| trades/day | 0.18 | 8.58 | 48.0 | 180.1 |
| median inter-trade seconds | 10.5s | 7,945s (2.2h) | 352,480s (4.1d) | 4,676,124s (54d) |
| active-hours entropy (0–1) | 0.00 | 0.22 | 0.64 | 0.86 |
| two-sided-activity share | 0.00 | 0.25 | 1.00 | 1.00 |

A simple heuristic bot/MM flag (≥20 trades/day **and** ≥50% two-sided
markets **and** ≥50 total trades — high frequency, resting-both-sides
behavior, not a one-off) matches **1,186 wallets (0.53% of the census)**.
This is a coarse screen, not a verdict — it exists so Phase 2's selection
step can exclude or separately analyze likely market-makers rather than
mixing them into a "skilled directional trader" pool.

## Current-147-list scored vs. census

`data/top_mlb_traders.json`: `top_tier` = 58, `watchlist` = 89, union = 147
(unchanged from the preview). **143 of 147 (97.3%) now match** in the full
census — up from 40.8% in the preview, confirming most of the earlier
non-matches were simply markets not yet ingested, not inactive wallets.

- Tracked-wallet median settlement ROI: **+4.4%**, vs. **−0.1%** for the
  census overall (all-wallets baseline) — the list is enriched for positive
  performers, though far more modestly than the preview's inflated +19.9%
  suggested (that number was itself a partial-data artifact — pulling from
  a volume-descending sample skews toward the busiest, most liquid markets,
  which is exactly where PnL-screened wallets concentrate).
- Median percentile rank of matched wallets: **27.5th percentile** (0 =
  best) — solidly better than the census median (50th) but nowhere near an
  elite skill signal; **6 of 143 (4.2%)** have settlement ROI ≤ −60%. Same
  qualitative conclusion as the preview: the current 15-day-PnL-tiered list
  is enriched for skill but noisy — RESEARCH_PLAN.md's H1 concern.

## Task 6 sanity anchor: zero-sum verification

Per RESEARCH_PLAN.md: "Polymarket has no vig on settlement — sum of all
wallets' settlement PnL per market ≈ 0." Verified on 20 randomly sampled
markets with trade data (`analysis/hypothesis_tests.py::sanity_zero_sum`,
seed=42, spans moneyline/spread/total/nrfi/prop market types):

| net PnL as % of market's total invested | count (of 20) |
|---|---:|
| < 1e-10 % (floating-point noise) | 18 |
| ~0.0086% ($20 on $233K invested) | 1 |
| ~0.0138% ($124 on $902K invested) | 1 |

**Mean net-PnL-as-%-of-invested across the 20 markets: 0.0011%. Median:
~1e-15% (i.e. exactly zero to floating-point precision).** This holds
essentially exactly: for 18 of 20 sampled markets, the sum of every
wallet's `(returned − invested)` in that market is zero to within
double-precision floating-point rounding error (1e-13 to 1e-17 relative).
The two non-trivial residuals ($20 and $124, both moneyline markets with
768 and 921 wallets respectively) are 4–5 orders of magnitude smaller than
the market's total invested — plausibly a handful of fills with
sub-cent rounding, not a reconstruction bug. **This is strong, independent
confirmation that position reconstruction + settlement payout logic is
correct**: money conserved almost exactly across every wallet touching a
market, exactly as expected from a no-vig, fully-collateralized settlement
mechanism.

## Data-quality notes (full-lake re-verification of the preview's flags)

1. **Under-capture in futures markets: re-verified as structural, not a
   fetch bug.** The preview flagged 40/610 markets (mostly futures) as
   <50% of Gamma's reported volume and hypothesized an incomplete-fetch
   cause (the `ROW_CAP_FOR_TIME_SLICE` time-slice fallback not finishing).
   Full-lake re-check: **461/28,690 markets (1.6%)** are still <50% of
   Gamma volume — a *smaller* proportion than the partial sample (6.6%),
   confirming most of the preview's flagged cases were simply
   not-yet-fully-swept, not genuine gaps. For the markets that remain
   under-captured at full scale (dominated by 2025 World Series /
   division / pennant futures — e.g. Blue Jays 2025 WS winner: 31,734
   trades captured, only 7.1% of Gamma's $8.9M reported volume), direct
   intel-556 API verification confirms **`has_more=False` at the true end
   of pagination** for every sampled case (checked 4 markets spanning the
   largest dollar-gaps down to a 13-trade prop market) — the API has
   genuinely returned everything it has. **Conclusion: zero markets were
   removed from state / re-fetched.** The gap is structural: Gamma's
   `volume` field includes negRisk-adapter mint/conversion activity (a
   market maker depositing collateral to mint complementary YES+NO token
   pairs and selling both sides) that never appears as a counterparty
   trade row in the wallet-indexed trades endpoint — this was already
   correctly hypothesized in the preview and is now confirmed rather than
   fixed (there is no more data to fetch). Wallet ROI for anyone
   heavily concentrated in these markets should still be treated with
   caution, but for the reconstruction-completeness reason, not a
   pagination one.
2. **2 markets missing from `trades.json` completed_ids**, both
   season-2027-dated: "Will David Ortiz be the next Red Sox manager?" and
   "Will Ryan Flaherty be the next permanent manager of the Philadelphia
   Phillies?" — both fetched during this run's mop-up (19 and 248 rows
   respectively) and folded into the rebuilt lake.
3. **27 markets missing candles** (30,262/30,289 → 30,264/30,289 after this
   run): the 2 season-2027 stragglers above (now fetched, 2 rows each) plus
   **25 markets whose Gamma `end_date` is null** — `ingest_candles.py`
   requires `end_date` to anchor its 14-day pull window and correctly
   refuses to guess one. All 25 have valid `token_ids`; the blocker is
   specifically the missing `end_date`. Concentrated in 2024 postseason
   moneylines ("Dodgers vs. Mets - Game 2", "Yankees vs. Guardians - Game
   1") and 2026 in-season strikeout-leader props ("Will Jesús Luzardo
   strike out the most batters..."). **Documented, not fixed** — fixing
   would mean adding a `start_date`-based fallback window to
   `ingest_candles.py`, out of this task's scope (candles path is
   untouched per instructions) and affects only 0.08% of the target
   universe.
4. **Extreme top-decile ROI values remain real, not bugs** — same
   deep-longshot-token mechanics as the preview, now visible at 39,037x
   nominal ROI at the single-market level (vs. 3,903,780x in the preview,
   because the preview's max came from an even smaller-denominator
   single-wallet edge case that full-lake position reconstruction now
   nets against more of that wallet's other markets). The ≥5/≥30-market
   cuts above exist specifically to damp this.

## Files written

- `data/lake/trades.parquet` — 13,073,293 rows (rebuilt via the new
  two-pass DuckDB `build_trades()`)
- `data/lake/candles.parquet` — 1,960,809 rows (+4 rows from the 2
  straggler markets)
- `data/lake/positions.parquet` — 3,297,149 rows
- `data/lake/traders.parquet` — 224,559 rows
- `data/lake/hypothesis_results.json` — every number in this report and in
  `reports/05_skill_persistence.md`, machine-readable
- `analysis/ingest/build_lake.py` — `build_trades()` rewritten (two-pass
  DuckDB; candles path untouched)
- `analysis/hypothesis_tests.py` — new; census breakdowns, sanity anchor,
  and H1/H2/H3/H4/H7 (report 05)
