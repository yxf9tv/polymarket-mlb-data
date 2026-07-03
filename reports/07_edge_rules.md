# Phase 3 Report — Signals & Edge Rules

Run date: 2026-07-03. Pipeline: `analysis/edge_rules.py` (target selection: `analysis/select_orderbook_targets.py`, ingest: `analysis/ingest/ingest_orderbooks.py`, lake build: `analysis/build_orderbook_lake.py`).

## Data-retention finding (discovered mid-ingest, not anticipated by the probes)

intel agent 572 (orderbook history) has **zero retention for the entire 2025 season** -- verified on 17 spot checks including the 5 highest-volume 2025 markets (World Series, Blue Jays/Dodgers, Nov 2025), all returned 0 snapshots. Coverage starts ~2026-03-26 (a 2026-03-03 spring-training market returned 0; 2026-03-26+ markets return data) -- a rolling ~3-month window as of the 2026-07-03 ingest date. **The planned 500-market 2025 orderbook sample is therefore empty by construction; every result below is 2026-only.** This also forced the rule-mining train/validate split to be within-2026 (time-based) instead of the planned 2025+early-2026 -> late-2026 split (see Edge rules section).

Sample: 4914 target markets in two cohorts -- `primary` (3114: all 2026 crowd-signal moneyline markets + each game's max-crowd-stake O/U line [avg 5.3 lines/game in the raw lake], plus the (empty) 500-market 2025 sample) and `alt_line` (1800: a random sample of the OTHER 2026 O/U lines, added after the reconciliation diagnostic below showed the zero-slippage edge concentrates there). 4035/4414 2026 markets (91.4%) yielded a usable pre-first-pitch orderbook snapshot -- the rest either had first_pitch before the 2026-03-26 retention floor or the market's book was truly empty at every polled timestamp.

## H5 verdict: does depth imbalance predict settlement, and add to the crowd-flow signal?

**NO.** Logistic outcome ~ flow_margin + depth_imbalance vs. flow_margin alone, n=4035 markets with a valid pre-first-pitch depth reading (depth_imbalance = resting bid-side $ / total resting $ on the crowd's chosen token -- see caveat below). AUC flow-only=0.6168, AUC flow+depth=0.6154, delta=-0.0014 95% CI [-0.0032, +0.0026]. depth_imbalance coefficient (standardized) = 0.070, 95% CI [-0.0159, +0.1545]. Univariate corr(depth_imbalance, win) = 0.143.

*Caveat on depth_imbalance's definition*: the production dashboard's depth-imbalance metric (`scripts/poll_live.py::compute_depth_imbalance`) uses BOTH outcome tokens' books (consensus bid + opposing ask vs. consensus ask + opposing bid). To stay inside the ingest budget, this study fetched orderbook history for only the crowd's CHOSEN token per market (one call/market instead of two), so depth_imbalance here is the single-token proxy: resting bid $ / (bid $ + ask $) on that token alone. This is directionally the same idea (net resting support for the signal side) but not numerically identical to the dashboard metric; a full two-token re-run would double the orderbook ingest cost. The fit pools both cohorts (primary + alt_line) -- valid for a conditional model, though the pooled sample over-represents primary lines relative to the market population.

## Crowd-baseline reconciliation: does Phase 2's edge even exist in this window?

Before slippage enters the picture, the zero-slippage crowd ROI must be reconciled with Phase 2's +2.0-2.2% (totals +5.1-5.3%): this sample differs from Phase 2's crowd baseline in two ways -- (a) it is restricted to the orderbook-retention window (2026 markets with first_pitch >= 2026-03-26), and (b) totals were deduped to ONE O/U line per game (max crowd stake) to fit the ingest budget, while Phase 2 scored every line. Zero-slippage crowd ROI, same signal construction, split three ways:

| slice | n | ROI (mean) | ROI 95% CI | totals-only ROI (n) | moneyline-only ROI (n) |
|---|---:|---:|---|---|---|
| all_lines_retention_window | 7934 | +4.19% | [+2.42%, +6.02%] | +5.07% (6585) | -0.11% (1349) |
| primary_lines(max_stake_dedup) | 2594 | -0.08% | [-3.57%, +3.39%] | -0.05% (1245) | -0.11% (1349) |
| alternate_lines(all) | 5340 | +6.27% | [+4.27%, +8.22%] | +6.27% (5340) | n/a (0) |

Reading: the all-lines slice (+4.19%) is the honest Phase-2-comparable number for this window; the primary (max-stake-dedup) lines (-0.08%) vs ALL alternate lines (+6.27%) shows where the zero-slippage edge concentrates -- which is why the alt_line cohort was added to the orderbook sample mid-phase.

## Executability: the edge-after-slippage table (headline)

Entry side = the crowd's chosen (signal) outcome token, walking the ASK side of its resting book at the last snapshot before first pitch, for a stake to be filled fully at that price (partial fills are excluded from that stake size's ROI, not padded/assumed).

Cohorts are reported separately -- `primary` (census: all 2026 crowd-signal moneyline markets + each game's max-crowd-stake O/U line) vs `alt_line` (random 1,800-market sample of the other O/U lines, where the reconciliation above shows the zero-slippage edge actually lives).

**Cohort: alt_line** (1588 signal markets)

| entry basis | n priced | fill coverage | ROI (mean) | ROI 95% CI |
|---|---:|---:|---:|---|
| zero-slippage (candle close) | 1586 | - | +4.69% | [+0.90%, +8.20%] |
| $50 depth-weighted fill | 1469 | 92.5% | -1.88% | [-5.39%, +1.79%] |
| $200 depth-weighted fill | 1402 | 88.3% | -1.45% | [-5.11%, +2.21%] |
| $1000 depth-weighted fill | 1261 | 79.4% | -1.10% | [-5.11%, +2.86%] |

| market_type | n signal markets | zero-slip ROI | $200 fill coverage | $200 ROI | $200 ROI CI |
|---|---:|---:|---:|---:|---|
| total | 1588 | +4.69% | 88.3% | -1.45% | [-5.11%, +2.21%] |

**Cohort: primary** (2447 signal markets)

| entry basis | n priced | fill coverage | ROI (mean) | ROI 95% CI |
|---|---:|---:|---:|---|
| zero-slippage (candle close) | 2441 | - | -0.10% | [-3.67%, +3.37%] |
| $50 depth-weighted fill | 2446 | 100.0% | -1.45% | [-5.08%, +2.06%] |
| $200 depth-weighted fill | 2423 | 99.0% | -1.46% | [-4.87%, +2.23%] |
| $1000 depth-weighted fill | 2413 | 98.6% | -1.66% | [-5.07%, +1.82%] |

| market_type | n signal markets | zero-slip ROI | $200 fill coverage | $200 ROI | $200 ROI CI |
|---|---:|---:|---:|---:|---|
| moneyline | 1270 | -0.21% | 98.2% | -1.52% | [-6.31%, +3.47%] |
| total | 1177 | +0.02% | 99.9% | -1.41% | [-6.79%, +3.90%] |

## Edge rules

0 rule(s) cleared the TRAIN bar (n>=50, bootstrap CI lower bound on $200-stake after-slippage ROI > 0); **0 survive re-scoring (not re-selection) on the held-out VALIDATE window** (n>=20 and validate CI lower bound > 0).

**No rule cleared even the train bar.** This is a decisive negative result, stated plainly: once the crowd-flow signal is priced through the actual resting book at $200 stakes, no (market_type x flow-margin x liquidity x price-band) bucket shows a bootstrap-CI-positive edge on this 2026-only sample. Do not deploy a bucket rule off this analysis.

## Copy-trade groundwork: price decay after topk_clv-cohort entries

20350 pre-game BUY fills from the 100 `data/trader_portfolio.json` closest_contender members, matched to the nearest candle (+/- 45min tolerance) at each horizon after their fill. Horizons landing AFTER first pitch are excluded (they'd measure in-game repricing of the score, not copyable pre-game drift), so n shrinks as the horizon grows. `move` = candle price at +h minus the trader's fill price on the SAME token: positive = the price already ran away from a copier; ~0 = a copier gets essentially the same entry; negative = the copier gets a BETTER entry.

| horizon | n | median price move | mean price move | frac moved against copier | 95% CI (mean) |
|---|---:|---:|---:|---:|---|
| +1h | 16208 | +0.0050 | +0.0305 | 59.4% | [+0.0290, +0.0321] |
| +3h | 13561 | +0.0050 | +0.0335 | 62.6% | [+0.0318, +0.0352] |
| +6h | 11159 | +0.0100 | +0.0359 | 65.4% | [+0.0340, +0.0378] |

## Recommendation for Phase 4

[alt_line] the crowd-flow edge does NOT clearly survive $200-stake slippage: after-slippage ROI -1.45% [-5.11%, +2.21%] (n=1402, 88% fill coverage) vs. zero-slippage +4.69% -- CI does not exclude zero and/or fill coverage too thin. [primary] the crowd-flow edge does NOT clearly survive $200-stake slippage: after-slippage ROI -1.46% [-4.87%, +2.23%] (n=2423, 99% fill coverage) vs. zero-slippage -0.10% -- CI does not exclude zero and/or fill coverage too thin. No interpretable rule survives both train and validate -- do not deploy a bucket rule from this analysis. Phase 4 should NOT wire a wallet-selection portfolio (confirmed non-beat in Phase 2) nor an unvalidated edge rule into `scripts/recommend.py`. If a cohort's crowd-flow-at-$200 number above is positive with CI excluding zero, that cohort's signal is the only candidate worth a small-stake paper-trading run in Phase 4 -- capped at the stake size actually tested here. Everything else in this report is a documented null result, not a deployment target.

## Files written

- `data/lake/orderbook_targets.parquet` -- the 4914-market sample design (primary + alt_line cohorts)
- `data/lake/orderbook.parquet` -- parsed pre-first-pitch orderbook snapshots
- `data/lake/edge_rules_raw.json` -- every number in this report (scalars only)
- `data/edge_rules.json` -- surviving rule specs (empty-but-valid if none survive)
- `reports/07_edge_rules.md` -- this report