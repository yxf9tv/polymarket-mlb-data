# Phase 4 Report — Cross-Sport Year-Round Traders (MLB / NFL / NBA)

Run date: 2026-07-08. Pipeline: `analysis/cross_sport_winners.py --sports mlb,nfl,nba --min-n 50`.

## Per-sport methodology (generalizes analysis/persistent_winners.py's MLB funnel)

Each sport independently: n >= min-n settled pre-game markets in EACH of its two periods, positive FULL-history settlement ROI in EACH period, positive PRE-GAME-only settlement ROI in EACH period, not MM/bot-shaped (two_sided_share < 0.20, active_hours_entropy < 0.95 [recalibrated], median_inter_trade_seconds >= 60s), median fill notional >= $20, and (final, mandatory stage 6, applied before ranking) passes the external-reality screen (`analysis/wallet_screen.py`): lifetime all-category PnL not below -$10,000, not a category-hot-streak-inside-a-modest-lifetime-record pattern, staleness noted. This stage exists because a wallet can pass every in-category criterion above while being a net loser in aggregate across every category they trade -- see reports/08's post-review section for the case (Yikes110) that motivated it. MLB's 2025-vs-2026 run is reused verbatim from `data/persistent_winners.json` -- not re-derived. NFL (2024 vs 2025) and NBA (2024-25 vs 2025-26) run the same criteria through a generic pipeline built on trader_metrics.py's tested reconstruct_positions/settle_positions/bot_heuristics and persistent_winners.py's tested rollup/bootstrap-CI helpers. Pre-game split uses Gamma's `game_start_time`, falling back to `end_date` when null. No candles exist for NFL/NBA in this phase, so the copyability panel for those sports omits the post-entry price-drift calc MLB reports.

## MLB — periods 2025 vs 2026 (summer)

Source: reused verbatim from data/persistent_winners.json (analysis/persistent_winners.py)

| funnel stage | n wallets |
|---|---:|
| 1_active_both_periods_n>=50 | 83 |
| 2_positive_full_roi_both | 28 |
| 3_positive_pregame_roi_both | 23 |
| 4_not_bot_shaped | 9 |
| 5_median_notional>=20 | 7 |
| 6_external_reality_screen | 6 |

**External-reality screen (stage 6) removed 1 wallet(s), FAIL verdict (lifetime all-category PnL below the -$10,000 floor): `0xa5dcb282cab760e31df1f3f5c18350731c95ec43`.**

### MLB survivors: 6 wallets

| rank | wallet | combined score | 2025 n(full/pregame) | 2025 ROI full/pregame | 2026 n(full/pregame) | 2026 ROI full/pregame | median stake $ | median min-to-start |
|---|---|---:|---|---|---|---|---:|---:|
| 1 | `0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad` | -0.07333005533129087 | 423/396 | +4.7%/+4.8% | 995/887 | +0.6%/+0.9% | $52 | 24.3 |
| 2 | `0x985c0516200c5a2c76dde58917ce2c852afde5d2` | -0.07972171571566056 | 212/206 | +5.5%/+5.7% | 219/213 | +12.3%/+13.9% | $45 | 112.3 |
| 3 | `0x681d7946e0280c6ec6562c7ce662e90812354cbf` | -0.11108611021882461 | 236/178 | +3.2%/+4.1% | 154/101 | +3.3%/+7.2% | $50 | 74.7 |
| 4 | `0x2934efa3cc0794953270262e9da00c04e21d0b51` | -0.11573207980165519 | 406/405 | +5.5%/+5.4% | 192/189 | +2.0%/+3.4% | $25 | 343.9 |
| 5 | `0xfc234be3c4c3568284a566613ba90033b9d98283` | -0.1395532600158474 | 201/128 | +34.4%/+9.5% | 177/161 | +6.6%/+4.5% | $100 | 215.1 |
| 6 | `0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d` | -0.58222247258985 | 303/295 | +12.2%/+11.2% | 75/75 | +8.7%/+7.5% | $26 | 397.4 |

### MLB walk-forward: select on 2025 only, measure 2026 OOS

- **770 wallets** selected on 2025-only criteria.
- 2026 OOS full ROI: **+1.5%** (95% CI -2.2% to +5.1%), n=8426 markets / 738 wallets.
- 2026 OOS pre-game ROI: **+7.2%** (95% CI -0.3% to +13.9%), n=5660 markets / 735 wallets.

**Verdict: NOT SUPPORTED** (at least one OOS ROI's 95% CI excludes zero: no).

## NFL — periods 2024 vs 2025 (fall)

Source: analysis/cross_sport_winners.py generic pipeline (trades_nfl.parquet, markets_nfl.parquet)

| funnel stage | n wallets |
|---|---:|
| 1_active_both_periods_n>=50 | 55 |
| 2_positive_full_roi_both | 26 |
| 3_positive_pregame_roi_both | 21 |
| 4_not_bot_shaped | 6 |
| 5_median_notional>=20 | 1 |
| 6_external_reality_screen | 1 |

### NFL survivors: 1 wallets

| rank | wallet | combined score | 2024 n(full/pregame) | 2024 ROI full/pregame | 2025 n(full/pregame) | 2025 ROI full/pregame | median stake $ | median min-to-start |
|---|---|---:|---|---|---|---|---:|---:|
| 1 | `0xdc4bc68529c164cfe402ae1215876badc02a5a92` | -0.1105 | 191/182 | +11.2%/+10.2% | 157/152 | +8.5%/+8.9% | $33 | 717.4 |

### NFL walk-forward: select on 2024 only, measure 2025 OOS

- **25 wallets** selected on 2024-only criteria.
- 2025 OOS full ROI: **+4.2%** (95% CI -5.1% to +14.8%), n=957 markets / 16 wallets.
- 2025 OOS pre-game ROI: **+4.6%** (95% CI -5.9% to +15.4%), n=829 markets / 14 wallets.

**Verdict: NOT SUPPORTED** (at least one OOS ROI's 95% CI excludes zero: no).

## NBA — periods 2024-25 vs 2025-26 (winter)

Source: analysis/cross_sport_winners.py generic pipeline (trades_nba.parquet, markets_nba.parquet)

| funnel stage | n wallets |
|---|---:|
| 1_active_both_periods_n>=50 | 59 |
| 2_positive_full_roi_both | 21 |
| 3_positive_pregame_roi_both | 17 |
| 4_not_bot_shaped | 1 |
| 5_median_notional>=20 | 0 |
| 6_external_reality_screen | 0 |

### NBA survivors: 0 wallets

Zero wallets survive all five criteria for this sport this run.

### NBA walk-forward: select on 2024-25 only, measure 2025-26 OOS

- **7 wallets** selected on 2024-25-only criteria.
- 2025-26 OOS full ROI: **+11.7%** (95% CI -3.3% to +27.6%), n=800 markets / 3 wallets.
- 2025-26 OOS pre-game ROI: **+20.0%** (95% CI -4.2% to +45.9%), n=487 markets / 3 wallets.

**Verdict: NOT SUPPORTED** (at least one OOS ROI's 95% CI excludes zero: no).

## Cross-sport join

### Tier 1 — multi-sport winners (strict: funnel-passing in >=2 sports)

**0 wallets** pass the strict per-sport funnel in 2 or more sports.

**Strict Tier 1 was empty -- relaxed tier applied** (funnel-passing in sport A AND positive pre-game ROI with n>=30 pooled (all seasons) in sport B):

- **mlb_funnel_survivor->nfl_pregame_edge**: 3 wallet(s)
  - `0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad`: n_pregame=59, roi_pregame=+12.2%
  - `0xfc234be3c4c3568284a566613ba90033b9d98283`: n_pregame=100, roi_pregame=+14.1%
  - `0x681d7946e0280c6ec6562c7ce662e90812354cbf`: n_pregame=69, roi_pregame=+8.9%

### Tier 2 — single-sport specialists (top funnel survivors per sport)

**MLB** (summer): 6 listed
- `0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad` combined_score=-0.07333005533129087
- `0x985c0516200c5a2c76dde58917ce2c852afde5d2` combined_score=-0.07972171571566056
- `0x681d7946e0280c6ec6562c7ce662e90812354cbf` combined_score=-0.11108611021882461
- `0x2934efa3cc0794953270262e9da00c04e21d0b51` combined_score=-0.11573207980165519
- `0xfc234be3c4c3568284a566613ba90033b9d98283` combined_score=-0.1395532600158474
- `0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d` combined_score=-0.58222247258985

**NFL** (fall): 1 listed
- `0xdc4bc68529c164cfe402ae1215876badc02a5a92` combined_score=-0.1105

**NBA** (winter): 0 listed

### Activity overlap matrix (each sport's survivors, activity in every OTHER sport)

**MLB survivors' activity in NFL:**

| wallet | n_full | roi_full | n_pregame | roi_pregame |
|---|---:|---:|---:|---:|
| `0x2934efa3cc0794953270262e9da00c04e21d0b51` | 72 | -7.8% | 72 | -7.8% |
| `0x681d7946e0280c6ec6562c7ce662e90812354cbf` | 84 | +7.3% | 69 | +8.9% |
| `0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad` | 60 | +12.4% | 59 | +12.2% |
| `0x985c0516200c5a2c76dde58917ce2c852afde5d2` | 0 | n/a | 0 | n/a |
| `0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d` | 2 | -8.0% | 1 | -100.0% |
| `0xfc234be3c4c3568284a566613ba90033b9d98283` | 102 | +19.0% | 100 | +14.1% |

**MLB survivors' activity in NBA:**

| wallet | n_full | roi_full | n_pregame | roi_pregame |
|---|---:|---:|---:|---:|
| `0x2934efa3cc0794953270262e9da00c04e21d0b51` | 98 | -11.5% | 98 | -11.5% |
| `0x681d7946e0280c6ec6562c7ce662e90812354cbf` | 248 | +1.0% | 188 | -1.8% |
| `0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad` | 427 | -3.8% | 344 | -7.5% |
| `0x985c0516200c5a2c76dde58917ce2c852afde5d2` | 0 | n/a | 0 | n/a |
| `0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d` | 10 | +16.1% | 7 | -5.5% |
| `0xfc234be3c4c3568284a566613ba90033b9d98283` | 187 | -9.6% | 177 | -9.4% |

**NFL survivors' activity in MLB:**

| wallet | n_full | roi_full | n_pregame | roi_pregame |
|---|---:|---:|---:|---:|
| `0xdc4bc68529c164cfe402ae1215876badc02a5a92` | 1039 | -2.6% | 962 | -3.2% |

**NFL survivors' activity in NBA:**

| wallet | n_full | roi_full | n_pregame | roi_pregame |
|---|---:|---:|---:|---:|
| `0xdc4bc68529c164cfe402ae1215876badc02a5a92` | 554 | -2.9% | 533 | -3.2% |

**NBA survivors' activity in MLB:**

(no survivors to check)

**NBA survivors' activity in NFL:**

(no survivors to check)

## Copyability panel per finalist

Minutes-to-game-start (or minutes-to-first-pitch for MLB) distribution, per survivor, per sport. MLB additionally reports 1h post-entry price drift against candles.parquet; NFL/NBA SKIP this calc entirely -- no candles_nfl.parquet / candles_nba.parquet exists in this phase, so there is no candle series to measure drift against.

- `0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad` (MLB): median minutes-to-first-pitch = 24.3, 1h drift = 0.0131 (n=4499)
- `0x985c0516200c5a2c76dde58917ce2c852afde5d2` (MLB): median minutes-to-first-pitch = 112.3, 1h drift = 0.0024 (n=503)
- `0x681d7946e0280c6ec6562c7ce662e90812354cbf` (MLB): median minutes-to-first-pitch = 74.7, 1h drift = -0.0067 (n=309)
- `0x2934efa3cc0794953270262e9da00c04e21d0b51` (MLB): median minutes-to-first-pitch = 343.9, 1h drift = -0.0031 (n=2010)
- `0xfc234be3c4c3568284a566613ba90033b9d98283` (MLB): median minutes-to-first-pitch = 215.1, 1h drift = -0.0023 (n=292)
- `0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d` (MLB): median minutes-to-first-pitch = 397.4, 1h drift = 0.0185 (n=1378)
- `0xdc4bc68529c164cfe402ae1215876badc02a5a92` (NFL): median minutes-to-game-start = 717.4 (no candles_nfl.parquet ingested in this phase -- price-drift-after-entry not computed)

## Hygiene notes

- All bootstrap CIs resample at the MARKET level (2,000 resamples, seed=42), same as persistent_winners.py.
- Settlement ROI (cash-flow accounting) is the sole source of truth throughout.
- MLB numbers are reused verbatim from `data/persistent_winners.json` -- never re-derived or forked in this script.
- NFL/NBA bot/stake filters use pooled (all-seasons-in-file) features, matching MLB's use of traders.parquet's pooled full-history features.

## Command to run once the real sweeps land

```
.venv/bin/python3 analysis/cross_sport_winners.py --sports mlb,nfl,nba --min-n 50
```
