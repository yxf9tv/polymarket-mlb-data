# Phase 2/3 Report — Persistent MLB Winners (FINAL)

Run date: 2026-07-07. Pipeline: `analysis/persistent_winners.py`. Full lake (13.07M trades, 224,559 wallets, 28,690 markets — see reports/04 and reports/05 for the underlying methodology this report builds on).

## Selection criteria

1. Active in BOTH 2025 and 2026 seasons: n >= 50 settled pre-game markets in EACH period independently.
2. Positive FULL-history settlement ROI (cash-flow accounting) in EACH period independently.
3. Positive PRE-GAME-only settlement ROI in EACH period independently.
4. Not MM/bot-shaped: two_sided_share < 0.2, active_hours_entropy < 0.95, median_inter_trade_seconds >= 60s (pooled features from traders.parquet; thresholds documented in the module docstring).
5. Median fill notional >= $20 (dust-bot floor).

## Selection funnel

| stage | n wallets |
|---|---:|
| 1_active_both_periods_n>=50 | 83 |
| 2_positive_full_roi_both | 28 |
| 3_positive_pregame_roi_both | 23 |
| 4_not_bot_shaped | 9 |
| 5_median_notional>=20 | 7 |
| 6_external_reality_screen | 6 |

## Survivors: 7 wallets

Ranked by combined score = min(pre-game ROI bootstrap-95%-CI lower bound, 2025 vs 2026) — a wallet only ranks high if BOTH periods' pre-game edge survives a conservative lower-bound haircut, not just point-estimate ROI.

**Caveat, stated plainly: all 7/7 survivors have a NEGATIVE combined score** — i.e. for every one of them, at least one period's pre-game-ROI 95% bootstrap CI still straddles zero. Passing criteria 1-5 (positive point-estimate ROI in both periods independently, plus the behavioral/stake filters) is a real screen — the eligible-pool base rate for 'positive ROI in both periods' was 23/83 (28%) at the pre-game-only cut — but at typical per-period sample sizes here (roughly 100-900 settled markets), no individual wallet's edge clears a strict 95%-CI-excludes-zero bar in both periods simultaneously. Read this list as 'best available candidates by a real but modest point-estimate persistence filter,' not as 'statistically proven alpha' — consistent with reports/05's finding that season-level skill is real in aggregate (H7) but too diffuse for confident single-wallet certification at these sample sizes.

| rank | wallet | combined score | 2025 n(full/pregame) | 2025 ROI full/pregame | 2025 win% | 2025 avg entry | 2026 n(full/pregame) | 2026 ROI full/pregame | 2026 win% | 2026 avg entry | total profit $ | in-147-list | in-CLV-cohort |
|---|---|---:|---|---|---:|---:|---|---|---:|---:|---:|---|---|
| 1 | `0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad` | -0.073 | 423/396 | +4.7%/+4.8% | 52.8% | 0.49 | 995/887 | +0.6%/+0.9% | 53.0% | 0.53 | $27,510 | - | - |
| 2 | `0x985c0516200c5a2c76dde58917ce2c852afde5d2` | -0.080 | 212/206 | +5.5%/+5.7% | 56.8% | 0.54 | 219/213 | +12.3%/+13.9% | 59.2% | 0.52 | $2,620 | - | - |
| 3 | `0x681d7946e0280c6ec6562c7ce662e90812354cbf` | -0.111 | 236/178 | +3.2%/+4.1% | 52.2% | 0.51 | 154/101 | +3.3%/+7.2% | 56.4% | 0.51 | $1,037 | - | - |
| 4 | `0x2934efa3cc0794953270262e9da00c04e21d0b51` | -0.116 | 406/405 | +5.5%/+5.4% | 53.6% | 0.49 | 192/189 | +2.0%/+3.4% | 47.6% | 0.46 | $9,890 | - | - |
| 5 | `0xfc234be3c4c3568284a566613ba90033b9d98283` | -0.140 | 201/128 | +34.4%/+9.5% | 50.8% | 0.51 | 177/161 | +6.6%/+4.5% | 55.3% | 0.55 | $15,133 | - | - |
| 6 | `0xa5dcb282cab760e31df1f3f5c18350731c95ec43` | -0.253 | 119/94 | +13.2%/+12.0% | 52.1% | 0.54 | 292/219 | +24.3%/+23.6% | 58.0% | 0.59 | $28,280 | - | - |
| 7 | `0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d` | -0.582 | 303/295 | +12.2%/+11.2% | 49.5% | 0.44 | 75/75 | +8.7%/+7.5% | 48.0% | 0.46 | $73,690 | - | - |

### Per-wallet detail: bootstrap CIs, bot features, sizing, copyability, market mix, recent form

**`0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad`** (combined score -0.073)

- 2025: n_full=423 n_pregame=396, ROI full=+4.7% (95% CI -7.3% to +17.5%), ROI pregame=+4.8% (95% CI -7.3% to +17.2%), win rate pregame=52.8%, avg entry=0.49, profit(full)=$21,320
- 2026: n_full=995 n_pregame=887, ROI full=+0.6% (95% CI -5.8% to +7.2%), ROI pregame=+0.9% (95% CI -6.1% to +8.2%), win rate pregame=53.0%, avg entry=0.53, profit(full)=$6,190
- 2026 recent form (last 30 pre-game-settled markets): ROI=-1.0%, win rate=50.0%
- Market-type mix (combined, full history): moneyline 45% (n=634), total 34% (n=484), spread 21% (n=296), prop 0% (n=2), futures 0% (n=1), nrfi 0% (n=1)
- Bot features: two_sided_share=0.0536, active_hours_entropy=0.7174, median_inter_trade_seconds=84.0, trades_per_day=15.45
- Sizing: median fill notional (all-time) = $52.16
- Copyability: median minutes fill-to-first-pitch = 24 min; 1h post-entry price drift (size-weighted) = +0.0131 (n=4499 fills matched to a candle)
- Total profit (full-history, both periods): $27,510

**`0x985c0516200c5a2c76dde58917ce2c852afde5d2`** (combined score -0.080)

- 2025: n_full=212 n_pregame=206, ROI full=+5.5% (95% CI -8.2% to +19.0%), ROI pregame=+5.7% (95% CI -8.0% to +19.7%), win rate pregame=56.8%, avg entry=0.54, profit(full)=$868
- 2026: n_full=219 n_pregame=213, ROI full=+12.3% (95% CI -1.1% to +25.2%), ROI pregame=+13.9% (95% CI -0.7% to +28.0%), win rate pregame=59.2%, avg entry=0.52, profit(full)=$1,752
- 2026 recent form (last 30 pre-game-settled markets): ROI=+21.5%, win rate=63.3%
- Market-type mix (combined, full history): moneyline 83% (n=357), spread 11% (n=47), total 6% (n=27)
- Bot features: two_sided_share=0.0673, active_hours_entropy=0.7922, median_inter_trade_seconds=1527.0, trades_per_day=1.76
- Sizing: median fill notional (all-time) = $45.00
- Copyability: median minutes fill-to-first-pitch = 112 min; 1h post-entry price drift (size-weighted) = +0.0024 (n=503 fills matched to a candle)
- Total profit (full-history, both periods): $2,620

**`0x681d7946e0280c6ec6562c7ce662e90812354cbf`** (combined score -0.111)

- 2025: n_full=236 n_pregame=178, ROI full=+3.2% (95% CI -6.8% to +15.8%), ROI pregame=+4.1% (95% CI -11.1% to +20.8%), win rate pregame=52.2%, avg entry=0.51, profit(full)=$786
- 2026: n_full=154 n_pregame=101, ROI full=+3.3% (95% CI -12.5% to +19.5%), ROI pregame=+7.2% (95% CI -10.8% to +25.1%), win rate pregame=56.4%, avg entry=0.51, profit(full)=$251
- 2026 recent form (last 30 pre-game-settled markets): ROI=+11.9%, win rate=56.7%
- Market-type mix (combined, full history): moneyline 80% (n=311), total 15% (n=58), nrfi 3% (n=11), spread 2% (n=6), futures 0% (n=2), other 0% (n=2)
- Bot features: two_sided_share=0.1, active_hours_entropy=0.8044, median_inter_trade_seconds=7462.0, trades_per_day=1.43
- Sizing: median fill notional (all-time) = $50.00
- Copyability: median minutes fill-to-first-pitch = 75 min; 1h post-entry price drift (size-weighted) = -0.0067 (n=309 fills matched to a candle)
- Total profit (full-history, both periods): $1,037

**`0x2934efa3cc0794953270262e9da00c04e21d0b51`** (combined score -0.116)

- 2025: n_full=406 n_pregame=405, ROI full=+5.5% (95% CI -4.5% to +15.5%), ROI pregame=+5.4% (95% CI -5.0% to +15.4%), win rate pregame=53.6%, avg entry=0.49, profit(full)=$8,923
- 2026: n_full=192 n_pregame=189, ROI full=+2.0% (95% CI -12.6% to +17.4%), ROI pregame=+3.4% (95% CI -11.6% to +18.8%), win rate pregame=47.6%, avg entry=0.46, profit(full)=$967
- 2026 recent form (last 30 pre-game-settled markets): ROI=-2.9%, win rate=46.7%
- Market-type mix (combined, full history): moneyline 100% (n=598)
- Bot features: two_sided_share=0.0017, active_hours_entropy=0.7675, median_inter_trade_seconds=197.5, trades_per_day=5.7
- Sizing: median fill notional (all-time) = $24.52
- Copyability: median minutes fill-to-first-pitch = 344 min; 1h post-entry price drift (size-weighted) = -0.0031 (n=2010 fills matched to a candle)
- Total profit (full-history, both periods): $9,890

**`0xfc234be3c4c3568284a566613ba90033b9d98283`** (combined score -0.140)

- 2025: n_full=201 n_pregame=128, ROI full=+34.4% (95% CI -2.2% to +62.7%), ROI pregame=+9.5% (95% CI -14.0% to +32.0%), win rate pregame=50.8%, avg entry=0.51, profit(full)=$12,190
- 2026: n_full=177 n_pregame=161, ROI full=+6.6% (95% CI -8.6% to +21.3%), ROI pregame=+4.5% (95% CI -11.7% to +19.7%), win rate pregame=55.3%, avg entry=0.55, profit(full)=$2,943
- 2026 recent form (last 30 pre-game-settled markets): ROI=+22.2%, win rate=66.7%
- Market-type mix (combined, full history): moneyline 73% (n=277), total 11% (n=42), spread 11% (n=41), futures 4% (n=15), nrfi 1% (n=3)
- Bot features: two_sided_share=0.1561, active_hours_entropy=0.7343, median_inter_trade_seconds=194.0, trades_per_day=1.51
- Sizing: median fill notional (all-time) = $100.00
- Copyability: median minutes fill-to-first-pitch = 215 min; 1h post-entry price drift (size-weighted) = -0.0023 (n=292 fills matched to a candle)
- Total profit (full-history, both periods): $15,133

**`0xa5dcb282cab760e31df1f3f5c18350731c95ec43`** (combined score -0.253)

- 2025: n_full=119 n_pregame=94, ROI full=+13.2% (95% CI -15.2% to +42.8%), ROI pregame=+12.0% (95% CI -25.3% to +45.4%), win rate pregame=52.1%, avg entry=0.54, profit(full)=$1,139
- 2026: n_full=292 n_pregame=219, ROI full=+24.3% (95% CI -11.3% to +46.9%), ROI pregame=+23.6% (95% CI -19.6% to +48.3%), win rate pregame=58.0%, avg entry=0.59, profit(full)=$27,141
- 2026 recent form (last 30 pre-game-settled markets): ROI=+46.4%, win rate=50.0%
- Market-type mix (combined, full history): moneyline 76% (n=314), nrfi 12% (n=50), total 6% (n=25), spread 5% (n=22)
- Bot features: two_sided_share=0.1411, active_hours_entropy=0.9036, median_inter_trade_seconds=835.0, trades_per_day=2.0
- Sizing: median fill notional (all-time) = $50.00
- Copyability: median minutes fill-to-first-pitch = 181 min; 1h post-entry price drift (size-weighted) = +0.0550 (n=397 fills matched to a candle)
- Total profit (full-history, both periods): $28,280

**`0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d`** (combined score -0.582)

- 2025: n_full=303 n_pregame=295, ROI full=+12.2% (95% CI -49.0% to +84.9%), ROI pregame=+11.2% (95% CI -58.2% to +90.7%), win rate pregame=49.5%, avg entry=0.44, profit(full)=$73,451
- 2026: n_full=75 n_pregame=75, ROI full=+8.7% (95% CI -21.1% to +39.7%), ROI pregame=+7.5% (95% CI -21.9% to +39.4%), win rate pregame=48.0%, avg entry=0.46, profit(full)=$239
- 2026 recent form (last 30 pre-game-settled markets): ROI=-26.0%, win rate=36.7%
- Market-type mix (combined, full history): moneyline 100% (n=378)
- Bot features: two_sided_share=0.0897, active_hours_entropy=0.8141, median_inter_trade_seconds=114.0, trades_per_day=4.47
- Sizing: median fill notional (all-time) = $25.75
- Copyability: median minutes fill-to-first-pitch = 397 min; 1h post-entry price drift (size-weighted) = +0.0185 (n=1378 fills matched to a candle)
- Total profit (full-history, both periods): $73,690

## Overlap with existing lists

- Old 147-wallet list (`data/top_mlb_traders.json`, top_tier+watchlist union): **0/7** survivors also appear there.
- CLV-cohort (`data/trader_portfolio.json` closest_contender, topk_clv k=100, reference-only, NOT a recommended deployed portfolio): **0/7** survivors also appear there.

Zero overlap on both isn't surprising: report 04 found the old 147-list is built from 15-day rolling PnL tiering (median percentile rank 27.5th vs. the full census, i.e. enriched-but-noisy, not elite) and report 05's H1 showed 15-day-window PnL barely predicts next-window settlement ROI (rho=0.02) -- a fundamentally different, weaker selection axis than this report's two-independent-full-season, pre-game-only, cash-flow-settlement filter. The CLV-cohort is explicitly reference-only (report 06 recommended against deploying it).

## Walk-forward criteria validation: select on 2025 only, measure 2026 out-of-sample

This is the number that tells us whether the *criteria themselves* (not just this particular list) survive walk-forward: apply criteria 1-5 using ONLY 2025 data (pregame n>=50 in 2025, positive full & pregame 2025 ROI, 2025-only-computed bot/stake filters — recomputed on 2025 trades specifically so no 2026 behavioral information leaks into selection), then measure that cohort's actual 2026 performance.

- **770 wallets** selected on 2025-only criteria.
- 2026 OOS full-history ROI (pooled, portfolio-level): **+1.5%** (95% CI -2.2% to +5.1%), n=8426 markets across 738 wallets with 2026 activity, $22,548,090 invested.
- 2026 OOS pre-game-only ROI (pooled, portfolio-level): **+7.2%** (95% CI -0.3% to +13.9%), n=5660 markets across 735 wallets with 2026 activity, $9,826,733 invested.

**Verdict: NOT SUPPORTED.** Both out-of-sample ROI CIs straddle zero (or are negative) — selecting a portfolio with these criteria on 2025 data alone does not reliably produce positive 2026 returns. Consistent with reports/05's H1/H7 findings that season-level persistence is real in aggregate but diffuse (97.5% top-decile turnover) — a criteria-based walk-forward portfolio is not obviously more robust than that diffuse population-level signal.

## Hygiene notes

- All bootstrap CIs resample at the MARKET level (2,000 resamples, seed=42), not the fill level, so within-market fill correlation doesn't understate variance.
- n is stated at every stage (funnel table, per-wallet detail, walk-forward section).
- Settlement ROI (cash-flow accounting: invested = buy cost, returned = sell proceeds + settlement payout) is the sole source of truth throughout, per reports/04 and reports/05's established methodology.

## External-reality screen (post-review)

A user review caught a survivorship-bias failure mode the five in-category criteria above cannot see: wallet `0xa5dcb282cab760e31df1f3f5c18350731c95ec43` ("Yikes110") passed all five MLB-specific criteria (+$28,280 total profit here, rank 6/7) while carrying a **-$262,176 lifetime, all-category** Polymarket PnL -- a busted gambler riding a hot MLB streak on the way down, not a specialist with durable edge. Settlement ROI computed only inside one category's lake cannot tell 'consistently skilled here' apart from 'currently up here, down everywhere else' -- the two look identical from inside a single-sport view. This section adds a sixth, EXTERNAL screen (`analysis/wallet_screen.py`, shared with `analysis/cross_sport_winners.py`'s NFL/NBA funnel so the same failure mode can't recur there): lifetime all-category PnL and current deployed value from Polymarket's own public leaderboard/data APIs, plus a staleness check against our own lake.

**Verdict rules:** FAIL if lifetime PnL < -$10,000 (net loser across their whole trading history, regardless of MLB performance); WARN if lifetime PnL < 25% of their MLB profit here (a category-hot-streak-inside-a-modest-lifetime-record pattern, e.g. Radahn-131's +$73,690 MLB profit vs. +$18,659 lifetime -- MLB alone is ~4x their entire lifetime PnL); staleness is noted, not scored, if no trade in 30 days.

| rank | wallet | pseudonym | lifetime PnL (all categories) | current value | last MLB trade | days since | verdict | status |
|---|---|---|---:|---:|---|---:|---|---|
| 1 | `0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad` | Trotz.85 | $49,304 | $23,375 | 2026-07-02 | 5d | OK | kept |
| 2 | `0x985c0516200c5a2c76dde58917ce2c852afde5d2` | AnEventHorizon | $2,321 | $115 | 2026-07-02 | 5d | OK | kept |
| 3 | `0x681d7946e0280c6ec6562c7ce662e90812354cbf` | GreatAgain28 | $6,032 | $1 | 2026-06-23 | 14d | OK | kept |
| 4 | `0x2934efa3cc0794953270262e9da00c04e21d0b51` | cleoclaudiu | $13,762 | $270 | 2026-07-01 | 5d | OK | kept |
| 5 | `0xfc234be3c4c3568284a566613ba90033b9d98283` | FishCrypto | $7,874 | $7,037 | 2026-07-01 | 5d | OK | kept |
| 6 | `0xa5dcb282cab760e31df1f3f5c18350731c95ec43` | Yikes110 | $-262,176 | $53 | 2026-06-21 | 16d | FAIL | **REMOVED** |
| 7 | `0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d` | Radahn-131 | $18,659 | $0 | 2026-06-28 | 8d | OK | kept |

**1 wallet(s) REMOVED by the external screen: `0xa5dcb282cab760e31df1f3f5c18350731c95ec43`.** The MLB-7 list above is retained in full for provenance (all five in-category criteria were genuinely passed), but the operative, deploy-worthy list after this review is the remaining 6 wallets (`funnel.6_external_reality_screen` in `data/persistent_winners.json`). See each survivor's `external_screen` block and the top-level `screened_out_wallets` field.

**OK, but worth flagging (lifetime PnL close to the 25% WARN threshold without crossing it):**

- `0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d` (Radahn-131): lifetime PnL $18,659 is 25.3% of category profit $73,690 -- close to the 25% WARN threshold, worth watching

**Lesson:** in-category settlement ROI, however carefully computed (cash-flow accounting, two-independent-season persistence, bootstrap CIs, bot/stake filters), is silent on what a wallet does OUTSIDE the category being screened. A short-lived hot streak inside one sport can coexist with a deeply negative lifetime track record everywhere else -- exactly what an in-category-only screen is structurally unable to detect, and exactly why this external-reality check is now a mandatory, non-optional stage in both this pipeline and the cross-sport NFL/NBA pipeline (`analysis/cross_sport_winners.py`), not an optional add-on.

## Rescan: near-miss candidates (post-review)

User-approved follow-up to the external-reality screen above (`analysis/rescan_near_miss.py`): apply the same external screen to the FULL stage-3 pool -- every wallet with positive full AND pre-game settlement ROI in both periods, BEFORE the bot/stake filters (23 wallets at the strict n>=50 floor), and again with the per-period eligibility floor relaxed to n>=30 (29 wallets). The question: did criteria 4/5 (or the strict n-floor) reject anyone who looks externally healthy -- i.e. are we leaving defensible candidates on the table? **Everything in this section is SECOND-TIER: no one here is promoted into the operative 6-wallet list. Promotion is a user decision.**

Relaxed funnel (floor n>=30, same criteria 2-5 and external screen):

| stage | n wallets |
|---|---:|
| 1_active_both_periods_n>=30 | 125 |
| 2_positive_full_roi_both | 37 |
| 3_positive_pregame_roi_both | 29 |
| 4_not_bot_shaped | 12 |
| 5_median_notional>=20 | 8 |

**Convincing external pass** = verdict OK, lifetime PnL >= $1,000, and (current value > $1,000 or traded on/after 2026-06-01).

| wallet | pseudonym | lifetime PnL | current value | last trade | verdict | convincing | 2025 pregame ROI (n) | 2026 pregame ROI (n) | profit $ | failed strict criteria |
|---|---|---:|---:|---|---|---|---|---|---:|---|
| `0x2005d16a84ceefa912d4e380cd32e7ff827875ea` | RN1 | $10,828,498 | $143,461 | 2026-07-02 | OK | **YES** | +17.6% (115) | +10.1% (140) | 253,553 | criterion 4 (median_inter_trade_seconds>=60s): 2s (short 58s); criterion 5 (median notional>=$20): $5.77 (short $14.23) |
| `0x84dbb7103982e3617704a2ed7d5b39691952aeeb` | Soarin22 | $2,044,129 | $0 | 2026-06-15 | OK | **YES** | +28.8% (129) | +34.1% (50) | 166,993 | criterion 5 (median notional>=$20): $15.06 (short $4.94) |
| `0xde0463ea7f611b065e8ab06bbfbddad75e6dfa37` | mwenya | $172,651 | $124,589 | 2026-06-13 | OK | **YES** | +18.0% (84) | +1.6% (296) | 14,245 | criterion 4 (median_inter_trade_seconds>=60s): 28s (short 32s) |
| `0x6a32a31f2b28c31c016c6a222f2942240d8d0086` | 0x6A32A31F2B28c31c016C6A222f2942240d8D0086-1756820952447 | $73,279 | $1,676 | 2026-07-01 | OK | **YES** | +21.3% (33) | +6.0% (205) | 42,347 | criterion 1 (n>=50 pregame both periods): 2025: n=33 (short 17) |
| `0x2ada299aaaf27c806424ec6dcb0c169d1a9c2f28` | wowfarm | $43,982 | $66 | 2026-07-02 | OK | **YES** | +92.7% (106) | +5.1% (31) | 28,456 | criterion 1 (n>=50 pregame both periods): 2026: n=31 (short 19); criterion 4 (median_inter_trade_seconds>=60s): 26s (short 34s) |
| `0xa39a38fbb98ad026c5a6b126d74d891e1be51246` | MrBently | $25,841 | $263 | 2026-06-14 | OK | **YES** | +10.1% (81) | +17.4% (87) | 35,416 | criterion 4 (median_inter_trade_seconds>=60s): 32s (short 28s); criterion 5 (median notional>=$20): $19.60 (short $0.40) |
| `0xf10299cf1fff507cff45e1a906800e5b44bf1348` | pjotrekkk | $21,360 | $1,406 | 2026-07-01 | OK | **YES** | +2.7% (708) | +11.9% (345) | 4,055 | criterion 4 (active_hours_entropy<0.95): 0.975 (over by 0.025); criterion 5 (median notional>=$20): $9.41 (short $10.59) |
| `0xe05f5943a8adf19d59d0d63d77b7eb681297b3e8` | TangledUpInBlue | $15,727 | $8,516 | 2026-05-02 | OK | **YES** | +4.2% (140) | +48.7% (91) | 18,087 | criterion 4 (active_hours_entropy<0.95): 0.974 (over by 0.024); criterion 5 (median notional>=$20): $1.23 (short $18.77) |
| `0x5adf695a088172a920b6d0b2e5b7d88aa8ffa5e7` | XxConorxX | $12,848 | $0 | 2026-06-30 | OK | **YES** | +2.1% (805) | +0.5% (638) | 9,069 | criterion 4 (two_sided_share<0.2): 0.901 (over by 0.701); criterion 5 (median notional>=$20): $8.87 (short $11.13) |
| `0xd61068efe9bdd4c44825b888cadc016eb17f4c9a` | LFGKKO | $11,482 | $224 | 2026-06-05 | OK | **YES** | +7.9% (614) | +4.7% (917) | 4,582 | criterion 4 (active_hours_entropy<0.95): 0.959 (over by 0.009); criterion 5 (median notional>=$20): $16.16 (short $3.84) |
| `0x3b4bc12a86691200bf2b96e7b9aaaeaa7cdc94d4` | pcd | $1,020 | $0 | 2026-06-28 | OK | **YES** | +14.5% (170) | +1.9% (99) | 956 | criterion 4 (two_sided_share<0.2): 0.266 (over by 0.066) |
| `0xee613b3fc183ee44f9da9c05f53e2da107e3debf` | sovereign2013 | $3,588,720 | $0 | 2026-04-24 | OK | no | +5.8% (144) | +8.3% (626) | 366,711 | criterion 4 (median_inter_trade_seconds>=60s): 2s (short 58s); criterion 5 (median notional>=$20): $4.74 (short $15.26) |
| `0x00f7cd432b127dab1bffd6d54701f4eb88b64476` | CrvenaZvezda | $268 | $598 | 2026-05-31 | OK | no | +16.5% (34) | +15.2% (32) | 312 | criterion 1 (n>=50 pregame both periods): 2025: n=34 (short 16); 2026: n=32 (short 18); criterion 5 (median notional>=$20): $12.75 (short $7.25) |
| `0xb64f2747856045517b16a910c52886e835043237` | ExplainWatermelon | $99 | $133 | 2026-06-09 | OK | no | +10.3% (91) | +6.6% (48) | 228 | criterion 1 (n>=50 pregame both periods): 2026: n=48 (short 2); criterion 5 (median notional>=$20): $15.12 (short $4.88) |
| `0xbef7795f3b451833d982ad41a22d68c44573c774` | Regg2024 | $-28 | $9 | 2026-06-10 | WARN | no | +0.3% (213) | +14.1% (37) | 16 | criterion 1 (n>=50 pregame both periods): 2026: n=37 (short 13); criterion 4 (median_inter_trade_seconds>=60s): 16s (short 44s); criterion 5 (median notional>=$20): $1.00 (short $19.00) |
| `0x2275c805b31785dd424cf1fc2d9a848b54db7a5d` | aasb1 | $-36 | $5 | 2026-06-08 | WARN | no | +0.8% (232) | +4.3% (89) | 7 | criterion 5 (median notional>=$20): $1.50 (short $18.50) |
| `0x71a475af7a7cc6a87661c9e087de2425f377083d` | rhetgen | $-253 | $1 | 2026-06-30 | WARN | no | +0.1% (147) | +2.5% (282) | 28 | criterion 4 (median_inter_trade_seconds>=60s): 23s (short 37s); criterion 5 (median notional>=$20): $2.00 (short $18.00) |
| `0x805968ede90e5c228c26069a63bcbffa6dd8c043` | BET | $-1,558 | $250 | 2026-06-01 | WARN | no | +4.3% (353) | +2.6% (277) | 1,563 | criterion 4 (median_inter_trade_seconds>=60s): 40s (short 20s) |
| `0x8fba2c29715c41dd87e781c23373aa1e0549d08a` | abura2025 | $-7,159 | $75,776 | 2026-06-26 | WARN | no | +25.5% (204) | +19.6% (159) | 482,827 | criterion 4 (median_inter_trade_seconds>=60s): 49s (short 11s); criterion 5 (median notional>=$20): $12.73 (short $7.27) |
| `0x7b3c17043318369b371c00d31ec6b2b8111dffc7` | Netscape | $-9,515 | $0 | 2026-05-23 | WARN | no | +4.6% (310) | +32.0% (62) | 17,505 | criterion 4 (two_sided_share<0.2): 0.237 (over by 0.037); criterion 4 (active_hours_entropy<0.95): 0.972 (over by 0.022) |
| `0xacbe72e8da366d4f89a06e59da2ef517e3b285d4` | LDM8 | $-13,956 | $515 | 2026-06-17 | FAIL | no | +30.0% (39) | +10.7% (133) | 909 | criterion 1 (n>=50 pregame both periods): 2025: n=39 (short 11); criterion 4 (active_hours_entropy<0.95): 0.958 (over by 0.008); criterion 5 (median notional>=$20): $16.00 (short $4.00) |
| `0xd81fbc5c53593e4e2923a641ff2bc7e2d9866b75` | LBZone | $-53,782 | $25,912 | 2026-05-23 | FAIL | no | +0.5% (347) | +1.3% (740) | 19,963 | criterion 4 (median_inter_trade_seconds>=60s): 42s (short 18s); criterion 5 (median notional>=$20): $10.79 (short $9.21) |

11/22 candidates pass the external screen convincingly. Full records (per-period detail, screen notes) in `data/persistent_winners.json` -> `near_miss_candidates`. NOTE: rerunning `analysis/persistent_winners.py` regenerates this report without this section -- rerun `analysis/rescan_near_miss.py` to re-append it.
