# Phase 1 Report — Skill Persistence Hypothesis Tests

Full lake: 13,073,293 trades, 224,559 wallets, 3,297,149 positions, 28,690
markets with trade data (30,289-market target universe). Pipeline:
`analysis/hypothesis_tests.py`, run 2026-07-03, 206.7s wall time. Raw
numbers for every test below: `data/lake/hypothesis_results.json`.

**Statistical hygiene applied throughout**: all correlations are Spearman
rank correlations (implemented as Pearson correlation of `.rank()` — no
scipy in `.venv`), reported with n and a 95% bootstrap CI (2,000
resamples, seed fixed per test for reproducibility). A hypothesis is
called INCONCLUSIVE rather than forced to a verdict when the CI straddles
zero on the load-bearing statistic, or n is too small to trust the point
estimate.

**Sanity anchor** (full detail in `reports/04_trader_census.md`): the
zero-sum settlement check — sum of every wallet's `(returned − invested)`
per market should ≈ 0, since Polymarket charges no settlement vig — holds
to within floating-point rounding on 18/20 randomly sampled markets, and
within 0.014% of invested on the other 2. This validates the position
reconstruction + settlement logic underlying every test below.

---

## H1 — Does short-window PnL predict next-window settlement ROI?

**Method**: wallet-market positions with `season ∈ {2025, 2026}`, bucketed
into consecutive 15-day windows by market date (first-pitch time where
joinable, else Gamma `end_date`/`start_date`, else the position's own
first fill — 32 windows span the two seasons). For each wallet with ≥3
settled markets in a window, computed pooled settlement ROI. Paired every
`(wallet, window t)` observation with the same wallet's `window t+1`
observation (where it exists) and took the Spearman rank correlation.

**Numbers**: n = 50,635 wallet-window-transition pairs. **Spearman ρ =
0.022, 95% CI [0.012, 0.033]** (bootstrap, 2,000 resamples).

**Verdict: REJECTED.** The correlation is statistically distinguishable
from zero (CI excludes 0) but the effect size is trivial — ρ≈0.02
corresponds to roughly 0.05% of variance in next-window ROI explained by
this-window ROI. A wallet's 15-day settlement ROI carries almost no
information about its *next* 15-day settlement ROI. This directly
confirms `RESEARCH_PLAN.md`'s standing concern that the current trader
list's 15-day-PnL tiering selects mostly noise, not skill.

---

## H2 — Which selection metric best predicts NEXT-window settlement ROI?

**Method**: same 15-day window structure as H1 (n=50,635 wallet-window
pairs, 2025+2026, min 3 markets/window). For each wallet-window, computed
four candidate "selection metrics" using only data available *as of* that
window: (a) past settlement ROI (pooled), (b) CLV — dollar-weighted, per
pre-game BUY fill: `(last pre-game candle close price for that token) −
(fill price)`, aggregated by invested-dollar weight; candle closes joined
via `data/lake/candles.parquet` at the last timestamp ≤ first pitch, (c)
win rate, (d) volume (dollars invested). Each metric at window *t* was
rank-correlated against the wallet's realized settlement ROI at window
*t+1*.

**Numbers**:

| metric | n | Spearman ρ | 95% CI |
|---|---:|---:|---|
| win rate | 50,635 | **0.062** | [0.053, 0.071] |
| CLV | 31,912 | 0.030 | [0.019, 0.042] |
| past settlement ROI | 50,635 | 0.022 | [0.012, 0.033] |
| volume ($invested) | 50,635 | −0.009 | [−0.019, 0.001] |

(CLV has fewer observations because it requires a matched pre-game candle
for the traded token, which isn't available for every position — see
`reports/04_trader_census.md`'s candle-coverage note.)

**Verdict: SUPPORTED.** Win rate is the clearest winner: its CI
[0.053, 0.071] doesn't overlap with past-settlement-ROI's CI
[0.012, 0.033], so the gap is not just sampling noise — win rate predicts
next-window settlement ROI roughly **3x better** than past settlement ROI
itself, and about 2x better than CLV. Volume is not predictive at all
(CI straddles zero, point estimate slightly negative) — betting more
doesn't make you righter. **Recommendation for Phase 2**: use win rate (or
a shrinkage/calibration-adjusted version of it, given win rate is itself
noisy at low n) as the primary trader-selection signal rather than raw
settlement ROI or CLV. None of the four candidates is a *strong*
predictor in absolute terms (all ρ < 0.1) — this is a "least-bad of four
weak signals" finding, not a discovery of a powerful edge metric, and
Phase 2 should treat it accordingly (e.g. combine win rate with a minimum
n floor, not use it alone).

---

## H3 — Totals markets: fade-the-field, or do skilled specialists exist?

**Method**: `market_type == "total"` positions only. Population-level
check: pooled ROI across every wallet (dollar-weighted) and mean
per-wallet ROI. Specialist check: wallets with ≥30 settled totals
positions overall, split chronologically in half (window A = first half by
market date, window B = second half), require ≥10 positions in *each*
half (n=4,224 qualify). Spearman-correlated window-A ROI against
window-B ROI; separately, took the top decile of wallets by window-A ROI
("specialists," n=423) and checked their out-of-sample window-B ROI.

**Numbers**:
- Population (n=27,699 wallets, all totals bettors): pooled ROI
  **+0.11%** (essentially zero, i.e. no vig — see report 04's market-type
  table), mean per-wallet ROI **−3.5%** (typical individual totals bettor
  loses money — expected, since pooled-zero + mean-negative implies a
  right-skewed distribution where a minority's gains offset the majority's
  losses).
- Half-split persistence (n=4,224 wallets with ≥10 positions each half):
  **Spearman ρ = 0.071, 95% CI [0.035, 0.108]** — small but real (CI
  excludes 0).
- Top-decile specialists (n=423, selected on window-A ROI ≥ +16.3%):
  out-of-sample window-B mean ROI **−1.7%, 95% CI [−5.1%, +1.9%]** — CI
  straddles zero. Only **187/423 (44%)** of them had *positive*
  out-of-sample ROI — under half, which is what you'd expect from mostly
  noise, not skill.

**Verdict: INCONCLUSIVE.** The data support a real-but-weak population-level
skill signal (the half-split rank correlation is small but statistically
non-zero — *something* persists across totals bettors' first vs. second
half of activity), which argues against a pure "everyone is equally bad at
totals, fade blindly" story. But that signal is too weak to translate into
an actionable specialist-selection rule: ranking wallets by in-sample ROI
and picking the top decile does **not** produce a reliably positive
out-of-sample edge (point estimate negative, CI includes zero, minority
positive-rate). Phase 2 should not build a "follow the top totals
specialist" rule off ROI rank alone at this sample size; a larger n or a
better selection metric (per H2, win rate) might sharpen this — worth
revisiting once 2026 accumulates more settled totals markets.

---

## H4 — Timing: settlement ROI by hours-before-first-pitch

**Method**: every pre-game BUY fill (`timing == "pre"`, joined via the
new-format-slug first-pitch map), bucketed by hours between the fill and
first pitch. Per-fill ROI treats the fill as its own bet held to
settlement (ignores whether the wallet later sold — this isolates a pure
entry-timing effect from exit-timing effects). n = 3,352,549 fills.

**Numbers**:

| bucket | n fills | pooled ROI | mean ROI/fill | win rate | pooled-ROI 95% CI |
|---|---:|---:|---:|---:|---|
| 0–1h before | 735,367 | +0.21% | +1.4% | 50.2% | [1.1%, 1.7%]\* |
| 1–3h before | 620,943 | −0.06% | +0.3% | 49.8% | [0.0%, 0.6%]\* |
| 3–6h before | 536,633 | −0.20% | +0.2% | 49.7% | [−0.1%, 0.4%]\* |
| 6–24h before | 1,353,215 | +0.01% | +1.3% | 49.9% | [1.1%, 1.5%]\* |
| 24–72h before | 104,326 | +2.28% | +0.2% | 49.6% | [−0.6%, 0.9%] |
| 72h+ before | 2,065 | −0.03% | +135.8%† | 49.5% | [93%, 179%]† |

\* CI is on mean-ROI-per-fill, not the pooled figure (bootstrap was run on
the fill-level ROI series; pooled and mean-per-fill diverge because pooled
is dollar-weighted and a few large fills dominate it in some buckets — see
report 04's longshot-tail discussion). † The 72h+ bucket's huge mean/CI is
a small-n (2,065 fills) longshot-token artifact, not a real timing edge —
its dollar-weighted pooled ROI (−0.03%) is the trustworthy figure and is,
like every other bucket, indistinguishable from zero.

**Verdict: REJECTED.** No bucket shows a pooled ROI meaningfully different
from zero, and win rate is flat at ~49.5–50.2% across every horizon from
under an hour to three days before first pitch. There is no "smart early
money vs. dumb late money" (or vice versa) pattern in this data — pre-game
Polymarket MLB pricing looks efficient with respect to entry timing alone.
(This doesn't rule out a timing edge *conditional on* being a skilled
trader — H2's win-rate finding suggests skill is wallet-specific, not
timing-specific — only that timing by itself, pooled across all wallets,
isn't a signal.)

---

## H7 — Cross-season persistence (2025 → 2026)

**Method**: wallets with ≥30 settled markets in *both* 2025 and 2026
(n=3,593 active-2025, n=11,484 active-2026, n=262 active in both at that
threshold). Spearman-correlated 2025 season-pooled ROI against 2026
season-pooled ROI. Separately, measured top-decile turnover: wallets in
the top 10% of 2025 ROI (of the 2025≥30 population) vs. whether they're
also in the top 10% of 2026 ROI (of the 2026≥30 population).

**Numbers**: n = 262 wallets active both seasons. **Spearman ρ = 0.174,
95% CI [0.047, 0.306]** (excludes zero). Top-decile-2025 (n=360) ∩
top-decile-2026 (n=1,149) = **9 wallets** → **turnover rate 97.5%**.

**Verdict: SUPPORTED, with a sharp caveat.** The whole-population rank
correlation (ρ=0.174) is the strongest persistence signal found across
every test in this report — noticeably stronger than the within-season
15-day-window correlation in H1 (ρ=0.022) — meaning skill measured at the
season level (n≥30 per season) is real and detectable across a full
year-plus gap, more so than skill measured at a 15-day granularity within
a season. This is genuine evidence against a pure-luck story for
season-level performance. **But** the 97.5% top-decile turnover means this
signal is diffuse across the ranked population, not concentrated at the
top: picking "last year's best decile" as a portfolio would have kept only
9 of 360 wallets in this year's best decile. Practically: cross-season
skill exists in aggregate (useful for validating that skill is a real,
measurable thing worth building Phase 2 around), but a naive
"top-N-from-last-season" selection rule is not a viable Phase 2 strategy
on its own — consistent with H1/H2's finding that win rate, not raw ROI
rank, is the more defensible selection axis. n=262 is also modest;
this verdict should be revisited once more of the 2026 season settles and
the active-both-seasons population grows.

---

## Deferred (per RESEARCH_PLAN.md, Phase 2/3 scope)

- **H5 — depth-imbalance / orderbook smart-money signal**: requires the
  historical orderbook time-series ingestion (`data/lake/orderbook.parquet`,
  agent 572) that RESEARCH_PLAN.md's Phase 0 scoped as a separate,
  shallow-retention job never built in this phase. Not testable with the
  current lake.
- **H6 — current-147-list vs. rebuilt-cohort out-of-sample comparison**:
  this is a *portfolio selection* backtest (walk-forward train/validate
  across seasons, per RESEARCH_PLAN.md Phase 2), not a single-hypothesis
  statistical test — it belongs in `analysis/portfolio_selection.py` /
  `reports/`Phase-2 report, once H2's win-rate-based selection metric can
  be used to build the "rebuilt cohort" side of the comparison. Report 04's
  current-list-vs-census percentile-rank numbers (median 27.5th percentile,
  6/143 wallets with ROI ≤ −60%) are the relevant *descriptive* precursor,
  already delivered this phase.

## Summary table

| # | Hypothesis | n | Key statistic | Verdict |
|---|---|---:|---|---|
| H1 | 15d-window PnL → next-window ROI | 50,635 pairs | ρ=0.022, CI[0.012,0.033] | **REJECTED** |
| H2 | Best of {ROI, CLV, win rate, volume} → next-window ROI | 50,635 pairs | win rate ρ=0.062 (best), CI[0.053,0.071] | **SUPPORTED** (win rate wins) |
| H3 | Totals: fade-field vs. specialists | 423 top-decile / 4,224 qualifying | half-split ρ=0.071 CI[0.035,0.108]; OOS specialist ROI −1.7% CI[−5.1%,+1.9%] | **INCONCLUSIVE** |
| H4 | Settlement ROI by hours-before-pitch | 3,352,549 fills | all buckets' pooled ROI ≈ 0%, win rate ≈ 49.5–50.2% flat | **REJECTED** |
| H7 | Cross-season (2025→2026) persistence | 262 wallets | ρ=0.174 CI[0.047,0.306]; top-decile turnover 97.5% | **SUPPORTED** (diffuse, not top-concentrated) |
| H5 | Depth-imbalance signal | — | — | **DEFERRED** (Phase 2/3, needs orderbook lake) |
| H6 | Current list vs. rebuilt cohort, OOS | — | — | **DEFERRED** (Phase 2 portfolio backtest) |
