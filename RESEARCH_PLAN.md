# Reframe Sentiment3 as a Data Science Project — Trader Edge Research

## Context

Sentiment3 currently works as a software project: a dashboard that polls Polymarket MLB markets, computes weighted sentiment from ~134 pre-selected "top traders," and emits betting recommendations from a coarse backtest. The user wants to reframe it as a **data science project**: the entire Polymarket MLB market dataset is the corpus, and the research goal is to find the combination of traders (and signals derived from them) that yields the greatest edge for signals + copy trading for the rest of the 2026 MLB season.

The existing trader list and edge rules were produced by a less capable model and are treated as a scaffold, not ground truth. Everything is open to revisiting: which traders to follow, what to bet (moneyline / spread / total / props), when to bet, and how liquidity / orderbook depth / resting orders relate to outcomes (OddsJam SmartMoney-style analysis).

Starting point per user: **traders** — the smallest, most foundational unit of data that everything else builds on.

## Exploration findings

### Data inventory (upstream API)
Single endpoint `POST /api/v2/semantic/retrieve/parameterized`, 12 documented agents, pagination max 200/page:

| agent_id | What | Used today? |
|---|---|---|
| 574 | Markets (incl. `closed=True`, `winning_outcome`) | yes (open markets + per-cid resolution lookups only) |
| 556 | Trades (`proxy_wallet` or "ALL", time-windowed) | yes (7d rolling live; 90d one-off in discovery) |
| 572 | Orderbook snapshots (historical, per token) | yes (latest snapshot only) |
| 568 | **Candlesticks OHLCV (1m/5m/1h/1d)** | **NO — price history never captured** |
| 569 | PnL per wallet (daily granularity) | yes (90d, portfolio-wide) |
| 579 | Polymarket leaderboard (1d/3d/7d/30d) | yes (discovery only) |
| 584 | H-Score skill leaderboard (15d filters) | yes (discovery only) |
| 581 | Wallet 360 (60+ metrics; window_days max 15) | yes (15d window only) |
| 575 | **Market Insights (liquidity pctile, whale concentration)** | **NO** |
| 565/573 | **Kalshi markets/trades (cross-exchange)** | **NO** |
| 585 | **Social Pulse (tweets, acceleration)** | **NO** |

Key gaps: no persisted price series, no orderbook time series (only live snapshots), trade history capped at 7d rolling (live) / 90d single-page (discovery, limit 200 — likely truncates busy wallets), caches cap at 50 trades/wallet. Resolution ground truth = agent 574 `winning_outcome`, cached in `outcome_cache.json` (367 markets).

### How the current trader list was built (`find_top_mlb_traders.py`)
- Funnel: H-Score leaderboard (736 wallets, 4 sorts) ∪ Polymarket leaderboard (205 wallets, 3 periods) → 825 screened → Wallet-360 15d profile → MLB trade confirmation (90d, single page of 200) → tiers.
- Tiering is crude: `top_tier` = `baseball_pnl_15d >= $500` (58 wallets); `watchlist` = anything else with any MLB trade in 90d — includes a **-$417k** 15-day loser (89 wallets).
- Sentiment weight = `0.5*pnl_norm + 0.2*(win_rate-0.5)*2 + 0.15*sharpe/2 + 0.15*human_likeness` — hand-picked coefficients, never validated.
- `human_likeness_score` = hand-tuned point deductions from behavioral flags, not empirical.
- **Survivorship/sampling bias**: only leaderboard-surfaced wallets are candidates; consistent modest winners invisible. 15-day PnL window for tiering = tiny sample, high variance (may select lucky, not skilled).
- Trader list generated Jun 29, from Jun 28 caches — one-shot, no refresh cadence.

### Edge measurement (backtest)
- `backtest.py`: 384 predictions Apr 3–Jun 29, 353 resolved, **54.4% accuracy, +8.8% "ROI"** — but ROI = flat $1 at even odds (win-rate spread), never actual entry prices.
- **Look-ahead bias**: sentiment includes trades placed during/after games (no pre-game cutoff) — backtest accuracy is inflated vs what a live bettor could achieve. (`tracker.py` does proper pre-game snapshots, but has 9 tracked games / 0 resolved so far.)
- Accuracy is FLAT across conviction levels (0.8→54.4%, 1.0→52.9%) — raw conviction is not a signal. `conviction=1.0` is also a data-sparsity artifact (single-outcome markets default to 1.0).
- Market-type splits: moneyline 56.5% (n=170), spread 63.5% (n=63), total 42.7% (n=103 — worse than coin flip, hence the FADE rule).
- EDGE_RULES in recommend.py are hand-copied from an earlier backtest run (n=43 and n=26 anchor the strongest rules), never recomputed at runtime; rule-order bug: first-match means "ML high volume" pre-empts overlapping rules.
- `depth_imbalance` (orderbook smart-money metric) is computed and displayed but used in zero edge rules.

## User decisions

- Research first, then rebuild the pipeline from findings.
- Bulk data pulls approved — long, resumable ingestion jobs OK (overnight fine).
- Optimize for manual betting signals now; design trader research to also answer copy-trading questions (entry-price decay, latency) later.
- Execution work runs on Sonnet subagents; planning/review stays on Fable (main loop).
- **Source of truth: market settlement.** Every skill metric, portfolio comparison, and edge rule is ultimately judged by backtesting against the resolved `winning_outcome` (settlement) of each market — ROI = payout at settlement (1 if the bet's outcome won, 0 if not) minus actual entry price. CLV and other price-based measures are secondary diagnostics (useful for persistence analysis), never the deciding criterion.
- **Two data sources allowed**: the intelligence API already in the repo AND the official public Polymarket APIs — Gamma (`gamma-api.polymarket.com`: markets/events incl. resolution data), CLOB (`clob.polymarket.com`: orderbooks, `prices-history` per token), and Data-API (`data-api.polymarket.com`: trades, positions, holders, wallet activity). Use whichever has better retention/rate limits per dataset, and cross-validate settlements between the two.
- **Full historical scope: ALL MLB seasons since Polymarket had MLB markets** (likely ~2022/2023 onward — probes discover the true earliest), not just the 2026 season. Multiple seasons of settled markets is the dataset; 2026 is the live validation set.

## Research plan

### Probes first (~30 min, ~10 calls) — validate risky assumptions BEFORE long ingestion

Deliverable: `analysis/ingest/probe.py` + `reports/00_probes.md` with go/no-go per item.

| # | Probe | Validates | Fallback |
|---|---|---|---|
| P1 | 574 `closed="True"` + `market_slug="mlb"`, 3 pages; then Gamma closed-MLB sweep sorted oldest-first | Bulk closed-market discovery, `winning_outcome` populated, **earliest MLB season available** (2022? 2023?) | Window by `end_date_min/max` per season |
| P2 | 556 trades for a market from the EARLIEST season found in P1 | **Trade retention back multiple years** (biggest risk) | **Data-API `/trades`** per market (public, full history) |
| P3 | 556 `proxy_wallet="ALL"` + `condition_id` | All-wallet per-market pulls (kills survivorship bias) | Data-API per-market trades; per-slug sweeps |
| P4 | 568 candles (1h) for a resolved April token | Historical price retention → CLV/entry-price modeling | **CLOB `prices-history`** per token |
| P5 | 572 orderbook history for a resolved token | Historical depth study | Depth study forward-only (CLOB book is live-only) |
| P6 | Paginate past page 5 on a busy query | `has_more` beyond current `max_pages=5` | Time-slice queries |
| P7 | `statsapi.mlb.com` schedule (free) | Game start times → pre-game cutoff | Infer first pitch from 1m-candle volume spike |
| P8 | Gamma + Data-API + CLOB reachability (no auth) for one known market | Official Polymarket APIs as second source; settlement cross-check vs agent 574 | Intelligence API only |

### Phase 0 — Dataset foundation ("lake")

Ingest **all MLB seasons from the earliest available (per P1, likely 2022–2023) → present**, then daily incremental. Ingestion runs per-season (oldest→newest), each season a separate resumable job:
- **574/Gamma**: all MLB markets open+closed with `winning_outcome`/resolution, per season (~1–6k markets/season → tens of thousands total).
- **556 or Data-API per condition_id**: every trade by every wallet in every MLB market — census backbone. Largest job: est. 20–80k calls across seasons ≈ several sessions or one long weekend of resumable runs; per-season chunks land in hours each.
- **568/CLOB prices-history**: 1h candles per outcome token game-day window (all seasons); 1m candles for final pre-game hours on signal markets (2026 focus).
- **572**: orderbook sample study — recent games at T-6h/T-1h/T-5m (~2–5k calls; depth history likely shallow — probe P5).
- **575**: snapshot-only → add to daily poll going forward.
- **581/569**: enrich only wallets with ≥10 MLB trades from census.
- **MLB schedule** via statsapi → first-pitch times for ALL ingested seasons (statsapi covers all years).

Storage: **Parquet + DuckDB** (columnar analytics over ~10⁵–10⁶ trade rows, serverless, pandas-interop). Raw API pages archived as append-only JSONL so tables rebuild without re-fetching. New deps: `duckdb`, `pyarrow`, `pandas`.

```
analysis/ingest/{client.py, probe.py, ingest_markets.py, ingest_trades.py,
                 ingest_candles.py, ingest_orderbooks.py, ingest_schedule.py}
analysis/build_lake.py            # raw JSONL → parquet
data/lake/raw/<job>/*.jsonl       # archived pages
data/lake/state/<job>.json        # completed ids → resumable
data/lake/{markets,trades,candles,orderbook,schedule}.parquet
```

`client.py` wraps BOTH sources behind one interface: the intelligence API (reusing the proven `api_call` 429-backoff from `scripts/poll_live.py`, `MAX_WORKERS=6`) and the official Polymarket APIs (Gamma/CLOB/Data-API, no auth). Per-dataset source selection comes from probe results; every job idempotent/resumable.

Success: ≥95% of resolved MLB markets in every ingested season have trades + `winning_outcome` + a pre-game closing price; daily row counts ≈ ~15 games/day in-season per year.

### Phase 1 — Trader census & skill metrics (real prices, first principles)

`analysis/trader_metrics.py` → `data/lake/traders.parquet` + `reports/01_trader_census.md`, `reports/02_skill_persistence.md`.

Per wallet: position reconstruction (BUY/SELL netting per token) → **primary metric: settlement ROI — actual entry prices vs resolved `winning_outcome` payout** (this is the source of truth for trader skill); secondary diagnostics: CLV (entry vs pre-game close, tested for persistence), pre-game vs in-game split; bet-type specialization (better slug+question classifier — current regex misses props); sizing; rolling-window consistency; behavioral MM/bot detection from trade patterns (inter-trade intervals, two-sided quoting, 24/7 activity).

Hypotheses to verdict (all judged on settlement outcomes): H1 15d-PnL tiering selects luck (settlement-ROI rank correlation across adjacent windows) · H2 which selection metric best predicts NEXT-window settlement ROI (past settlement ROI vs CLV vs win rate) · H3 skilled totals-traders exist vs fade-everyone · H4 early bettors beat late bettors (settlement ROI by hours-to-pitch) · H5 depth imbalance predicts settlement outcomes · H6 current 134-trader list underperforms a rebuilt cohort on out-of-sample settlement ROI · **H7 skill persists ACROSS SEASONS** (do 2024/2025 winners stay winners in 2026? how much trader turnover between seasons?) — the multi-year dataset makes this the strongest luck-vs-skill test available.

Success: full census (expect 5–20k wallets vs 825 screened today); every current top_tier trader re-scored with settlement ROI at real entry prices.

### Phase 2 — Trader portfolio selection

`analysis/portfolio_selection.py` → `reports/03_portfolio.md` + `data/trader_portfolio.json`.

- Walk-forward across seasons: train on earlier seasons → validate on the next; final holdout = 2026 season to date. Within 2026: rolling 6-week train / 2-week validate for in-season adaptation. Handle trader turnover explicitly (a portfolio must degrade gracefully when its traders go dormant between seasons).
- Selectors: settlement-ROI-top-K, CLV-top-K, greedy marginal-add, L1-regularized logistic (settlement outcome ~ signed trader stakes) — all selected on train, judged on validation settlement outcomes.
- Weightings: equal / skill-proportional / learned — vs baselines: current 134-list + 0.5/0.2/0.15/0.15 formula, market-favorite, unweighted-everyone.
- Overfit guards: min 30 pre-game settled bets per trader, bootstrap CIs, select on train only.

Success: portfolio beats current list on held-out validation windows (prior-season → next-season AND 2026-to-date) in **settlement ROI** (real entry prices vs settlement payouts), CI excluding zero.

### Phase 3 — Signals & edge rules

`analysis/backtest_v2.py` → `reports/04_edge_rules.md` + `data/edge_rules.json`.

- Every candidate rule backtested against **settlement outcomes**: pre-game cutoff (fixes look-ahead), entry at prevailing candle price at signal time, ROI = settlement payout − entry price, **EV = p̂(win at settlement) − entry price** (breakeven is the price, not 50%).
- Features: consensus strength, depth imbalance, liquidity percentile (575), volume trend, time-to-game, price level, line movement.
- Rules mined as shallow interpretable buckets (n≥50 settled markets, binomial CI lower bound on settlement win rate > entry price + slippage) → `data/edge_rules.json`, never hardcoded again.
- Copy-trading groundwork: entry-price decay curves after sharp-trader entries → latency budget.

### Phase 4 — Pipeline rebuild + forward validation

- `find_top_mlb_traders.py` → census-driven `analysis/refresh_traders.py` (weekly), emitting portfolio+weights consumed by `scripts/compute_sentiment.py` / `scripts/poll_live.py`.
- `scripts/recommend.py` reads `data/edge_rules.json` dynamically.
- `scripts/tracker.py` extended: entry+closing price per snapshot (live CLV), paper-trade ledger with bankroll.
- 2–4 weeks paper trading; success = positive real-price ROI and mean CLV > 0 on tracked picks.

## Dependencies & risks

Probes → 0 → 1 → 2 → 3 → 4 strictly. Riskiest: P2 multi-year trade retention and P4 candle retention on the intelligence API — the official Polymarket Data-API/CLOB provide full-history fallbacks for both; worst remaining risk is P5 (historical orderbook depth may be intelligence-API-only and shallow). Multi-season lake build is the biggest job (est. 20–80k calls) but runs per-season, oldest first, fully resumable — Phase 1 analysis can begin on completed seasons while later ingestion continues. Early seasons may be thin (low liquidity when Polymarket MLB was new) — per-season volume stats decide what's usable.

## Verification

- **Probes**: each probe prints raw row counts + earliest timestamps; go/no-go recorded in `reports/00_probes.md` before any bulk job starts.
- **Lake integrity**: DuckDB sanity queries — games/day vs MLB schedule, % resolved markets with winning_outcome, trades-per-market distribution; spot-check 3 known games end-to-end.
- **Settlement ground truth**: audit `winning_outcome` coverage/correctness — every resolved market must have a settlement; spot-check ~10 settlements against actual MLB final scores (statsapi) to validate agent 574's resolution data before trusting it project-wide.
- **Metrics correctness**: reconstruct 2–3 wallets' positions by hand from raw trades and compare to computed settlement ROI; compare census PnL vs API Wallet-360 PnL for a sample (should correlate, not match exactly).
- **Backtest honesty**: assert zero trades after first pitch enter any signal; walk-forward results reported on validation windows only.
- **End-to-end**: after Phase 4, run `scripts/poll_live.py` + dashboard and confirm recommendations now derive from `data/edge_rules.json`; tracker accumulates paper-trade entries with entry prices.


## Execution status (as of 2026-07-03)

- **Probes: DONE** (reports/00_probes.md). CLOB era only (2024 playoffs+2025+2026). CAUTION: Data-API is NOT census-safe (drops one counterparty leg per matched trade; hard offset-3000 cap) — intel 556 is the only valid trade source.
- **Phase 0 lake: DONE.** data/lake/: markets 42,893 ($2.0B, settlements cross-validated 100%), trades 13,073,026 unique rows / 229,128 wallets / 28,688 markets, candles 1.96M rows / 30,262 markets, schedule all seasons. Raw JSONL archived (7.9GB trades). trades.parquet MUST be built via two-pass DuckDB (per-season JSONL→parquet, then DISTINCT; pandas + single-pass DuckDB window both OOM on 16GB).
- **Phase 1 preview: DONE** (reports/03_census_preview.md, partial data): H1 direction confirmed (median wallet -0.1% ROI, positive only at n>=10-50); current 147-list median +19.9% but 13% catastrophic losers.
- **Phase 1 full run: IN PROGRESS** (Sonnet agent): build_lake trades patch, straggler mop-up (2 missing markets, ~40 futures under-capture recheck, 25 candle gaps), full trader_metrics re-run, reports/04_trader_census.md + reports/05_skill_persistence.md with H1/H2/H3/H4/H7 verdicts (H5/H6 deferred to Phases 2-3). Zero-sum sanity check per market included.
- Sweeps all finished; no background ingestion running. Ingest scripts use manual argv parsing — NEVER pass --help (silently starts a default sweep).
- **Phase 2: DONE** (reports/06_portfolio.md). NO trader-selection portfolio deployably beats baselines; the aggregate crowd (all-wallets-equal) beat market-favorite +2.0-2.2% at zero-slippage entry, concentrated in totals — handed to Phase 3 as the executability question.
- **Phase 3: DONE** (reports/07_edge_rules.md, data/edge_rules.json). Orderbook sample ingested (intel 572; 4,914 target markets in primary + alt_line cohorts, 253k snapshots) — DISCOVERED: 572 retention is a rolling ~3-month window (zero 2025 coverage; floor ~2026-03-26), so all Phase 3 results are 2026-only. Verdicts: H5 REJECTED (depth imbalance adds nothing to crowd flow: AUC delta -0.001, coef CI spans 0, n=4,035). The crowd zero-slippage edge REPLICATES in-window (+4.2% all lines; +6.3% on alternate O/U totals lines; ~0% on max-stake primary lines and moneyline) but is NOT EXECUTABLE: at $50/$200/$1000 depth-weighted fills it inverts to -1.1%..-1.9% (all CIs span 0) — the "edge" equals the spread/stale-candle gap on thin alt-line books (mean top-of-book spread 6.1c there). ZERO edge rules cleared even the train bar (n≥50, after-slippage bootstrap CI>0); data/edge_rules.json is empty-but-valid. Copy-trade: topk_clv-cohort entries run +3c mean (median +0.5c, 59% adverse) against a copier within 1h — no viable copy window. Recommendation: Phase 4 deploys NOTHING from trader selection or crowd-flow-at-close; only forward-collected executable-price (live book) signals justify further modeling.
- Process rule: plan/orchestrate/review on Fable; execute via Sonnet subagents.
- **Phase 4-lite: DONE.** `scripts/recommend.py` loads rules from `data/edge_rules.json` dynamically (hardcoded EDGE_RULES deleted) — currently 0 rules, so `bet_recommendations.json` carries `bets: [] / fades: [] / notice: "..."` explaining the Phase 3 null result; dashboard (`scripts/static/app.js` + `index.html`) no longer computes its own confidence badges/edge filter off the debunked 74%/73%/64% backtest numbers, and renders the notice in the betting slip instead. `scripts/tracker.py` snapshots now also record executable entry pricing per predicted outcome (best ask + top-of-book depth via `poll_live.fetch_orderbook`, plus last pre-game candle close via CLOB's public `prices-history`) so `tracked_games.json` becomes an honest signal-vs-executable-price-vs-settlement forward test-bed. New `analysis/ingest/collect_orderbooks_daily.py` runs daily against today's live-signal MLB markets to keep feeding intel-572 orderbook history into `data/lake/raw/orderbooks/` going forward — **intel 572 retention is only a rolling ~3-month window (reports/07_edge_rules.md), so any market-day not collected by this script (or the Phase 3 bulk sweep) is lost permanently, with no backfill possible.**
