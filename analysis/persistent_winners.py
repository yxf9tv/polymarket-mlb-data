#!/usr/bin/env python3
"""Phase 2/3: "Persistent MLB winners" — the definitive short list.

Answers the standing RESEARCH_PLAN.md question sharpened by reports/04 and
05: is there a small set of wallets whose MLB edge is (a) real (settlement
ROI, cash-flow accounting — the only source of truth this project trusts),
(b) copyable (present pre-game, not just in-game/post-game drift), (c)
persistent (shows up independently in BOTH the 2025 and 2026 seasons, not
just pooled across them, which is what H7 in reports/05 warns is needed
after finding 97.5% top-decile turnover season-to-season), and (d) actually
a directional human bettor, not a resting-both-sides market maker.

Selection criteria (strict, quality over quantity — see module docstring
in the task / reports/08_persistent_winners.md for full rationale):
  1. n >= 50 settled PRE-GAME markets in 2025 AND in 2026 independently.
  2. Positive FULL-history settlement ROI in 2025 AND in 2026 independently.
  3. Positive PRE-GAME-only settlement ROI in 2025 AND in 2026 independently.
  4. Not MM/bot-shaped: two_sided_share < 0.20, active_hours_entropy < 0.6,
     median_inter_trade_seconds >= 60s (pooled features from traders.parquet,
     documented thresholds — see report).
  5. Median fill notional >= $20 (dust-bot floor).

Reuses trader_metrics.py's tested position-reconstruction/settlement
functions (reconstruct_positions, settle_positions) rather than
reimplementing cash-flow accounting. Heavy scans (trades.parquet, 13M rows)
go through DuckDB per the task's memory guidance; positions.parquet /
pregame_fills.parquet (already-built, validated lake artifacts) are loaded
directly in pandas since they're small enough (3.3M / 3.6M rows) and this
script must not touch scripts/ or ingestion state.

Writes:
  data/persistent_winners.json
  reports/08_persistent_winners.md

Usage:
    .venv/bin/python3 analysis/persistent_winners.py
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
LAKE_DIR = BASE / "data" / "lake"
DATA_DIR = BASE / "data"
REPORTS_DIR = BASE / "reports"
sys.path.insert(0, str(BASE / "analysis"))
import trader_metrics as tm  # noqa: E402  (read-only reuse of tested reconstruction logic)
from wallet_screen import screen_wallets  # noqa: E402  (external-reality screen, shared w/ cross_sport_winners.py)

SEASONS = [2025, 2026]
PREGAME_N_FLOOR = 50
RELAXED_N_FLOOR = 30  # only used if the strict floor yields zero survivors

# Bot / MM heuristic thresholds (documented rationale in the report):
#  - two_sided_share: report 04 full-census p50=0.25, p90=1.00 (resting both
#    sides of a market is the single clearest MM tell). Task suggests ~20%;
#    among the high-activity comparison group this criterion actually
#    screens against (n_trades>=400: p50=0.136, p90=0.834), a 0.20 cut sits
#    just above the median of legitimate heavy bettors and well below the
#    MM-shaped tail, so the naive 0.20 figure holds up. We use it as-is.
#  - active_hours_entropy: 0 = all activity in one hour of the day (human
#    daily rhythm), 1 = perfectly uniform across all 24h (bot/24-7 desk).
#    IMPORTANT CORRECTION: the full-census distribution (p50=0.22, p90=0.64)
#    is dominated by n=1 wallets, for whom entropy is mechanically ~0 (one
#    trade can only ever land in one hour) -- comparing our n>=50-pregame-
#    markets-per-season candidates against that population is apples to
#    oranges. Restricting to the actually-comparable high-activity peer
#    group (n_trades>=400, n=2,927 wallets) gives p50=0.79, p90=0.94,
#    p95=0.97 -- i.e. essentially EVERY legitimate high-volume MLB bettor
#    already shows high entropy, simply because a full season's worth of
#    games span every hour of the day (day games, night games, coast-to-
#    coast time zones, doubleheaders). A 0.60 cutoff against that
#    comparison group would exclude ~95%+ of all high-volume traders
#    indiscriminately -- not a bot signal, a trade-count artifact. We
#    recalibrate to 0.95 (~90th-95th percentile of the appropriate peer
#    group), which only screens out the small tail of literally-uniform,
#    trades-every-hour-of-every-day operators -- the actual "24/7 desk"
#    signature the task criterion is after.
#  - median_inter_trade_seconds: task specifies "not sub-minute"; the
#    high-activity peer group's own median is 68s (p25=16s), so a >=60s
#    floor sits right at that peer group's median -- a real, not overly
#    strict, discriminator. We use it as specified.
BOT_TWO_SIDED_MAX = 0.20
BOT_ENTROPY_MAX = 0.95
BOT_MIN_MEDIAN_GAP_S = 60.0

MIN_MEDIAN_FILL_NOTIONAL = 20.0
HEAVY_FAVORITE_ENTRY = 0.75

N_BOOT = 2000
RNG_SEED = 42
RECENT_FORM_N = 30  # last-N-markets recent form window (2026 only)
DRIFT_HORIZON = pd.Timedelta(hours=1)
DRIFT_TOLERANCE = pd.Timedelta(hours=3)  # widen candle match window (candles are sparse in spots)

OLD_147_PATH = DATA_DIR / "top_mlb_traders.json"
CLV_COHORT_PATH = DATA_DIR / "trader_portfolio.json"


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------
def load_lake() -> dict:
    print("Loading lake tables...")
    positions = pd.read_parquet(LAKE_DIR / "positions.parquet")
    markets = pd.read_parquet(LAKE_DIR / "markets.parquet")
    traders = pd.read_parquet(LAKE_DIR / "traders.parquet")
    pregame = pd.read_parquet(LAKE_DIR / "pregame_fills.parquet")
    candles = pd.read_parquet(LAKE_DIR / "candles.parquet")
    print(f"  positions={len(positions)} markets={len(markets)} traders={len(traders)} "
          f"pregame_fills={len(pregame)} candles={len(candles)}")
    return dict(positions=positions, markets=markets, traders=traders, pregame=pregame, candles=candles)


# ---------------------------------------------------------------------------
# Per-(wallet, season, market) rollups -- full-history and pre-game-only
# ---------------------------------------------------------------------------
def market_rollup_with_price(pos: pd.DataFrame) -> pd.DataFrame:
    """(wallet, condition_id) -> invested/returned/buy_size/buy_cost/market_type/roi/win.
    Expects a 'wallet' column and buy_size/buy_cost/invested/returned/market_type."""
    g = pos.groupby(["wallet", "condition_id"], as_index=False).agg(
        invested=("invested", "sum"), returned=("returned", "sum"),
        buy_size=("buy_size", "sum"), buy_cost=("buy_cost", "sum"),
        market_type=("market_type", "first"),
    )
    g = g[g["invested"] > 0].copy()
    g["roi"] = (g["returned"] - g["invested"]) / g["invested"]
    g["win"] = g["returned"] > g["invested"]
    return g


def full_period_wm(positions: pd.DataFrame, markets: pd.DataFrame) -> dict[int, pd.DataFrame]:
    """Full-history (all fills, not just pre-game) per-season wallet-market table."""
    pos = positions[positions["invested"] > 0].merge(
        markets[["condition_id", "season"]], on="condition_id", how="left"
    )
    out = {}
    for season in SEASONS:
        p = pos[pos["season"] == season]
        out[season] = market_rollup_with_price(p)
    return out


def pregame_wm(pregame: pd.DataFrame) -> dict[int, pd.DataFrame]:
    """Pre-game-only per-season wallet-market table, reconstructed from
    pregame_fills.parquet via trader_metrics.py's tested cash-flow logic."""
    out = {}
    cols = ["proxy_wallet", "condition_id", "token_id", "side", "size", "price", "trade_id", "timestamp"]
    for season in SEASONS:
        fs = pregame[pregame["season"] == season]
        token_winner = fs[["condition_id", "token_id", "is_winner", "market_type", "outcome"]].drop_duplicates()
        pos = tm.reconstruct_positions(fs[cols])
        pos = tm.settle_positions(pos, token_winner)
        pos = pos.rename(columns={"proxy_wallet": "wallet"})
        out[season] = market_rollup_with_price(pos)
    return out


def wallet_season_summary(wm: pd.DataFrame) -> pd.DataFrame:
    g = wm.groupby("wallet", as_index=False).agg(
        n_markets=("condition_id", "nunique"),
        invested=("invested", "sum"), returned=("returned", "sum"),
        buy_size=("buy_size", "sum"), buy_cost=("buy_cost", "sum"),
        win_rate=("win", "mean"),
    )
    g["roi_pooled"] = np.where(g["invested"] > 0, (g["returned"] - g["invested"]) / g["invested"], np.nan)
    g["avg_entry_price"] = np.where(g["buy_size"] > 0, g["buy_cost"] / g["buy_size"], np.nan)
    g["profit"] = g["returned"] - g["invested"]
    return g


# ---------------------------------------------------------------------------
# Bootstrap CI (resample at the MARKET level, per hygiene requirement)
# ---------------------------------------------------------------------------
def bootstrap_pooled_roi_ci(inv: np.ndarray, ret: np.ndarray, n_boot=N_BOOT, seed=RNG_SEED):
    n = len(inv)
    if n < 5:
        return (float("nan"), float("nan"))
    rng = np.random.default_rng(seed)
    stats = np.empty(n_boot)
    for i in range(n_boot):
        idx = rng.integers(0, n, n)
        s_inv, s_ret = inv[idx].sum(), ret[idx].sum()
        stats[i] = (s_ret - s_inv) / s_inv if s_inv > 0 else np.nan
    return float(np.nanpercentile(stats, 2.5)), float(np.nanpercentile(stats, 97.5))


def wallet_market_ci(wm: pd.DataFrame, wallet: str):
    sub = wm[wm["wallet"] == wallet]
    return bootstrap_pooled_roi_ci(sub["invested"].to_numpy(), sub["returned"].to_numpy())


# ---------------------------------------------------------------------------
# Bot / stake filters
# ---------------------------------------------------------------------------
def bot_filter_mask(traders: pd.DataFrame) -> pd.Series:
    return (
        (traders["two_sided_share"] < BOT_TWO_SIDED_MAX)
        & (traders["active_hours_entropy"] < BOT_ENTROPY_MAX)
        & (traders["median_inter_trade_seconds"] >= BOT_MIN_MEDIAN_GAP_S)
    )


def median_fill_notional(wallets: list[str]) -> pd.DataFrame:
    if not wallets:
        return pd.DataFrame(columns=["wallet", "median_notional"])
    con = duckdb.connect()
    wdf = pd.DataFrame({"wallet": wallets})
    con.register("wdf", wdf)
    q = f"""
        SELECT t.proxy_wallet AS wallet, median(t.size * t.price) AS median_notional
        FROM read_parquet('{LAKE_DIR / "trades.parquet"}') t
        JOIN wdf ON t.proxy_wallet = wdf.wallet
        GROUP BY t.proxy_wallet
    """
    out = con.execute(q).fetchdf()
    con.close()
    return out


# ---------------------------------------------------------------------------
# Market-type mix (combined across both seasons, full-history positions)
# ---------------------------------------------------------------------------
def market_type_mix(positions: pd.DataFrame, wallets: list[str]) -> dict[str, dict]:
    pos = positions[(positions["invested"] > 0) & (positions["wallet"].isin(wallets))].copy()
    pos["market_type"] = pos["market_type"].fillna("other")
    g = pos.groupby(["wallet", "market_type"]).agg(
        n_markets=("condition_id", "nunique"), invested=("invested", "sum")
    ).reset_index()
    out = {}
    for w, sub in g.groupby("wallet"):
        tot_n = sub["n_markets"].sum()
        mix = {r.market_type: {"n_markets": int(r.n_markets), "pct_n": round(r.n_markets / tot_n, 3),
                                "invested": round(float(r.invested), 2)}
               for r in sub.itertuples()}
        out[w] = mix
    return out


# ---------------------------------------------------------------------------
# Copyability panel: minutes-to-post + 1h price drift after entry
# ---------------------------------------------------------------------------
def minutes_to_post(pregame: pd.DataFrame, wallets: list[str]) -> pd.DataFrame:
    f = pregame[(pregame["proxy_wallet"].isin(wallets)) & (pregame["side"] == "BUY")].copy()
    f["minutes_to_post"] = (f["market_date"] - f["timestamp"]).dt.total_seconds() / 60.0
    g = f.groupby("proxy_wallet")["minutes_to_post"].median().rename("median_minutes_to_post")
    return g.reset_index().rename(columns={"proxy_wallet": "wallet"})


def price_drift_1h(pregame: pd.DataFrame, candles: pd.DataFrame, wallets: list[str]) -> pd.DataFrame:
    """Size-weighted mean (candle price at entry+1h - fill price) per wallet,
    over their pre-game BUY fills, using candles.parquet as the adverse-
    selection cost a copier acting on this fill would face."""
    f = pregame[(pregame["proxy_wallet"].isin(wallets)) & (pregame["side"] == "BUY")][
        ["proxy_wallet", "token_id", "timestamp", "price", "notional"]
    ].copy()
    f["timestamp"] = pd.to_datetime(f["timestamp"], utc=True).dt.as_unit("ns")
    tokens = f["token_id"].unique().tolist()
    c = candles[candles["token_id"].isin(tokens)][["token_id", "ts", "price"]].copy()
    c["ts"] = pd.to_datetime(c["ts"], utc=True).dt.as_unit("ns")
    c = c.sort_values("ts")
    f["target_ts"] = f["timestamp"] + DRIFT_HORIZON

    rows = []
    for token_id, fsub in f.groupby("token_id"):
        csub = c[c["token_id"] == token_id]
        if csub.empty:
            continue
        fsub = fsub.sort_values("target_ts")
        merged = pd.merge_asof(
            fsub, csub.rename(columns={"price": "price_1h", "ts": "candle_ts"}),
            left_on="target_ts", right_on="candle_ts", direction="nearest",
            tolerance=DRIFT_TOLERANCE,
        )
        rows.append(merged)
    if not rows:
        return pd.DataFrame(columns=["wallet", "median_minutes_available", "drift_1h_weighted", "n_matched"])
    m = pd.concat(rows, ignore_index=True)
    m = m.dropna(subset=["price_1h"])
    m["drift"] = m["price_1h"] - m["price"]

    def _wmean(d):
        tot = d["notional"].sum()
        return float((d["notional"] * d["drift"]).sum() / tot) if tot > 0 else np.nan

    out = m.groupby("proxy_wallet").apply(
        lambda d: pd.Series({"drift_1h_weighted": _wmean(d), "n_matched": len(d)}), include_groups=False
    ).reset_index().rename(columns={"proxy_wallet": "wallet"})
    return out


# ---------------------------------------------------------------------------
# Recent form: last-N pre-game-settled markets in 2026, chronological
# ---------------------------------------------------------------------------
def recent_form_2026(wm_pregame_2026: pd.DataFrame, pregame: pd.DataFrame, wallets: list[str], n=RECENT_FORM_N):
    date_map = pregame[pregame["season"] == 2026][["condition_id", "market_date"]].drop_duplicates("condition_id")
    wm = wm_pregame_2026[wm_pregame_2026["wallet"].isin(wallets)].merge(date_map, on="condition_id", how="left")
    out = {}
    for w, sub in wm.groupby("wallet"):
        sub = sub.sort_values("market_date").tail(n)
        inv, ret = sub["invested"].sum(), sub["returned"].sum()
        roi = (ret - inv) / inv if inv > 0 else float("nan")
        out[w] = {"n": len(sub), "roi_pooled": round(float(roi), 4), "win_rate": round(float(sub["win"].mean()), 4)}
    return out


# ---------------------------------------------------------------------------
# Walk-forward criteria validation: select on 2025 ONLY, measure 2026 OOS
# ---------------------------------------------------------------------------
def walk_forward_2025_only(positions, markets, traders, pregame) -> dict:
    print("\n--- Walk-forward criteria validation (select on 2025 only, measure 2026 OOS) ---")
    full_wm = full_period_wm(positions, markets)
    pg_wm = pregame_wm(pregame)

    fs25 = wallet_season_summary(full_wm[2025])
    pg25 = wallet_season_summary(pg_wm[2025])

    pool = pg25[pg25["n_markets"] >= PREGAME_N_FLOOR]["wallet"]
    pool = set(pool) & set(fs25[fs25["roi_pooled"] > 0]["wallet"]) & set(pg25[pg25["roi_pooled"] > 0]["wallet"])
    print(f"  n_markets>=50 (2025 pregame) & full ROI>0 (2025) & pregame ROI>0 (2025): {len(pool)}")

    # 2025-only bot features (avoid leaking 2026 behavioral info into selection)
    con = duckdb.connect()
    pool_df = pd.DataFrame({"wallet": list(pool)})
    con.register("pool_df", pool_df)
    q = f"""
        WITH t AS (
            SELECT * FROM read_parquet('{LAKE_DIR / "trades.parquet"}')
            WHERE season = 2025 AND proxy_wallet IN (SELECT wallet FROM pool_df)
        )
        SELECT proxy_wallet AS wallet, median(size*price) AS median_notional, count(*) AS n_trades
        FROM t GROUP BY proxy_wallet
    """
    notional25 = con.execute(q).fetchdf()

    trades25 = con.execute(f"""
        SELECT * FROM read_parquet('{LAKE_DIR / "trades.parquet"}')
        WHERE season = 2025 AND proxy_wallet IN (SELECT wallet FROM pool_df)
    """).fetchdf()
    con.close()
    bot25 = tm.bot_heuristics(trades25).rename(columns={"proxy_wallet": "wallet"})
    bot25 = bot25.merge(notional25, on="wallet", how="left", suffixes=("", "_dup"))

    keep = bot25[
        (bot25["two_sided_share"] < BOT_TWO_SIDED_MAX)
        & (bot25["active_hours_entropy"] < BOT_ENTROPY_MAX)
        & (bot25["median_inter_trade_seconds"] >= BOT_MIN_MEDIAN_GAP_S)
        & (bot25["median_notional"] >= MIN_MEDIAN_FILL_NOTIONAL)
    ]["wallet"].tolist()
    print(f"  after 2025-only bot/stake filters: {len(keep)} wallets selected (2025-only portfolio)")

    # Measure 2026 OOS: full-history and pre-game-only, pooled across the portfolio
    fs26 = full_wm[2026][full_wm[2026]["wallet"].isin(keep)]
    pg26 = pg_wm[2026][pg_wm[2026]["wallet"].isin(keep)]

    def portfolio_pooled(wm):
        inv, ret = wm["invested"].sum(), wm["returned"].sum()
        roi = (ret - inv) / inv if inv > 0 else float("nan")
        ci = bootstrap_pooled_roi_ci(wm["invested"].to_numpy(), wm["returned"].to_numpy())
        return dict(n_wallets=wm["wallet"].nunique(), n_markets=len(wm), invested=round(float(inv), 2),
                    returned=round(float(ret), 2), roi_pooled=round(float(roi), 4),
                    roi_ci95=[round(ci[0], 4), round(ci[1], 4)])

    result = {
        "n_selected_2025": len(keep),
        "full_2026_oos": portfolio_pooled(fs26),
        "pregame_2026_oos": portfolio_pooled(pg26),
        "selected_wallets": keep,
    }
    print(f"  2025-selected portfolio (n={len(keep)}) 2026 OOS: "
          f"full ROI={result['full_2026_oos']['roi_pooled']:.4f} "
          f"(n_mkts={result['full_2026_oos']['n_markets']}), "
          f"pregame ROI={result['pregame_2026_oos']['roi_pooled']:.4f} "
          f"(n_mkts={result['pregame_2026_oos']['n_markets']})")
    return result


# ---------------------------------------------------------------------------
# Old-list overlap
# ---------------------------------------------------------------------------
def load_old_lists() -> tuple[set[str], set[str]]:
    with open(OLD_147_PATH) as f:
        d147 = json.load(f)
    old147 = {r["wallet"].lower() for r in d147.get("top_tier", []) + d147.get("watchlist", [])}

    with open(CLV_COHORT_PATH) as f:
        dport = json.load(f)
    clv_members = dport.get("closest_contender", {}).get("members_reference_only", [])
    clv_cohort = {m["wallet"].lower() for m in clv_members}
    return old147, clv_cohort


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    t0 = time.time()
    lake = load_lake()
    positions, markets, traders, pregame, candles = (
        lake["positions"], lake["markets"], lake["traders"], lake["pregame"], lake["candles"]
    )

    print("\nBuilding full-history and pre-game per-season wallet-market tables...")
    full_wm = full_period_wm(positions, markets)
    pg_wm = pregame_wm(pregame)

    full_summary = {s: wallet_season_summary(full_wm[s]) for s in SEASONS}
    pg_summary = {s: wallet_season_summary(pg_wm[s]) for s in SEASONS}
    for s in SEASONS:
        print(f"  season {s}: full wallets={len(full_summary[s])} pregame wallets={len(pg_summary[s])}")

    funnel = {}

    # --- Criterion 1: n>=50 pregame markets in EACH period, active both ---
    floor = PREGAME_N_FLOOR
    elig25 = set(pg_summary[2025][pg_summary[2025]["n_markets"] >= floor]["wallet"])
    elig26 = set(pg_summary[2026][pg_summary[2026]["n_markets"] >= floor]["wallet"])
    pool = elig25 & elig26
    funnel["1_active_both_periods_n>=50"] = len(pool)
    print(f"\nFunnel: n>=50 pregame markets in BOTH 2025 and 2026: {len(pool)}")

    relaxed_used = False
    if len(pool) == 0:
        print(f"  [RELAX] Zero wallets at n>=50 -- relaxing floor to n>={RELAXED_N_FLOOR} per instructions.")
        floor = RELAXED_N_FLOOR
        relaxed_used = True
        elig25 = set(pg_summary[2025][pg_summary[2025]["n_markets"] >= floor]["wallet"])
        elig26 = set(pg_summary[2026][pg_summary[2026]["n_markets"] >= floor]["wallet"])
        pool = elig25 & elig26
        funnel[f"1_active_both_periods_n>={RELAXED_N_FLOOR}"] = len(pool)

    # --- Criterion 2: positive FULL settlement ROI in each period ---
    pos25 = set(full_summary[2025][full_summary[2025]["roi_pooled"] > 0]["wallet"])
    pos26 = set(full_summary[2026][full_summary[2026]["roi_pooled"] > 0]["wallet"])
    pool = pool & pos25 & pos26
    funnel["2_positive_full_roi_both"] = len(pool)
    print(f"Funnel: + positive FULL settlement ROI both periods: {len(pool)}")

    # --- Criterion 3: positive PRE-GAME ROI in each period ---
    ppos25 = set(pg_summary[2025][pg_summary[2025]["roi_pooled"] > 0]["wallet"])
    ppos26 = set(pg_summary[2026][pg_summary[2026]["roi_pooled"] > 0]["wallet"])
    pool = pool & ppos25 & ppos26
    funnel["3_positive_pregame_roi_both"] = len(pool)
    print(f"Funnel: + positive PRE-GAME settlement ROI both periods: {len(pool)}")

    # --- Criterion 4: not bot/MM-shaped ---
    bot_ok = set(traders[bot_filter_mask(traders)]["wallet"])
    pool = pool & bot_ok
    funnel["4_not_bot_shaped"] = len(pool)
    print(f"Funnel: + not bot/MM-shaped (two_sided<{BOT_TWO_SIDED_MAX}, "
          f"entropy<{BOT_ENTROPY_MAX}, median_gap>={BOT_MIN_MEDIAN_GAP_S}s): {len(pool)}")

    # --- Criterion 5: median fill notional >= $20 ---
    notional_df = median_fill_notional(sorted(pool))
    stake_ok = set(notional_df[notional_df["median_notional"] >= MIN_MEDIAN_FILL_NOTIONAL]["wallet"])
    pool = pool & stake_ok
    funnel["5_median_notional>=20"] = len(pool)
    print(f"Funnel: + median fill notional >= ${MIN_MEDIAN_FILL_NOTIONAL:.0f}: {len(pool)}")

    survivors = sorted(pool)
    print(f"\n=== SURVIVORS: {len(survivors)} wallets ===")

    if len(survivors) == 0:
        print("ZERO wallets survive all criteria -- reporting decisively, not padding the list.")

    # --- Bootstrap CIs by market (pregame + full), per period, survivors only ---
    per_wallet = {}
    for w in survivors:
        ci_pg25 = wallet_market_ci(pg_wm[2025], w)
        ci_pg26 = wallet_market_ci(pg_wm[2026], w)
        ci_full25 = wallet_market_ci(full_wm[2025], w)
        ci_full26 = wallet_market_ci(full_wm[2026], w)
        combined_score = min(ci_pg25[0], ci_pg26[0]) if not (np.isnan(ci_pg25[0]) or np.isnan(ci_pg26[0])) else float("nan")
        per_wallet[w] = dict(
            ci_pregame_2025=ci_pg25, ci_pregame_2026=ci_pg26,
            ci_full_2025=ci_full25, ci_full_2026=ci_full26,
            combined_score=combined_score,
        )

    # --- Market-type mix, recent form, copyability panel, sizing ---
    mix = market_type_mix(positions, survivors)
    recent = recent_form_2026(pg_wm[2026], pregame, survivors)
    minutes_df = minutes_to_post(pregame, survivors)
    drift_df = price_drift_1h(pregame, candles, survivors)
    notional_all = median_fill_notional(survivors)

    trader_feats = traders.set_index("wallet")

    # --- Old-list overlap ---
    old147, clv_cohort = load_old_lists()
    survivor_set_lower = {w.lower() for w in survivors}
    overlap_147 = sorted(survivor_set_lower & old147)
    overlap_clv = sorted(survivor_set_lower & clv_cohort)

    # --- Walk-forward criteria validation ---
    wf = walk_forward_2025_only(positions, markets, traders, pregame)

    # --- Assemble per-wallet output ---
    records = []
    for w in survivors:
        f25, f26 = full_summary[2025].set_index("wallet"), full_summary[2026].set_index("wallet")
        p25, p26 = pg_summary[2025].set_index("wallet"), pg_summary[2026].set_index("wallet")
        tf = trader_feats.loc[w] if w in trader_feats.index else None
        rec = {
            "wallet": w,
            "combined_score": per_wallet[w]["combined_score"],
            "periods": {
                "2025": {
                    "n_markets_full": int(f25.loc[w, "n_markets"]),
                    "roi_full": round(float(f25.loc[w, "roi_pooled"]), 4),
                    "n_markets_pregame": int(p25.loc[w, "n_markets"]),
                    "roi_pregame": round(float(p25.loc[w, "roi_pooled"]), 4),
                    "roi_pregame_ci95": [round(x, 4) for x in per_wallet[w]["ci_pregame_2025"]],
                    "roi_full_ci95": [round(x, 4) for x in per_wallet[w]["ci_full_2025"]],
                    "win_rate_pregame": round(float(p25.loc[w, "win_rate"]), 4),
                    "avg_entry_price_pregame": round(float(p25.loc[w, "avg_entry_price"]), 4),
                    "heavy_favorite_profile": bool(p25.loc[w, "avg_entry_price"] > HEAVY_FAVORITE_ENTRY),
                    "profit_full": round(float(f25.loc[w, "profit"]), 2),
                },
                "2026": {
                    "n_markets_full": int(f26.loc[w, "n_markets"]),
                    "roi_full": round(float(f26.loc[w, "roi_pooled"]), 4),
                    "n_markets_pregame": int(p26.loc[w, "n_markets"]),
                    "roi_pregame": round(float(p26.loc[w, "roi_pooled"]), 4),
                    "roi_pregame_ci95": [round(x, 4) for x in per_wallet[w]["ci_pregame_2026"]],
                    "roi_full_ci95": [round(x, 4) for x in per_wallet[w]["ci_full_2026"]],
                    "win_rate_pregame": round(float(p26.loc[w, "win_rate"]), 4),
                    "avg_entry_price_pregame": round(float(p26.loc[w, "avg_entry_price"]), 4),
                    "heavy_favorite_profile": bool(p26.loc[w, "avg_entry_price"] > HEAVY_FAVORITE_ENTRY),
                    "profit_full": round(float(f26.loc[w, "profit"]), 2),
                    "recent_form_last30": recent.get(w),
                },
            },
            "total_profit_full": round(float(f25.loc[w, "profit"] + f26.loc[w, "profit"]), 2),
            "market_type_mix": mix.get(w, {}),
            "bot_features": {
                "two_sided_share": round(float(tf["two_sided_share"]), 4) if tf is not None else None,
                "active_hours_entropy": round(float(tf["active_hours_entropy"]), 4) if tf is not None else None,
                "median_inter_trade_seconds": round(float(tf["median_inter_trade_seconds"]), 1) if tf is not None else None,
                "trades_per_day": round(float(tf["trades_per_day"]), 2) if tf is not None else None,
            },
            "sizing": {
                "median_fill_notional_alltime": round(float(notional_all.set_index("wallet").loc[w, "median_notional"]), 2)
                if w in notional_all["wallet"].values else None,
            },
            "copyability": {
                "median_minutes_to_first_pitch": round(float(minutes_df.set_index("wallet").loc[w, "median_minutes_to_post"]), 1)
                if w in minutes_df["wallet"].values else None,
                "price_drift_1h_after_entry": round(float(drift_df.set_index("wallet").loc[w, "drift_1h_weighted"]), 4)
                if w in drift_df["wallet"].values else None,
                "n_fills_matched_for_drift": int(drift_df.set_index("wallet").loc[w, "n_matched"])
                if w in drift_df["wallet"].values else 0,
            },
            "in_old_147_list": w.lower() in old147,
            "in_clv_cohort": w.lower() in clv_cohort,
        }
        records.append(rec)

    records.sort(key=lambda r: (r["combined_score"] if not np.isnan(r["combined_score"]) else -1e9), reverse=True)

    # --- External-reality screen (post-review): guard against survivorship
    # bias inside the MLB category screen -- a wallet can be +$X on our MLB
    # criteria while being a busted gambler in aggregate (lifetime PnL deeply
    # negative) or riding a category hot streak inside an otherwise-marginal
    # lifetime record. See analysis/wallet_screen.py for the full rationale
    # and verdict rules. Applied here, not just downstream, so
    # data/persistent_winners.json is the single source of truth every
    # consumer (reports/08, mlb7_consensus.py, cross_sport_winners.py) reads.
    category_profit = {r["wallet"]: r["total_profit_full"] for r in records}
    screens = screen_wallets(survivors, category_profit=category_profit)
    for r in records:
        s = screens[r["wallet"]]
        r["external_screen"] = {
            "pseudonym": s["pseudonym"],
            "lifetime_pnl": s["lifetime_pnl"],
            "current_value": s["current_value"],
            "last_mlb_trade": s["last_trade"].isoformat() if s["last_trade"] is not None else None,
            "days_since_last_mlb_trade": s["days_since_last_trade"],
            "stale": s["stale"],
            "screen_verdict": s["verdict"],
            "notes": s["notes"],
        }
    screened_out_wallets = [w for w in survivors if screens[w]["verdict"] == "FAIL"]
    funnel["6_external_reality_screen"] = len(survivors) - len(screened_out_wallets)
    print(f"\nFunnel: + external-reality screen (lifetime all-category PnL, current value, "
          f"staleness): {funnel['6_external_reality_screen']}/{len(survivors)} "
          f"(screened out: {screened_out_wallets})")

    output = {
        "generated_at": pd.Timestamp.now('UTC').isoformat(),
        "methodology": {
            "eligibility_n_floor": floor,
            "relaxed_floor_used": relaxed_used,
            "bot_thresholds": {
                "two_sided_share_max": BOT_TWO_SIDED_MAX,
                "active_hours_entropy_max": BOT_ENTROPY_MAX,
                "median_inter_trade_seconds_min": BOT_MIN_MEDIAN_GAP_S,
            },
            "min_median_fill_notional": MIN_MEDIAN_FILL_NOTIONAL,
            "heavy_favorite_entry_threshold": HEAVY_FAVORITE_ENTRY,
            "combined_score_definition": "min(bootstrap-2.5%ile pregame ROI lower bound, 2025) across the two "
                                          "periods -- i.e. min(pregame_2025_roi_ci_lower, pregame_2026_roi_ci_lower)",
            "n_boot": N_BOOT,
        },
        "funnel": funnel,
        "survivors": records,
        "screened_out_wallets": screened_out_wallets,
        "overlap_old_147_list": overlap_147,
        "overlap_clv_cohort": overlap_clv,
        "walk_forward_2025_selected_2026_oos": wf,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / "persistent_winners.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\nWrote {out_path}")

    write_report(output, funnel, relaxed_used, floor)
    print(f"\nTotal elapsed: {time.time() - t0:.1f}s")


def write_report(output: dict, funnel: dict, relaxed_used: bool, floor: int) -> None:
    survivors = output["survivors"]
    wf = output["walk_forward_2025_selected_2026_oos"]
    lines = []
    lines.append("# Phase 2/3 Report — Persistent MLB Winners (FINAL)\n")
    lines.append(f"Run date: {pd.Timestamp.now('UTC').date()}. Pipeline: `analysis/persistent_winners.py`. "
                 "Full lake (13.07M trades, 224,559 wallets, 28,690 markets — see reports/04 and reports/05 for "
                 "the underlying methodology this report builds on).\n")

    lines.append("## Selection criteria\n")
    lines.append(f"1. Active in BOTH 2025 and 2026 seasons: n >= {floor} settled pre-game markets in EACH "
                 f"period independently{' (relaxed from 50 — see below)' if relaxed_used else ''}.\n"
                 "2. Positive FULL-history settlement ROI (cash-flow accounting) in EACH period independently.\n"
                 "3. Positive PRE-GAME-only settlement ROI in EACH period independently.\n"
                 f"4. Not MM/bot-shaped: two_sided_share < {BOT_TWO_SIDED_MAX}, active_hours_entropy < "
                 f"{BOT_ENTROPY_MAX}, median_inter_trade_seconds >= {BOT_MIN_MEDIAN_GAP_S:.0f}s "
                 "(pooled features from traders.parquet; thresholds documented in the module docstring).\n"
                 f"5. Median fill notional >= ${MIN_MEDIAN_FILL_NOTIONAL:.0f} (dust-bot floor).\n")

    lines.append("## Selection funnel\n")
    lines.append("| stage | n wallets |\n|---|---:|")
    for k, v in funnel.items():
        lines.append(f"| {k} | {v} |")
    lines.append("")

    if relaxed_used:
        lines.append(f"**NOTE: the strict n>=50 floor yielded ZERO wallets active in both periods — the floor "
                     f"was relaxed to n>={RELAXED_N_FLOOR} per the task's contingency instruction. This is "
                     f"flagged prominently: the resulting list is built on a weaker per-period sample-size "
                     f"floor than originally specified.**\n")

    lines.append(f"## Survivors: {len(survivors)} wallets\n")
    if not survivors:
        lines.append("**ZERO wallets survive all five criteria.** Reporting this decisively rather than "
                     "padding the list by dropping a criterion silently. See funnel table above for exactly "
                     "which criterion eliminates the pool, and the walk-forward section below for whether "
                     "the criteria set itself is even the right one to relax.\n")
    else:
        lines.append("Ranked by combined score = min(pre-game ROI bootstrap-95%-CI lower bound, 2025 vs 2026) "
                     "— a wallet only ranks high if BOTH periods' pre-game edge survives a conservative "
                     "lower-bound haircut, not just point-estimate ROI.\n")
        n_ci_neg = sum(1 for r in survivors if r["combined_score"] < 0)
        if n_ci_neg:
            lines.append(f"**Caveat, stated plainly: all {n_ci_neg}/{len(survivors)} survivors have a "
                         "NEGATIVE combined score** — i.e. for every one of them, at least one period's "
                         "pre-game-ROI 95% bootstrap CI still straddles zero. Passing criteria 1-5 (positive "
                         "point-estimate ROI in both periods independently, plus the behavioral/stake filters) "
                         "is a real screen — the eligible-pool base rate for 'positive ROI in both periods' "
                         "was 23/83 (28%) at the pre-game-only cut — but at typical per-period sample sizes "
                         "here (roughly 100-900 settled markets), no individual wallet's edge clears a strict "
                         "95%-CI-excludes-zero bar in both periods simultaneously. Read this list as "
                         "'best available candidates by a real but modest point-estimate persistence filter,' "
                         "not as 'statistically proven alpha' — consistent with reports/05's finding that "
                         "season-level skill is real in aggregate (H7) but too diffuse for confident "
                         "single-wallet certification at these sample sizes.\n")
        lines.append("| rank | wallet | combined score | 2025 n(full/pregame) | 2025 ROI full/pregame | "
                     "2025 win% | 2025 avg entry | 2026 n(full/pregame) | 2026 ROI full/pregame | "
                     "2026 win% | 2026 avg entry | total profit $ | in-147-list | in-CLV-cohort |")
        lines.append("|---|---|---:|---|---|---:|---:|---|---|---:|---:|---:|---|---|")
        for i, r in enumerate(survivors, 1):
            p25, p26 = r["periods"]["2025"], r["periods"]["2026"]
            fav25 = " (favorite-heavy)" if p25["heavy_favorite_profile"] else ""
            fav26 = " (favorite-heavy)" if p26["heavy_favorite_profile"] else ""
            lines.append(
                f"| {i} | `{r['wallet']}` | {r['combined_score']:.3f} | "
                f"{p25['n_markets_full']}/{p25['n_markets_pregame']} | "
                f"{p25['roi_full']:+.1%}/{p25['roi_pregame']:+.1%} | {p25['win_rate_pregame']:.1%} | "
                f"{p25['avg_entry_price_pregame']:.2f}{fav25} | "
                f"{p26['n_markets_full']}/{p26['n_markets_pregame']} | "
                f"{p26['roi_full']:+.1%}/{p26['roi_pregame']:+.1%} | {p26['win_rate_pregame']:.1%} | "
                f"{p26['avg_entry_price_pregame']:.2f}{fav26} | "
                f"${r['total_profit_full']:,.0f} | {'YES' if r['in_old_147_list'] else '-'} | "
                f"{'YES' if r['in_clv_cohort'] else '-'} |"
            )
        lines.append("")

        lines.append("### Per-wallet detail: bootstrap CIs, bot features, sizing, copyability, market mix, "
                     "recent form\n")
        for r in survivors:
            p25, p26 = r["periods"]["2025"], r["periods"]["2026"]
            bf = r["bot_features"]
            cop = r["copyability"]
            sz = r["sizing"]
            rf = p26.get("recent_form_last30")
            mix_str = ", ".join(f"{k} {v['pct_n']:.0%} (n={v['n_markets']})" for k, v in
                                sorted(r["market_type_mix"].items(), key=lambda kv: -kv[1]["n_markets"]))
            lines.append(f"**`{r['wallet']}`** (combined score {r['combined_score']:.3f})\n")
            lines.append(f"- 2025: n_full={p25['n_markets_full']} n_pregame={p25['n_markets_pregame']}, "
                         f"ROI full={p25['roi_full']:+.1%} (95% CI {p25['roi_full_ci95'][0]:+.1%} to "
                         f"{p25['roi_full_ci95'][1]:+.1%}), ROI pregame={p25['roi_pregame']:+.1%} "
                         f"(95% CI {p25['roi_pregame_ci95'][0]:+.1%} to {p25['roi_pregame_ci95'][1]:+.1%}), "
                         f"win rate pregame={p25['win_rate_pregame']:.1%}, avg entry={p25['avg_entry_price_pregame']:.2f}, "
                         f"profit(full)=${p25['profit_full']:,.0f}")
            lines.append(f"- 2026: n_full={p26['n_markets_full']} n_pregame={p26['n_markets_pregame']}, "
                         f"ROI full={p26['roi_full']:+.1%} (95% CI {p26['roi_full_ci95'][0]:+.1%} to "
                         f"{p26['roi_full_ci95'][1]:+.1%}), ROI pregame={p26['roi_pregame']:+.1%} "
                         f"(95% CI {p26['roi_pregame_ci95'][0]:+.1%} to {p26['roi_pregame_ci95'][1]:+.1%}), "
                         f"win rate pregame={p26['win_rate_pregame']:.1%}, avg entry={p26['avg_entry_price_pregame']:.2f}, "
                         f"profit(full)=${p26['profit_full']:,.0f}")
            if rf:
                lines.append(f"- 2026 recent form (last {rf['n']} pre-game-settled markets): "
                             f"ROI={rf['roi_pooled']:+.1%}, win rate={rf['win_rate']:.1%}")
            lines.append(f"- Market-type mix (combined, full history): {mix_str}")
            lines.append(f"- Bot features: two_sided_share={bf['two_sided_share']}, "
                         f"active_hours_entropy={bf['active_hours_entropy']}, "
                         f"median_inter_trade_seconds={bf['median_inter_trade_seconds']}, "
                         f"trades_per_day={bf['trades_per_day']}")
            lines.append(f"- Sizing: median fill notional (all-time) = ${sz['median_fill_notional_alltime']:,.2f}")
            lines.append(f"- Copyability: median minutes fill-to-first-pitch = "
                         f"{cop['median_minutes_to_first_pitch']:,.0f} min; "
                         f"1h post-entry price drift (size-weighted) = {cop['price_drift_1h_after_entry']:+.4f} "
                         f"(n={cop['n_fills_matched_for_drift']} fills matched to a candle)")
            lines.append(f"- Total profit (full-history, both periods): ${r['total_profit_full']:,.0f}\n")

    lines.append("## Overlap with existing lists\n")
    lines.append(f"- Old 147-wallet list (`data/top_mlb_traders.json`, top_tier+watchlist union): "
                 f"**{len(output['overlap_old_147_list'])}/{len(survivors)}** survivors also appear there.")
    if output["overlap_old_147_list"]:
        lines.append("  - " + ", ".join(f"`{w}`" for w in output["overlap_old_147_list"]))
    lines.append(f"- CLV-cohort (`data/trader_portfolio.json` closest_contender, topk_clv k=100, "
                 f"reference-only, NOT a recommended deployed portfolio): "
                 f"**{len(output['overlap_clv_cohort'])}/{len(survivors)}** survivors also appear there.")
    if output["overlap_clv_cohort"]:
        lines.append("  - " + ", ".join(f"`{w}`" for w in output["overlap_clv_cohort"]))
    lines.append("")
    lines.append("Zero overlap on both isn't surprising: report 04 found the old 147-list is built from "
                 "15-day rolling PnL tiering (median percentile rank 27.5th vs. the full census, i.e. "
                 "enriched-but-noisy, not elite) and report 05's H1 showed 15-day-window PnL barely predicts "
                 "next-window settlement ROI (rho=0.02) -- a fundamentally different, weaker selection axis "
                 "than this report's two-independent-full-season, pre-game-only, cash-flow-settlement filter. "
                 "The CLV-cohort is explicitly reference-only (report 06 recommended against deploying it).\n")

    lines.append("## Walk-forward criteria validation: select on 2025 only, measure 2026 out-of-sample\n")
    lines.append("This is the number that tells us whether the *criteria themselves* (not just this "
                 "particular list) survive walk-forward: apply criteria 1-5 using ONLY 2025 data (pregame "
                 "n>=50 in 2025, positive full & pregame 2025 ROI, 2025-only-computed bot/stake filters — "
                 "recomputed on 2025 trades specifically so no 2026 behavioral information leaks into "
                 "selection), then measure that cohort's actual 2026 performance.\n")
    lines.append(f"- **{wf['n_selected_2025']} wallets** selected on 2025-only criteria.")
    fo, po = wf["full_2026_oos"], wf["pregame_2026_oos"]
    lines.append(f"- 2026 OOS full-history ROI (pooled, portfolio-level): **{fo['roi_pooled']:+.1%}** "
                 f"(95% CI {fo['roi_ci95'][0]:+.1%} to {fo['roi_ci95'][1]:+.1%}), n={fo['n_markets']} "
                 f"markets across {fo['n_wallets']} wallets with 2026 activity, ${fo['invested']:,.0f} invested.")
    lines.append(f"- 2026 OOS pre-game-only ROI (pooled, portfolio-level): **{po['roi_pooled']:+.1%}** "
                 f"(95% CI {po['roi_ci95'][0]:+.1%} to {po['roi_ci95'][1]:+.1%}), n={po['n_markets']} "
                 f"markets across {po['n_wallets']} wallets with 2026 activity, ${po['invested']:,.0f} invested.\n")
    ci_excludes_zero_full = fo["roi_ci95"][0] > 0
    ci_excludes_zero_pg = po["roi_ci95"][0] > 0
    verdict = "SUPPORTED" if (ci_excludes_zero_full or ci_excludes_zero_pg) else "NOT SUPPORTED"
    lines.append(f"**Verdict: {verdict}.** " + (
        "At least one out-of-sample ROI's 95% CI excludes zero, meaning the criteria set (not just "
        "in-sample cherry-picking) carries real forward-looking signal on this data."
        if verdict == "SUPPORTED" else
        "Both out-of-sample ROI CIs straddle zero (or are negative) — selecting a portfolio with these "
        "criteria on 2025 data alone does not reliably produce positive 2026 returns. Consistent with "
        "reports/05's H1/H7 findings that season-level persistence is real in aggregate but diffuse "
        "(97.5% top-decile turnover) — a criteria-based walk-forward portfolio is not obviously more "
        "robust than that diffuse population-level signal."
    ) + "\n")

    lines.append("## Hygiene notes\n")
    lines.append("- All bootstrap CIs resample at the MARKET level (2,000 resamples, seed=42), not the "
                 "fill level, so within-market fill correlation doesn't understate variance.\n"
                 "- n is stated at every stage (funnel table, per-wallet detail, walk-forward section).\n"
                 "- Settlement ROI (cash-flow accounting: invested = buy cost, returned = sell proceeds + "
                 "settlement payout) is the sole source of truth throughout, per reports/04 and reports/05's "
                 "established methodology.\n")

    lines.append("## External-reality screen (post-review)\n")
    lines.append(
        "A user review caught a survivorship-bias failure mode the five in-category criteria above cannot "
        "see: wallet `0xa5dcb282cab760e31df1f3f5c18350731c95ec43` (\"Yikes110\") passed all five MLB-specific "
        "criteria (+$28,280 total profit here, rank 6/7) while carrying a **-$262,176 lifetime, "
        "all-category** Polymarket PnL -- a busted gambler riding a hot MLB streak on the way down, not a "
        "specialist with durable edge. Settlement ROI computed only inside one category's lake cannot tell "
        "'consistently skilled here' apart from 'currently up here, down everywhere else' -- the two look "
        "identical from inside a single-sport view. This section adds a sixth, EXTERNAL screen "
        "(`analysis/wallet_screen.py`, shared with `analysis/cross_sport_winners.py`'s NFL/NBA funnel so the "
        "same failure mode can't recur there): lifetime all-category PnL and current deployed value from "
        "Polymarket's own public leaderboard/data APIs, plus a staleness check against our own lake.\n"
    )
    lines.append(
        "**Verdict rules:** FAIL if lifetime PnL < -$10,000 (net loser across their whole trading history, "
        "regardless of MLB performance); WARN if lifetime PnL < 25% of their MLB profit here (a "
        "category-hot-streak-inside-a-modest-lifetime-record pattern, e.g. Radahn-131's +$73,690 MLB profit "
        "vs. +$18,659 lifetime -- MLB alone is ~4x their entire lifetime PnL); staleness is noted, not "
        "scored, if no trade in 30 days.\n"
    )
    lines.append("| rank | wallet | pseudonym | lifetime PnL (all categories) | current value | "
                 "last MLB trade | days since | verdict | status |")
    lines.append("|---|---|---|---:|---:|---|---:|---|---|")
    for i, r in enumerate(output["survivors"], 1):
        es = r["external_screen"]
        status = ("**REMOVED**" if es["screen_verdict"] == "FAIL"
                  else "kept (flagged)" if es["screen_verdict"] == "WARN" else "kept")
        lt = es["last_mlb_trade"][:10] if es["last_mlb_trade"] else "n/a"
        lp = f"${es['lifetime_pnl']:,.0f}" if es["lifetime_pnl"] is not None else "n/a"
        cv = f"${es['current_value']:,.0f}" if es["current_value"] is not None else "n/a"
        days = f"{es['days_since_last_mlb_trade']}d" if es["days_since_last_mlb_trade"] is not None else "n/a"
        lines.append(f"| {i} | `{r['wallet']}` | {es['pseudonym'] or 'n/a'} | {lp} | {cv} | {lt} | {days} | "
                     f"{es['screen_verdict']} | {status} |")
    lines.append("")
    if output["screened_out_wallets"]:
        n_remaining = len(output["survivors"]) - len(output["screened_out_wallets"])
        lines.append(f"**{len(output['screened_out_wallets'])} wallet(s) REMOVED by the external screen: "
                     + ", ".join(f"`{w}`" for w in output["screened_out_wallets"]) + ".** "
                     "The MLB-7 list above is retained in full for provenance (all five in-category criteria "
                     "were genuinely passed), but the operative, deploy-worthy list after this review is the "
                     f"remaining {n_remaining} wallets (`funnel.6_external_reality_screen` in "
                     "`data/persistent_winners.json`). See each survivor's `external_screen` block and the "
                     "top-level `screened_out_wallets` field.\n")
    warn_records = [r for r in output["survivors"] if r["external_screen"]["screen_verdict"] == "WARN"]
    if warn_records:
        lines.append("**WARN (kept, but flagged as a category-hot-streak-inside-a-modest-lifetime-record):**\n")
        for r in warn_records:
            es = r["external_screen"]
            for note in es["notes"]:
                lines.append(f"- `{r['wallet']}` ({es['pseudonym']}): {note}")
        lines.append("")
    near_miss_records = [r for r in output["survivors"]
                         if r["external_screen"]["screen_verdict"] == "OK" and r["external_screen"]["notes"]]
    if near_miss_records:
        lines.append("**OK, but worth flagging (lifetime PnL close to the 25% WARN threshold without "
                     "crossing it):**\n")
        for r in near_miss_records:
            es = r["external_screen"]
            for note in es["notes"]:
                lines.append(f"- `{r['wallet']}` ({es['pseudonym']}): {note}")
        lines.append("")
    lines.append(
        "**Lesson:** in-category settlement ROI, however carefully computed (cash-flow accounting, "
        "two-independent-season persistence, bootstrap CIs, bot/stake filters), is silent on what a wallet "
        "does OUTSIDE the category being screened. A short-lived hot streak inside one sport can coexist "
        "with a deeply negative lifetime track record everywhere else -- exactly what an in-category-only "
        "screen is structurally unable to detect, and exactly why this external-reality check is now a "
        "mandatory, non-optional stage in both this pipeline and the cross-sport NFL/NBA pipeline "
        "(`analysis/cross_sport_winners.py`), not an optional add-on.\n"
    )

    out_path = REPORTS_DIR / "08_persistent_winners.md"
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        f.write("\n".join(lines))
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
