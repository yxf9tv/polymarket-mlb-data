#!/usr/bin/env python3
"""Phase 1 hypothesis tests + census stats for reports/04_trader_census.md and
reports/05_skill_persistence.md, run against the FULL lake (30,289-market
target universe, 13,073,293 trades, 224,559 wallets, 3,297,149 positions).

Reuses trader_metrics.py's first-pitch join / token-winner map helpers
directly rather than duplicating that logic. Read-only w.r.t. every lake
file; writes one summary JSON (data/lake/hypothesis_results.json) with every
number this script prints, so the reports can cite exact figures.

No scipy in .venv -- Spearman rank correlation is implemented as Pearson
correlation of ranks (pandas .rank() + .corr(), both scipy-free); bootstrap
CIs are plain numpy resampling.

Usage:
    .venv/bin/python3 analysis/hypothesis_tests.py
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
LAKE_DIR = BASE / "data" / "lake"
sys.path.insert(0, str(BASE / "analysis"))
import trader_metrics as tm  # noqa: E402

RESULTS: dict = {}


def _json_default(o):
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, (np.bool_,)):
        return bool(o)
    if isinstance(o, pd.Timestamp):
        return o.isoformat()
    return str(o)


# ---------------------------------------------------------------------------
# Stats helpers (scipy-free)
# ---------------------------------------------------------------------------
def spearman(x, y) -> tuple[float, int]:
    x = pd.Series(x).astype(float).reset_index(drop=True)
    y = pd.Series(y).astype(float).reset_index(drop=True)
    mask = x.notna() & y.notna()
    n = int(mask.sum())
    if n < 3:
        return float("nan"), n
    rx, ry = x[mask].rank(), y[mask].rank()
    return float(rx.corr(ry)), n


def bootstrap_ci_corr(x, y, n_boot: int = 2000, seed: int = 0) -> tuple[float, float]:
    x = pd.Series(x).astype(float).reset_index(drop=True)
    y = pd.Series(y).astype(float).reset_index(drop=True)
    mask = x.notna() & y.notna()
    x, y = x[mask].to_numpy(), y[mask].to_numpy()
    n = len(x)
    if n < 10:
        return float("nan"), float("nan")
    rng = np.random.default_rng(seed)
    stats = np.empty(n_boot)
    for i in range(n_boot):
        idx = rng.integers(0, n, n)
        xs, ys = pd.Series(x[idx]), pd.Series(y[idx])
        stats[i] = xs.rank().corr(ys.rank())
    return float(np.nanpercentile(stats, 2.5)), float(np.nanpercentile(stats, 97.5))


def bootstrap_ci_mean(values, n_boot: int = 2000, seed: int = 0) -> tuple[float, float]:
    v = pd.Series(values).dropna().to_numpy()
    n = len(v)
    if n < 10:
        return float("nan"), float("nan")
    rng = np.random.default_rng(seed)
    means = np.empty(n_boot)
    for i in range(n_boot):
        idx = rng.integers(0, n, n)
        means[i] = v[idx].mean()
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def pooled_roi(invested, returned) -> float:
    inv = np.nansum(invested)
    ret = np.nansum(returned)
    return float((ret - inv) / inv) if inv > 0 else float("nan")


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------
def load_all():
    print("Loading lake tables...")
    positions = pd.read_parquet(LAKE_DIR / "positions.parquet")
    traders = pd.read_parquet(LAKE_DIR / "traders.parquet")
    markets = pd.read_parquet(LAKE_DIR / "markets.parquet")
    schedule = pd.read_parquet(LAKE_DIR / "schedule.parquet")
    candles = pd.read_parquet(LAKE_DIR / "candles.parquet")
    trades, _, _, completed = tm.load_data()
    print(f"  positions={len(positions)} traders={len(traders)} markets={len(markets)} "
          f"schedule={len(schedule)} candles={len(candles)} trades={len(trades)}")
    return positions, traders, markets, schedule, candles, trades


def build_wallet_market_table(positions: pd.DataFrame, markets: pd.DataFrame,
                               first_pitch: dict) -> pd.DataFrame:
    """Roll token-level positions up to (wallet, condition_id), attach a
    market_date (first-pitch if joinable, else Gamma end_date, else
    start_date, else the position's own first fill time) and season."""
    pos = positions.rename(columns={"wallet": "proxy_wallet"})
    wm = pos.groupby(["proxy_wallet", "condition_id"], as_index=False).agg(
        invested=("invested", "sum"), returned=("returned", "sum"),
        n_fills=("n_fills", "sum"), market_type=("market_type", "first"),
        first_ts=("first_ts", "min"), last_ts=("last_ts", "max"),
    )
    wm["roi"] = np.where(wm["invested"] > 0, (wm["returned"] - wm["invested"]) / wm["invested"], np.nan)
    wm["win"] = wm["returned"] > wm["invested"]

    mkt = markets.set_index("condition_id")
    fp_series = wm["condition_id"].map(first_pitch)
    end_date = pd.to_datetime(wm["condition_id"].map(mkt["end_date"]), utc=True, errors="coerce")
    start_date = pd.to_datetime(wm["condition_id"].map(mkt["start_date"]), utc=True, errors="coerce")
    wm["market_date"] = fp_series.fillna(end_date).fillna(start_date).fillna(wm["first_ts"])
    wm["season"] = wm["condition_id"].map(mkt["season"])
    return wm


# ---------------------------------------------------------------------------
# Task 6: zero-sum sanity anchor
# ---------------------------------------------------------------------------
def sanity_zero_sum(positions: pd.DataFrame, markets: pd.DataFrame, n: int = 20, seed: int = 42) -> pd.DataFrame:
    resolved_with_trades = positions["condition_id"].unique()
    rng = np.random.default_rng(seed)
    sample = rng.choice(resolved_with_trades, size=min(n, len(resolved_with_trades)), replace=False)
    mkt_idx = markets.set_index("condition_id")
    rows = []
    for cid in sample:
        sub = positions[positions["condition_id"] == cid]
        total_invested = sub["invested"].sum()
        net = (sub["returned"] - sub["invested"]).sum()
        q = mkt_idx.loc[cid, "question"] if cid in mkt_idx.index else "?"
        gv = mkt_idx.loc[cid, "volume"] if cid in mkt_idx.index else np.nan
        mtype = mkt_idx.loc[cid, "market_type"] if cid in mkt_idx.index else "?"
        rows.append({
            "condition_id": cid, "question": q, "market_type": mtype,
            "n_wallets": sub["wallet"].nunique(), "total_invested": total_invested,
            "gamma_volume": gv, "net_pnl_sum": net,
            "net_pnl_pct_of_invested": (net / total_invested) if total_invested else np.nan,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# CLV: dollar-weighted pre-game entry price vs last pre-game candle close
# ---------------------------------------------------------------------------
def build_closing_price_map(candles: pd.DataFrame, markets: pd.DataFrame, first_pitch: dict) -> pd.Series:
    tok_to_cid = {}
    for r in markets.itertuples(index=False):
        if isinstance(r.token_ids, str) and r.token_ids:
            for tid in r.token_ids.split(","):
                tok_to_cid[tid] = r.condition_id
    c = candles.copy()
    c["condition_id_fp"] = c["token_id"].map(tok_to_cid)
    c["cutoff"] = c["condition_id_fp"].map(first_pitch)
    c = c.dropna(subset=["cutoff"])
    c = c[c["ts"] <= c["cutoff"]]
    c = c.sort_values("ts")
    closing = c.groupby("token_id").tail(1).set_index("token_id")["price"]
    return closing


def compute_wallet_window_clv(trades: pd.DataFrame) -> pd.DataFrame:
    """Per (wallet, window): dollar-weighted CLV over pre-game BUY fills.
    `trades` must already have a `window` column (assigned via a merge, not
    positional/index alignment -- trades frames here have non-contiguous
    indices after upstream filtering). CLV per fill = closing_price(token) -
    fill_price (positive = bought before price moved toward its pre-game
    close -- a favorable entry)."""
    t = trades[trades["side"] == "BUY"].copy()
    t = t.dropna(subset=["closing"])
    t["cost"] = t["size"] * t["price"]
    t["clv_dollars"] = t["cost"] * (t["closing"] - t["price"])
    g = t.groupby(["proxy_wallet", "window"], as_index=False).agg(
        clv_cost=("cost", "sum"), clv_num=("clv_dollars", "sum"),
    )
    g["clv"] = np.where(g["clv_cost"] > 0, g["clv_num"] / g["clv_cost"], np.nan)
    return g[["proxy_wallet", "window", "clv"]]


# ---------------------------------------------------------------------------
# Census stats (report 04)
# ---------------------------------------------------------------------------
def census_stats(traders: pd.DataFrame, wm: pd.DataFrame) -> dict:
    out = {}
    out["census_size"] = int(len(traders))
    out["total_dollar_volume"] = float(traders["gross_volume"].sum())

    def deciles(s: pd.Series) -> dict:
        qs = [0, .1, .2, .3, .4, .5, .6, .7, .8, .9, 1.0]
        return {f"p{int(q*100)}": float(s.quantile(q)) for q in qs}

    has_settled = traders[traders["n_markets"] >= 1]
    out["roi_pooled_deciles_all"] = deciles(has_settled["roi_pooled"])
    out["roi_pooled_deciles_all_n"] = int(len(has_settled))

    ge5 = traders[traders["n_markets"] >= 5]
    out["roi_pooled_deciles_ge5"] = deciles(ge5["roi_pooled"])
    out["roi_pooled_deciles_ge5_n"] = int(len(ge5))

    ge30 = traders[traders["n_markets"] >= 30]
    out["roi_pooled_deciles_ge30"] = deciles(ge30["roi_pooled"])
    out["roi_pooled_deciles_ge30_n"] = int(len(ge30))

    # n_markets bucket table
    bins = [0, 1, 4, 9, 19, 49, np.inf]
    labels = ["1", "2-4", "5-9", "10-19", "20-49", "50+"]
    traders = traders.copy()
    traders["nb"] = pd.cut(traders["n_markets"], bins=bins, labels=labels)
    bucket_tbl = traders.groupby("nb", observed=True).agg(
        n_wallets=("wallet", "count"), mean_roi=("roi_pooled", "mean"),
        median_roi=("roi_pooled", "median"), mean_invested=("invested", "mean"),
        win_rate=("win_rate", "mean"),
    ).reset_index()
    out["n_markets_bucket_table"] = bucket_tbl.to_dict("records")

    # pregame-only ROI, n>=20
    pg = traders[traders["pregame_n_markets"] >= 20]
    out["pregame_ge20_n"] = int(len(pg))
    out["pregame_roi_deciles_ge20"] = deciles(pg["pregame_roi_pooled"])
    top10 = pg.sort_values("pregame_roi_pooled", ascending=False).head(10)
    out["pregame_top10"] = top10[["wallet", "pregame_n_markets", "pregame_roi_pooled",
                                    "pregame_win_rate", "pregame_invested", "roi_pooled"]].to_dict("records")

    # market-type splits (population-level pooled roi per type, from wm)
    mt_tbl = wm[wm["invested"] > 0].groupby("market_type", observed=True).agg(
        n_positions=("roi", "count"), invested=("invested", "sum"), returned=("returned", "sum"),
        n_wallets=("proxy_wallet", "nunique"),
    ).reset_index()
    mt_tbl["pooled_roi"] = (mt_tbl["returned"] - mt_tbl["invested"]) / mt_tbl["invested"]
    out["market_type_table"] = mt_tbl.to_dict("records")

    # season splits (wallet-season pooled roi)
    ws = wm[wm["invested"] > 0].groupby(["proxy_wallet", "season"], as_index=False).agg(
        invested=("invested", "sum"), returned=("returned", "sum"), n_markets=("condition_id", "nunique"),
    )
    ws["roi"] = (ws["returned"] - ws["invested"]) / ws["invested"]
    season_tbl = ws.groupby("season", observed=True).agg(
        n_wallets=("proxy_wallet", "nunique"), mean_roi=("roi", "mean"), median_roi=("roi", "median"),
        total_invested=("invested", "sum"),
    ).reset_index()
    out["season_table"] = season_tbl.to_dict("records")

    # bot/MM feature distributions
    feat_cols = ["trades_per_day", "median_inter_trade_seconds", "active_hours_entropy", "two_sided_share"]
    feat_tbl = {}
    for c in feat_cols:
        s = traders[c].dropna()
        feat_tbl[c] = {"p10": float(s.quantile(.1)), "p50": float(s.quantile(.5)),
                        "p90": float(s.quantile(.9)), "p99": float(s.quantile(.99)), "n": int(len(s))}
    out["bot_feature_distributions"] = feat_tbl

    # heuristic bot flag: >=20 trades/day AND >=0.5 two-sided share AND >=50 trades
    bot_like = traders[(traders["trades_per_day"] >= 20) & (traders["two_sided_share"] >= 0.5)
                        & (traders["n_trades"] >= 50)]
    out["bot_like_count"] = int(len(bot_like))
    out["bot_like_pct"] = float(len(bot_like) / len(traders) * 100)

    return out


def current_list_overlap(traders: pd.DataFrame) -> dict:
    path = BASE / "data" / "top_mlb_traders.json"
    d = json.load(open(path))

    def _wallets(key: str) -> set[str]:
        return {(e["wallet"] if isinstance(e, dict) else e).lower() for e in d.get(key, [])}

    top_tier = _wallets("top_tier")
    watchlist = _wallets("watchlist")
    union = top_tier | watchlist

    t = traders.copy()
    t["wallet_lc"] = t["wallet"].str.lower()
    t = t.sort_values("roi_pooled", ascending=False).reset_index(drop=True)
    t["pct_rank"] = t.index / max(len(t) - 1, 1)  # 0 = best

    matched = t[t["wallet_lc"].isin(union)]
    out = {
        "list_top_tier_n": len(top_tier), "list_watchlist_n": len(watchlist),
        "list_union_n": len(union),
        "matched_in_census_n": int(len(matched)),
        "matched_pct": float(len(matched) / len(union) * 100) if union else float("nan"),
        "matched_median_roi": float(matched["roi_pooled"].median()) if len(matched) else float("nan"),
        "matched_median_pct_rank": float(matched["pct_rank"].median()) if len(matched) else float("nan"),
        "census_median_roi": float(t["roi_pooled"].median()),
        "n_matched_le_minus60pct": int((matched["roi_pooled"] <= -0.60).sum()),
    }
    return out


# ---------------------------------------------------------------------------
# H1: rolling 15d windows within 2025+2026, window(t) roi -> window(t+1) roi
# ---------------------------------------------------------------------------
def h1_rolling_persistence(wm: pd.DataFrame, min_per_window: int = 3) -> dict:
    sub = wm[(wm["season"].isin([2025, 2026])) & (wm["invested"] > 0) & wm["market_date"].notna()].copy()
    t0 = sub["market_date"].min()
    sub["window"] = ((sub["market_date"] - t0).dt.total_seconds() // (15 * 86400)).astype(int)

    win = sub.groupby(["proxy_wallet", "window"], as_index=False).agg(
        invested=("invested", "sum"), returned=("returned", "sum"), n=("condition_id", "nunique"),
    )
    win["roi"] = (win["returned"] - win["invested"]) / win["invested"]
    win = win[win["n"] >= min_per_window]

    win_next = win.copy()
    win_next["window"] = win_next["window"] - 1  # shift so join gives (t, t+1) pairs
    merged = win.merge(win_next, on=["proxy_wallet", "window"], suffixes=("_t", "_t1"))
    corr, n = spearman(merged["roi_t"], merged["roi_t1"])
    ci = bootstrap_ci_corr(merged["roi_t"], merged["roi_t1"])
    return {
        "n_windows_total": int(sub["window"].nunique()),
        "min_markets_per_window": min_per_window,
        "n_wallet_window_pairs": n,
        "spearman_rho": corr, "ci95": ci,
    }


# ---------------------------------------------------------------------------
# H2: selection metric (roi vs CLV vs win_rate vs volume) at t -> settlement ROI at t+1
# ---------------------------------------------------------------------------
def h2_selection_metrics(wm: pd.DataFrame, trades: pd.DataFrame, closing_price: pd.Series,
                          min_per_window: int = 3) -> dict:
    sub = wm[(wm["season"].isin([2025, 2026])) & (wm["invested"] > 0) & wm["market_date"].notna()].copy()
    t0 = sub["market_date"].min()
    sub["window"] = ((sub["market_date"] - t0).dt.total_seconds() // (15 * 86400)).astype(int)

    win = sub.groupby(["proxy_wallet", "window"], as_index=False).agg(
        invested=("invested", "sum"), returned=("returned", "sum"), n=("condition_id", "nunique"),
        win_rate=("win", "mean"),
    )
    win["roi"] = (win["returned"] - win["invested"]) / win["invested"]
    win = win[win["n"] >= min_per_window]

    # (wallet, condition_id) -> window lookup, from the same `sub` used above
    # so CLV windows line up exactly with the settlement-ROI windows.
    wc_window = sub[["proxy_wallet", "condition_id", "window"]].drop_duplicates(["proxy_wallet", "condition_id"])
    trades_pre = trades[trades["timing"] == "pre"].reset_index(drop=True)
    trades_pre = trades_pre.merge(wc_window, on=["proxy_wallet", "condition_id"], how="inner")
    trades_pre["closing"] = trades_pre["token_id"].map(closing_price)
    clv_tbl = compute_wallet_window_clv(trades_pre)

    win = win.merge(clv_tbl, on=["proxy_wallet", "window"], how="left")

    win_next = win[["proxy_wallet", "window", "roi"]].copy()
    win_next["window"] = win_next["window"] - 1
    win_next = win_next.rename(columns={"roi": "roi_next"})
    merged = win.merge(win_next, on=["proxy_wallet", "window"])

    results = {}
    for metric_name, col in [("past_settlement_roi", "roi"), ("clv", "clv"),
                              ("win_rate", "win_rate"), ("volume", "invested")]:
        corr, n = spearman(merged[col], merged["roi_next"])
        ci = bootstrap_ci_corr(merged[col], merged["roi_next"])
        results[metric_name] = {"spearman_rho": corr, "n": n, "ci95": ci}
    return {"n_wallet_window_pairs": int(len(merged)), "metrics": results}


# ---------------------------------------------------------------------------
# H3: totals markets -- fade the field vs skilled specialists
# ---------------------------------------------------------------------------
def h3_totals(wm: pd.DataFrame) -> dict:
    tot = wm[(wm["market_type"] == "total") & (wm["invested"] > 0) & wm["market_date"].notna()].copy()
    n_all = tot.groupby("proxy_wallet")["condition_id"].nunique()
    all_wallets_roi = tot.groupby("proxy_wallet").apply(
        lambda g: pooled_roi(g["invested"], g["returned"]), include_groups=False)
    pop_pooled_roi = pooled_roi(tot["invested"], tot["returned"])
    pop_mean_wallet_roi = float(all_wallets_roi.mean())

    qual = n_all[n_all >= 30].index
    sub = tot[tot["proxy_wallet"].isin(qual)].sort_values("market_date")

    def split_half(g):
        g = g.sort_values("market_date")
        k = len(g) // 2
        a, b = g.iloc[:k], g.iloc[k:]
        return pd.Series({
            "n_a": len(a), "n_b": len(b),
            "roi_a": pooled_roi(a["invested"], a["returned"]),
            "roi_b": pooled_roi(b["invested"], b["returned"]),
        })

    halves = sub.groupby("proxy_wallet").apply(split_half, include_groups=False).reset_index()
    halves = halves[(halves["n_a"] >= 10) & (halves["n_b"] >= 10)]
    corr, n = spearman(halves["roi_a"], halves["roi_b"])
    ci = bootstrap_ci_corr(halves["roi_a"], halves["roi_b"])

    top_decile_cut = halves["roi_a"].quantile(0.9) if len(halves) else np.nan
    specialists = halves[halves["roi_a"] >= top_decile_cut]
    specialists_mean_b = float(specialists["roi_b"].mean()) if len(specialists) else float("nan")
    specialists_ci = bootstrap_ci_mean(specialists["roi_b"]) if len(specialists) else (float("nan"), float("nan"))
    n_specialists_positive_b = int((specialists["roi_b"] > 0).sum())

    return {
        "population_n_wallets": int(tot["proxy_wallet"].nunique()),
        "population_pooled_roi": pop_pooled_roi,
        "population_mean_wallet_roi": pop_mean_wallet_roi,
        "n_ge30_wallets": int(len(qual)),
        "n_ge30_both_halves": int(len(halves)),
        "half_split_spearman_rho": corr, "half_split_n": n, "half_split_ci95": ci,
        "top_decile_cut_roi_a": float(top_decile_cut) if pd.notna(top_decile_cut) else float("nan"),
        "n_specialists_top_decile": int(len(specialists)),
        "specialists_mean_roi_b_oos": specialists_mean_b,
        "specialists_roi_b_ci95": specialists_ci,
        "n_specialists_positive_oos": n_specialists_positive_b,
    }


# ---------------------------------------------------------------------------
# H4: settlement ROI by hours-before-first-pitch bucket (pre-game BUY fills)
# ---------------------------------------------------------------------------
def h4_timing(trades: pd.DataFrame, markets: pd.DataFrame, first_pitch: dict) -> dict:
    token_winner = tm.build_token_winner_map(markets)
    tw = token_winner.set_index(["condition_id", "token_id"])["is_winner"]

    t = trades[(trades["side"] == "BUY")].copy()
    t["fp"] = t["condition_id"].map(first_pitch)
    t = t.dropna(subset=["fp"])
    t = t[t["timestamp"] < t["fp"]]
    t["hours_before"] = (t["fp"] - t["timestamp"]).dt.total_seconds() / 3600.0
    t["is_winner"] = t.set_index(["condition_id", "token_id"]).index.map(tw).to_numpy()
    t = t.dropna(subset=["is_winner"])
    t["invested"] = t["size"] * t["price"]
    t["returned"] = np.where(t["is_winner"], t["size"] * 1.0, 0.0)
    t["roi_fill"] = (t["returned"] - t["invested"]) / t["invested"]

    bins = [0, 1, 3, 6, 24, 72, np.inf]
    labels = ["0-1h", "1-3h", "3-6h", "6-24h", "24-72h", "72h+"]
    t["bucket"] = pd.cut(t["hours_before"], bins=bins, labels=labels)

    tbl = t.groupby("bucket", observed=True).agg(
        n_fills=("roi_fill", "count"), invested=("invested", "sum"), returned=("returned", "sum"),
        mean_roi_fill=("roi_fill", "mean"), win_rate=("is_winner", "mean"),
    ).reset_index()
    tbl["pooled_roi"] = (tbl["returned"] - tbl["invested"]) / tbl["invested"]
    ci_rows = []
    for lab in labels:
        vals = t.loc[t["bucket"] == lab, "roi_fill"]
        ci_rows.append({"bucket": lab, "ci95": bootstrap_ci_mean(vals, n_boot=1000)})
    return {"table": tbl.to_dict("records"), "ci": ci_rows, "n_total_fills": int(len(t))}


# ---------------------------------------------------------------------------
# H7: cross-season persistence (2025 -> 2026), n>=30 each season
# ---------------------------------------------------------------------------
def h7_cross_season(wm: pd.DataFrame) -> dict:
    ws = wm[wm["invested"] > 0].groupby(["proxy_wallet", "season"], as_index=False).agg(
        invested=("invested", "sum"), returned=("returned", "sum"), n_markets=("condition_id", "nunique"),
    )
    ws["roi"] = (ws["returned"] - ws["invested"]) / ws["invested"]
    p25 = ws[(ws["season"] == 2025) & (ws["n_markets"] >= 30)][["proxy_wallet", "roi", "n_markets"]]
    p26 = ws[(ws["season"] == 2026) & (ws["n_markets"] >= 30)][["proxy_wallet", "roi", "n_markets"]]
    merged = p25.merge(p26, on="proxy_wallet", suffixes=("_25", "_26"))
    corr, n = spearman(merged["roi_25"], merged["roi_26"])
    ci = bootstrap_ci_corr(merged["roi_25"], merged["roi_26"])

    # top-decile turnover
    if len(p25) >= 10:
        cut25 = p25["roi"].quantile(0.9)
        top25 = set(p25.loc[p25["roi"] >= cut25, "proxy_wallet"])
    else:
        top25 = set()
    if len(p26) >= 10:
        cut26 = p26["roi"].quantile(0.9)
        top26 = set(p26.loc[p26["roi"] >= cut26, "proxy_wallet"])
    else:
        top26 = set()
    overlap = top25 & top26
    turnover = 1 - (len(overlap) / len(top25)) if top25 else float("nan")

    return {
        "n_active_2025_ge30": int(len(p25)), "n_active_2026_ge30": int(len(p26)),
        "n_active_both_ge30": int(len(merged)),
        "spearman_rho": corr, "n": n, "ci95": ci,
        "top_decile_2025_n": len(top25), "top_decile_2026_n": len(top26),
        "top_decile_overlap_n": len(overlap), "top_decile_turnover_rate": turnover,
    }


# ---------------------------------------------------------------------------
def main():
    t0 = time.time()
    positions, traders, markets, schedule, candles, trades = load_all()

    print("Building first-pitch join + token-winner map...")
    first_pitch = tm.build_first_pitch_map(markets, schedule)
    trades["timing"] = tm.classify_timing(trades, first_pitch)

    print("Building wallet-market table...")
    wm = build_wallet_market_table(positions, markets, first_pitch)

    print("\n=== Task 6: zero-sum sanity anchor (20 random markets) ===")
    zs = sanity_zero_sum(positions, markets, n=20)
    print(zs.to_string())
    RESULTS["zero_sum_check"] = zs.to_dict("records")
    RESULTS["zero_sum_check_summary"] = {
        "mean_pct_of_invested": float(zs["net_pnl_pct_of_invested"].mean()),
        "median_pct_of_invested": float(zs["net_pnl_pct_of_invested"].median()),
    }

    print("\n=== Census stats ===")
    RESULTS["census"] = census_stats(traders, wm)
    print(json.dumps(RESULTS["census"], indent=2, default=_json_default)[:3000])

    print("\n=== Current-list overlap ===")
    RESULTS["list_overlap"] = current_list_overlap(traders)
    print(json.dumps(RESULTS["list_overlap"], indent=2, default=_json_default))

    print("\n=== H1: rolling 15d window persistence (2025+2026) ===")
    RESULTS["H1"] = h1_rolling_persistence(wm)
    print(json.dumps(RESULTS["H1"], indent=2, default=_json_default))

    print("\n=== H2: selection metric comparison ===")
    closing_price = build_closing_price_map(candles, markets, first_pitch)
    RESULTS["H2"] = h2_selection_metrics(wm, trades, closing_price)
    print(json.dumps(RESULTS["H2"], indent=2, default=_json_default))

    print("\n=== H3: totals markets ===")
    RESULTS["H3"] = h3_totals(wm)
    print(json.dumps(RESULTS["H3"], indent=2, default=_json_default))

    print("\n=== H4: timing buckets ===")
    RESULTS["H4"] = h4_timing(trades, markets, first_pitch)
    print(json.dumps(RESULTS["H4"], indent=2, default=_json_default))

    print("\n=== H7: cross-season persistence ===")
    RESULTS["H7"] = h7_cross_season(wm)
    print(json.dumps(RESULTS["H7"], indent=2, default=_json_default))

    out_path = LAKE_DIR / "hypothesis_results.json"
    with open(out_path, "w") as f:
        json.dump(RESULTS, f, indent=2, default=_json_default)
    print(f"\nWrote {out_path}")
    print(f"Total elapsed: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
