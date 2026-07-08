#!/usr/bin/env python3
"""Cross-sport "year-round traders" join — generalizes analysis/persistent_winners.py's
MLB persistent-winner methodology (funnel, bootstrap CIs, walk-forward validation,
copyability panel) to NFL and NBA, then joins across sports to answer the standing
research question: is there a small set of wallets whose edge shows up in MORE THAN
ONE sport (a "year-round trader"), or is skill sport-specific (seasonal specialists)?

Per-sport period definitions (independent two-period persistence check, same shape
as MLB's 2025-vs-2026 test):
  MLB: 2025 season vs 2026 season (REUSED VERBATIM from analysis/persistent_winners.py
       -- this script does not re-derive MLB's funnel/survivors/walk-forward; it loads
       data/persistent_winners.json, generating it via a single `import
       persistent_winners; persistent_winners.main()` call if that file doesn't exist
       yet. MLB logic is never forked/reimplemented here.)
  NFL: 2024 season vs 2025 season (Gamma `season` int, e.g. 2024.0 in markets_nfl.parquet,
       "2024" string in trades_nfl.parquet's season_dir column).
  NBA: "2024-25" season vs "2025-26" season (Gamma `season` string label, matching
       verbatim between markets_nba.parquet and trades_nba.parquet).

NFL/NBA schema differences from MLB handled explicitly:
  - No schedule.parquet / statsapi join for these sports. Pre-game cutoff uses Gamma's
    own `game_start_time` field (markets_<sport>.parquet), falling back to `end_date`
    when game_start_time is null (common for futures/season-long markets, which have
    no single "game" to start).
  - No candles_<sport>.parquet exists (not ingested for this phase) -- the copyability
    panel reports minutes-to-game-start only; the price-drift-after-entry calc MLB
    does against candles.parquet is explicitly SKIPPED and noted as unavailable.
  - trades_nba.parquet's `season` column is VARCHAR ("2025-26"), trades_nfl.parquet's
    is VARCHAR too but numeric-looking ("2024") since NFL's native season is castable
    to int; markets_nfl.parquet's `season` column is float64 (2024.0) while
    markets_nba.parquet's is a VARCHAR label -- season filtering is done via the
    `_season_key()` helper below rather than assuming one dtype.

Position reconstruction / settlement / bot heuristics reuse trader_metrics.py's
tested functions unchanged (reconstruct_positions, settle_positions,
build_token_winner_map, bot_heuristics). Wallet-market rollup, wallet-season summary,
and the bootstrap-CI-by-market routine reuse persistent_winners.py's own helpers
(market_rollup_with_price, wallet_season_summary, bootstrap_pooled_roi_ci) and its
bot/stake threshold constants verbatim (BOT_TWO_SIDED_MAX=0.20,
BOT_ENTROPY_MAX=0.95 recalibrated threshold, BOT_MIN_MEDIAN_GAP_S=60s,
MIN_MEDIAN_FILL_NOTIONAL=$20) -- nothing here re-derives or forks that logic.

Heavy scans (trades_<sport>.parquet) go through DuckDB for the stake/median-notional
queries per the project's memory-guidance convention; the sport trades files are
small enough (tens of MB even at full-sweep scale) to reconstruct positions on in
pandas directly, same as persistent_winners.py does for MLB's pregame_fills.parquet.

Writes:
  data/year_round_traders.json
  reports/09_year_round_traders.md

Usage:
    .venv/bin/python3 analysis/cross_sport_winners.py --sports mlb,nfl,nba
    .venv/bin/python3 analysis/cross_sport_winners.py --sports nfl,nba --min-n 1   # dry run vs smoke-test parquets
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from itertools import combinations
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
LAKE_DIR = BASE / "data" / "lake"
DATA_DIR = BASE / "data"
REPORTS_DIR = BASE / "reports"
sys.path.insert(0, str(BASE / "analysis"))
import persistent_winners as pw  # noqa: E402  (MLB methodology reused verbatim)
import trader_metrics as tm  # noqa: E402  (tested position/settlement/bot-feature logic)
from wallet_screen import screen_wallets  # noqa: E402  (external-reality screen, shared w/ persistent_winners.py)

DEFAULT_MIN_N = 50
RELAXED_MIN_N = 30  # per-sport strict-funnel relax, only auto-applied at the default floor
TIER1_RELAXED_MIN_N = 30  # cross-sport Tier-1-relaxed pregame-n floor in the "other" sport

SPORT_PERIODS: dict[str, list] = {
    "mlb": [2025, 2026],
    "nfl": [2024, 2025],
    "nba": ["2024-25", "2025-26"],
}
SEASON_ROTATION = {"mlb": "summer", "nfl": "fall", "nba": "winter"}
MLB_JSON_PATH = DATA_DIR / "persistent_winners.json"

# Reused verbatim from persistent_winners.py -- not re-derived here.
BOT_TWO_SIDED_MAX = pw.BOT_TWO_SIDED_MAX
BOT_ENTROPY_MAX = pw.BOT_ENTROPY_MAX
BOT_MIN_MEDIAN_GAP_S = pw.BOT_MIN_MEDIAN_GAP_S
MIN_MEDIAN_FILL_NOTIONAL = pw.MIN_MEDIAN_FILL_NOTIONAL
N_BOOT = pw.N_BOOT
RNG_SEED = pw.RNG_SEED


# ---------------------------------------------------------------------------
# Shared helpers (sport-agnostic)
# ---------------------------------------------------------------------------
def _season_key(sport: str, period) -> str:
    """trades_<sport>.parquet's `season` column is always the season_dir VARCHAR
    string used at ingest time (see targets_sport.py): str(int(season)) for NFL,
    str(season) for NBA."""
    return str(int(period)) if sport == "nfl" else str(period)


def wallet_market_from_trades(trades: pd.DataFrame, token_winner: pd.DataFrame) -> pd.DataFrame:
    """trades -> (wallet, condition_id) rollup with invested/returned/roi/win, via
    trader_metrics's tested reconstruct_positions/settle_positions and
    persistent_winners's tested market_rollup_with_price -- reused unchanged."""
    cols = ["wallet", "condition_id", "invested", "returned", "buy_size", "buy_cost", "market_type", "roi", "win"]
    if trades.empty:
        return pd.DataFrame(columns=cols)
    pos = tm.reconstruct_positions(trades)
    pos = tm.settle_positions(pos, token_winner)
    pos = pos.rename(columns={"proxy_wallet": "wallet"})
    return pw.market_rollup_with_price(pos)


def wallet_ci(wm: pd.DataFrame, wallet: str) -> tuple[float, float]:
    sub = wm[wm["wallet"] == wallet]
    return pw.bootstrap_pooled_roi_ci(sub["invested"].to_numpy(), sub["returned"].to_numpy())


def median_fill_notional_path(parquet_path: Path, wallets: list[str]) -> pd.DataFrame:
    """DuckDB scan of a trades parquet, median(size*price) per wallet -- mirrors
    persistent_winners.py's median_fill_notional(), parameterized by file path so
    it works for trades_nfl.parquet / trades_nba.parquet as well as trades.parquet."""
    if not wallets or not parquet_path.exists():
        return pd.DataFrame(columns=["wallet", "median_notional"])
    con = duckdb.connect()
    wdf = pd.DataFrame({"wallet": wallets})
    con.register("wdf", wdf)
    q = f"""
        SELECT t.proxy_wallet AS wallet, median(t.size * t.price) AS median_notional
        FROM read_parquet('{parquet_path.as_posix()}') t
        JOIN wdf ON t.proxy_wallet = wdf.wallet
        GROUP BY t.proxy_wallet
    """
    out = con.execute(q).fetchdf()
    con.close()
    return out


def median_fill_notional_df(trades: pd.DataFrame) -> pd.DataFrame:
    """Same stat, computed from an in-memory (already period-filtered) trades
    frame -- used by the walk-forward step, which must compute stake features
    on the TRAIN period only (no pandas-vs-duckdb behavioral difference)."""
    if trades.empty:
        return pd.DataFrame(columns=["wallet", "median_notional"])
    t = trades.copy()
    t["notional"] = t["size"] * t["price"]
    out = t.groupby("proxy_wallet")["notional"].median().reset_index()
    return out.rename(columns={"proxy_wallet": "wallet", "notional": "median_notional"})


def portfolio_pooled(wm: pd.DataFrame) -> dict:
    if wm.empty:
        return dict(n_wallets=0, n_markets=0, invested=0.0, returned=0.0,
                    roi_pooled=float("nan"), roi_ci95=[float("nan"), float("nan")])
    inv, ret = wm["invested"].sum(), wm["returned"].sum()
    roi = (ret - inv) / inv if inv > 0 else float("nan")
    ci = pw.bootstrap_pooled_roi_ci(wm["invested"].to_numpy(), wm["returned"].to_numpy())
    return dict(n_wallets=int(wm["wallet"].nunique()), n_markets=int(len(wm)),
                invested=round(float(inv), 2), returned=round(float(ret), 2),
                roi_pooled=round(float(roi), 4), roi_ci95=[round(ci[0], 4), round(ci[1], 4)])


def _safe_score(score) -> float:
    return score if (score is not None and not (isinstance(score, float) and np.isnan(score))) else -1e9


# ---------------------------------------------------------------------------
# NFL / NBA generic pipeline (mirrors persistent_winners.py's funnel structure)
# ---------------------------------------------------------------------------
def load_sport_lake(sport: str) -> dict:
    trades = pd.read_parquet(LAKE_DIR / f"trades_{sport}.parquet")
    markets = pd.read_parquet(LAKE_DIR / f"markets_{sport}.parquet")
    print(f"  [{sport}] trades={len(trades)} rows, {trades['condition_id'].nunique()} markets, "
          f"{trades['proxy_wallet'].nunique()} wallets, seasons={sorted(trades['season'].unique().tolist())}")
    print(f"  [{sport}] markets={len(markets)} rows")
    return {"trades": trades, "markets": markets}


def build_cutoff_map(markets: pd.DataFrame) -> pd.Series:
    """condition_id -> pre-game cutoff timestamp: Gamma's game_start_time, falling
    back to end_date when game_start_time is null (futures/season-long markets, or
    any market Gamma didn't attach a start time to)."""
    gst = pd.to_datetime(markets["game_start_time"], utc=True, errors="coerce")
    end = pd.to_datetime(markets["end_date"], utc=True, errors="coerce")
    cutoff = gst.fillna(end)
    n_gst, n_fallback = int(gst.notna().sum()), int((gst.isna() & end.notna()).sum())
    n_missing = int(cutoff.isna().sum())
    print(f"    cutoff map: {n_gst} from game_start_time, {n_fallback} fell back to end_date, "
          f"{n_missing} have neither (excluded from pre-game classification)")
    # NOTE: must reassign the index in place (not rebuild via .values) -- extracting
    # a tz-aware Series's .values drops tz-awareness (-> naive datetime64[ns] numpy
    # array), which then blows up as "Cannot compare tz-naive and tz-aware" the
    # moment classify_pregame() compares it against trades["timestamp"] (UTC-aware).
    cutoff.index = markets["condition_id"].values
    return cutoff


def classify_pregame(trades: pd.DataFrame, cutoff: pd.Series) -> pd.Series:
    cut = trades["condition_id"].map(cutoff)
    return cut.notna() & (trades["timestamp"] < cut)


def minutes_to_cutoff_sport(trades: pd.DataFrame, cutoff: pd.Series, wallets: list[str]) -> pd.DataFrame:
    if not wallets:
        return pd.DataFrame(columns=["wallet", "median_minutes_to_game_start"])
    f = trades[(trades["proxy_wallet"].isin(wallets)) & (trades["side"] == "BUY")].copy()
    f["cutoff"] = f["condition_id"].map(cutoff)
    f = f.dropna(subset=["cutoff"])
    f["minutes_to_cutoff"] = (f["cutoff"] - f["timestamp"]).dt.total_seconds() / 60.0
    f = f[f["minutes_to_cutoff"] >= 0]
    g = f.groupby("proxy_wallet")["minutes_to_cutoff"].median().rename("median_minutes_to_game_start")
    return g.reset_index().rename(columns={"proxy_wallet": "wallet"})


def sport_wallet_activity_alltime(sport: str, trades: pd.DataFrame, markets: pd.DataFrame,
                                   token_winner: pd.DataFrame, cutoff: pd.Series,
                                   wallets: list[str] | None = None) -> pd.DataFrame:
    """Pooled, ALL-seasons-in-file (not just the two funnel periods) wallet activity
    for `sport`: n_markets/roi, full-history and pre-game. Used for the cross-sport
    overlap matrix and the Tier-1-relaxed check -- "is this wallet active at all in
    this other sport, and what's their edge there." Restricting to `wallets` up front
    keeps the reconstruction cheap even at full-sweep scale."""
    t = trades if wallets is None else trades[trades["proxy_wallet"].isin(wallets)]
    wm_full = wallet_market_from_trades(t, token_winner)
    is_pre = classify_pregame(t, cutoff) if not t.empty else pd.Series(dtype=bool)
    wm_pg = wallet_market_from_trades(t[is_pre] if not t.empty else t, token_winner)
    full_s = pw.wallet_season_summary(wm_full).rename(
        columns={"n_markets": "n_markets_full", "roi_pooled": "roi_full"})
    pg_s = pw.wallet_season_summary(wm_pg).rename(
        columns={"n_markets": "n_markets_pregame", "roi_pooled": "roi_pregame"})
    cols_full = full_s[["wallet", "n_markets_full", "roi_full"]] if not full_s.empty else \
        pd.DataFrame(columns=["wallet", "n_markets_full", "roi_full"])
    cols_pg = pg_s[["wallet", "n_markets_pregame", "roi_pregame"]] if not pg_s.empty else \
        pd.DataFrame(columns=["wallet", "n_markets_pregame", "roi_pregame"])
    return cols_full.merge(cols_pg, on="wallet", how="outer")


def walk_forward_sport(sport: str, trades: pd.DataFrame, token_winner: pd.DataFrame,
                        cutoff: pd.Series, periods: list, floor: int) -> dict:
    """Select on period[0] ONLY (using period[0]-only-computed bot/stake features,
    so no period[1] behavioral info leaks into selection), measure period[1] OOS.
    Same honesty check as persistent_winners.py's walk_forward_2025_only, generalized."""
    p0, p1 = periods[0], periods[1]
    print(f"\n  --- [{sport}] walk-forward: select on {p0} only, measure {p1} OOS ---")
    tp0 = trades[trades["season"] == _season_key(sport, p0)]
    is_pre0 = classify_pregame(tp0, cutoff) if not tp0.empty else pd.Series(dtype=bool)
    wm_full0 = wallet_market_from_trades(tp0, token_winner)
    wm_pg0 = wallet_market_from_trades(tp0[is_pre0] if not tp0.empty else tp0, token_winner)
    fs0, pg0 = pw.wallet_season_summary(wm_full0), pw.wallet_season_summary(wm_pg0)

    pool = set(pg0[pg0["n_markets"] >= floor]["wallet"]) if not pg0.empty else set()
    pool &= set(fs0[fs0["roi_pooled"] > 0]["wallet"]) if not fs0.empty else set()
    pool &= set(pg0[pg0["roi_pooled"] > 0]["wallet"]) if not pg0.empty else set()
    print(f"    n>={floor} pregame({p0}) & full ROI>0({p0}) & pregame ROI>0({p0}): {len(pool)}")

    bot0 = tm.bot_heuristics(tp0).rename(columns={"proxy_wallet": "wallet"}) if not tp0.empty else \
        pd.DataFrame(columns=["wallet", "two_sided_share", "active_hours_entropy", "median_inter_trade_seconds"])
    notional0 = median_fill_notional_df(tp0)
    feats = bot0.merge(notional0, on="wallet", how="left")
    keep_mask = (
        (feats["two_sided_share"] < BOT_TWO_SIDED_MAX)
        & (feats["active_hours_entropy"] < BOT_ENTROPY_MAX)
        & (feats["median_inter_trade_seconds"] >= BOT_MIN_MEDIAN_GAP_S)
        & (feats["median_notional"] >= MIN_MEDIAN_FILL_NOTIONAL)
    ) if not feats.empty else pd.Series(dtype=bool)
    keep = sorted(set(feats[keep_mask]["wallet"]) & pool) if not feats.empty else []
    print(f"    after {p0}-only bot/stake filters: {len(keep)} wallets selected ({p0}-only portfolio)")

    tp1 = trades[trades["season"] == _season_key(sport, p1)]
    is_pre1 = classify_pregame(tp1, cutoff) if not tp1.empty else pd.Series(dtype=bool)
    tp1_keep = tp1[tp1["proxy_wallet"].isin(keep)]
    wm_full1 = wallet_market_from_trades(tp1_keep, token_winner)
    wm_pg1 = wallet_market_from_trades(
        tp1_keep[is_pre1.reindex(tp1_keep.index)] if not tp1_keep.empty else tp1_keep, token_winner)

    full_oos, pg_oos = portfolio_pooled(wm_full1), portfolio_pooled(wm_pg1)
    print(f"    {p0}-selected portfolio (n={len(keep)}) {p1} OOS: full ROI={full_oos['roi_pooled']} "
          f"(n_mkts={full_oos['n_markets']}), pregame ROI={pg_oos['roi_pooled']} (n_mkts={pg_oos['n_markets']})")
    return {
        "train_period": str(p0), "test_period": str(p1),
        "n_selected_train": len(keep),
        "full_oos": full_oos, "pregame_oos": pg_oos,
        "selected_wallets": keep,
    }


def run_generic_sport(sport: str, min_n: int) -> tuple[dict, dict]:
    print(f"\n{'=' * 78}\nSport: {sport.upper()}\n{'=' * 78}")
    periods = SPORT_PERIODS[sport]
    lake = load_sport_lake(sport)
    trades, markets = lake["trades"], lake["markets"]
    token_winner = tm.build_token_winner_map(markets)
    cutoff = build_cutoff_map(markets)
    bot_feats_all = tm.bot_heuristics(trades).rename(columns={"proxy_wallet": "wallet"})

    wm_full, wm_pregame = {}, {}
    for p in periods:
        tp = trades[trades["season"] == _season_key(sport, p)]
        is_pre = classify_pregame(tp, cutoff) if not tp.empty else pd.Series(dtype=bool)
        wm_full[p] = wallet_market_from_trades(tp, token_winner)
        wm_pregame[p] = wallet_market_from_trades(tp[is_pre] if not tp.empty else tp, token_winner)
        print(f"  period {p}: {tp['condition_id'].nunique()} markets traded, "
              f"{len(wm_full[p])} wallet-market full rows, {len(wm_pregame[p])} wallet-market pregame rows")

    full_summary = {p: pw.wallet_season_summary(wm_full[p]) for p in periods}
    pg_summary = {p: pw.wallet_season_summary(wm_pregame[p]) for p in periods}

    funnel = {}
    floor = min_n

    def _elig(summary_dict, col, thresh, op):
        sets = []
        for p in periods:
            s = summary_dict[p]
            sets.append(set(s[op(s[col], thresh)]["wallet"]) if not s.empty else set())
        return set.intersection(*sets) if sets else set()

    pool = _elig(pg_summary, "n_markets", floor, lambda c, t: c >= t)
    funnel[f"1_active_both_periods_n>={floor}"] = len(pool)
    print(f"  Funnel: n>={floor} pregame markets in BOTH periods: {len(pool)}")

    relaxed_used = False
    if len(pool) == 0 and min_n == DEFAULT_MIN_N:
        floor = RELAXED_MIN_N
        relaxed_used = True
        pool = _elig(pg_summary, "n_markets", floor, lambda c, t: c >= t)
        funnel[f"1_active_both_periods_n>={floor}_relaxed"] = len(pool)
        print(f"  [RELAX] zero at n>={min_n} -- floor -> n>={floor}: {len(pool)}")

    if pool:
        pool &= _elig(full_summary, "roi_pooled", 0, lambda c, t: c > t)
    funnel["2_positive_full_roi_both"] = len(pool)
    print(f"  Funnel: + positive FULL settlement ROI both periods: {len(pool)}")

    if pool:
        pool &= _elig(pg_summary, "roi_pooled", 0, lambda c, t: c > t)
    funnel["3_positive_pregame_roi_both"] = len(pool)
    print(f"  Funnel: + positive PRE-GAME settlement ROI both periods: {len(pool)}")

    bot_ok = set(bot_feats_all[
        (bot_feats_all["two_sided_share"] < BOT_TWO_SIDED_MAX)
        & (bot_feats_all["active_hours_entropy"] < BOT_ENTROPY_MAX)
        & (bot_feats_all["median_inter_trade_seconds"] >= BOT_MIN_MEDIAN_GAP_S)
    ]["wallet"]) if not bot_feats_all.empty else set()
    pool &= bot_ok
    funnel["4_not_bot_shaped"] = len(pool)
    print(f"  Funnel: + not bot/MM-shaped (two_sided<{BOT_TWO_SIDED_MAX}, entropy<{BOT_ENTROPY_MAX}, "
          f"median_gap>={BOT_MIN_MEDIAN_GAP_S:.0f}s): {len(pool)}")

    notional_df = median_fill_notional_path(LAKE_DIR / f"trades_{sport}.parquet", sorted(pool))
    stake_ok = set(notional_df[notional_df["median_notional"] >= MIN_MEDIAN_FILL_NOTIONAL]["wallet"]) \
        if not notional_df.empty else set()
    pool &= stake_ok
    funnel["5_median_notional>=20"] = len(pool)
    print(f"  Funnel: + median fill notional >= ${MIN_MEDIAN_FILL_NOTIONAL:.0f}: {len(pool)}")

    survivors_pre_screen = sorted(pool)
    print(f"  === {sport.upper()} FUNNEL SURVIVORS (pre-screen): {len(survivors_pre_screen)} wallets ===")

    full_idx = {p: full_summary[p].set_index("wallet") for p in periods}
    pg_idx = {p: pg_summary[p].set_index("wallet") for p in periods}
    bot_idx = bot_feats_all.set_index("wallet")

    # --- Criterion 6 (mandatory, final -- before ranking): external-reality
    # screen. Same shared helper (analysis/wallet_screen.py) and verdict
    # rules as MLB's screen in analysis/persistent_winners.py, so a
    # Yikes110-shaped busted-gambler-riding-a-hot-streak wallet can't slip
    # into the NFL/NBA finalist list once the real sweeps land either.
    category_profit = {
        w: float(sum(full_idx[p].loc[w, "profit"] for p in periods if w in full_idx[p].index))
        for w in survivors_pre_screen
    }
    screens = screen_wallets(survivors_pre_screen, category_profit=category_profit)
    screened_out = [w for w in survivors_pre_screen if screens[w]["verdict"] == "FAIL"]
    survivors = [w for w in survivors_pre_screen if w not in screened_out]
    funnel["6_external_reality_screen"] = len(survivors)
    print(f"  Funnel: + external-reality screen (lifetime all-category PnL, current value, staleness): "
          f"{len(survivors)}/{len(survivors_pre_screen)} (screened out: {screened_out})")

    notional_idx = notional_df.set_index("wallet") if not notional_df.empty else notional_df
    minutes_df = minutes_to_cutoff_sport(trades, cutoff, survivors)
    minutes_idx = minutes_df.set_index("wallet") if not minutes_df.empty else minutes_df

    records = []
    for w in survivors:
        per = {}
        ci_pg_lowers = []
        for p in periods:
            ci_full = wallet_ci(wm_full[p], w)
            ci_pg = wallet_ci(wm_pregame[p], w)
            ci_pg_lowers.append(ci_pg[0])
            per[str(p)] = {
                "n_markets_full": int(full_idx[p].loc[w, "n_markets"]),
                "roi_full": round(float(full_idx[p].loc[w, "roi_pooled"]), 4),
                "roi_full_ci95": [round(ci_full[0], 4), round(ci_full[1], 4)],
                "n_markets_pregame": int(pg_idx[p].loc[w, "n_markets"]),
                "roi_pregame": round(float(pg_idx[p].loc[w, "roi_pooled"]), 4),
                "roi_pregame_ci95": [round(ci_pg[0], 4), round(ci_pg[1], 4)],
                "win_rate_pregame": round(float(pg_idx[p].loc[w, "win_rate"]), 4),
            }
        combined_score = min(ci_pg_lowers) if all(not np.isnan(x) for x in ci_pg_lowers) else float("nan")
        tf = bot_idx.loc[w] if w in bot_idx.index else None
        records.append({
            "wallet": w,
            "combined_score": round(combined_score, 4) if not np.isnan(combined_score) else None,
            "periods": per,
            "bot_features": {
                "two_sided_share": round(float(tf["two_sided_share"]), 4) if tf is not None else None,
                "active_hours_entropy": round(float(tf["active_hours_entropy"]), 4) if tf is not None else None,
                "median_inter_trade_seconds": round(float(tf["median_inter_trade_seconds"]), 1) if tf is not None else None,
            },
            "sizing": {
                "median_fill_notional": round(float(notional_idx.loc[w, "median_notional"]), 2)
                if not notional_idx.empty and w in notional_idx.index else None,
            },
            "copyability": {
                "median_minutes_to_game_start": round(float(minutes_idx.loc[w, "median_minutes_to_game_start"]), 1)
                if not minutes_idx.empty and w in minutes_idx.index else None,
                "candle_drift_available": False,
                "note": f"no candles_{sport}.parquet ingested in this phase -- price-drift-after-entry not computed",
            },
            "external_screen": {
                "pseudonym": screens[w]["pseudonym"],
                "lifetime_pnl": screens[w]["lifetime_pnl"],
                "current_value": screens[w]["current_value"],
                "last_trade": screens[w]["last_trade"].isoformat() if screens[w]["last_trade"] is not None else None,
                "days_since_last_trade": screens[w]["days_since_last_trade"],
                "stale": screens[w]["stale"],
                "screen_verdict": screens[w]["verdict"],
                "notes": screens[w]["notes"],
            },
        })
    records.sort(key=lambda r: _safe_score(r["combined_score"]), reverse=True)

    wf = walk_forward_sport(sport, trades, token_winner, cutoff, periods, floor)

    output = {
        "sport": sport,
        "periods": [str(p) for p in periods],
        "min_n_floor": floor,
        "relaxed_floor_used": relaxed_used,
        "funnel": funnel,
        "survivors": records,
        "screened_out_wallets": screened_out,
        "walk_forward": wf,
        "source": f"analysis/cross_sport_winners.py generic pipeline (trades_{sport}.parquet, "
                  f"markets_{sport}.parquet)",
    }
    ctx = {"trades": trades, "markets": markets, "token_winner": token_winner, "cutoff": cutoff}
    return output, ctx


# ---------------------------------------------------------------------------
# MLB adapter -- reuse persistent_winners.py's output verbatim
# ---------------------------------------------------------------------------
def run_mlb(min_n: int) -> dict:
    print(f"\n{'=' * 78}\nSport: MLB (reusing analysis/persistent_winners.py output verbatim)\n{'=' * 78}")
    if not MLB_JSON_PATH.exists():
        print(f"  {MLB_JSON_PATH} not found -- running analysis/persistent_winners.py once to generate it...")
        pw.main()
    with open(MLB_JSON_PATH) as f:
        mlb = json.load(f)
    print(f"  Funnel (reused from data/persistent_winners.json): {mlb['funnel']}")

    # --- Criterion 6 (mandatory, final -- before ranking): external-reality
    # screen. MLB's screen already ran inside analysis/persistent_winners.py
    # (same analysis/wallet_screen.py helper NFL/NBA use below) and its
    # verdicts are baked into data/persistent_winners.json -- reused
    # verbatim, not re-fetched, so this stays a read-only adapter.
    screened_out = set(w.lower() for w in mlb.get("screened_out_wallets", []))
    if screened_out:
        print(f"  Funnel: + external-reality screen (reused from data/persistent_winners.json): "
              f"screened out {sorted(screened_out)}")

    records = []
    for r in mlb["survivors"]:
        if r["wallet"].lower() in screened_out:
            continue
        per = {}
        for p in SPORT_PERIODS["mlb"]:
            src = r["periods"][str(p)]
            per[str(p)] = {
                "n_markets_full": src["n_markets_full"], "roi_full": src["roi_full"],
                "roi_full_ci95": src["roi_full_ci95"],
                "n_markets_pregame": src["n_markets_pregame"], "roi_pregame": src["roi_pregame"],
                "roi_pregame_ci95": src["roi_pregame_ci95"], "win_rate_pregame": src["win_rate_pregame"],
            }
        records.append({
            "wallet": r["wallet"],
            "combined_score": r["combined_score"],
            "periods": per,
            "bot_features": {
                "two_sided_share": r["bot_features"]["two_sided_share"],
                "active_hours_entropy": r["bot_features"]["active_hours_entropy"],
                "median_inter_trade_seconds": r["bot_features"]["median_inter_trade_seconds"],
            },
            "sizing": {"median_fill_notional": r["sizing"]["median_fill_notional_alltime"]},
            "copyability": {
                "median_minutes_to_game_start": r["copyability"]["median_minutes_to_first_pitch"],
                "candle_drift_available": True,
                "price_drift_1h_after_entry": r["copyability"]["price_drift_1h_after_entry"],
                "n_fills_matched_for_drift": r["copyability"]["n_fills_matched_for_drift"],
            },
            "external_screen": r.get("external_screen"),
        })
    records.sort(key=lambda r: _safe_score(r["combined_score"]), reverse=True)

    wf_raw = mlb["walk_forward_2025_selected_2026_oos"]
    wf = {
        "train_period": "2025", "test_period": "2026",
        "n_selected_train": wf_raw["n_selected_2025"],
        "full_oos": wf_raw["full_2026_oos"], "pregame_oos": wf_raw["pregame_2026_oos"],
        "selected_wallets": wf_raw["selected_wallets"],
    }

    return {
        "sport": "mlb",
        "periods": [str(p) for p in SPORT_PERIODS["mlb"]],
        "min_n_floor": mlb["methodology"]["eligibility_n_floor"],
        "relaxed_floor_used": mlb["methodology"]["relaxed_floor_used"],
        "funnel": mlb["funnel"],
        "survivors": records,
        "screened_out_wallets": mlb.get("screened_out_wallets", []),
        "walk_forward": wf,
        "source": "reused verbatim from data/persistent_winners.json (analysis/persistent_winners.py)",
    }


def mlb_wallet_activity_pooled(wallets: list[str]) -> pd.DataFrame:
    """Pooled (all MLB seasons in the lake, not just 2025/2026), all-time wallet
    activity for an arbitrary wallet list -- the MLB-side counterpart to
    sport_wallet_activity_alltime(), used for the cross-sport overlap matrix / Tier-1-
    relaxed check. Full-history side scans positions.parquet (already-settled, tested
    artifact); pre-game side reconstructs from pregame_fills.parquet via the same
    tested reconstruct_positions/settle_positions functions used everywhere else in
    this script -- both via DuckDB, filtered to the (small) cross-sport candidate
    wallet set up front so this stays cheap even though the underlying files are
    hundreds of MB."""
    empty = pd.DataFrame(columns=["wallet", "n_markets_full", "roi_full", "n_markets_pregame", "roi_pregame"])
    if not wallets:
        return empty
    con = duckdb.connect()
    wdf = pd.DataFrame({"wallet": [w.lower() for w in wallets]})
    con.register("wdf", wdf)
    full = con.execute(f"""
        SELECT p.wallet AS wallet, sum(p.invested) AS invested, sum(p.returned) AS returned,
               count(DISTINCT p.condition_id) AS n_markets_full
        FROM read_parquet('{(LAKE_DIR / "positions.parquet").as_posix()}') p
        JOIN wdf ON p.wallet = wdf.wallet
        WHERE p.invested > 0
        GROUP BY p.wallet
    """).fetchdf()
    if not full.empty:
        full["roi_full"] = np.where(full["invested"] > 0, (full["returned"] - full["invested"]) / full["invested"], np.nan)
    pregame_rows = con.execute(f"""
        SELECT proxy_wallet, condition_id, token_id, side, size, price, trade_id, timestamp,
               is_winner, market_type, outcome
        FROM read_parquet('{(LAKE_DIR / "pregame_fills.parquet").as_posix()}')
        WHERE proxy_wallet IN (SELECT wallet FROM wdf)
    """).fetchdf()
    con.close()

    if pregame_rows.empty:
        pg_out = pd.DataFrame(columns=["wallet", "n_markets_pregame", "roi_pregame"])
    else:
        token_winner = pregame_rows[["condition_id", "token_id", "is_winner", "market_type", "outcome"]].drop_duplicates()
        pos = tm.reconstruct_positions(
            pregame_rows[["proxy_wallet", "condition_id", "token_id", "side", "size", "price", "trade_id", "timestamp"]]
        )
        pos = tm.settle_positions(pos, token_winner).rename(columns={"proxy_wallet": "wallet"})
        wm = pw.market_rollup_with_price(pos)
        pg = pw.wallet_season_summary(wm)
        pg_out = pg[["wallet", "n_markets", "roi_pooled"]].rename(
            columns={"n_markets": "n_markets_pregame", "roi_pooled": "roi_pregame"})

    full_out = full[["wallet", "n_markets_full", "roi_full"]] if not full.empty else \
        pd.DataFrame(columns=["wallet", "n_markets_full", "roi_full"])
    return full_out.merge(pg_out, on="wallet", how="outer")


# ---------------------------------------------------------------------------
# Cross-sport join
# ---------------------------------------------------------------------------
def cross_sport_join(outputs: dict, activity: dict[str, pd.DataFrame]) -> dict:
    sports = list(outputs.keys())
    survivor_sets = {s: {r["wallet"].lower() for r in outputs[s]["survivors"]} for s in sports}

    if len(sports) < 2:
        return {"note": f"cross-sport join requires >=2 sports; only {sports} was run this time."}

    # --- Tier 1 strict: funnel-passing in >=2 sports ---
    pair_overlaps = {}
    multi = set()
    for a, b in combinations(sports, 2):
        inter = survivor_sets[a] & survivor_sets[b]
        if inter:
            pair_overlaps[f"{a}&{b}"] = sorted(inter)
            multi |= inter
    tier1_strict = {"wallets": sorted(multi), "pair_overlaps": pair_overlaps}

    tier1_relaxed = None
    if not multi:
        relaxed_hits = {}
        for a in sports:
            for b in sports:
                if a == b:
                    continue
                b_idx = activity[b].set_index("wallet") if not activity[b].empty else activity[b]
                hits = []
                for w in survivor_sets[a]:
                    if not b_idx.empty and w in b_idx.index:
                        row = b_idx.loc[w]
                        n_pg, roi_pg = row.get("n_markets_pregame"), row.get("roi_pregame")
                        if pd.notna(n_pg) and n_pg >= TIER1_RELAXED_MIN_N and pd.notna(roi_pg) and roi_pg > 0:
                            hits.append({"wallet": w, "n_markets_pregame": int(n_pg), "roi_pregame": round(float(roi_pg), 4)})
                if hits:
                    relaxed_hits[f"{a}_funnel_survivor->{b}_pregame_edge"] = hits
        tier1_relaxed = {
            "criterion": f"funnel-passing in sport A AND positive pre-game ROI with n>={TIER1_RELAXED_MIN_N} "
                         f"pooled (all seasons) in sport B",
            "hits": relaxed_hits,
        }

    # --- Tier 2: single-sport specialists (top funnel survivors per sport) ---
    tier2 = {s: outputs[s]["survivors"][:10] for s in sports}

    # --- Activity overlap matrix: each sport's survivors' activity (any n) in every OTHER sport ---
    matrix = {}
    for a in sports:
        matrix[a] = {}
        for b in sports:
            if a == b:
                continue
            b_idx = activity[b].set_index("wallet") if not activity[b].empty else activity[b]
            rows = []
            for w in sorted(survivor_sets[a]):
                if not b_idx.empty and w in b_idx.index:
                    row = b_idx.loc[w]
                    rows.append({
                        "wallet": w,
                        "n_markets_full": int(row["n_markets_full"]) if pd.notna(row["n_markets_full"]) else 0,
                        "roi_full": round(float(row["roi_full"]), 4) if pd.notna(row["roi_full"]) else None,
                        "n_markets_pregame": int(row["n_markets_pregame"]) if pd.notna(row["n_markets_pregame"]) else 0,
                        "roi_pregame": round(float(row["roi_pregame"]), 4) if pd.notna(row["roi_pregame"]) else None,
                    })
                else:
                    rows.append({"wallet": w, "n_markets_full": 0, "roi_full": None,
                                 "n_markets_pregame": 0, "roi_pregame": None})
            matrix[a][b] = rows

    return {
        "tier1_strict": tier1_strict,
        "tier1_relaxed": tier1_relaxed,
        "tier2_single_sport_specialists": tier2,
        "activity_overlap_matrix": matrix,
    }


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def _fmt_pct(x) -> str:
    return f"{x:+.1%}" if x is not None and not (isinstance(x, float) and np.isnan(x)) else "n/a"


def write_report(outputs: dict, cross: dict, args: argparse.Namespace, smoke_sports: set[str]) -> None:
    lines = []
    lines.append("# Phase 4 Report — Cross-Sport Year-Round Traders (MLB / NFL / NBA)\n")
    lines.append(f"Run date: {pd.Timestamp.now('UTC').date()}. Pipeline: `analysis/cross_sport_winners.py "
                 f"--sports {','.join(outputs.keys())} --min-n {args.min_n}`.\n")
    if smoke_sports:
        lines.append(f"**DRY-RUN NOTICE: {', '.join(sorted(smoke_sports))} ran against the tiny smoke-test "
                     "parquets (a handful of markets), not the full trades sweeps -- this run exists to prove "
                     "the pipeline executes end-to-end without crashing on the schema differences (VARCHAR "
                     "seasons, missing candles, null game_start_time), NOT to produce a real result. Re-run "
                     "with the command at the bottom of this report once the sweeps land.**\n")

    lines.append("## Per-sport methodology (generalizes analysis/persistent_winners.py's MLB funnel)\n")
    lines.append("Each sport independently: n >= min-n settled pre-game markets in EACH of its two periods, "
                 "positive FULL-history settlement ROI in EACH period, positive PRE-GAME-only settlement ROI "
                 "in EACH period, not MM/bot-shaped (two_sided_share < 0.20, active_hours_entropy < 0.95 "
                 "[recalibrated], median_inter_trade_seconds >= 60s), median fill notional >= $20, and (final, "
                 "mandatory stage 6, applied before ranking) passes the external-reality screen "
                 "(`analysis/wallet_screen.py`): lifetime all-category PnL not below -$10,000, not a "
                 "category-hot-streak-inside-a-modest-lifetime-record pattern, staleness noted. This stage "
                 "exists because a wallet can pass every in-category criterion above while being a net loser "
                 "in aggregate across every category they trade -- see reports/08's post-review section for "
                 "the case (Yikes110) that motivated it. MLB's 2025-vs-2026 run is reused verbatim from "
                 "`data/persistent_winners.json` -- not re-derived. NFL (2024 vs 2025) and NBA (2024-25 vs "
                 "2025-26) run the same criteria through a generic pipeline built on trader_metrics.py's "
                 "tested reconstruct_positions/settle_positions/bot_heuristics and persistent_winners.py's "
                 "tested rollup/bootstrap-CI helpers. Pre-game split "
                 "uses Gamma's `game_start_time`, falling back to `end_date` when null. No candles exist for "
                 "NFL/NBA in this phase, so the copyability panel for those sports omits the post-entry "
                 "price-drift calc MLB reports.\n")

    for sport, out in outputs.items():
        lines.append(f"## {sport.upper()} — periods {out['periods'][0]} vs {out['periods'][1]} "
                     f"({SEASON_ROTATION.get(sport, '?')})\n")
        lines.append(f"Source: {out['source']}\n")
        lines.append("| funnel stage | n wallets |\n|---|---:|")
        for k, v in out["funnel"].items():
            lines.append(f"| {k} | {v} |")
        lines.append("")
        if out["relaxed_floor_used"]:
            lines.append(f"**NOTE: strict n>={DEFAULT_MIN_N if args.min_n == DEFAULT_MIN_N else args.min_n} "
                         f"floor yielded zero wallets active both periods -- relaxed to n>={out['min_n_floor']}.**\n")
        if out.get("screened_out_wallets"):
            lines.append(f"**External-reality screen (stage 6) removed {len(out['screened_out_wallets'])} "
                         "wallet(s), FAIL verdict (lifetime all-category PnL below the -$10,000 floor): "
                         + ", ".join(f"`{w}`" for w in out["screened_out_wallets"]) + ".**\n")
        survivors = out["survivors"]
        lines.append(f"### {sport.upper()} survivors: {len(survivors)} wallets\n")
        if not survivors:
            lines.append("Zero wallets survive all five criteria for this sport this run.\n")
        else:
            p0, p1 = out["periods"]
            lines.append(f"| rank | wallet | combined score | {p0} n(full/pregame) | {p0} ROI full/pregame | "
                         f"{p1} n(full/pregame) | {p1} ROI full/pregame | median stake $ | median min-to-start |")
            lines.append("|---|---|---:|---|---|---|---|---:|---:|")
            for i, r in enumerate(survivors, 1):
                a, b = r["periods"][p0], r["periods"][p1]
                lines.append(
                    f"| {i} | `{r['wallet']}` | {r['combined_score']} | "
                    f"{a['n_markets_full']}/{a['n_markets_pregame']} | "
                    f"{_fmt_pct(a['roi_full'])}/{_fmt_pct(a['roi_pregame'])} | "
                    f"{b['n_markets_full']}/{b['n_markets_pregame']} | "
                    f"{_fmt_pct(b['roi_full'])}/{_fmt_pct(b['roi_pregame'])} | "
                    f"${r['sizing']['median_fill_notional'] or 0:,.0f} | "
                    f"{r['copyability']['median_minutes_to_game_start'] if r['copyability']['median_minutes_to_game_start'] is not None else 'n/a'} |"
                )
            lines.append("")

        wf = out["walk_forward"]
        lines.append(f"### {sport.upper()} walk-forward: select on {wf['train_period']} only, "
                     f"measure {wf['test_period']} OOS\n")
        lines.append(f"- **{wf['n_selected_train']} wallets** selected on {wf['train_period']}-only criteria.")
        fo, po = wf["full_oos"], wf["pregame_oos"]
        lines.append(f"- {wf['test_period']} OOS full ROI: **{_fmt_pct(fo['roi_pooled'])}** "
                     f"(95% CI {_fmt_pct(fo['roi_ci95'][0])} to {_fmt_pct(fo['roi_ci95'][1])}), "
                     f"n={fo['n_markets']} markets / {fo['n_wallets']} wallets.")
        lines.append(f"- {wf['test_period']} OOS pre-game ROI: **{_fmt_pct(po['roi_pooled'])}** "
                     f"(95% CI {_fmt_pct(po['roi_ci95'][0])} to {_fmt_pct(po['roi_ci95'][1])}), "
                     f"n={po['n_markets']} markets / {po['n_wallets']} wallets.\n")
        verdict = "SUPPORTED" if (fo["roi_ci95"][0] > 0 or po["roi_ci95"][0] > 0) else "NOT SUPPORTED"
        lines.append(f"**Verdict: {verdict}** (at least one OOS ROI's 95% CI excludes zero: "
                     f"{'yes' if verdict == 'SUPPORTED' else 'no'}).\n")

    lines.append("## Cross-sport join\n")
    if "note" in cross:
        lines.append(cross["note"] + "\n")
    else:
        t1 = cross["tier1_strict"]
        lines.append(f"### Tier 1 — multi-sport winners (strict: funnel-passing in >=2 sports)\n")
        lines.append(f"**{len(t1['wallets'])} wallets** pass the strict per-sport funnel in 2 or more sports.\n")
        if t1["wallets"]:
            for w in t1["wallets"]:
                lines.append(f"- `{w}`")
            lines.append("")
            for pair, wallets in t1["pair_overlaps"].items():
                lines.append(f"- {pair}: {len(wallets)} shared wallet(s)")
            lines.append("")
        if cross.get("tier1_relaxed") is not None:
            tr = cross["tier1_relaxed"]
            lines.append(f"**Strict Tier 1 was empty -- relaxed tier applied** ({tr['criterion']}):\n")
            if tr["hits"]:
                for k, hits in tr["hits"].items():
                    lines.append(f"- **{k}**: {len(hits)} wallet(s)")
                    for h in hits:
                        lines.append(f"  - `{h['wallet']}`: n_pregame={h['n_markets_pregame']}, "
                                     f"roi_pregame={_fmt_pct(h['roi_pregame'])}")
            else:
                lines.append("- Zero wallets meet the relaxed criterion either -- no multi-sport signal "
                             "detected this run (expected on a dry-run smoke test with near-empty periods).")
            lines.append("")

        lines.append("### Tier 2 — single-sport specialists (top funnel survivors per sport)\n")
        for sport, top in cross["tier2_single_sport_specialists"].items():
            lines.append(f"**{sport.upper()}** ({SEASON_ROTATION.get(sport, '?')}): {len(top)} listed")
            for r in top:
                lines.append(f"- `{r['wallet']}` combined_score={r['combined_score']}")
            lines.append("")

        lines.append("### Activity overlap matrix (each sport's survivors, activity in every OTHER sport)\n")
        matrix = cross["activity_overlap_matrix"]
        for a, per_b in matrix.items():
            for b, rows in per_b.items():
                lines.append(f"**{a.upper()} survivors' activity in {b.upper()}:**\n")
                if not rows:
                    lines.append("(no survivors to check)\n")
                    continue
                lines.append("| wallet | n_full | roi_full | n_pregame | roi_pregame |")
                lines.append("|---|---:|---:|---:|---:|")
                for r in rows:
                    lines.append(f"| `{r['wallet']}` | {r['n_markets_full']} | {_fmt_pct(r['roi_full'])} | "
                                 f"{r['n_markets_pregame']} | {_fmt_pct(r['roi_pregame'])} |")
                lines.append("")

    lines.append("## Copyability panel per finalist\n")
    lines.append("Minutes-to-game-start (or minutes-to-first-pitch for MLB) distribution, per survivor, per "
                 "sport. MLB additionally reports 1h post-entry price drift against candles.parquet; NFL/NBA "
                 "SKIP this calc entirely -- no candles_nfl.parquet / candles_nba.parquet exists in this phase, "
                 "so there is no candle series to measure drift against.\n")
    for sport, out in outputs.items():
        for r in out["survivors"]:
            cop = r["copyability"]
            if cop.get("candle_drift_available"):
                lines.append(f"- `{r['wallet']}` ({sport.upper()}): median minutes-to-first-pitch = "
                             f"{cop['median_minutes_to_game_start']}, 1h drift = "
                             f"{cop.get('price_drift_1h_after_entry')} (n={cop.get('n_fills_matched_for_drift')})")
            else:
                lines.append(f"- `{r['wallet']}` ({sport.upper()}): median minutes-to-game-start = "
                             f"{cop['median_minutes_to_game_start']} ({cop['note']})")
    lines.append("")

    lines.append("## Hygiene notes\n")
    lines.append("- All bootstrap CIs resample at the MARKET level (2,000 resamples, seed=42), same as "
                 "persistent_winners.py.\n"
                 "- Settlement ROI (cash-flow accounting) is the sole source of truth throughout.\n"
                 "- MLB numbers are reused verbatim from `data/persistent_winners.json` -- never re-derived "
                 "or forked in this script.\n"
                 "- NFL/NBA bot/stake filters use pooled (all-seasons-in-file) features, matching MLB's use "
                 "of traders.parquet's pooled full-history features.\n")

    real_cmd = f".venv/bin/python3 analysis/cross_sport_winners.py --sports {','.join(outputs.keys())} --min-n {DEFAULT_MIN_N}"
    lines.append(f"## Command to run once the real sweeps land\n\n```\n{real_cmd}\n```\n")

    out_path = REPORTS_DIR / "09_year_round_traders.md"
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        f.write("\n".join(lines))
    print(f"\nWrote {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sports", default="mlb,nfl,nba", help="comma-separated subset of mlb,nfl,nba")
    p.add_argument("--min-n", type=int, default=DEFAULT_MIN_N,
                   help=f"pre-game settled-markets-per-period floor (default {DEFAULT_MIN_N}; "
                        "pass 1 for a dry run against the smoke-test parquets)")
    return p.parse_args()


def main() -> None:
    t0 = time.time()
    args = parse_args()
    sports = [s.strip() for s in args.sports.split(",") if s.strip()]
    unknown = set(sports) - set(SPORT_PERIODS)
    if unknown:
        raise SystemExit(f"unknown sport(s) {unknown}; expected subset of {list(SPORT_PERIODS)}")

    outputs: dict[str, dict] = {}
    ctxs: dict[str, dict] = {}
    smoke_sports: set[str] = set()

    for s in sports:
        if s == "mlb":
            outputs["mlb"] = run_mlb(args.min_n)
        else:
            out, ctx = run_generic_sport(s, args.min_n)
            outputs[s] = out
            ctxs[s] = ctx
            if ctx["trades"]["condition_id"].nunique() < 50:
                smoke_sports.add(s)

    all_wallets = sorted({r["wallet"] for s in outputs for r in outputs[s]["survivors"]})
    print(f"\nTotal unique funnel-survivor wallets across {len(outputs)} sport(s): {len(all_wallets)}")

    activity: dict[str, pd.DataFrame] = {}
    for s in outputs:
        if s == "mlb":
            activity["mlb"] = mlb_wallet_activity_pooled(all_wallets)
        else:
            ctx = ctxs[s]
            activity[s] = sport_wallet_activity_alltime(
                s, ctx["trades"], ctx["markets"], ctx["token_winner"], ctx["cutoff"], wallets=all_wallets)

    cross = cross_sport_join(outputs, activity)

    output_json = {
        "generated_at": pd.Timestamp.now("UTC").isoformat(),
        "args": {"sports": sports, "min_n": args.min_n},
        "per_sport": outputs,
        "cross_sport": cross,
        "dry_run_sports": sorted(smoke_sports),
    }
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / "year_round_traders.json"
    with open(out_path, "w") as f:
        json.dump(output_json, f, indent=2, default=str)
    print(f"\nWrote {out_path}")

    write_report(outputs, cross, args, smoke_sports)
    print(f"\nTotal elapsed: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
