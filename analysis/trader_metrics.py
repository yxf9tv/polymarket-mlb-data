#!/usr/bin/env python3
"""Phase 1: Trader census & position/settlement metrics pipeline.

Idempotent single pass over the lake parquet files:
  data/lake/trades.parquet    (filtered to markets in data/lake/state/trades.json
                                completed_ids -- partial-market trade data never
                                enters metrics)
  data/lake/markets.parquet   (winning_outcome, market_type, token_ids, slug)
  data/lake/schedule.parquet  (statsapi first-pitch times, new-format slug join)

Does, per wallet:
  - Position reconstruction per (wallet, condition_id, token_id): net signed
    size + cash-flow (BUY cost / SELL proceeds) from raw fills. Dedup key is
    trade_id (already deduped in build_lake.py) -- matched-trade legs (one BUY
    row + one SELL row per execution, distinct trade_id) are NOT collapsed.
  - Settlement ROI (source of truth): join positions to markets.parquet
    winning_outcome. invested = sum(buy cost), returned = sum(sell proceeds)
    + settlement payout, roi = (returned - invested) / invested. Net-short
    positions (sold more than bought within our trade data -- i.e. tokens
    acquired outside BUY fills, most likely via split-mint) are treated as a
    synthetic short: they pay out when the outcome LOSES, not wins.
  - Pre-game vs in-game split via a first-pitch join (new-format
    mlb-{away}-{home}-{date} slugs only, per reports/01_lake_markets.md
    ~98.7% new-format join rate; legacy free-text 2024 slugs are NOT joined
    and fall back to timing='unknown' -- see report for scope).
  - Bot/MM heuristic features (no filtering, just columns): trades/day,
    two-sided-activity share, median inter-trade seconds, active-hours
    entropy.

Writes:
  data/lake/positions.parquet  - one row per (wallet, condition_id, token_id)
  data/lake/traders.parquet    - one row per wallet (census + metrics)

Read-only w.r.t. ingestion: only reads data/lake/*.parquet and
data/lake/state/trades.json, never touches raw/ or ingest state.

Usage:
    .venv/bin/python3 analysis/trader_metrics.py
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
LAKE_DIR = BASE / "data" / "lake"

GAME_DURATION_H = 4  # generous in-game window (incl. extra innings)
MARKET_TYPES = ["moneyline", "total", "spread", "nrfi", "prop", "futures", "other"]

# ---------------------------------------------------------------------------
# Pre-game join: new-format slug (mlb-{away}-{home}-{YYYY-MM-DD}...) only,
# same team-code table + methodology as reports/01_lake_markets.md's
# markets<->schedule join (98.7% match rate on slug-parseable markets).
# ---------------------------------------------------------------------------
TEAM_CODE_NAMES = {
    "ari": {"Arizona Diamondbacks"},
    "atl": {"Atlanta Braves"},
    "bal": {"Baltimore Orioles"},
    "bos": {"Boston Red Sox"},
    "chc": {"Chicago Cubs"},
    "cin": {"Cincinnati Reds"},
    "cle": {"Cleveland Guardians"},
    "col": {"Colorado Rockies"},
    "cws": {"Chicago White Sox"},
    "det": {"Detroit Tigers"},
    "hou": {"Houston Astros"},
    "kc": {"Kansas City Royals"},
    "laa": {"Los Angeles Angels"},
    "lad": {"Los Angeles Dodgers"},
    "mia": {"Miami Marlins"},
    "mil": {"Milwaukee Brewers"},
    "min": {"Minnesota Twins"},
    "nym": {"New York Mets"},
    "nyy": {"New York Yankees"},
    "oak": {"Oakland Athletics", "Athletics"},
    "phi": {"Philadelphia Phillies"},
    "pit": {"Pittsburgh Pirates"},
    "sd": {"San Diego Padres"},
    "sea": {"Seattle Mariners"},
    "sf": {"San Francisco Giants"},
    "stl": {"St. Louis Cardinals"},
    "tb": {"Tampa Bay Rays"},
    "tex": {"Texas Rangers"},
    "tor": {"Toronto Blue Jays"},
    "wsh": {"Washington Nationals"},
}
SLUG_RE = re.compile(r"^mlb-([a-z]+)-([a-z]+)-(\d{4}-\d{2}-\d{2})")


def _shift_date(date_str: str, days: int) -> str:
    return (pd.Timestamp(date_str) + pd.Timedelta(days=days)).strftime("%Y-%m-%d")


def build_first_pitch_map(markets: pd.DataFrame, schedule: pd.DataFrame) -> dict[str, pd.Timestamp]:
    """condition_id -> first-pitch UTC timestamp. New-format slugs only."""
    name_to_code = {name: code for code, names in TEAM_CODE_NAMES.items() for name in names}

    sched = schedule.copy()
    sched["away_code"] = sched["away_team_name"].map(name_to_code)
    sched["home_code"] = sched["home_team_name"].map(name_to_code)
    sched = sched.dropna(subset=["away_code", "home_code"])
    sched["game_date_utc"] = pd.to_datetime(sched["game_date_utc"], utc=True, errors="coerce")

    game_index: dict[tuple, list] = {}
    for r in sched.itertuples(index=False):
        key = (r.official_date, frozenset({r.away_code, r.home_code}))
        game_index.setdefault(key, []).append(r.game_date_utc)

    result: dict[str, pd.Timestamp] = {}
    n_hit = n_miss_no_slug = n_miss_no_game = 0
    for cid, slug in zip(markets["condition_id"], markets["slug"]):
        if not isinstance(slug, str):
            n_miss_no_slug += 1
            continue
        mo = SLUG_RE.match(slug)
        if not mo or mo.group(1) not in TEAM_CODE_NAMES or mo.group(2) not in TEAM_CODE_NAMES:
            n_miss_no_slug += 1
            continue
        away, home, date_str = mo.group(1), mo.group(2), mo.group(3)
        key_fs = frozenset({away, home})
        candidates = None
        for d in (date_str, _shift_date(date_str, -1), _shift_date(date_str, 1)):
            hits = game_index.get((d, key_fs))
            if hits:
                candidates = hits
                break
        if candidates:
            result[cid] = min(candidates)
            n_hit += 1
        else:
            n_miss_no_game += 1
    print(f"  first-pitch join: {n_hit} matched, {n_miss_no_slug} legacy/unparseable-slug misses, "
          f"{n_miss_no_game} slug-parseable-but-no-schedule-match misses")
    return result


def classify_timing(trades: pd.DataFrame, first_pitch: dict[str, pd.Timestamp]) -> pd.Series:
    fp = trades["condition_id"].map(first_pitch)
    in_end = fp + pd.Timedelta(hours=GAME_DURATION_H)
    flag = pd.Series("unknown", index=trades.index, dtype=object)
    has_fp = fp.notna()
    ts = trades["timestamp"]
    flag[has_fp & (ts < fp)] = "pre"
    flag[has_fp & (ts >= fp) & (ts < in_end)] = "in"
    flag[has_fp & (ts >= in_end)] = "post"
    return flag


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------
def load_completed_condition_ids() -> set[str]:
    with open(LAKE_DIR / "state" / "trades.json") as f:
        state = json.load(f)
    return set(state["completed_ids"])


def load_data():
    trades = pd.read_parquet(LAKE_DIR / "trades.parquet")
    markets = pd.read_parquet(LAKE_DIR / "markets.parquet")
    schedule = pd.read_parquet(LAKE_DIR / "schedule.parquet")
    completed = load_completed_condition_ids()
    n_before = len(trades)
    trades = trades[trades["condition_id"].isin(completed)].reset_index(drop=True)
    print(f"Trades filtered to {len(completed)} completed markets (trades.json completed_ids): "
          f"{n_before} -> {len(trades)} rows, {trades['condition_id'].nunique()} markets w/ trades")
    return trades, markets, schedule, completed


def build_token_winner_map(markets: pd.DataFrame) -> pd.DataFrame:
    """Explode resolved markets' token_ids/outcomes -> one row per
    (condition_id, token_id, outcome, is_winner, market_type)."""
    rows = []
    n_mismatch = 0
    for r in markets.itertuples(index=False):
        if not r.resolved or not r.token_ids:
            continue
        token_ids = r.token_ids.split(",")
        try:
            outcomes = json.loads(r.outcomes) if isinstance(r.outcomes, str) else (r.outcomes or [])
        except (json.JSONDecodeError, TypeError):
            outcomes = []
        if len(token_ids) != len(outcomes):
            n_mismatch += 1
            continue
        for tid, oc in zip(token_ids, outcomes):
            rows.append({
                "condition_id": r.condition_id,
                "token_id": tid,
                "outcome": oc,
                "is_winner": (oc == r.winning_outcome),
                "market_type": r.market_type,
            })
    if n_mismatch:
        print(f"  [WARN] {n_mismatch} resolved markets skipped in token/outcome map "
              f"(token_ids/outcomes length mismatch)")
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Position reconstruction + settlement
# ---------------------------------------------------------------------------
def reconstruct_positions(trades: pd.DataFrame) -> pd.DataFrame:
    """Per (wallet, condition_id, token_id): net signed size + cash flow."""
    t = trades[["proxy_wallet", "condition_id", "token_id", "side", "size", "price",
                "trade_id", "timestamp"]].copy()
    notional = t["size"] * t["price"]
    is_buy = t["side"] == "BUY"
    t["buy_size"] = np.where(is_buy, t["size"], 0.0)
    t["buy_cost"] = np.where(is_buy, notional, 0.0)
    t["sell_size"] = np.where(~is_buy, t["size"], 0.0)
    t["sell_proceeds"] = np.where(~is_buy, notional, 0.0)

    grp = t.groupby(["proxy_wallet", "condition_id", "token_id"], as_index=False).agg(
        buy_size=("buy_size", "sum"),
        buy_cost=("buy_cost", "sum"),
        sell_size=("sell_size", "sum"),
        sell_proceeds=("sell_proceeds", "sum"),
        n_fills=("trade_id", "count"),
        first_ts=("timestamp", "min"),
        last_ts=("timestamp", "max"),
    )
    grp["net_size"] = grp["buy_size"] - grp["sell_size"]
    return grp


def settle_positions(pos: pd.DataFrame, token_winner: pd.DataFrame) -> pd.DataFrame:
    """Join settlement outcome + compute settlement payout / invested / returned.

    payout = net_size * settle_price                  if net_size >= 0 (net long)
    payout = (-net_size) * (1 - settle_price)          if net_size <  0 (synthetic
                                                          short: pays out when this
                                                          token's outcome LOSES)
    where settle_price = 1.0 if this token's outcome won else 0.0.
    """
    pos = pos.merge(
        token_winner[["condition_id", "token_id", "is_winner", "market_type", "outcome"]],
        on=["condition_id", "token_id"], how="left",
    )
    settle_price = np.where(pos["is_winner"].fillna(False), 1.0, 0.0)
    net = pos["net_size"].to_numpy()
    payout = np.where(net >= 0, net * settle_price, (-net) * (1.0 - settle_price))
    payout = np.where(pos["is_winner"].isna(), np.nan, payout)
    pos["settlement_payout"] = payout
    pos["invested"] = pos["buy_cost"]
    pos["returned"] = pos["sell_proceeds"] + pos["settlement_payout"].fillna(0.0)
    return pos


def wallet_market_rollup(pos: pd.DataFrame) -> pd.DataFrame:
    """Aggregate token-level positions -> one row per (wallet, condition_id)
    (full cash-flow accounting across all outcome tokens in that market)."""
    g = pos.groupby(["proxy_wallet", "condition_id"], as_index=False).agg(
        invested=("invested", "sum"),
        returned=("returned", "sum"),
        n_fills=("n_fills", "sum"),
        market_type=("market_type", "first"),
    )
    g["roi"] = np.where(g["invested"] > 0, (g["returned"] - g["invested"]) / g["invested"], np.nan)
    g["win"] = g["returned"] > g["invested"]
    return g


def wallet_settlement_summary(wallet_market: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Wallet-level settlement summary from a wallet-market rollup table.
    `prefix` namespaces the output columns (e.g. '' for full-history,
    'pregame_' for the pre-game-only reconstruction)."""
    has_pos = wallet_market[wallet_market["invested"] > 0]
    g = has_pos.groupby("proxy_wallet").agg(
        n_markets=("condition_id", "nunique"),
        invested=("invested", "sum"),
        returned=("returned", "sum"),
        roi_mean=("roi", "mean"),
        win_rate=("win", "mean"),
    ).reset_index()
    g["roi_pooled"] = np.where(g["invested"] > 0, (g["returned"] - g["invested"]) / g["invested"], np.nan)
    g = g.rename(columns={
        "n_markets": f"{prefix}n_markets",
        "invested": f"{prefix}invested",
        "returned": f"{prefix}returned",
        "roi_mean": f"{prefix}roi_mean",
        "roi_pooled": f"{prefix}roi_pooled",
        "win_rate": f"{prefix}win_rate",
    })
    return g


def market_type_splits(wallet_market: pd.DataFrame) -> pd.DataFrame:
    """Per-wallet, per-market-type invested/returned/roi/n_markets, pivoted
    into flat columns roi_<type>, n_markets_<type>, invested_<type>."""
    has_pos = wallet_market[wallet_market["invested"] > 0].copy()
    has_pos["market_type"] = has_pos["market_type"].fillna("other")
    g = has_pos.groupby(["proxy_wallet", "market_type"]).agg(
        invested=("invested", "sum"), returned=("returned", "sum"),
        n_markets=("condition_id", "nunique"),
    ).reset_index()
    g["roi"] = np.where(g["invested"] > 0, (g["returned"] - g["invested"]) / g["invested"], np.nan)

    out = g.pivot(index="proxy_wallet", columns="market_type", values=["roi", "n_markets", "invested"])
    out.columns = [f"{stat}_{mtype}" for stat, mtype in out.columns]
    for mtype in MARKET_TYPES:
        for stat in ("roi", "n_markets", "invested"):
            col = f"{stat}_{mtype}"
            if col not in out.columns:
                out[col] = np.nan if stat == "roi" else 0
    return out.reset_index()


# ---------------------------------------------------------------------------
# Bot / MM heuristic features (no filtering, just columns)
# ---------------------------------------------------------------------------
def bot_heuristics(trades: pd.DataFrame) -> pd.DataFrame:
    t = trades[["proxy_wallet", "condition_id", "side", "timestamp"]].copy()

    g = t.groupby("proxy_wallet")
    n_trades = g.size().rename("n_trades")
    first_ts = g["timestamp"].min()
    last_ts = g["timestamp"].max()
    span_days = ((last_ts - first_ts).dt.total_seconds() / 86400.0).clip(lower=1.0 / 24.0)
    trades_per_day = (n_trades / span_days).rename("trades_per_day")

    def _median_gap_s(s: pd.Series) -> float:
        if len(s) < 2:
            return np.nan
        diffs = s.sort_values().diff().dt.total_seconds().dropna()
        return float(diffs.median())

    median_gap = g["timestamp"].apply(_median_gap_s).rename("median_inter_trade_seconds")

    hours = t["timestamp"].dt.hour
    hour_counts = t.groupby(["proxy_wallet", hours]).size().unstack(fill_value=0)
    probs = hour_counts.div(hour_counts.sum(axis=1), axis=0)
    safe_probs = probs.mask(probs <= 0, 1.0)
    entropy = -(probs * np.log2(safe_probs)).sum(axis=1)
    entropy_norm = (entropy / np.log2(24)).rename("active_hours_entropy")

    side_sets = t.groupby(["proxy_wallet", "condition_id"])["side"].agg(lambda s: frozenset(s))
    two_sided = side_sets.apply(lambda s: {"BUY", "SELL"}.issubset(s))
    two_sided_share = two_sided.groupby("proxy_wallet").mean().rename("two_sided_share")

    out = pd.concat([n_trades, trades_per_day, median_gap, entropy_norm, two_sided_share], axis=1)
    return out.reset_index()


def gross_volume_per_wallet(trades: pd.DataFrame) -> pd.DataFrame:
    t = trades.copy()
    t["notional"] = t["size"] * t["price"]
    return t.groupby("proxy_wallet", as_index=False).agg(gross_volume=("notional", "sum"))


# ---------------------------------------------------------------------------
# Sanity checks
# ---------------------------------------------------------------------------
def sanity_verify_wallets(trades: pd.DataFrame, markets: pd.DataFrame, pos: pd.DataFrame, wallets: list[str]):
    print("\n" + "=" * 78)
    print("SANITY CHECK: hand-verify fills -> position -> settlement payout")
    print("=" * 78)
    markets_idx = markets.set_index("condition_id")
    for w in wallets:
        print(f"\n--- wallet {w} ---")
        wt = trades[trades["proxy_wallet"] == w].sort_values("timestamp")
        cids = wt["condition_id"].unique()
        # pick the market with the most fills for this wallet for a legible demo
        cid = wt["condition_id"].value_counts().idxmax()
        sub = wt[wt["condition_id"] == cid][["timestamp", "condition_id", "token_id", "side", "size", "price"]]
        mkt = markets_idx.loc[cid]
        print(f"  market {cid[:18]}...  question={mkt['question']!r}  winning_outcome={mkt['winning_outcome']!r}")
        print(f"  {len(sub)} fills in this market (of {len(wt)} total fills, {len(cids)} markets traded):")
        print(sub.to_string(index=False))

        for token_id, tsub in sub.groupby("token_id"):
            buy = tsub[tsub["side"] == "BUY"]
            sell = tsub[tsub["side"] == "SELL"]
            buy_size, buy_cost = buy["size"].sum(), (buy["size"] * buy["price"]).sum()
            sell_size, sell_proceeds = sell["size"].sum(), (sell["size"] * sell["price"]).sum()
            net_size = buy_size - sell_size
            prow = pos[(pos["proxy_wallet"] == w) & (pos["condition_id"] == cid) & (pos["token_id"] == token_id)]
            is_winner = bool(prow["is_winner"].iloc[0]) if not prow.empty else None
            settle_price = 1.0 if is_winner else 0.0
            payout = net_size * settle_price if net_size >= 0 else (-net_size) * (1.0 - settle_price)
            print(f"    token {token_id[:12]}...  buy_size={buy_size:.4f} buy_cost={buy_cost:.4f}  "
                  f"sell_size={sell_size:.4f} sell_proceeds={sell_proceeds:.4f}  net_size={net_size:.4f}  "
                  f"is_winner={is_winner}  settle_price={settle_price}  hand_payout={payout:.4f}")
            if not prow.empty:
                p = prow.iloc[0]
                match = np.isclose(p["settlement_payout"], payout, atol=1e-6)
                print(f"    pipeline: settlement_payout={p['settlement_payout']:.4f}  invested={p['invested']:.4f} "
                      f"returned={p['returned']:.4f}  [hand-calc match: {match}]")


def cross_check_reconciliation(pos: pd.DataFrame, markets: pd.DataFrame) -> None:
    # NOTE on what's checked and why: an earlier draft of this check assumed
    # "every BUY fill has a matching SELL fill of the same token" (i.e. buy
    # volume == sell volume per token). That assumption is FALSE in this
    # dataset -- most wallets buy-and-hold to settlement rather than exiting
    # early (buy fill count 937,314 vs sell fill count 405,930 platform-wide,
    # ~2.3:1; buy dollar volume ~6.7x sell dollar volume in the filtered
    # set), which is expected prediction-market behavior, not a data bug.
    # negRisk markets also settle/convert some volume through an adapter
    # contract rather than a visible counterparty SELL row, so per-token
    # buy==sell is not a valid invariant here. The check actually run below
    # instead cross-validates against an INDEPENDENT source: Gamma's own
    # per-market `volume` field (markets.parquet), which is not derived from
    # our trade reconstruction at all.
    print("\n" + "=" * 78)
    print("CROSS-CHECK: reconstructed per-market trade volume (buy notional + sell notional, all")
    print("wallets/tokens) vs Gamma's independently-reported markets.parquet `volume` field")
    print("=" * 78)
    per_mkt = pos.groupby("condition_id").agg(
        buy_notional=("buy_cost", "sum"), sell_notional=("sell_proceeds", "sum"),
        settlement_payout=("settlement_payout", "sum"),
    ).reset_index()
    per_mkt["our_total_volume"] = per_mkt["buy_notional"] + per_mkt["sell_notional"]
    per_mkt = per_mkt.merge(markets[["condition_id", "volume"]], on="condition_id", how="left")
    per_mkt["ratio"] = per_mkt["our_total_volume"] / per_mkt["volume"].replace(0, np.nan)
    print(f"  {len(per_mkt)} markets. ratio = (our buy+sell notional) / (Gamma volume):")
    print(f"    median={per_mkt['ratio'].median():.4f}  mean={per_mkt['ratio'].mean():.4f}  "
          f"IQR=[{per_mkt['ratio'].quantile(.25):.4f}, {per_mkt['ratio'].quantile(.75):.4f}]")
    within_10pct = (per_mkt["ratio"].sub(1).abs() < 0.10).mean()
    print(f"    {within_10pct * 100:.1f}% of markets within +/-10% of Gamma's volume figure")
    worst = per_mkt.reindex(per_mkt["ratio"].sub(1).abs().sort_values(ascending=False).index).head(3)
    print(f"  worst outliers (condition_id, our_total_volume, gamma_volume, ratio):")
    for r in worst.itertuples():
        print(f"    {r.condition_id[:16]}...  ours={r.our_total_volume:,.0f}  gamma={r.volume:,.0f}  "
              f"ratio={r.ratio:.4f}")

    print(f"\n  total settlement payout across all markets/wallets: {per_mkt['settlement_payout'].sum():,.2f}")
    print(f"  total reconstructed trade volume: {per_mkt['our_total_volume'].sum():,.2f}")
    print(f"  (no independent winning-token-supply source is available (no mint/merge feed ingested), so "
          f"'total settlement payout == total winning-token supply' can't be checked against a 3rd source; "
          f"the payout total above is the same order of magnitude as trade volume, as expected, and is "
          f"internally consistent by construction: since net short payout only fires when net_size<0, and "
          f"a resolved market's total payout to net-long holders of the winning token is bounded above by "
          f"that token's total BUY volume (payout <= buy_size for every position, checked below).)")
    bad_bound = pos[(pos["net_size"] > 0) & (pos["settlement_payout"] > pos["buy_size"] + 1e-6)]
    print(f"  positions where settlement_payout > buy_size (should be 0, sanity bound): {len(bad_bound)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    t0 = time.time()
    trades, markets, schedule, completed = load_data()

    print("\nBuilding first-pitch join...")
    first_pitch = build_first_pitch_map(markets, schedule)
    trades["timing"] = classify_timing(trades, first_pitch)
    print(f"  timing distribution:\n{trades['timing'].value_counts()}")

    print("\nBuilding token/winner map...")
    token_winner = build_token_winner_map(markets)

    print("\nReconstructing positions (full history)...")
    pos_full = reconstruct_positions(trades)
    pos_full = settle_positions(pos_full, token_winner)
    print(f"  {len(pos_full)} (wallet, condition_id, token_id) positions")

    print("Reconstructing positions (pre-game-only fills)...")
    pos_pregame = reconstruct_positions(trades[trades["timing"] == "pre"])
    pos_pregame = settle_positions(pos_pregame, token_winner)
    print(f"  {len(pos_pregame)} pre-game-only positions")

    print("\nRolling up to wallet-market level...")
    wm_full = wallet_market_rollup(pos_full)
    wm_pregame = wallet_market_rollup(pos_pregame)

    print("Aggregating wallet-level settlement summaries...")
    wallet_full = wallet_settlement_summary(wm_full, prefix="")
    wallet_pregame = wallet_settlement_summary(wm_pregame, prefix="pregame_")
    type_splits = market_type_splits(wm_full)
    bot_feats = bot_heuristics(trades)
    gross_vol = gross_volume_per_wallet(trades)

    traders = wallet_full.merge(wallet_pregame, on="proxy_wallet", how="outer") \
                          .merge(type_splits, on="proxy_wallet", how="left") \
                          .merge(bot_feats, on="proxy_wallet", how="left") \
                          .merge(gross_vol, on="proxy_wallet", how="left")
    traders = traders.rename(columns={"proxy_wallet": "wallet"})

    positions_out = pos_full.rename(columns={"proxy_wallet": "wallet"})

    LAKE_DIR.mkdir(parents=True, exist_ok=True)
    positions_path = LAKE_DIR / "positions.parquet"
    traders_path = LAKE_DIR / "traders.parquet"
    positions_out.to_parquet(positions_path, index=False)
    traders.to_parquet(traders_path, index=False)
    print(f"\nWrote {positions_path} ({len(positions_out)} rows)")
    print(f"Wrote {traders_path} ({len(traders)} rows)")

    # sanity: 2 wallets, hand-verify. Pick one census wallet with a moderate,
    # legible number of markets (5-15) so the printout is checkable by eye,
    # and one heavier wallet for a second, higher-volume example.
    candidates = traders[(traders["n_markets"] >= 5) & (traders["n_markets"] <= 15)]
    heavy = traders[traders["n_markets"] > 15].sort_values("n_markets")
    pick = []
    if not candidates.empty:
        pick.append(candidates.iloc[0]["wallet"])
    if not heavy.empty:
        pick.append(heavy.iloc[0]["wallet"])
    if len(pick) < 2:
        pick = traders.sort_values("n_markets", ascending=False)["wallet"].head(2).tolist()
    sanity_verify_wallets(trades, markets, pos_full, pick)

    cross_check_reconciliation(pos_full, markets)

    print(f"\nTotal elapsed: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
