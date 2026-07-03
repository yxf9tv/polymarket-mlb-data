#!/usr/bin/env python3
"""Phase 3 step 1a: pick the orderbook-ingest sample.

RESEARCH_PLAN.md Phase 3 sample design: "all 2026 markets that had a crowd
signal AND are totals or moneyline ... plus a 500-market 2025 sample."

The crowd signal reused here is the SAME one Phase 2 found the edge in
(reports/06_portfolio.md baseline_all_wallets_equal): every wallet that
traded pre-game, weight=1, chosen_token = the outcome with the highest
absolute weighted net stake per market.

Budget guard: `total` market_type in the lake is one row PER O/U LINE, not
per game (avg 5.3 lines/game in 2026). Sampling every line would blow the
~3-6k call budget for ~1,250 games. Since only one line typically carries
real two-sided depth, we deduplicate totals to ONE market per game -- the
line with the largest crowd abs-stake (i.e. the line that actually traded,
not a quoted-but-untouched alternate line). Moneyline is already one row per
game so needs no dedup.

Output: data/lake/orderbook_targets.parquet
  condition_id, token_id (crowd's chosen/signal-side token), market_type,
  season, first_pitch (UTC ts), is_winner (of chosen_token), signal_strength,
  abs_stake, entry_price (last pre-game candle close for chosen_token).

Usage:
    .venv/bin/python3 analysis/select_orderbook_targets.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
LAKE_DIR = BASE / "data" / "lake"
sys.path.insert(0, str(BASE / "analysis"))
import trader_metrics as tm  # noqa: E402

MARKET_TYPES = {"total", "moneyline"}
SAMPLE_2025_N = 500
SAMPLE_SEED = 0
SLUG_GAME_RE = re.compile(r"^(mlb-[a-z]+-[a-z]+-\d{4}-\d{2}-\d{2})")


def _game_key(slug) -> str | None:
    if not isinstance(slug, str):
        return None
    mo = SLUG_GAME_RE.match(slug)
    return mo.group(1) if mo else None


def build_closing_price_map(candles: pd.DataFrame, markets: pd.DataFrame, first_pitch: dict) -> pd.Series:
    """Same logic as portfolio_selection.py::build_closing_price_map."""
    tok_to_cid = {}
    for r in markets.itertuples(index=False):
        if isinstance(r.token_ids, str) and r.token_ids:
            for tid in r.token_ids.split(","):
                tok_to_cid[tid] = r.condition_id
    c = candles.copy()
    c["condition_id_fp"] = c["token_id"].map(tok_to_cid)
    c["cutoff"] = c["condition_id_fp"].map(first_pitch)
    c = c.dropna(subset=["cutoff"])
    c["cutoff"] = pd.to_datetime(c["cutoff"], utc=True)
    c["ts"] = pd.to_datetime(c["ts"], utc=True)
    c = c[c["ts"] < c["cutoff"]].sort_values(["token_id", "ts"])
    return c.groupby("token_id").tail(1).set_index("token_id")["price"]


def main() -> None:
    print("Loading lake tables...")
    markets = pd.read_parquet(LAKE_DIR / "markets.parquet")
    schedule = pd.read_parquet(LAKE_DIR / "schedule.parquet")
    candles = pd.read_parquet(LAKE_DIR / "candles.parquet")
    fills = pd.read_parquet(LAKE_DIR / "pregame_fills.parquet")

    first_pitch = tm.build_first_pitch_map(markets, schedule)
    closing_price = build_closing_price_map(candles, markets, first_pitch)

    fills = fills[fills["market_type"].isin(MARKET_TYPES)].copy()
    print(f"pre-game fills, total+moneyline: {len(fills)} rows, "
          f"{fills['condition_id'].nunique()} markets")

    # crowd signal: weight=1 for every wallet, pick per-market chosen token
    per_tok = fills.groupby(["condition_id", "token_id"], as_index=False).agg(
        stake=("stake_signed", "sum"), is_winner=("is_winner", "first"),
        market_type=("market_type", "first"), season=("season", "first"),
    )
    per_tok["abs_stake"] = per_tok["stake"].abs()
    per_tok = per_tok.sort_values(["condition_id", "abs_stake"], ascending=[True, False])
    top = per_tok.groupby("condition_id", as_index=False).nth(0)
    second_abs = per_tok.groupby("condition_id")["abs_stake"].nth(1)
    denom = per_tok.groupby("condition_id")["abs_stake"].sum()
    top = top.set_index("condition_id")
    top["second_abs"] = second_abs.reindex(top.index).fillna(0.0)
    top["denom"] = denom.reindex(top.index)
    top["signal_strength"] = np.where(top["denom"] > 0, (top["abs_stake"] - top["second_abs"]) / top["denom"], np.nan)
    top = top.reset_index()
    print(f"crowd-signal markets (total+moneyline): {len(top)}")

    # attach slug/first_pitch/entry_price
    slug_map = markets.set_index("condition_id")["slug"]
    top["slug"] = top["condition_id"].map(slug_map)
    top["first_pitch"] = top["condition_id"].map(first_pitch)
    top["entry_price"] = top["token_id"].map(closing_price)
    top = top.dropna(subset=["first_pitch"]).copy()
    print(f"  after requiring a matched first-pitch time: {len(top)}")

    # dedup totals to one line per game: keep max abs_stake
    top["game_key"] = top["slug"].apply(_game_key)
    is_total = top["market_type"] == "total"
    totals = top[is_total].copy()
    n_totals_before = len(totals)
    totals_ded = (totals.sort_values("abs_stake", ascending=False)
                  .drop_duplicates(subset=["game_key"], keep="first"))
    print(f"  totals dedup (1 line/game, max crowd stake): {n_totals_before} -> {len(totals_ded)}")
    moneyline = top[~is_total].copy()
    target = pd.concat([moneyline, totals_ded], ignore_index=True)

    # season split: keep all 2026, sample 500 from 2025 (stratified by market_type)
    t2026 = target[target["season"] == 2026].copy()
    t2025 = target[target["season"] == 2025].copy()
    other = target[~target["season"].isin([2025, 2026])].copy()
    print(f"2026 target markets (all kept): {len(t2026)}")
    print(f"2025 candidate markets (pre-sample): {len(t2025)} "
          f"(moneyline={int((t2025['market_type'] == 'moneyline').sum())}, "
          f"total={int((t2025['market_type'] == 'total').sum())})")

    if len(t2025) > SAMPLE_2025_N:
        frac = SAMPLE_2025_N / len(t2025)
        parts = [d.sample(frac=frac, random_state=SAMPLE_SEED) for _, d in t2025.groupby("market_type")]
        t2025_sample = pd.concat(parts, ignore_index=True)
        # top up/trim to exactly SAMPLE_2025_N due to rounding
        if len(t2025_sample) > SAMPLE_2025_N:
            t2025_sample = t2025_sample.sample(n=SAMPLE_2025_N, random_state=SAMPLE_SEED)
        elif len(t2025_sample) < SAMPLE_2025_N:
            remainder = t2025.drop(t2025_sample.index, errors="ignore")
            top_up = remainder.sample(n=min(SAMPLE_2025_N - len(t2025_sample), len(remainder)), random_state=SAMPLE_SEED)
            t2025_sample = pd.concat([t2025_sample, top_up], ignore_index=True)
    else:
        t2025_sample = t2025
    print(f"2025 sample kept: {len(t2025_sample)} "
          f"(moneyline={int((t2025_sample['market_type'] == 'moneyline').sum())}, "
          f"total={int((t2025_sample['market_type'] == 'total').sum())})")

    if len(other):
        print(f"NOTE: {len(other)} target markets outside 2025/2026 dropped (out of Phase-3 scope).")

    final = pd.concat([t2026, t2025_sample], ignore_index=True)
    final = final[["condition_id", "token_id", "market_type", "season", "first_pitch",
                    "is_winner", "signal_strength", "abs_stake", "entry_price", "slug"]]
    final["first_pitch"] = pd.to_datetime(final["first_pitch"], utc=True)
    final = final.sort_values(["season", "first_pitch"]).reset_index(drop=True)

    LAKE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = LAKE_DIR / "orderbook_targets.parquet"
    final.to_parquet(out_path, index=False)
    print(f"\nWrote {out_path}: {len(final)} orderbook-ingest target markets.")
    print(final.groupby(["season", "market_type"]).size())
    n_missing_price = final["entry_price"].isna().sum()
    print(f"entry_price missing for {n_missing_price}/{len(final)} targets (still ingested; excluded from ROI calcs later)")


if __name__ == "__main__":
    main()
