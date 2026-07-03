#!/usr/bin/env python3
"""Phase 3 step 1a-bis: extend the orderbook-ingest sample with alternate
totals lines.

Motivation (crowd_baseline_diagnostic in edge_rules.py, first run): the
crowd's zero-slippage edge in the orderbook-retention window is
concentrated ENTIRELY in the totals lines that select_orderbook_targets.py
deduped away (+6.3% [+4.3%, +8.2%] on the 5,340 excluded alternate lines vs
-0.1% on the kept max-stake lines). The dedup assumed one line per game
carries the signal; Phase 2's all-wallets baseline actually bets every line
as a separate market. Executability on those THIN alternate books is
exactly the make-or-break question, so this script appends a random sample
of them (stratified only by random seed, capped to stay near the ingest
budget) to data/lake/orderbook_targets.parquet with cohort='alt_line'
(existing rows are back-filled cohort='primary').

Idempotent: running twice will not duplicate rows (existing alt_line
condition_ids are excluded from re-sampling).

Usage:
    .venv/bin/python3 analysis/add_alt_line_targets.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
LAKE_DIR = BASE / "data" / "lake"
sys.path.insert(0, str(BASE / "analysis"))
import trader_metrics as tm  # noqa: E402
import select_orderbook_targets as sot  # noqa: E402

ALT_SAMPLE_N = 1800
SAMPLE_SEED = 1
RETENTION_FLOOR = pd.Timestamp("2026-03-26", tz="UTC")  # intel 572 retention -- see ingest_orderbooks.py


def main() -> None:
    print("Loading lake tables...")
    markets = pd.read_parquet(LAKE_DIR / "markets.parquet")
    schedule = pd.read_parquet(LAKE_DIR / "schedule.parquet")
    candles = pd.read_parquet(LAKE_DIR / "candles.parquet")
    fills = pd.read_parquet(LAKE_DIR / "pregame_fills.parquet")
    targets = pd.read_parquet(LAKE_DIR / "orderbook_targets.parquet")
    if "cohort" not in targets.columns:
        targets["cohort"] = "primary"

    first_pitch = tm.build_first_pitch_map(markets, schedule)
    closing_price = sot.build_closing_price_map(candles, markets, first_pitch)

    f = fills[(fills["season"] == 2026) & (fills["market_type"] == "total")].copy()
    per_tok = f.groupby(["condition_id", "token_id"], as_index=False).agg(
        stake=("stake_signed", "sum"), is_winner=("is_winner", "first"),
        market_type=("market_type", "first"), season=("season", "first"))
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

    slug_map = markets.set_index("condition_id")["slug"]
    top["slug"] = top["condition_id"].map(slug_map)
    top["first_pitch"] = pd.to_datetime(top["condition_id"].map(first_pitch), utc=True)
    top["entry_price"] = top["token_id"].map(closing_price)
    top = top.dropna(subset=["first_pitch"])
    top = top[top["first_pitch"] >= RETENTION_FLOOR]

    already = set(targets["condition_id"])
    alt = top[~top["condition_id"].isin(already)].copy()
    print(f"alternate-line candidates in retention window not already targeted: {len(alt)}")

    if len(alt) > ALT_SAMPLE_N:
        alt = alt.sample(n=ALT_SAMPLE_N, random_state=SAMPLE_SEED)
    print(f"sampled: {len(alt)}")

    alt = alt[["condition_id", "token_id", "market_type", "season", "first_pitch",
                "is_winner", "signal_strength", "abs_stake", "entry_price", "slug"]].copy()
    alt["cohort"] = "alt_line"

    out = pd.concat([targets, alt], ignore_index=True)
    out = out.sort_values(["season", "first_pitch"]).reset_index(drop=True)
    out_path = LAKE_DIR / "orderbook_targets.parquet"
    out.to_parquet(out_path, index=False)
    print(f"Wrote {out_path}: {len(out)} total targets "
          f"({(out['cohort'] == 'primary').sum()} primary, {(out['cohort'] == 'alt_line').sum()} alt_line)")


if __name__ == "__main__":
    main()
