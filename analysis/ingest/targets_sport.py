#!/usr/bin/env python3
"""Target-market selection for the NFL/NBA trades sweeps (mirrors targets.py's
selection + ordering logic for MLB, generalized to two sports whose `season`
column has different dtypes/semantics).

Scope (final, per the research-program handoff for this phase):
  Common: resolved == True, volume > 0.
  NFL: season in {2024, 2025, 2026} (native season is float64, e.g. 2024.0,
       with some NaN - dropped before the isin compare). All market types.
       -> 15,494 markets.
  NBA: season in {"2024-25", "2025-26", "2026-27"} (native season is a
       string label, with some NaN). For season == "2025-26" ONLY, further
       restrict to market_type in {moneyline, spread, total} - the ~22.9k
       dropped props/futures/other for that season carry negligible volume
       (quality-over-quantity mandate). 2024-25 and 2026-27 keep all types.
       -> 2,990 + 19,383 + 474 = 22,847 markets.

Ingest ordering: season ascending, then volume descending within season -
same spec as targets.py::load_target_markets.
"""

from __future__ import annotations

import pandas as pd

from client import LAKE_DIR

NFL_SEASONS = [2024, 2025, 2026]
NBA_SEASONS = ["2024-25", "2025-26", "2026-27"]
NBA_RESTRICTED_SEASON = "2025-26"
NBA_RESTRICTED_MARKET_TYPES = ["moneyline", "spread", "total"]


def _load_nfl(limit: int | None) -> pd.DataFrame:
    df = pd.read_parquet(LAKE_DIR / "markets_nfl.parquet")
    df = df[(df["resolved"] == True) & (df["volume"] > 0)]  # noqa: E712
    df = df.dropna(subset=["season"])
    df = df[df["season"].isin(NFL_SEASONS)]
    df["season_dir"] = df["season"].astype(int).astype(str)
    return df


def _load_nba(limit: int | None) -> pd.DataFrame:
    df = pd.read_parquet(LAKE_DIR / "markets_nba.parquet")
    df = df[(df["resolved"] == True) & (df["volume"] > 0)]  # noqa: E712
    df = df.dropna(subset=["season"])
    df = df[df["season"].isin(NBA_SEASONS)]
    restricted_mask = df["season"] == NBA_RESTRICTED_SEASON
    keep = ~restricted_mask | df["market_type"].isin(NBA_RESTRICTED_MARKET_TYPES)
    df = df[keep]
    df["season_dir"] = df["season"].astype(str)
    return df


def load_target_markets_sport(sport: str, limit: int | None = None) -> pd.DataFrame:
    """Return the target-market DataFrame for `sport` ('nfl' or 'nba'):
    condition_id, season (native dtype), season_dir (filesystem-safe string),
    start_date, end_date, volume - ordered season ascending, then volume
    descending within season. `limit` caps the total row count after
    ordering (for smoke testing)."""
    if sport == "nfl":
        df = _load_nfl(limit)
    elif sport == "nba":
        df = _load_nba(limit)
    else:
        raise ValueError(f"unknown sport: {sport!r} (expected 'nfl' or 'nba')")

    df = df.sort_values(["season", "volume"], ascending=[True, False]).reset_index(drop=True)

    print(f"{sport} target universe: {len(df)} markets")
    for season, count in df.groupby("season_dir", sort=False).size().items():
        print(f"  season {season}: {count} markets")

    if limit:
        df = df.head(limit)
    return df


if __name__ == "__main__":
    import sys

    sport_arg = sys.argv[1] if len(sys.argv) > 1 else "nfl"
    load_target_markets_sport(sport_arg)
