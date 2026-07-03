#!/usr/bin/env python3
"""Shared target-market selection for the trades/candles sweeps.

The trades-sweep universe (per reports/01_lake_markets.md handoff): CLOB-era
(season >= 2024) resolved markets with volume > 0, 30,289 rows as of the
markets-lake build. Both ingest_trades.py and ingest_candles.py loop over
this same set, so the selection + ordering logic lives in one place.
"""

from __future__ import annotations

import pandas as pd

from client import LAKE_DIR

MIN_SEASON = 2024


def load_target_markets(seasons: list[int] | None = None, limit: int | None = None) -> pd.DataFrame:
    """Return the target-market DataFrame: season >= 2024, resolved, volume > 0.

    Ordered season ascending (2024 -> 2025 -> 2026 -> ...), then volume
    descending within season, per the ingestion-order spec. `seasons`
    restricts to a subset (e.g. [2024, 2025] for testing); `limit` caps the
    total row count after ordering (also for testing)."""
    df = pd.read_parquet(LAKE_DIR / "markets.parquet")
    df = df[(df["season"] >= MIN_SEASON) & (df["resolved"] == True) & (df["volume"] > 0)]  # noqa: E712
    if seasons:
        df = df[df["season"].isin(seasons)]
    df = df.sort_values(["season", "volume"], ascending=[True, False]).reset_index(drop=True)
    if limit:
        df = df.head(limit)
    return df


def parse_seasons_arg(args: list[str]) -> list[int] | None:
    """--seasons 2024,2025 -> [2024, 2025]; returns None if not present."""
    for i, a in enumerate(args):
        if a == "--seasons" and i + 1 < len(args):
            return [int(s) for s in args[i + 1].split(",") if s.strip()]
        if a.startswith("--seasons="):
            return [int(s) for s in a.split("=", 1)[1].split(",") if s.strip()]
    return None


def parse_limit_markets_arg(args: list[str]) -> int | None:
    for i, a in enumerate(args):
        if a == "--limit-markets" and i + 1 < len(args):
            return int(args[i + 1])
        if a.startswith("--limit-markets="):
            return int(a.split("=", 1)[1])
    return None


def parse_workers_arg(args: list[str], default: int) -> int:
    for i, a in enumerate(args):
        if a == "--workers" and i + 1 < len(args):
            return int(args[i + 1])
        if a.startswith("--workers="):
            return int(a.split("=", 1)[1])
    return default
