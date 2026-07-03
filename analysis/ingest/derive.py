#!/usr/bin/env python3
"""Shared derivation helpers for Gamma MLB market records: settlement winner,
season year, and market-type classification. Used by both ingest_markets.py
(cross-check sampling) and build_lake.py (parquet construction) so the logic
lives in exactly one place.
"""

from __future__ import annotations

import json
import re
from typing import Any

# ---------------------------------------------------------------------------
# Settlement winner: argmax(outcomePrices) if max > 0.99, else unresolved.
# AMM-era markets settle to prices like 0.9999992, not exactly 1.0 (probe P8).
# ---------------------------------------------------------------------------
def parse_outcomes_prices(market: dict) -> tuple[list[str], list[float]]:
    outcomes_raw = market.get("outcomes", "[]")
    prices_raw = market.get("outcomePrices", "[]")
    try:
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])
    except (json.JSONDecodeError, TypeError):
        outcomes = []
    try:
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else (prices_raw or [])
    except (json.JSONDecodeError, TypeError):
        prices = []
    try:
        prices_f = [float(p) for p in prices]
    except (TypeError, ValueError):
        prices_f = []
    return outcomes, prices_f


def derive_winner(market: dict) -> tuple[str | None, list[float]]:
    """Returns (winning_outcome or None, raw_prices)."""
    outcomes, prices = parse_outcomes_prices(market)
    if not outcomes or not prices or len(outcomes) != len(prices):
        return None, prices
    pairs = list(zip(outcomes, prices))
    best_outcome, best_price = max(pairs, key=lambda x: x[1])
    if best_price > 0.99:
        return best_outcome, prices
    return None, prices


def parse_token_ids(market: dict) -> list[str]:
    raw = market.get("clobTokenIds", "[]")
    try:
        return json.loads(raw) if isinstance(raw, str) else (raw or [])
    except (json.JSONDecodeError, TypeError):
        return []


# ---------------------------------------------------------------------------
# Season derivation: prefer endDate (market, then event) over startDate,
# since many rows carry a garbage startDate default of 2021-01-01T17:00:00Z
# (discovered in probe P1). Falls back to startDate / creationDate only when
# no endDate is present at all.
# ---------------------------------------------------------------------------
def _year_of(date_str: str | None) -> int | None:
    if not date_str or not isinstance(date_str, str) or len(date_str) < 4:
        return None
    try:
        return int(date_str[:4])
    except ValueError:
        return None


def derive_season(event: dict, market: dict) -> tuple[int | None, str]:
    """Returns (season_year, source_field_used)."""
    candidates = [
        ("market.endDate", market.get("endDate")),
        ("event.endDate", event.get("endDate")),
        ("market.startDate", market.get("startDate")),
        ("event.startDate", event.get("startDate")),
        ("event.creationDate", event.get("creationDate")),
    ]
    for source, val in candidates:
        year = _year_of(val)
        if year and 2015 <= year <= 2035:
            return year, source
    return None, "none"


# ---------------------------------------------------------------------------
# Market-type classification (guess) from slug + question text. Heuristic —
# not a ground-truth label. Buckets: moneyline / spread / total / nrfi /
# futures / prop / other.
# ---------------------------------------------------------------------------
_TOTAL_RE = re.compile(
    r"\btotal\b|over[/\s-]?under|o/u\b|combine[sd]?\s+for.*\bruns?\b", re.I
)
_SPREAD_RE = re.compile(r"\bspread\b|run\s*line|\brl\b", re.I)
_NRFI_RE = re.compile(
    r"\bnrfi\b|\byrfi\b|(?:no\s*run|run\s*scored).*first\s*inning", re.I
)
_FUTURES_RE = re.compile(
    r"world\s*series\s*champion|win\s+the\s+(?:\d{4}\s+)?world\s*series\b|"
    r"win\s+the\s+(?:\d{4}\s+)?(al|nl)\s*(east|west|central)|win\s+their\s+division|"
    r"win\s+the\s+(american|national)\s+league|"
    r"\bmvp\b|cy\s*young|make\s+the\s+playoffs|win\s+the\s+pennant",
    re.I,
)
_MONEYLINE_RE = re.compile(
    r"who\s+will\s+win|\bvs\.?\b|\bmoneyline\b|\bbeat\b|"
    r"will\s+the\s+.+\bor\b.+\bwin\b",  # "Will the Yankees or Red Sox win ..." (game/series head-to-head)
    re.I,
)
_PROP_RE = re.compile(
    r"home\s*run|hit\s+a|strike\s*out|no[\s-]?hitter|grand\s*slam|"
    r"\bprops?\b|first\s+to\s+score|combined\s+hits|innings\s+pitched|"
    r"first[\s-]?five|\bf5\b",
    re.I,
)


def classify_market_type(slug: str, question: str) -> str:
    text = f"{slug or ''} {question or ''}"
    if _TOTAL_RE.search(text):
        return "total"
    if _SPREAD_RE.search(text):
        return "spread"
    if _NRFI_RE.search(text):
        return "nrfi"
    if _FUTURES_RE.search(text):
        return "futures"
    if _PROP_RE.search(text):
        return "prop"
    if _MONEYLINE_RE.search(text):
        return "moneyline"
    # New-format per-game slug with no explicit suffix defaults to moneyline
    # (e.g. "mlb-lad-wsh-2026-04-03"); anything else is unclassified.
    if re.match(r"^mlb-[a-z]+-[a-z]+-\d{4}-\d{2}-\d{2}$", slug or ""):
        return "moneyline"
    return "other"
