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


# ---------------------------------------------------------------------------
# NFL/NBA ("sport") market-type + season derivation. Gamma markets for these
# sports carry a `sportsMarketType` field MLB markets don't - far more
# reliable than slug/question regex, which is only a fallback here for the
# null case (futures/season-long markets, or pre-2024 rows missing the
# field).
# ---------------------------------------------------------------------------
_FUTURES_KW_SPORT = (
    "champion", "mvp", "make-the-playoffs", "win-the-division",
    "conference-champion", "draft", "coach-of-the-year",
    "offensive-player", "defensive-player", "rookie-of-the-year",
    "super-bowl", "nba-finals", "win-the-nba-finals", "nba-cup",
)
_PER_GAME_SLUG_RE = re.compile(r"^(nfl|nba)-[a-z]+-[a-z]+-\d{4}-\d{2}-\d{2}$")


def classify_market_type_sport(sports_market_type: str | None, slug: str, question: str) -> str:
    """Bucket into moneyline/spread/total/prop/futures/other using Gamma's
    sportsMarketType field primarily (much more reliable than MLB's slug/question
    regex, which is only a fallback here for markets where the field is null)."""
    t = (sports_market_type or "").lower()
    if "moneyline" in t:
        return "moneyline"
    if "spread" in t:
        return "spread"
    if "total" in t:
        return "total"
    if t:  # non-empty, non-matched -> some other prop type (e.g. anytime_touchdowns)
        return "prop"
    # sports_market_type is null/empty -> likely a futures/season-long market, or
    # an older pre-2024 market missing the field. Fallback heuristics:
    text = f"{slug or ''} {question or ''}".lower()
    if any(k in text for k in _FUTURES_KW_SPORT):
        return "futures"
    if _PER_GAME_SLUG_RE.match(slug or ""):
        return "moneyline"
    return "other"


_SEASON_SLUG_RE = re.compile(r"^(nfl|nba)-(\d{4})$")


def derive_season_sport(event: dict, market: dict, sport: str) -> tuple["str | int | None", str]:
    """Returns (season_label, source). NFL season_label is an int year (e.g. 2025
    for the 2025 season, which runs Sep 2025 - Feb 2026). NBA season_label is a
    string like "2025-26" (season starting Oct 2025, ending Jun 2026).

    Preferred source: the event's `series` list - look for a slug matching
    `nfl-(\\d{4})` or `nba-(\\d{4})`; for NBA the series slug year is the season's
    ENDING year (nba-2026 series == the "2025-26" season), confirmed live.

    Fallback (no matching series, e.g. legacy series_id=1/2 events, or events with
    no series at all): derive from market.endDate (else event.endDate), using the
    standard sports-season convention - for NFL, month>=8 (Aug-Dec) => season=year;
    month<=7 (Jan-Jul, covers playoffs/Super Bowl + offseason draft) => season=year-1.
    For NBA, month>=8 (Aug-Dec) => season_start=year; month<=7 (Jan-Jul, covers
    playoffs/Finals + offseason) => season_start=year-1; label=f"{season_start}-{str(season_start+1)[2:]}".
    """
    for s in (event.get("series") or []):
        slug = (s or {}).get("slug") or ""
        m = _SEASON_SLUG_RE.match(slug)
        if not m or m.group(1) != sport:
            continue
        series_year = int(m.group(2))
        if sport == "nfl":
            return series_year, "event.series"
        else:  # nba: series slug year is the season's ENDING year
            season_start = series_year - 1
            return f"{season_start}-{str(season_start + 1)[2:]}", "event.series"

    date_val = market.get("endDate") or event.get("endDate")
    year, month = _year_month_of(date_val)
    if year is None:
        return None, "none"

    if sport == "nfl":
        season = year if month >= 8 else year - 1
        return season, "date_fallback"
    else:  # nba
        season_start = year if month >= 8 else year - 1
        return f"{season_start}-{str(season_start + 1)[2:]}", "date_fallback"


def _year_month_of(date_str: str | None) -> tuple[int | None, int | None]:
    """Extends _year_of to also return the month, for season-boundary logic
    that needs to know which side of the season-year cutover a date falls on."""
    if not date_str or not isinstance(date_str, str) or len(date_str) < 7:
        return None, None
    try:
        year = int(date_str[:4])
        month = int(date_str[5:7])
        return year, month
    except ValueError:
        return None, None
