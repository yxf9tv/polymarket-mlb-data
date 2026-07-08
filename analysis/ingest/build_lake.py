#!/usr/bin/env python3
"""Build the Parquet data lake from raw archived JSONL.

Reads:
  data/lake/raw/markets/events.jsonl     (Gamma events, each with nested markets)
  data/lake/raw/schedule/<year>.jsonl    (statsapi date-buckets, each with games)
  data/lake/raw/trades/<season>/<cid>.jsonl   (intel-556 / Data-API trade records)
  data/lake/raw/candles/<season>/<cid>.jsonl  (CLOB / intel-568 price-history points)

Writes:
  data/lake/events.parquet    - one row per Gamma event
  data/lake/markets.parquet   - one row per Gamma market (condition_id)
  data/lake/schedule.parquet  - one row per MLB game (statsapi gamePk)
  data/lake/trades.parquet    - one row per trade (deduped on trade_id)
  data/lake/candles.parquet   - one row per price-history point

Idempotent: always rebuilds parquet from the current raw JSONL, so it's safe
to rerun after every incremental ingest. `--tables trades,candles` builds
only those tables (default with no flag: events+markets+schedule, matching
the original Phase-0 behavior). trades/candles builds are incremental-friendly:
each raw/<job>/<season>/ directory is parsed once and cached to
data/lake/raw/_parsed_cache/<job>_<season>.parquet, keyed by a cheap
(file count, total bytes, max mtime) signature - unchanged season dirs are
loaded from cache instead of re-parsing every JSONL line on every rebuild.

Usage:
    .venv/bin/python3 analysis/ingest/build_lake.py                       # events+markets+schedule
    .venv/bin/python3 analysis/ingest/build_lake.py --tables trades,candles
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd

from client import LAKE_DIR, RAW_DIR
from derive import (
    classify_market_type,
    classify_market_type_sport,
    derive_season,
    derive_season_sport,
    derive_winner,
    parse_token_ids,
)


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


# ---------------------------------------------------------------------------
# events.parquet + markets.parquet (from Gamma raw events)
# ---------------------------------------------------------------------------
def build_events_and_markets() -> tuple[pd.DataFrame, pd.DataFrame]:
    events_path = RAW_DIR / "markets" / "events.jsonl"
    raw_events = load_jsonl(events_path)
    print(f"Loaded {len(raw_events)} raw events from {events_path}")

    # De-dupe events by id (a rerun sweep resumes by cursor and shouldn't
    # duplicate, but guard anyway - last occurrence wins, i.e. freshest data).
    events_by_id: dict[str, dict] = {}
    for ev in raw_events:
        events_by_id[ev.get("id")] = ev
    print(f"  {len(events_by_id)} unique events after de-dup")

    event_rows = []
    market_rows = []
    fallback_season_count = 0

    for event in events_by_id.values():
        event_id = event.get("id")
        event_slug = event.get("slug")
        markets = event.get("markets", []) or []

        event_volume = 0.0
        try:
            event_volume = float(event.get("volume") or 0)
        except (TypeError, ValueError):
            pass

        event_rows.append({
            "event_id": event_id,
            "slug": event_slug,
            "title": event.get("title"),
            "ticker": event.get("ticker"),
            "start_date": event.get("startDate"),
            "end_date": event.get("endDate"),
            "creation_date": event.get("creationDate"),
            "closed": bool(event.get("closed")),
            "active": bool(event.get("active")),
            "archived": bool(event.get("archived")),
            "volume": event_volume,
            "n_markets": len(markets),
            "neg_risk": bool(event.get("negRisk") or event.get("enableNegRisk") or False),
        })

        for market in markets:
            cid = market.get("conditionId")
            if not cid:
                continue
            winner, prices = derive_winner(market)
            season, season_source = derive_season(event, market)
            if season_source != "market.endDate" and season_source != "event.endDate":
                fallback_season_count += 1
            token_ids = parse_token_ids(market)
            slug = market.get("slug") or ""
            question = market.get("question") or ""
            market_type = classify_market_type(slug, question)

            volume = 0.0
            try:
                volume = float(market.get("volume") or 0)
            except (TypeError, ValueError):
                pass

            outcomes_raw = market.get("outcomes", "[]")
            outcomes = outcomes_raw if isinstance(outcomes_raw, str) else json.dumps(outcomes_raw)

            market_rows.append({
                "condition_id": cid,
                "event_id": event_id,
                "event_slug": event_slug,
                "market_id": market.get("id"),
                "slug": slug,
                "question": question,
                "market_type": market_type,
                "outcomes": outcomes,
                "outcome_prices_raw": ",".join(str(p) for p in prices) if prices else "",
                "winning_outcome": winner,
                "resolved": winner is not None,
                "token_ids": ",".join(token_ids) if token_ids else "",
                "volume": volume,
                "liquidity": _safe_float(market.get("liquidity")),
                "start_date": market.get("startDate"),
                "end_date": market.get("endDate"),
                "closed": bool(market.get("closed")),
                "active": bool(market.get("active")),
                "enable_order_book": bool(market.get("enableOrderBook")),
                "neg_risk": bool(market.get("negRisk") or False),
                "season": season,
                "season_source": season_source,
            })

    events_df = pd.DataFrame(event_rows)
    markets_df = pd.DataFrame(market_rows)
    # de-dupe markets by condition_id (last wins) in case of overlapping raw pages
    if not markets_df.empty:
        markets_df = markets_df.drop_duplicates(subset=["condition_id"], keep="last")

    print(f"  {len(markets_df)} unique markets; "
          f"{fallback_season_count} used a season-date fallback beyond endDate")
    return events_df, markets_df


def _safe_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# markets_<sport>.parquet (NFL/NBA, from Gamma raw events archived by
# ingest_markets_sport.py). Mirrors build_events_and_markets()'s markets-row
# structure, but classifies market_type via Gamma's sportsMarketType field
# (classify_market_type_sport) and derives season via the sport-aware series/
# date logic (derive_season_sport) instead of the MLB-specific versions.
# derive_winner/parse_token_ids are sport-agnostic and reused unchanged.
# No events_<sport>.parquet is built - only the markets table is needed
# downstream (per the research-program spec for this phase).
# ---------------------------------------------------------------------------
def build_events_and_markets_sport(sport: str) -> pd.DataFrame:
    events_path = RAW_DIR / f"markets_{sport}" / "events.jsonl"
    raw_events = load_jsonl(events_path)
    print(f"Loaded {len(raw_events)} raw events from {events_path}")

    # De-dupe events by id (two sweeps - tag + legacy series - may overlap).
    events_by_id: dict[str, dict] = {}
    for ev in raw_events:
        events_by_id[ev.get("id")] = ev
    print(f"  {len(events_by_id)} unique events after de-dup")

    market_rows = []
    fallback_season_count = 0

    for event in events_by_id.values():
        event_id = event.get("id")
        event_slug = event.get("slug")
        markets = event.get("markets", []) or []

        for market in markets:
            cid = market.get("conditionId")
            if not cid:
                continue
            winner, prices = derive_winner(market)
            season, season_source = derive_season_sport(event, market, sport)
            if season_source == "date_fallback":
                fallback_season_count += 1
            token_ids = parse_token_ids(market)
            slug = market.get("slug") or ""
            question = market.get("question") or ""
            sports_market_type_raw = market.get("sportsMarketType")
            market_type = classify_market_type_sport(sports_market_type_raw, slug, question)

            volume = 0.0
            try:
                volume = float(market.get("volume") or 0)
            except (TypeError, ValueError):
                pass

            outcomes_raw = market.get("outcomes", "[]")
            outcomes = outcomes_raw if isinstance(outcomes_raw, str) else json.dumps(outcomes_raw)

            market_rows.append({
                "condition_id": cid,
                "event_id": event_id,
                "event_slug": event_slug,
                "market_id": market.get("id"),
                "slug": slug,
                "question": question,
                "market_type": market_type,
                "sports_market_type_raw": sports_market_type_raw,
                "game_start_time": market.get("gameStartTime"),
                "outcomes": outcomes,
                "outcome_prices_raw": ",".join(str(p) for p in prices) if prices else "",
                "winning_outcome": winner,
                "resolved": winner is not None,
                "token_ids": ",".join(token_ids) if token_ids else "",
                "volume": volume,
                "liquidity": _safe_float(market.get("liquidity")),
                "start_date": market.get("startDate"),
                "end_date": market.get("endDate"),
                "closed": bool(market.get("closed")),
                "active": bool(market.get("active")),
                "enable_order_book": bool(market.get("enableOrderBook")),
                "neg_risk": bool(market.get("negRisk") or False),
                "season": season,
                "season_source": season_source,
            })

    markets_df = pd.DataFrame(market_rows)
    if not markets_df.empty:
        markets_df = markets_df.drop_duplicates(subset=["condition_id"], keep="last")

    print(f"  {len(markets_df)} unique {sport} markets; "
          f"{fallback_season_count} used the date fallback (no matching series)")
    return markets_df


# ---------------------------------------------------------------------------
# schedule.parquet (from statsapi raw date-buckets)
# ---------------------------------------------------------------------------
def build_schedule() -> pd.DataFrame:
    schedule_dir = RAW_DIR / "schedule"
    if not schedule_dir.exists():
        print("No schedule raw data found")
        return pd.DataFrame()

    rows = []
    for path in sorted(schedule_dir.glob("*.jsonl")):
        buckets = load_jsonl(path)
        for bucket in buckets:
            for game in bucket.get("games", []):
                teams = game.get("teams", {})
                away = teams.get("away", {}).get("team", {})
                home = teams.get("home", {}).get("team", {})
                status = game.get("status", {})
                rows.append({
                    "game_pk": game.get("gamePk"),
                    "game_date_utc": game.get("gameDate"),
                    "official_date": game.get("officialDate"),
                    "season": _safe_int(game.get("season")),
                    "game_type": game.get("gameType"),
                    "away_team_id": away.get("id"),
                    "away_team_name": away.get("name"),
                    "home_team_id": home.get("id"),
                    "home_team_name": home.get("name"),
                    "abstract_state": status.get("abstractGameState"),
                    "detailed_state": status.get("detailedState"),
                    "venue_id": (game.get("venue") or {}).get("id"),
                    "venue_name": (game.get("venue") or {}).get("name"),
                })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates(subset=["game_pk"], keep="last")
    print(f"Loaded {len(df)} unique games from {schedule_dir}")
    return df


def _safe_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Incremental per-season-directory parse cache, shared by trades + candles.
# Signature = (n_files, total_bytes, max_mtime) over a season dir's *.jsonl -
# cheap to compute, changes whenever new raw data lands (append-only jsonl
# growth changes size/mtime; a brand-new market file changes n_files).
# ---------------------------------------------------------------------------
PARSED_CACHE_DIR = RAW_DIR / "_parsed_cache"


def _dir_signature(season_dir: Path) -> str:
    n_files = 0
    total_bytes = 0
    max_mtime = 0.0
    for f in season_dir.glob("*.jsonl"):
        st = f.stat()
        n_files += 1
        total_bytes += st.st_size
        max_mtime = max(max_mtime, st.st_mtime)
    return f"{n_files}:{total_bytes}:{max_mtime}"


def _load_season_cached(
    job: str, season_dir: Path, parse_fn, manifest: dict, post_fn=None
) -> pd.DataFrame:
    """Return the parsed DataFrame for one raw/<job>/<season>/ directory,
    reusing data/lake/raw/_parsed_cache/<job>_<season>.parquet if the
    directory's signature hasn't changed since the cache was written.
    `post_fn(df)` (if given) runs once per freshly-parsed season DataFrame
    before caching - used for vectorized timestamp parsing (calling
    pd.to_datetime once per season instead of once per record is ~50x
    faster, which matters at full-sweep scale)."""
    season = season_dir.name
    sig = _dir_signature(season_dir)
    manifest_key = f"{job}_{season}"
    cache_path = PARSED_CACHE_DIR / f"{manifest_key}.parquet"

    if manifest.get(manifest_key) == sig and cache_path.exists():
        return pd.read_parquet(cache_path)

    rows = []
    for jsonl_path in sorted(season_dir.glob("*.jsonl")):
        for rec in load_jsonl(jsonl_path):
            row = parse_fn(rec, season)
            if row is not None:
                rows.append(row)
    df = pd.DataFrame(rows)
    if post_fn is not None and not df.empty:
        df = post_fn(df)
    PARSED_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path, index=False)
    manifest[manifest_key] = sig
    return df


def _load_manifest() -> dict:
    path = LAKE_DIR / "state" / "build_lake_manifest.json"
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_manifest(manifest: dict) -> None:
    path = LAKE_DIR / "state" / "build_lake_manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2)


# ---------------------------------------------------------------------------
# trades.parquet - two-pass DuckDB build (memory-safe at 13M+ rows / 7.9GB
# raw JSONL). The original approach parsed every JSONL record into a Python
# dict then built a pandas DataFrame per season -- at full-lake scale that
# OOMs (millions of Python dict objects + pandas concat of all seasons held
# in memory at once). DuckDB streams/spills instead of materializing
# everything as Python objects.
#
# Pass 1 (per season, cached like the other build_lake tables): normalize
# raw/trades/<season>/*.jsonl -- a mix of intel-556 records (snake_case,
# unique `id`) and Data-API fallback records (camelCase, no `id`, one
# synthesized from a hash of its natural key) -- straight into a parquet
# projection via DuckDB's read_json. `token_id`/`id` are large hex-like
# strings and must be declared VARCHAR explicitly: DuckDB's JSON type
# inference can otherwise mis-detect them as numeric and silently corrupt/
# truncate the value.
#
# Pass 2: SELECT DISTINCT across all cached season parquet files -> final
# trades.parquet in one tuned connection (6GB memory limit, 3 threads,
# insertion order not preserved so DuckDB is free to spill/stream, temp
# directory set for disk spill). Written to a temp file and atomically
# swapped into place via os.replace so a crash mid-write never leaves a
# truncated trades.parquet.
# ---------------------------------------------------------------------------
TRADE_JSON_COLUMNS = {
    "condition_id": "VARCHAR",
    "proxy_wallet": "VARCHAR",
    "conditionId": "VARCHAR",
    "proxyWallet": "VARCHAR",
    "side": "VARCHAR",
    "size": "VARCHAR",
    "price": "VARCHAR",
    "timestamp": "VARCHAR",
    "token_id": "VARCHAR",
    "asset": "VARCHAR",
    "id": "VARCHAR",
    "transactionHash": "VARCHAR",
}


def _duckdb_connect_tuned() -> "duckdb.DuckDBPyConnection":
    """Fresh in-process DuckDB connection tuned for bounded peak memory on
    the full-lake trades rebuild."""
    tmp_dir = RAW_DIR / "_duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=3")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{tmp_dir.as_posix()}'")
    # trades.parquet's timestamp column must stay tz=UTC (matches the schema
    # every downstream consumer, e.g. trader_metrics.py, expects) -- without
    # this, TIMESTAMPTZ casts/conversions use the machine's local timezone.
    con.execute("SET TimeZone='UTC'")
    return con


def _build_trades_season_cached(season_dir: Path, manifest: dict) -> Path | None:
    """Pass 1 for one raw/trades/<season>/ directory. Returns the cached
    per-season parquet path (or None if the directory has no jsonl data),
    reusing the same (job, season) signature scheme as the other
    build_lake tables so an unchanged season dir is skipped on rebuild."""
    season = season_dir.name
    sig = _dir_signature(season_dir)
    manifest_key = f"trades_{season}"
    cache_path = PARSED_CACHE_DIR / f"{manifest_key}.parquet"

    if manifest.get(manifest_key) == sig and cache_path.exists():
        return cache_path

    if not any(season_dir.glob("*.jsonl")):
        return None

    glob_path = (season_dir / "*.jsonl").as_posix()
    PARSED_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp_out = cache_path.with_suffix(".tmp.parquet")
    con = _duckdb_connect_tuned()
    try:
        con.execute(f"""
            COPY (
                SELECT
                    COALESCE(condition_id, conditionId) AS condition_id,
                    COALESCE(proxy_wallet, proxyWallet) AS proxy_wallet,
                    side,
                    TRY_CAST(size AS DOUBLE) AS size,
                    TRY_CAST(price AS DOUBLE) AS price,
                    COALESCE(token_id, asset) AS token_id,
                    CASE WHEN id IS NOT NULL AND id != '' THEN id
                         ELSE 'da_' || md5(
                             COALESCE(transactionHash, '') || '|' || COALESCE(proxyWallet, '') || '|' ||
                             COALESCE(timestamp, '') || '|' || COALESCE(asset, '') || '|' ||
                             COALESCE(side, '') || '|' || COALESCE(size, '')
                         )
                    END AS trade_id,
                    CASE WHEN id IS NOT NULL AND id != ''
                         THEN TRY_CAST(timestamp AS TIMESTAMPTZ)
                         ELSE to_timestamp(TRY_CAST(timestamp AS DOUBLE))::TIMESTAMPTZ
                    END AS timestamp,
                    {int(season)} AS season,
                    CASE WHEN id IS NOT NULL AND id != '' THEN 'intel' ELSE 'data_api' END AS source
                FROM read_json('{glob_path}', format='newline_delimited', columns={TRADE_JSON_COLUMNS!r})
            ) TO '{tmp_out.as_posix()}' (FORMAT PARQUET)
        """)
    finally:
        con.close()
    os.replace(tmp_out, cache_path)
    manifest[manifest_key] = sig
    return cache_path


def _build_trades_season_cached_sport(season_dir: Path, manifest: dict, sport: str) -> Path | None:
    """Sport-path counterpart to _build_trades_season_cached(): same pass-1
    normalization, but season is carried as VARCHAR (NBA season labels like
    "2025-26" aren't castable to int, unlike MLB's int season) and cache/
    manifest keys are namespaced per sport (trades_<sport>_<season_dir>) so
    they never collide with MLB's trades_<season> keys. Kept as a parallel
    function (rather than parameterizing _build_trades_season_cached) so the
    MLB SQL stays byte-for-byte unchanged."""
    season_dir_name = season_dir.name
    sig = _dir_signature(season_dir)
    manifest_key = f"trades_{sport}_{season_dir_name}"
    cache_path = PARSED_CACHE_DIR / f"{manifest_key}.parquet"

    if manifest.get(manifest_key) == sig and cache_path.exists():
        return cache_path

    if not any(season_dir.glob("*.jsonl")):
        return None

    glob_path = (season_dir / "*.jsonl").as_posix()
    PARSED_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp_out = cache_path.with_suffix(".tmp.parquet")
    con = _duckdb_connect_tuned()
    try:
        con.execute(f"""
            COPY (
                SELECT
                    COALESCE(condition_id, conditionId) AS condition_id,
                    COALESCE(proxy_wallet, proxyWallet) AS proxy_wallet,
                    side,
                    TRY_CAST(size AS DOUBLE) AS size,
                    TRY_CAST(price AS DOUBLE) AS price,
                    COALESCE(token_id, asset) AS token_id,
                    CASE WHEN id IS NOT NULL AND id != '' THEN id
                         ELSE 'da_' || md5(
                             COALESCE(transactionHash, '') || '|' || COALESCE(proxyWallet, '') || '|' ||
                             COALESCE(timestamp, '') || '|' || COALESCE(asset, '') || '|' ||
                             COALESCE(side, '') || '|' || COALESCE(size, '')
                         )
                    END AS trade_id,
                    CASE WHEN id IS NOT NULL AND id != ''
                         THEN TRY_CAST(timestamp AS TIMESTAMPTZ)
                         ELSE to_timestamp(TRY_CAST(timestamp AS DOUBLE))::TIMESTAMPTZ
                    END AS timestamp,
                    CAST('{season_dir_name}' AS VARCHAR) AS season,
                    CASE WHEN id IS NOT NULL AND id != '' THEN 'intel' ELSE 'data_api' END AS source
                FROM read_json('{glob_path}', format='newline_delimited', columns={TRADE_JSON_COLUMNS!r})
            ) TO '{tmp_out.as_posix()}' (FORMAT PARQUET)
        """)
    finally:
        con.close()
    os.replace(tmp_out, cache_path)
    manifest[manifest_key] = sig
    return cache_path


def build_trades_sport(sport: str) -> Path:
    """Sport-path counterpart to build_trades(): two-pass DuckDB rebuild of
    trades_<sport>.parquet from raw/trades_<sport>/<season_dir>/*.jsonl.
    season column is VARCHAR (see _build_trades_season_cached_sport)."""
    trades_dir = RAW_DIR / f"trades_{sport}"
    trades_path = LAKE_DIR / f"trades_{sport}.parquet"
    if not trades_dir.exists():
        print(f"No trades_{sport} raw data found")
        return trades_path

    manifest = _load_manifest()
    season_cache_paths = []
    for season_dir in sorted(p for p in trades_dir.iterdir() if p.is_dir()):
        cache_path = _build_trades_season_cached_sport(season_dir, manifest, sport)
        if cache_path is not None:
            season_cache_paths.append(cache_path)
    _save_manifest(manifest)

    if not season_cache_paths:
        print(f"No trades_{sport} records parsed")
        return trades_path

    glob_list = ", ".join(f"'{p.as_posix()}'" for p in season_cache_paths)
    tmp_out = trades_path.with_suffix(".tmp.parquet")
    con = _duckdb_connect_tuned()
    try:
        n_before = con.execute(f"SELECT count(*) FROM read_parquet([{glob_list}])").fetchone()[0]
        con.execute(f"""
            COPY (
                SELECT DISTINCT condition_id, proxy_wallet, side, size, price,
                                 token_id, trade_id, timestamp, season, source
                FROM read_parquet([{glob_list}])
            ) TO '{tmp_out.as_posix()}' (FORMAT PARQUET)
        """)
        n_after = con.execute(f"SELECT count(*) FROM read_parquet('{tmp_out.as_posix()}')").fetchone()[0]
    finally:
        con.close()
    os.replace(tmp_out, trades_path)
    print(f"Loaded {n_before} trades_{sport} rows ({n_after} unique after SELECT DISTINCT dedup, "
          f"{n_before - n_after} duplicate rows deduped)")
    return trades_path


def build_trades() -> Path:
    """Two-pass DuckDB rebuild of trades.parquet. Writes the parquet itself
    (unlike the other build_* functions, it does NOT return a DataFrame to
    hold in the caller's Python process -- at 13M+ rows that defeats the
    whole point of the memory-safe rebuild)."""
    trades_dir = RAW_DIR / "trades"
    trades_path = LAKE_DIR / "trades.parquet"
    if not trades_dir.exists():
        print("No trades raw data found")
        return trades_path

    manifest = _load_manifest()
    season_cache_paths = []
    for season_dir in sorted(p for p in trades_dir.iterdir() if p.is_dir()):
        cache_path = _build_trades_season_cached(season_dir, manifest)
        if cache_path is not None:
            season_cache_paths.append(cache_path)
    _save_manifest(manifest)

    if not season_cache_paths:
        print("No trade records parsed")
        return trades_path

    glob_list = ", ".join(f"'{p.as_posix()}'" for p in season_cache_paths)
    tmp_out = trades_path.with_suffix(".tmp.parquet")
    con = _duckdb_connect_tuned()
    try:
        n_before = con.execute(f"SELECT count(*) FROM read_parquet([{glob_list}])").fetchone()[0]
        con.execute(f"""
            COPY (
                SELECT DISTINCT condition_id, proxy_wallet, side, size, price,
                                 token_id, trade_id, timestamp, season, source
                FROM read_parquet([{glob_list}])
            ) TO '{tmp_out.as_posix()}' (FORMAT PARQUET)
        """)
        n_after = con.execute(f"SELECT count(*) FROM read_parquet('{tmp_out.as_posix()}')").fetchone()[0]
    finally:
        con.close()
    os.replace(tmp_out, trades_path)
    print(f"Loaded {n_before} trade rows ({n_after} unique after SELECT DISTINCT dedup, "
          f"{n_before - n_after} duplicate rows deduped)")
    return trades_path


# ---------------------------------------------------------------------------
# candles.parquet - normalizes CLOB prices-history points ({t, p, ...+source})
# and intel-568 OHLC candles (candle_time/close, +source) into a common
# (token_id, condition_id, ts, price, source) schema.
# ---------------------------------------------------------------------------
def _parse_candle_record(rec: dict, season: str) -> dict | None:
    source = rec.get("source")
    if source == "clob":
        ts_raw, is_epoch = str(rec.get("t")), True
        price = _safe_float(rec.get("p"))
    elif source == "intel":
        ts_raw, is_epoch = rec.get("candle_time"), False
        price = _safe_float(rec.get("close"))
    else:
        return None  # unrecognized/legacy record shape - skip rather than guess
    return {
        "token_id": rec.get("token_id"),
        "condition_id": rec.get("condition_id"),
        "ts_raw": ts_raw,
        "ts_is_epoch": is_epoch,
        "price": price,
        "source": source,
    }


def _post_candles(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorized UTC timestamp parse for a season's candle rows (epoch
    seconds from CLOB + ISO strings from intel 568)."""
    ts = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")
    epoch_mask = df["ts_is_epoch"]
    if epoch_mask.any():
        ts.loc[epoch_mask] = pd.to_datetime(
            pd.to_numeric(df.loc[epoch_mask, "ts_raw"], errors="coerce"),
            unit="s", utc=True, errors="coerce",
        )
    if (~epoch_mask).any():
        ts.loc[~epoch_mask] = pd.to_datetime(
            df.loc[~epoch_mask, "ts_raw"], utc=True, errors="coerce", format="ISO8601",
        )
    df = df.drop(columns=["ts_raw", "ts_is_epoch"])
    df["ts"] = ts
    return df


def build_candles() -> pd.DataFrame:
    candles_dir = RAW_DIR / "candles"
    if not candles_dir.exists():
        print("No candles raw data found")
        return pd.DataFrame()

    manifest = _load_manifest()
    season_dfs = []
    for season_dir in sorted(p for p in candles_dir.iterdir() if p.is_dir()):
        df = _load_season_cached("candles", season_dir, _parse_candle_record, manifest, post_fn=_post_candles)
        if not df.empty:
            season_dfs.append(df)
    _save_manifest(manifest)

    if not season_dfs:
        print("No candle records parsed")
        return pd.DataFrame()

    df = pd.concat(season_dfs, ignore_index=True)
    n_before = len(df)
    df = df.drop_duplicates(subset=["token_id", "ts", "source"], keep="last")
    print(f"Loaded {n_before} candle rows ({len(df)} unique after dedup, "
          f"{n_before - len(df)} duplicate rows deduped)")
    return df


# ---------------------------------------------------------------------------
def _parse_tables_arg(args: list[str], default: set[str] | None = None) -> set[str]:
    for a in args:
        if a.startswith("--tables"):
            value = a.split("=", 1)[1] if "=" in a else args[args.index(a) + 1]
            return {t.strip() for t in value.split(",") if t.strip()}
    return default if default is not None else {"events", "markets", "schedule"}  # original Phase-0 default


def _parse_sport_arg(args: list[str]) -> str:
    for a in args:
        if a.startswith("--sport"):
            return (a.split("=", 1)[1] if "=" in a else args[args.index(a) + 1]).strip()
    return "mlb"


def main() -> None:
    LAKE_DIR.mkdir(parents=True, exist_ok=True)
    sport = _parse_sport_arg(sys.argv[1:])

    if sport != "mlb":
        # NFL/NBA path: markets_<sport>.parquet and/or trades_<sport>.parquet
        # (no events/schedule/candles equivalent for these sports in this
        # phase). Default (no --tables flag) stays markets-only, matching
        # this path's original behavior.
        tables = _parse_tables_arg(sys.argv[1:], default={"markets"})

        if "markets" in tables:
            markets_df = build_events_and_markets_sport(sport)
            markets_path = LAKE_DIR / f"markets_{sport}.parquet"
            markets_df.to_parquet(markets_path, index=False)
            print(f"Wrote {markets_path} ({len(markets_df)} rows)")

        if "trades" in tables:
            trades_path = build_trades_sport(sport)  # writes trades_<sport>.parquet itself
            print(f"Wrote {trades_path}")
        return

    tables = _parse_tables_arg(sys.argv[1:])

    if "events" in tables or "markets" in tables:
        events_df, markets_df = build_events_and_markets()
        events_path = LAKE_DIR / "events.parquet"
        markets_path = LAKE_DIR / "markets.parquet"
        events_df.to_parquet(events_path, index=False)
        markets_df.to_parquet(markets_path, index=False)
        print(f"Wrote {events_path} ({len(events_df)} rows)")
        print(f"Wrote {markets_path} ({len(markets_df)} rows)")

    if "schedule" in tables:
        schedule_df = build_schedule()
        schedule_path = LAKE_DIR / "schedule.parquet"
        schedule_df.to_parquet(schedule_path, index=False)
        print(f"Wrote {schedule_path} ({len(schedule_df)} rows)")

    if "trades" in tables:
        trades_path = build_trades()  # writes trades.parquet itself (see docstring)
        print(f"Wrote {trades_path}")

    if "candles" in tables:
        candles_df = build_candles()
        candles_path = LAKE_DIR / "candles.parquet"
        candles_df.to_parquet(candles_path, index=False)
        print(f"Wrote {candles_path} ({len(candles_df)} rows)")


if __name__ == "__main__":
    main()
