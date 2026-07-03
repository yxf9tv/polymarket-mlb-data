#!/usr/bin/env python3
"""Phase 3 step 1c: raw orderbook JSONL -> data/lake/orderbook.parquet.

Reads data/lake/raw/orderbooks/<season>/<condition_id>.jsonl (written by
analysis/ingest/ingest_orderbooks.py), parses each snapshot's bids/asks
(JSON-string-encoded list of {"price","size"} at ingest time) into sorted
numeric level arrays, and writes one row per snapshot with:
  token_id, condition_id, season, ts (UTC), bids_json, asks_json
    (bids sorted best-to-worst i.e. price descending; asks price ascending;
     each re-serialized as a compact [[price, size], ...] JSON string so
     downstream book-walking doesn't need bids/asks-shape guessing),
  best_bid, best_ask, mid, spread,
  bid_depth_usd, ask_depth_usd (sum price*size over ALL levels),
  n_bid_levels, n_ask_levels.

Standalone (not wired into analysis/ingest/build_lake.py) per the Phase-3
scripts' own-cache convention (see portfolio_selection.py).

Usage:
    .venv/bin/python3 analysis/build_orderbook_lake.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parent.parent
LAKE_DIR = BASE / "data" / "lake"
RAW_DIR = LAKE_DIR / "raw" / "orderbooks"


def _parse_levels(raw, reverse: bool) -> list[list[float]]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    if not isinstance(raw, list):
        return []
    out = []
    for lvl in raw:
        try:
            p = float(lvl.get("price"))
            s = float(lvl.get("size"))
        except (TypeError, ValueError, AttributeError):
            continue
        if p > 0 and s > 0:
            out.append([p, s])
    out.sort(key=lambda x: x[0], reverse=reverse)
    return out


def _parse_record(rec: dict) -> dict | None:
    ts = rec.get("timestamp")
    if not ts:
        return None
    bids = _parse_levels(rec.get("bids", "[]"), reverse=True)   # best bid first (highest price)
    asks = _parse_levels(rec.get("asks", "[]"), reverse=False)  # best ask first (lowest price)
    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    mid = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None
    spread = (best_ask - best_bid) if best_bid is not None and best_ask is not None else None
    return {
        "token_id": rec.get("token_id"),
        "condition_id": rec.get("condition_id"),
        "season": rec.get("season"),
        "ts_raw": ts,
        "bids_json": json.dumps(bids),
        "asks_json": json.dumps(asks),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": mid,
        "spread": spread,
        "bid_depth_usd": sum(p * s for p, s in bids),
        "ask_depth_usd": sum(p * s for p, s in asks),
        "n_bid_levels": len(bids),
        "n_ask_levels": len(asks),
    }


def main() -> None:
    if not RAW_DIR.exists():
        print(f"No raw orderbook data at {RAW_DIR} -- run ingest_orderbooks.py first.")
        return

    rows = []
    n_files = 0
    for season_dir in sorted(p for p in RAW_DIR.iterdir() if p.is_dir()):
        for jsonl_path in sorted(season_dir.glob("*.jsonl")):
            n_files += 1
            with open(jsonl_path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    parsed = _parse_record(rec)
                    if parsed is not None:
                        rows.append(parsed)

    print(f"Parsed {len(rows)} snapshot rows from {n_files} files under {RAW_DIR}")
    if not rows:
        print("Nothing to write.")
        return

    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["ts_raw"], utc=True, format="ISO8601", errors="coerce")
    df = df.drop(columns=["ts_raw"])
    n_before = len(df)
    df = df.dropna(subset=["ts", "token_id", "condition_id"])
    df = df.drop_duplicates(subset=["token_id", "ts"], keep="last")
    print(f"  {n_before} parsed -> {len(df)} after dropping unparseable rows + de-duplication")
    df = df.sort_values(["condition_id", "ts"]).reset_index(drop=True)

    out_path = LAKE_DIR / "orderbook.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Wrote {out_path}: {len(df)} rows, {df['condition_id'].nunique()} markets, "
          f"{df['token_id'].nunique()} tokens.")
    print(f"  snapshots/market distribution:\n{df.groupby('condition_id').size().describe()}")
    print(f"  markets with zero bid or ask levels: "
          f"{int(((df['n_bid_levels'] == 0) | (df['n_ask_levels'] == 0)).sum())}/{len(df)} snapshot rows")


if __name__ == "__main__":
    main()
