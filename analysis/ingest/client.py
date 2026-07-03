#!/usr/bin/env python3
"""Shared HTTP clients + resumable-ingestion helpers for the Phase 0 data lake.

Two families of API:
  A) Intelligence API (auth, Bearer token from .env `intelligence_api_key`).
     POST-based, agent_id + params + pagination. Retry/backoff pattern copied
     from scripts/poll_live.py::api_call (proven in production).
  B) Public Polymarket APIs (Gamma, CLOB, Data-API) + MLB statsapi. GET-based,
     no auth, retried on 429/network errors.

Also provides:
  - append_jsonl(job, name, records): append-only raw-page archival to
    data/lake/raw/<job>/<name>.jsonl
  - IngestState: resumable job state (completed ids + cursor) persisted to
    data/lake/state/<job>.json
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Iterable

import requests
from dotenv import load_dotenv

BASE = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE / "data"
LAKE_DIR = DATA_DIR / "lake"
RAW_DIR = LAKE_DIR / "raw"
STATE_DIR = LAKE_DIR / "state"

INTEL_URL = "https://narrative.agent.heisenberg.so/api/v2/semantic/retrieve/parameterized"
GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"
DATA_API_URL = "https://data-api.polymarket.com"
MLB_STATSAPI_URL = "https://statsapi.mlb.com/api/v1/schedule"

AGENT_MARKETS = 574
AGENT_TRADES = 556
AGENT_CANDLES = 568
AGENT_ORDERBOOK = 572

MAX_WORKERS = 6  # proven-safe parallelism from scripts/poll_live.py


# ---------------------------------------------------------------------------
# Intelligence API (Bearer auth, POST, 429 backoff)
# ---------------------------------------------------------------------------
def load_api_key() -> str:
    load_dotenv(BASE / ".env")
    key = os.getenv("intelligence_api_key")
    if not key:
        raise RuntimeError("intelligence_api_key not found in .env")
    return key


def intel_call(
    agent_id: int,
    params: dict,
    pagination: dict | None,
    api_key: str,
    retries: int = 5,
) -> dict | None:
    """POST to the intelligence API with 429 exponential backoff. Returns
    None (and prints a warning) on unrecoverable failure rather than raising,
    so one bad call doesn't kill a long-running ingest job."""
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    body: dict[str, Any] = {
        "agent_id": agent_id,
        "params": params,
        "formatter_config": {"format_type": "raw"},
    }
    if pagination:
        body["pagination"] = pagination
    for attempt in range(retries):
        try:
            resp = requests.post(INTEL_URL, json=body, headers=headers, timeout=60)
            if resp.status_code == 429:
                wait = (2**attempt) * 5
                print(f"  [intel 429, retry in {wait}s]")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt < retries - 1:
                wait = (2**attempt) * 3
                print(f"  [intel error: {e}, retry in {wait}s]")
                time.sleep(wait)
            else:
                print(f"  [intel FAILED after {retries} attempts: {e}]")
                return None
    return None


def intel_paginated(
    agent_id: int,
    params: dict,
    api_key: str,
    limit: int = 200,
    max_pages: int = 50,
    sleep_s: float = 0.3,
) -> list[dict]:
    """Page through an intel agent until has_more is False or max_pages hit."""
    all_results: list[dict] = []
    offset = 0
    for _ in range(max_pages):
        resp = intel_call(agent_id, params, {"limit": limit, "offset": offset}, api_key)
        if resp is None:
            break
        results = resp.get("data", {}).get("results", [])
        if not results:
            break
        all_results.extend(results)
        if not resp.get("pagination", {}).get("has_more", False):
            break
        offset += limit
        time.sleep(sleep_s)
    return all_results


# ---------------------------------------------------------------------------
# Public APIs (no auth, GET, 429 backoff)
# ---------------------------------------------------------------------------
def http_get(url: str, params: dict | None = None, retries: int = 5, timeout: int = 20) -> requests.Response | None:
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                wait = (2**attempt) * 5
                print(f"  [public-api 429, retry in {wait}s]")
                time.sleep(wait)
                continue
            return resp
        except requests.RequestException as e:
            if attempt < retries - 1:
                wait = (2**attempt) * 2
                print(f"  [public-api error: {e}, retry in {wait}s]")
                time.sleep(wait)
            else:
                print(f"  [public-api FAILED: {e}]")
                return None
    return None


def gamma_get(path: str, params: dict | None = None) -> Any | None:
    """GET a Gamma API path (e.g. '/events/keyset'). Returns parsed JSON or None."""
    r = http_get(f"{GAMMA_URL}{path}", params)
    if r is None or r.status_code != 200:
        return None
    try:
        return r.json()
    except ValueError:
        return None


def clob_get(path: str, params: dict | None = None) -> Any | None:
    r = http_get(f"{CLOB_URL}{path}", params)
    if r is None or r.status_code != 200:
        return None
    try:
        return r.json()
    except ValueError:
        return None


def data_api_get(path: str, params: dict | None = None) -> Any | None:
    r = http_get(f"{DATA_API_URL}{path}", params)
    if r is None or r.status_code != 200:
        return None
    try:
        return r.json()
    except ValueError:
        return None


def statsapi_get(params: dict) -> Any | None:
    r = http_get(MLB_STATSAPI_URL, params)
    if r is None or r.status_code != 200:
        return None
    try:
        return r.json()
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Raw-page archival: append-only JSONL under data/lake/raw/<job>/<name>.jsonl
# ---------------------------------------------------------------------------
def append_jsonl(job: str, name: str, records: Iterable[dict]) -> int:
    """Append records (one JSON object per line) to data/lake/raw/<job>/<name>.jsonl.
    `name` may include subdirectories (e.g. '<season>/<condition_id>') - any
    missing intermediate directories are created. Returns the number of
    records written."""
    out_path = RAW_DIR / job / f"{name}.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(out_path, "a") as f:
        for rec in records:
            f.write(json.dumps(rec, default=str) + "\n")
            n += 1
    return n


def read_jsonl(path: Path) -> list[dict]:
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
# Resumable job state: data/lake/state/<job>.json
#   {"completed_ids": [...], "cursor": ..., ...extra fields...}
# ---------------------------------------------------------------------------
class IngestState:
    """Small resumable-state helper. Tracks a set of completed ids (for
    per-item jobs, e.g. per-condition_id trade pulls) and/or a cursor (for
    keyset-paginated sweeps). Call .save() after each unit of durable
    progress so a killed/restarted job resumes without re-fetching or
    duplicating archived rows."""

    def __init__(self, job: str):
        self.job = job
        self.path = STATE_DIR / f"{job}.json"
        self._state: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        if self.path.exists():
            try:
                with open(self.path) as f:
                    data = json.load(f)
                data["completed_ids"] = set(data.get("completed_ids", []))
                return data
            except (json.JSONDecodeError, OSError):
                pass
        return {"completed_ids": set(), "cursor": None}

    def save(self) -> None:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        serializable = dict(self._state)
        serializable["completed_ids"] = sorted(self._state.get("completed_ids", set()))
        tmp_path = self.path.with_suffix(".tmp")
        with open(tmp_path, "w") as f:
            json.dump(serializable, f, indent=2, default=str)
        tmp_path.replace(self.path)

    @property
    def cursor(self) -> Any:
        return self._state.get("cursor")

    @cursor.setter
    def cursor(self, value: Any) -> None:
        self._state["cursor"] = value

    @property
    def completed_ids(self) -> set:
        return self._state["completed_ids"]

    def mark_done(self, item_id: str) -> None:
        self._state["completed_ids"].add(item_id)

    def is_done(self, item_id: str) -> bool:
        return item_id in self._state["completed_ids"]

    def get(self, key: str, default: Any = None) -> Any:
        return self._state.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._state[key] = value
