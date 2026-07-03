# Polymarket MLB Data — Trader Edge Research

A data-science project over the complete Polymarket MLB dataset (CLOB era: 2024 playoffs → present): a 13M-trade lake, a 224k-wallet trader census scored on market settlements, walk-forward portfolio selection, and orderbook-priced executability analysis — plus the live sentiment dashboard the research grew out of.

**Headline finding:** no tested signal (trader selection, crowd flow, depth imbalance, copy-trading) survives honest execution pricing. Full chain of evidence in `reports/00`–`07`; program design and history in `RESEARCH_PLAN.md`.

## Layout

- `scripts/` — live dashboard (FastAPI + vanilla JS): `./run_dashboard.sh`, port 8000. Recommendations load dynamically from `data/edge_rules.json` (currently empty — by evidence, not omission).
- `analysis/ingest/` — resumable ingestion jobs (intelligence API + Gamma/CLOB/statsapi) writing raw JSONL + state checkpoints.
- `analysis/` — lake builder, trader metrics, hypothesis tests, portfolio selection, edge-rule mining.
- `reports/` — the research reports (probes → lake → census → persistence → portfolio → edge rules).
- `data/lake/` — parquet tables. Small analysis-grade tables are committed (markets, settlements, schedule, candles, per-trader metrics, **orderbook** — irreplaceable, the API retains only ~3 months of depth history). Large fill-level tables are gitignored and rebuildable.

## Rebuilding the full lake on a fresh clone

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
echo 'intelligence_api_key=YOUR_KEY' > .env
.venv/bin/python3 analysis/ingest/ingest_markets.py      # ~15 min
.venv/bin/python3 analysis/ingest/ingest_schedule.py     # ~1 min
.venv/bin/python3 analysis/ingest/ingest_trades.py --seasons 2024,2025,2026 --workers 6   # ~11h, resumable
.venv/bin/python3 analysis/ingest/ingest_candles.py --seasons 2024,2025,2026 --workers 10 # ~45 min
.venv/bin/python3 analysis/ingest/build_lake.py           # raw JSONL → parquet
.venv/bin/python3 analysis/trader_metrics.py              # census + positions
```

Caveats: the ingest scripts use manual argv parsing — **never pass `--help`** (it silently starts a default sweep). The public Data-API is not a valid trade source (drops one counterparty leg per matched trade). `data/lake/state/` checkpoints are machine-local; transfer them together with `data/lake/raw/` or not at all.

Run `analysis/ingest/collect_orderbooks_daily.py` daily to keep preserving depth history before the upstream rolling window discards it.
