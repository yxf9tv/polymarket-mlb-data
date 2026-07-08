#!/bin/bash
# Sequential NFL -> NBA trades sweep, each followed by its parquet build.
# Sequential (not parallel) because both sweeps hit the same intelligence-API
# rate limit - running them together would just split the same throughput
# while doubling 429 pressure. NFL first (smaller universe: ~15.5k markets
# vs NBA ~22.8k). Both ingest scripts are fully resumable (IngestState), so
# re-running this script after a crash/kill picks up where it left off.
set -u
cd /Users/bzcruz/projects/Sentiment3
LOGDIR=data/lake/logs
mkdir -p "$LOGDIR"
TS=$(date +%Y%m%d_%H%M%S)

echo "=== NFL trades sweep start $(date -u +%FT%TZ) ===" >> "$LOGDIR/trades_nfl_$TS.log"
.venv/bin/python3 analysis/ingest/ingest_trades_sport.py --sport nfl --workers 6 \
  >> "$LOGDIR/trades_nfl_$TS.log" 2>&1
.venv/bin/python3 analysis/ingest/build_lake.py --sport nfl --tables trades \
  >> "$LOGDIR/trades_nfl_$TS.log" 2>&1
echo "=== NFL done $(date -u +%FT%TZ) ===" >> "$LOGDIR/trades_nfl_$TS.log"

echo "=== NBA trades sweep start $(date -u +%FT%TZ) ===" >> "$LOGDIR/trades_nba_$TS.log"
.venv/bin/python3 analysis/ingest/ingest_trades_sport.py --sport nba --workers 6 \
  >> "$LOGDIR/trades_nba_$TS.log" 2>&1
.venv/bin/python3 analysis/ingest/build_lake.py --sport nba --tables trades \
  >> "$LOGDIR/trades_nba_$TS.log" 2>&1
echo "=== NBA done $(date -u +%FT%TZ) ===" >> "$LOGDIR/trades_nba_$TS.log"

echo "ALL SWEEPS COMPLETE $(date -u +%FT%TZ)" >> "$LOGDIR/trades_sweeps_done_$TS.log"
