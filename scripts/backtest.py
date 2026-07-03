"""Backtest trader sentiment convictions against actual outcomes.

Reads historical sentiment scores, resolves actual outcomes via Agent 574 API,
and computes accuracy metrics across conviction thresholds and market types.
"""

import json, time, os, sys, re
from pathlib import Path
from dotenv import load_dotenv

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
CACHE_DIR = DATA_DIR / "cache"
SENTIMENT_PATH = DATA_DIR / "sentiment_scores.json"
CACHE_PATH = CACHE_DIR / "outcome_cache.json"
OUTPUT_PATH = DATA_DIR / "backtest_results.json"

API_URL = "https://narrative.agent.heisenberg.so/api/v2/semantic/retrieve/parameterized"

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})$")


def load_env_key():
    load_dotenv()
    key = os.getenv("intelligence_api_key")
    if not key:
        print("ERROR: intelligence_api_key not found in .env", file=sys.stderr)
        sys.exit(1)
    return key


def load_cache():
    if CACHE_PATH.exists():
        with open(CACHE_PATH) as f:
            return json.load(f)
    return {}


def save_cache(cache):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2)


def query_resolved_outcome(condition_id, api_key):
    """Query Agent 574 for a resolved market's winning outcome."""
    import requests
    body = {
        "agent_id": 574,
        "params": {"condition_id": condition_id, "closed": "True"},
        "formatter_config": {"format_type": "raw"},
    }
    try:
        resp = requests.post(
            API_URL, json=body,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            timeout=30,
        )
        if resp.status_code != 200:
            return None
        results = (resp.json().get("data") or {}).get("results") or []
        if results:
            return results[0].get("winning_outcome")
    except Exception:
        pass
    return None


def resolve_markets(markets, api_key):
    """Resolve outcomes for markets via API with caching. Returns count of newly resolved."""
    cache = load_cache()
    new_resolved = 0

    for m in markets:
        cid = m.get("condition_id", "")
        if not cid:
            continue
        if cid in cache:
            m["actual_outcome"] = cache[cid]["winning_outcome"]
            m["correct"] = (m["top_outcome"] == cache[cid]["winning_outcome"])
            continue

        winning = query_resolved_outcome(cid, api_key)
        if winning:
            cache[cid] = {
                "winning_outcome": winning,
                "resolved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            m["actual_outcome"] = winning
            m["correct"] = (m["top_outcome"] == winning)
            new_resolved += 1
            time.sleep(0.5)

    if new_resolved:
        save_cache(cache)

    return new_resolved


def extract_game_date(market):
    slug = market.get("event_slug") or market.get("slug", "")
    m = DATE_RE.search(slug)
    return m.group(1) if m else market.get("first_trade_date", "")


def compute_stats(predictions):
    """Compute backtest statistics across thresholds and breakdowns."""
    resolved = [p for p in predictions if p.get("correct") is not None]
    n_resolved = len(resolved)
    total = len(predictions)

    if n_resolved == 0:
        return {"total_markets": total, "resolved": 0}

    correct = sum(1 for p in resolved if p["correct"])
    incorrect = n_resolved - correct
    accuracy = correct / n_resolved

    # By conviction threshold (cumulative: >= X)
    thresholds = [0.8, 0.9, 0.95, 0.99, 1.0]
    by_threshold = {}
    for t in thresholds:
        subset = [p for p in resolved if p.get("conviction", 0) >= t]
        n = len(subset)
        c = sum(1 for p in subset if p["correct"])
        by_threshold[str(t)] = {
            "total": sum(1 for p in predictions if p.get("conviction", 0) >= t),
            "resolved": n,
            "correct": c,
            "incorrect": n - c,
            "accuracy": round(c / n, 4) if n else 0,
        }

    # By market type
    by_type = {}
    for p in predictions:
        mt = p.get("market_type", "other")
        if mt not in by_type:
            by_type[mt] = {"total": 0, "resolved": 0, "correct": 0, "incorrect": 0}
        by_type[mt]["total"] += 1
        if p.get("correct") is not None:
            by_type[mt]["resolved"] += 1
            if p["correct"]:
                by_type[mt]["correct"] += 1
            else:
                by_type[mt]["incorrect"] += 1
    for mt, d in by_type.items():
        d["accuracy"] = round(d["correct"] / d["resolved"], 4) if d["resolved"] else 0

    # By conviction band (non-overlapping buckets)
    bands = [(0.8, 0.9), (0.9, 0.95), (0.95, 0.99), (0.99, 1.01)]
    by_band = {}
    for lo, hi in bands:
        subset = [p for p in resolved if lo <= p.get("conviction", 0) < hi]
        n = len(subset)
        c = sum(1 for p in subset if p["correct"])
        label = f"{lo:.2f}-{min(hi,1):.2f}" if hi < 1.01 else "1.00"
        by_band[label] = {
            "total": sum(1 for p in predictions if lo <= p.get("conviction", 0) < hi),
            "resolved": n,
            "correct": c,
            "incorrect": n - c,
            "accuracy": round(c / n, 4) if n else 0,
        }

    # Simulated PnL (even $1 stakes, even odds)
    simulated_pnl = correct - incorrect

    return {
        "total_markets": total,
        "resolved": n_resolved,
        "correct": correct,
        "incorrect": incorrect,
        "accuracy": round(accuracy, 4),
        "simulated_pnl": simulated_pnl,
        "simulated_roi": round(simulated_pnl / n_resolved * 100, 2) if n_resolved else 0,
        "by_threshold": by_threshold,
        "by_market_type": by_type,
        "by_conviction_band": by_band,
    }


def main(threshold=0.8):
    import requests

    api_key = load_env_key()

    if not SENTIMENT_PATH.exists():
        print(f"ERROR: sentiment data not found at {SENTIMENT_PATH}")
        sys.exit(1)

    with open(SENTIMENT_PATH) as f:
        sentiment = json.load(f)

    markets = sentiment.get("by_market", [])
    print(f"Loaded {len(markets)} historical markets "
          f"({sentiment['summary']['data_start_date']} to {sentiment['summary']['data_end_date']})")

    candidates = [m for m in markets if m.get("conviction", 0) >= threshold]
    print(f"Markets with conviction >= {threshold}: {len(candidates)}")

    cache = load_cache()
    cached_count = sum(1 for m in candidates if m.get("condition_id", "") in cache)
    print(f"Already cached: {cached_count}, need resolution: {len(candidates) - cached_count}")

    new_res = resolve_markets(candidates, api_key)
    print(f"Newly resolved: {new_res}")

    predictions = []
    for m in candidates:
        predictions.append({
            "slug": m.get("slug", ""),
            "event_slug": m.get("event_slug", ""),
            "game_date": extract_game_date(m),
            "market_type": m.get("market_type", "other"),
            "predicted_outcome": m.get("top_outcome", ""),
            "actual_outcome": m.get("actual_outcome", None),
            "conviction": m.get("conviction", 0),
            "total_weighted_volume": m.get("total_weighted_volume", 0),
            "unique_traders": m.get("unique_traders", 0),
            "condition_id": m.get("condition_id", ""),
            "correct": m.get("correct", None),
        })

    stats = compute_stats(predictions)

    output = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "data_range": {
            "start": sentiment["summary"]["data_start_date"],
            "end": sentiment["summary"]["data_end_date"],
        },
        "threshold": threshold,
        "stats": stats,
        "predictions": predictions,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n{'='*60}")
    print(f"BACKTEST RESULTS (conviction >= {threshold})")
    print(f"{'='*60}")
    print(f"Total markets:   {stats['total_markets']}")
    resolved = stats['resolved']
    print(f"Resolved:        {resolved} / {stats['total_markets']}")
    if resolved:
        print(f"Correct:         {stats['correct']}")
        print(f"Incorrect:       {stats['incorrect']}")
        print(f"Accuracy:        {stats['accuracy']:.1%}")
        print(f"Simulated PnL:   ${stats['simulated_pnl']} ({stats['simulated_roi']}% ROI)")

        print(f"\nBy threshold:")
        for t, d in stats["by_threshold"].items():
            if d["resolved"]:
                print(f"  >= {float(t):.0%}: {d['accuracy']:.1%} ({d['correct']}/{d['resolved']})")

        print(f"\nBy market type:")
        for mt, d in sorted(stats["by_market_type"].items()):
            if d["resolved"]:
                print(f"  {mt:15s}: {d['accuracy']:.1%} ({d['correct']}/{d['resolved']})  [{d['total']} total]")

        print(f"\nBy conviction band:")
        for b, d in stats["by_conviction_band"].items():
            if d["resolved"]:
                print(f"  {b:>8s}: {d['accuracy']:.1%} ({d['correct']}/{d['resolved']})")

    print(f"\nResults saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    thresh = float(sys.argv[1]) if len(sys.argv) > 1 else 0.8
    main(thresh)
