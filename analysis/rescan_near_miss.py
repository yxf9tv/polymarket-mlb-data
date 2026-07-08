#!/usr/bin/env python3
"""Post-review rescan: near-miss candidates for the MLB persistent-winners list.

User-approved follow-up to the external-reality screen (see reports/08's
"External-reality screen (post-review)" section). Two questions:

  (a) Applying analysis/wallet_screen.py's external screen to the FULL
      stage-3 pool (wallets with positive full AND pre-game settlement ROI
      in both 2025 and 2026, BEFORE the bot/stake filters -- 23 wallets at
      the strict n>=50 floor), does anyone the internal criteria 4/5
      rejected look externally healthy enough to be worth a second look?
  (b) Same question with the eligibility floor relaxed to n>=30 pre-game
      markets per period (which widens stage 3 to 29 wallets).

Near-miss candidates = stage-3 pool at the n>=30 floor, MINUS the 7 wallets
already on the main list (they were screened in persistent_winners.py; their
verdicts are reused from data/persistent_winners.json, not re-fetched).

"Convincing external pass" (documented, deliberately strict): screen verdict
OK, lifetime all-category PnL >= +$1,000 ("clearly positive"), AND
(current deployed value > $1,000 OR traded in June 2026 or later).

For every candidate we report which STRICT internal criterion(s) they
failed and by how much, plus per-period pre-game ROI/n -- so the reader can
judge whether the internal rejection was marginal or decisive.

SECOND-TIER, EXPLICITLY: nothing here promotes anyone into the main 6.
This writes:
  - `near_miss_candidates` array (+ `near_miss_rescan_meta`) into
    data/persistent_winners.json
  - a "Rescan: near-miss candidates (post-review)" section appended to
    reports/08_persistent_winners.md (idempotent: replaces its own section
    if already present). NOTE: rerunning analysis/persistent_winners.py
    regenerates reports/08 wholesale and drops this section -- rerun this
    script afterwards to re-append it.

Usage:
    .venv/bin/python3 analysis/rescan_near_miss.py
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parent.parent
DATA_DIR = BASE / "data"
REPORTS_DIR = BASE / "reports"
sys.path.insert(0, str(BASE / "analysis"))
import persistent_winners as pw  # noqa: E402  (funnel logic reused verbatim)
from wallet_screen import screen_wallets  # noqa: E402

JSON_PATH = DATA_DIR / "persistent_winners.json"
REPORT_PATH = REPORTS_DIR / "08_persistent_winners.md"

STRICT_FLOOR = pw.PREGAME_N_FLOOR  # 50
RELAXED_FLOOR = 30

CONVINCING_MIN_LIFETIME_PNL = 1_000.0
CONVINCING_MIN_VALUE = 1_000.0
CONVINCING_RECENT_TRADE_CUTOFF = pd.Timestamp("2026-06-01", tz="UTC")

SECTION_HEADER = "## Rescan: near-miss candidates (post-review)"


# ---------------------------------------------------------------------------
# Funnel pools (reuses persistent_winners.py's summaries verbatim)
# ---------------------------------------------------------------------------
def stage_pools(pg_s: dict, full_s: dict, floor: int) -> dict[str, set]:
    elig = set.intersection(*[set(pg_s[s][pg_s[s]["n_markets"] >= floor]["wallet"]) for s in pw.SEASONS])
    p2 = elig & set.intersection(*[set(full_s[s][full_s[s]["roi_pooled"] > 0]["wallet"]) for s in pw.SEASONS])
    p3 = p2 & set.intersection(*[set(pg_s[s][pg_s[s]["roi_pooled"] > 0]["wallet"]) for s in pw.SEASONS])
    return {"1_active": elig, "2_full_roi": p2, "3_pregame_roi": p3}


# ---------------------------------------------------------------------------
# Strict-criteria failure analysis per candidate
# ---------------------------------------------------------------------------
def failed_criteria(wallet: str, pg_idx: dict, trader_row, notional: float | None) -> list[str]:
    fails = []
    n25 = int(pg_idx[2025].loc[wallet, "n_markets"]) if wallet in pg_idx[2025].index else 0
    n26 = int(pg_idx[2026].loc[wallet, "n_markets"]) if wallet in pg_idx[2026].index else 0
    if n25 < STRICT_FLOOR or n26 < STRICT_FLOOR:
        short = [f"{y}: n={n} (short {STRICT_FLOOR - n})"
                 for y, n in ((2025, n25), (2026, n26)) if n < STRICT_FLOOR]
        fails.append(f"criterion 1 (n>={STRICT_FLOOR} pregame both periods): " + "; ".join(short))
    if trader_row is not None:
        ts = float(trader_row["two_sided_share"])
        ent = float(trader_row["active_hours_entropy"])
        gap = float(trader_row["median_inter_trade_seconds"])
        if ts >= pw.BOT_TWO_SIDED_MAX:
            fails.append(f"criterion 4 (two_sided_share<{pw.BOT_TWO_SIDED_MAX}): {ts:.3f} "
                         f"(over by {ts - pw.BOT_TWO_SIDED_MAX:.3f})")
        if ent >= pw.BOT_ENTROPY_MAX:
            fails.append(f"criterion 4 (active_hours_entropy<{pw.BOT_ENTROPY_MAX}): {ent:.3f} "
                         f"(over by {ent - pw.BOT_ENTROPY_MAX:.3f})")
        if gap < pw.BOT_MIN_MEDIAN_GAP_S:
            fails.append(f"criterion 4 (median_inter_trade_seconds>={pw.BOT_MIN_MEDIAN_GAP_S:.0f}s): "
                         f"{gap:.0f}s (short {pw.BOT_MIN_MEDIAN_GAP_S - gap:.0f}s)")
    else:
        fails.append("criterion 4: no pooled bot features in traders.parquet (cannot evaluate)")
    if notional is None:
        fails.append("criterion 5: median fill notional unavailable")
    elif notional < pw.MIN_MEDIAN_FILL_NOTIONAL:
        fails.append(f"criterion 5 (median notional>=${pw.MIN_MEDIAN_FILL_NOTIONAL:.0f}): "
                     f"${notional:.2f} (short ${pw.MIN_MEDIAN_FILL_NOTIONAL - notional:.2f})")
    return fails


def is_convincing(screen: dict) -> bool:
    if screen["verdict"] != "OK" or screen["lifetime_pnl"] is None:
        return False
    if screen["lifetime_pnl"] < CONVINCING_MIN_LIFETIME_PNL:
        return False
    value_ok = screen["current_value"] is not None and screen["current_value"] > CONVINCING_MIN_VALUE
    recent_ok = screen["last_trade"] is not None and screen["last_trade"] >= CONVINCING_RECENT_TRADE_CUTOFF
    return value_ok or recent_ok


# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
def build_record(wallet: str, pg_idx: dict, full_idx: dict, fails: list[str], screen: dict) -> dict:
    per_period = {}
    for s in pw.SEASONS:
        in_pg = wallet in pg_idx[s].index
        per_period[str(s)] = {
            "n_markets_pregame": int(pg_idx[s].loc[wallet, "n_markets"]) if in_pg else 0,
            "roi_pregame": round(float(pg_idx[s].loc[wallet, "roi_pooled"]), 4) if in_pg else None,
            "roi_full": round(float(full_idx[s].loc[wallet, "roi_pooled"]), 4)
            if wallet in full_idx[s].index else None,
        }
    profit = sum(float(full_idx[s].loc[wallet, "profit"]) for s in pw.SEASONS if wallet in full_idx[s].index)
    return {
        "wallet": wallet,
        "tier": "second-tier near-miss (NOT promoted; user decides)",
        "periods": per_period,
        "total_profit_full": round(profit, 2),
        "failed_strict_criteria": fails,
        "external_screen": {
            "pseudonym": screen["pseudonym"],
            "lifetime_pnl": screen["lifetime_pnl"],
            "current_value": screen["current_value"],
            "last_trade": screen["last_trade"].isoformat() if screen["last_trade"] is not None else None,
            "days_since_last_trade": screen["days_since_last_trade"],
            "stale": screen["stale"],
            "screen_verdict": screen["verdict"],
            "notes": screen["notes"],
        },
        "convincing_external_pass": is_convincing(screen),
    }


def update_json(records: list[dict], funnel_relaxed: dict, n_candidates: int) -> None:
    with open(JSON_PATH) as f:
        data = json.load(f)
    data["near_miss_rescan_meta"] = {
        "generated_at": pd.Timestamp.now("UTC").isoformat(),
        "pipeline": "analysis/rescan_near_miss.py",
        "definition": f"stage-3 pool (positive full & pregame ROI both periods, BEFORE bot/stake "
                      f"filters) at relaxed n>={RELAXED_FLOOR} floor, minus the 7 main-list wallets",
        "n_candidates_screened": n_candidates,
        "relaxed_funnel": funnel_relaxed,
        "convincing_pass_definition": f"verdict OK AND lifetime PnL >= ${CONVINCING_MIN_LIFETIME_PNL:,.0f} "
                                       f"AND (current value > ${CONVINCING_MIN_VALUE:,.0f} OR traded on/after "
                                       f"{CONVINCING_RECENT_TRADE_CUTOFF.date()})",
        "tier": "SECOND-TIER: nothing here is promoted into the main list; user decides",
    }
    data["near_miss_candidates"] = records
    with open(JSON_PATH, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"Updated {JSON_PATH} (near_miss_candidates: {len(records)})")


def section_lines(records: list[dict], funnel_relaxed: dict) -> list[str]:
    lines = [SECTION_HEADER, ""]
    lines.append(
        "User-approved follow-up to the external-reality screen above (`analysis/rescan_near_miss.py`): "
        "apply the same external screen to the FULL stage-3 pool -- every wallet with positive full AND "
        "pre-game settlement ROI in both periods, BEFORE the bot/stake filters (23 wallets at the strict "
        f"n>={STRICT_FLOOR} floor), and again with the per-period eligibility floor relaxed to "
        f"n>={RELAXED_FLOOR} (29 wallets). The question: did criteria 4/5 (or the strict n-floor) reject "
        "anyone who looks externally healthy -- i.e. are we leaving defensible candidates on the table? "
        "**Everything in this section is SECOND-TIER: no one here is promoted into the operative 6-wallet "
        "list. Promotion is a user decision.**\n"
    )
    lines.append(f"Relaxed funnel (floor n>={RELAXED_FLOOR}, same criteria 2-5 and external screen):\n")
    lines.append("| stage | n wallets |\n|---|---:|")
    for k, v in funnel_relaxed.items():
        lines.append(f"| {k} | {v} |")
    lines.append("")
    lines.append(f"**Convincing external pass** = verdict OK, lifetime PnL >= "
                 f"${CONVINCING_MIN_LIFETIME_PNL:,.0f}, and (current value > "
                 f"${CONVINCING_MIN_VALUE:,.0f} or traded on/after {CONVINCING_RECENT_TRADE_CUTOFF.date()}).\n")
    lines.append("| wallet | pseudonym | lifetime PnL | current value | last trade | verdict | "
                 "convincing | 2025 pregame ROI (n) | 2026 pregame ROI (n) | profit $ | failed strict criteria |")
    lines.append("|---|---|---:|---:|---|---|---|---|---|---:|---|")
    for r in records:
        es = r["external_screen"]
        p25, p26 = r["periods"]["2025"], r["periods"]["2026"]
        lp = f"${es['lifetime_pnl']:,.0f}" if es["lifetime_pnl"] is not None else "n/a"
        cv = f"${es['current_value']:,.0f}" if es["current_value"] is not None else "n/a"
        lt = es["last_trade"][:10] if es["last_trade"] else "n/a"
        roi25 = f"{p25['roi_pregame']:+.1%} ({p25['n_markets_pregame']})" if p25["roi_pregame"] is not None else "n/a"
        roi26 = f"{p26['roi_pregame']:+.1%} ({p26['n_markets_pregame']})" if p26["roi_pregame"] is not None else "n/a"
        fails = "; ".join(r["failed_strict_criteria"]) or "none (passed all strict criteria?)"
        lines.append(f"| `{r['wallet']}` | {es['pseudonym'] or 'n/a'} | {lp} | {cv} | {lt} | "
                     f"{es['screen_verdict']} | {'**YES**' if r['convincing_external_pass'] else 'no'} | "
                     f"{roi25} | {roi26} | {r['total_profit_full']:,.0f} | {fails} |")
    lines.append("")
    n_conv = sum(1 for r in records if r["convincing_external_pass"])
    lines.append(f"{n_conv}/{len(records)} candidates pass the external screen convincingly. Full records "
                 "(per-period detail, screen notes) in `data/persistent_winners.json` -> "
                 "`near_miss_candidates`. NOTE: rerunning `analysis/persistent_winners.py` regenerates this "
                 "report without this section -- rerun `analysis/rescan_near_miss.py` to re-append it.\n")
    return lines


def append_or_replace_section(lines: list[str]) -> None:
    text = REPORT_PATH.read_text()
    block = "\n".join(lines)
    if SECTION_HEADER in text:
        head, _, tail = text.partition(SECTION_HEADER)
        # our section runs to the next "## " header after it, or EOF
        rest = tail.split("\n## ", 1)
        remainder = ("\n## " + rest[1]) if len(rest) > 1 else "\n"
        new_text = head + block + remainder
    else:
        new_text = text.rstrip("\n") + "\n\n" + block
    REPORT_PATH.write_text(new_text)
    print(f"Updated {REPORT_PATH} ({SECTION_HEADER!r})")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    t0 = time.time()
    with open(JSON_PATH) as f:
        main_json = json.load(f)
    main7 = {r["wallet"] for r in main_json["survivors"]}
    print(f"Main-list wallets (already screened, excluded from rescan): {len(main7)}")

    lake = pw.load_lake()
    positions, markets, traders, pregame = (
        lake["positions"], lake["markets"], lake["traders"], lake["pregame"]
    )
    full_wm = pw.full_period_wm(positions, markets)
    pg_wm = pw.pregame_wm(pregame)
    full_s = {s: pw.wallet_season_summary(full_wm[s]) for s in pw.SEASONS}
    pg_s = {s: pw.wallet_season_summary(pg_wm[s]) for s in pw.SEASONS}
    full_idx = {s: full_s[s].set_index("wallet") for s in pw.SEASONS}
    pg_idx = {s: pg_s[s].set_index("wallet") for s in pw.SEASONS}

    pools_strict = stage_pools(pg_s, full_s, STRICT_FLOOR)
    pools_relaxed = stage_pools(pg_s, full_s, RELAXED_FLOOR)
    print(f"Stage-3 pool: strict n>={STRICT_FLOOR}: {len(pools_strict['3_pregame_roi'])}; "
          f"relaxed n>={RELAXED_FLOOR}: {len(pools_relaxed['3_pregame_roi'])}")

    # Relaxed funnel through criteria 4/5 for the report table
    trader_idx = traders.set_index("wallet")
    bot_ok = set(traders[pw.bot_filter_mask(traders)]["wallet"])
    pool4_relaxed = pools_relaxed["3_pregame_roi"] & bot_ok
    notional_df = pw.median_fill_notional(sorted(pools_relaxed["3_pregame_roi"]))
    notional_idx = notional_df.set_index("wallet")["median_notional"] if not notional_df.empty else pd.Series(dtype=float)
    stake_ok = set(notional_df[notional_df["median_notional"] >= pw.MIN_MEDIAN_FILL_NOTIONAL]["wallet"])
    pool5_relaxed = pool4_relaxed & stake_ok
    funnel_relaxed = {
        f"1_active_both_periods_n>={RELAXED_FLOOR}": len(pools_relaxed["1_active"]),
        "2_positive_full_roi_both": len(pools_relaxed["2_full_roi"]),
        "3_positive_pregame_roi_both": len(pools_relaxed["3_pregame_roi"]),
        "4_not_bot_shaped": len(pool4_relaxed),
        "5_median_notional>=20": len(pool5_relaxed),
    }
    print(f"Relaxed funnel: {funnel_relaxed}")

    candidates = sorted(pools_relaxed["3_pregame_roi"] - main7)
    print(f"Near-miss candidates to screen (stage-3 relaxed minus main {len(main7)}): {len(candidates)}")

    category_profit = {
        w: sum(float(full_idx[s].loc[w, "profit"]) for s in pw.SEASONS if w in full_idx[s].index)
        for w in candidates
    }
    screens = screen_wallets(candidates, category_profit=category_profit)

    records = []
    for w in candidates:
        trow = trader_idx.loc[w] if w in trader_idx.index else None
        notion = float(notional_idx.loc[w]) if w in notional_idx.index else None
        fails = failed_criteria(w, pg_idx, trow, notion)
        records.append(build_record(w, pg_idx, full_idx, fails, screens[w]))
    records.sort(key=lambda r: (not r["convincing_external_pass"],
                                 -(r["external_screen"]["lifetime_pnl"] or -1e12)))

    update_json(records, funnel_relaxed, len(candidates))
    append_or_replace_section(section_lines(records, funnel_relaxed))

    n_conv = sum(1 for r in records if r["convincing_external_pass"])
    print(f"\nDone: {len(records)} candidates screened, {n_conv} convincing external passes. "
          f"Elapsed {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
