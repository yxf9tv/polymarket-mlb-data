# Report 10 — MLB-7 Consensus Analysis

Do the 7 persistent-winner wallets (`data/persistent_winners.json`) agree with each other on pre-game MLB markets, and is agreement predictive of settlement ROI? Pipeline: `analysis/mlb7_consensus.py`. Pre-game fills, cash-flow settlement ROI, dominant-token-by-net-signed-stake direction — same conventions as `analysis/persistent_winners.py`.

Wallets: `0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad`, `0x985c0516200c5a2c76dde58917ce2c852afde5d2`, `0x681d7946e0280c6ec6562c7ce662e90812354cbf`, `0x2934efa3cc0794953270262e9da00c04e21d0b51`, `0xfc234be3c4c3568284a566613ba90033b9d98283`, `0xa5dcb282cab760e31df1f3f5c18350731c95ec43`, `0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d`

**Post-review note:** `0xa5dcb282cab760e31df1f3f5c18350731c95ec43` ("Yikes110") was subsequently REMOVED from the MLB-7 by the external-reality screen (reports/08 — a -$262,176 lifetime, all-category loser) but the consensus stats below still include it; this analysis was not rerun. The direction of the conclusions is unchanged — the solo-dominance finding and the ~35% disagreement rate don't hinge on any one of the 7 wallets — but the exact numbers reflect 7 wallets, not the current 6.

## pooled 2025+2026

### Q1 — Overlap frequency

Total distinct pre-game markets touched by the 7: 2547

Distribution of markets by count of the 7 present:

| count of 7 present | n markets |
|---|---:|
| 1 | 1786 |
| 2 | 558 |
| 3+ | 203 |

Per-wallet same-MARKET overlap (fraction of own pre-game markets with >=1 other of the 7 also present):

| wallet | n markets | n with overlap | frac overlap |
|---|---:|---:|---:|
| `0x2934efa3cc0794953270262e9da00c04e21d0b51` | 594 | 379 | 63.8% |
| `0x681d7946e0280c6ec6562c7ce662e90812354cbf` | 279 | 161 | 57.7% |
| `0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad` | 1283 | 389 | 30.3% |
| `0x985c0516200c5a2c76dde58917ce2c852afde5d2` | 419 | 260 | 62.1% |
| `0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d` | 371 | 254 | 68.5% |
| `0xa5dcb282cab760e31df1f3f5c18350731c95ec43` | 313 | 164 | 52.4% |
| `0xfc234be3c4c3568284a566613ba90033b9d98283` | 289 | 155 | 53.6% |

Per-wallet same-GAME overlap (looser: >=1 other of the 7 anywhere in the same event, any market):

| wallet | n markets | n with game overlap | frac |
|---|---:|---:|---:|
| `0x2934efa3cc0794953270262e9da00c04e21d0b51` | 594 | 440 | 74.1% |
| `0x681d7946e0280c6ec6562c7ce662e90812354cbf` | 279 | 218 | 78.1% |
| `0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad` | 1283 | 784 | 61.1% |
| `0x985c0516200c5a2c76dde58917ce2c852afde5d2` | 419 | 334 | 79.7% |
| `0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d` | 371 | 269 | 72.5% |
| `0xa5dcb282cab760e31df1f3f5c18350731c95ec43` | 313 | 257 | 82.1% |
| `0xfc234be3c4c3568284a566613ba90033b9d98283` | 289 | 227 | 78.5% |

### Q2 — Agreement rate (when >=2 of the 7 share a market)

n markets with >=2 of the 7 present: **761**. Same side: 267. Opposite sides: 494. **Agreement rate: 35.1%** (n=761).

### Q3 — The money question: settlement ROI by consensus bucket

| bucket | n markets | n wallet-market rows | invested $ | ROI (dollar-wtd) | 95% CI |
|---|---:|---:|---:|---:|---|
| solo | 1787 | 1787 | $1,142,415 | +18.9% | [-2.6%, +48.7%] |
| 2_agree | 236 | 472 | $190,883 | +6.4% | [-11.0%, +23.0%] |
| 3+_agree | 30 | 92 | $114,068 | -60.3% | [-89.9%, +54.2%] |
| disagree | 494 | 1196 | $727,492 | -5.6% | [-24.4%, +15.6%] |

Disagreement markets: bigger-stake side wins settlement **47.8%** of the time (n=494).

### Q4 — Market-type mix of overlaps, and time-to-copy

- overlap markets (n=761): moneyline 92%, spread 4%, total 4%
- solo markets (n=1786): moneyline 49%, total 29%, spread 18%, nrfi 4%

Time gap between first and second wallet's entry in agreement markets: median **181 min** (IQR 75-336 min), n=267.

## 2025

### Q1 — Overlap frequency

Total distinct pre-game markets touched by the 7: 1017

Distribution of markets by count of the 7 present:

| count of 7 present | n markets |
|---|---:|
| 1 | 524 |
| 2 | 333 |
| 3+ | 160 |

Per-wallet same-MARKET overlap (fraction of own pre-game markets with >=1 other of the 7 also present):

| wallet | n markets | n with overlap | frac overlap |
|---|---:|---:|---:|
| `0x2934efa3cc0794953270262e9da00c04e21d0b51` | 405 | 295 | 72.8% |
| `0x681d7946e0280c6ec6562c7ce662e90812354cbf` | 178 | 123 | 69.1% |
| `0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad` | 396 | 218 | 55.0% |
| `0x985c0516200c5a2c76dde58917ce2c852afde5d2` | 206 | 161 | 78.2% |
| `0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d` | 296 | 217 | 73.3% |
| `0xa5dcb282cab760e31df1f3f5c18350731c95ec43` | 94 | 79 | 84.0% |
| `0xfc234be3c4c3568284a566613ba90033b9d98283` | 128 | 86 | 67.2% |

Per-wallet same-GAME overlap (looser: >=1 other of the 7 anywhere in the same event, any market):

| wallet | n markets | n with game overlap | frac |
|---|---:|---:|---:|
| `0x2934efa3cc0794953270262e9da00c04e21d0b51` | 405 | 306 | 75.6% |
| `0x681d7946e0280c6ec6562c7ce662e90812354cbf` | 178 | 143 | 80.3% |
| `0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad` | 396 | 293 | 74.0% |
| `0x985c0516200c5a2c76dde58917ce2c852afde5d2` | 206 | 168 | 81.5% |
| `0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d` | 296 | 220 | 74.3% |
| `0xa5dcb282cab760e31df1f3f5c18350731c95ec43` | 94 | 81 | 86.2% |
| `0xfc234be3c4c3568284a566613ba90033b9d98283` | 128 | 103 | 80.5% |

### Q2 — Agreement rate (when >=2 of the 7 share a market)

n markets with >=2 of the 7 present: **493**. Same side: 165. Opposite sides: 328. **Agreement rate: 33.5%** (n=493).

### Q3 — The money question: settlement ROI by consensus bucket

| bucket | n markets | n wallet-market rows | invested $ | ROI (dollar-wtd) | 95% CI |
|---|---:|---:|---:|---:|---|
| solo | 525 | 525 | $335,558 | +48.9% | [-18.5%, +120.3%] |
| 2_agree | 142 | 284 | $124,292 | +12.3% | [-9.8%, +32.6%] |
| 3+_agree | 22 | 68 | $106,133 | -63.5% **UNDERPOWERED (n<30)** | [-93.3%, +66.1%] |
| disagree | 328 | 825 | $548,557 | -4.8% | [-30.4%, +21.6%] |

Disagreement markets: bigger-stake side wins settlement **48.8%** of the time (n=328).

### Q4 — Market-type mix of overlaps, and time-to-copy

- overlap markets (n=493): moneyline 97%, spread 1%, total 1%
- solo markets (n=524): moneyline 73%, total 18%, spread 10%

Time gap between first and second wallet's entry in agreement markets: median **223 min** (IQR 89-378 min), n=165.

## 2026

### Q1 — Overlap frequency

Total distinct pre-game markets touched by the 7: 1530

Distribution of markets by count of the 7 present:

| count of 7 present | n markets |
|---|---:|
| 1 | 1262 |
| 2 | 225 |
| 3+ | 43 |

Per-wallet same-MARKET overlap (fraction of own pre-game markets with >=1 other of the 7 also present):

| wallet | n markets | n with overlap | frac overlap |
|---|---:|---:|---:|
| `0x2934efa3cc0794953270262e9da00c04e21d0b51` | 189 | 84 | 44.4% |
| `0x681d7946e0280c6ec6562c7ce662e90812354cbf` | 101 | 38 | 37.6% |
| `0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad` | 887 | 171 | 19.3% |
| `0x985c0516200c5a2c76dde58917ce2c852afde5d2` | 213 | 99 | 46.5% |
| `0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d` | 75 | 37 | 49.3% |
| `0xa5dcb282cab760e31df1f3f5c18350731c95ec43` | 219 | 85 | 38.8% |
| `0xfc234be3c4c3568284a566613ba90033b9d98283` | 161 | 69 | 42.9% |

Per-wallet same-GAME overlap (looser: >=1 other of the 7 anywhere in the same event, any market):

| wallet | n markets | n with game overlap | frac |
|---|---:|---:|---:|
| `0x2934efa3cc0794953270262e9da00c04e21d0b51` | 189 | 134 | 70.9% |
| `0x681d7946e0280c6ec6562c7ce662e90812354cbf` | 101 | 75 | 74.3% |
| `0x87531c465a5a729850e1e2c4fd4af3dcb87d85ad` | 887 | 491 | 55.4% |
| `0x985c0516200c5a2c76dde58917ce2c852afde5d2` | 213 | 166 | 77.9% |
| `0x9a7f417c09c2d14f7b425b4bd38e9d6311084b5d` | 75 | 49 | 65.3% |
| `0xa5dcb282cab760e31df1f3f5c18350731c95ec43` | 219 | 176 | 80.4% |
| `0xfc234be3c4c3568284a566613ba90033b9d98283` | 161 | 124 | 77.0% |

### Q2 — Agreement rate (when >=2 of the 7 share a market)

n markets with >=2 of the 7 present: **268**. Same side: 102. Opposite sides: 166. **Agreement rate: 38.1%** (n=268).

### Q3 — The money question: settlement ROI by consensus bucket

| bucket | n markets | n wallet-market rows | invested $ | ROI (dollar-wtd) | 95% CI |
|---|---:|---:|---:|---:|---|
| solo | 1262 | 1262 | $806,857 | +6.4% | [-2.0%, +14.0%] |
| 2_agree | 94 | 188 | $66,591 | -4.5% | [-29.5%, +20.6%] |
| 3+_agree | 8 | 24 | $7,935 | -18.3% **UNDERPOWERED (n<30)** | [-76.1%, +40.2%] |
| disagree | 166 | 371 | $178,936 | -8.0% | [-20.8%, +4.6%] |

Disagreement markets: bigger-stake side wins settlement **45.8%** of the time (n=166).

### Q4 — Market-type mix of overlaps, and time-to-copy

- overlap markets (n=268): moneyline 82%, spread 9%, total 9%
- solo markets (n=1262): moneyline 40%, total 34%, spread 22%, nrfi 5%

Time gap between first and second wallet's entry in agreement markets: median **125 min** (IQR 62-256 min), n=102.
