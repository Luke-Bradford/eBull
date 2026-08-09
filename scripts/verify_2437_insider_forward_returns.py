"""Do insider PURCHASES predict forward returns on OUR data? (#2437)

    PYTHONPATH=. uv run python scripts/verify_2437_insider_forward_returns.py

⚠ NOTHING IS WRITTEN. Gate on the EXIT CODE. Never pipe into head/tail.

WHY
---
`quant/strategy-evidence.md` ranks opportunistic insider purchases as the
best-evidenced return signal we could actually build: Cohen/Malloy/Pomorski put
opportunistic trades at 82 bps/month and routine ones at zero;
Jeng/Metrick/Zeckhauser at ~11.2%/yr on purchases, with **sales showing no
effect**. All of that is other people's data. This is ours.

⚠⚠ THIS IS A FIRST LOOK, NOT A BACKTEST. It deliberately does the cheapest honest
version so the answer arrives before anything is built on the assumption:

  - **Purchases only** (`txn_code = 'P'`). Sales carry no signal in the
    literature and including them would dilute a real effect.
  - **Forward return measured from the NEXT bar's close after the transaction
    date**, never the transaction bar itself. ⚠ A Form 4 is filed up to two
    business days later, so even this is OPTIMISTIC about what was knowable —
    the honest version keys on the FILING timestamp, which this does not have
    wired. Read the result as an upper bound.
  - **A matched control**: the same instrument's return over the same horizon
    starting from a random other date. Without it, a positive number just says
    "stocks went up 2004-2026".
  - **Year-clustered inference** — the correction that erased our momentum
    finding (`quant/measurement-discipline.md` §1.1). Pooling 47k events as
    independent would produce a spectacular and meaningless t.

⚠ NOT done here, and all of it matters before this becomes a strategy:
routine-vs-opportunistic classification (only ~10% of insiders have the history),
costs, liquidity screens, market/beta adjustment, and the filing-date key.
"""

from __future__ import annotations

import sys
from collections import defaultdict

import numpy as np
import psycopg

from app.config import settings

HORIZONS = (21, 63, 126, 252)  # 1m, 3m, 6m, 12m

_SQL = """
    WITH purchases AS (
        SELECT DISTINCT it.instrument_id, it.txn_date, s.series_id
        FROM insider_transactions it
        JOIN research_price_series s ON s.instrument_id = it.instrument_id
        WHERE it.txn_code = 'P'
          AND NOT it.txn_date_invalid
          AND it.txn_date BETWEEN '2004-01-01' AND '2025-06-30'
    )
    SELECT series_id, txn_date FROM purchases ORDER BY series_id, txn_date
"""

_BARS = """
    SELECT bar_date, adj_close
    FROM research_price_daily
    WHERE series_id = %(sid)s AND adj_close > 0
    ORDER BY bar_date
"""


def main() -> int:
    rng = np.random.default_rng(20260809)
    with psycopg.connect(settings.database_url) as conn:
        events = conn.execute(_SQL).fetchall()
        print(f"{len(events):,} insider purchase events with a price series", flush=True)
        by_series: dict[int, list] = defaultdict(list)
        for sid, d in events:
            by_series[sid].append(d)

        # horizon -> year -> [event_return, control_return]
        buckets: dict[int, dict[int, list[tuple[float, float]]]] = {k: defaultdict(list) for k in HORIZONS}
        used = matched = 0
        for n, (sid, dates) in enumerate(by_series.items(), start=1):
            rows = conn.execute(_BARS, {"sid": sid}).fetchall()
            if len(rows) < max(HORIZONS) + 40:
                continue
            bar_dates = [r[0] for r in rows]
            px = np.asarray([float(r[1]) for r in rows])
            index = {d: i for i, d in enumerate(bar_dates)}
            used += 1
            for d in dates:
                i = index.get(d)
                if i is None:  # transaction on a non-trading day
                    nxt = [j for j, bd in enumerate(bar_dates) if bd > d]
                    if not nxt:
                        continue
                    i = nxt[0]
                entry = i + 1  # ⚠ NEXT bar, never the transaction bar
                for k in HORIZONS:
                    if entry + k >= len(px):
                        continue
                    ev = px[entry + k] / px[entry] - 1.0
                    # matched control: same series, same horizon, random other start
                    lo, hi = 0, len(px) - k - 1
                    if hi <= lo:
                        continue
                    c = int(rng.integers(lo, hi))
                    ct = px[c + k] / px[c] - 1.0
                    buckets[k][d.year].append((float(ev), float(ct)))
                    matched += 1
            if n % 500 == 0:
                print(f"  {n}/{len(by_series)} series", flush=True)

    print(f"\nseries used {used:,}   event-horizon observations {matched:,}\n")
    print("INSIDER PURCHASE FORWARD RETURN vs MATCHED SAME-SERIES CONTROL")
    print("⚠ year-clustered: mean computed WITHIN each year, then across years\n")
    print(f"{'horizon':>8}{'events':>10}{'event ret':>12}{'control':>11}{'EXCESS':>11}{'t':>8}{'years':>7}")
    print("-" * 67)
    failures = 0
    for k in HORIZONS:
        per_year = []
        total = 0
        for _year, pairs in sorted(buckets[k].items()):
            if len(pairs) < 30:
                continue
            arr = np.asarray(pairs)
            per_year.append(float(arr[:, 0].mean() - arr[:, 1].mean()))
            total += len(pairs)
        if len(per_year) < 5:
            print(f"{k:>8}{'insufficient years':>50}")
            continue
        a = np.asarray(per_year)
        mean = float(a.mean())
        se = float(a.std(ddof=1) / np.sqrt(len(a)))
        t = mean / se if se > 0 else 0.0
        ev_mean = float(np.mean([np.asarray(p)[:, 0].mean() for p in buckets[k].values() if len(p) >= 30]))
        ct_mean = float(np.mean([np.asarray(p)[:, 1].mean() for p in buckets[k].values() if len(p) >= 30]))
        label = f"{k // 21}mo" if k < 252 else "12mo"
        print(
            f"{label:>8}{total:>10,}{ev_mean * 100:>11.2f}%{ct_mean * 100:>10.2f}%"
            f"{mean * 100:>10.2f}%{t:>8.2f}{len(a):>7}"
        )
        if abs(t) < 2.0:
            failures += 1

    print("\n⚠ Read this as an UPPER BOUND on what is achievable:")
    print("   - keyed on TRANSACTION date, not the FILING date we could actually see")
    print("   - no routine/opportunistic split, so the informative subset is diluted")
    print("   - NO COSTS. A ~50 bps round trip must come off every excess figure above")
    print("   - survivorship: the corpus is survivor-heavy, which flatters any long signal")
    print(f"\n{len(HORIZONS) - failures} of {len(HORIZONS)} horizons reach |t| >= 2 on year-clustered inference.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
