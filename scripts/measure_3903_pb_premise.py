"""#3903 legacy P/B premise: the legacy arm's per-share ``pb_ratio`` (``metric_price / book_value``) against a
count-based form on the #3850 market cap.

Read-only; every database read shares one REPEATABLE READ snapshot. "Legacy" is the arm sql/241 names: an instrument
with no complete TTM window, valued from its latest ``fundamentals_snapshot`` row, whose ``book_value`` is that
quarter's ``shareholders_equity / shares_outstanding`` (``app/services/fundamentals/__init__.py::
_SNAPSHOT_WRITE_THROUGH_SQL``).

"Proposed" (spec "Construction") is ``market_cap_live / E`` with E the latest normalized quarter's
``shareholders_equity`` (``financial_periods_ttm``, the column the complete arm divides by), NULL when the cap is
refused, E is non-finite or <= 0, the quarter's end is past JKP's ``max_data_lag`` (the session falls after the
month-end 18 months after the quarter end; ``GlobalFactors/accounting_chars.sas:1075``, ``main.sas:92-93``), or the
instrument's trading currency is not USD (NULL included).

Prints:

1. both arms: rows, ``pb_ratio`` published today, and published under a refused cap;
2. legacy rows by today's and the proposed publication, with each NULL's first failing condition; every lost and
   every gained row listed with its inputs;
3. the equity source: snapshot ``book_value * shares_outstanding`` against the latest quarter's
   ``shareholders_equity``, the two columns' declared types, and whether the snapshot's ``as_of_date`` is that
   quarter's end;
4. rows publishing both ways: today ÷ proposed = price factor × count factor × equity factor, asserted per row; each
   row outside [0.95, 1.05] traced: the stored quarter-end count against the stored share-count instants at that
   quarter end (concept, accession, filed date) and the PIT step's concept, accession and context date;
5. the 18-month bound on both arms: rows it alone refuses, the maximum quarter-end age of a proposed publication, and
   proposed publications whose equity quarter was filed after the session;
6. currency: the trading currency of every view row against the cap's publication, and the units of every stored
   fact on the two equity concepts;
7. a partial SEC proxy of JKP ``be_x`` on every proposed publication, both arms: SEQ + TXDITC − PSTK at the quarter
   end from the local SEC ``companyfacts.zip`` (the concept lists ``app/services/factor_panel.py`` uses, first concept
   with a USD instant, latest-filed), without JKP's SEQ fallbacks or its ``sum(txdb, itcb)`` branch; which concept
   the stored E equals; and the ``financial_facts_raw`` row count of the TXDITC and PSTK concepts;
8. rows with a published cap whose latest normalized period with equity (any period type, FY included) ends after
   the latest quarter ``financial_periods_ttm`` reads, with both equities;
9. ties in the latest-quarter selection: normalized Q rows sharing instrument, period end and filed date, with how
   many disagree on equity, and how many sit at the winning filed-date rank (``filed_date DESC NULLS LAST``) of an
   instrument's latest quarter.

    PYTHONPATH=. uv run python scripts/measure_3903_pb_premise.py
"""

from __future__ import annotations

import json
import subprocess
import zipfile
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.security.master_key import resolve_data_dir
from app.services.factor_panel import PSTK, SEQ, TXDITC

SEQ_NCI = "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"

_ROWS_SQL = """
SELECT v.instrument_id, i.symbol, (v.is_complete_ttm IS NULL) AS legacy, v.pb_ratio, v.market_cap_reason,
       v.market_cap_live, v.valuation_price, v.current_price, v.session,
       fs.as_of_date, fs.book_value, fs.shares_outstanding AS snapshot_shares,
       fs.book_value * fs.shares_outstanding AS snapshot_equity,
       ttm.shareholders_equity AS equity, ttm.shares_outstanding AS ttm_shares, ttm.ttm_end, ttm.reported_currency,
       (date_trunc('month', ttm.ttm_end + interval '18 months') + interval '1 month - 1 day')::date AS valid_through,
       m.market_cap AS pit_cap, m.close AS pit_close, m.shares_concept, m.split_factor, m.context_date,
       m.shares_reported, m.accession AS pit_accession, lpad(ei.identifier_value, 10, '0') AS cik10,
       i.currency, lq.filed_date AS equity_filed
FROM instrument_valuation v
JOIN instruments i USING (instrument_id)
LEFT JOIN LATERAL (
    SELECT * FROM fundamentals_snapshot f WHERE f.instrument_id = v.instrument_id ORDER BY f.as_of_date DESC LIMIT 1
) fs ON TRUE
LEFT JOIN financial_periods_ttm ttm ON ttm.instrument_id = v.instrument_id
-- The latest quarter's filing date, under financial_periods_ttm's own dedup (latest filed wins per period end).
LEFT JOIN LATERAL (
    SELECT fp.filed_date FROM financial_periods fp
    WHERE fp.instrument_id = v.instrument_id AND fp.period_end_date = ttm.ttm_end
      AND fp.period_type IN ('Q1','Q2','Q3','Q4') AND fp.superseded_at IS NULL
      AND fp.normalization_status = 'normalized'
    ORDER BY fp.filed_date DESC NULLS LAST LIMIT 1
) lq ON TRUE
LEFT JOIN instrument_market_cap_pit m ON m.instrument_id = v.instrument_id AND m.price_date = v.session
LEFT JOIN external_identifiers ei ON ei.instrument_id = v.instrument_id AND ei.provider = 'sec'
     AND ei.identifier_type = 'cik' AND ei.is_primary
"""

_RATIO_SQL = "SELECT instrument_id, ratio FROM ads_ratio"

_TYPES_SQL = """
SELECT table_name, column_name, data_type, numeric_precision, numeric_scale FROM information_schema.columns
WHERE (table_name, column_name) IN (('fundamentals_snapshot', 'book_value'),
                                    ('financial_periods', 'shareholders_equity'))
ORDER BY 1
"""

_COUNT_FACTS_SQL = """
SELECT taxonomy, concept, period_end, val, accession_number, filed_date FROM financial_facts_raw
WHERE instrument_id = %(iid)s AND period_end = %(end)s AND period_start IS NULL
  AND concept IN ('EntityCommonStockSharesOutstanding', 'CommonStockSharesOutstanding')
ORDER BY concept, filed_date
"""

_STORED_COUNTS_SQL = """
SELECT period_end_date, period_type, shares_outstanding, filed_date FROM financial_periods
WHERE instrument_id = %(iid)s AND superseded_at IS NULL AND normalization_status = 'normalized'
ORDER BY period_end_date DESC LIMIT 4
"""

_LATEST_PERIOD_SQL = """
SELECT DISTINCT ON (instrument_id) instrument_id, period_end_date, period_type, shareholders_equity
FROM financial_periods
WHERE superseded_at IS NULL AND normalization_status = 'normalized' AND shareholders_equity IS NOT NULL
ORDER BY instrument_id, period_end_date DESC, filed_date DESC NULLS LAST
"""

_TIES_SQL = """
WITH q AS (
    SELECT instrument_id, period_end_date, filed_date, shareholders_equity,
           dense_rank() OVER (PARTITION BY instrument_id, period_end_date
                              ORDER BY filed_date DESC NULLS LAST) AS filed_rank
    FROM financial_periods
    WHERE period_type IN ('Q1','Q2','Q3','Q4') AND superseded_at IS NULL AND normalization_status = 'normalized'
)
SELECT q.instrument_id, q.period_end_date, count(*) AS n, count(DISTINCT q.shareholders_equity) AS n_eq,
       count(q.shareholders_equity) AS n_nonnull,
       bool_or(t.ttm_end = q.period_end_date AND q.filed_rank = 1) AS at_latest
FROM q
LEFT JOIN financial_periods_ttm t ON t.instrument_id = q.instrument_id
GROUP BY q.instrument_id, q.period_end_date, q.filed_date
HAVING count(*) > 1
"""

_EQUITY_UNITS_SQL = """
SELECT concept, unit, count(*) AS n FROM financial_facts_raw
WHERE concept IN ('StockholdersEquity', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest')
GROUP BY 1, 2 ORDER BY 1, 2
"""

_UNTRACKED_SQL = "SELECT count(*) AS n FROM financial_facts_raw WHERE concept = ANY(%(concepts)s)"

_BAND = (Decimal("0.95"), Decimal("1.05"))


def proposed(r: dict[str, Any]) -> tuple[Decimal | None, str]:
    """The proposed P/B and, when NULL, its first failing condition (construction order)."""
    if r["market_cap_reason"] is not None:
        return None, f"cap refused: {r['market_cap_reason']}"
    if r["equity"] is None:
        return None, "no stored equity"
    if not r["equity"].is_finite():
        return None, "non-finite equity"
    if r["equity"] <= 0:
        return None, "equity <= 0"
    if r["session"] > r["valid_through"]:
        return None, "past the 18-month bound"
    if r["currency"] != "USD":
        return None, "trading currency not USD"
    return r["market_cap_live"] / r["equity"], ""


def outside(x: Decimal) -> bool:
    return not _BAND[0] <= x <= _BAND[1]


def first(gaap: dict[str, Any], concepts: tuple[str, ...] | list[str], end: str) -> tuple[Decimal | None, str]:
    """The first concept with a USD instant at ``end``, its latest-filed value."""
    for c in concepts:
        facts = [f for f in gaap.get(c, {}).get("units", {}).get("USD", []) if f["end"] == end and "start" not in f]
        if facts:
            val = Decimal(str(max(facts, key=lambda f: (f["filed"], f["accn"]))["val"]))
            return (val, c) if val.is_finite() else (None, f"{c} non-finite")
    return None, ""


def main() -> None:
    head = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
    print(f"#3903 legacy P/B premise; {datetime.now(UTC):%Y-%m-%d %H:%MZ}; code {head}")
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SET LOCAL statement_timeout = '900s'")
            rows = cur.execute(_ROWS_SQL).fetchall()
            ratios = {r["instrument_id"]: r["ratio"] for r in cur.execute(_RATIO_SQL).fetchall()}
            types = cur.execute(_TYPES_SQL).fetchall()
            latest_period = {r["instrument_id"]: r for r in cur.execute(_LATEST_PERIOD_SQL).fetchall()}
            ties = cur.execute(_TIES_SQL).fetchall()
            equity_units = cur.execute(_EQUITY_UNITS_SQL).fetchall()
            untracked = cur.execute(_UNTRACKED_SQL, {"concepts": [*TXDITC, *PSTK]}).fetchone()
            assert untracked is not None
            traces: dict[int, tuple[list[dict[str, Any]], list[dict[str, Any]]]] = {}
            for r in rows:
                if r["legacy"] and r["pb_ratio"] is not None and r["snapshot_shares"] and r["pit_cap"]:
                    facts = cur.execute(_COUNT_FACTS_SQL, {"iid": r["instrument_id"], "end": r["ttm_end"]}).fetchall()
                    stored = cur.execute(_STORED_COUNTS_SQL, {"iid": r["instrument_id"]}).fetchall()
                    traces[r["instrument_id"]] = (facts, stored)
    pub_rows = [r for r in rows if proposed(r)[0] is not None]

    print("\n1. pb_ratio by arm:")
    for arm in (False, True):
        sub = [r for r in rows if r["legacy"] is arm]
        pub = [r for r in sub if r["pb_ratio"] is not None]
        refused = [r for r in pub if r["market_cap_reason"] is not None]
        print(
            f"   {'legacy' if arm else 'complete'}: rows {len(sub)}, published {len(pub)},"
            f" under a refused cap {len(refused)}"
        )

    legacy = [r for r in rows if r["legacy"]]
    print(f"\n2. legacy rows {len(legacy)}:")
    cells: Counter[tuple[str, str]] = Counter()
    for r in legacy:
        p, cause = proposed(r)
        today = "today pub" if r["pb_ratio"] is not None else "today NULL"
        cells[(today, "proposed pub" if p is not None else f"proposed NULL ({cause})")] += 1
    for k, n in sorted(cells.items()):
        print(f"   {k[0]:10} | {k[1]:45} {n}")
    lost = [r for r in legacy if r["pb_ratio"] is not None and proposed(r)[0] is None]
    print("   lost: symbol, today's pb_ratio, cause, snapshot as_of, book per share, snapshot shares")
    for r in sorted(lost, key=lambda r: (proposed(r)[1], r["symbol"])):
        print(
            f"     {r['symbol']:8} {r['pb_ratio']:.4f} {proposed(r)[1]:30} {r['as_of_date']} {r['book_value']}"
            f" {r['snapshot_shares']}"
        )
    gained = [r for r in legacy if r["pb_ratio"] is None and proposed(r)[0] is not None]
    print("   gained: symbol, proposed, book_value, snapshot shares, stored equity, quarter end")
    for r in gained:
        print(
            f"     {r['symbol']:8} {proposed(r)[0]:.4f} {r['book_value']} {r['snapshot_shares']} {r['equity']}"
            f" {r['ttm_end']}"
        )

    print("\n3. equity source over legacy rows:")
    for t in types:
        print(
            f"   {t['table_name']}.{t['column_name']}: {t['data_type']}({t['numeric_precision']},{t['numeric_scale']})"
        )
    nonfinite = [
        r["symbol"]
        for r in rows
        if any(r[k] is not None and not r[k].is_finite() for k in ("equity", "snapshot_equity"))
    ]
    print(f"   non-finite stored or snapshot equity, every view row (excluded below): {nonfinite}")
    both = [
        r
        for r in legacy
        if r["snapshot_equity"] is not None
        and r["snapshot_equity"].is_finite()
        and r["equity"] is not None
        and r["equity"].is_finite()
    ]
    rel: Counter[str] = Counter()
    for r in both:
        if r["equity"] == 0:
            rel["stored equity 0"] += 1
            continue
        d = abs(r["snapshot_equity"] / r["equity"] - 1)
        rel["<=0.1%" if d <= Decimal("0.001") else "<=1%" if d <= Decimal("0.01") else ">1%"] += 1
    print(f"   both figures {len(both)}: {dict(rel)}")
    for r in both:
        if r["equity"] != 0 and abs(r["snapshot_equity"] / r["equity"] - 1) > Decimal("0.01"):
            print(f"   >1%: {r['symbol']} snapshot {r['snapshot_equity']} stored {r['equity']} book {r['book_value']}")
    only_snap = [r["symbol"] for r in legacy if r["snapshot_equity"] is not None and r["equity"] is None]
    only_stored = [r for r in legacy if r["snapshot_equity"] is None and r["equity"] is not None]
    print(f"   snapshot only: {only_snap}")
    print(
        f"   stored only: {len(only_stored)}; snapshot shares NULL on"
        f" {sum(1 for r in only_stored if r['snapshot_shares'] is None)}"
    )
    same_end = Counter(r["as_of_date"] == r["ttm_end"] for r in both)
    print(f"   snapshot as_of_date == latest quarter end: {dict(same_end)}")
    stored_ids = {r["instrument_id"] for r in only_stored}
    print(
        f"   every gained row is stored-only with NULL snapshot shares: {
            all(r['instrument_id'] in stored_ids and r['snapshot_shares'] is None for r in gained)
        }"
    )

    print("\n4. legacy rows publishing both ways, today ÷ proposed = price × count × equity factor:")
    pairs = []
    for r in legacy:
        p, _ = proposed(r)
        if r["pb_ratio"] is None or p is None or not r["snapshot_equity"].is_finite():
            continue
        metric_price = r["current_price"] / ratios.get(r["instrument_id"], 1)
        pit_shares = Decimal(str(r["pit_cap"])) / r["pit_close"]
        price_f = metric_price / r["pit_close"]
        count_f = r["snapshot_shares"] / pit_shares
        equity_f = r["equity"] / r["snapshot_equity"]
        ratio = r["pb_ratio"] / p
        # book_value's 4 dp rounding enters through equity_f, so the identity is exact up to Decimal precision.
        assert abs(ratio / (price_f * count_f * equity_f) - 1) < Decimal("1e-9"), r["symbol"]
        pairs.append((r, ratio, price_f, count_f, equity_f, pit_shares))
    band: Counter[str] = Counter()
    for _, ratio, *_ in pairs:
        d = abs(ratio - 1)
        band["<=1%" if d <= Decimal("0.01") else "<=5%" if d <= Decimal("0.05") else ">5%"] += 1
    print(f"   rows {len(pairs)}: {dict(band)}")
    if pairs:
        pf = sorted(t[2] for t in pairs)
        print(f"   price factor min {pf[0]:.4f} max {pf[-1]:.4f}")
    print(
        "   outside [0.95, 1.05]: symbol, today, proposed, ratio, price f, count f, equity f;"
        " quarter end, snapshot shares; PIT shares, concept, context date, split factor"
    )
    for r, ratio, price_f, count_f, equity_f, pit_shares in sorted(pairs, key=lambda t: t[1]):
        if outside(ratio):
            print(
                f"   {r['symbol']:8} {r['pb_ratio']:.4f} {r['pb_ratio'] / ratio:.4f} {ratio:.4f}"
                f" {price_f:.4f} {count_f:.4f} {equity_f:.4f}; {r['as_of_date']} {r['snapshot_shares']:.0f};"
                f" {pit_shares:.0f} {r['shares_concept']} {r['context_date']} {r['split_factor']} {r['pit_accession']}"
            )
            facts, stored = traces[r["instrument_id"]]
            for f in facts:
                print(f"       instant {f['concept']} {f['val']:.0f} {f['accession_number']} filed {f['filed_date']}")
            counts = [(str(q["period_end_date"]), q["period_type"], q["shares_outstanding"]) for q in stored]
            print(f"       stored counts: {counts}")

    print("\n5. the 18-month bound alone refuses (cap published, equity > 0):")
    for arm in (False, True):
        hit = [
            r
            for r in rows
            if r["legacy"] is arm
            and r["market_cap_reason"] is None
            and r["equity"] is not None
            and r["equity"].is_finite()
            and r["equity"] > 0
            and r["session"] > r["valid_through"]
        ]
        print(
            f"   {'legacy' if arm else 'complete'}: {len(hit)}"
            f" {[(r['symbol'], str(r['ttm_end']), r['pb_ratio'] is not None) for r in hit]}"
        )
    oldest = max(pub_rows, key=lambda r: (r["session"] - r["ttm_end"]).days)
    print(
        f"   maximum quarter-end age of a proposed publication: {(oldest['session'] - oldest['ttm_end']).days} days"
        f" ({oldest['symbol']}, quarter end {oldest['ttm_end']}, session {oldest['session']})"
    )
    for arm in (False, True):
        late = [r for r in pub_rows if r["legacy"] is arm and r["equity_filed"] and r["equity_filed"] > r["session"]]
        unknown = sum(1 for r in pub_rows if r["legacy"] is arm and r["equity_filed"] is None)
        print(
            f"   {'legacy' if arm else 'complete'}: equity quarter filed after the session {len(late)}"
            f" {[(r['symbol'], str(r['equity_filed']), str(r['session'])) for r in late]}; filed date NULL {unknown}"
        )

    print("\n6. trading currency of every view row by cap publication:")
    cur = Counter((r["currency"], r["market_cap_reason"] is None) for r in rows)
    print(f"   (instruments.currency, cap published): {dict(cur)}")
    print(f"   equity-concept facts by unit: {[(u['concept'], u['unit'], u['n']) for u in equity_units]}")

    zpath = resolve_data_dir() / "sec" / "bulk" / "companyfacts.zip"
    shafile = zpath.with_name(zpath.name + ".sha256")
    sha = shafile.read_text().strip()[:12] if shafile.exists() else None
    print(
        f"\n7. partial be_x proxy: stored E ÷ (SEQ + TXDITC − PSTK); {zpath.name} {zpath.stat().st_size} B, sha {sha}:"
    )
    print(f"   financial_facts_raw rows on the TXDITC and PSTK lists {[*TXDITC, *PSTK]}: {untracked['n']}")
    with zipfile.ZipFile(zpath) as z:
        names = set(z.namelist())
        for arm in (False, True):
            tally: Counter[str] = Counter()
            drivers: Counter[str] = Counter()
            quotients = []
            listed = []
            source: Counter[str] = Counter()
            for r in (x for x in pub_rows if x["legacy"] is arm):
                if r["cik10"] is None or f"CIK{r['cik10']}.json" not in names:
                    tally["no companyfacts"] += 1
                    continue
                gaap = json.loads(z.read(f"CIK{r['cik10']}.json")).get("facts", {}).get("us-gaap", {})
                end = r["ttm_end"].isoformat()
                seq, _ = first(gaap, SEQ, end)
                nci, _ = first(gaap, [SEQ_NCI], end)
                source[
                    "SEQ"
                    if seq is not None and seq == r["equity"]
                    else "NCI-inclusive"
                    if nci is not None and nci == r["equity"]
                    else "neither"
                ] += 1
                if seq is None:
                    tally["no SEQ instant"] += 1
                    continue
                txditc, tx_c = first(gaap, TXDITC, end)
                pstk, ps_c = first(gaap, PSTK, end)
                tally[f"txditc {'found' if txditc is not None else 'absent'}"] += 1
                tally[f"pstk {'found' if pstk else 'absent or 0'}"] += 1
                be = seq + (txditc or 0) - (pstk or 0)
                if be <= 0:
                    tally["be_x <= 0"] += 1
                    continue
                q = r["equity"] / be
                quotients.append(q)
                if not outside(q):
                    tally["in band"] += 1
                    continue
                tally["outside band"] += 1
                drivers[f"seq{'=' if seq == r['equity'] else '!='}E txditc={bool(txditc)} pstk={bool(pstk)}"] += 1
                listed.append((r["symbol"], f"{q:.4f}", tx_c, ps_c))
            quotients.sort()
            n = len(quotients)
            print(f"   {'legacy' if arm else 'complete'}: {dict(tally)}")
            print(f"   stored E equals the companyfacts instant of: {dict(source)}")
            if n:
                print(
                    f"   E/be_x quantiles p05 {quotients[n // 20]:.4f} p25 {quotients[n // 4]:.4f}"
                    f" p50 {quotients[n // 2]:.4f} p75 {quotients[3 * n // 4]:.4f} p95 {quotients[19 * n // 20]:.4f}"
                )
            print(f"   outside band by component: {dict(drivers)}")
            if arm:
                print(f"   legacy outside band: {listed}")

    print("\n8. cap published, a later period with equity than the latest quarter read (any period type):")
    for arm in (False, True):
        newer = [
            (r, lp)
            for r in rows
            if r["legacy"] is arm
            and r["market_cap_reason"] is None
            and (lp := latest_period.get(r["instrument_id"])) is not None
            and (r["ttm_end"] is None or lp["period_end_date"] > r["ttm_end"])
        ]
        print(f"   {'legacy' if arm else 'complete'}: {len(newer)}")
        for r, lp in newer:
            print(
                f"     {r['symbol']:8} quarter {r['ttm_end']} E {r['equity']}; {lp['period_type']}"
                f" {lp['period_end_date']} E {lp['shareholders_equity']}"
            )

    print("\n9. latest-quarter selection ties (same instrument, period end, filed date):")
    conflicting = [t for t in ties if t["n_eq"] > 1 or 0 < t["n_nonnull"] < t["n"]]
    at_latest = sum(1 for t in ties if t["at_latest"])
    print(
        f"   tied groups {len(ties)}; disagreeing on equity {len(conflicting)}; at a latest quarter's winning rank"
        f" {at_latest}, of them disagreeing {sum(1 for t in conflicting if t['at_latest'])}"
    )


if __name__ == "__main__":
    main()
