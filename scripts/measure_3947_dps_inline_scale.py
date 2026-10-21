"""#3947 premise measurements for the DPS inline-scale spec.

Read-only. Reproduces the figures in
``docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md``:

* ``[1]`` population: accessions holding a ``CommonStockDividendsPerShareDeclared``
  fact, by filing year and by the manifest's primary-document extension.
* ``[2]`` pilot: every ``.htm`` accession with a fact >= 10 plus a seeded random 150
  of the rest. Fetches each primary document once (not stored) and reads the
  ``scale`` attribute of every ``ix:nonFraction`` DPS occurrence (DQC.US.0179.10141).
  Counts are regex-detected occurrences in the primary document only: the regex reads
  the ``ix:``/``us-gaap:`` prefixes as spelled (single- or double-quoted attributes) and resolves
  no context, unit or value, so a miss means "no regex match", not "no occurrence".
  Requests are sequential and at least 0.15 s apart; the script does not share the jobs
  process's SEC limiter, so run it outside a heavy SEC ingest window.
* ``[3]`` the FY / 9-month-YTD ratio of the same issuer's DPS facts (the falsified
  cents-class candidate).

    PYTHONPATH=. uv run python -m scripts.measure_3947_dps_inline_scale [--skip-fetch]
"""

from __future__ import annotations

import argparse
import collections
import random
import re
import time

import httpx
import psycopg

from app.config import settings

_CONCEPT = "CommonStockDividendsPerShareDeclared"
_TAG = re.compile(
    r'<ix:nonFraction\b([^>]*\bname=["\']us-gaap:' + _CONCEPT + r'["\'][^>]*)>(.*?)</ix:nonFraction>', re.S | re.I
)
_SCALE_LEXICAL = re.compile(r"[-+]?[0-9]+")  # xs:integer lexical space
_ATTR = re.compile(r'([\w:]+)=(?:"([^"]*)"|\'([^\']*)\')')
# The one primary-document-extension expression both [1] and [2] use, so "an .htm accession" means one thing.
_EXTENSION_SQL = "substring(m.primary_document_url FROM '\\.([a-z]+)$')"


def _scale(raw: str | None) -> int | str | None:
    """The ``scale`` attribute as an integer; an unparseable value is returned as text and never counts as > 0."""
    if raw is None:
        return None
    collapsed = raw.strip()
    return int(collapsed) if _SCALE_LEXICAL.fullmatch(collapsed) else raw


def _population(conn: psycopg.Connection[tuple]) -> None:
    print("[1] accessions with a DPS fact, by filing year")
    for row in conn.execute(
        "SELECT extract(year FROM filed_date)::int, count(DISTINCT accession_number) FROM financial_facts_raw"
        " WHERE concept = %s GROUP BY 1 ORDER BY 1",
        (_CONCEPT,),
    ):
        print("   ", row)
    total = conn.execute(
        "SELECT count(DISTINCT accession_number) FROM financial_facts_raw"
        " WHERE concept = %s AND unit = 'USD/shares' AND filed_date >= '2019-06-15'",
        (_CONCEPT,),
    ).fetchone()
    print(
        f"[1] filed >= 2019-06-15: {total[0] if total else 0} accessions,"
        " by form and manifest primary-document extension"
    )
    for row in conn.execute(
        f"""
        WITH a AS (SELECT DISTINCT accession_number, form_type FROM financial_facts_raw
                   WHERE concept = %s AND unit = 'USD/shares' AND filed_date >= '2019-06-15')
        SELECT a.form_type, coalesce({_EXTENSION_SQL}, '<none>'), count(*)
        FROM a LEFT JOIN sec_filing_manifest m USING (accession_number) GROUP BY 1, 2 ORDER BY 3 DESC
        """,
        (_CONCEPT,),
    ):
        print("   ", row)


def _pilot(conn: psycopg.Connection[tuple]) -> None:
    rows = conn.execute(
        f"""
        WITH f AS (SELECT accession_number, bool_or(val >= 10) AS big FROM financial_facts_raw
                   WHERE concept = %s AND unit = 'USD/shares' AND filed_date >= '2019-06-15' GROUP BY 1)
        SELECT f.accession_number, f.big, m.primary_document_url
        FROM f JOIN sec_filing_manifest m USING (accession_number)
        WHERE {_EXTENSION_SQL} = 'htm' ORDER BY 1
        """,
        (_CONCEPT,),
    ).fetchall()
    big = [r for r in rows if r[1]]
    small = [r for r in rows if not r[1]]
    random.seed(3947)
    arms = {"val>=10 (all)": big, "val<10 (random 150)": random.sample(small, min(150, len(small)))}
    print(f"[2] .htm population {len(rows)}: {len(big)} with a fact >= 10, {len(small)} without")
    client = httpx.Client(headers={"User-Agent": settings.sec_user_agent}, timeout=60, follow_redirects=True)
    for label, sample in arms.items():
        if not sample:
            print(f"    {label}: empty")
            continue
        scales: collections.Counter[int | str | None] = collections.Counter()
        scaled_accessions: list[str] = []
        no_fact = 0
        fetch_failed: list[str] = []
        total_bytes = 0
        for accession, _big, url in sample:
            started = time.monotonic()
            try:
                response = client.get(url)
                response.raise_for_status()
            except httpx.HTTPError as exc:
                # Counted and listed, not fatal: one transient SEC error must not void the run.
                fetch_failed.append(f"{accession} {exc!r}")
                continue
            body = response.text
            total_bytes += len(body.encode())
            found = [{k: dq or sq for k, dq, sq in _ATTR.findall(m.group(1))} for m in _TAG.finditer(body)]
            no_fact += not found
            parsed = [_scale(a.get("scale")) for a in found]
            for scale in parsed:
                scales[scale] += 1
            if any(isinstance(scale, int) and scale > 0 for scale in parsed):
                scaled_accessions.append(accession)
            time.sleep(max(0.0, 0.15 - (time.monotonic() - started)))
        print(
            f"    {label}: accessions {len(sample)}, fetch failures {len(fetch_failed)},"
            f" no regex-detected occurrence in primary doc {no_fact},"
            f" occurrences by scale {dict(scales)}, accessions with scale > 0 {len(scaled_accessions)},"
            f" mean MB {total_bytes / max(1, len(sample) - len(fetch_failed)) / 1e6:.2f}"
        )
        for failure in fetch_failed:
            print("       fetch failed", failure)
        for accession in scaled_accessions:
            symbol = conn.execute(
                "SELECT i.symbol FROM financial_facts_raw f JOIN instruments i USING (instrument_id)"
                " WHERE f.accession_number = %s LIMIT 1",
                (accession,),
            ).fetchone()
            print("       scaled", accession, symbol[0] if symbol else None)


def _fy_over_ytd(conn: psycopg.Connection[tuple]) -> None:
    rows = conn.execute(
        """
        WITH d AS (SELECT instrument_id, accession_number, period_start s, period_end e, val, filed_date,
                          period_end - period_start AS days
                   FROM financial_facts_raw
                   WHERE concept = %s AND unit = 'USD/shares' AND period_start IS NOT NULL),
        -- 350-380 days is a fiscal year (52/53 weeks included); 260-285 days is nine months (39-40 weeks
        -- is 273-280 days). Neither window can hold a 6-month (~182 days) or 12-month (~365 days) duration,
        -- and ytd.s = fy.s pins the YTD to the same fiscal year's start.
        fy AS (SELECT DISTINCT ON (instrument_id, s, e) * FROM d WHERE days BETWEEN 350 AND 380
               ORDER BY instrument_id, s, e, filed_date DESC, accession_number DESC),
        ytd AS (SELECT DISTINCT ON (instrument_id, s, e) * FROM d WHERE days BETWEEN 260 AND 285
                ORDER BY instrument_id, s, e, filed_date DESC, accession_number DESC)
        SELECT i.symbol, fy.s, fy.e, fy.val, ytd.val, fy.val / ytd.val AS r
        FROM fy JOIN ytd ON ytd.instrument_id = fy.instrument_id AND ytd.s = fy.s AND ytd.e < fy.e
        JOIN instruments i ON i.instrument_id = fy.instrument_id
        WHERE ytd.val > 0 ORDER BY r DESC
        """,
        (_CONCEPT,),
    ).fetchall()
    ties = conn.execute(
        """
        SELECT count(*) FROM (
            SELECT 1 FROM financial_facts_raw
            WHERE concept = %s AND unit = 'USD/shares' AND period_start IS NOT NULL
            GROUP BY instrument_id, period_start, period_end, filed_date, accession_number
            HAVING count(DISTINCT val) > 1) t
        """,
        (_CONCEPT,),
    ).fetchone()
    print(f"[3] selection keys holding more than one value (an arbitrary pick): {ties[0] if ties else 0}")
    print(f"[3] FY/9M pairs {len(rows)}; ratio >= 10: {sum(r[5] >= 10 for r in rows)}")
    for row in rows:
        if row[5] >= 10:
            print("   ", row[0], row[1], row[2], row[3], row[4], round(row[5], 2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-fetch", action="store_true", help="skip the SEC fetch in [2]")
    args = parser.parse_args()
    with psycopg.connect(settings.database_url) as conn:
        _population(conn)
        if not args.skip_fetch:
            _pilot(conn)
        _fy_over_ytd(conn)


if __name__ == "__main__":
    main()
