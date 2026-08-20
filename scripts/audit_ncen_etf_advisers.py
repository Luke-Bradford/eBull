"""Operator audit: can `institutional_filers.filer_type = 'ETF'` be populated from N-CEN? (#2214)

#2214 reads as a seeding ticket — the curated `etf_filer_cik_seeds` list holds
one row, `ncen_filer_classifications` holds none, so `classify_filer_type`
falls through to `INV` for every filer and the `etfs` ownership wedge is empty.
This script measures whether SEC's own data can fill that gap, and reports the
four numbers that answer it. Every figure quoted in
`.claude/skills/data-sources/sec-edgar.md` §2.2.1 is produced here — none is
written by hand.

The source rule it exercises:

  * Form N-CEN Item C.3.a — `FUND_REPORTED_INFO.IS_ETF`, the structured
    per-series ETF flag. Registrant-level `INVESTMENT_COMPANY_TYPE` cannot
    substitute: every ETF is `N-1A`, the same code as an ordinary open-end
    mutual fund.
  * Form N-CEN Item C.7 — `ADVISER.ADVISER_NAME` / `FILE_NUM` / `CRD_NUM` /
    `ADVISER_LEI`. No adviser CIK exists anywhere in N-CEN, so the join to a
    13F manager can only be by name.
  * Form 13F special instructions — a manager whose holdings are reported by
    another manager files a **13F-NT** notice and reports no holdings.

Sections, in the order they matter:

  1. NAMESPACE — do 13F-manager CIKs and RIC-trust CIKs overlap at all?
     (They must, for `ncen_classifier`'s seed walk to ever return a filing.)
  2. MATCH — how many 13F filers canonical-name-match a primary adviser of an
     `IS_ETF = Y` series?
  3. CONSOLIDATION — of those matches, how many file 13F-NT (holdings
     consolidated into a parent) versus carrying holdings of their own?
  4. IMPACT — what share of the observations layer would move into the `etfs`
     wedge, pooled and on the golden panel, and how much of that is a single
     multi-mandate filer.

Run from repo root:

    uv run python -m scripts.audit_ncen_etf_advisers
    uv run python -m scripts.audit_ncen_etf_advisers --quarters 2025q1,2025q2,2025q3,2025q4,2026q1
    uv run python -m scripts.audit_ncen_etf_advisers --cache-dir /tmp/ncen

N-CEN is an ANNUAL filing, so five consecutive quarters is the smallest window
that covers every registrant's cycle with one quarter of overlap. Fewer
quarters undercounts advisers and the script says so rather than pretending.

Read-only. Writes nothing to the database.
"""

from __future__ import annotations

import argparse
import collections
import csv
import io
import os
import re
import sys
import urllib.request
import zipfile
from collections.abc import Iterator
from pathlib import Path

import psycopg

from app.config import settings

_DERA_URL = "https://www.sec.gov/files/dera/data/form-n-cen-data-sets/{quarter}_ncen.zip"

DEFAULT_QUARTERS = ("2025q1", "2025q2", "2025q3", "2025q4", "2026q1")

GOLDEN_PANEL = ("AAPL", "GME", "HD", "JPM", "MSFT")

# The largest single 13F filer in the observations layer, reported separately
# because it is the whole argument: one multi-mandate manager dominating the
# proposed slice is what makes a filer-level split dishonest rather than coarse.
_VANGUARD_GROUP_CIK = "0000102909"

# Legal-form spellings that differ only in punctuation are the SAME form and
# are folded together. Forms are NOT stripped: "Brown Advisory LLC" and
# "Brown Advisory Ltd" are different registered entities and a normaliser that
# drops the suffix silently merges them.
_LEGAL_FORM_ALIASES = {
    "INCORPORATED": "INC",
    "LIMITED": "LTD",
}

# Legal forms that punctuation-stripping SPLITS into several tokens: `L.L.C.`
# becomes `L L C`, which no per-word alias can ever reach. These must be folded
# against the joined string, after tokenisation. Codex caught the earlier form,
# where both sat in the per-word table and were therefore dead entries —
# `L.L.C.` and `LLC` canonicalised differently and never matched.
_SPACED_LEGAL_FORMS = {
    "L L C": "LLC",
    "L P": "LP",
}


def _canonical_name(raw: str) -> str:
    """Case/punctuation-insensitive entity name, legal form PRESERVED.

    ⚠ The spaced-form fold is whole-token, so a name whose trailing initials
    happen to read `… L P` folds too. Preferred over leaving `L.P.` unmatchable:
    an initials collision needs two entities identical up to those initials,
    where the miss it replaces was systematic across every punctuated filer.
    """
    upper = raw.upper().replace("&", " AND ")
    words = re.sub(r"[^A-Z0-9]+", " ", upper).split()
    joined = " ".join(_LEGAL_FORM_ALIASES.get(w, w) for w in words)
    for spaced, folded in _SPACED_LEGAL_FORMS.items():
        joined = re.sub(rf"(?:^|(?<= )){re.escape(spaced)}(?=$| )", folded, joined)
    return joined


def _download(quarter: str, cache_dir: Path, user_agent: str) -> Path:
    """Fetch one DERA quarter into the cache, atomically.

    The cache-hit test is `st_size > 0`, which a truncated file passes — so an
    interrupted download must never be visible at `dest`. Write to a sibling
    `.part` and `os.replace` it, which is atomic within one filesystem. The
    `finally` clears the `.part` on an exception; a hard kill can still leave one
    behind, but nothing ever reads it, so the next run re-downloads either way.
    """
    dest = cache_dir / f"{quarter}_ncen.zip"
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    cache_dir.mkdir(parents=True, exist_ok=True)
    url = _DERA_URL.format(quarter=quarter)
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})  # noqa: S310 — fixed https SEC host.
    partial = dest.with_suffix(".zip.part")
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:  # noqa: S310
            partial.write_bytes(resp.read())
        os.replace(partial, dest)
    finally:
        partial.unlink(missing_ok=True)
    return dest


def _read_tsv(archive: Path, member: str) -> Iterator[dict[str, str]]:
    with zipfile.ZipFile(archive) as zf, zf.open(member) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
        yield from csv.DictReader(text, delimiter="\t")


def _etf_adviser_names(archives: list[Path]) -> dict[str, set[str]]:
    """Canonical adviser name -> the raw N-CEN spellings behind it.

    Only `ADVISER_TYPE = 'Advisor'` counts. A sub-adviser runs a sleeve of
    somebody else's fund and does not file that fund's 13F; a terminated
    adviser no longer does either.
    """
    by_canonical: dict[str, set[str]] = collections.defaultdict(set)
    for archive in archives:
        etf_fund_ids = {
            row["FUND_ID"]
            for row in _read_tsv(archive, "FUND_REPORTED_INFO.tsv")
            if (row.get("IS_ETF") or "").strip().upper() == "Y"
        }
        for row in _read_tsv(archive, "ADVISER.tsv"):
            if (row.get("ADVISER_TYPE") or "").strip().lower() != "advisor":
                continue
            if row["FUND_ID"] not in etf_fund_ids:
                continue
            name = (row.get("ADVISER_NAME") or "").strip()
            if name:
                by_canonical[_canonical_name(name)].add(name)
    return by_canonical


def _one(cur: psycopg.Cursor[tuple]) -> tuple:
    """First row of an aggregate that must produce exactly one.

    A `SELECT` with aggregates and no `GROUP BY` always returns a row, so
    `None` here is an unreachable invariant violation rather than an empty
    result — raise instead of ignoring the type, so a future edit that adds a
    `GROUP BY` fails loudly.
    """
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("aggregate produced no row")
    return row


def _section_namespace(conn: psycopg.Connection[tuple]) -> None:
    filers, trusts, overlap = _one(
        conn.execute(
            """
        SELECT (SELECT count(*) FROM institutional_filers),
               (SELECT count(*) FROM sec_nport_filer_directory),
               (SELECT count(*) FROM institutional_filers f
                  JOIN sec_nport_filer_directory n
                    ON lpad(f.cik, 10, '0') = lpad(n.cik, 10, '0'))
        """
        )
    )
    print("1. NAMESPACE — 13F managers vs RIC trusts")
    print(f"   institutional_filers            {filers:,}")
    print(f"   sec_nport_filer_directory       {trusts:,}")
    print(f"   CIKs in BOTH                    {overlap:,}")
    print("   (ncen_classifier walks the 13F-manager list for N-CEN filings;")
    print("    the overlap is its entire reachable universe.)")
    print()


def _section_match(
    conn: psycopg.Connection[tuple],
    adviser_names: dict[str, set[str]],
) -> dict[str, str]:
    filers = conn.execute("SELECT cik, name FROM institutional_filers").fetchall()
    matched: dict[str, str] = {}
    for cik, name in filers:
        canonical = _canonical_name(str(name))
        if canonical and canonical in adviser_names:
            matched[str(cik).zfill(10)] = str(name)
    print("2. MATCH — 13F filers that are a primary adviser of an IS_ETF=Y series")
    print(f"   distinct ETF-adviser names in N-CEN   {len(adviser_names):,}")
    print(f"   13F filers                            {len(filers):,}")
    print(f"   canonical-name matches                {len(matched):,}")
    print()
    return matched


def _section_consolidation(conn: psycopg.Connection[tuple], matched: dict[str, str]) -> None:
    ciks = list(matched)
    with_notice = _one(
        conn.execute(
            """
        SELECT count(DISTINCT lpad(filer_cik, 10, '0'))
        FROM institutional_filer_13f_notices
        WHERE lpad(filer_cik, 10, '0') = ANY(%s)
        """,
            (ciks,),
        )
    )
    with_holdings = _one(
        conn.execute(
            """
        SELECT count(DISTINCT lpad(filer_cik, 10, '0'))
        FROM ownership_institutions_current
        WHERE lpad(filer_cik, 10, '0') = ANY(%s)
        """,
            (ciks,),
        )
    )
    print("3. CONSOLIDATION — do the matched advisers report holdings of their own?")
    print(f"   matched advisers with a 13F-NT notice   {with_notice[0]:,}")
    print(f"   matched advisers with observation rows  {with_holdings[0]:,}")
    print("   Per-entity detail (the precisely-typable ETF advisers):")
    for cik in sorted(ciks):
        notices, holdings = _one(
            conn.execute(
                """
            SELECT (SELECT count(*) FROM institutional_filer_13f_notices
                      WHERE lpad(filer_cik, 10, '0') = %(c)s),
                   (SELECT count(*) FROM ownership_institutions_current
                      WHERE lpad(filer_cik, 10, '0') = %(c)s)
            """,
                {"c": cik},
            )
        )
        if notices and not holdings:
            print(f"     {cik}  {matched[cik][:44]:44s}  NT={notices:>2}  holdings=0")
    print()


def _section_impact(conn: psycopg.Connection[tuple], matched: dict[str, str]) -> None:
    ciks = list(matched)
    row = conn.execute(
        """
        SELECT count(DISTINCT instrument_id),
               count(DISTINCT instrument_id) FILTER (
                   WHERE lpad(filer_cik, 10, '0') = ANY(%(c)s)),
               round(100.0 * sum(shares) FILTER (
                   WHERE lpad(filer_cik, 10, '0') = ANY(%(c)s))
                   / nullif(sum(shares), 0), 2)
        FROM ownership_institutions_current
        WHERE exposure_kind IS NULL OR exposure_kind = 'EQUITY'
        """,
        {"c": ciks},
    )
    instruments, with_slice, pooled = _one(row)
    print("4. IMPACT — what would move into the `etfs` wedge")
    print(f"   instruments in the observations layer   {instruments:,}")
    print(f"   instruments gaining a non-zero slice    {with_slice:,}")
    print(f"   pooled share of institutional total     {pooled}%")
    print()
    print("   Golden panel (pct of that instrument's institutional shares):")
    print(f"   {'symbol':8s} {'all matched':>12s} {'Vanguard Group alone':>22s}")
    panel = conn.execute(
        """
        SELECT i.symbol,
               round(100.0 * sum(o.shares) FILTER (
                   WHERE lpad(o.filer_cik, 10, '0') = ANY(%(c)s))
                   / nullif(sum(o.shares), 0), 2),
               round(100.0 * sum(o.shares) FILTER (
                   WHERE lpad(o.filer_cik, 10, '0') = %(v)s)
                   / nullif(sum(o.shares), 0), 2)
        FROM ownership_institutions_current o
        JOIN instruments i USING (instrument_id)
        WHERE i.symbol = ANY(%(p)s)
          AND (o.exposure_kind IS NULL OR o.exposure_kind = 'EQUITY')
        GROUP BY 1 ORDER BY 1
        """,
        {"c": ciks, "v": _VANGUARD_GROUP_CIK, "p": list(GOLDEN_PANEL)},
    ).fetchall()
    for symbol, all_pct, vanguard_pct in panel:
        vg = "—" if vanguard_pct is None else f"{vanguard_pct}%"
        print(f"   {symbol:8s} {str(all_pct) + '%':>12s} {vg:>22s}")
    print()
    if panel:
        shares = [p[2] for p in panel if p[2] is not None]
        if shares:
            lo, hi = min(shares), max(shares)
            print(
                f"   One multi-mandate filer (VANGUARD GROUP INC) supplies "
                f"{lo}%–{hi}% of the panel's institutional shares on its own."
            )
    print()


def _section_vanguard_series(archives: list[Path]) -> None:
    """Vanguard-advised series by IS_ETF — a COUNT of series-filings, not AUM.

    The point is not the ratio's precision; it is that the ratio is nowhere
    near 1. A filer-level `ETF` type assigns 100% of this manager's 13F book
    to the ETF wedge.
    """
    counts: collections.Counter[str] = collections.Counter()
    for archive in archives:
        is_etf = {
            row["FUND_ID"]: (row.get("IS_ETF") or "").strip().upper()
            for row in _read_tsv(archive, "FUND_REPORTED_INFO.tsv")
        }
        for row in _read_tsv(archive, "ADVISER.tsv"):
            if (row.get("ADVISER_TYPE") or "").strip().lower() != "advisor":
                continue
            if "VANGUARD" not in (row.get("ADVISER_NAME") or "").upper():
                continue
            counts[is_etf.get(row["FUND_ID"], "?") or "(blank)"] += 1
    print("5. OVER-ATTRIBUTION — Vanguard-advised series-filings by IS_ETF")
    print("   (COUNT of series-filings across the window, NOT assets)")
    for flag, n in sorted(counts.items()):
        print(f"     IS_ETF={flag:8s} {n:,}")
    print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--quarters",
        default=",".join(DEFAULT_QUARTERS),
        help="comma-separated DERA N-CEN quarters, e.g. 2025q1,2025q2",
    )
    parser.add_argument("--cache-dir", default="/tmp/ncen", help="where the DERA ZIPs are cached")
    args = parser.parse_args(argv)

    quarters = [q.strip() for q in args.quarters.split(",") if q.strip()]
    if len(quarters) < 5:
        print(
            f"WARNING: {len(quarters)} quarter(s) requested. N-CEN is ANNUAL — "
            "fewer than 5 consecutive quarters undercounts advisers.",
            file=sys.stderr,
        )

    archives = [_download(q, Path(args.cache_dir), settings.sec_user_agent) for q in quarters]
    adviser_names = _etf_adviser_names(archives)

    print(f"N-CEN quarters: {', '.join(quarters)}")
    print()
    with psycopg.connect(settings.database_url) as conn:
        _section_namespace(conn)
        matched = _section_match(conn, adviser_names)
        _section_consolidation(conn, matched)
        _section_impact(conn, matched)
    _section_vanguard_series(archives)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
