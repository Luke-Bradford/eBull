"""#3227 — Form 4 Table I holding rows (``nonDerivativeHolding``) are never stored.

    PYTHONPATH=. uv run python -m scripts.census_3227_form4_holding_rows
    PYTHONPATH=. uv run python -m scripts.census_3227_form4_holding_rows --examples 5
    PYTHONPATH=. uv run python -m scripts.census_3227_form4_holding_rows --show 0000933430-25-000004

Read-only.  Nothing is written, nothing is refreshed.  Spec:
docs/specs/ownership/2026-10-18-3227-form4-holding-rows.md, whose rule this script applies so
every count the spec quotes comes from here.

## What is being measured

``parse_form_4_xml`` reads only ``nonDerivativeTransaction`` / ``derivativeTransaction``, so a
Form 4's ``nonDerivativeHolding`` lines (a Table I row with column 5 and no transaction) never
reach ``insider_transactions`` or ``ownership_insiders_observations``; a holdings-only Form 4
returns ``None`` and is tombstoned.  The DERA bulk path writes ``NONDERIV_HOLDING`` rows only for
accessions with no ``NONDERIV_TRANS`` row, so on a Form 4 that also reports a transaction neither
writer keeps the holding line.  SLSN ``0000933430-25-000004``: the ``direct`` key holds 25,467,020
from a 2024 Form 4, while this 2025 Form 4's ``D`` holding line reads 108,141.

## Cohort

Every stored ``form4_xml`` payload whose text contains ``erivativeHolding`` (either table), with
its ``insider_filings`` row.  Payloads with no filing row are counted, not parsed.  Everything is
read from the payload itself — holdings, transactions (for the holding-only test and the
equal-amount guard), dates, owners — with the production helpers; lines are attributed to
``filers[0]``, as every XML writer does.

## The rule applied (the spec's Change section)

A group is one filer's ``D`` (or ``I``) non-derivative holding lines in one Form 4/4A.  It is
*holding-only* when the payload has no non-derivative transaction line for that filer and D/I (a
transaction with no D/I counts as ``D``: the writers label it ``direct``).  A holding-only group
is written when it is one share-bearing line, or several that pass the line-sum conditions;
otherwise it is refused and writes nothing.  Its ``period_end`` is the signature date when
``period_of_report <= signature <= filed date``, else ``period_of_report`` when it is on or before
the filed date, else nothing.  It needs a manifest ``filed_at`` inside the Form 4 retention window.

## What it cannot establish

* That a summed group's lines are disjoint holdings: the conditions refuse equal amounts, not a
  block nested inside a larger unequal one (the same residual as the Form 3 line sum).
* The ``_current`` move exactly: the winner comparison below re-implements the order (source
  rank, ``period_end`` DESC, ``filed_at`` DESC) against the stored winner only, without the
  amendment filter or de-collision.  The build's A/B diffs the real refreshers.
"""

from __future__ import annotations

import argparse
import re
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Final, LiteralString

import psycopg

from app.config import settings
from app.services.insider_transactions import (
    _XMLNS_RE,
    _date,
    _extract_filers,
    _extract_holdings,
    _extract_transactions,
    _localname,
    _text,
    _unwrap_sgml_submission,
    form4_within_retention,
)

_PAYLOADS: Final[LiteralString] = """
SELECT r.accession_number, f.instrument_id, f.document_type, f.is_tombstone,
       m.filed_at, r.payload
  FROM filing_raw_documents r
  LEFT JOIN insider_filings f ON f.accession_number = r.accession_number
  LEFT JOIN sec_filing_manifest m ON m.accession_number = r.accession_number
 WHERE r.document_kind = 'form4_xml'
   AND r.payload LIKE '%%erivativeHolding%%'
"""

_FORM5_WITH_HOLDINGS: Final[LiteralString] = """
SELECT count(*) FROM filing_raw_documents
 WHERE document_kind = 'form5_xml' AND payload LIKE '%%nonDerivativeHolding%%'
"""

_CURRENT: Final[LiteralString] = """
SELECT instrument_id, holder_identity_key, ownership_nature, source, source_document_id,
       source_accession, period_end, filed_at, shares
  FROM ownership_insiders_current
"""

_SOURCE_RANK: Final[dict[str, int]] = {"form4": 1, "form3": 2, "13d": 3, "13g": 3, "def14a": 4, "13f": 5}
_NATURE: Final[dict[str, str]] = {"D": "direct", "I": "indirect"}
_DERA_ID = re.compile(r":(NDT|NDH):")


@dataclass(frozen=True)
class Group:
    instrument_id: int
    accession: str
    document_type: str
    is_tombstone: bool
    filed_at: datetime
    filer_cik: str
    di: str
    titles: tuple[str, ...]
    shares: tuple[Decimal, ...]
    verdict: str  # one_line | summed | refused:<reason>
    value: Decimal | None
    period_end: date | None


def _refusal(
    lines: list[tuple[str, Decimal]],
    holding_amounts: dict[str, list[Decimal]],
    txn_balances: dict[str, set[Decimal]],
    owner_count: int,
) -> str | None:
    """The spec's line-sum conditions: ``form3_observation_shares``'s over the filing's holding
    lines, plus no amount of the group equal to a transaction column-5 balance of the title.
    Returns the first failed condition, or None when the lines sum."""
    if owner_count != 1:
        return "joint_filing"
    if any(s < 0 for _, s in lines):
        return "negative"
    titles = {t for t, _ in lines}
    if len(titles) != 1:
        return "multi_title"
    title = next(iter(titles))
    if not title.strip():
        return "blank_title"
    amounts = holding_amounts.get(title, [])
    if len(set(amounts)) != len(amounts):
        return "equal_amounts"
    if {s for _, s in lines} & txn_balances.get(title, set()):
        return "equal_to_transaction_balance"
    return None


def _as_of(signature: date | None, period: date | None, filed: date) -> date | None:
    if signature is not None and period is not None and period <= signature <= filed:
        return signature
    if period is not None and period <= filed:
        return period
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--examples", type=int, default=3)
    ap.add_argument("--show", action="append", default=[], help="print every group of this accession")
    args = ap.parse_args()

    groups: list[Group] = []
    counts: Counter[str] = Counter()
    accessions_seen: set[str] = set()
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        with conn.cursor(name="payloads") as cur:
            cur.itersize = 2000
            cur.execute(_PAYLOADS)
            for accession, instrument_id, document_type, is_tombstone, filed_at, payload in cur:
                counts["payload_rows"] += 1
                accessions_seen.add(accession)
                if instrument_id is None:
                    counts["excluded_no_filing_row"] += 1
                    continue
                try:
                    root = ET.fromstring(_XMLNS_RE.sub("", _unwrap_sgml_submission(payload)))  # noqa: S314
                except ET.ParseError:
                    counts["excluded_unparseable"] += 1
                    continue
                if _localname(root.tag) != "ownershipDocument":
                    counts["excluded_not_ownership_document"] += 1
                    continue
                filers = _extract_filers(root)
                if not filers:
                    counts["excluded_no_filers"] += 1
                    continue
                cik0 = filers[0].filer_cik
                all_holdings = _extract_holdings(root, default_filer_cik=cik0)
                holdings = [h for h in all_holdings if not h.is_derivative]
                if not holdings:
                    if not all_holdings:
                        counts["excluded_no_holding_parsed"] += 1
                        continue
                    counts["derivative_holdings_only"] += 1
                    if is_tombstone:
                        counts["derivative_holdings_only_tombstoned"] += 1
                        payload_type = _text(root.find("./documentType")) or "4"
                        if (
                            payload_type in ("4", "4/A")
                            and filed_at is not None
                            and form4_within_retention(filed_at.astimezone(UTC).date())
                        ):
                            counts["derivative_holdings_only_tombstoned_replay_eligible"] += 1
                    continue
                counts["with_nonderivative_holding"] += 1
                counts["holding_lines"] += len(holdings)
                payload_type = _text(root.find("./documentType")) or "4"
                if payload_type != document_type:
                    counts["document_type_mismatch_payload_vs_filing_row"] += 1
                if payload_type not in ("4", "4/A"):
                    counts["excluded_document_type"] += 1
                    continue
                if filed_at is None:
                    counts["excluded_no_manifest_filed_at"] += 1
                    continue
                filed = filed_at.astimezone(UTC).date()
                if not form4_within_retention(filed):
                    counts["excluded_retention"] += 1
                    continue
                txns = [t for t in _extract_transactions(root, default_filer_cik=cik0) if not t.is_derivative]
                covered = {(t.filer_cik, "I" if t.direct_indirect == "I" else "D") for t in txns}
                holding_amounts: dict[str, list[Decimal]] = defaultdict(list)
                for h in holdings:
                    if h.shares is not None:
                        holding_amounts[(h.security_title or "").strip()].append(h.shares)
                txn_balances: dict[str, set[Decimal]] = defaultdict(set)
                for t in txns:
                    if t.post_transaction_shares is not None:
                        txn_balances[(t.security_title or "").strip()].add(t.post_transaction_shares)
                as_of = _as_of(
                    _date(_text(root.find("./ownerSignature/signatureDate"))),
                    _date(_text(root.find("./periodOfReport"))),
                    filed,
                )
                owner_count = len({f.filer_cik for f in filers})
                by_di: dict[str, list[tuple[str, Decimal]]] = defaultdict(list)
                unreadable: set[str] = set()
                for h in holdings:
                    if h.direct_indirect not in _NATURE:
                        counts["holding_lines_no_di"] += 1
                        continue
                    if (cik0, h.direct_indirect) in covered:
                        counts[f"mixed_lines_{h.direct_indirect}"] += 1
                        continue
                    if h.shares is None:
                        # A value-only line (the SEC value branch) or an unreadable amount: either
                        # way the group's share total is unknown, so the group refuses.
                        counts["holding_lines_no_share_count"] += 1
                        unreadable.add(h.direct_indirect)
                        continue
                    by_di[h.direct_indirect].append(((h.security_title or "").strip(), h.shares))
                for di in unreadable - set(by_di):
                    by_di[di] = []
                for di, lines in by_di.items():
                    if di in unreadable:
                        verdict, value = "refused:no_share_count_line", None
                    elif len(lines) == 1:
                        verdict, value = "one_line", lines[0][1]
                    else:
                        reason = _refusal(lines, holding_amounts, txn_balances, owner_count)
                        verdict = "summed" if reason is None else f"refused:{reason}"
                        value = sum((s for _, s in lines), Decimal(0)) if reason is None else None
                    groups.append(
                        Group(
                            instrument_id=instrument_id,
                            accession=accession,
                            document_type=document_type,
                            is_tombstone=is_tombstone,
                            filed_at=filed_at,
                            filer_cik=(cik0 or "").strip(),
                            di=di,
                            titles=tuple(t for t, _ in lines),
                            shares=tuple(s for _, s in lines),
                            verdict=verdict,
                            value=value,
                            period_end=as_of,
                        )
                    )
        form5 = conn.execute(_FORM5_WITH_HOLDINGS).fetchone()
        current_rows = conn.execute(_CURRENT).fetchall()

    current: dict[tuple[int, str, str], tuple[Any, ...]] = {}
    dera_by_accession: dict[tuple[int, str, str], list[tuple[Any, ...]]] = defaultdict(list)
    for r in current_rows:
        iid, hik, nature, source, sdoc, sacc, pend, filed, shares = r
        current[(iid, hik, nature)] = (source, sdoc, pend, filed, shares)
        if sdoc and _DERA_ID.search(sdoc):
            dera_by_accession[(iid, hik, sacc)].append(r)

    print("== cohort ==")
    print(f"  distinct accessions: {len(accessions_seen):,}")
    for k in sorted(counts):
        print(f"  {k}: {counts[k]:,}")
    print(f"  form5_xml payloads with a nonDerivativeHolding (out of scope): {form5[0] if form5 else 0:,}")
    print("  (date, retention, document-type and mixed/no-share counters cover the non-derivative cohort only)")

    print("\n== holding-only groups (instrument, accession, filer, D/I) ==")
    shape: Counter[tuple[str, str, str]] = Counter()
    for g in groups:
        shape[(g.di, "tombstone" if g.is_tombstone else "live", g.verdict)] += 1
    for (di, tomb, verdict), n in sorted(shape.items()):
        print(f"  {di} {tomb:9s} {verdict:24s} {n:,}")
    no_date = sum(1 for g in groups if g.value is not None and g.period_end is None)
    print(f"  written groups with no period_end: {no_date:,}")

    # The latest written group per key, by the winner order's own keys.
    best: dict[tuple[int, str, str], Group] = {}
    name_only = sum(1 for g in groups if g.value is not None and not g.filer_cik.isdigit())
    print(f"  written groups with no numeric filer CIK (left out of the key comparison): {name_only:,}")
    for g in groups:
        if g.value is None or g.period_end is None or not g.filer_cik.isdigit():
            continue
        key = (g.instrument_id, f"CIK:{g.filer_cik.zfill(10)}", _NATURE[g.di])
        prev = best.get(key)
        if prev is None or (g.period_end, g.filed_at, g.accession) > (
            prev.period_end or date.min,
            prev.filed_at,
            prev.accession,
        ):
            best[key] = g

    print("\n== the latest written group per key vs the stored _current winner ==")
    verdicts: Counter[tuple[str, str]] = Counter()
    instruments: dict[str, set[int]] = defaultdict(set)
    examples: dict[str, list[str]] = defaultdict(list)
    winning: dict[tuple[int, str, str], set[str]] = defaultdict(set)
    for key, g in best.items():
        assert g.period_end is not None and g.value is not None
        cur_row = current.get(key)
        if cur_row is None:
            verdict, prov = "new_key", "-"
        else:
            source, sdoc, cur_pend, cur_filed, cur_shares = cur_row
            prov = f"{source}:{'dera' if sdoc and _DERA_ID.search(sdoc) else 'xml'}"
            new_rank, cur_rank = 1, _SOURCE_RANK.get(source, 10)
            new_order = (g.period_end, g.filed_at)
            cur_order = (cur_pend or date.min, cur_filed or datetime.min.replace(tzinfo=g.filed_at.tzinfo))
            # Production orders ``period_end DESC, filed_at DESC`` with PostgreSQL's default NULLS
            # FIRST, so a same-rank NULL sorts ahead of any value.
            null_first = new_rank == cur_rank and (cur_pend is None or (cur_pend == g.period_end and cur_filed is None))
            if null_first:
                verdict = "loses_to_current_null_date"
            elif new_rank > cur_rank or (new_rank == cur_rank and new_order < cur_order):
                verdict = "loses_to_current"
            elif new_rank == cur_rank and new_order == cur_order:
                verdict = "ties_current"
            elif new_rank < cur_rank and cur_order > new_order:
                verdict = "wins_by_source_rank_over_later"
            elif cur_shares == g.value:
                verdict = "wins_same_value"
            else:
                verdict = "wins_value_differs"
            ex_key = f"{verdict}:{g.di}:{g.verdict}"
            wanted = verdict in ("wins_value_differs", "wins_by_source_rank_over_later")
            if wanted and len(examples[ex_key]) < args.examples:
                examples[ex_key].append(
                    f"iid={key[0]} {key[1]} {g.di} current={cur_shares} ({prov} {sdoc} pe={cur_pend}) "
                    f"-> {g.accession} pe={g.period_end} value={g.value} lines={list(g.shares)}"
                )
        verdicts[(verdict, prov)] += 1
        instruments[verdict].add(key[0])
        if verdict == "new_key" or verdict.startswith("wins"):
            winning[(key[0], key[1], g.accession)].add(key[2])
    for (verdict, prov), n in sorted(verdicts.items()):
        print(f"  {verdict:32s} {prov:12s} {n:,}")
    totals: Counter[str] = Counter()
    for (verdict, _p), n in verdicts.items():
        totals[verdict] += n
    for verdict, iids in sorted(instruments.items()):
        print(f"  {verdict}: keys {totals[verdict]:,} over instruments {len(iids):,}")

    print("\n== DERA _current winners on an accession where a holding group wins (de-collision clause 1) ==")
    written = [g for g in groups if g.is_tombstone and g.value is not None and g.period_end is not None]
    written = [g for g in written if g.filer_cik.isdigit()]
    xml_values = {
        (g.instrument_id, f"CIK:{g.filer_cik.zfill(10)}", g.accession, g.value)
        for g in groups
        if g.value is not None and g.filer_cik.isdigit()
    }
    won_keys = {(k[0], k[1], nature) for k, natures in winning.items() for nature in natures}
    # Clause 1 needs an XML WINNER on the accession, so only holders whose holding group wins its key.
    # A DERA row on the very key the holding wins is REPLACED there, not suppressed; count it apart.
    same_key = [r for k, natures in winning.items() for r in dera_by_accession.get(k, []) if r[2] in natures]
    # A DERA row on a key some OTHER accession's holding wins is replaced there too, not dropped.
    suppressed = [r for k in winning for r in dera_by_accession.get(k, []) if (r[0], r[1], r[2]) not in won_keys]
    print(f"  DERA winners replaced on the key the holding wins: {len(same_key):,}")
    amount_equal = sum(1 for r in suppressed if (r[0], r[1], r[5], r[8]) in xml_values)
    print(f"  tombstoned accessions with a written holding-only group: {len({g.accession for g in written}):,}")
    print(f"  DERA winners suppressed: {len(suppressed):,} over instruments {len({r[0] for r in suppressed}):,}")
    print(f"  ... amount-equal to an XML observation of the accession (NOT proof of coverage): {amount_equal:,}")

    print("\n== examples ==")
    for k, rows in sorted(examples.items()):
        print(f"  [{k}]")
        for row in rows:
            print(f"    {row}")
    for accession in args.show:
        print(f"\n== --show {accession} ==")
        for g in groups:
            if g.accession == accession:
                print(
                    f"  iid={g.instrument_id} CIK:{g.filer_cik} {g.di} {g.verdict} value={g.value} "
                    f"pe={g.period_end} lines={list(zip(g.titles, g.shares, strict=True))}"
                )
                cur_row = current.get((g.instrument_id, f"CIK:{g.filer_cik.zfill(10)}", _NATURE[g.di]))
                print(f"    current winner: {cur_row}")


if __name__ == "__main__":
    main()
