"""#3227 — a Form 4's holding lines beside transaction lines of the same filer and D/I ("mixed").

    PYTHONPATH=. uv run python -m scripts.census_3227_form4_mixed_groups
    PYTHONPATH=. uv run python -m scripts.census_3227_form4_mixed_groups --examples 5 \
        --show 0001858681-24-000036

Read-only.  Nothing is written, nothing is refreshed.  Spec:
docs/specs/ownership/2026-10-18-3227-form4-mixed-groups.md, whose rule this script applies so every
count the spec quotes comes from here.

## What is being measured

#3882 writes a Form 4 holding line (``insider_form4_holdings``) only when the accession has no
transaction line of the same filer and D/I.  Where one exists, the observation is the transaction
writers' value — the latest line's column 5, or the ``I`` vehicle sum of
``form4_observation_shares`` — and the holding lines are ignored.  APO ``0001858681-24-000036``
(Kleinman): the ``I`` observation is HCM Series C's 110,752; the same filing's nine other indirect
vehicles (Series A 324,435, KRT Delaware 1,806,086, ...) are holding lines.

## Cohort

Every ``(instrument, accession, filer, D/I)`` with a non-derivative holding line and a
non-derivative transaction line of that D/I (a transaction with no D/I counts as ``D``), on a live
Form 4/4A, read from ``insider_form4_holdings`` / ``insider_transactions`` as stored.  Today's value
is the stored current observation (``known_to IS NULL``) of that accession, holder and nature.

## The rule applied (the spec's Change section)

Each holding line is folded into the unit as a one-line form of its own, and the key's value is the
sum of its forms' final balances, under ``form4_observation_shares``'s conditions extended to the
holding lines; any failed condition keeps today's value.

## What it cannot establish

* That folded lines are disjoint holdings: a block nested inside a larger unequal line passes.
* The ``_current`` move beyond the winner's own value: the observation's ``period_end`` does not
  change, so only keys whose stored winner is this accession's observation can move here.  The
  build's A/B diffs the real refreshers.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Final, LiteralString

import psycopg
import psycopg.rows

from app.config import settings
from app.services.insider_transactions import form4_earlier_balances, form4_form_key

_UNITS: Final[LiteralString] = """
SELECT DISTINCT h.instrument_id, h.accession_number, h.filer_cik, f.document_type
  FROM insider_form4_holdings h
  JOIN insider_filings f USING (accession_number)
 WHERE NOT h.is_derivative AND f.document_type IN ('4', '4/A') AND NOT f.is_tombstone
   AND h.direct_indirect IN ('D', 'I')
   AND EXISTS (
         SELECT 1 FROM insider_transactions t
          WHERE t.accession_number = h.accession_number AND t.instrument_id = h.instrument_id
            AND t.filer_cik = h.filer_cik AND NOT t.is_derivative
            AND (CASE WHEN t.direct_indirect = 'I' THEN 'I' ELSE 'D' END) = h.direct_indirect)
"""

_TXNS: Final[LiteralString] = """
SELECT instrument_id, accession_number, filer_cik, txn_row_num, security_title, txn_date,
       txn_date_invalid, shares, acquired_disposed_code, post_transaction_shares, direct_indirect,
       nature_of_ownership
  FROM insider_transactions
 WHERE accession_number = ANY(%(accs)s::text[]) AND NOT is_derivative
"""

_HOLDINGS: Final[LiteralString] = """
SELECT instrument_id, accession_number, filer_cik, row_num, security_title, shares,
       direct_indirect, nature_of_ownership
  FROM insider_form4_holdings
 WHERE accession_number = ANY(%(accs)s::text[]) AND NOT is_derivative
"""

_OWNERS: Final[LiteralString] = """
SELECT accession_number, array_agg(filer_cik) FROM insider_filers
 WHERE accession_number = ANY(%(accs)s::text[]) GROUP BY 1
"""

_OBSERVATIONS: Final[LiteralString] = """
SELECT instrument_id, source_document_id, holder_cik, ownership_nature, shares, period_end
  FROM ownership_insiders_observations
 WHERE source = 'form4' AND known_to IS NULL AND source_document_id = ANY(%(accs)s::text[])
"""

_CURRENT: Final[LiteralString] = """
SELECT instrument_id, holder_cik, ownership_nature, source_document_id, shares
  FROM ownership_insiders_current WHERE source = 'form4'
"""

_CHUNK: Final = 2000
_NATURE: Final[dict[str, str]] = {"D": "direct", "I": "indirect"}
SUMS: Final = "sums"


@dataclass(frozen=True)
class Txn:
    txn_row_num: int
    security_title: str | None
    txn_date: date | None
    txn_date_invalid: bool
    shares: Decimal | None
    acquired_disposed_code: str | None
    post_transaction_shares: Decimal | None
    direct_indirect: str | None
    nature_of_ownership: str | None
    is_derivative: bool = False


@dataclass(frozen=True)
class Holding:
    row_num: int
    security_title: str | None
    shares: Decimal | None
    direct_indirect: str | None
    nature_of_ownership: str | None


def _opening(t: Txn) -> Decimal | None:
    if t.post_transaction_shares is None or t.shares is None:
        return None
    if t.acquired_disposed_code == "A":
        return t.post_transaction_shares - t.shares
    if t.acquired_disposed_code == "D":
        return t.post_transaction_shares + t.shares
    return None


def _txn_usable(t: Txn) -> bool:
    post, shares, opening = t.post_transaction_shares, t.shares, _opening(t)
    return (
        post is not None
        and post >= 0
        and shares is not None
        and shares >= 0
        and opening is not None
        and opening >= 0
        and t.txn_date is not None
        and not t.txn_date_invalid
        and t.direct_indirect in ("D", "I")
        and bool((t.security_title or "").strip())
        and (t.direct_indirect == "D" or form4_form_key("I", t.nature_of_ownership)[1] != "")
    )


def _holding_usable(h: Holding) -> bool:
    return (
        h.shares is not None
        and h.shares >= 0
        and h.direct_indirect in ("D", "I")
        and bool((h.security_title or "").strip())
        and (h.direct_indirect == "D" or form4_form_key("I", h.nature_of_ownership)[1] != "")
    )


Form = tuple[str | None, str]  # a transaction form (form4_form_key), or ("H", row_num) for a holding


def fold_verdict(
    txns: list[Txn],
    holdings: list[Holding],
    di: str,
    *,
    filer_cik: str,
    owner_ciks: Iterable[str | None],
    earlier_balances: Callable[[str], Iterable[tuple[str | None, str | None, Decimal]] | None],
) -> tuple[str, Decimal | None]:
    """The spec's rule for ONE mixed key: the filer's ``di`` value in this accession folds its
    holding lines in as one-line forms.  Returns ``(verdict, value)``; the value is set only on
    :data:`SUMS`.  Verdict labels name the first failed condition, in the spec's order."""
    txns = sorted(txns, key=lambda t: t.txn_row_num)
    if not all(_txn_usable(t) for t in txns) or not all(_holding_usable(h) for h in holdings):
        return "2 incomplete", None
    if len({t.security_title for t in txns} | {h.security_title for h in holdings}) != 1:
        return "3 multi_title", None
    if {(c or "").strip() for c in owner_ciks} != {filer_cik}:
        return "4 joint_or_owner_mismatch", None

    by_form: dict[Form, list[Txn]] = defaultdict(list)
    for t in txns:
        by_form[form4_form_key(t.direct_indirect, t.nature_of_ownership)].append(t)
    for lines in by_form.values():
        for prev, cur in zip(lines, lines[1:], strict=False):
            if _opening(cur) != prev.post_transaction_shares:
                return "5a form_breaks_chain", None
    at_balance: dict[Decimal, set[Form]] = defaultdict(set)
    for form, lines in by_form.items():
        for t in lines:
            if t.post_transaction_shares:
                at_balance[t.post_transaction_shares].add(form)
    for h in holdings:
        if h.shares:
            at_balance[h.shares].add(("H", str(h.row_num)))
    for form, lines in by_form.items():
        for t in lines:
            opening = _opening(t)
            if opening is not None and at_balance.get(opening, set()) - {form}:
                return "5b running_total", None
    txn_balances = {t.post_transaction_shares for t in txns if t.post_transaction_shares}
    if any(h.shares in txn_balances for h in holdings):
        return "5b holding_equals_transaction_balance", None

    summed = [f for f in by_form if (f[0] == "I") == (di == "I")]
    firsts = {f: _opening(by_form[f][0]) for f in summed}
    if any(o != 0 for o in firsts.values()):
        title = txns[0].security_title or ""
        history = earlier_balances(title)
        if history is None:
            return "5c running_total_across_filings", None
        seen = {(form4_form_key(d, n), bal) for d, n, bal in history}
        for form, opening in firsts.items():
            if opening == 0:
                continue
            if any(bal == opening and f != form for f, bal in seen) and (form, opening) not in seen:
                return "5c running_total_across_filings", None

    finals: list[Decimal] = []
    for lines in by_form.values():
        last = max(lines, key=lambda t: (t.txn_date or date.min, t.txn_row_num))
        finals.append(last.post_transaction_shares or Decimal(0))
    finals.extend(h.shares or Decimal(0) for h in holdings)
    nonzero = [v for v in finals if v != 0]
    if len(set(nonzero)) != len(nonzero):
        return "6 equal_finals", None

    value = Decimal(0)
    for f in summed:
        last = max(by_form[f], key=lambda t: (t.txn_date or date.min, t.txn_row_num))
        value += last.post_transaction_shares or Decimal(0)
    value += sum((h.shares or Decimal(0) for h in holdings if h.direct_indirect == di), Decimal(0))
    return SUMS, value


@dataclass(frozen=True)
class Result:
    instrument_id: int
    accession: str
    filer_cik: str
    document_type: str
    di: str
    verdict: str
    value: Decimal | None
    today: Decimal | None
    holding_lines: int


def collect(conn: psycopg.Connection[Any]) -> tuple[list[Result], Counter[str]]:
    counts: Counter[str] = Counter()
    units = conn.execute(_UNITS).fetchall()
    counts["mixed_units"] = len(units)
    accessions = sorted({u[1] for u in units})
    unit_set = {(u[0], u[1], u[2]): u[3] for u in units}
    results: list[Result] = []
    for i in range(0, len(accessions), _CHUNK):
        chunk = accessions[i : i + _CHUNK]
        chunk_set = set(chunk)
        txns: dict[tuple[int, str, str], list[Txn]] = defaultdict(list)
        holds: dict[tuple[int, str, str], list[Holding]] = defaultdict(list)
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            for r in cur.execute(_TXNS, {"accs": chunk}).fetchall():
                txns[(r["instrument_id"], r["accession_number"], r["filer_cik"])].append(
                    Txn(
                        txn_row_num=r["txn_row_num"],
                        security_title=r["security_title"],
                        txn_date=r["txn_date"],
                        txn_date_invalid=bool(r["txn_date_invalid"]),
                        shares=r["shares"],
                        acquired_disposed_code=r["acquired_disposed_code"],
                        post_transaction_shares=r["post_transaction_shares"],
                        direct_indirect=r["direct_indirect"],
                        nature_of_ownership=r["nature_of_ownership"],
                    )
                )
            for r in cur.execute(_HOLDINGS, {"accs": chunk}).fetchall():
                holds[(r["instrument_id"], r["accession_number"], r["filer_cik"])].append(
                    Holding(
                        row_num=r["row_num"],
                        security_title=r["security_title"],
                        shares=r["shares"],
                        direct_indirect=r["direct_indirect"],
                        nature_of_ownership=r["nature_of_ownership"],
                    )
                )
        owners = {a: list(c) for a, c in conn.execute(_OWNERS, {"accs": chunk}).fetchall()}
        today: dict[tuple[int, str, str, str], tuple[Decimal, date]] = {}
        for iid, acc, cik, nature, shares, pend in conn.execute(_OBSERVATIONS, {"accs": chunk}).fetchall():
            today[(iid, acc, (cik or "").strip(), nature)] = (shares, pend)
        for key, document_type in unit_set.items():
            if key[1] not in chunk_set:
                continue
            iid, acc, cik = key
            unit_cik = (cik or "").strip()
            t_lines, h_lines = txns.get(key, []), holds.get(key, [])
            covered = {"I" if t.direct_indirect == "I" else "D" for t in t_lines}
            txn_forms = {form4_form_key(t.direct_indirect, t.nature_of_ownership) for t in t_lines}
            for h in h_lines:
                if h.direct_indirect == "I" and "I" in covered:
                    same = form4_form_key("I", h.nature_of_ownership) in txn_forms
                    counts["mixed_I_lines_sharing_a_transacting_nature" if same else "mixed_I_lines_other_nature"] += 1
            for di in ("D", "I"):
                if di not in covered or not any(h.direct_indirect == di for h in h_lines):
                    continue
                if not unit_cik:
                    verdict, value = "0 blank_cik", None
                else:
                    verdict, value = fold_verdict(
                        t_lines,
                        h_lines,
                        di,
                        filer_cik=unit_cik,
                        owner_ciks=owners.get(acc, []),
                        earlier_balances=lambda title, iid=iid, acc=acc, cik=unit_cik: form4_earlier_balances(
                            conn, title, instrument_id=iid, accession_number=acc, filer_cik=cik
                        ),
                    )
                obs = today.get((iid, acc, unit_cik, _NATURE[di]))
                results.append(
                    Result(
                        instrument_id=iid,
                        accession=acc,
                        filer_cik=unit_cik,
                        document_type=document_type,
                        di=di,
                        verdict=verdict,
                        value=value,
                        today=obs[0] if obs else None,
                        holding_lines=sum(1 for h in h_lines if h.direct_indirect == di),
                    )
                )
    return results, counts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--examples", type=int, default=3)
    ap.add_argument("--show", action="append", default=[], help="print every mixed key of this accession")
    args = ap.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        results, counts = collect(conn)
        current = {
            (iid, (cik or "").strip(), nature): (sdoc, shares)
            for iid, cik, nature, sdoc, shares in conn.execute(_CURRENT).fetchall()
        }

    print("== cohort ==")
    for k in sorted(counts):
        print(f"  {k}: {counts[k]:,}")
    print(f"  mixed keys (unit, D/I): {len(results):,}")
    print(
        f"  mixed holding lines: D {sum(r.holding_lines for r in results if r.di == 'D'):,}"
        f" I {sum(r.holding_lines for r in results if r.di == 'I'):,}"
    )
    print(f"  keys with no stored observation today: {sum(1 for r in results if r.today is None):,}")

    print("\n== verdict per mixed key ==")
    print("  di verdict                                   keys  instruments  4/A  current-winner keys")
    table: dict[tuple[str, str], list[Result]] = defaultdict(list)
    for r in results:
        table[(r.di, r.verdict)].append(r)
    winner = {
        id(r) for r in results if current.get((r.instrument_id, r.filer_cik, _NATURE[r.di]), (None,))[0] == r.accession
    }
    for (di, verdict), rs in sorted(table.items()):
        print(
            f"  {di}  {verdict:38s} {len(rs):7,} {len({r.instrument_id for r in rs}):11,}"
            f" {sum(1 for r in rs if r.document_type == '4/A'):5,} {sum(1 for r in rs if id(r) in winner):10,}"
        )

    print("\n== sums vs today's observation ==")
    moves: Counter[tuple[str, str]] = Counter()
    for r in results:
        if r.verdict != SUMS or r.value is None:
            continue
        if r.today is None:
            moves[(r.di, "no_observation_today")] += 1
        else:
            moves[(r.di, "up" if r.value > r.today else "down" if r.value < r.today else "equal")] += 1
    for (di, m), n in sorted(moves.items()):
        print(f"  {di} {m:22s} {n:,}")
    moved_winners = [
        r for r in results if r.verdict == SUMS and id(r) in winner and r.value is not None and r.value != r.today
    ]
    print(
        f"  _current keys whose winner value moves: {len(moved_winners):,}"
        f" over instruments {len({r.instrument_id for r in moved_winners}):,}"
    )
    for di in ("D", "I"):
        ws = [r for r in moved_winners if r.di == di]
        print(f"    {di}: {len(ws):,} keys over {len({r.instrument_id for r in ws}):,} instruments")

    print("\n== examples ==")
    shown: Counter[tuple[str, str]] = Counter()
    for r in sorted(results, key=lambda r: (r.di, r.verdict, r.accession)):
        k = (r.di, r.verdict)
        if shown[k] >= args.examples or r.verdict == SUMS and r.value == r.today:
            continue
        shown[k] += 1
        print(
            f"  [{r.di} {r.verdict}] iid={r.instrument_id} {r.accession} CIK:{r.filer_cik} today={r.today} -> {r.value}"
        )
    for acc in args.show:
        print(f"\n== --show {acc} ==")
        for r in results:
            if r.accession == acc:
                print(f"  iid={r.instrument_id} CIK:{r.filer_cik} {r.di} {r.verdict} today={r.today} value={r.value}")


if __name__ == "__main__":
    main()
