"""DB-backed tests for the Section 16 exit-box release on the insiders wedge
(#2788 / #2226 M1).

Third sibling of ``test_ownership_13f_hr_supersession.py`` (#2229) and
``test_ownership_insider_retention_bound.py`` (#2788's first half), and the three-way
contrast is the point:

* #2229 removes a stale 13F row because Form 13F Special Instruction 5b makes a holdings
  report a COMPLETE statement, so omission is affirmative evidence of an exit.
* The retention bound removes a Form 4 row because we no longer hold in-retention evidence
  of it. Explicitly NOT an exit claim — Form 4 is transaction-triggered and silence proves
  nothing.
* **This** removes a holder because the filer TOLD US, on the form: SEC Form 4 General
  Instruction 1(b), *"A reporting person no longer subject to Section 16 … must check the
  exit box appearing on this Form."* Affirmative, like #2229; but what it affirms is a
  change of STATUS, never a disposal — which is why the operator-facing string is asserted
  here and not left to drift.

The predicate lives in SQL (``_INSIDER_SECTION16_EXIT_SQL``, interpolated into BOTH
``_collect_canonical_holders_from_current`` and ``_read_section16_exit_insiders``), so the
behavioural cases run against a real Postgres. The structural case is pure and pins the
shared-fragment pairing.

Instrument-id range 2_226_xxx is reserved for these scenarios.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest

from app.services import ownership_rollup
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

# Well inside the Form 4 retention window, so these scenarios exercise the exit predicate
# rather than tripping ``_INSIDER_BEYOND_RETENTION_SQL`` first.
_TIP_DAY = date(2026, 6, 1)
_EARLIER = date(2024, 1, 15)
_LATER = date(2026, 8, 1)

_EXITED_CIK = "0001111111"
_STAYING_CIK = "0002222222"
_ISSUER_CIK = "0009999999"


# ---------------------------------------------------------------------------
# Structural — pure, no DB
# ---------------------------------------------------------------------------


def test_exclusion_and_telemetry_share_one_predicate() -> None:
    """Reader exclusion and telemetry producer must select exact complements, and the
    only thing that guarantees it is that both interpolate the SAME fragment. #2229 kept
    its pair in step with a comment, and a comment does not fail when someone edits one
    side.

    Asserts on the SOURCE of both functions with their docstrings stripped — both
    docstrings NAME the fragment, so a plain substring check over ``inspect.getsource``
    passes on the prose alone and would not catch a future edit that inlined the
    predicate. Same technique, and same reason, as the retention-bound sibling.
    """
    import ast
    import inspect
    import textwrap

    def _body_without_docstring(fn: object) -> str:
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))  # type: ignore[arg-type]
        node = tree.body[0]
        assert isinstance(node, ast.FunctionDef)
        body = node.body
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            body = body[1:]
        return "\n".join(ast.unparse(stmt) for stmt in body)

    fragment = ownership_rollup._INSIDER_SECTION16_EXIT_SQL

    # Arm 1 — an unidentified holder is never released. A NULL CIK SATISFIES a bare
    # NOT EXISTS, so without this line the least identifiable rows would be the easiest
    # to remove.
    assert "oc.holder_cik IS NOT NULL" in fragment
    # Arm 3 — NULL is "we do not know", never an exit. ``IS NOT TRUE`` (not ``IS FALSE``)
    # is what makes an all-NULL tip fail closed; 310,780 filings carry a NULL flag.
    assert "IS NOT TRUE" in fragment
    assert "IS NOT FALSE" not in fragment
    # Arm 4 — the later-filing guard must be ``>=`` minus the tip's own accessions, not a
    # strict ``>``. ``period_of_report`` is the covered transaction date, not filing order,
    # so a same-period amendment clearing the box slips past a strict comparison (93
    # holders measured). Asserted on the operator because it is a one-character regression.
    assert "lf.period_of_report >= " in fragment
    assert "lf.period_of_report > " not in fragment
    # Arm 4 must also reach share-class siblings via the issuer, since the entity-level
    # filing lives under ONE instrument while observations fan out to all of them.
    assert "lf.issuer_cik" in fragment
    # Arm 5 — the deregistration carve-out.
    assert "sec_form25_common_equity_delistings" in fragment
    # ⚠ The evidence joins key on accession, which is ``insider_filings``' PRIMARY KEY.
    # Adding an instrument equality there looks like tighter attribution and is not: it is
    # redundant on a PK join and silently drops share-class siblings, 1,284 of 74,164
    # resolvable rows. The holder is attributed through ``insider_filers`` instead.
    assert "tf.instrument_id    = t.instrument_id" not in fragment
    assert "insider_filers tfl" in fragment
    # Tombstones are excluded everywhere the flag or a date is read: ``period_of_report``
    # is NULL on every tombstone, and a NULL fails the arm-4 comparison OPEN.
    assert fragment.count("is_tombstone") >= 3

    for fn in (
        ownership_rollup._collect_canonical_holders_from_current,
        ownership_rollup._read_section16_exit_insiders,
    ):
        src = _body_without_docstring(fn)
        assert "_INSIDER_SECTION16_EXIT_SQL" in src, f"{fn.__name__} does not use the shared fragment"
    # The producer must also reproduce the rest of the reader's selection, or it reports
    # rows the reader never excluded.
    producer_src = _body_without_docstring(ownership_rollup._read_section16_exit_insiders)
    for name in ("_INSIDER_DUAL_PIPELINE_DECOLLISION_SQL", "_INSIDER_BEYOND_RETENTION_SQL"):
        assert name in producer_src, f"producer does not reproduce {name}"


def test_census_measures_the_shipped_predicate() -> None:
    """The census must IMPORT the shipped predicate, not restate it.

    An earlier revision copied the tip definition into the script and kept the copies in
    step with a string-equality test. That test passed while the census measured the wrong
    thing: the tip rule alone, without arms 1, 4 and 5, which is 3,992 holders against the
    3,666 that actually ship. A census is only evidence if it measures the code it
    describes, and a copy plus a drift test is not the same as one definition.
    """
    from scripts import audit_2788_section16_exit as census

    assert census._EXIT is ownership_rollup._INSIDER_SECTION16_EXIT_SQL
    assert census._TIP is ownership_rollup._INSIDER_TIP_PERIOD_SQL
    # And the guards must actually reach the queries, not merely be imported.
    shipped = [sql for title, sql in census.QUERIES if "SHIPPED" in title]
    assert shipped, "no census query exercises the shipped predicate"
    assert any(ownership_rollup._INSIDER_SECTION16_EXIT_SQL in sql for sql in shipped)


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_outstanding(conn: psycopg.Connection[tuple], *, iid: int, shares: str) -> None:
    period_end = date(2026, 3, 31)
    conn.execute(
        """
        INSERT INTO financial_facts_raw (
            instrument_id, taxonomy, concept, unit, period_end, val,
            form_type, filed_date, accession_number, fiscal_year, fiscal_period
        ) VALUES (%s, 'dei', 'EntityCommonStockSharesOutstanding', 'shares', %s, %s,
                  '10-Q', %s, %s, %s, 'Q4')
        ON CONFLICT DO NOTHING
        """,
        (iid, period_end, Decimal(shares), period_end, f"OUT-{iid}", period_end.year),
    )


def _accession(iid: int, holder_cik: str, on: date) -> str:
    # sql/134 CHECKs ^[0-9]{10}-[0-9]{2}-[0-9]{6}$ — the sequence is SIX digits.
    return f"{holder_cik}-{on:%y}-{(iid + on.toordinal()) % 1_000_000:06d}"


def _seed_filing(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    holder_cik: str,
    on: date,
    exit_box: bool | None,
    issuer_cik: str | None = _ISSUER_CIK,
    is_tombstone: bool = False,
) -> str:
    """One ``insider_filings`` row plus its ``insider_filers`` child.

    ``period_of_report`` tracks ``on`` so a fixture cannot pass the tip comparison on one
    column while failing it on another — the predicate reads ``period_of_report`` for the
    later-filing arm and ``ownership_insiders_current.period_end`` for the tip itself, and
    a fixture whose dates disagree would hide which arm it exercised.
    """
    accession = _accession(iid, holder_cik, on)
    conn.execute(
        """
        INSERT INTO insider_filings (
            accession_number, instrument_id, document_type, period_of_report,
            not_subject_to_section_16, issuer_cik, is_tombstone
        ) VALUES (%s, %s, '4', %s, %s, %s, %s)
        ON CONFLICT (accession_number) DO UPDATE
           SET not_subject_to_section_16 = EXCLUDED.not_subject_to_section_16
        """,
        (accession, iid, on, exit_box, issuer_cik, is_tombstone),
    )
    conn.execute(
        """
        INSERT INTO insider_filers (accession_number, filer_cik, filer_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (accession_number, filer_cik) DO NOTHING
        """,
        (accession, holder_cik, f"HOLDER {holder_cik}"),
    )
    return accession


def _seed_insider_current(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    holder_cik: str | None,
    shares: str,
    on: date,
    accession: str | None,
    source: str = "form4",
    nature: str = "direct",
    identity_key: str | None = None,
) -> None:
    key = identity_key or (f"CIK:{holder_cik}" if holder_cik else f"NAME:UNKNOWN-{iid}")
    conn.execute(
        """
        INSERT INTO ownership_insiders_current (
            instrument_id, holder_cik, holder_name, holder_identity_key,
            ownership_nature, source, source_document_id, source_accession,
            filed_at, period_end, shares
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument_id, holder_identity_key, ownership_nature)
        DO UPDATE SET shares = EXCLUDED.shares, period_end = EXCLUDED.period_end
        """,
        (
            iid,
            holder_cik,
            f"HOLDER {holder_cik or 'UNKNOWN'}",
            key,
            nature,
            source,
            f"{key}-{on:%Y%m%d}-{nature}",
            accession,
            datetime(on.year, on.month, on.day, tzinfo=UTC),
            on,
            Decimal(shares),
        ),
    )


def _seed_form25(conn: psycopg.Connection[tuple], *, issuer_cik: str) -> None:
    conn.execute(
        """
        INSERT INTO sec_form25_register (
            accession_number, form, filed_date, issuer_cik,
            provision_class, security_class
        ) VALUES (%s, '25', %s, %s, 'equity_delisting', 'common_equity')
        ON CONFLICT (accession_number) DO NOTHING
        """,
        (f"{issuer_cik}-25-000001", date(2026, 5, 1), issuer_cik),
    )


def _insider_ciks(rollup: ownership_rollup.OwnershipRollup) -> set[str | None]:
    out: set[str | None] = set()
    for slc in rollup.slices:
        if slc.category == "insiders":
            out.update(h.filer_cik for h in slc.holders)
    return out


def _exit_corrections(rollup: ownership_rollup.OwnershipRollup) -> list[ownership_rollup.CorrectionApplied]:
    return [c for c in rollup.corrections_applied if c.kind == "insider_section16_exit_declared"]


# ---------------------------------------------------------------------------
# Behavioural
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_declared_exit_releases_the_holder_and_explains_why(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The base case: the holder's latest filing carries the exit box, so the holder
    leaves the insiders wedge and the removal is explained.

    The ``detail`` assertions are not decoration. The declaration is a change of STATUS,
    and a correction whose string reads as a disposal will be quoted back as though the
    filer had sold — which the source rule does not support (Rule 16a-2(b) is titled
    *transactions after termination of insider status*, i.e. the holder may still be
    trading the same shares).
    """
    conn = ebull_test_conn
    iid = 2_226_001
    _seed_instrument(conn, iid=iid, symbol="EXITED")
    _seed_outstanding(conn, iid=iid, shares="1000")
    acc = _seed_filing(conn, iid=iid, holder_cik=_EXITED_CIK, on=_TIP_DAY, exit_box=True)
    _seed_insider_current(conn, iid=iid, holder_cik=_EXITED_CIK, shares="400", on=_TIP_DAY, accession=acc)

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="EXITED", instrument_id=iid)

    assert _EXITED_CIK not in _insider_ciks(rollup)
    corrections = _exit_corrections(rollup)
    assert len(corrections) == 1
    correction = corrections[0]
    assert correction.filer_cik == _EXITED_CIK
    assert correction.shares_removed == Decimal(400)
    assert correction.superseded_period == _TIP_DAY
    assert correction.source_channel == "form4"
    assert correction.winning_accession == acc
    assert "STATUS, not a sale" in correction.detail
    assert "General Instruction" in correction.detail
    # The shares return to the unattributed residual rather than vanishing from the ring.
    assert rollup.residual.pct_outstanding == Decimal(1)


@pytest.mark.integration
def test_unflagged_holder_is_untouched(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The other side of the boundary: a filer who has NOT declared an exit stays, however
    old the row is. Without this the test file would only pin the direction that removes,
    and a predicate that released everything would still pass.
    """
    conn = ebull_test_conn
    iid = 2_226_002
    _seed_instrument(conn, iid=iid, symbol="STAYIN")
    _seed_outstanding(conn, iid=iid, shares="1000")
    acc = _seed_filing(conn, iid=iid, holder_cik=_STAYING_CIK, on=_TIP_DAY, exit_box=False)
    _seed_insider_current(conn, iid=iid, holder_cik=_STAYING_CIK, shares="250", on=_TIP_DAY, accession=acc)

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="STAYIN", instrument_id=iid)

    assert _STAYING_CIK in _insider_ciks(rollup)
    assert _exit_corrections(rollup) == []


@pytest.mark.integration
def test_unreadable_tip_is_not_released(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A holder whose tip carries NO readable flag must stay — the vacuity case.

    This is the ``x <> ALL('{}')`` failure mode in predicate form: the unanimity arm is a
    ``NOT EXISTS``, and an empty or all-NULL evidence set satisfies a ``NOT EXISTS``
    trivially. 71.2% of the live population is unreadable, so a predicate that passed
    vacuously would release most of the table while every other test still passed.
    """
    conn = ebull_test_conn
    iid = 2_226_003
    _seed_instrument(conn, iid=iid, symbol="NOFLAG")
    _seed_outstanding(conn, iid=iid, shares="1000")
    acc = _seed_filing(conn, iid=iid, holder_cik=_EXITED_CIK, on=_TIP_DAY, exit_box=None)
    _seed_insider_current(conn, iid=iid, holder_cik=_EXITED_CIK, shares="400", on=_TIP_DAY, accession=acc)

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="NOFLAG", instrument_id=iid)

    assert _EXITED_CIK in _insider_ciks(rollup)
    assert _exit_corrections(rollup) == []


@pytest.mark.integration
def test_disagreeing_tip_is_not_released(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Two rows at the same tip period, one flagged and one not → refuse. 6 holders in the
    live corpus have this shape, and the conservative direction is the only defensible one:
    the box is document-level, so disagreement between two of the holder's own tip rows
    means we cannot say which document describes their status."""
    conn = ebull_test_conn
    iid = 2_226_004
    _seed_instrument(conn, iid=iid, symbol="SPLITT")
    _seed_outstanding(conn, iid=iid, shares="1000")
    flagged = _seed_filing(conn, iid=iid, holder_cik=_EXITED_CIK, on=_TIP_DAY, exit_box=True)
    unflagged = _accession(iid, _EXITED_CIK, _TIP_DAY.replace(day=2))
    conn.execute(
        """
        INSERT INTO insider_filings (accession_number, instrument_id, document_type,
                                     period_of_report, not_subject_to_section_16, issuer_cik)
        VALUES (%s, %s, '4', %s, FALSE, %s)
        ON CONFLICT (accession_number) DO NOTHING
        """,
        (unflagged, iid, _TIP_DAY, _ISSUER_CIK),
    )
    conn.execute(
        "INSERT INTO insider_filers (accession_number, filer_cik, filer_name) VALUES (%s, %s, %s)"
        " ON CONFLICT DO NOTHING",
        (unflagged, _EXITED_CIK, "HOLDER"),
    )
    # Same holder_identity_key, same period_end, different nature → both are tip rows.
    _seed_insider_current(conn, iid=iid, holder_cik=_EXITED_CIK, shares="300", on=_TIP_DAY, accession=flagged)
    _seed_insider_current(
        conn, iid=iid, holder_cik=_EXITED_CIK, shares="100", on=_TIP_DAY, accession=unflagged, nature="indirect"
    )

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="SPLITT", instrument_id=iid)

    assert _EXITED_CIK in _insider_ciks(rollup)
    assert _exit_corrections(rollup) == []


@pytest.mark.integration
def test_tip_mixing_a_flagged_row_with_an_unreadable_one_is_not_released(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Flagged row + NULL-flag row at the same tip → refuse.

    ⚠ This is the ONLY case that discriminates ``IS NOT TRUE`` from ``IS FALSE`` in the
    unanimity arm, and it was added after a revert probe: flipping the operator left every
    other behavioural test green, because an all-NULL tip is already refused by the
    POSITIVE arm and a FALSE-vs-TRUE tip is refused either way. Without this case the
    NULL hole would have been caught only by a string assertion.

    The hole matters at scale: 310,780 filings carry a NULL flag and 71.2% of the live
    population has no readable status at all, so "NULL does not object" would release
    holders on the strength of one flagged row beside evidence we cannot read.
    """
    conn = ebull_test_conn
    iid = 2_226_009
    _seed_instrument(conn, iid=iid, symbol="MIXNUL")
    _seed_outstanding(conn, iid=iid, shares="1000")
    flagged = _seed_filing(conn, iid=iid, holder_cik=_EXITED_CIK, on=_TIP_DAY, exit_box=True)
    unreadable = _accession(iid, _EXITED_CIK, _TIP_DAY.replace(day=3))
    conn.execute(
        """
        INSERT INTO insider_filings (accession_number, instrument_id, document_type,
                                     period_of_report, not_subject_to_section_16, issuer_cik)
        VALUES (%s, %s, '4', %s, NULL, %s)
        ON CONFLICT (accession_number) DO NOTHING
        """,
        (unreadable, iid, _TIP_DAY, _ISSUER_CIK),
    )
    conn.execute(
        "INSERT INTO insider_filers (accession_number, filer_cik, filer_name) VALUES (%s, %s, %s)"
        " ON CONFLICT DO NOTHING",
        (unreadable, _EXITED_CIK, "HOLDER"),
    )
    _seed_insider_current(conn, iid=iid, holder_cik=_EXITED_CIK, shares="300", on=_TIP_DAY, accession=flagged)
    _seed_insider_current(
        conn, iid=iid, holder_cik=_EXITED_CIK, shares="100", on=_TIP_DAY, accession=unreadable, nature="indirect"
    )

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="MIXNUL", instrument_id=iid)

    assert _EXITED_CIK in _insider_ciks(rollup)
    assert _exit_corrections(rollup) == []


@pytest.mark.integration
def test_null_holder_cik_is_never_released(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """An unidentified holder cannot be guarded by the later-filing arm, so it is never
    released — even though its tip row points at a filing carrying the box.

    Written because the natural predicate has this exactly backwards: a NULL
    ``holder_cik`` makes the later-filing ``NOT EXISTS`` succeed trivially, so without an
    explicit refusal the LEAST identifiable rows would be the EASIEST to remove. 0 such
    rows exist today, which is why only a test holds the line.

    ⚠ Honest about what this case does and does not prove. TWO arms currently refuse it:
    the explicit ``oc.holder_cik IS NOT NULL``, and the positive arm's join to
    ``insider_filers`` on that same CIK, which cannot match a NULL. A revert probe that
    deleted the explicit arm left this test GREEN for that reason — it is caught by the
    structural test instead. The explicit arm stays because it is the one that survives if
    the positive arm's join is ever relaxed, and because a reader should not have to derive
    the refusal from a join condition three lines further down.
    """
    conn = ebull_test_conn
    iid = 2_226_005
    _seed_instrument(conn, iid=iid, symbol="NOCIK")
    _seed_outstanding(conn, iid=iid, shares="1000")
    acc = _seed_filing(conn, iid=iid, holder_cik=_EXITED_CIK, on=_TIP_DAY, exit_box=True)
    _seed_insider_current(
        conn, iid=iid, holder_cik=None, shares="400", on=_TIP_DAY, accession=acc, identity_key="NAME:ANON"
    )

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="NOCIK", instrument_id=iid)

    assert None in _insider_ciks(rollup)
    assert _exit_corrections(rollup) == []


@pytest.mark.integration
def test_older_sibling_row_is_released_with_its_holder(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The reason the release is holder-level and not row-level.

    A row-level rule removes only the flagged row and leaves the holder's other
    ``_current`` rows standing — measured at 992 of 4,135 candidates, 233 of them
    ``form3``, i.e. the INITIAL statement and therefore an even OLDER balance than the row
    removed. The declaration is about the person, so both rows go.
    """
    conn = ebull_test_conn
    iid = 2_226_006
    _seed_instrument(conn, iid=iid, symbol="SIBLNG")
    _seed_outstanding(conn, iid=iid, shares="1000")
    tip = _seed_filing(conn, iid=iid, holder_cik=_EXITED_CIK, on=_TIP_DAY, exit_box=True)
    old = _seed_filing(conn, iid=iid, holder_cik=_EXITED_CIK, on=_EARLIER, exit_box=False)
    _seed_insider_current(conn, iid=iid, holder_cik=_EXITED_CIK, shares="300", on=_TIP_DAY, accession=tip)
    _seed_insider_current(
        conn,
        iid=iid,
        holder_cik=_EXITED_CIK,
        shares="500",
        on=_EARLIER,
        accession=old,
        source="form3",
        nature="beneficial",
    )

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="SIBLNG", instrument_id=iid)

    assert _EXITED_CIK not in _insider_ciks(rollup)
    removed = {c.shares_removed for c in _exit_corrections(rollup)}
    assert removed == {Decimal(300), Decimal(500)}
    # Each correction reports the channel of the row it REMOVED, not the channel the
    # evidence came from. Reporting the Form 3 row as form4 would make it
    # indistinguishable from a removed Form 4 row in the telemetry, while
    # ``winning_source`` is what records that the exit box itself is a Form 4/5 thing.
    by_shares = {c.shares_removed: c for c in _exit_corrections(rollup)}
    assert by_shares[Decimal(300)].source_channel == "form4"
    assert by_shares[Decimal(500)].source_channel == "form3"
    assert {c.winning_source for c in _exit_corrections(rollup)} == {"form4"}


@pytest.mark.integration
def test_same_period_filing_outside_the_tip_blocks_the_release(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A live filing at the SAME period as the tip, which produced no observation, blocks
    the release.

    ``period_of_report`` is the covered transaction date, not filing order — so an
    amendment filed later that clears the exit box can carry the same period as the flagged
    filing and would slip past a strict ``>``. 93 released holders have a filing in this
    position. The guard is ``>=`` minus the tip's OWN accessions, because a bare ``>=``
    would match the flagged filing itself and block every release.
    """
    conn = ebull_test_conn
    iid = 2_226_010
    _seed_instrument(conn, iid=iid, symbol="SAMEPD")
    _seed_outstanding(conn, iid=iid, shares="1000")
    tip = _seed_filing(conn, iid=iid, holder_cik=_EXITED_CIK, on=_TIP_DAY, exit_box=True)
    # Same period, different accession, no ``_current`` row of its own.
    other = _accession(iid, _EXITED_CIK, _TIP_DAY.replace(day=4))
    conn.execute(
        """
        INSERT INTO insider_filings (accession_number, instrument_id, document_type,
                                     period_of_report, not_subject_to_section_16, issuer_cik)
        VALUES (%s, %s, '4/A', %s, FALSE, %s)
        ON CONFLICT (accession_number) DO NOTHING
        """,
        (other, iid, _TIP_DAY, _ISSUER_CIK),
    )
    conn.execute(
        "INSERT INTO insider_filers (accession_number, filer_cik, filer_name) VALUES (%s, %s, %s)"
        " ON CONFLICT DO NOTHING",
        (other, _EXITED_CIK, "HOLDER"),
    )
    _seed_insider_current(conn, iid=iid, holder_cik=_EXITED_CIK, shares="400", on=_TIP_DAY, accession=tip)

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="SAMEPD", instrument_id=iid)

    assert _EXITED_CIK in _insider_ciks(rollup)
    assert _exit_corrections(rollup) == []


@pytest.mark.integration
def test_later_filing_on_a_share_class_sibling_blocks_the_release(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A later filing stored under a SIBLING instrument of the same issuer blocks the
    release.

    Observations fan out to every share-class sibling (#1117 PR-B) while the entity-level
    ``insider_filings`` row keeps ONE ``instrument_id``. An instrument-keyed guard therefore
    cannot see a later filing recorded against the other class, and would release a holder
    whose exit has already been superseded. The guard reaches it through ``issuer_cik``.

    ⚠ Deliberately NOT relaxed to filer-only: ``insider_filers.filer_cik`` is global, so a
    later filing about an unrelated issuer would then block a release it says nothing about
    — 1,320 released holders have one of those, against 1 genuine sibling case.
    """
    conn = ebull_test_conn
    iid = 2_226_011
    sibling_iid = 2_226_012
    _seed_instrument(conn, iid=iid, symbol="CLASSA")
    _seed_instrument(conn, iid=sibling_iid, symbol="CLASSB")
    _seed_outstanding(conn, iid=iid, shares="1000")
    tip = _seed_filing(conn, iid=iid, holder_cik=_EXITED_CIK, on=_TIP_DAY, exit_box=True)
    # Later filing recorded against the SIBLING instrument, same issuer.
    _seed_filing(conn, iid=sibling_iid, holder_cik=_EXITED_CIK, on=_LATER, exit_box=False)
    _seed_insider_current(conn, iid=iid, holder_cik=_EXITED_CIK, shares="400", on=_TIP_DAY, accession=tip)

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="CLASSA", instrument_id=iid)

    assert _EXITED_CIK in _insider_ciks(rollup)
    assert _exit_corrections(rollup) == []


@pytest.mark.integration
def test_later_filing_about_a_different_issuer_does_not_block(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The other side of the sibling guard: a later filing by the same person about an
    UNRELATED issuer must not block.

    Written because the cheap fix for the sibling case — drop the instrument condition and
    key on ``filer_cik`` alone — passes the sibling test above and is wrong. A director of
    two companies files for both; the filings say nothing about each other. 1,320 released
    holders would have been wrongly retained by that version, against the 1 the sibling
    case fixes.
    """
    conn = ebull_test_conn
    iid = 2_226_013
    other_iid = 2_226_014
    _seed_instrument(conn, iid=iid, symbol="ISSUR1")
    _seed_instrument(conn, iid=other_iid, symbol="ISSUR2")
    _seed_outstanding(conn, iid=iid, shares="1000")
    tip = _seed_filing(conn, iid=iid, holder_cik=_EXITED_CIK, on=_TIP_DAY, exit_box=True)
    _seed_filing(conn, iid=other_iid, holder_cik=_EXITED_CIK, on=_LATER, exit_box=False, issuer_cik="0008888888")
    _seed_insider_current(conn, iid=iid, holder_cik=_EXITED_CIK, shares="400", on=_TIP_DAY, accession=tip)

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="ISSUR1", instrument_id=iid)

    assert _EXITED_CIK not in _insider_ciks(rollup)
    assert len(_exit_corrections(rollup)) == 1


@pytest.mark.integration
def test_later_filing_beyond_the_tip_blocks_the_release(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A filing after the tip that produced no observation still blocks the release.

    36 live holders have this shape. The tip is read off ``ownership_insiders_current``,
    which is the projection of both ingest pipelines — so a later filing with no row means
    a parse gap, and a parse gap is not evidence that the declared exit is still the last
    word. Fail closed.
    """
    conn = ebull_test_conn
    iid = 2_226_007
    _seed_instrument(conn, iid=iid, symbol="LATERF")
    _seed_outstanding(conn, iid=iid, shares="1000")
    tip = _seed_filing(conn, iid=iid, holder_cik=_EXITED_CIK, on=_TIP_DAY, exit_box=True)
    _seed_filing(conn, iid=iid, holder_cik=_EXITED_CIK, on=_LATER, exit_box=False)  # no _current row
    _seed_insider_current(conn, iid=iid, holder_cik=_EXITED_CIK, shares="400", on=_TIP_DAY, accession=tip)

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="LATERF", instrument_id=iid)

    assert _EXITED_CIK in _insider_ciks(rollup)
    assert _exit_corrections(rollup) == []


@pytest.mark.integration
def test_deregistered_issuer_blocks_the_release(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A Form 25 issuer is refused, because there the exit box says nothing about the
    person.

    Section 16 attaches to a class registered under Section 12. When the issuer's
    registration terminates, EVERY reporting person becomes "no longer subject to
    Section 16" and must tick the box — while remaining a director or officer. The box is
    correct and the release would be wrong, so the carve-out is on the register rather
    than on a date window (a window would be a fitted constant with no source rule).
    """
    conn = ebull_test_conn
    iid = 2_226_008
    _seed_instrument(conn, iid=iid, symbol="DELIST")
    _seed_outstanding(conn, iid=iid, shares="1000")
    _seed_form25(conn, issuer_cik=_ISSUER_CIK)
    acc = _seed_filing(conn, iid=iid, holder_cik=_EXITED_CIK, on=_TIP_DAY, exit_box=True)
    _seed_insider_current(conn, iid=iid, holder_cik=_EXITED_CIK, shares="400", on=_TIP_DAY, accession=acc)

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="DELIST", instrument_id=iid)

    assert _EXITED_CIK in _insider_ciks(rollup)
    assert _exit_corrections(rollup) == []
