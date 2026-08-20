"""DB-backed tests for the Form 4 retention bound on the insiders wedge (#2788).

Sibling of ``test_ownership_13f_hr_supersession.py`` (#2229), and the contrast between
them is the point. #2229 removes a stale 13F row because Form 13F Special Instruction 5b
makes a holdings report a COMPLETE statement — omission is affirmative evidence of an
exit. **Section 16 has no such rule.** Form 4 is transaction-triggered, so a holder who
simply stopped filing may still hold every share, and silence proves nothing either way.

So this bound is NOT an exit claim. ``form4_within_retention`` (#1233 §4.3) is the
boundary every Form 4 WRITER already enforces; the rollup was rendering rows that,
under that posture, should never have been stored — 4.5M of them arrived on 2026-08-14
when the #2701 research ingest ran ``ingest_insider_dataset_archive`` with
``retention_cutoff_override`` against the shared observations table. The read path now
honours the same boundary and says why in ``corrections_applied``.

The predicate lives in SQL (``_INSIDER_BEYOND_RETENTION_SQL``, interpolated into both
``_collect_canonical_holders_from_current`` and ``_read_beyond_retention_insiders``), so
the behavioural cases run against a real Postgres. The one structural case below is pure:
it pins that BOTH call sites interpolate the shared fragment, which is what stops the
exclusion and its telemetry drifting apart.

Instrument-id range 2_788_xxx is reserved for these scenarios.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest

from app.services import ownership_rollup
from app.services.insider_transactions import form4_retention_cutoff
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

_CUTOFF = form4_retention_cutoff()
_BEYOND = _CUTOFF - timedelta(days=1)
_INSIDE = _CUTOFF + timedelta(days=1)

_STALE_CIK = "0001731303"
_FRESH_CIK = "0000320193"


# ---------------------------------------------------------------------------
# Structural — pure, no DB
# ---------------------------------------------------------------------------


def test_exclusion_and_telemetry_share_one_predicate() -> None:
    """The rollup's exclusion and its telemetry producer must select exact
    complements. #2229 kept its pair in step with a comment; a comment does not fail
    when someone edits one side, so the fragment is shared and this pins that.

    Asserting on the SOURCE of both functions rather than on a rendered result,
    because the failure this guards is a future edit that inlines the predicate into
    one call site — which no behavioural test on today's code would catch.
    """
    import ast
    import inspect
    import textwrap

    def _body_without_docstring(fn: object) -> str:
        """Source of ``fn`` with its docstring removed.

        ⚠ Both docstrings NAME the fragment, so a plain ``in inspect.getsource(...)``
        check passes on the prose alone — a revert probe that inlined the predicate in
        the producer came back NOT CAUGHT against exactly that weaker assertion. The
        docstring is dropped via AST rather than ``src.replace(fn.__doc__, "")``,
        which does not match reliably.
        """
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))  # type: ignore[arg-type]
        node = tree.body[0]
        assert isinstance(node, ast.FunctionDef)
        body = node.body
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            body = body[1:]
        return "\n".join(ast.unparse(stmt) for stmt in body)

    fragment = ownership_rollup._INSIDER_BEYOND_RETENTION_SQL
    assert "oc.source = 'form4'" in fragment
    assert "form4_cutoff" in fragment
    # Form 3 must NOT appear: #1233 §4.3 exempts it, and gating it would drop the only
    # evidence we hold for a holder who has never transacted.
    assert "form3" not in fragment

    for fn in (
        ownership_rollup._collect_canonical_holders_from_current,
        ownership_rollup._read_beyond_retention_insiders,
    ):
        src = _body_without_docstring(fn)
        for name in ("_INSIDER_BEYOND_RETENTION_SQL", "_INSIDER_DUAL_PIPELINE_DECOLLISION_SQL"):
            assert name in src, f"{fn.__name__} does not use the shared {name}"


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


def _seed_insider_current(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    holder_cik: str,
    holder_name: str,
    shares: str,
    filed_on: date,
    source: str = "form4",
    nature: str = "direct",
) -> None:
    """One ``ownership_insiders_current`` row.

    ``period_end`` tracks ``filed_on`` so the fixture cannot accidentally pass the
    bound on one column while failing it on the other — the predicate reads
    ``filed_at``, and a fixture whose two dates disagree would hide which one it
    tests.
    """
    conn.execute(
        """
        INSERT INTO ownership_insiders_current (
            instrument_id, holder_cik, holder_name, holder_identity_key,
            ownership_nature, source, source_document_id, source_accession,
            filed_at, period_end, shares
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument_id, holder_identity_key, ownership_nature)
        DO UPDATE SET shares = EXCLUDED.shares, filed_at = EXCLUDED.filed_at
        """,
        (
            iid,
            holder_cik,
            holder_name,
            f"CIK:{holder_cik}",
            nature,
            source,
            f"{holder_cik}-{filed_on:%Y%m%d}-{source}",
            # sql/134 CHECKs ^[0-9]{10}-[0-9]{2}-[0-9]{6}$ — the sequence is SIX digits.
            f"{holder_cik}-{filed_on:%y}-{iid % 1_000_000:06d}",
            datetime(filed_on.year, filed_on.month, filed_on.day, tzinfo=UTC),
            filed_on,
            Decimal(shares),
        ),
    )


def _insider_ciks(rollup: ownership_rollup.OwnershipRollup) -> set[str | None]:
    out: set[str | None] = set()
    for slc in rollup.slices:
        if slc.category == "insiders":
            out.update(h.filer_cik for h in slc.holders)
    return out


# ---------------------------------------------------------------------------
# Behavioural
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_beyond_retention_form4_leaves_wedge_and_emits_correction(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A Form 4 filed one day before the cutoff must leave the insiders wedge, and the
    removal must be explained — an operator watching the wedge shrink needs the reason
    (#1639's contract), and the reason here is coverage, not a sale."""
    conn = ebull_test_conn
    iid = 2_788_001
    _seed_instrument(conn, iid=iid, symbol="RETOLD")
    _seed_outstanding(conn, iid=iid, shares="1000")
    _seed_insider_current(
        conn, iid=iid, holder_cik=_STALE_CIK, holder_name="FORGE ENERGY LLC", shares="400", filed_on=_BEYOND
    )

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="RETOLD", instrument_id=iid)

    assert _STALE_CIK not in _insider_ciks(rollup)
    corrections = [c for c in rollup.corrections_applied if c.kind == "insider_beyond_form4_retention"]
    assert len(corrections) == 1
    correction = corrections[0]
    assert correction.filer_cik == _STALE_CIK
    assert correction.shares_removed == Decimal(400)
    assert correction.superseded_period == _BEYOND
    assert correction.source_channel == "form4"
    # ⚠ The operator-facing string must not read as an exit claim. Section 16 gives no
    # rule that makes silence evidence of a sale, and a correction that implies one
    # would be quoted back as though it did.
    assert "NOT evidence the holder sold" in correction.detail
    assert str(_CUTOFF) in correction.detail
    # The shares return to the public residual rather than vanishing.
    assert rollup.residual.pct_outstanding == Decimal(1)


@pytest.mark.integration
def test_in_retention_form4_is_untouched(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The boundary is inclusive-on-the-inside: a filing one day AFTER the cutoff stays.
    Pairs with the case above so the test file pins both sides of the boundary rather
    than only the side the fix removes."""
    conn = ebull_test_conn
    iid = 2_788_002
    _seed_instrument(conn, iid=iid, symbol="RETNEW")
    _seed_outstanding(conn, iid=iid, shares="1000")
    _seed_insider_current(
        conn, iid=iid, holder_cik=_FRESH_CIK, holder_name="RECENT OFFICER", shares="250", filed_on=_INSIDE
    )

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="RETNEW", instrument_id=iid)

    assert _FRESH_CIK in _insider_ciks(rollup)
    assert [c for c in rollup.corrections_applied if c.kind == "insider_beyond_form4_retention"] == []


@pytest.mark.integration
def test_filing_exactly_at_the_cutoff_is_retained(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """``form4_within_retention`` documents the boundary as INCLUSIVE — "a filing
    exactly at the cutoff date is retained (no off-by-one drop on the rolling
    boundary)". The read bound must agree, so the predicate is ``< cutoff`` and never
    ``<= cutoff``.

    Without this case a ``<`` → ``<=`` edit passes every other test in the file: the
    others sit a day either side of the boundary and cannot see it move by one day."""
    conn = ebull_test_conn
    iid = 2_788_005
    _seed_instrument(conn, iid=iid, symbol="RETEDGE")
    _seed_outstanding(conn, iid=iid, shares="1000")
    _seed_insider_current(
        conn, iid=iid, holder_cik=_FRESH_CIK, holder_name="BOUNDARY OFFICER", shares="100", filed_on=_CUTOFF
    )

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="RETEDGE", instrument_id=iid)

    assert _FRESH_CIK in _insider_ciks(rollup)
    assert [c for c in rollup.corrections_applied if c.kind == "insider_beyond_form4_retention"] == []


@pytest.mark.integration
def test_form3_is_never_gated_by_the_form4_cutoff(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """#1233 §4.3 exempts Form 3 from the retention cap deliberately — the initial
    statement is the only evidence we hold for a holder who has never transacted, so
    gating it would delete real positions rather than stale ones. A Form 3 filed well
    before the cutoff therefore stays in the wedge.

    This is the case most likely to be 'fixed' by a future reader who sees an old row
    surviving and assumes the bound leaks."""
    conn = ebull_test_conn
    iid = 2_788_003
    _seed_instrument(conn, iid=iid, symbol="RETF3")
    _seed_outstanding(conn, iid=iid, shares="1000")
    _seed_insider_current(
        conn,
        iid=iid,
        holder_cik=_STALE_CIK,
        holder_name="FOUNDER WHO NEVER TRADED",
        shares="300",
        filed_on=_BEYOND,
        source="form3",
    )

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="RETF3", instrument_id=iid)

    assert _STALE_CIK in _insider_ciks(rollup)
    assert [c for c in rollup.corrections_applied if c.kind == "insider_beyond_form4_retention"] == []


@pytest.mark.integration
def test_dual_pipeline_stale_accession_is_reported_once(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A stale accession present in BOTH ingest pipelines contributes ONE row to the
    wedge, so it must contribute ONE correction.

    The same Form 4 accession is written by the XML manifest parser (bare
    ``source_document_id``) and by the DERA bulk dataset (``<accn>:NDT:<sk>``). The
    insiders read drops the dataset copy via the de-collision predicate (#788). A
    telemetry query that omits that predicate selects both and reports **twice the
    shares that actually left the wedge** — Codex checkpoint 2 caught exactly that on
    the first cut of this change, and an operator reconciling the wedge against
    ``shares_removed`` would not have been able to make it balance."""
    conn = ebull_test_conn
    iid = 2_788_006
    _seed_instrument(conn, iid=iid, symbol="RETDUAL")
    _seed_outstanding(conn, iid=iid, shares="1000")
    accession = f"{_STALE_CIK}-{_BEYOND:%y}-000456"
    for doc_id, nature in ((accession, "direct"), (f"{accession}:NDT:1", "indirect")):
        conn.execute(
            """
            INSERT INTO ownership_insiders_current (
                instrument_id, holder_cik, holder_name, holder_identity_key,
                ownership_nature, source, source_document_id, source_accession,
                filed_at, period_end, shares
            ) VALUES (%s, %s, 'DUAL PIPELINE HOLDER', %s, %s, 'form4', %s, %s, %s, %s, 200)
            """,
            (
                iid,
                _STALE_CIK,
                f"CIK:{_STALE_CIK}",
                nature,
                doc_id,
                accession,
                datetime(_BEYOND.year, _BEYOND.month, _BEYOND.day, tzinfo=UTC),
                _BEYOND,
            ),
        )

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="RETDUAL", instrument_id=iid)

    corrections = [c for c in rollup.corrections_applied if c.kind == "insider_beyond_form4_retention"]
    assert len(corrections) == 1, [c.filer_name for c in corrections]
    assert corrections[0].shares_removed == Decimal(200)


@pytest.mark.integration
def test_oversubscribed_wedge_falls_back_under_outstanding(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The #2788 headline shape: one live holder plus a beyond-retention ghost that
    together exceed ``shares_outstanding``. Dropping the ghost brings the wedge back
    under the denominator — which is the operator-visible defect the ticket is about,
    not merely a row count."""
    conn = ebull_test_conn
    iid = 2_788_004
    _seed_instrument(conn, iid=iid, symbol="RETOVER")
    _seed_outstanding(conn, iid=iid, shares="1000")
    _seed_insider_current(
        conn, iid=iid, holder_cik=_FRESH_CIK, holder_name="CURRENT CEO", shares="600", filed_on=_INSIDE
    )
    _seed_insider_current(
        conn, iid=iid, holder_cik=_STALE_CIK, holder_name="LONG GONE SPONSOR", shares="900", filed_on=_BEYOND
    )

    rollup = ownership_rollup.get_ownership_rollup(conn, symbol="RETOVER", instrument_id=iid)

    insiders = next(s for s in rollup.slices if s.category == "insiders")
    assert insiders.total_shares == Decimal(600)
    assert rollup.shares_outstanding is not None
    assert insiders.total_shares <= rollup.shares_outstanding
    assert rollup.residual.oversubscribed is False
