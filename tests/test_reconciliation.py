"""Tests for the reconciliation spot-check framework.

Pins the contract:

  * Registry: ``register_check`` is idempotent / overwrite-on-name.
  * Run lifecycle: ``run_spot_check`` opens + closes a row in
    ``data_reconciliation_runs`` and persists findings into
    ``data_reconciliation_findings``.
  * Failure isolation: a single check raising must not abort the
    sweep.
  * Severity classification: ``shares_outstanding_freshness``
    classifies drift at the documented thresholds (clean / info /
    warning / critical).
  * Operator surface: ``iter_recent_findings`` filters by severity.

The shares-outstanding check fetches from SEC; tests inject a fake
fetch via ``monkeypatch`` so they run without network.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest

from app.services import reconciliation
from app.services.reconciliation import (
    Finding,
    InstrumentSubject,
    check_shares_outstanding_freshness,
    iter_recent_findings,
    register_check,
    registered_checks,
    run_spot_check,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    symbol: str,
    cik: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )
    if cik is not None:
        conn.execute(
            """
            INSERT INTO external_identifiers (
                instrument_id, provider, identifier_type, identifier_value, is_primary
            ) VALUES (%s, 'sec', 'cik', %s, TRUE)
            ON CONFLICT (provider, identifier_type, identifier_value, instrument_id)
                WHERE provider = 'sec' AND identifier_type = 'cik'
            DO NOTHING
            """,
            (iid, cik),
        )


def _seed_share_count(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    shares: int,
    period_end: str = "2026-03-31",
    accession: str = "0000000000-26-000001",
) -> None:
    """Seed an ``EntityCommonStockSharesOutstanding`` fact so that
    ``instrument_share_count_latest`` (the view) returns ``shares``
    for the instrument."""
    conn.execute(
        """
        INSERT INTO financial_facts_raw (
            instrument_id, taxonomy, concept, unit, period_end,
            val, accession_number, form_type, filed_date
        ) VALUES (%s, 'dei', 'EntityCommonStockSharesOutstanding',
                  'shares', %s, %s, %s, '10-K', %s)
        """,
        (iid, period_end, shares, accession, period_end),
    )


@pytest.fixture
def isolated_registry() -> Iterator[None]:
    """Snapshot + restore the global check registry around each test
    so a test that registers a custom check doesn't leak into the
    sweep run by the next test."""
    saved = registered_checks()
    try:
        yield
    finally:
        reconciliation._REGISTRY.clear()
        for name, fn in saved.items():
            register_check(name, fn)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_register_check_overwrites_on_same_name(isolated_registry: None) -> None:
    def first(_conn: object, _subj: object) -> tuple[Finding, ...]:
        return ()

    def second(_conn: object, _subj: object) -> tuple[Finding, ...]:
        return ()

    register_check("dup_name", first)  # type: ignore[arg-type]
    register_check("dup_name", second)  # type: ignore[arg-type]

    assert registered_checks()["dup_name"] is second


# ---------------------------------------------------------------------------
# Shares-outstanding check
# ---------------------------------------------------------------------------


def test_check_no_cik_emits_info_finding(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    findings = check_shares_outstanding_freshness(
        ebull_test_conn,
        InstrumentSubject(instrument_id=1, symbol="X", cik=None),
    )
    assert len(findings) == 1
    assert findings[0].severity == "info"
    assert "No SEC CIK" in findings[0].summary


def test_check_no_stored_value_emits_warning(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Instrument has CIK + SEC has DEI value but our store is empty
    → warn that the SEC fundamentals ingester didn't reach this
    instrument. Note: SEC must have a DEI value for the warning to
    fire — both sides empty is a clean no-data case, not drift."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=910_001, symbol="NOFACTS", cik="0000111222")
    conn.commit()

    monkeypatch.setattr(
        reconciliation,
        "_fetch_latest_dei_shares_outstanding",
        lambda _conn, _cik: 50_000_000,  # SEC has a value; we don't
    )

    findings = check_shares_outstanding_freshness(
        conn,
        InstrumentSubject(instrument_id=910_001, symbol="NOFACTS", cik="0000111222"),
    )
    assert len(findings) == 1
    assert findings[0].severity == "warning"
    assert "No stored shares_outstanding" in findings[0].summary


def test_check_clean_when_both_sides_empty(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Foreign issuer / fund / recently-registered: CIK exists, SEC
    publishes no DEI EntityCommonStockSharesOutstanding. Nothing to
    reconcile on either side — must NOT emit a false warning."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=910_009, symbol="FOREIGN", cik="0000111230")
    conn.commit()

    monkeypatch.setattr(
        reconciliation,
        "_fetch_latest_dei_shares_outstanding",
        lambda _conn, _cik: None,
    )

    findings = check_shares_outstanding_freshness(
        conn,
        InstrumentSubject(instrument_id=910_009, symbol="FOREIGN", cik="0000111230"),
    )
    assert findings == ()


def test_check_clean_when_stored_matches_sec(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, iid=910_002, symbol="MATCH", cik="0000111223")
    _seed_share_count(conn, iid=910_002, shares=100_000_000)
    conn.commit()

    monkeypatch.setattr(
        reconciliation,
        "_fetch_latest_dei_shares_outstanding",
        lambda _conn, _cik: 100_000_000,
    )

    findings = check_shares_outstanding_freshness(
        conn,
        InstrumentSubject(instrument_id=910_002, symbol="MATCH", cik="0000111223"),
    )
    assert findings == ()


def test_check_below_threshold_drift_treated_as_clean(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """< 0.1% drift is rounding / share-class slicing — clean."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=910_003, symbol="ROUND", cik="0000111224")
    _seed_share_count(conn, iid=910_003, shares=100_000_000)
    conn.commit()

    # 0.05% drift
    monkeypatch.setattr(
        reconciliation,
        "_fetch_latest_dei_shares_outstanding",
        lambda _conn, _cik: 100_050_000,
    )

    findings = check_shares_outstanding_freshness(
        conn,
        InstrumentSubject(instrument_id=910_003, symbol="ROUND", cik="0000111224"),
    )
    assert findings == ()


def test_check_classifies_warning_drift(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, iid=910_004, symbol="WARN", cik="0000111225")
    _seed_share_count(conn, iid=910_004, shares=100_000_000)
    conn.commit()

    # 1% drift
    monkeypatch.setattr(
        reconciliation,
        "_fetch_latest_dei_shares_outstanding",
        lambda _conn, _cik: 101_000_000,
    )

    findings = check_shares_outstanding_freshness(
        conn,
        InstrumentSubject(instrument_id=910_004, symbol="WARN", cik="0000111225"),
    )
    assert len(findings) == 1
    assert findings[0].severity == "warning"
    assert findings[0].expected == "101000000"
    assert findings[0].observed == "100000000"


def test_check_classifies_critical_drift(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, iid=910_005, symbol="CRIT", cik="0000111226")
    _seed_share_count(conn, iid=910_005, shares=100_000_000)
    conn.commit()

    # 10% drift
    monkeypatch.setattr(
        reconciliation,
        "_fetch_latest_dei_shares_outstanding",
        lambda _conn, _cik: 110_000_000,
    )

    findings = check_shares_outstanding_freshness(
        conn,
        InstrumentSubject(instrument_id=910_005, symbol="CRIT", cik="0000111226"),
    )
    assert len(findings) == 1
    assert findings[0].severity == "critical"


def test_check_fetch_failure_emits_info_finding(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Transient SEC outage shouldn't cascade into critical alerts —
    the framework downgrades to info so operator can re-run."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=910_006, symbol="FETCHFAIL", cik="0000111227")
    _seed_share_count(conn, iid=910_006, shares=100_000_000)
    conn.commit()

    def _boom(_conn: object, _cik: str) -> int | None:
        raise RuntimeError("SEC unreachable")

    monkeypatch.setattr(reconciliation, "_fetch_latest_dei_shares_outstanding", _boom)

    findings = check_shares_outstanding_freshness(
        conn,
        InstrumentSubject(instrument_id=910_006, symbol="FETCHFAIL", cik="0000111227"),
    )
    assert len(findings) == 1
    assert findings[0].severity == "info"
    assert "SEC fetch failed" in findings[0].summary


def test_check_clean_when_sec_has_no_dei_value(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SEC concept absent → not a drift signal, just no upstream
    data to compare against. Clean."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=910_007, symbol="NOSEC", cik="0000111228")
    _seed_share_count(conn, iid=910_007, shares=100_000_000)
    conn.commit()

    monkeypatch.setattr(
        reconciliation,
        "_fetch_latest_dei_shares_outstanding",
        lambda _conn, _cik: None,
    )

    findings = check_shares_outstanding_freshness(
        conn,
        InstrumentSubject(instrument_id=910_007, symbol="NOSEC", cik="0000111228"),
    )
    assert findings == ()


def test_check_emits_freshness_warning_when_value_clean_but_stale(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """as_of_date older than the staleness threshold is itself a
    finding even when the stored value still matches SEC. Catches
    the failure mode where the SEC fundamentals ingester silently
    stopped reaching this instrument but the share count happens
    to be unchanged."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=910_008, symbol="STALE", cik="0000111229")
    # period_end well past the 180-day staleness threshold.
    _seed_share_count(
        conn,
        iid=910_008,
        shares=100_000_000,
        period_end="2024-01-31",
        accession="0000000000-24-000001",
    )
    conn.commit()

    monkeypatch.setattr(
        reconciliation,
        "_fetch_latest_dei_shares_outstanding",
        lambda _conn, _cik: 100_000_000,  # value matches; only freshness should fire
    )

    findings = check_shares_outstanding_freshness(
        conn,
        InstrumentSubject(instrument_id=910_008, symbol="STALE", cik="0000111229"),
    )
    assert len(findings) == 1
    assert findings[0].severity == "warning"
    assert "stale" in findings[0].summary.lower()


def test_fetch_latest_picks_amended_filing_for_same_period(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SEC's companyfacts payload publishes the original filing
    alongside any 10-K/A amendment under the same ``end`` date.
    Without a ``filed`` tie-break, payload order decides the winner —
    creating spurious drift findings when the amendment restates
    the share count. The tie-break must pick the latest ``filed``."""
    fake_payload: dict[str, Any] = {
        "facts": {
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "units": {
                        "shares": [
                            {"end": "2026-03-31", "filed": "2026-04-15", "val": 100},
                            {"end": "2026-03-31", "filed": "2026-05-20", "val": 200},
                        ]
                    }
                }
            }
        }
    }

    # Stub the payload-fetcher rather than urllib so the test
    # focuses on the parsing tie-break logic. Cache wiring is
    # exercised by test_fetch_uses_cik_raw_cache below.
    monkeypatch.setattr(
        reconciliation,
        "_fetch_companyfacts_payload",
        lambda _conn, _cik: fake_payload,
    )

    val = reconciliation._fetch_latest_dei_shares_outstanding(ebull_test_conn, "0000999999")

    assert val == 200  # amendment wins on filed-desc tie-break


def test_fetch_uses_cik_raw_cache(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First call hits SEC and writes cik_raw_documents; second
    call within the cache TTL serves from the cached row without
    re-fetching."""
    conn = ebull_test_conn

    fetched_payload = (
        '{"facts": {"dei": {"EntityCommonStockSharesOutstanding": '
        '{"units": {"shares": [{"end": "2026-03-31", "filed": "2026-04-15", "val": 12345}]}}}}}'
    )
    fetch_count = 0

    class _Resp:
        def __enter__(self) -> _Resp:
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def read(self) -> bytes:
            nonlocal fetch_count
            fetch_count += 1
            return fetched_payload.encode("utf-8")

    monkeypatch.setattr(
        reconciliation.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Resp(),
    )

    val_first = reconciliation._fetch_latest_dei_shares_outstanding(conn, "0000999998")
    assert val_first == 12345
    assert fetch_count == 1

    # Second call within TTL must NOT re-fetch.
    val_second = reconciliation._fetch_latest_dei_shares_outstanding(conn, "0000999998")
    assert val_second == 12345
    assert fetch_count == 1  # still only one network call

    # Cache row was persisted with the right kind + source URL.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT document_kind, source_url FROM cik_raw_documents WHERE cik = %s",
            ("0000999998",),
        )
        row = cur.fetchone()
    assert row is not None
    kind, src = row
    assert kind == "companyfacts_json"
    assert src is not None and src.endswith("/CIK0000999998.json")


def test_fetch_cache_write_failure_does_not_break_outer_transaction(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed cache write must NOT corrupt the caller's
    transaction state — the savepoint isolates the failure so the
    next statement on the same connection still works."""
    conn = ebull_test_conn
    fetched_payload = (
        '{"facts": {"dei": {"EntityCommonStockSharesOutstanding": '
        '{"units": {"shares": [{"end": "2026-03-31", "filed": "2026-04-15", "val": 999}]}}}}}'
    )

    class _Resp:
        def __enter__(self) -> _Resp:
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def read(self) -> bytes:
            return fetched_payload.encode("utf-8")

    monkeypatch.setattr(
        reconciliation.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Resp(),
    )

    # Force store_cik_raw to raise — simulates DB hiccup mid cache
    # write. The SEC value should still be returned (cache write is
    # best-effort) and the connection must be usable for subsequent
    # reads.
    def _boom(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("simulated DB hiccup")

    monkeypatch.setattr(reconciliation, "store_cik_raw", _boom)

    val = reconciliation._fetch_latest_dei_shares_outstanding(conn, "0000999997")
    assert val == 999

    # Caller's connection still usable — cache writes go through a
    # SEPARATE short-lived connection, so a failure there can't
    # taint the caller's transaction state at all.
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        row = cur.fetchone()
    assert row == (1,)


def test_run_spot_check_findings_count_matches_persisted_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """A check that emits a valid finding followed by an invalid one
    must NOT roll back the valid finding. ``findings_emitted`` on the
    run row must match the actual persisted row count — no orphaned
    accounting where the run says N findings but the table has M < N."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=920_030, symbol="MIXED", cik=None)
    conn.commit()

    reconciliation._REGISTRY.clear()

    def emit_one_good_one_bad(
        _conn: psycopg.Connection[tuple],
        _subj: InstrumentSubject,
    ) -> tuple[Finding, ...]:
        return (
            Finding(check_name="mix", severity="info", summary="good"),
            Finding(
                check_name="mix",
                severity="not_a_severity",  # type: ignore[arg-type]
                summary="bad",
            ),
        )

    register_check("emit_one_good_one_bad", emit_one_good_one_bad)  # type: ignore[arg-type]

    with pytest.raises(psycopg.errors.CheckViolation):
        run_spot_check(conn, sample_size=1, sample_seed=137)

    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT findings_emitted, status FROM data_reconciliation_runs WHERE sample_seed = %s",
            (137,),
        )
        run_row = cur.fetchone()
        cur.execute(
            "SELECT count(*) FROM data_reconciliation_findings WHERE run_id = ("
            "SELECT run_id FROM data_reconciliation_runs WHERE sample_seed = %s)",
            (137,),
        )
        persisted = cur.fetchone()
    assert run_row is not None
    assert persisted is not None
    findings_emitted, status = run_row
    persisted_count = persisted[0]
    assert status == "failed"
    # The valid "good" finding must survive; the invalid one must not.
    # findings_emitted on the run row must equal what's actually persisted.
    assert findings_emitted == persisted_count == 1


def test_run_spot_check_uses_consistent_check_set_across_subjects(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """A concurrent ``register_check`` call mid-run must not change
    the set of checks executed for later subjects. The registry is
    snapshotted once per run."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=920_040, symbol="A", cik=None)
    _seed_instrument(conn, iid=920_041, symbol="B", cik=None)
    conn.commit()

    reconciliation._REGISTRY.clear()

    call_log: list[int] = []

    def first_check(
        _conn: psycopg.Connection[tuple],
        subj: InstrumentSubject,
    ) -> tuple[Finding, ...]:
        call_log.append(subj.instrument_id)
        # Mid-run mutation of the registry — should NOT affect the
        # check set executed against the next subject.
        register_check("late_arrival", first_check)  # type: ignore[arg-type]
        return ()

    register_check("first_check", first_check)  # type: ignore[arg-type]

    summary = run_spot_check(conn, sample_size=10, sample_seed=88)

    assert summary.instruments_checked == 2
    # Each subject runs only ``first_check`` (registered at start),
    # not the late-arrival registered mid-loop. So the call log has
    # exactly 2 entries (1 per subject), not 4 or 3.
    assert len(call_log) == 2


def test_run_spot_check_finalises_when_persist_fails(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """A check that emits an invalid finding (e.g., severity not
    matching the CHECK constraint) puts the connection into
    InFailedSqlTransaction. The framework must roll back before
    finalising so the run row doesn't get stuck in 'running'
    forever."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=920_020, symbol="BADFINDING", cik=None)
    conn.commit()

    reconciliation._REGISTRY.clear()

    def emit_invalid(
        _conn: psycopg.Connection[tuple],
        _subj: InstrumentSubject,
    ) -> tuple[Finding, ...]:
        # Bypass dataclass validation by constructing a Finding with
        # a severity outside the CHECK constraint enum. The runtime
        # type is ``str``; the constraint fires on INSERT.
        return (
            Finding(
                check_name="bad",
                severity="not_a_severity",  # type: ignore[arg-type]
                summary="intentional bad row",
            ),
        )

    register_check("emit_invalid", emit_invalid)  # type: ignore[arg-type]

    with pytest.raises(psycopg.errors.CheckViolation):
        run_spot_check(conn, sample_size=1, sample_seed=99)

    # Reset the connection state so the post-test check can read
    # the run row that was finalised on its own clean transaction.
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, error FROM data_reconciliation_runs WHERE sample_seed = %s",
            (99,),
        )
        row = cur.fetchone()
    assert row is not None
    status, err = row
    assert status == "failed"
    assert err is not None and "CheckViolation" in err


# ---------------------------------------------------------------------------
# Run orchestration
# ---------------------------------------------------------------------------


def test_run_spot_check_finalises_run_with_zero_subjects(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """Empty instruments table → sweep still produces a finalised run
    row with status=success and zero counters."""
    conn = ebull_test_conn
    summary = run_spot_check(conn, sample_size=10, sample_seed=1, triggered_by="operator")

    assert summary.instruments_checked == 0
    assert summary.findings_emitted == 0

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, instruments_checked, findings_emitted, sample_seed,"
            " triggered_by FROM data_reconciliation_runs WHERE run_id = %s",
            (summary.run_id,),
        )
        row = cur.fetchone()
    assert row is not None
    status, checked, emitted, seed, triggered = row
    assert status == "success"
    assert checked == 0
    assert emitted == 0
    assert seed == 1
    assert triggered == "operator"


def test_run_spot_check_persists_findings(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """End-to-end happy path: register a deterministic check, run the
    sweep, observe one finding row per instrument."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=920_001, symbol="A", cik=None)
    _seed_instrument(conn, iid=920_002, symbol="B", cik=None)
    conn.commit()

    reconciliation._REGISTRY.clear()

    def always_warn(
        _conn: psycopg.Connection[tuple],
        subj: InstrumentSubject,
    ) -> tuple[Finding, ...]:
        return (
            Finding(
                check_name="always_warn",
                severity="warning",
                summary=f"synthetic warning for {subj.symbol}",
            ),
        )

    register_check("always_warn", always_warn)  # type: ignore[arg-type]

    summary = run_spot_check(conn, sample_size=10, sample_seed=42)

    assert summary.instruments_checked == 2
    assert summary.findings_emitted == 2

    with conn.cursor() as cur:
        cur.execute(
            "SELECT instrument_id, severity, summary FROM data_reconciliation_findings "
            "WHERE run_id = %s ORDER BY instrument_id",
            (summary.run_id,),
        )
        rows = cur.fetchall()
    assert len(rows) == 2
    assert {r[0] for r in rows} == {920_001, 920_002}
    assert all(r[1] == "warning" for r in rows)
    assert all("synthetic warning for" in r[2] for r in rows)


def test_run_spot_check_isolates_per_check_failure(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """A check that raises must not abort the sweep — the framework
    downgrades the crash to an info finding so the operator sees it
    in triage and other checks still run."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=920_010, symbol="X", cik=None)
    conn.commit()

    reconciliation._REGISTRY.clear()

    def crashy(_conn: object, _subj: object) -> tuple[Finding, ...]:
        raise ValueError("boom")

    register_check("crashy", crashy)  # type: ignore[arg-type]

    summary = run_spot_check(conn, sample_size=10, sample_seed=7)

    assert summary.instruments_checked == 1
    assert summary.findings_emitted == 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT severity, summary FROM data_reconciliation_findings WHERE run_id = %s",
            (summary.run_id,),
        )
        row = cur.fetchone()
    assert row is not None
    severity, summary_text = row
    assert severity == "info"
    assert "Check raised" in summary_text
    assert "ValueError" in summary_text


def test_run_spot_check_seed_is_reproducible(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """Same ``sample_seed`` must select the same instrument cohort —
    operator triage workflow ("is this finding still there?")
    depends on it."""
    conn = ebull_test_conn
    for i in range(20):
        _seed_instrument(conn, iid=930_000 + i, symbol=f"SEED{i}", cik=None)
    conn.commit()

    reconciliation._REGISTRY.clear()

    captured: list[set[int]] = []

    def capture(
        _conn: psycopg.Connection[tuple],
        subj: InstrumentSubject,
    ) -> tuple[Finding, ...]:
        captured.append({subj.instrument_id})
        return ()

    register_check("capture", capture)  # type: ignore[arg-type]

    run_spot_check(conn, sample_size=5, sample_seed=12345)
    first = {iid for s in captured for iid in s}
    captured.clear()
    run_spot_check(conn, sample_size=5, sample_seed=12345)
    second = {iid for s in captured for iid in s}

    assert first == second
    assert len(first) == 5


# ---------------------------------------------------------------------------
# Operator surface
# ---------------------------------------------------------------------------


def test_iter_recent_findings_severity_filter(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """``severity_min='warning'`` excludes info; ``'critical'``
    excludes both info and warning."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=940_001, symbol="A", cik=None)
    conn.commit()

    reconciliation._REGISTRY.clear()

    def mixed(
        _conn: psycopg.Connection[tuple],
        _subj: InstrumentSubject,
    ) -> tuple[Finding, ...]:
        return (
            Finding(check_name="mixed", severity="info", summary="i"),
            Finding(check_name="mixed", severity="warning", summary="w"),
            Finding(check_name="mixed", severity="critical", summary="c"),
        )

    register_check("mixed", mixed)  # type: ignore[arg-type]
    run_spot_check(conn, sample_size=1, sample_seed=1)

    all_findings = list(iter_recent_findings(conn, limit=50))
    warn_plus = list(iter_recent_findings(conn, limit=50, severity_min="warning"))
    crit_only = list(iter_recent_findings(conn, limit=50, severity_min="critical"))

    assert len(all_findings) == 3
    assert {r["severity"] for r in warn_plus} == {"warning", "critical"}
    assert {r["severity"] for r in crit_only} == {"critical"}
