"""The generalised per-accession ownership write lock (#817).

Mirrors the 13F helper (#1542) for the other ownership kinds — same
``hashtextextended`` XOR scheme, its own namespace so the lock key is
distinct from the 13F key space for the same accession string.
"""

from unittest.mock import MagicMock

import psycopg

from app.services.institutional_holdings import acquire_13f_accession_write_lock
from app.services.raw_filings import acquire_filing_accession_write_lock
from tests.fixtures.ebull_test_db import test_database_url

_PROBE_SQL = (
    "SELECT pg_try_advisory_xact_lock((hashtextextended('ingest_filing_accession', 0) # hashtextextended(%s, 0)))"
)


def test_helper_issues_xact_lock_with_accession_keyed_hash():
    conn = MagicMock()
    acquire_filing_accession_write_lock(conn, "0001234567-25-000001")
    assert conn.execute.call_count == 1
    sql, params = conn.execute.call_args.args
    # transaction-scoped advisory lock (auto-released on commit/rollback)
    assert "pg_advisory_xact_lock" in sql
    # own namespace, accession-keyed via hashtextextended XOR — every writer
    # of these kinds must issue the identical key (prevention-log L337-338)
    assert "hashtextextended('ingest_filing_accession', 0)" in sql
    assert "hashtextextended(%s, 0)" in sql
    assert params == ("0001234567-25-000001",)


def test_namespace_is_distinct_from_13f_lock():
    """The new namespace string differs from the 13F one, so the two lock
    keys for the same accession do not (modulo 64-bit birthday risk) alias —
    13F filings keep their own #1542 serialisation domain."""
    conn_a, conn_b = MagicMock(), MagicMock()
    acquire_filing_accession_write_lock(conn_a, "0001234567-25-000001")
    acquire_13f_accession_write_lock(conn_b, "0001234567-25-000001")
    sql_new = conn_a.execute.call_args.args[0]
    sql_13f = conn_b.execute.call_args.args[0]
    assert "ingest_filing_accession" in sql_new
    assert "ingest_13f_accession" in sql_13f
    assert "ingest_filing_accession" not in sql_13f


def test_every_guarded_ownership_writer_acquires_the_lock():
    """Structural census guard (#817): an advisory lock only protects callers
    that take it, so EVERY once-per-accession writer of the guarded ownership
    typed tables must acquire ``acquire_filing_accession_write_lock`` (or, for
    13F, the #1542 helper). Pins prevention-log "Advisory lock scope vs
    concurrent writers" so a sixth kind / a new caller can't silently bypass it.
    """
    import inspect

    from app.services import def14a_ingest, insider_form3_ingest, insider_transactions, rewash_filings
    from app.services.manifest_parsers import def14a as mp_def14a
    from app.services.manifest_parsers import sec_13dg as mp_13dg

    # (function, required-lock-token). All non-13F writers share the new helper;
    # the 13F rewash apply keeps its #1542 lock.
    guarded = [
        # insider form4/5 + form3 chokepoints (cover manifest+rewash+legacy)
        (insider_transactions.upsert_filing, "acquire_filing_accession_write_lock"),
        (insider_form3_ingest.upsert_form_3_filing, "acquire_filing_accession_write_lock"),
        # def14a: rewash, live manifest, legacy
        (rewash_filings._apply_def14a, "acquire_filing_accession_write_lock"),
        (mp_def14a._parse_def14a, "acquire_filing_accession_write_lock"),
        (def14a_ingest._ingest_single_accession, "acquire_filing_accession_write_lock"),
        # blockholders: rewash, live manifest
        (rewash_filings._apply_blockholders, "acquire_filing_accession_write_lock"),
        (mp_13dg._parse_13dg, "acquire_filing_accession_write_lock"),
        # 13F (#1542) — regression guard that its symmetric lock survives
        (rewash_filings._apply_13f_infotable, "acquire_13f_accession_write_lock"),
    ]
    missing = [fn.__qualname__ for fn, token in guarded if token not in inspect.getsource(fn)]
    assert not missing, f"writers missing per-accession write lock: {missing}"


def test_lock_actually_serialises_two_sessions():
    """DB-tier interleave: while one session holds the per-accession lock, a
    second session's ``pg_try_advisory_xact_lock`` on the SAME key fails;
    once the holder commits, the key is acquirable. Proves real serialisation
    (not a defensive SELECT) — prevention-log "process lock != DB isolation".
    """
    accn = "0001234567-25-000777"
    test_url = test_database_url()

    holder = psycopg.connect(test_url)
    holder.autocommit = False
    try:
        acquire_filing_accession_write_lock(holder, accn)  # held until txn ends

        with psycopg.connect(test_url) as other:
            other.autocommit = False
            row = other.execute(_PROBE_SQL, (accn,)).fetchone()
            assert row is not None and row[0] is False, "second session acquired a held per-accession lock"
            other.rollback()

        holder.commit()  # release

        with psycopg.connect(test_url) as other:
            other.autocommit = False
            row = other.execute(_PROBE_SQL, (accn,)).fetchone()
            assert row is not None and row[0] is True, "lock not acquirable after holder committed"
            other.rollback()
    finally:
        holder.close()
