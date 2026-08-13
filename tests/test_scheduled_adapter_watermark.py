import pytest

from app.services.processes.scheduled_adapter import _source_watermark_behind

pytestmark = pytest.mark.db


def test_source_behind_when_ingest_is_erroring(ebull_test_conn):
    conn = ebull_test_conn
    # institutional_filer (not issuer) so the chk_freshness_issuer_has_instrument
    # CHECK is satisfied without an instrument_id — the predicate is source-level.
    conn.execute(
        "INSERT INTO data_freshness_index (subject_type, subject_id, source, state, last_polled_outcome) "
        "VALUES ('institutional_filer','0000320193','sec_form4','error','error')"
    )
    assert _source_watermark_behind(conn, source="sec_form4") is True


def test_source_not_behind_when_all_current(ebull_test_conn):
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO data_freshness_index (subject_type, subject_id, source, state, last_polled_outcome) "
        "VALUES ('institutional_filer','0000320193','sec_form4','current','current')"
    )
    assert _source_watermark_behind(conn, source="sec_form4") is False


def test_quiet_source_with_no_rows_is_not_behind(ebull_test_conn):
    assert _source_watermark_behind(ebull_test_conn, source="sec_form4") is False
