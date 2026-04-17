"""Unit tests for the SEC daily master-index parser."""

from __future__ import annotations

from pathlib import Path

from app.providers.implementations.sec_edgar import MasterIndexEntry, parse_master_index

FIXTURE = Path("tests/fixtures/sec/master_20260415.idx")


def test_parses_all_entries_from_fixture() -> None:
    body = FIXTURE.read_bytes()
    entries = parse_master_index(body)

    assert len(entries) == 4
    assert entries[0] == MasterIndexEntry(
        cik="0000320193",
        company_name="APPLE INC",
        form_type="10-Q",
        date_filed="2026-04-15",
        accession_number="0000320193-26-000042",
    )


def test_zero_pads_short_ciks() -> None:
    """CIKs shorter than 10 digits in the source must be left-padded
    with zeros. Already-padded CIKs are covered separately by the
    parser's reuse of _zero_pad_cik."""
    body = FIXTURE.read_bytes()
    entries = parse_master_index(body)
    ciks = {e.cik for e in entries}
    assert "0000320193" in ciks  # 6-digit input 320193
    assert "0000789019" in ciks  # 6-digit input 789019
    assert "0001045810" in ciks  # 7-digit input 1045810


def test_extracts_accession_number_from_filename() -> None:
    body = FIXTURE.read_bytes()
    entries = parse_master_index(body)
    accessions = {e.accession_number for e in entries}
    assert "0000320193-26-000042" in accessions
    assert "0000789019-26-000017" in accessions
    assert "0001045810-26-000003" in accessions


def test_ignores_header_and_separator_lines() -> None:
    body = FIXTURE.read_bytes()
    entries = parse_master_index(body)
    form_types = [e.form_type for e in entries]
    # Header row "Form Type" must not leak through as a data row.
    assert "Form Type" not in form_types
    # Dashed separator line must not be parsed as a data row.
    assert not any(set(ft) == {"-"} for ft in form_types)
    # Four data rows in fixture — zero header/separator leakage means
    # exactly four entries.
    assert len(entries) == 4


def test_returns_empty_list_for_body_with_no_data_rows() -> None:
    body = b"Description: empty\n\nCIK|Company Name|Form Type|Date Filed|Filename\n-----\n"
    entries = parse_master_index(body)
    assert entries == []


def test_skips_malformed_rows_silently() -> None:
    body = (
        b"CIK|Company Name|Form Type|Date Filed|Filename\n"
        b"------\n"
        b"320193|APPLE INC|10-Q|2026-04-15|edgar/data/320193/0000320193-26-000042.txt\n"
        b"malformed row with no pipes\n"
        b"789019|MICROSOFT CORP|8-K|2026-04-15|edgar/data/789019/0000789019-26-000017.txt\n"
    )
    entries = parse_master_index(body)
    assert len(entries) == 2


def test_reconstructs_dashed_accession_from_nodash_filename() -> None:
    """Some SEC tools emit filenames with the 18-digit accession and
    no dashes. Parser must normalise to the canonical dashed form."""
    body = (
        b"CIK|Company Name|Form Type|Date Filed|Filename\n"
        b"------\n"
        b"320193|APPLE INC|10-Q|2026-04-15|edgar/data/320193/000032019326000042.txt\n"
    )
    entries = parse_master_index(body)
    assert len(entries) == 1
    assert entries[0].accession_number == "0000320193-26-000042"
