"""Tests for the top-13F-filer discovery primitive.

Pins the contract: form.idx parser handles the various
form-type / company-name shapes in SEC's index, aggregator sums
counts across quarters, per-quarter fetch failures don't abort
the sweep.
"""

from __future__ import annotations

from datetime import date

import pytest

from app.services.top_filer_discovery import (
    aggregate_top_filers,
    parse_form_index,
)

_HEADER = "Description: x\n\nForm Type   Company Name   CIK   Date Filed   File Name\n" + "-" * 100 + "\n"


def _row(form_type: str, company: str, cik: int, filed: str, file: str) -> str:
    """Build one form.idx row with canonical 2+-space gaps. SEC's
    real layout uses fixed-width columns; the parser anchors on
    the trailing CIK / date / file columns and only requires 2+
    spaces between form_type and company_name + before each
    trailing field."""
    return f"{form_type}  {company}  {cik}  {filed}  {file}\n"


def test_parse_form_index_handles_basic_13fhr_row() -> None:
    payload = _HEADER + _row("13F-HR", "BlackRock Advisors LLC", 1086364, "2025-03-01", "edgar/data/1086364/a.txt")
    entries = list(parse_form_index(payload))
    assert len(entries) == 1
    e = entries[0]
    assert e.form_type == "13F-HR"
    assert e.company_name == "BlackRock Advisors LLC"
    assert e.cik == "0001086364"
    assert e.date_filed == date(2025, 3, 1)
    assert e.file_name == "edgar/data/1086364/a.txt"


def test_parse_form_index_pads_cik_to_ten_digits() -> None:
    """SEC stores CIKs as plain integers in form.idx but every
    other table in the codebase uses 10-digit zero-padded form."""
    payload = _HEADER + _row("13F-HR", "Small Filer", 42, "2025-02-14", "edgar/data/42/x.txt")
    entries = list(parse_form_index(payload))
    assert entries[0].cik == "0000000042"


def test_parse_form_index_handles_form_types_with_spaces() -> None:
    """Form types like ``1-A POS`` contain a single space; SEC's
    layout puts a 2+-space gap before the company name as the
    canonical boundary."""
    payload = _HEADER + _row("1-A POS", "Foo Bar LLC", 222222, "2025-02-14", "edgar/data/222222/w.txt")
    entries = list(parse_form_index(payload))
    assert entries[0].form_type == "1-A POS"
    assert entries[0].company_name == "Foo Bar LLC"


def test_parse_form_index_skips_preamble_and_malformed_lines() -> None:
    payload = (
        _HEADER
        + "garbage line with no structure\n"
        + _row("13F-HR", "ValidFiler", 100, "2025-01-15", "edgar/data/100/x.txt")
        + "another garbage\n"
    )
    entries = list(parse_form_index(payload))
    assert len(entries) == 1
    assert entries[0].cik == "0000000100"


def test_aggregate_top_filers_counts_13f_variants_only() -> None:
    """13F-HR / 13F-HR/A / 13F-NT count toward the aggregation;
    other forms (10-K, Form 4) do not."""
    payload = (
        _HEADER
        + _row("13F-HR", "Filer A", 100, "2025-01-15", "edgar/data/100/a.txt")
        + _row("13F-HR/A", "Filer A", 100, "2025-02-01", "edgar/data/100/b.txt")
        + _row("10-K", "Filer A", 100, "2025-03-01", "edgar/data/100/k.txt")
        + _row("13F-NT", "Filer B", 200, "2025-01-15", "edgar/data/200/x.txt")
        + _row("4", "Apple", 320193, "2025-01-15", "edgar/data/320193/z.txt")
    )
    fetched: list[tuple[int, int]] = []

    def _fake(year: int, q: int) -> str:
        fetched.append((year, q))
        return payload

    top = aggregate_top_filers([(2025, 1)], top_n=10, fetch=_fake)
    assert fetched == [(2025, 1)]
    by_cik = {c.cik: c for c in top}
    assert by_cik["0000000100"].filing_count == 2  # 13F-HR + 13F-HR/A
    assert by_cik["0000000200"].filing_count == 1  # 13F-NT
    assert "0000320193" not in by_cik  # Form 4 ignored


def test_aggregate_top_filers_sums_across_quarters_and_truncates() -> None:
    """Counts sum across quarters; result is truncated to top_n
    sorted by count desc."""
    by_quarter: dict[tuple[int, int], str] = {
        (2024, 4): _HEADER
        + _row("13F-HR", "Filer A", 100, "2024-11-15", "edgar/data/100/a.txt")
        + _row("13F-HR", "Filer B", 200, "2024-11-15", "edgar/data/200/b.txt")
        + _row("13F-HR", "Filer C", 300, "2024-11-15", "edgar/data/300/c.txt"),
        (2025, 1): _HEADER
        + _row("13F-HR", "Filer A", 100, "2025-02-15", "edgar/data/100/a2.txt")
        + _row("13F-HR", "Filer A", 100, "2025-03-15", "edgar/data/100/a3.txt")
        + _row("13F-HR/A", "Filer B", 200, "2025-02-15", "edgar/data/200/b2.txt"),
    }

    def _fake(year: int, q: int) -> str:
        return by_quarter[(year, q)]

    top = aggregate_top_filers([(2024, 4), (2025, 1)], top_n=2, fetch=_fake)
    assert [(c.cik, c.filing_count) for c in top] == [
        ("0000000100", 3),
        ("0000000200", 2),
    ]


def test_aggregate_top_filers_per_quarter_failure_isolated() -> None:
    """A fetch failure on one quarter must not abort the whole
    sweep. Surviving quarters still aggregate."""

    def _fake(year: int, q: int) -> str:
        if (year, q) == (2025, 1):
            raise RuntimeError("simulated SEC outage")
        return _HEADER + _row("13F-HR", "Filer A", 100, "2024-11-15", "edgar/data/100/a.txt")

    top = aggregate_top_filers([(2024, 4), (2025, 1)], top_n=10, fetch=_fake)
    assert len(top) == 1
    assert top[0].cik == "0000000100"
    assert top[0].filing_count == 1


def test_aggregate_top_filers_uses_latest_quarter_name() -> None:
    """When a filer renames mid-year, the aggregated record uses
    the most recent quarter's name."""
    by_quarter: dict[tuple[int, int], str] = {
        (2024, 4): _HEADER + _row("13F-HR", "OLD CORPORATE NAME LLC", 100, "2024-11-15", "edgar/data/100/a.txt"),
        (2025, 1): _HEADER + _row("13F-HR", "NEW REBRANDED NAME LLC", 100, "2025-02-15", "edgar/data/100/b.txt"),
    }

    def _fake(year: int, q: int) -> str:
        return by_quarter[(year, q)]

    top = aggregate_top_filers([(2024, 4), (2025, 1)], top_n=10, fetch=_fake)
    assert top[0].latest_name == "NEW REBRANDED NAME LLC"


def test_aggregate_top_filers_picks_latest_name_independent_of_quarter_iteration_order() -> None:
    """latest_name is keyed by date_filed, not iteration order, so
    a caller passing quarters newest→oldest still gets the most
    recent name. Regression for the high-severity Codex finding."""
    by_quarter: dict[tuple[int, int], str] = {
        (2024, 4): _HEADER + _row("13F-HR", "OLD CORPORATE NAME LLC", 100, "2024-11-15", "edgar/data/100/a.txt"),
        (2025, 1): _HEADER + _row("13F-HR", "NEW REBRANDED NAME LLC", 100, "2025-02-15", "edgar/data/100/b.txt"),
    }

    def _fake(year: int, q: int) -> str:
        return by_quarter[(year, q)]

    # Pass quarters newest-first (the order _last_n_quarters returns).
    top = aggregate_top_filers([(2025, 1), (2024, 4)], top_n=10, fetch=_fake)
    assert top[0].latest_name == "NEW REBRANDED NAME LLC"


def test_last_n_quarters_returns_newest_first_excluding_current() -> None:
    """The CLI helper _last_n_quarters skips the in-progress
    quarter and returns N closed quarters newest-first."""
    from datetime import date as _d

    from scripts.seed_top_13f_filers import _last_n_quarters

    # March is in Q1; the most recent CLOSED quarter is Q4 of the
    # prior year.
    assert _last_n_quarters(_d(2025, 3, 15), 4) == [
        (2024, 4),
        (2024, 3),
        (2024, 2),
        (2024, 1),
    ]
    # July is in Q3; closed quarters are Q2 of same year going back.
    assert _last_n_quarters(_d(2025, 7, 1), 3) == [
        (2025, 2),
        (2025, 1),
        (2024, 4),
    ]


def test_cli_apply_skips_existing_seeded_ciks(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The --apply CLI must NOT overwrite operator-curated rows.
    CIKs already present in institutional_filer_seeds are preserved
    and reported in the summary as existing_preserved. Regression
    for the medium-severity Codex finding (label clobber)."""
    import psycopg as _psycopg

    from app.services import filer_seed_verification, top_filer_discovery
    from scripts import seed_top_13f_filers as cli
    from tests.fixtures.ebull_test_db import test_database_url

    # Pull a connection fresh; we can't use the fixture here because
    # this isn't a fixture-using test, so do it manually.
    test_url = test_database_url()
    setup_conn = _psycopg.connect(test_url)
    try:
        # Truncate first to start clean.
        setup_conn.execute("TRUNCATE institutional_filer_seeds CASCADE")
        # Seed an existing curated row.
        setup_conn.execute(
            """
            INSERT INTO institutional_filer_seeds (cik, label, expected_name, active)
            VALUES ('0000111000', 'Curated Display Label', 'CURATED EXPECTED', TRUE)
            """,
        )
        setup_conn.commit()

        # Stub aggregator to return a candidate matching the curated CIK.
        monkeypatch.setattr(
            top_filer_discovery,
            "aggregate_top_filers",
            lambda quarters, top_n=200, fetch=None: [
                top_filer_discovery.TopFilerCandidate(
                    cik="0000111000",
                    latest_name="Raw SEC Name",
                    filing_count=10,
                ),
            ],
        )
        # Patch the CLI's import too — resolves at module load.
        monkeypatch.setattr(cli, "aggregate_top_filers", top_filer_discovery.aggregate_top_filers)
        monkeypatch.setattr(
            filer_seed_verification,
            "_fetch_submissions",
            lambda _conn, _cik: {"name": "Raw SEC Name"},
        )
        # Route the CLI's psycopg.connect to the test DB.
        original_connect = _psycopg.connect
        monkeypatch.setattr(cli.psycopg, "connect", lambda _url: original_connect(test_url))

        cli.main(["--top-n", "10", "--apply"])
        capsys.readouterr()  # drain

        # Curated label MUST be preserved.
        with setup_conn.cursor() as cur:
            cur.execute(
                "SELECT label FROM institutional_filer_seeds WHERE cik = '0000111000'",
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "Curated Display Label"  # NOT clobbered to 'Raw SEC Name'
    finally:
        setup_conn.execute("TRUNCATE institutional_filer_seeds CASCADE")
        setup_conn.commit()
        setup_conn.close()
