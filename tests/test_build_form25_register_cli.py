"""#2721 — the Form 25 register builder's multi-year CLI contract.

Only the pure/parse layer: every guard here fires in ``main`` before any DB
connection or network call, so these run in the fast tier.
"""

from __future__ import annotations

import pytest

from scripts.build_2282_form25_register import main, parse_years


def test_years_range_is_inclusive_both_ends() -> None:
    assert parse_years("2013-2024") == list(range(2013, 2025))


def test_single_year_spec_is_one_year() -> None:
    assert parse_years("2019") == [2019]


def test_backwards_range_is_refused() -> None:
    with pytest.raises(ValueError):
        parse_years("2024-2013")


def test_emit_fixture_for_another_year_cannot_overwrite_the_frozen_fixture() -> None:
    # tests/fixtures/form25_2023_cohort.csv is the vendor acceptance test for
    # any future price source. `--emit-fixture --year 2013` on the default
    # path would silently replace it with a different year's cohort — a
    # different test wearing the same filename.
    with pytest.raises(SystemExit):
        main(["--emit-fixture", "--year", "2013"])


def test_emit_fixture_is_single_year_only() -> None:
    with pytest.raises(SystemExit):
        main(["--emit-fixture", "--years", "2013-2024"])


def test_year_and_years_together_are_refused() -> None:
    with pytest.raises(SystemExit):
        main(["--census", "--year", "2019", "--years", "2013-2024"])


def test_malformed_years_is_an_argparse_error_not_a_traceback() -> None:
    with pytest.raises(SystemExit):
        main(["--census", "--years", "20x3-2024"])
