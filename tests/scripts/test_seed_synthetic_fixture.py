"""Tests for ``scripts/perf_bench/seed_synthetic_fixture``.

DB-free coverage: helpers, refusal paths, generator shape. The
``ownership_institutions_current`` end-to-end seed is run by the
operator against a bench DB per
``docs/operator/runbooks/perf-investigation.md`` — CI does not
provision a bench Postgres instance.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from scripts.perf_bench.seed_synthetic_fixture import (
    SENTINEL_INSTRUMENT_ID_BASE,
    require_bench_db_url,
    sentinel_instrument_id,
)
from scripts.perf_bench.seed_synthetic_fixture import (
    seed_ownership_institutions_current as seed_iic,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_sentinel_base_is_one_billion() -> None:
    """The base must stay >= 1e9 so the assertion in
    ``assert_sentinel_range_clear`` matches the spec."""
    assert SENTINEL_INSTRUMENT_ID_BASE == 1_000_000_000


def test_sentinel_instrument_id_monotonic() -> None:
    assert sentinel_instrument_id(0) == SENTINEL_INSTRUMENT_ID_BASE
    assert sentinel_instrument_id(1) == SENTINEL_INSTRUMENT_ID_BASE + 1
    assert sentinel_instrument_id(999) == SENTINEL_INSTRUMENT_ID_BASE + 999


def test_sentinel_instrument_id_rejects_negative() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        sentinel_instrument_id(-1)


@pytest.fixture
def _clear_bench_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.delenv("EBULL_BENCH_DB_URL", raising=False)
    yield


def test_require_bench_db_url_refuses_unset(_clear_bench_env: None, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        require_bench_db_url()
    assert excinfo.value.code == 2
    assert "EBULL_BENCH_DB_URL unset" in capsys.readouterr().err


def test_require_bench_db_url_refuses_missing_bench_substring(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("EBULL_BENCH_DB_URL", "postgresql://x/staging_db")
    with pytest.raises(SystemExit) as excinfo:
        require_bench_db_url()
    assert excinfo.value.code == 2
    assert "must contain the substring" in capsys.readouterr().err


def test_require_bench_db_url_refuses_dev_substring(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("EBULL_BENCH_DB_URL", "postgresql://x/bench_dev")
    with pytest.raises(SystemExit) as excinfo:
        require_bench_db_url()
    assert excinfo.value.code == 2
    assert "denylisted substring 'dev'" in capsys.readouterr().err


def test_require_bench_db_url_refuses_prod_substring(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("EBULL_BENCH_DB_URL", "postgresql://x/bench_prod_mirror")
    with pytest.raises(SystemExit) as excinfo:
        require_bench_db_url()
    assert excinfo.value.code == 2
    assert "denylisted substring 'prod'" in capsys.readouterr().err


def test_require_bench_db_url_accepts_bench_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EBULL_BENCH_DB_URL", "postgresql://x/ebull_bench")
    assert require_bench_db_url() == "postgresql://x/ebull_bench"


def test_require_bench_db_url_case_insensitive(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Uppercase 'DEV' must also be caught — denylist is case-insensitive.
    monkeypatch.setenv("EBULL_BENCH_DB_URL", "postgresql://x/BenchDEVmirror")
    with pytest.raises(SystemExit) as excinfo:
        require_bench_db_url()
    assert excinfo.value.code == 2
    assert "denylisted substring 'dev'" in capsys.readouterr().err


def test_generate_rows_count_matches_product() -> None:
    rows = list(seed_iic._generate_rows(num_instruments=4, num_filers=3))
    # 4 × 3 × 5 ownership_natures
    assert len(rows) == 60


def test_generate_rows_all_sentinel_range() -> None:
    rows = list(seed_iic._generate_rows(num_instruments=5, num_filers=2))
    for row in rows:
        assert row[0] >= SENTINEL_INSTRUMENT_ID_BASE


def test_generate_rows_pk_unique() -> None:
    rows = list(seed_iic._generate_rows(num_instruments=10, num_filers=5))
    # PK is (instrument_id, filer_cik, ownership_nature, exposure_kind)
    pk_set = {(r[0], r[1], r[4], r[9]) for r in rows}
    assert len(pk_set) == len(rows)


def test_generate_rows_check_constraints() -> None:
    """Every emitted row must satisfy the CHECK constraints at
    sql/114_ownership_institutions_observations.sql:120-135 AND the
    regex CHECK from sql/134_ownership_identifier_check_constraints.sql:54-58
    (``filer_cik ~ '^[0-9]{10}$'``). Codex 2 pre-push catch (2026-05-27):
    initial implementation generated ``SYN`` + zero-padded CIKs which
    pass the sql/114 NOT NULL constraint but fail the sql/134 regex
    CHECK on COPY; the seeder would abort on the first row."""
    import re

    allowed_natures = {"direct", "indirect", "beneficial", "voting", "economic"}
    allowed_filer_types = {"ETF", "INV", "INS", "BD", "OTHER"}
    allowed_sources = {
        "form4",
        "form3",
        "13d",
        "13g",
        "def14a",
        "13f",
        "nport",
        "ncsr",
        "xbrl_dei",
        "10k_note",
        "finra_si",
        "derived",
    }
    allowed_exposure = {"EQUITY", "PUT", "CALL"}
    cik_re = re.compile(r"^[0-9]{10}$")
    for row in seed_iic._generate_rows(num_instruments=2, num_filers=3):
        _iid, cik, _name, ftype, nature, source, _doc, _pe, _fa, exp = row
        assert ftype in allowed_filer_types
        assert nature in allowed_natures
        assert source in allowed_sources
        assert exp in allowed_exposure
        assert cik_re.fullmatch(cik), f"filer_cik {cik!r} violates sql/134 CHECK"


def test_synthetic_filer_cik_overflow_guard() -> None:
    """The synthetic CIK base is 9_000_000_000; offsets beyond
    999_999_999 overflow into an 11-digit string that would fail the
    sql/134 ``^[0-9]{10}$`` CHECK. Helper must refuse before the row
    reaches COPY."""
    assert seed_iic._synthetic_filer_cik(0) == "9000000000"
    assert seed_iic._synthetic_filer_cik(999_999_999) == "9999999999"
    with pytest.raises(ValueError, match="overflows synthetic 10-digit CIK range"):
        seed_iic._synthetic_filer_cik(1_000_000_000)


def test_synthetic_filer_cik_rejects_negative() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        seed_iic._synthetic_filer_cik(-1)


def test_main_dry_run_succeeds(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.delenv("EBULL_BENCH_DB_URL", raising=False)
    exit_code = seed_iic.main(["--num-instruments", "1000", "--num-filers", "200", "--dry-run"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "dry-run" in captured.out
    assert "1000000" in captured.out


def test_main_dry_run_rejects_below_floor(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.delenv("EBULL_BENCH_DB_URL", raising=False)
    exit_code = seed_iic.main(["--num-instruments", "10", "--num-filers", "10", "--dry-run"])
    assert exit_code == 2
    assert "below floor" in capsys.readouterr().err


def test_main_rejects_non_positive_args(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.delenv("EBULL_BENCH_DB_URL", raising=False)
    assert seed_iic.main(["--num-instruments", "0", "--num-filers", "200", "--dry-run"]) == 2
    assert "must be positive" in capsys.readouterr().err


@pytest.mark.parametrize(
    "module_name",
    [
        "seed_ownership_institutions_observations",
        "seed_ownership_insiders_observations",
        "seed_ownership_funds_observations",
        "seed_financial_facts_raw",
        "seed_sec_filing_manifest",
        "seed_filing_events",
    ],
)
def test_stub_modules_raise_not_implemented(module_name: str) -> None:
    """Each of the 6 stub modules must refuse to run + cite the spec."""
    mod = importlib.import_module(f"scripts.perf_bench.seed_synthetic_fixture.{module_name}")
    with pytest.raises(NotImplementedError, match="not implemented in Phase 0"):
        mod.main()


def test_all_seven_floor_tables_have_a_module() -> None:
    """Every key in floors.yaml must have a corresponding seed_<table>.py
    module so floor coverage is visible from filesystem alone."""
    from scripts.perf_bench._floors import load_floors

    floor_tables = set(load_floors().keys())
    package_root = REPO_ROOT / "scripts" / "perf_bench" / "seed_synthetic_fixture"
    present = {p.stem.removeprefix("seed_") for p in package_root.glob("seed_*.py")}
    missing = floor_tables - present
    assert not missing, (
        f"floors.yaml lists tables without a seed module: {sorted(missing)}; "
        "add the stub per docs/operator/runbooks/perf-investigation.md"
    )


def test_stub_modules_main_runs_via_subprocess() -> None:
    """Confirm the ``if __name__ == '__main__'`` block in a stub propagates
    a non-zero exit code (NotImplementedError is uncaught → exit 1)."""
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.perf_bench.seed_synthetic_fixture.seed_filing_events",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "NotImplementedError" in result.stderr
