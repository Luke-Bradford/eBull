"""#3609 step 1 slice 3c-ii: artefact file mechanics, construction hashes and the frozen-input round trip."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.services.factor_panel import PanelError
from app.services.factor_panel_artefact import (
    GzLines,
    construction_versions,
    gz_content_sha256,
    import_closure,
    read_gz_lines,
    sha256_file,
    write_gz_lines,
)
from app.services.series_termination import TerminationEvidence
from app.services.universe_selection import AdmittedSeries
from scripts.build_3609_factor_panel import Frozen, _admitted_from, _admitted_json, read_rf


def test_gz_lines_are_byte_deterministic_and_write_once(tmp_path: Path) -> None:
    rows = [{"b": 1.5, "a": None}, [3, "x", float("nan")]]
    write_gz_lines(tmp_path / "one.jsonl.gz", rows)
    write_gz_lines(tmp_path / "two.jsonl.gz", rows)
    assert sha256_file(tmp_path / "one.jsonl.gz") == sha256_file(tmp_path / "two.jsonl.gz")
    assert gz_content_sha256(tmp_path / "one.jsonl.gz") == gz_content_sha256(tmp_path / "two.jsonl.gz")
    back = list(read_gz_lines(tmp_path / "one.jsonl.gz"))
    assert back[0] == {"a": None, "b": 1.5} and back[1][:2] == [3, "x"]
    with pytest.raises(FileExistsError):
        GzLines(tmp_path / "one.jsonl.gz")


def _repo(tmp_path: Path) -> Path:
    for relative, text in {
        "app/__init__.py": "",
        "app/services/__init__.py": "",
        "app/services/a.py": "from app.services.b import thing\n",
        "app/services/b.py": "def f():\n    import app.services.c\n",
        "app/services/c.py": "import json\n",
        "app/services/unused.py": "",
        "scripts/root.py": "from app.services import a\n",
    }.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return tmp_path


def test_import_closure_follows_app_imports_transitively_including_function_level(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    got = import_closure([repo / "scripts/root.py"], repo)
    assert sorted(got) == [
        "app/__init__.py",
        "app/services/__init__.py",
        "app/services/a.py",
        "app/services/b.py",
        "app/services/c.py",
        "scripts/root.py",
    ]


def test_import_closure_refuses_relative_imports(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    (repo / "app/services/c.py").write_text("from . import a\n")
    with pytest.raises(PanelError, match="relative import"):
        import_closure([repo / "scripts/root.py"], repo)


def test_construction_versions_are_per_characteristic_and_move_with_any_source(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    before = construction_versions(["be_me", "ret_12_1"], "spec", import_closure([repo / "scripts/root.py"], repo))
    assert before["be_me"] != before["ret_12_1"]
    (repo / "app/services/c.py").write_text("import json  # edited\n")
    after = construction_versions(["be_me", "ret_12_1"], "spec", import_closure([repo / "scripts/root.py"], repo))
    assert after["be_me"] != before["be_me"]
    (repo / "app/services/unused.py").write_text("# not imported\n")
    unchanged = construction_versions(["be_me", "ret_12_1"], "spec", import_closure([repo / "scripts/root.py"], repo))
    assert unchanged == after
    assert (
        construction_versions(["be_me"], "spec2", {})["be_me"] != construction_versions(["be_me"], "spec", {})["be_me"]
    )


def test_the_real_builder_closure_covers_its_rule_modules() -> None:
    repo = Path(__file__).resolve().parents[1]
    got = import_closure([repo / "scripts/build_3609_factor_panel.py"], repo)
    for module in ("factor_panel", "factor_panel_prices", "factor_panel_artefact", "total_return_reader"):
        assert f"app/services/{module}.py" in got


@pytest.mark.parametrize(
    "series",
    [
        AdmittedSeries(7, 70, 70, None),
        AdmittedSeries(8, -8, None, TerminationEvidence(True, "(b)", False), date(2019, 3, 1)),
    ],
)
def test_admitted_series_round_trip(series: AdmittedSeries, tmp_path: Path) -> None:
    write_gz_lines(tmp_path / Frozen.ADMITTED, [_admitted_json(series, "SYM")])
    (line,) = read_gz_lines(tmp_path / Frozen.ADMITTED)
    assert _admitted_from(line) == series
    assert line["symbol"] == "SYM"


def test_read_rf_takes_only_rf_in_the_window_and_checks_its_unit(tmp_path: Path) -> None:
    rows = [
        ["Mkt-RF", "2019-01-02", "0.01", "decimal_return"],
        ["RF", "2013-12-31", "0.5", "decimal_return"],
        ["RF", "2019-01-02", "0.0001", "decimal_return"],
        ["RF", "2021-06-01", "0.5", "decimal_return"],
    ]
    write_gz_lines(tmp_path / Frozen.snapshot("french_three_factor_daily"), rows)
    assert read_rf(tmp_path, date(2014, 1, 1)) == {date(2019, 1, 2): 0.0001}
    (tmp_path / Frozen.snapshot("french_three_factor_daily")).unlink()
    write_gz_lines(
        tmp_path / Frozen.snapshot("french_three_factor_daily"), [["RF", "2019-01-02", "1", "percent_per_annum"]]
    )
    with pytest.raises(PanelError, match="unit"):
        read_rf(tmp_path, date(2014, 1, 1))


def test_every_value_read_in_the_builder_carries_the_hold_out_bound() -> None:
    import scripts.build_3609_factor_panel as builder

    reads = {name: sql for name, sql in vars(builder).items() if name.endswith("_SQL") and isinstance(sql, str)}
    counts_only = {"_SNAPSHOT_COUNT_SQL"}  # integrity count of the pinned snapshot; reads no value
    assert {"_DAILY_SQL", "_DECISION_BARS_SQL", "_SNAPSHOT_SQL", "_SPLITS_SQL", "_SPY_SESSIONS_SQL"} <= set(reads)
    unbounded = sorted(name for name, sql in reads.items() if name not in counts_only and "%(bound)s" not in sql)
    assert unbounded == []
