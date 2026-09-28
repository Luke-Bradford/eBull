"""#1822 slice 1b-ii: the terms sidecar, declaration, register mapping, query registry, access
boundary, readout gate and readout composition (spec v8 "Declaration (v8)")."""

from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from app.services import ranking_ablation as ra
from app.services import ranking_ablation_reader as reader
from app.services import ranking_ablation_readout as readout
from app.services import ranking_ablation_terms as terms
from app.services import result_ledger
from app.services.prereg_contract import declaration_refusals
from app.services.price_quarantine import Bar
from app.services.series_termination import TerminationClass
from app.services.trial_register import TRIAL_REGISTER, TrialRegister
from scripts import run_1822_ablation_readout as script

_REPO = Path(__file__).resolve().parents[1]
_FACT_DAY = date(2026, 9, 28)


def _facts(**overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "fact_date": _FACT_DAY,
        "entry_sessions": [date(2026, 7, 23), date(2026, 7, 24), date(2026, 9, 1)],
        "first_entry": date(2026, 7, 23),
        "g_k": date(2026, 8, 24),
    }
    kwargs.update(overrides)
    return terms.frozen_facts(**kwargs)


@pytest.fixture
def sidecar_dir(tmp_path: Path) -> Path:
    return tmp_path / "1822-route-f"


# --- Semantic terms and the inventory ---------------------------------------


def test_semantic_terms_are_deterministic_and_charge_96() -> None:
    first = terms.semantic_terms()
    assert terms.canonical_bytes(first) == terms.canonical_bytes(terms.semantic_terms())
    assert first["searches"] == terms.searches(first["inventory"]) == 96 == 8 * 6 * 2
    assert first["construction_revision"] == ra.CONSTRUCTION_REVISION
    assert first["sql"] == dict(reader.ROUTE_F_SQL)
    assert first["inventory"]["evaluations"] == [
        "canonical",
        "t3_excluded",
        "as_traded",
        "verbatim_bar_rule",
        "canonical@classified_worst",
        "canonical@classified_best",
    ]


def test_canonical_bytes_are_sorted_compact_ascii_with_one_newline() -> None:
    assert terms.canonical_bytes({"b": 1, "a": "é"}) == b'{"a":"\\u00e9","b":1}\n'


# --- Frozen facts ---------------------------------------------------------


def test_floor_counts_entry_sessions_on_the_grid_only() -> None:
    facts = _facts()
    assert facts["min_independent_decision_dates"] == 2  # 2026-09-01 is past G_k
    assert facts["min_calendar_weeks"] == 5  # ceil(32 / 7)
    assert facts["entry_sessions_on_grid"] == ["2026-07-23", "2026-07-24"]
    assert len(facts["derivation"]) <= 1000


def test_floor_not_positive_refuses() -> None:
    with pytest.raises(terms.FloorNotPositive):
        _facts(entry_sessions=[date(2026, 9, 1)])
    with pytest.raises(terms.FloorNotPositive):
        _facts(g_k=date(2026, 7, 23), entry_sessions=[date(2026, 7, 23)])


# --- Sidecars -------------------------------------------------------------


def test_sidecar_round_trips_and_is_selected_by_its_terms(sidecar_dir: Path) -> None:
    written = terms.write_sidecar(terms.semantic_terms(), _facts(), sidecar_dir)
    (loaded,) = terms.load_sidecars(sidecar_dir)
    assert loaded == written
    assert Path(loaded.path).name == f"terms-{loaded.sha256}.json"
    assert terms.select_sidecar(terms.semantic_terms(), [loaded]) == loaded
    assert terms.select_sidecar({"other": 1}, [loaded]) == "no_sidecar_for_terms"


def test_sidecar_with_already_declared_terms_is_refused(sidecar_dir: Path) -> None:
    terms.write_sidecar(terms.semantic_terms(), _facts(), sidecar_dir)
    with pytest.raises(terms.SidecarRefused, match="terms_already_declared"):
        terms.write_sidecar(terms.semantic_terms(), _facts(fact_date=date(2026, 9, 29)), sidecar_dir)


def test_two_sidecars_with_equal_terms_are_ambiguous(sidecar_dir: Path) -> None:
    one = terms.write_sidecar(terms.semantic_terms(), _facts(), sidecar_dir)
    other = replace(one, sha256="f" * 64)
    assert terms.select_sidecar(terms.semantic_terms(), [one, other]) == "ambiguous_sidecars"


def test_tampered_or_non_canonical_sidecar_is_refused(sidecar_dir: Path) -> None:
    written = terms.write_sidecar(terms.semantic_terms(), _facts(), sidecar_dir)
    path = sidecar_dir / Path(written.path).name
    path.write_bytes(path.read_bytes().replace(b"canonical", b"Canonical", 1))
    with pytest.raises(terms.SidecarRefused, match="does not match its name"):
        terms.load_sidecars(sidecar_dir)
    pretty = json.dumps({"semantic_terms": {}, "frozen_facts": {}}, indent=1).encode()
    path.unlink()
    import hashlib

    (sidecar_dir / f"terms-{hashlib.sha256(pretty).hexdigest()}.json").write_bytes(pretty)
    with pytest.raises(terms.SidecarRefused, match="not canonical"):
        terms.load_sidecars(sidecar_dir)


# --- Identity, declaration and register -------------------------------------


def test_identity_uses_the_full_digest_within_sql_333_limits(sidecar_dir: Path) -> None:
    sidecar = terms.write_sidecar(terms.semantic_terms(), _facts(), sidecar_dir)
    ident = terms.identity(sidecar.sha256)
    assert ident.strategy_version == f"v1.5-balanced+{sidecar.sha256}"
    assert len(ident.strategy_version) == 78
    assert ident.trial_id == f"ranking-ablation-1822-route-f-{sidecar.sha256}"
    assert ident.contract_version == f"1822-route-f-terms-{sidecar.sha256}"


def test_declaration_is_coherent_falsification_over_survivor_only(sidecar_dir: Path) -> None:
    declaration = terms.build_declaration(terms.write_sidecar(terms.semantic_terms(), _facts(), sidecar_dir))
    assert declaration_refusals(declaration) == ()
    assert declaration.prereg_purpose == "falsification_only"
    assert declaration.expected_structural_refusals == ("universe_basis_not_survivorship_free",)
    assert declaration.forward_shadow.min_independent_decision_dates == 2
    assert declaration.forward_shadow.min_calendar_weeks == 5


def test_register_mapping_is_one_entry_per_sidecar(sidecar_dir: Path) -> None:
    sidecar = terms.write_sidecar(terms.semantic_terms(), _facts(), sidecar_dir)
    entry = terms.expected_register_entry(sidecar)
    assert entry.searches == 96
    assert entry.evidence.endswith("pinned_specs=96")
    base = TRIAL_REGISTER.trials
    ok = TrialRegister(version="t", trials=(*base, entry))
    assert terms.register_refusals([sidecar], ok) == ()
    assert terms.register_refusals([sidecar], TrialRegister(version="t", trials=base)) == (
        f"{sidecar.path}: 0 register entries",
    )
    wrong = TrialRegister(version="t", trials=(*base, replace(entry, searches=95)))
    assert f"{sidecar.path}: register entry searches differs" in terms.register_refusals([sidecar], wrong)
    assert terms.register_refusals([], ok) == (f"{entry.trial_id}: no sidecar",)


def test_committed_sidecars_and_register_agree() -> None:
    """Integrity of every committed sidecar, and its entry. Coherence is checked for the selected one only."""
    sidecars = terms.load_sidecars()
    assert terms.register_refusals(sidecars, TRIAL_REGISTER) == ()
    selected = terms.select_sidecar(terms.semantic_terms(), sidecars)
    if not isinstance(selected, str):
        assert declaration_refusals(terms.build_declaration(selected)) == ()


# --- Query registry and the sealed-outcome boundary -------------------------

_ALLOWED_RELATIONS = {
    "scores",
    "job_runs",
    "instruments",
    "etoro_instrument_types",
    "exchanges",
    "price_daily",
    "instrument_cik_history",
    "sec_form25_common_equity_delistings",
    "instrument_dividend_summary",
    "dividend_events",
    "financial_facts_raw",
    "theses",
    "news_events",
    "strategy_preregistration_declarations",
}
_RELATION = re.compile(r"\b(?:FROM|JOIN)\s+([a-z_][a-z0-9_]*)\b", re.IGNORECASE)
_CTE = re.compile(r"\b([a-z_][a-z0-9_]*)\s+AS\s*\(", re.IGNORECASE)


def test_execute_refuses_an_unregistered_query() -> None:
    with pytest.raises(reader.UnregisteredQuery):
        reader.execute(None, "SELECT 1")  # type: ignore[arg-type]


def test_every_registered_relation_is_allowlisted() -> None:
    ledger_sql = [result_ledger._SELECT_DECLARATION, result_ledger._FREEZE_DECLARATION]
    named = {match.lower() for sql in [*reader.ROUTE_F_SQL.values(), *ledger_sql] for match in _RELATION.findall(sql)}
    named -= {"unnest"}  # a function in FROM, not a relation
    named -= {cte.lower() for sql in reader.ROUTE_F_SQL.values() for cte in _CTE.findall(sql)}
    assert named <= _ALLOWED_RELATIONS, named - _ALLOWED_RELATIONS
    assert not named & {"strategy_results_store", "strategy_holdout_accesses"}


_ROUTE_F_FILES = (
    "scripts/run_1822_ablation_readout.py",
    "app/services/ranking_ablation.py",
    "app/services/ranking_ablation_reader.py",
    "app/services/ranking_ablation_terms.py",
    "app/services/ranking_ablation_readout.py",
)
#: Lazy imports inside the app.services modules route F imports, measured 2026-09-28. A new one
#: fails here and must be checked for a path to the withheld side before it is added.
_KNOWN_LAZY_IMPORTS = {
    "zoneinfo",
    "io",
    "pyarrow.parquet",
    "app.services.risk_metrics",
    "app.services.strategy_recent_evidence",
}


def _nested_imports(path: Path) -> set[str]:
    found: set[str] = set()
    for function in ast.walk(ast.parse(path.read_text())):
        if isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef):
            for node in ast.walk(function):
                if isinstance(node, ast.ImportFrom) and node.module:
                    found.add(node.module)
                elif isinstance(node, ast.Import):
                    found.update(alias.name for alias in node.names)
    return found


def _imported_service_modules(path: Path) -> set[str]:
    modules: set[str] = set()
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.ImportFrom) and node.module == "app.services":
            modules.update(f"app.services.{alias.name}" for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("app.services."):
            modules.add(node.module)
    return modules


def test_route_f_imports_at_top_level_only() -> None:
    for name in _ROUTE_F_FILES:
        assert _nested_imports(_REPO / name) == set(), name
    lazy: set[str] = set()
    for name in _ROUTE_F_FILES:
        for module in _imported_service_modules(_REPO / name):
            file = _REPO / (module.replace(".", "/") + ".py")
            lazy |= _nested_imports(file if file.exists() else _REPO / module.replace(".", "/") / "__init__.py")
    assert lazy <= _KNOWN_LAZY_IMPORTS, lazy - _KNOWN_LAZY_IMPORTS


def test_route_f_never_loads_backtest_run() -> None:
    code = (
        "import sys, scripts.run_1822_ablation_readout, app.services.risk_metrics, "
        "app.services.strategy_recent_evidence; print('app.services.backtest_run' in sys.modules)"
    )
    result = subprocess.run([sys.executable, "-c", code], cwd=_REPO, capture_output=True, text=True, check=True)
    assert result.stdout.strip() == "False"


# --- Readout gate ---------------------------------------------------------


def test_refused_gate_never_reads_a_price(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("the price reader ran behind a refused gate")

    monkeypatch.setattr(script, "git_state", lambda: ("c" * 40, False))
    monkeypatch.setattr(script, "readout_gate", lambda _conn, *, dirty: "not_frozen")
    monkeypatch.setattr(reader, "load_series", forbidden)
    monkeypatch.setattr(script, "load_inputs", forbidden)
    looks = tmp_path / "looks.jsonl"
    assert script.run_readout(None, _FACT_DAY, looks_path=looks) == {"outcome": "refused", "reason": "not_frozen"}  # type: ignore[arg-type]
    assert not looks.exists()


def test_gate_refuses_a_dirty_tree_before_anything_else() -> None:
    assert script.readout_gate(None, dirty=True) == "dirty_tree"  # type: ignore[arg-type]


def test_gate_refuses_when_no_sidecar_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(terms, "load_sidecars", lambda *_a: ())
    assert script.readout_gate(None, dirty=False) == "no_sidecar_for_terms"  # type: ignore[arg-type]


def test_passing_gate_logs_the_look_before_reading(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    looks = tmp_path / "looks.jsonl"
    sidecar = terms.write_sidecar(terms.semantic_terms(), _facts(), tmp_path / "sc")
    gate = script.Gate(sidecar, 7, datetime(2026, 9, 29, tzinfo=UTC))
    monkeypatch.setattr(script, "git_state", lambda: ("c" * 40, False))
    monkeypatch.setattr(script, "readout_gate", lambda _conn, *, dirty: gate)

    def load(_conn: Any, _day: date) -> str:
        (record,) = [json.loads(line) for line in looks.read_text().splitlines()]
        assert record["trial_id"] == terms.identity(sidecar.sha256).trial_id
        return "no_witnessed_run"

    monkeypatch.setattr(script, "load_inputs", load)
    result = script.run_readout(None, _FACT_DAY, looks_path=looks)  # type: ignore[arg-type]
    assert result["outcome"] == "refused" and result["reason"] == "no_witnessed_run"


# --- Freeze ---------------------------------------------------------------


class _Conn:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


@pytest.fixture
def selected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[terms.Sidecar]:
    sidecar = terms.write_sidecar(terms.semantic_terms(), _facts(), tmp_path / "sc")
    monkeypatch.setattr(script, "_selected", lambda: sidecar)
    monkeypatch.setattr(reader, "execute", lambda _c, name, _p=None: [(datetime(2026, 9, 29, tzinfo=UTC),)])
    yield sidecar


def test_freeze_refuses_a_dirty_tree(monkeypatch: pytest.MonkeyPatch, selected: terms.Sidecar) -> None:
    monkeypatch.setattr(script, "git_state", lambda: ("c" * 40, True))
    code, record = script.freeze(_Conn(), dry_run=False)  # type: ignore[arg-type]
    assert (code, record["reason"]) == (1, "dirty_tree")


def test_freeze_retry_reports_already_frozen_identical(
    monkeypatch: pytest.MonkeyPatch, selected: terms.Sidecar
) -> None:
    import psycopg

    declaration = terms.build_declaration(selected)
    monkeypatch.setattr(script, "git_state", lambda: ("c" * 40, False))

    def unique(*_a: Any) -> int:
        raise psycopg.errors.UniqueViolation()

    monkeypatch.setattr(script, "freeze_preregistration", unique)
    stored = result_ledger.FrozenPreregistration(
        declaration_id=11, declaration=declaration, declaration_sha256=declaration.sha256, chain_declaration_ids=(11,)
    )
    monkeypatch.setattr(script, "load_preregistration", lambda *_a: stored)
    conn = _Conn()
    code, record = script.freeze(conn, dry_run=False)  # type: ignore[arg-type]
    assert (code, record["outcome"], record["declaration_id"]) == (0, "already_frozen_identical", 11)
    assert conn.rolled_back and conn.committed

    other = replace(stored, declaration_sha256="0" * 64)
    monkeypatch.setattr(script, "load_preregistration", lambda *_a: other)
    code, record = script.freeze(_Conn(), dry_run=False)  # type: ignore[arg-type]
    assert (code, record["outcome"]) == (1, "conflicting_declaration_already_frozen")


def test_freeze_dry_run_prints_the_full_digest_payload(
    monkeypatch: pytest.MonkeyPatch, selected: terms.Sidecar
) -> None:
    monkeypatch.setattr(script, "git_state", lambda: ("c" * 40, True))
    code, record = script.freeze(_Conn(), dry_run=True)  # type: ignore[arg-type]
    declaration = terms.build_declaration(selected)
    assert code == 0 and record["outcome"] == "dry_run"
    assert {k: record[k] for k in declaration.digest_payload} == declaration.digest_payload


# --- Readout composition (synthetic; never real prices before the freeze) ------------


def _series(sessions: list[date], closes: list[float], *, volume: str | None = "1000") -> reader.ReadSeries:
    bars = [
        Bar(
            price_date=day,
            open=Decimal(repr(close)),
            high=Decimal(repr(close * 1.01)),
            low=Decimal(repr(close * 0.99)),
            close=Decimal(repr(close)),
            volume=None if volume is None else Decimal(volume),
        )
        for day, close in zip(sessions, closes, strict=True)
    ]
    return reader.read_series(bars, "us_equity", as_of=date(2026, 12, 31))


def _stored(iid: int, quality: float, momentum: float) -> reader.StoredRow:
    row = ra.parse_score_row(
        iid,
        rank=1,
        family_scores={
            "quality": quality,
            "value": 0.5,
            "turnaround": 0.5,
            "momentum": momentum,
            "sentiment": 0.5,
            "confidence": 0.5,
        },
        raw_total=0.5,
        penalties_json=[],
    )
    assert isinstance(row, ra.ScoreRow)
    return reader.StoredRow(
        row=row, raw_total=0.5, total_score=None, value_from_thesis=True, confidence_from_thesis=True
    )


def _inputs(*, names: int = 10, volume: str | None = "1000") -> readout.Inputs:
    sessions = ra.nyse_sessions(date(2026, 7, 1), date(2026, 10, 30))
    window = [day for day in sessions if day <= date(2026, 10, 20)]
    runs = []
    rows: dict[datetime, dict[int, reader.StoredRow]] = {}
    for day in window[5:15]:
        scored = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=6)
        runs.append(ra.Run(scored_at=scored, known_at=scored + timedelta(minutes=1)))
        rows[scored] = {iid: _stored(iid, quality=iid / names, momentum=1 - iid / names) for iid in range(names)}
    series = {
        iid: _series(window, [10.0 * (1 + 0.001 * iid) ** k for k in range(len(window))], volume=volume)
        for iid in range(names)
    }
    population = reader.Population(
        runs=rows, excluded_rows={}, excluded_names={}, symbols={i: f"S{i}" for i in range(names)}
    )
    return readout.Inputs(
        population=population,
        run_map=ra.map_runs(runs, sessions),
        sessions=sessions,
        cutoff=date(2026, 10, 20),
        series=series,
        termination={},
    )


def test_readout_reports_every_evaluation_and_family() -> None:
    result = readout.readout(_inputs(), frozen_at=datetime(2027, 1, 1, tzinfo=UTC))
    assert list(result) == ["pooled", "prospective"]
    assert list(result["pooled"]) == list(terms.EVALUATIONS)
    canonical = result["pooled"]["canonical"]
    assert set(canonical["families"]) == set(ra.FAMILY_ORDER)
    assert canonical["active_formations"] == 10
    # Quality and momentum are exactly opposed; dropping either flips the arm.
    assert canonical["families"]["quality"]["symmetric_difference_max"] > 0
    assert canonical["families"]["value"]["symmetric_difference_max"] == 0
    assert result["prospective"]["canonical"]["refused"] == "empty_grid"


def test_verbatim_bar_rule_drops_null_volume_names_only_there() -> None:
    inputs = _inputs(volume=None)
    canonical = readout.evaluate(inputs, "canonical", prospective_after=None)
    assert canonical["active_formations"] == 10
    verbatim = readout.evaluate(inputs, "verbatim_bar_rule", prospective_after=None)
    assert verbatim["refused"] == "empty_grid"


def test_prospective_uses_only_runs_known_after_the_freeze() -> None:
    inputs = _inputs()
    known = sorted(run.known_at for run in inputs.run_map.entries.values())
    result = readout.evaluate(inputs, "canonical", prospective_after=known[4])
    assert result["active_formations"] == 5


def test_unknown_evaluation_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown evaluation"):
        readout.parse_evaluation("canonical@legacy_best")


def test_terminating_series_ends_at_its_last_bar() -> None:
    sessions = list(ra.nyse_sessions(date(2026, 8, 3), date(2026, 8, 7)))
    read = _series(sessions, [10.0, 11.0, 12.0, 13.0, 14.0])
    index = {day: ordinal for ordinal, day in enumerate(sessions)}
    assert readout.series_prices(read, index, terminal_class=TerminationClass.Q_SUFFIX_OTC).terminal_ordinal == 4
    assert readout.series_prices(read, index, terminal_class=TerminationClass.UNKNOWN).terminal_ordinal is None
    assert readout.series_prices(read, index, terminal_class=None).terminal_ordinal is None


def test_population_digest_moves_on_a_score_change_no_count_sees() -> None:
    row = (
        datetime(2026, 8, 3, tzinfo=UTC),
        1,
        1,
        0.5,
        0.4,
        0.3,
        0.2,
        0.1,
        0.5,
        0.35,
        0.35,
        [],
        "Stocks",
        "USD",
        "A",
        "",
    )
    changed = (*row[:3], 0.6, *row[4:])
    one, other = reader.build_population([row]), reader.build_population([changed])
    assert one.excluded_rows == other.excluded_rows
    assert one.rows_sha256 != other.rows_sha256


def test_gate_refuses_a_stored_row_that_no_longer_matches_its_digest(
    monkeypatch: pytest.MonkeyPatch, selected: terms.Sidecar
) -> None:
    declaration = terms.build_declaration(selected)
    tampered = replace(declaration, declared_by="someone else")
    stored = result_ledger.FrozenPreregistration(
        declaration_id=11, declaration=tampered, declaration_sha256=declaration.sha256, chain_declaration_ids=(11,)
    )
    monkeypatch.setattr(script, "load_preregistration", lambda *_a: stored)
    assert script.readout_gate(None, dirty=False) == "stored_row_does_not_match_its_digest"  # type: ignore[arg-type]


def test_look_record_names_the_calendar_cutoff(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    looks = tmp_path / "looks.jsonl"
    sidecar = terms.write_sidecar(terms.semantic_terms(), _facts(), tmp_path / "sc")
    monkeypatch.setattr(script, "git_state", lambda: ("c" * 40, False))
    monkeypatch.setattr(
        script, "readout_gate", lambda _conn, *, dirty: script.Gate(sidecar, 7, datetime(2026, 9, 29, tzinfo=UTC))
    )
    monkeypatch.setattr(script, "load_inputs", lambda _conn, _day: "no_witnessed_run")
    script.run_readout(None, _FACT_DAY, looks_path=looks)  # type: ignore[arg-type]
    (record,) = [json.loads(line) for line in looks.read_text().splitlines()]
    assert record["cutoff_c_k"] == "2026-09-22"


class _MainConn(_Conn):
    isolation_level: object = None
    read_only = False

    def __enter__(self) -> _MainConn:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


@pytest.mark.parametrize(
    ("argv", "target", "result", "expected"),
    [
        (["--terms"], "generate_terms", {"outcome": "refused", "reason": "no_witnessed_run"}, 1),
        (["--terms"], "generate_terms", {"outcome": "written"}, 0),
        (["--census"], "census", {"refused": "no witnessed run"}, 1),
        (["--census"], "census", {"grid": {"refused": "empty_grid"}}, 1),
        (["--census"], "census", {"grid": {}}, 0),
        (["--readout"], "run_readout", {"outcome": "refused", "reason": "not_frozen"}, 1),
        (["--readout"], "run_readout", {"outcome": "read"}, 0),
    ],
)
def test_main_exit_code_reflects_the_outcome(
    monkeypatch: pytest.MonkeyPatch, argv: list[str], target: str, result: dict[str, Any], expected: int
) -> None:
    monkeypatch.setattr(script.psycopg, "connect", lambda _url: _MainConn())
    monkeypatch.setattr(script, target, lambda _conn, _day: result)
    assert script.main(argv) == expected


@pytest.mark.parametrize(("code", "expected"), [(1, 1), (0, 0)])
def test_main_freeze_exit_code_is_the_freeze_code(monkeypatch: pytest.MonkeyPatch, code: int, expected: int) -> None:
    monkeypatch.setattr(script.psycopg, "connect", lambda _url: _MainConn())
    monkeypatch.setattr(script, "freeze", lambda _conn, *, dry_run: (code, {"outcome": "x"}))
    assert script.main(["--freeze"]) == expected
    assert script.main(["--freeze", "--dry-run"]) == expected
