"""#3609 step 2 slice 3c-iv: the report's verified, read-once artefact loader and per-formation scoring.

Fixtures only: nothing here reads a published artefact or the database.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import scripts.build_3609_factor_panel as builder
from app.services.factor_book import CHARACTERISTICS, UNCLASSIFIED, UNIVERSE_SIZE, BookRefusal
from app.services.factor_book_reference import Ff12Map, load_ff12
from app.services.factor_panel import PanelError
from app.services.factor_panel_artefact import gz_content_sha256, sha256_file, write_gz_lines
from app.services.factor_panel_prices import HoldingStatus
from scripts import report_3609_step2 as report

M1, M2 = "2019-01-31", "2019-02-28"
HOLDING = {M1: "2019-02", M2: "2019-03"}
SIGNS = {"gp_at": 1, "be_me": 1, "ni_me": 1, "ocf_me": 1, "at_gr1": -1}


@pytest.fixture(scope="module")
def ff12() -> Ff12Map:
    return load_ff12()


def _row(month: str, name: int, *, admitted: bool = True, close: str = "10.5", **over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "M": month,
        "s_M": month,
        "holding_month": HOLDING[month],
        "name_key": name,
        "series_id": 100 + name,
        "exclusion": None if admitted else "no_recent_evidence",
    }
    if admitted:
        row |= {
            "me": {"value": str(1e9 + name), "close": close},
            "sic": 3720,
            "sic_status": "sic",
            "characteristics": {c: {"value": 0.1 * (name % 7) + i} for i, c in enumerate(CHARACTERISTICS)},
            "prices": {
                "holding": {"status": "observed", "by_arm": {"best_case": 0.02, "worst_case": 0.01}},
            },
        }
    return row | over


def _artefact(
    tmp_path: Path,
    rows: list[dict[str, Any]],
    bars: list[list[Any]] | None = None,
    *,
    formations: tuple[str, ...] = (M1,),
    count: int | None = None,
) -> tuple[Path, str]:
    out = tmp_path / "artefact"
    signs_path = out / report.TABLE9_SIGNS
    signs_path.parent.mkdir(parents=True)
    signs_path.write_text("name,sign\n" + "".join(f"{k},{v}\n" for k, v in SIGNS.items()))
    if bars is None:
        # One bar per (series, session), the first row's close: a repeated series is the rows' fault, not the bars'.
        first = {(r["series_id"], r["s_M"]): r.get("me", {}).get("close", "10.5") for r in reversed(rows)}
        bars = [[sid, day, close] for (sid, day), close in sorted(first.items())]
    write_gz_lines(out / report.DECISION_BARS, bars)
    write_gz_lines(out / builder.ROWS_FILE, rows)
    (out / builder.CENSUS_FILE).write_text("{}\n")
    inputs = sorted(p for p in (out / "inputs").rglob("*") if p.is_file())
    manifest = {
        "schema": builder.MANIFEST_SCHEMA,
        "pinned_manifests": dict(builder.STAGE_A_PINS),
        "formations": list(formations),
        "inputs": {p.relative_to(out).as_posix(): sha256_file(p) for p in inputs},
        "rows": {
            "path": builder.ROWS_FILE,
            "count": len(rows) if count is None else count,
            "sha256": sha256_file(out / builder.ROWS_FILE),
            "content_sha256": gz_content_sha256(out / builder.ROWS_FILE),
        },
        "census": {"path": builder.CENSUS_FILE, "sha256": sha256_file(out / builder.CENSUS_FILE)},
    }
    (out / builder.MANIFEST_FILE).write_text(json.dumps(manifest))
    return out, sha256_file(out / builder.MANIFEST_FILE)


def _load(tmp_path: Path, ff12: Ff12Map, rows: list[dict[str, Any]], **kw: Any) -> list[report.PanelMonth]:
    out, digest = _artefact(tmp_path, rows, **kw)
    verified = builder.read_verified_artefact(out, digest, keep=report.CONSUMED_INPUTS)
    return report.read_panel(verified, ff12)


# --------------------------------------------------------------------------- read once (finding 149)


def test_the_bytes_checked_are_the_bytes_used(tmp_path: Path, ff12: Ff12Map) -> None:
    rows = [_row(M1, 1), _row(M1, 2, admitted=False)]
    out, digest = _artefact(tmp_path, rows)
    verified = builder.read_verified_artefact(out, digest, keep=report.CONSUMED_INPUTS)
    # Replaced after the check: the report parses the bytes it hashed, never the file now on disk.
    (out / builder.ROWS_FILE).unlink()
    write_gz_lines(out / builder.ROWS_FILE, [_row(M1, 9)])
    (out / report.DECISION_BARS).unlink()
    write_gz_lines(out / report.DECISION_BARS, [[101, M1, "99"]])
    [month] = report.read_panel(verified, ff12)
    assert set(month.admitted) == {1} and month.close == {1: 10.5, 2: 10.5}
    assert set(verified.files) == {*report.CONSUMED_INPUTS, builder.ROWS_FILE, builder.CENSUS_FILE}


def test_a_kept_input_must_be_listed(tmp_path: Path) -> None:
    out, digest = _artefact(tmp_path, [_row(M1, 1)])
    with pytest.raises(PanelError, match="kept inputs not in the manifest"):
        builder.read_verified_artefact(out, digest, keep=["inputs/absent.jsonl.gz"])


@pytest.mark.parametrize("target", [report.DECISION_BARS, builder.ROWS_FILE, builder.MANIFEST_FILE])
def test_a_replaced_file_refuses_before_any_parse(tmp_path: Path, target: str) -> None:
    out, digest = _artefact(tmp_path, [_row(M1, 1)])
    (out / target).write_bytes((out / target).read_bytes() + b"\0")
    with pytest.raises(PanelError, match="digest moved"):
        builder.read_verified_artefact(out, digest, keep=report.CONSUMED_INPUTS)


# --------------------------------------------------------------------------- the loader


def test_rows_become_admitted_names_with_closes_industries_and_signed_values(tmp_path: Path, ff12: Ff12Map) -> None:
    rows = [
        _row(M1, 1),
        _row(M1, 2, sic=None, sic_status="sic_unloaded"),
        _row(M1, 3, admitted=False),
        _row(M2, 1, close="11"),
    ]
    rows[0]["characteristics"]["ni_me"]["value"] = None
    rows[3]["prices"]["holding"] = {"status": "terminal", "by_arm": {"best_case": -0.5, "worst_case": -1.0}}
    first, second = _load(tmp_path, ff12, rows, formations=(M1, M2))
    assert (str(first.formation), str(first.session)) == (M1, M1)
    assert set(first.admitted) == {1, 2} and first.close == {1: 10.5, 2: 10.5, 3: 10.5}
    one = first.admitted[1]
    assert one.series_id == 101 and one.me == 1e9 + 1
    assert one.industry == ff12.industry(3720) and first.admitted[2].industry == UNCLASSIFIED
    assert set(one.signed) == set(CHARACTERISTICS) - {"ni_me"}
    assert one.signed["at_gr1"] == -rows[0]["characteristics"]["at_gr1"]["value"]  # Table 9 sign -1
    assert one.signed["gp_at"] == rows[0]["characteristics"]["gp_at"]["value"]
    assert second.admitted[1].holding.status is HoldingStatus.TERMINAL
    assert second.admitted[1].holding.by_arm == {"best_case": -0.5, "worst_case": -1.0}
    assert second.close == {1: 11.0}


def test_a_name_repeated_after_an_unpriced_excluded_row_refuses(tmp_path: Path, ff12: Ff12Map) -> None:
    # The excluded row has no bar, so it leaves no close and no admitted entry; the repeat must still refuse.
    rows = [_row(M1, 3, admitted=False), _row(M1, 3, series_id=900)]
    with pytest.raises(report.ReportError, match="name_key"):
        _load(tmp_path, ff12, rows, bars=[[900, M1, "10.5"]])


def test_an_unadmitted_name_without_a_bar_has_no_close(tmp_path: Path, ff12: Ff12Map) -> None:
    rows = [_row(M1, 1), _row(M1, 2, admitted=False)]
    [month] = _load(tmp_path, ff12, rows, bars=[[101, M1, "10.5"]])
    assert month.close == {1: 10.5}


def _mutate(index: int, change: Callable[[dict[str, Any]], None]) -> Callable[[list[dict[str, Any]]], None]:
    def apply(rows: list[dict[str, Any]]) -> None:
        change(rows[index])

    return apply


@pytest.mark.parametrize(
    ("mutate", "kw", "error", "match"),
    [
        pytest.param(
            lambda rows: rows.append(_row(M1, 1, series_id=900)), {}, report.ReportError, "name_key", id="repeated name"
        ),
        pytest.param(
            lambda rows: rows.append(_row(M1, 7, series_id=101)),
            {},
            report.ReportError,
            "series_id",
            id="repeated series",
        ),
        pytest.param(_mutate(0, lambda r: r["me"].update(value="0")), {}, BookRefusal, "ME_INVALID", id="zero ME"),
        pytest.param(_mutate(0, lambda r: r["me"].update(value=None)), {}, BookRefusal, "ME_INVALID", id="no ME"),
        pytest.param(
            _mutate(0, lambda r: r.update(s_M="2019-01-30")),
            {},
            report.ReportError,
            "two decision sessions",
            id="two sessions",
        ),
        pytest.param(
            _mutate(0, lambda r: r.update(holding_month="2019-03")),
            {},
            report.ReportError,
            "holding month",
            id="holding month",
        ),
        pytest.param(
            _mutate(0, lambda r: r.update(sic_status="sic_other")),
            {},
            report.ReportError,
            "sic_status",
            id="unknown sic status",
        ),
        pytest.param(
            _mutate(0, lambda r: r["prices"]["holding"]["by_arm"].pop("best_case")),
            {},
            report.ReportError,
            "arms",
            id="missing arm",
        ),
        pytest.param(lambda rows: None, {"count": 3}, report.ReportError, "rows read", id="row count"),
        pytest.param(
            lambda rows: None,
            {"formations": (M1, M2)},
            report.ReportError,
            "no rows for formations",
            id="empty formation",
        ),
        pytest.param(
            lambda rows: rows.append(_row(M2, 5)),
            {},
            report.ReportError,
            "not in the manifest",
            id="unlisted formation",
        ),
    ],
)
def test_the_loader_refuses(
    tmp_path: Path,
    ff12: Ff12Map,
    mutate: Callable[[list[dict[str, Any]]], None],
    kw: dict[str, Any],
    error: type[Exception],
    match: str,
) -> None:
    rows = [_row(M1, 1), _row(M1, 2)]
    mutate(rows)
    with pytest.raises(error, match=match):
        _load(tmp_path, ff12, rows, **kw)


@pytest.mark.parametrize(
    "bars",
    [
        pytest.param([[101, M1, "10.4"], [102, M1, "10.5"]], id="ME close differs from the bar"),
        pytest.param([[102, M1, "10.5"]], id="admitted name without a bar"),
        pytest.param([[101, M1, "10.5"], [101, M1, "10.5"], [102, M1, "10.5"]], id="repeated bar"),
    ],
)
def test_the_decision_bars_must_agree_with_the_rows(tmp_path: Path, ff12: Ff12Map, bars: list[list[Any]]) -> None:
    with pytest.raises(report.ReportError, match="decision bar|ME close"):
        _load(tmp_path, ff12, [_row(M1, 1), _row(M1, 2)], bars=bars)


# --------------------------------------------------------------------------- scoring one formation


def test_score_takes_the_top_universe_by_me_and_bands_its_composite(tmp_path: Path, ff12: Ff12Map) -> None:
    extra = 5
    rows = [_row(M1, name) for name in range(1, UNIVERSE_SIZE + extra + 1)]
    [month] = _load(tmp_path, ff12, rows)
    scored = report.score(month)
    # ME rises with name_key in the fixture, so the five smallest names fall outside the universe.
    assert scored.universe == tuple(range(UNIVERSE_SIZE + extra, extra, -1))
    assert set(scored.bands.order) <= set(scored.universe)
    n = len(scored.bands.order)
    assert n == UNIVERSE_SIZE  # one industry, every name with all five values
    assert len(scored.bands.decile) == -(-n // 10) and len(scored.bands.tercile) == -(-n // 3)
