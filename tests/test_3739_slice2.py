"""#3739 slice 2: K and G calibration and U pass 1 (``scripts/build_3739_slice2.py``)."""

from __future__ import annotations

import io
import json
import zipfile
from datetime import date
from pathlib import Path

import pytest

from scripts import build_3739_slice2 as s2

F = date(2024, 7, 31)


def row(m: str, key: int, cik: str | None, me: str | None, exclusion: str | None = None) -> dict[str, object]:
    out: dict[str, object] = {"M": m, "name_key": key, "exclusion": exclusion, "cik": cik}
    if me is not None:
        out["me"] = {"value": me}
    return out


def formation(*securities: tuple[int, str, float]) -> s2.Formation:
    return {key: s2.Security(key, cik, me) for key, cik, me in securities}


def test_admitted_keeps_unexcluded_rows_and_refuses_an_unusable_one() -> None:
    panel = s2.admitted([row("2024-07-31", 1, "A", "10"), row("2024-07-31", 2, "B", "5", "reit")])
    assert panel == {F: {1: s2.Security(1, "A", 10.0)}}
    with pytest.raises(ValueError, match="no usable ME"):
        s2.admitted([row("2024-07-31", 1, "A", "0")])
    with pytest.raises(ValueError, match="no usable ME"):
        s2.admitted([row("2024-07-31", 1, None, "10")])


def test_issuer_rank_is_its_largest_security_and_ties_break_by_name_key() -> None:
    f = formation((3, "A", 5.0), (1, "B", 9.0), (2, "A", 9.0), (4, "C", 1.0))
    # Order: key 1 (9, B), key 2 (9, A), key 3 (5, A), key 4 (1, C).
    assert s2.issuer_ranks(f) == {"B": 1, "A": 2, "C": 4}


def test_outside_k_share_excludes_issuers_absent_at_the_start(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(s2, "UNIVERSE_SIZE", 3)
    start = formation((1, "A", 10.0), (2, "B", 5.0), (3, "C", 1.0))
    # D is new at the end and drops out of both sides; C (rank 3 at start) is the only issuer outside K = 2.
    end = formation((1, "A", 10.0), (3, "C", 6.0), (5, "D", 50.0), (2, "B", 0.5))
    total, shares = s2.outside_k_shares(start, end, (2, 3))
    assert total == 16.0
    assert shares == {2: 6.0 / 16.0, 3: 0.0}


def test_choose_k_takes_the_smallest_passing_candidate_else_the_largest() -> None:
    def pairs(*worst: float) -> list[dict[str, object]]:
        return [{"shares": dict(zip(map(str, s2.K_CANDIDATES), worst, strict=True))}]

    assert s2.choose_k(pairs(0.01, 0.0025, 0.001, 0.0))[0] == 2000
    assert s2.choose_k(pairs(0.01, 0.01, 0.01, 0.01))[0] == 3000


def test_k_pairs_stop_at_24_months_and_at_the_last_formation() -> None:
    months = [date(2000 + i, 1, 31) for i in range(30)]  # only the order matters
    panel = {m: formation((1, "A", 1.0)) for m in months}
    pairs = list(s2.k_pairs(panel, months[0], months[-1]))
    assert len(pairs) == sum(min(24, 29 - i) for i in range(30))
    assert max(p["h"] for p in pairs) == 24


def test_quantile_matches_percentile_cont() -> None:
    assert s2.quantile([1.0, 2.0, 3.0, 4.0], 0.5) == 2.5
    assert s2.quantile([1.0, 2.0, 3.0, 4.0], 1.0) == 4.0
    assert s2.quantile([7.0], 0.999) == 7.0


def test_g_uses_both_directions_and_only_securities_admitted_at_both_ends() -> None:
    m = [date(2024, 5, 31), date(2024, 6, 30), date(2024, 7, 31)]
    panel = {
        m[0]: formation((1, "A", 10.0), (2, "B", 10.0)),
        m[1]: formation((1, "A", 40.0), (2, "B", 5.0)),
        m[2]: formation((1, "A", 20.0)),  # B terminated after m[1]: its pairs end there
    }
    table = s2.g_table(panel, (1, 2))
    assert table[1]["pairs"] == 3  # A m0->m1 (4), B m0->m1 (2), A m1->m2 (2)
    assert table[2] == {"pairs": 1, "G": 2.0}


def zip_of(members: dict[str, object]) -> zipfile.ZipFile:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        for name, payload in members.items():
            zf.writestr(name, json.dumps(payload))
    return zipfile.ZipFile(buffer)


def columns(*filings: tuple[str, str, str, str]) -> dict[str, list[str]]:
    return {
        "accessionNumber": [f[0] for f in filings],
        "form": [f[1] for f in filings],
        "acceptanceDateTime": [f[2] for f in filings],
        "items": [f[3] for f in filings],
    }


def test_extract_reads_main_files_and_overflow_pages_and_filters_form_and_date() -> None:
    zf = zip_of(
        {
            "CIK0000000001.json": {
                "filings": {
                    "recent": columns(
                        ("a1", "8-K", "2024-05-01T14:00:00.000Z", "5.03,9.01"),
                        ("a2", "10-Q", "2024-05-01T14:00:00.000Z", ""),
                        ("a3", "8-K", "2024-04-01T02:00:00.000Z", "5.06"),  # 2024-03-31 in New York
                    )
                }
            },
            "CIK0000000001-submissions-001.json": columns(("a4", "8-A12B", "2024-06-03T20:00:00.000Z", "")),
        }
    )
    got = list(s2.extract_filings(zf))
    assert [(f.accession, f.items) for f in got] == [("a4", ()), ("a1", ("5.03", "9.01"))]
    with pytest.raises(ValueError, match="unexpected"):
        list(s2.extract_filings(zip_of({"README.txt": {}})))


def filing(form: str, accepted: str, items: tuple[str, ...] = (), cik: str = "X") -> s2.Filing:
    return s2.Filing(cik, f"{cik}-{form}-{accepted}", form, accepted, items)


@pytest.mark.parametrize(
    ("form", "accepted", "items", "expected"),
    [
        ("8-A12B", "2024-04-30T20:00:00.000Z", (), False),  # 2024-04-30 New York: F - 92 days, excluded
        ("8-A12B", "2024-05-01T14:00:00.000Z", (), True),
        ("10-12B", "2026-07-31T21:00:00.000Z", (), True),  # 17:00 New York on the last day
        ("8-K12B", "2026-08-01T14:00:00.000Z", (), False),
        ("8-K", "2025-01-02T14:00:00.000Z", ("5.06",), True),
        ("8-K", "2025-01-02T14:00:00.000Z", ("5.03",), False),
        ("8-K/A", "2025-01-02T14:00:00.000Z", ("5.06",), False),
        ("8-A12G", "2025-01-02T14:00:00.000Z", (), False),
    ],
)
def test_entrant_filing_window_and_forms(form: str, accepted: str, items: tuple[str, ...], expected: bool) -> None:
    assert s2.is_entrant_filing(filing(form, accepted, items), F) is expected


def test_pass1_bases() -> None:
    a, b, c, d, e = (f"000000000{i}" for i in range(1, 6))
    base = formation((1, a, 30.0), (2, b, 20.0), (3, c, 10.0))
    filings = [
        filing("8-A12B", "2025-01-02T14:00:00.000Z", cik=c),  # admitted at F: not an entrant
        filing("8-A12B", "2025-01-02T14:00:00.000Z", cik=d),
    ]
    u = s2.pass1(base, F, 2, filings, [(c, "r1"), (e, "r2")])
    assert set(u) == {a, b, c, d, e}
    assert u[a] == {"incumbent": ["1"]}
    assert u[c] == {"terminated": ["r1"]}
    assert set(u[d]) == {"entrant"}


def test_register_reader_returns_cik_and_accession(tmp_path: Path) -> None:
    path = tmp_path / "register.csv"
    path.write_text(",".join(s2.REGISTER_COLUMNS) + "\n0000000002,0000000002-24-000001,25-NSE,2024-08-01,(b)\n")
    assert s2.read_register(path) == [("0000000002", "0000000002-24-000001")]


def test_pass1_refuses_a_cik_that_is_not_ten_digits() -> None:
    base = formation((1, "0000000001", 30.0))
    with pytest.raises(ValueError, match="not 10-digit"):
        s2.pass1(base, F, 1, [], [("2", "r1")])


def test_calibrated_k_checks_the_artefact_pins_and_the_choice() -> None:
    pairs = [{"shares": {"1500": 0.001, "2000": 0.0, "2500": 0.0, "3000": 0.0}}]
    good = {
        "artefacts": {"stage_a": {"manifest_sha256": s2.STAGE_A[1]}, "stage_b": {"manifest_sha256": s2.STAGE_B[1]}},
        "K": {"chosen": 1500, "pairs": pairs},
    }
    assert s2.calibrated_k(good) == 1500
    with pytest.raises(ValueError, match="other artefacts"):
        s2.calibrated_k({**good, "artefacts": {**good["artefacts"], "stage_b": {"manifest_sha256": "x"}}})
    with pytest.raises(ValueError, match="not the one its pairs give"):
        s2.calibrated_k({**good, "K": {"chosen": 2000, "pairs": pairs}})
