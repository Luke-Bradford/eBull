"""#3609 step 1 slice 1: Table 9 signs and FSDS SUB parsing."""

from __future__ import annotations

import io
import zipfile
from datetime import datetime

import pytest

from app.services.factor_panel_reference import (
    KNOWN_TABLE9_DIRECTION_CONFLICTS,
    ReferenceInputError,
    fsds_sub_quarters,
    jkp_directions,
    load_table9_signs,
    parse_fsds_sub,
    table9_direction_conflicts,
)

#: Spec §"Source rule: JKP": the eight step-1 characteristics.
STEP1_CHARACTERISTICS = ("gp_at", "be_me", "ope_be", "ni_me", "ocf_me", "at_gr1", "ret_12_1", "rvol_21d")


def _jkp_zip(rows: list[tuple[str, str]]) -> bytes:
    text = "location,name,freq,weighting,direction,n_stocks,n_stocks_min,date,ret\n" + "".join(
        f"usa,{name},monthly,vw_cap,{direction},500,8,2020-01-31,0.01\n" for name, direction in rows
    )
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("data.csv", text)
    return output.getvalue()


def test_committed_table9_csv_covers_every_jkp_factor_and_the_step1_signs() -> None:
    signs = load_table9_signs()
    assert len(signs) == 153  # JKP's 153 US factors, one Table 9 row each
    # Table 9 as printed; the long leg is the tercile the original paper finds earns more.
    assert {name: signs[name] for name in STEP1_CHARACTERISTICS} == {
        "gp_at": 1,
        "be_me": 1,
        "ope_be": 1,
        "ni_me": 1,
        "ocf_me": 1,
        "at_gr1": -1,
        "ret_12_1": 1,
        "rvol_21d": -1,
    }
    assert KNOWN_TABLE9_DIRECTION_CONFLICTS <= set(signs)
    assert not KNOWN_TABLE9_DIRECTION_CONFLICTS & set(STEP1_CHARACTERISTICS)


def test_direction_conflicts_are_named_and_factor_sets_must_match() -> None:
    directions = jkp_directions(_jkp_zip([("gp_at", "1"), ("gp_at", "1"), ("at_gr1", "1")]))
    assert directions == {"gp_at": 1, "at_gr1": 1}
    assert table9_direction_conflicts({"gp_at": 1, "at_gr1": -1}, directions) == {"at_gr1"}
    with pytest.raises(ReferenceInputError, match="factor sets differ"):
        table9_direction_conflicts({"gp_at": 1}, directions)


def test_a_factor_with_two_directions_is_refused() -> None:
    with pytest.raises(ReferenceInputError, match="direction values"):
        jkp_directions(_jkp_zip([("gp_at", "1"), ("gp_at", "-1")]))


_SUB_HEADER = "adsh\tcik\tname\tsic\tform\tperiod\tfiled\taccepted\n"


def _sub(*rows: str) -> bytes:
    return (_SUB_HEADER + "".join(rows)).encode("latin-1")


def test_sub_parser_keeps_null_sic_and_counts_acceptance_outside_the_quarter() -> None:
    parsed = parse_fsds_sub(
        _sub(
            "0001521065-12-000005\t1521065\tDILMAX CORP.\t7200\t10-Q\t20111130\t20120118\t2012-01-18 13:34:00.0\n",
            "0000950123-12-000001\t320193\tAPPLE INC\t\t10-K\t20111231\t20120330\t2012-03-30 17:01:02.0\n",
            "0000950123-12-000002\t1000\tREIT ONE\t6798\t10-K/A\t20111231\t20120402\t2012-04-02 09:00:00.0\n",
        ),
        quarter="2012q1",
    )
    assert [(r.cik, r.sic, r.form) for r in parsed.records] == [
        (1521065, 7200, "10-Q"),
        (320193, None, "10-K"),
        (1000, 6798, "10-K/A"),
    ]
    assert parsed.records[0].accepted == datetime(2012, 1, 18, 13, 34)
    assert (parsed.sic_null, parsed.accepted_outside_quarter) == (1, 1)
    assert parsed.forms == {"10-Q": 1, "10-K": 1, "10-K/A": 1}


@pytest.mark.parametrize(
    ("rows", "match"),
    [
        (("000152106512000005\t1\tX\t7200\t10-Q\t\t\t2012-01-18 13:34:00.0\n",), "adsh"),
        (
            (
                "0001521065-12-000005\t1\tX\t7200\t10-Q\t\t\t2012-01-18 13:34:00.0\n",
                "0001521065-12-000005\t1\tX\t7200\t10-Q\t\t\t2012-01-18 13:34:00.0\n",
            ),
            "repeated adsh",
        ),
        (("0001521065-12-000005\t1\tX\t72\t10-Q\t\t\t2012-01-18 13:34:00.0\n",), "3-4 digit"),
        (("0001521065-12-000005\tABC\tX\t7200\t10-Q\t\t\t2012-01-18 13:34:00.0\n",), "invalid cik"),
        (("0001521065-12-000005\t1\tX\t7200\t10-Q\t\t\t2012-01-18\n",), "accepted"),
        (("0001521065-12-000005\t1\tX\t7200\t10-Q\t\t2012-01-18 13:34:00.0\n",), "fields"),
        ((), "no rows"),
    ],
)
def test_sub_parser_refuses_drift(rows: tuple[str, ...], match: str) -> None:
    with pytest.raises(ReferenceInputError, match=match):
        parse_fsds_sub(_sub(*rows), quarter="2012q1")


def test_sub_parser_requires_its_columns() -> None:
    with pytest.raises(ReferenceInputError, match=r"lacks columns \['sic'\]"):
        parse_fsds_sub(b"adsh\tcik\tform\taccepted\n", quarter="2012q1")


def test_stage_a_sub_quarters_run_2012q1_to_2021q2() -> None:
    quarters = fsds_sub_quarters()
    assert (quarters[0], quarters[-1], len(quarters)) == ("2012q1", "2021q2", 38)
    assert quarters[3:5] == ("2012q4", "2013q1")
