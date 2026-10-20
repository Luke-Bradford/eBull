"""#2327 — rule v1 of the 13D/G CUSIP bridge, on constructed inputs (spec "Construction" step 5).

Pure: no DB. The DB path (insert, provenance, conflict, race, re-read) is ``test_cusip_bridge_13dg_db.py``.

CUSIPs are built with :func:`cusip_check_digit`, whose own expectations are hand-transcribed in
``test_cusip_check_digit.py``; here it only constructs valid inputs. Issuer numbers (characters 1–6) are chosen so each
test controls which CUSIPs share one.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

import pytest

from app.services.cusip_bridge_13dg import BridgeInputs, decide, filing_maps
from app.services.cusip_check_digit import cusip_check_digit
from app.services.sec_13f_securities_list import WindowList

IID = 7001
CIK = "0000320193"


def _c(first_eight: str) -> str:
    return first_eight + str(cusip_check_digit(first_eight))


A_OLD = _c("11111110")  # issuer 111111, issue 10
A_NEW = _c("11111120")  # issuer 111111, issue 20
A_CLB = _c("11111130")  # issuer 111111, issue 30
B_OTHER = _c("22222210")  # issuer 222222


def _lists(*quarters: Iterable[str] | None) -> list[WindowList]:
    """One WindowList per argument, oldest first; ``None`` is a gap."""
    out: list[WindowList] = []
    for n, rows in enumerate(quarters):
        q = f"q{n}"
        if rows is None:
            out.append(WindowList(q, f"sha{n}", frozenset(), "duplicate_payload"))
        else:
            out.append(WindowList(q, f"sha{n}", frozenset(rows), None))
    return out


def _inputs(
    lists: list[WindowList],
    *,
    filings: Mapping[str, Iterable[str]] | None = None,
    cik_instruments: Mapping[str, tuple[int, ...]] | None = None,
    rows: Mapping[str, Iterable[tuple[str, int]]] | None = None,
) -> BridgeInputs:
    filings = filings if filings is not None else {A_NEW: [CIK]}
    rows = rows or {}
    instrument_cusips: dict[int, set[str]] = {}
    for cusip, pairs in rows.items():
        for _, iid in pairs:
            instrument_cusips.setdefault(iid, set()).add(cusip)
    return BridgeInputs(
        filing_ciks={c: frozenset(k) for c, k in filings.items()},
        cik_instruments=cik_instruments if cik_instruments is not None else {CIK: (IID,)},
        cusip_rows={c: frozenset(p) for c, p in rows.items()},
        instrument_cusips={i: frozenset(c) for i, c in instrument_cusips.items()},
        lists=lists,
    )


def _outcome(inputs: BridgeInputs, cusip: str = A_NEW) -> str:
    return decide(inputs)[cusip].outcome


class TestNormalisation:
    def test_trailing_newline_and_case_are_normalised_before_r1(self) -> None:
        ciks, accessions = filing_maps(
            [(A_NEW.lower() + "\n", " 320193 ", "0001-26-1"), (A_NEW, "320193", "0001-26-2")]
        )
        assert ciks == {A_NEW: frozenset({CIK})}
        assert accessions == {A_NEW: ("0001-26-1", "0001-26-2")}

    def test_blank_cusip_is_dropped(self) -> None:
        assert filing_maps([("  ", CIK, "a")]) == ({}, {})

    def test_non_numeric_cik_is_kept_as_written(self) -> None:
        ciks, _ = filing_maps([(A_NEW, "N/A", "a")])
        assert ciks == {A_NEW: frozenset({"N/A"})}


class TestR1toR4:
    def test_non_ascii_digit_fails_r1(self) -> None:
        bad = A_NEW[:8] + "٣"  # ARABIC-INDIC DIGIT THREE: isdigit() is True, not a CUSIP character
        assert _outcome(_inputs(_lists([bad]), filings={bad: [CIK]}), bad) == "R1"

    @pytest.mark.parametrize("value", ["US50101L1061", "MULTIPLES", A_NEW[:8] + str((int(A_NEW[8]) + 1) % 10)])
    def test_wrong_shape_or_check_digit_fails_r1(self, value: str) -> None:
        assert _outcome(_inputs(_lists([value]), filings={value: [CIK]}), value) == "R1"

    def test_two_ciks_fail_r2(self) -> None:
        assert _outcome(_inputs(_lists([A_NEW]), filings={A_NEW: [CIK, "0000000002"]})) == "R2"

    def test_cik_without_instrument_fails_r3(self) -> None:
        assert _outcome(_inputs(_lists([A_NEW]), cik_instruments={})) == "R3_none"

    def test_cik_with_two_instruments_fails_r3(self) -> None:
        assert _outcome(_inputs(_lists([A_NEW]), cik_instruments={CIK: (IID, IID + 1)})) == "R3_multi"

    def test_not_on_any_available_list_fails_r4(self) -> None:
        assert _outcome(_inputs(_lists([B_OTHER], [B_OTHER]))) == "R4"

    def test_latest_list_gap_refuses_to_decide(self) -> None:
        with pytest.raises(ValueError):
            decide(_inputs(_lists([A_NEW], None)))


class TestOutcomes:
    def test_unmapped_candidate_binds(self) -> None:
        d = decide(_inputs(_lists([A_NEW])))[A_NEW]
        assert (d.outcome, d.instrument_id, d.issuer_cik) == ("bind", IID, CIK)

    def test_openfigi_row_on_same_instrument_confirms(self) -> None:
        assert _outcome(_inputs(_lists([A_NEW]), rows={A_NEW: [("openfigi", IID)]})) == "confirm"

    def test_sec_row_on_same_instrument_is_noop(self) -> None:
        assert _outcome(_inputs(_lists([A_NEW]), rows={A_NEW: [("sec", IID)]})) == "noop"

    def test_row_on_another_instrument_fails_r6(self) -> None:
        assert _outcome(_inputs(_lists([A_NEW]), rows={A_NEW: [("openfigi", IID + 1)]})) == "R6"


class TestAmbiguity:
    def test_succession_co_listed_once_and_old_gone_from_latest_binds(self) -> None:
        lists = _lists([A_OLD], [A_OLD, A_NEW], [A_NEW])
        assert _outcome(_inputs(lists, rows={A_OLD: [("sec", IID)]})) == "bind"

    def test_dual_class_co_listed_twice_fails_r5a(self) -> None:
        lists = _lists([A_OLD, A_NEW], [A_OLD, A_NEW], [A_NEW])
        assert _outcome(_inputs(lists, rows={A_OLD: [("sec", IID)]})) == "R5a"

    def test_both_on_latest_list_fails_r5a(self) -> None:
        lists = _lists([A_NEW], [A_OLD, A_NEW])
        assert _outcome(_inputs(lists, rows={A_OLD: [("sec", IID)]})) == "R5a"

    def test_r5a_refuses_every_candidate_of_the_instrument(self) -> None:
        lists = _lists([A_OLD, A_NEW], [A_OLD, A_NEW])
        out = decide(_inputs(lists, filings={A_OLD: [CIK], A_NEW: [CIK]}))
        assert {out[A_OLD].outcome, out[A_NEW].outcome} == {"R5a"}

    def test_gap_next_to_a_single_co_listing_fails_r5a(self) -> None:
        # Co-listed once (q0); q1 is a gap whose nearest available neighbour q0 co-lists the pair, so it counts.
        lists = _lists([A_OLD, A_NEW], None, [A_NEW])
        assert _outcome(_inputs(lists, rows={A_OLD: [("sec", IID)]})) == "R5a"

    def test_gap_not_next_to_the_co_listing_does_not_count(self) -> None:
        lists = _lists([A_OLD, A_NEW], [A_NEW], None, [A_NEW])
        assert _outcome(_inputs(lists, rows={A_OLD: [("sec", IID)]})) == "bind"

    def test_unheld_same_issuer_class_fails_r5b(self) -> None:
        lists = _lists([A_NEW, A_CLB], [A_NEW, A_CLB])
        assert _outcome(_inputs(lists)) == "R5b"

    def test_unheld_class_of_another_issuer_number_is_not_r5b(self) -> None:
        lists = _lists([A_NEW, B_OTHER], [A_NEW, B_OTHER])
        assert _outcome(_inputs(lists)) == "bind"

    def test_new_issuer_number_on_a_mapped_instrument_fails_r5c(self) -> None:
        lists = _lists([B_OTHER], [A_NEW])
        assert _outcome(_inputs(lists, rows={B_OTHER: [("sec", IID)]})) == "R5c"

    def test_r6_precedes_r5(self) -> None:
        lists = _lists([A_OLD, A_NEW], [A_OLD, A_NEW])
        rows = {A_OLD: [("sec", IID)], A_NEW: [("openfigi", IID + 1)]}
        assert _outcome(_inputs(lists, rows=rows)) == "R6"


def test_each_candidate_keeps_its_own_cik_when_two_ciks_map_to_one_instrument() -> None:
    old_cik = "0000000099"  # a superseded CIK row kept on the same instrument
    inputs = _inputs(
        _lists([A_NEW, A_OLD]),
        filings={A_NEW: [CIK], A_OLD: [old_cik]},
        cik_instruments={CIK: (IID,), old_cik: (IID,)},
    )
    out = decide(inputs)
    assert (out[A_NEW].issuer_cik, out[A_OLD].issuer_cik) == (CIK, old_cik)
