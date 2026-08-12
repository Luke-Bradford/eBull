"""#2282 stage 2b — symbol-resolution policy for the research-corpus ingest.

Pure tests, no database. The resolution policy is where this ingest can go
quietly wrong, and every failure mode below was found by measuring the actual
dev universe rather than imagined:

* 558 ``.RTH`` and 9 ``.24-7`` rows are eToro VENUE VARIANTS of a company that
  already has an instrument row. Resolving deep history onto one of those
  attaches the corpus to the wrong row AND consumes the
  ``uq_research_price_series_vendor_instrument`` slot the real instrument needs.
* 224 ``.US`` rows are the only row for their company (``ABT.US`` is Abbott;
  there is no ``ABT``), so that suffix must be stripped, not skipped.
* The archive spells share classes Yahoo's way — ``BRK-A`` where we write
  ``BRK.B``.
* 27 keys still collide after all of that. They stay UNRESOLVED: an ambiguous
  join recorded as ``symbol_exact`` would be a lie about its own evidence, and
  sql/249's whole point is that an unresolved series is a measurement.

The schema invariants themselves live in ``test_research_corpus_schema.py``.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.services.research_corpus_ingest import (
    HF_ARCHIVE,
    INTRADER_ARCHIVE,
    Form25Match,
    archive_symbol_candidates,
    classify_form25_match,
    index_instruments,
    normalise_vendor_symbol,
    parse_intrader_rows,
    resolve_archive_symbol,
)


@pytest.mark.parametrize(
    ("vendor_symbol", "expected"),
    [
        ("AAPL", "AAPL"),
        ("aapl", "AAPL"),
        (" AAPL ", "AAPL"),
        # Yahoo spells a share class with a hyphen; we spell it with a dot.
        ("BRK-A", "BRK.A"),
        ("BRK-B", "BRK.B"),
        ("BF-B", "BF.B"),
    ],
)
def test_normalise_vendor_symbol(vendor_symbol: str, expected: str) -> None:
    assert normalise_vendor_symbol(vendor_symbol) == expected


def test_venue_variants_never_win_the_key() -> None:
    """``AAPL.RTH`` and ``AAPL.24-7`` are the same company on a different session.

    Order matters here: the variants are listed FIRST so a naive
    last-write-wins index would resolve ``AAPL`` onto instrument 8754 (the RTH
    row) instead of 1001.
    """
    index, ambiguous = index_instruments(
        [
            (8754, "AAPL.RTH"),
            (15569, "AAPL.24-7"),
            (1001, "AAPL"),
        ]
    )
    assert index == {"AAPL": 1001}
    assert ambiguous == set()


def test_us_suffix_is_stripped_not_skipped() -> None:
    """``ABT.US`` is Abbott's only instrument row — skipping it loses the name."""
    index, ambiguous = index_instruments([(1552, "ABT.US"), (9001, "ABT.RTH")])
    assert index == {"ABT": 1552}
    assert ambiguous == set()


def test_share_class_dots_are_preserved() -> None:
    """``.B`` is part of the ticker; only the venue suffixes are meaningful."""
    index, _ = index_instruments([(1118, "BRK.B")])
    assert index[normalise_vendor_symbol("BRK-B")] == 1118


def test_colliding_key_resolves_to_nothing() -> None:
    """Two instruments claiming one key leaves BOTH unresolved.

    Picking one would record ``resolution_method = 'symbol_exact'`` against a
    join that was in fact a coin flip. Unresolved is a measurement; a wrong
    resolution is a corrupted one, and it is silent.
    """
    index, ambiguous = index_instruments([(10, "ABT"), (11, "ABT.US")])
    assert "ABT" not in index
    assert ambiguous == {"ABT"}


def test_same_instrument_listed_twice_is_not_a_collision() -> None:
    """Idempotence: the same (id, symbol) pair repeated is not ambiguity."""
    index, ambiguous = index_instruments([(10, "ABT"), (10, "ABT")])
    assert index == {"ABT": 10}
    assert ambiguous == set()


# ---------------------------------------------------------------------------
# #2297 — Form 25 delisting-link policy
# ---------------------------------------------------------------------------
#
# The three cases below are the whole policy, and TWO OF THEM ARE REFUSALS.
# That ratio is the finding: measured on the full 2023 register, `(b)` — the
# provision where truncating a series is unambiguously right — states a
# suspension date on 0 of 105 cohort rows, so `no_suspension` is the normal
# outcome and `write` is the exception. A test suite that only exercised the
# happy path would describe a guard that does not exist.


def _match(**overrides: object) -> Form25Match:
    """A match that WRITES unless an override makes it refuse.

    ⚠ Deliberately not a "neutral" baseline: this fixture is the write case,
    stated as such. A baseline that the classifier already refuses would make
    every override look like it worked (test-quality.md, "a neutral fixture is
    not neutral if the thing under test classifies it").
    """
    base: dict[str, object] = {
        "symbol": "LIN",
        "first_bar": date(1992, 6, 17),
        "last_bar": date(2026, 7, 7),
        "earliest_filed": date(2023, 3, 2),
        "provision_variants": 1,
        "provision": "(a)(3)",
        "suspension_variants": 1,
        "suspension_date": date(2023, 3, 2),
    }
    base.update(overrides)
    return Form25Match(**base)  # type: ignore[arg-type]


def test_stated_suspension_date_is_written() -> None:
    # LIN, verified against SEC EDGAR direct: the EX-99 for accession
    # 0000876661-23-000160 names THREE dates — removal-effective March 13,
    # operation-of-law March 01, "suspended from trading on March 02, 2023".
    # 2023-03-02 is the one that truncates correctly, per §2.6 trap 5.
    assert classify_form25_match(_match()) == "write"


def test_absent_suspension_date_is_never_backfilled_from_filed_date() -> None:
    # The filing date is RIGHT THERE on the match and is a different event.
    # Substituting it is the failure mode sql/249's CHECK pair exists to stop.
    match = _match(suspension_date=None)
    assert match.earliest_filed is not None
    assert classify_form25_match(match) == "no_suspension"


def test_disagreeing_filings_are_left_null_rather_than_tie_broken() -> None:
    assert classify_form25_match(_match(suspension_variants=2)) == "conflict"
    assert classify_form25_match(_match(provision_variants=2, provision=None)) == "conflict"


def test_conflict_outranks_the_missing_date_branch() -> None:
    # ⚠ The discriminating case, and the reason it is written with
    # suspension_date=None: with the two branches in the other order this
    # returns "no_suspension", which would UNDERCOUNT conflicts by silently
    # filing them as ordinary missing dates. Probed by swapping the branches —
    # this assertion fails, the other four still pass.
    assert classify_form25_match(_match(provision_variants=2, suspension_date=None)) == "conflict"


def test_provision_does_not_change_the_write_decision() -> None:
    # ⚠ The classifier is provision-BLIND on purpose. Storing the date is
    # always right (it is a true fact about the security); it is TRUNCATING on
    # it that must be provision-aware, which is why sql/253 carries the
    # provision alongside and nothing in this module truncates.
    assert classify_form25_match(_match(provision="(b)")) == "write"
    assert classify_form25_match(_match(provision="(a)(3)")) == "write"


# ---------------------------------------------------------------------------
# #2597 — the Q-suffix bankruptcy rule, and the Intrader CSV reader
# ---------------------------------------------------------------------------
#
# The strip is a SOURCE RULE, not a lookup convenience: a Form 25 cover page
# carries the POST-bankruptcy ticker while every price archive keys the
# pre-bankruptcy one, so resolving without it loses precisely the bankruptcies
# and keeps the acquisitions. That biases the corpus along the exact axis a
# survivorship-free corpus exists to protect.


def test_exact_match_outranks_the_q_strip() -> None:
    """``NHIQ`` is a real archive symbol. A blind strip would rebind it."""
    assert resolve_archive_symbol("NHIQ", {"NHIQ", "NHI"}) == "NHIQ"


def test_q_strip_recovers_a_bankruptcy_the_archive_keys_pre_filing() -> None:
    assert resolve_archive_symbol("BBBYQ", {"BBBY"}) == "BBBY"
    assert resolve_archive_symbol("YELLQ", {"YELL"}) == "YELL"


def test_q_strip_never_fires_on_a_non_bankruptcy_symbol() -> None:
    """Only a trailing ``Q`` licenses the strip — no other suffix does."""
    assert resolve_archive_symbol("SNAP", {"SNA"}) is None
    assert resolve_archive_symbol("Q", {""}) is None


def test_separator_variants_are_tried_both_ways() -> None:
    assert resolve_archive_symbol("BRK.A", {"BRK-A"}) == "BRK-A"
    assert resolve_archive_symbol("BRK-A", {"BRK.A"}) == "BRK.A"


def test_unserved_symbol_resolves_to_none_rather_than_a_guess() -> None:
    assert resolve_archive_symbol("MNKTQ", {"AAPL", "MSFT"}) is None


def test_candidate_order_puts_every_exact_spelling_before_any_stripped_one() -> None:
    """Precedence is the whole rule; a set would lose it."""
    candidates = archive_symbol_candidates("BRK.AQ")
    assert candidates.index("BRK.AQ") < candidates.index("BRK.A")
    assert candidates.index("BRK-AQ") < candidates.index("BRK.A")


def test_intrader_row_keeps_a_failed_company_at_a_fraction_of_a_cent() -> None:
    """NO price floor. A $0.0004 last bar is the signal, not a data defect."""
    rows = list(parse_intrader_rows("DEAD", iter(["2023-06-30,0.0005,0.0006,0.0004,0.0004,1200,1,0,0.0004"])))
    assert len(rows) == 1
    assert rows[0].close == Decimal("0.0004")
    assert rows[0].volume == 1200


def test_intrader_row_stores_raw_close_and_adjusted_separately() -> None:
    """The measured basis: OHLC unadjusted, ninth column split+dividend adjusted."""
    line = "2020-08-27,508.57,509.94,495.33,500.04,38536674,1,0,122.169116674666"
    row = next(parse_intrader_rows("AAPL", iter([line])))
    assert row.close == Decimal("500.04")
    assert row.adj_close == Decimal("122.169116674666")
    assert row.bar_date == date(2020, 8, 27)


def test_intrader_row_without_a_usable_close_reads_as_absent_not_zero() -> None:
    """``close=None`` and NOT 0, because ``load_archive`` counts those.

    The parser deliberately still YIELDS the row: dropping it here would make
    ``LoadCensus.rows_without_close`` silently under-count, and a coverage
    figure that omits its own failures is what #2282 exists to prevent. An
    unparseable DATE has no such counter and is the one thing skipped.
    """
    lines = iter(
        [
            "2023-01-03,1,2,0.5,,100,1,0,1",  # empty close
            "2023-01-04,1,2,0.5,nan,100,1,0,1",  # NaN close
            "not-a-date,1,2,0.5,1,100,1,0,1",  # no counter exists -> skipped
            "2023-01-05,1,2,0.5,1.25,100,1,0,1",
        ]
    )
    rows = list(parse_intrader_rows("X", lines))
    assert [r.close for r in rows] == [None, None, Decimal("1.25")]
    assert [r.bar_date for r in rows] == [date(2023, 1, 3), date(2023, 1, 4), date(2023, 1, 5)]


def test_a_fractional_volume_is_absent_not_truncated() -> None:
    """A fractional volume means the WRONG ARCHIVE, not a roundable share count.

    `Stonks/tickers` scales volume to millions with three decimals — AAPL's
    469,033,600 reads `469.034` there. Truncating that to 469 would understate
    turnover by six orders of magnitude on every bar, silently. Measured: zero
    of this archive's volume fields carry a decimal point.
    """
    row = next(parse_intrader_rows("X", iter(["2023-01-05,1,2,0.5,1.25,469.034,1,0,1.25"])))
    assert row.volume is None
    assert row.close == Decimal("1.25")

    integral = next(parse_intrader_rows("X", iter(["2023-01-05,1,2,0.5,1.25,469033600,1,0,1.25"])))
    assert integral.volume == 469033600


def test_nan_and_inf_are_absent_even_though_decimal_accepts_them() -> None:
    """`Decimal('nan')` does NOT raise — the finiteness test is what catches it."""
    rows = list(
        parse_intrader_rows(
            "X",
            iter(
                [
                    "2023-01-03,1,2,0.5,inf,100,1,0,1",
                    "2023-01-04,nope,2,0.5,1.25,100,1,0,1",
                ]
            ),
        )
    )
    assert rows[0].close is None
    assert rows[1].open is None and rows[1].close == Decimal("1.25")


def test_intrader_row_keeps_a_partial_bar_rather_than_dropping_it() -> None:
    """A missing high is absence of data; only the close is load-bearing."""
    row = next(parse_intrader_rows("X", iter(["2023-01-05,1,,0.5,1.25,,1,0,1.25"])))
    assert row.high is None
    assert row.volume is None
    assert row.close == Decimal("1.25")


def test_the_two_archives_disagree_about_their_adjustment_basis() -> None:
    """One constant per archive, because they are genuinely opposite.

    A shared module constant is what let #2398 stamp this vendor
    ``split_adjusted`` while storing raw OHLC.
    """
    assert HF_ARCHIVE.adjustment_basis == "split_adjusted"
    assert INTRADER_ARCHIVE.adjustment_basis == "unadjusted"
    assert HF_ARCHIVE.vendor != INTRADER_ARCHIVE.vendor
    # Both are Yahoo redistributions, so agreement between them is circular
    # rather than corroborating — sql/249's `upstream_source` exists for this.
    assert HF_ARCHIVE.upstream_source == INTRADER_ARCHIVE.upstream_source == "yahoo_derivative"
