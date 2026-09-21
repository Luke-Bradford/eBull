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
from pathlib import Path

import psycopg
import pytest

from app.services.research_corpus_ingest import (
    HF_ARCHIVE,
    INTRADER_ARCHIVE,
    ArchiveProvenance,
    Form25Match,
    IntraderCsvArchive,
    archive_symbol_candidates,
    classify_form25_match,
    index_instruments,
    load_archive,
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
# The three verdicts below are the whole policy. Measured on the full 2023
# register, `(b)` — the provision where truncating a series is unambiguously
# right — states a suspension date on 0 of 105 cohort rows, so
# `no_suspension` is the normal outcome and `write` is the exception. Since
# #2721 `no_suspension` persists the EVIDENCE (source, provision, filed date)
# and withholds only `delisting_date`; `conflict` remains a full refusal. A
# test suite that only exercised the happy path would describe a guard that
# does not exist.


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
        "latest_filed": date(2023, 3, 2),
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
    # Substituting it is the failure mode sql/353's constraints exist to stop:
    # the writer maps `no_suspension` to an evidence write whose
    # delisting_date is NULL, never to a date borrowed from the filing.
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


def test_series_starting_after_the_filing_is_refused() -> None:
    # ALPS: Form 25 filed 2023-07-27, series first bar 2025-10-31 — a later
    # occupant of the ticker (or a post-Ch11 relisting, DBD). The filing
    # removed a security whose history this demonstrably is not; writing
    # evidence here marks a live series as delisted.
    match = _match(first_bar=date(2025, 10, 31), suspension_date=None)
    assert classify_form25_match(match) == "identity_unverified"


def test_series_straddling_two_filings_is_a_conflict_not_an_identity_refusal() -> None:
    # A relist-then-delist cycle: filings 2015 and 2023, series starts 2018.
    # The 2023 filing may describe THIS series' security, so this is not
    # "starts after every filing" — but the aggregate collapses the two
    # events, so the evidence cannot be attributed without inventing a
    # precedence order. Refused as ambiguity. Zero such symbols in the 2023
    # register (no resolved symbol carries two distinct filed dates); the
    # 2013-2024 expansion is what makes this branch reachable.
    match = _match(
        first_bar=date(2018, 1, 5),
        earliest_filed=date(2015, 3, 2),
        latest_filed=date(2023, 3, 2),
        suspension_date=None,
    )
    assert classify_form25_match(match) == "conflict"


def test_identity_gate_also_blocks_dated_writes() -> None:
    # The dated writer always had this hole (it wrote unconditionally) and it
    # never fired — none of 2023's four identity-unverified overlaps states a
    # suspension date. The gate closes it for both write shapes.
    assert classify_form25_match(_match(first_bar=date(2025, 10, 31))) == "identity_unverified"


def test_conflict_outranks_the_identity_gate() -> None:
    # Disagreeing filings are a stronger refusal: two events on one ticker is
    # ambiguity about WHICH filing the bar test should even run against.
    match = _match(first_bar=date(2025, 10, 31), provision_variants=2)
    assert classify_form25_match(match) == "conflict"


def test_unknown_first_bar_is_not_refused() -> None:
    # A bar-less series cannot fail a bar test; it stays at the pre-existing
    # policy (evidence written) rather than being refused on missing data.
    match = _match(first_bar=None, suspension_date=None)
    assert classify_form25_match(match) == "no_suspension"


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


# ---------------------------------------------------------------------------
# #2834 §7 item 2 — the per-bar corporate-action stamps
# ---------------------------------------------------------------------------


def test_intrader_row_carries_the_split_stamp_on_the_effective_bar() -> None:
    """Field 6 is stamped ON the split bar, not on the one before it.

    AAPL's 4:1 settled 2020-08-31, and the archive prints ``4`` against that
    date. The direction matters to every consumer: the scale correcting a bar
    is the product of the factors of the events STRICTLY AFTER it, so a stamp
    read as belonging to the previous bar would leave the split bar itself
    divided by 4.
    """
    line = "2020-08-31,127.58,131,126,129.04,223505733,4,0,126.107533922878"
    row = next(parse_intrader_rows("AAPL", iter([line])))
    assert row.split_factor == Decimal("4")
    assert row.close == Decimal("129.04")
    assert row.dividend == Decimal("0")


def test_an_ordinary_bar_stamps_one_not_none() -> None:
    """``1`` and ``None`` are DIFFERENT states and the parser must not fuse them.

    ``1`` is "the vendor looked and there was no event"; ``None`` is "this
    archive ships no stamp column at all". ``COALESCE(split_factor, 1)`` reads
    them identically downstream, which is why the distinction is made here and
    recorded per series (sql/405 §3).
    """
    row = next(parse_intrader_rows("X", iter(["2023-01-05,1,2,0.5,1.25,100,1,0,1.25"])))
    assert row.split_factor == Decimal("1")
    assert row.split_factor is not None


@pytest.mark.parametrize("raw", ["0", "-2", "-0.25", "abc", "", "nan", "-inf"])
def test_an_unusable_split_factor_is_absent_rather_than_stored(raw: str) -> None:
    """The derivation DIVIDES by a product of these, so 0 and -2 are not data.

    Absent is nullable, counted by ``LoadCensus.split_stamps_absent`` and
    refusable; a stored ``-2`` silently flips the sign of every price before
    the event. ``nan``/``-inf`` parse fine as ``Decimal`` and are caught by the
    finiteness test in ``_csv_decimal``, not by the except clause.
    """
    row = next(parse_intrader_rows("X", iter([f"2023-01-05,1,2,0.5,1.25,100,{raw},0,1.25"])))
    assert row.split_factor is None
    # The rest of the bar survives: an unusable stamp is not an unusable price.
    assert row.close == Decimal("1.25")


def test_a_negative_dividend_is_stored_as_published() -> None:
    """AGII 2016-05-27 really does publish ``-4.80545454545455``.

    One row in 460,693 non-zero dividends across the full mirror. Nothing
    consumes field 7 yet, so rejecting it would be this ingest inventing a rule
    about a column it does not read — and a CHECK would abort a 50.1M-row load
    over one vendor artefact.
    """
    line = "2016-05-27,52.32,53.6065,52.32,52.86,96011,1,-4.80545454545455,58.9677700515509"
    row = next(parse_intrader_rows("AGII", iter([line])))
    assert row.dividend == Decimal("-4.80545454545455")
    assert row.split_factor == Decimal("1")


def test_only_the_stamped_archive_declares_stamps() -> None:
    """The marker is per ARCHIVE and the two archives genuinely differ.

    The Parquet archive has no stamp columns at all, so every one of its
    25.8M bars is NULL — which is 'unknown', not 'no corporate actions'. That
    is the whole reason the marker is a stored column rather than an inference
    from the bars.
    """
    assert INTRADER_ARCHIVE.corporate_action_stamps == "vendor_supplied"
    assert HF_ARCHIVE.corporate_action_stamps == "absent"


def test_a_new_archive_is_presumed_stampless() -> None:
    """The default fails towards 'unknown', which is the safe direction.

    An archive whose stamps nobody has measured must not present as carrying
    them: that would let a derivation divide by a scale built from absent data
    and report a corrected series.
    """
    unmeasured = ArchiveProvenance(
        vendor="someone/new",
        upstream_source="yahoo_derivative",
        licence="other/unspecified",
        adjustment_basis="unknown",
        quarantine_as_of=date(2026, 1, 1),
    )
    assert unmeasured.corporate_action_stamps == "absent"


@pytest.mark.db
def test_a_series_missing_a_stamp_is_marked_absent_not_vendor_supplied(
    ebull_test_conn: psycopg.Connection[tuple], tmp_path: Path
) -> None:
    """The marker is downgraded PER SERIES, by the code that writes it.

    The load cannot roll back batches it has already committed, so a refusal
    that lives in the CLI's exit code runs too late: the marker would already
    be published claiming coverage the bars do not have. This asserts the only
    guard that holds — `load_archive` writing `absent` for the affected series
    while its clean sibling still gets `vendor_supplied`.

    GOOD has both stamps on every bar. BAD has one bar whose split field is
    unparseable, which `_split_factor` reads as absent.
    """
    (tmp_path / "GOOD.csv").write_text("2023-01-05,1,2,0.5,1.25,100,1,0,1.25\n2023-01-06,1,2,0.5,1.30,100,2,0,1.30\n")
    (tmp_path / "BAD.csv").write_text("2023-01-05,1,2,0.5,1.25,100,1,0,1.25\n2023-01-06,1,2,0.5,1.30,100,abc,0,1.30\n")

    census = load_archive(
        ebull_test_conn,
        IntraderCsvArchive(tmp_path),
        provenance=INTRADER_ARCHIVE,
    )
    assert census.split_stamps_absent == 1
    assert census.dividend_stamps_absent == 0
    assert census.split_events == 1  # GOOD's factor of 2

    markers = dict(
        ebull_test_conn.execute(
            "SELECT vendor_symbol, corporate_action_stamps FROM research_price_series"
            " WHERE vendor = %s AND vendor_symbol IN ('GOOD', 'BAD')",
            (INTRADER_ARCHIVE.vendor,),
        ).fetchall()
    )
    assert markers == {"GOOD": "vendor_supplied", "BAD": "absent"}


@pytest.mark.db
def test_an_absent_dividend_downgrades_the_marker_too(
    ebull_test_conn: psycopg.Connection[tuple], tmp_path: Path
) -> None:
    """Both halves of the contract, not just the one the derivation divides by.

    sql/405 says `vendor_supplied` means split_factor AND dividend are
    populated on every bar. A missing dividend with a perfectly good factor
    therefore falsifies the marker, and it is a distinct defect: `_csv_decimal`
    is the only gate on field 7 — there is no sign or range rule that would
    catch it.
    """
    (tmp_path / "NODIV.csv").write_text("2023-01-05,1,2,0.5,1.25,100,1,0,1.25\n2023-01-06,1,2,0.5,1.30,100,1,,1.30\n")

    census = load_archive(ebull_test_conn, IntraderCsvArchive(tmp_path), provenance=INTRADER_ARCHIVE)
    assert census.split_stamps_absent == 0
    assert census.dividend_stamps_absent == 1

    row = ebull_test_conn.execute(
        "SELECT corporate_action_stamps FROM research_price_series WHERE vendor = %s AND vendor_symbol = 'NODIV'",
        (INTRADER_ARCHIVE.vendor,),
    ).fetchone()
    assert row == ("absent",)
