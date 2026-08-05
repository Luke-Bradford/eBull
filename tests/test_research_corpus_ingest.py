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

import pytest

from app.services.research_corpus_ingest import (
    Form25Match,
    classify_form25_match,
    index_instruments,
    normalise_vendor_symbol,
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
