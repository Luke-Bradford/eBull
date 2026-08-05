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

import pytest

from app.services.research_corpus_ingest import (
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
