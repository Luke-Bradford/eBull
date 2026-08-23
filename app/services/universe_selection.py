"""#2721 step 3 — which corpus series a backtest universe admits, and why.

One versioned mapping from the ``Universe`` label to a vendor pin and an
admission rule. Before this module the corpus queries selected on
``instrument_id = ANY(validated)`` with no vendor filter — one series per
instrument held only because one vendor was linked. #2597 linked 5,172
``icyDenev/Intrader`` series and 4,549 validated instruments then carried TWO
series, which the engine's per-name close maps would clobber silently
(``raw_closes_by_instrument[key] =``). Admission is therefore SERIES-based and
vendor-pinned here, in one place, and the engine derives everything — axis,
pairs, breaks, benchmark books — from the admitted set.

⚠⚠ ``UNIVERSE_SELECTION_RULE_VERSION`` IS HASHED INTO EVERY STRATEGY IDENTITY
(``strategy_registry.INPUT_RULE_SETS``). The vendor literals, the alive cut and
the capture constant all decide what a result contains; the bare ``universe``
label on the identity does not version them (ckpt-1). Changing anything here is
a new version of every strategy by construction.

Source rule: the admission and the alive/terminated cut have NO published
formulation — fixed by construction below (spec:
``docs/proposals/ta/2026-08-15-2721-step3-survivorship-free-universe.md``) and
frozen by this module's code hash. The capture date follows
``verify_2597_survivorship_acceptance._capture_date``'s rule — it IS
``max(last_bar)`` over the vendor, so it is DECLARED here and ASSERTED against
that measurement at load (the #2720 FX-gate idiom: re-assert a mutable premise
on the run's own data, refuse loudly), never trusted from either side alone.

The alive/terminated cut, by construction: 9,946 of 22,879 Intrader series end
on the capture date itself and 292 more within 4 calendar days (weekend +
short halts); the tail beyond is ~10/day, so any cut in 5-20 days moves tens
of series in 22,879. Frozen at 7 calendar days. Reproduce:
``SELECT (DATE '2024-09-27' - last_bar), count(*) FROM research_price_series
WHERE vendor = 'icyDenev/Intrader' GROUP BY 1 ORDER BY 1``.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Final

import psycopg

from app.services.indicator_series import Universe
from app.services.research_corpus_ingest import vendor_symbol_has_bankruptcy_suffix
from app.services.series_termination import TerminationEvidence

_RULE_ID: Final = "universe-selection-v1"


def _code_hash() -> str:
    """Hash this module's source, per ``indicator_series._code_hash``'s idiom."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


#: Joins ``INPUT_RULE_SETS`` — every change to this file moves every strategy
#: version. See the module docstring.
UNIVERSE_SELECTION_RULE_VERSION: Final[str] = f"{_RULE_ID}+{_code_hash()}"

#: The survivor-only vendor — the corpus the 324 pre-existing results describe.
SURVIVOR_ONLY_VENDOR: Final = "paperswithbacktest/Stocks-Daily-Price"

#: The survivorship-free vendor (#2597): live + dead names from one scrape,
#: internally consistent, fully quarantine-judged.
SURVIVORSHIP_FREE_VENDOR: Final = "icyDenev/Intrader"

#: The Intrader archive's freeze date. ⚠ DECLARED here and ASSERTED equal to
#: ``max(last_bar)`` over the vendor at load — a re-harvested archive that
#: moves the measurement must refuse, not silently shift what the
#: ``survivorship_free`` label means. #2721's hard bound: a window ending
#: after this date can never earn the label on this corpus.
INTRADER_CAPTURE_DATE: Final = date(2024, 9, 27)

#: A series whose last bar is within this many calendar days of capture is
#: ALIVE (weekend + short halt); anything older TERMINATED. By construction —
#: see the module docstring for the measured cliff.
ALIVE_CUT_DAYS: Final = 7

# Nasdaq Symbol Directory ``Test Issue=Y`` on the two official directory
# responses frozen in #2912 correction 1 (capture 2026-08-23).  These are
# synthetic production-feed instruments, not companies.  Keeping the identity
# rule here (rather than filtering extreme returns) prevents test traffic from
# entering any strategy population and moves the hashed universe rule version.
EXCHANGE_TEST_ISSUE_SYMBOLS: Final[frozenset[str]] = frozenset(
    {
        "ATEST",
        "ATEST.A",
        "ATEST.B",
        "ATEST.C",
        "CBO",
        "CBX",
        "CTEST",
        "CTEST.E",
        "CTEST.G",
        "CTEST.L",
        "CTEST.O",
        "CTEST.S",
        "CTEST.V",
        "IGZ",
        "MTEST",
        "NTEST",
        "NTEST.A",
        "NTEST.B",
        "NTEST.C",
        "ZAZZT",
        "ZBZX",
        "ZBZZT",
        "ZCZZT",
        "ZEXIT",
        "ZIEXT",
        "ZJZZT",
        "ZTEST",
        "ZVV",
        "ZVZZT",
        "ZWZZT",
        "ZXIET",
        "ZXYZ.A",
        "ZXZZT",
    }
)


def _is_exchange_test_issue(symbol: object) -> bool:
    return str(symbol).strip().upper() in EXCHANGE_TEST_ISSUE_SYMBOLS


def vendor_for(universe: Universe) -> str:
    if universe == "survivor_only":
        return SURVIVOR_ONLY_VENDOR
    if universe == "survivorship_free":
        return SURVIVORSHIP_FREE_VENDOR
    raise ValueError(f"unknown universe {universe!r}")


@dataclass(frozen=True)
class AdmittedSeries:
    """One admitted series and how the engine must treat it.

    ``name_key`` is the engine's in-pass per-name key: the real
    ``instrument_id`` for a linked live series, ``-series_id`` for a series
    admitted without one (instrument ids are positive, so collision is
    impossible). ⚠ IN-PASS ONLY — no negative key may reach any column typed
    as an instrument id; ``tests/test_2721_universe_selection.py`` walks the
    write boundary.

    ``termination`` is ``None`` for a live series and the series' own evidence
    for a terminating one. ⚠ The 509 linked-but-early-ending series carry
    evidence and a POSITIVE name key: a live instrument whose series stopped
    before the alive cut is a symbol-reuse suspect, admitted as TERMINATING on
    the SERIES' evidence, never trusted as live (spec §admission).
    """

    series_id: int
    name_key: int
    instrument_id: int | None
    termination: TerminationEvidence | None
    #: The series' stored terminal bar (``research_price_series.last_bar``).
    #: The engine gates termination on THIS date against the evaluation window
    #: — never on the loaded series' own last date, which a masked arm (or any
    #: future window clipping) can pull earlier and turn into look-ahead
    #: (Codex ckpt-2 on #2721 step 3).
    last_bar: date | None = None


@dataclass(frozen=True)
class UniverseSelection:
    """The admitted set plus the census strata acceptance reconciles against."""

    universe: Universe
    vendor: str
    #: ``None`` for ``survivor_only`` — no capture bound applies to its window.
    capture_date: date | None
    admitted: tuple[AdmittedSeries, ...]
    #: Alive at capture but resolvable to no validated instrument — the
    #: eToro-listing bias the parent spec §6 names. Excluded and COUNTED.
    unlinked_alive_excluded: int
    #: Of the admitted terminating series, how many carry a (suspect) link to
    #: a live instrument. A stratum of ``admitted``, not an exclusion.
    linked_early_reuse_suspect: int
    #: Official exchange test issues — synthetic feed traffic, not equities.
    exchange_test_issues_excluded: int
    #: Vendor rows with no harvested bars (``bar_count IS NULL``) — outside
    #: the admission query by construction, counted so the strata reconcile to
    #: the vendor's full series count.
    unharvested_excluded: int
    #: The vendor's full ``research_price_series`` row count, the total every
    #: census reconciliation sums back to.
    vendor_series_total: int


_SERIES_ROWS_SQL = """
    SELECT series_id, vendor_symbol, instrument_id, last_bar,
           delisting_source, delisting_provision
    FROM research_price_series
    WHERE vendor = %(vendor)s
      AND bar_count IS NOT NULL
    ORDER BY series_id
"""


def _assert_capture(conn: psycopg.Connection[Any], *, vendor: str, declared: date) -> None:
    row = conn.execute(
        "SELECT max(last_bar) FROM research_price_series WHERE vendor = %(vendor)s",
        {"vendor": vendor},
    ).fetchone()
    measured = row[0] if row else None
    if measured != declared:
        raise RuntimeError(
            f"declared capture date {declared} for {vendor!r} but max(last_bar) measures {measured} — the archive "
            "has moved under the frozen constant, and running on would silently change what the survivorship label "
            "means. Re-freeze the constant deliberately (a corpus-version event) or fix the ingest."
        )


def load_universe_selection(
    conn: psycopg.Connection[Any],
    *,
    universe: Universe,
    validated_ids: frozenset[int],
) -> UniverseSelection:
    """The admitted series set for ``universe``. One query; series-based.

    ``survivor_only``: linked ∩ validated on the pinned survivor vendor —
    exactly the population the pre-#2597 predicate produced, now enforced
    rather than assumed.

    ``survivorship_free``: linked ∩ validated AND alive at capture, plus every
    terminating series regardless of link (spec §admission). Unlinked-alive
    series are excluded and counted.
    """
    vendor = vendor_for(universe)
    totals = conn.execute(
        "SELECT count(*), count(*) FILTER (WHERE bar_count IS NULL) "
        "FROM research_price_series WHERE vendor = %(vendor)s",
        {"vendor": vendor},
    ).fetchone()
    vendor_total, unharvested = (int(totals[0]), int(totals[1])) if totals else (0, 0)
    rows = conn.execute(_SERIES_ROWS_SQL, {"vendor": vendor}).fetchall()

    admitted: list[AdmittedSeries] = []
    unlinked_alive = 0
    reuse_suspects = 0
    exchange_test_issues = 0

    if universe == "survivor_only":
        capture = None
        for series_id, symbol, instrument_id, _last_bar, _source, _provision in rows:
            if _is_exchange_test_issue(symbol):
                exchange_test_issues += 1
                continue
            if instrument_id is None or int(instrument_id) not in validated_ids:
                continue
            admitted.append(
                AdmittedSeries(
                    series_id=int(series_id),
                    name_key=int(instrument_id),
                    instrument_id=int(instrument_id),
                    termination=None,
                    last_bar=_last_bar,
                )
            )
    else:
        capture = INTRADER_CAPTURE_DATE
        _assert_capture(conn, vendor=vendor, declared=capture)
        alive_floor = capture - timedelta(days=ALIVE_CUT_DAYS)
        for series_id, symbol, instrument_id, last_bar, source, provision in rows:
            if _is_exchange_test_issue(symbol):
                exchange_test_issues += 1
                continue
            linked_instrument = int(instrument_id) if instrument_id is not None else None
            alive = last_bar is not None and last_bar > alive_floor
            if alive:
                if linked_instrument is not None and linked_instrument in validated_ids:
                    admitted.append(
                        AdmittedSeries(
                            series_id=int(series_id),
                            name_key=linked_instrument,
                            instrument_id=linked_instrument,
                            termination=None,
                            last_bar=last_bar,
                        )
                    )
                else:
                    unlinked_alive += 1
                continue
            # Terminating: admitted on the SERIES' own evidence, link or not.
            evidence = TerminationEvidence(
                linked=(source == "sec_form25"),
                provision=provision,
                q_suffix=vendor_symbol_has_bankruptcy_suffix(str(symbol)),
            )
            if linked_instrument is not None and linked_instrument in validated_ids:
                reuse_suspects += 1
                name_key = linked_instrument
            else:
                name_key = -int(series_id)
            admitted.append(
                AdmittedSeries(
                    series_id=int(series_id),
                    name_key=name_key,
                    instrument_id=linked_instrument,
                    termination=evidence,
                    last_bar=last_bar,
                )
            )

    # ⚠ One admitted series per name key, ASSERTED — within-vendor uniqueness
    # is a measured premise (0 duplicates on 2026-08-15), not a guarantee, and
    # a future ingest recreating it would clobber the engine's per-name close
    # maps silently.
    key_counts = Counter(series.name_key for series in admitted)
    duplicated = sorted(key for key, count in key_counts.items() if count > 1)
    if duplicated:
        raise RuntimeError(
            f"universe {universe!r} admits {len(duplicated)} duplicate name key(s) (sample {duplicated[:5]}) — "
            "the engine keys per-name close maps on this value and the second series would silently clobber "
            "the first"
        )

    return UniverseSelection(
        universe=universe,
        vendor=vendor,
        capture_date=capture,
        admitted=tuple(admitted),
        unlinked_alive_excluded=unlinked_alive,
        linked_early_reuse_suspect=reuse_suspects,
        exchange_test_issues_excluded=exchange_test_issues,
        unharvested_excluded=unharvested,
        vendor_series_total=vendor_total,
    )


__all__ = [
    "ALIVE_CUT_DAYS",
    "EXCHANGE_TEST_ISSUE_SYMBOLS",
    "INTRADER_CAPTURE_DATE",
    "SURVIVOR_ONLY_VENDOR",
    "SURVIVORSHIP_FREE_VENDOR",
    "UNIVERSE_SELECTION_RULE_VERSION",
    "AdmittedSeries",
    "UniverseSelection",
    "load_universe_selection",
    "vendor_for",
]
