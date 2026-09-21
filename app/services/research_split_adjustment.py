"""The split-only price correction for the research corpus — #2834 §7 item 2, slice B.

WHAT THIS IS
------------
`b65abd9c` settled the signal basis: `icyDenev/Intrader` ships a per-bar split
ratio in CSV field 6, `d15e680e` settled the correction POLICY (`apply_all` —
every stamp, no adjudication), and `fc72804b` stored the stamps. This module is
the DERIVATION those three left open:

    scale(d) = ∏ { split_factor(e) : e is a bar of this series, e.date > d.date }
    corrected close(d) = close(d) / scale(d)

Nothing here writes. The scale is a pure function of stamps that are already
stored, so it is computed where it is consumed rather than materialised.

WHY NOT A STORED COLUMN
-----------------------
`sql/405` §2 refused to store a corrected `close` and the reason transfers
verbatim to a stored scale: *"the raw close is an observation; the corrected
close is an opinion about it"*. A scale column is that same opinion in a cheaper
encoding — derived state whose correctness depends on a policy decision recorded
in a different file, going silently stale the moment the policy moves. It also
buys nothing: a consumer holding a series' bars already holds every factor the
product needs, so the derivation is O(n) over memory it has, not a join.

⚠ It would also put `research_price_series.adjustment_basis = 'unadjusted'` into
tension with a table that carries an adjusted level beside the raw one. The
corpus stays single-basis; the adjustment lives in the reader.

THE PRECONDITION A CONSUMER CANNOT SKIP
---------------------------------------
:func:`split_scales` REQUIRES the series' `corporate_action_stamps` marker and
refuses `absent`. `COALESCE(split_factor, 1)` reads "this vendor ships no
stamps" identically to "no split on this bar", and the other loaded vendor is
NULL across all 25.8M of its bars — so a derivation that does not read the
marker silently converts a vendor with UNKNOWN corporate actions into one with
none. The marker is a parameter of this function for exactly that reason: it
cannot be forgotten, only mis-supplied.

Refs #2834, #2437.
"""

from __future__ import annotations

import decimal
import hashlib
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path
from typing import Final

#: The correction policy, frozen — `d15e680e` (PR #3281), verdict §5.
#:
#: ⚠⚠ APPLY EVERY STAMP. Do not condition the correction on a second processing
#: in EITHER direction. The three correcting alternatives were refused on
#: recorded grounds: `apply_only_corroborated` on BIAS (it can only correct
#: names the reference serves, and that population carries delisting evidence at
#: 1.67% against 17.33% for the population it cannot reach — a correction
#: quality that varies with survival, in the one corpus built to remove that
#: correlation); `apply_unless_refuted` because its discriminator's precision is
#: unmeasured (32 of 383 refutations are the reference's own unadjusted basis)
#: and it can only fire on the served half; `re_date` because it reaches 39
#: measured events.
#:
#: ⚠ `apply_all` is not *uniquely* parameter-free — `suppress_all` has no
#: adjudication parameter either. The narrower true claim, and the one that
#: makes this honestly freezable in the absence of a published rule: among the
#: candidates that CORRECT, it is the only one with no free parameter. No
#: tolerance, no reference vendor and no adjudication threshold appears
#: anywhere below, and a grep of this module for one is the check.
SPLIT_CORRECTION_POLICY: Final[str] = "apply_all"

_RULE_SET_ID: Final[str] = "split-only-correction-v1"

#: ⚠⚠ CARRIED IN A CONSUMER'S IDENTITY HASH, NOT LEFT IMPLICIT — §5's
#: "frozen by construction" clause, and §4 rule 11's requirement that identity is
#: `code + config + data contract`. A strategy that starts reading a corrected
#: close changes its data contract and owes a NEW id; this constant is what makes
#: that rotation mechanical rather than remembered.
#:
#: Composed like `strategy_price_basis.PRICE_BASIS_RULE_VERSION`: a stable id,
#: then this module's own source. The policy name is inside the source, so it is
#: covered by the hash and is not appended twice.
SPLIT_ADJUSTMENT_RULE_VERSION: Final[str] = (
    f"{_RULE_SET_ID}+{hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]}"
)

#: The `research_price_series.corporate_action_stamps` member that permits a
#: correction. ⚠ A SET OF ONE, and derived from nothing — `sql/405`'s CHECK
#: carries two members and `absent` means the archive's corporate actions are
#: UNKNOWN, not nil. A third member added to that CHECK is refused here until
#: somebody states why it licenses a division.
CORRECTABLE_STAMP_MARKERS: Final[frozenset[str]] = frozenset({"vendor_supplied"})

#: Multiplications are EXACT or they raise. `decimal` traps `Inexact`, so a
#: product that would need more than `prec` significant digits fails loudly
#: instead of silently rounding the corpus.
#:
#: ⚠ The headroom is real rather than nominal: the longest stamped series in the
#: corpus carries 58 events (measured 2026-09-21) and the widest stored factor is
#: 15 significant digits, so the worst exact product needs ~870 digits. `prec`
#: is an order of magnitude above that and the trap is what makes the margin an
#: invariant rather than an estimate.
_PRODUCT_PRECISION: Final[int] = 10_000


class StampsUnavailable(ValueError):
    """The series' corporate actions are not known, so no correction is defined.

    ⚠ NOT "no split happened". Raised rather than returning a scale of 1,
    because a neutral default is precisely the failure this module exists to
    prevent — see the module docstring.
    """


class UncorrectableStamp(ValueError):
    """A stamp inside a correctable series is missing or not a usable factor.

    The loader already downgrades such a series' marker to `absent`
    (`research_corpus_ingest._write_census`), so reaching this means the stored
    marker and the stored bars disagree. Fail closed: a corpus that contradicts
    its own coverage claim must not be silently divided.
    """


def require_correctable(marker: str | None) -> None:
    """Refuse a series whose corporate actions are not known.

    ``marker`` is ``research_price_series.corporate_action_stamps``. ``None`` is
    refused with the rest: the column is ``NOT NULL`` in the schema, so a
    ``None`` here means the caller did not read it.
    """
    if marker not in CORRECTABLE_STAMP_MARKERS:
        raise StampsUnavailable(
            f"corporate_action_stamps is {marker!r}; a split correction is defined only for "
            f"{sorted(CORRECTABLE_STAMP_MARKERS)}. 'absent' means the archive's corporate actions are "
            "UNKNOWN, not nil — deriving a scale of 1 from it would convert a vendor with unknown "
            "splits into a vendor with none (sql/405 §3)."
        )


def _validated_factor(factor: Decimal | None, index: int) -> Decimal:
    """One stored stamp as a usable multiplier, or raise.

    ⚠⚠ `factor > 0` IS NOT THE CHECK, AND THE REASON IS MEASURED RATHER THAN
    REASONED: `select 'NaN'::numeric > 0, 'Infinity'::numeric > 0` returns
    `t | t` in Postgres, which orders NaN above every non-NaN value. The same
    hole was closed in `sql/405`'s CHECK at Codex checkpoint 2; a Python-side
    `> 0` would have reopened it for every row that predates that constraint or
    arrives through a different path.
    """
    if factor is None:
        raise UncorrectableStamp(
            f"bar {index} carries no split factor inside a series marked correctable; "
            "the loader downgrades such a series to 'absent', so the marker and the bars disagree"
        )
    if not factor.is_finite():
        raise UncorrectableStamp(f"bar {index} carries a non-finite split factor {factor!r}")
    if factor <= 0:
        raise UncorrectableStamp(f"bar {index} carries a non-positive split factor {factor!r}")
    return factor


def split_scales(factors: Sequence[Decimal | None], *, stamps_marker: str | None) -> tuple[Decimal, ...]:
    """The scale of every bar: the product of the factors of every LATER bar.

    ``factors`` is ``research_price_daily.split_factor`` for one series, in
    ASCENDING bar-date order — the same order and length as the bars it will
    correct. The returned tuple is aligned to it.

    ⚠⚠ STRICTLY AFTER, AND THE BAR'S OWN STAMP IS EXCLUDED. The archive stamps
    the factor on the bar that FIRST PRINTS THE POST-SPLIT LEVEL
    (``IntraderEngine::SplitCheck()``), so that bar is already on the new basis
    and dividing it by its own factor would adjust it twice. Measured on AAPL's
    4:1 settling 2020-08-31: ``close`` runs 499.23 (08-28) → 129.04 (08-31), and
    it is 08-28 that needs the 4, not 08-31.

    Consequences worth stating because they are the shape of the output:

    * the LAST bar always scales to 1 — there is no later event;
    * the scale is non-increasing in date across forward splits and
      non-decreasing across reverse ones, and it is a STEP function: it changes
      only at a stamped bar, so a 11,039-bar series with 5 events has 6 levels.

    ⚠ ``stamps_marker`` is keyword-only and mandatory. See the module docstring:
    the precondition is a parameter so that it cannot be forgotten.

    ⚠⚠ THREE CALLER OBLIGATIONS THIS FUNCTION CANNOT VERIFY, NAMED RATHER THAN
    ASSUMED. A bare sequence of factors carries no dates, so none of these is
    checkable here — the same hole ``BarSeries`` exists to close for OHLC, and
    the reason a consumer should derive its factor list FROM a validated
    ``BarSeries`` rather than from a second query:

    1. **Order.** Factors reversed or shuffled produce a well-formed, wrong
       answer silently — the exact failure ``BarSeries``' docstring records for
       ``rsi_series(closes)``.
    2. **Completeness.** The scale of bar ``i`` is a claim about *every* later
       event in the series. Hand this function a SLICE and it silently
       re-anchors the whole result to the slice's last bar, which is a different
       basis, not a subset of the same one.
    3. **Growth.** A later re-load that appends a NEW split changes the
       corrected LEVEL of every earlier bar. The corrected basis is a function
       of the series as it stands, not a fixed property of a bar.
       ⚠ An earlier draft added "and therefore … every ratio computed across
       it", which is FALSE and contradicts the anchor-invariance identity in
       ``research_split_corrected_reader``: a ratio whose two endpoints both
       precede the new event is unchanged, because the new factor enters both
       scales and cancels. Only ratios that SPAN the new event move — which is
       the correction doing its job, not drift. Levels move unconditionally.
    """
    require_correctable(stamps_marker)
    scales: list[Decimal] = []
    with decimal.localcontext() as context:
        context.prec = _PRODUCT_PRECISION
        context.traps[decimal.Inexact] = True
        running = Decimal(1)
        # Backwards: bar i's scale is the product over (i, n), which is bar
        # i+1's scale times bar i+1's own factor. One pass, no quadratic
        # re-multiplication, and every partial product is exact.
        for index in range(len(factors) - 1, -1, -1):
            scales.append(running)
            running *= _validated_factor(factors[index], index)
    scales.reverse()
    return tuple(scales)


def _validated_scale(scale: Decimal) -> Decimal:
    """A scale a caller may divide by.

    ⚠ The appliers are PUBLIC and a caller can reach them without
    :func:`split_scales` — so they cannot assume a scale this module produced.
    An unchecked ``0`` raises ``DivisionByZero`` from inside the arithmetic and
    an unchecked ``NaN`` PROPAGATES SILENTLY, turning one bad scale into a whole
    corrected series of ``NaN`` that compares false against every threshold it
    meets. Checked here so both are the same loud failure.
    """
    if not scale.is_finite() or scale <= 0:
        raise UncorrectableStamp(f"scale {scale!r} is not a positive finite multiplier")
    return scale


def corrected_price(price: Decimal | None, scale: Decimal) -> Decimal | None:
    """A price level on the split-only basis: ``price / scale``.

    ``None`` passes through — ``open``/``high``/``low`` are nullable in
    ``research_price_daily`` and an absent level has no corrected form. (Only
    ``close`` is ``NOT NULL``.)

    ⚠⚠ EVERY PRICE FIELD TAKES THE SAME SCALE — §7 contract (b), settled here.
    open, high, low and close are one measurement of one instrument in one unit,
    and a split re-denominates the unit. Correcting ``close`` alone before a
    FORWARD split divides it while ``low`` keeps its raw level, so the bar can
    print ``close < low``; before a REVERSE split the scale is below 1 and the
    same omission pushes ``close`` above ``high``. Either way the ordering every
    OHLC consumer assumes is broken, and the direction depends on the event.

    ⚠ THE DIVISION IS NOT EXACT IN GENERAL and this module does not trap it: a
    corrected price is a derived quantity, not an observation. Exactness is
    enforced on the PRODUCT, where it is achievable and where an error would
    compound across events.

    ⚠⚠ THE ARITHMETIC CONTEXT HERE IS THE CALLER'S, NOT THIS MODULE'S, and that
    is a stated limit rather than a guarantee. A caller who has trapped
    ``Inexact`` makes ``Decimal(100) / 3`` RAISE; one who has narrowed ``prec``
    gets a coarser quotient. That is deliberate — the caller owns the precision
    of its own derived prices, as it already does everywhere else in this
    codebase — but it means nothing about this quotient enters
    ``SPLIT_ADJUSTMENT_RULE_VERSION``, so two consumers on the same rule version
    can legitimately produce different corrected prices.
    """
    if price is None:
        return None
    return price / _validated_scale(scale)


def corrected_volume(volume: int | None, scale: Decimal) -> Decimal | None:
    """Share count on the split-only basis: ``volume * scale``.

    ⚠⚠ MULTIPLIED, NOT DIVIDED — §7 contract (b), the half that is easy to get
    backwards.

    ⚠ THE TARGET UNIT IS THE SERIES' TERMINAL (LATEST) BASIS, NOT A PRE-SPLIT
    ONE, and an earlier draft of this docstring said the opposite. Because
    ``scale`` is the product of every LATER factor, the whole corrected series
    is denominated in the share unit of the LAST bar. A 4:1 split quadruples the
    share count, so a bar recorded in OLD shares is multiplied to express the
    same holding in the new — AAPL's 1980 bar is restated in today's shares, not
    today's bars restated in 1980's. Measured on the same AAPL bars: volume runs
    46,907,479 (08-28, old shares) → 223,505,733 (08-31, new shares).

    ⚠⚠ THIS IS NOT COSMETIC — ``close * volume`` IS SPLIT-INVARIANT AND T3
    DEPENDS ON IT (``price_quarantine.py``). Correcting the price and leaving the
    volume raw would break that invariant on every bar before an event, which is
    precisely the silent failure §7 contract (b) was raised to prevent.

    ⚠ The two functions are published as a PAIR so that correcting one and not
    the other is a visible omission. That is a legibility property and NOT an
    enforcement one — nothing here can stop a consumer calling only
    :func:`corrected_price`. The enforcement, if it is wanted, belongs to the
    consumer that reads both.

    ⚠ Returns a ``Decimal``, never a rounded ``int``. A restated share count is
    genuinely fractional under a reverse split (1:10 turns 1,000 shares into
    100, and 5 into 0.5), and rounding here would destroy the invariant above to
    make the type tidier.
    """
    if volume is None:
        return None
    return Decimal(volume) * _validated_scale(scale)


__all__ = [
    "CORRECTABLE_STAMP_MARKERS",
    "SPLIT_ADJUSTMENT_RULE_VERSION",
    "SPLIT_CORRECTION_POLICY",
    "StampsUnavailable",
    "UncorrectableStamp",
    "corrected_price",
    "corrected_volume",
    "require_correctable",
    "split_scales",
]
