"""The per-series price-basis CARRIER (#2840) — per-bar, declared, fail-closed.

WHAT THIS REPLACED, AND IT IS NOW GONE
--------------------------------------
``s12_cheapest_band_price_gated_breakout`` used to refuse every bar when
``universe not in AS_TRADED_UNIVERSES`` — a **universe token standing in for a
price-basis fact**, wrong in both directions: the live scan (``survivor_only``)
was refused unconditionally, all 5,791 stored S-12 observations being that one
refusal, while a ``survivorship_free`` run whose archive policy was WITHHELD
passed with no refusal anywhere, because
``backtest_run._resolve_liquidity_policy`` returns ``None`` rather than blocking.

This module carries the fact itself, per bar, so a rule can declare it. The token
was removed once it did (#2840 §6 item 3), so this carrier is now S-12's SOLE
price-provenance gate — which is why ``s12_signals`` also checks
``rule_set_version`` and why the scan reaches this module through
``from_undeclared_source`` rather than through a ``str | None`` constant.

WHY IT IS AN ``EvaluableSeries`` AND NOT A MASK
-----------------------------------------------
``strategy_registry.EvaluableSeries`` is a STRUCTURAL protocol — ``values`` plus
``not_evaluable_indices`` plus ``__len__`` — and that is the whole reason this
design needs no new refusal machinery. Declared as a ``StrategyInput``,
``evaluate`` refuses the bar BEFORE the strategy body runs, so an uncertified bar
is ``not_evaluable`` and never ``not_fired``.

⚠⚠ THE ALTERNATIVE — MASKING — IS MEASURED TERMINAL AND CANNOT CARRY THIS.
``2026-09-20-2840-per-series-price-basis-carrier.md`` §3.3: one masked close at
index 120 of 200 makes ``atr_series`` ``None`` for 120…199, never recovering —
80 refusals from one bar. That is a DECISION (``adx_series``: *"Wilder smoothing
is recursive … everything from the first unusable bar is not_evaluable"*), not a
defect. A ``PriceBasisSeries`` never enters the indicator recursion, so one
uncertified bar refuses THAT BAR.

⚠ And that is a statement about VERDICTS, not about contamination. An uncertified
bar's OHLC still feeds ATR, compression and prior-high for later bars. Scale
continuity is ``price_segments`` / ``unresolved_breaks``' job (``sql/249:82``
documents the danger), and it stays theirs. Today's only source is constant
across a series, so no mixed case exists yet; the obligation lands with the
per-bar source.

WHAT IT DOES NOT CERTIFY
------------------------
Entry VERDICTS only. A certified signal at ``t`` still fills at an uncertified
``open(t+1)``; a held position still consumes uncertified ``high``/``low`` and
its terminal mark; the four charge consumers still read
``_Corpus.cost_price_basis``, which this does not touch. Entry refusal cannot
certify a trade's price path.

Spec: ``docs/proposals/ta/2026-09-20-2840-the-price-basis-carrier.md``.
Refs #2840, #2437.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, get_args

from app.services.research_corpus_ingest import RESEARCH_ARCHIVES

#: The provenance of a nominal price level — ``sql/305``'s CHECK, executable.
#:
#: ⚠⚠ DEFINED HERE AND IMPORTED BY ``strategy_decision_context``, WHICH IS THE
#: REVERSE OF HOW IT STARTED, and the reversal was forced by a guard rather than
#: chosen for tidiness. ``tests/test_strategy_registry.py::TestTheEngineIsWalked
#: TooNotJustTheStrategies`` walks every module reachable from the signal scan and
#: requires each versioned rule set it finds to be hashed into an identity.
#: Importing the type FROM ``strategy_decision_context`` pulled that module — and
#: therefore ``market_calendar.RULE_SET_VERSION`` — into the scan's reachable set,
#: so a calendar change would have reused every stored ``strategy_version``
#: (#3031). The two honest fixes were to hash the calendar into every identity
#: (rotating all 11 strategies to carry a rule this path does not read) or to
#: stop reaching it. This is the second.
#:
#: ⚠ ONE definition, not two. ``strategy_decision_context`` re-exports it, so
#: ``sql/305``'s column and this carrier cannot drift apart — the
#: closed-vocabulary-in-N-places defect (#2218).
AsTradedPriceBasis = Literal["observed_unadjusted", "reconstructed_unadjusted", "unknown"]

#: The basis members a bar may CARRY.
#:
#: ⚠⚠ ``AsTradedPriceBasis`` MINUS ``"unknown"``, and the subtraction is the
#: single most important line in this module. ``evaluate`` tests only
#: ``value is None``, so a carrier whose values were ``"unknown"`` would EVALUATE
#: EVERY BAR — a fail-open wearing the vocabulary this module reuses. Here
#: "unknown" is spelled ``None`` plus a ``not_evaluable_indices`` entry, and
#: permitting both spellings would leave one of them permissive forever.
#:
#: ⚠ DERIVED by subtraction, never restated — the closed-vocabulary-in-N-places
#: defect ``strategy_registry`` already fixed with ``get_args`` (#2218). A member
#: added to ``AsTradedPriceBasis`` arrives here automatically and must be
#: excluded deliberately if it is not a certification.
UNCERTIFIED_BASIS: Final = "unknown"
CERTIFIED_PRICE_BASES: Final[frozenset[str]] = frozenset(get_args(AsTradedPriceBasis)) - {UNCERTIFIED_BASIS}

#: The stored ``research_price_series.adjustment_basis`` member that certifies a
#: nominal level. ⚠ A SET OF ONE, deliberately: ``sql/249``'s CHECK carries four
#: members and the other three (``split_adjusted``,
#: ``split_and_dividend_adjusted``, ``unknown``) are all rescaled or undeclared.
#: A fifth member added to that CHECK is refused here until somebody states why
#: it is as traded — which was ``AS_TRADED_UNIVERSES``' own declared posture,
#: moved off the universe name and onto the fact.
#:
#: ⚠ ``sql/249``'s CHECK is a VOCABULARY, not an evidence standard. The rule that
#: makes a level eligible for a NOMINAL-price gate is ``sql/305``'s — a directly
#: observed unadjusted level, or a point-in-time reconstruction. ``unadjusted`` is
#: the one stored label we treat as satisfying it, and that mapping is an
#: operational assumption inherited from ingest, not a schema guarantee: see
#: ``from_archive_basis``' note on the two AAPL bars it rests on.
CERTIFYING_ARCHIVE_BASES: Final[frozenset[str]] = frozenset({"unadjusted"})

_RULE_SET_ID: Final[str] = "price-basis-carrier-v1"


def _archives_hash() -> str:
    """Hash of the PINNED archive provenances this rule reads a basis from."""
    payload = repr(
        sorted((archive.vendor, archive.upstream_source, archive.adjustment_basis) for archive in RESEARCH_ARCHIVES)
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:12]


#: ⚠⚠ COMPOSED, NOT A BARE CONSTANT — a Codex checkpoint-1 finding, and the
#: reason is that a constant name is not a versioning mechanism. This rule's
#: verdict is a function of something it does not own: the pinned archive
#: provenance it reads (``RESEARCH_ARCHIVES``). A re-declared vendor basis
#: changes what this certifies without changing this file's bytes.
#:
#: Follows ``bar_capture_certificate.CAPTURE_CERTIFICATE_VERSION``'s idiom — a
#: stable id, this module's own source, then each dependency's version — rather
#: than inventing a second hash scheme, because two schemes in one codebase
#: produce version strings that look comparable and are not.
PRICE_BASIS_RULE_VERSION: Final[str] = (
    f"{_RULE_SET_ID}+{hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]}+archives-{_archives_hash()}"
)


@dataclass(frozen=True)
class PriceBasisSeries:
    """Per-bar as-traded price basis, aligned to the input bars.

    ``None`` at an index means THIS BAR'S LEVEL IS NOT CERTIFIED AS TRADED. It
    does not mean "probably fine": a rule reading an absolute price must refuse,
    which is what declaring this as a ``StrategyInput`` achieves.

    ⚠⚠ STRICTER THAN ``RegimeSeries``, AND THE DIFFERENCE IS DELIBERATE.
    ``RegimeSeries`` permits a ``None`` OUTSIDE ``not_evaluable_indices`` because
    there it means WARM-UP — a real third state, distinguished structurally.
    **Price provenance has no warm-up state.** Copying that invariant produced
    two fail-opens at Codex checkpoint 1:

    * an unmarked ``None`` reaches ``_unevaluable_reason_at``'s warm-up branch
      and is recorded as ``insufficient_warmup`` — a provenance gap wearing an
      indicator's reason code, which is parent criterion 8's exact prohibition;
    * ``"unknown"`` is a member of ``AsTradedPriceBasis`` and ``evaluate`` tests
      only ``is None``, so a carrier of ``"unknown"`` values evaluates every bar.

    So correspondence is COMPLETE and checked in both directions, and the
    vocabulary is checked at runtime — ``Literal`` enforces nothing there, the
    point ``StrategySignal.__post_init__`` already makes.
    """

    values: tuple[AsTradedPriceBasis | None, ...]
    #: Indices whose level is not certified as traded. ⚠ Exactly the ``None``
    #: positions — see the class docstring.
    not_evaluable_indices: tuple[int, ...] = ()
    rule_set_version: str = PRICE_BASIS_RULE_VERSION

    def __post_init__(self) -> None:
        seen: set[int] = set()
        previous = -1
        for index in self.not_evaluable_indices:
            if index < 0 or index >= len(self.values):
                raise ValueError(f"not_evaluable_indices contains {index}, outside a series of {len(self.values)} bars")
            if index in seen:
                raise ValueError(f"not_evaluable_indices repeats index {index}")
            if index < previous:
                # Sorted is not cosmetic: ``segment`` preserves order by
                # construction, and a caller comparing two carriers position by
                # position would otherwise read equal sets as unequal tuples.
                raise ValueError(f"not_evaluable_indices is not sorted at index {index}")
            seen.add(index)
            previous = index
            if self.values[index] is not None:
                raise ValueError(
                    f"index {index} is marked uncertified but carries basis {self.values[index]!r}; "
                    "a certified bar is not an uncertified one"
                )
        for index, value in enumerate(self.values):
            if value is None:
                if index not in seen:
                    # ⚠ The half ``RegimeSeries`` deliberately allows. Here it is
                    # the fail-open: an unmarked ``None`` is reported as
                    # ``insufficient_warmup`` by a rule that has no warm-up.
                    raise ValueError(
                        f"index {index} carries no basis but is not in not_evaluable_indices; "
                        "price provenance has no warm-up state"
                    )
            elif value not in CERTIFIED_PRICE_BASES:
                raise ValueError(
                    f"index {index} carries basis {value!r}, which is not one of {sorted(CERTIFIED_PRICE_BASES)}; "
                    f"{UNCERTIFIED_BASIS!r} is spelled None plus a not_evaluable index"
                )

    def __len__(self) -> int:
        return len(self.values)

    def certifies_nothing(self) -> bool:
        """Every bar uncertified — the shape a withheld archive policy produces.

        ⚠ Exists so ``s12_signals`` can return the uniform refusal list WITHOUT
        calling ``evaluate``. ``_unevaluable_reason_at`` does
        ``index in series.not_evaluable_indices`` on a TUPLE, so an all-refused
        carrier of n bars is O(n²) and n here is the whole corpus. The protocol
        types that field ``tuple[int, ...]`` and widening it means editing
        ``strategy_registry``, which is hashed into EVERY identity — so the
        short-circuit lives at the consumer instead.
        """
        return len(self.not_evaluable_indices) == len(self.values)

    def segment(self, start: int, end: int) -> PriceBasisSeries:
        """Bars ``[start, end)`` as a series indexed from zero.

        ⚠⚠ THE INDICES ARE REMAPPED, AND THIS METHOD EXISTS BECAUSE SLICING
        CANNOT BE — ``RegimeSeries.segment``'s reason verbatim.
        ``PriceBasisSeries(values=self.values[start:end])`` type-checks and
        silently drops ``not_evaluable_indices``; here that would ALSO raise,
        because the dropped indices leave unmarked ``None`` values behind. The
        remap lives on the data.

        ``rule_set_version`` is preserved: a segment is the same rule's verdict
        over fewer bars, and re-defaulting it would let a stale carrier be
        consumed under today's version.
        """
        if start < 0 or end > len(self.values) or start > end:
            raise ValueError(f"segment [{start}, {end}) is not inside a series of {len(self.values)} bars")
        return PriceBasisSeries(
            values=self.values[start:end],
            not_evaluable_indices=tuple(index - start for index in self.not_evaluable_indices if start <= index < end),
            rule_set_version=self.rule_set_version,
        )


def from_archive_basis(adjustment_basis: str | None, *, n_bars: int) -> PriceBasisSeries:
    """The carrier for a run served by a PINNED research archive.

    ``adjustment_basis`` is ``_Corpus.liquidity_policy.adjustment_basis``, or
    ``None`` where the policy was withheld.

    ⚠⚠ ROUTED FROM THE POLICY, NOT FROM ``cost_price_basis``. The prior draft
    routed the derived charging field and Codex refused it: ``ab_3238_cost_basis``
    builds its control as ``replace(corpus, cost_price_basis="split_adjusted")``
    (``:355``), so gating signals on it would stop the control arm TRADING rather
    than charge it differently. The same script's docstring (``:30``) says
    ``liquidity_policy`` is DELIBERATELY NOT REPLACED, which is what makes the
    root value safe to read and the derived one not.

    ⚠ ``None`` IS WITHHOLDING, NEVER ELIGIBILITY — ``archive_policy_for``'s own
    rule, and ``sql/305`` refuses an undeclared basis exactly as it refuses a
    split-adjusted one.

    ⚠ ONE DISAGREEING SERIES WITHHOLDS THE WHOLE RUN, and turning that into a
    universal refusal is a CHOICE rather than a derivation. Taken deliberately:
    the withheld state means we do not know WHICH series are mis-labelled, and a
    nominal-price gate under an unknown scale is the failure this exists to
    prevent.

    ⚠⚠ THE LABEL IS MEASURED ON A SAMPLE, AND THAT IS THE WEAKEST LINK.
    ``research_corpus_ingest.py:165-171`` records the Intrader archive's
    ``unadjusted`` basis from TWO AAPL bars (2020-08-27 reads 500.04 against
    Yahoo's split-adjusted; the 1980-12-12 IPO bar reads 28.75 against 0.1283).
    That satisfies ``sql/305``'s first arm — *"a directly observed unadjusted
    level"* — and it is the best evidence that exists, but 22,880 stored labels
    are not 22,880 validated payloads. This moves the gate from a universe NAME
    to a measured-on-a-sample LABEL: strictly closer to the fact, not the fact.

    ⚠ ``308d1e38``'s split probe is NOT evidence here. It measured eToro's
    delivered history, not this archive. Two vendors; citing one for the other is
    the coincidence-window error #2840 already carries a prevention entry for.
    """
    if n_bars < 0:
        raise ValueError(f"n_bars must be non-negative, got {n_bars}")
    if adjustment_basis in CERTIFYING_ARCHIVE_BASES:
        return PriceBasisSeries(values=("observed_unadjusted",) * n_bars)
    return PriceBasisSeries(values=(None,) * n_bars, not_evaluable_indices=tuple(range(n_bars)))


def from_undeclared_source(*, n_bars: int) -> PriceBasisSeries:
    """The carrier for a path with NO DECLARED as-traded provenance source — refuses every bar.

    ⚠⚠ A POLICY, STATED AS ONE, AND NOT AN INFERENCE FROM THE SCHEMA. It would be
    easy to write "``price_daily`` has no adjustment-basis column, therefore no
    certificate is possible", and ``9246f77c`` expressly withdrew that step: a
    missing column does not imply missing evidence. What is true is narrower and
    sufficient — **no source of as-traded provenance has been declared for this
    path** — and an undeclared basis is refused exactly as ``sql/305`` and
    ``archive_policy_for`` already refuse one.

    ⚠⚠ THIS EXISTS BECAUSE A MODULE CONSTANT IS THE WRONG SHAPE FOR THE FACT.
    It replaces ``strategy_signal_scan.SCAN_ARCHIVE_ADJUSTMENT_BASIS``, a
    ``str | None`` that only ever held ``None``: once S-12's universe token was
    removed (#2840 §6 item 3) that constant was the ONLY thing standing between
    the scan and a nominal ``>= $100`` gate on ``price_daily``, whose history the
    provider back-adjusts at fetch time and whose #2066 split-cliff guard HEALS a
    mixed series onto the back-adjusted basis (``market_data.py:751``). One token
    edit would have certified all of it. There is no token here.

    ⚠ AND THAT IS THE WHOLE OF THE CLAIM (Codex checkpoint 1). It does NOT make
    certifying the scan require a real source: ``PriceBasisSeries`` is public and
    structural, so a caller can still write ``values=("observed_unadjusted",) * n``
    inline. What this buys is that the dangerous change is a visible constructor
    call rather than a ``None`` in a ``str | None`` slot a reader could take for a
    measured fact.

    ⚠ ALSO THE RIGHT CALLEE FOR A PATH WITH NO ARCHIVE. ``from_archive_basis``
    names a pinned archive; the live scan has none. Four verification scripts had
    been passing it a literal ``"unadjusted"`` for exactly that reason and were
    only safe because S-12's universe token refused them first.

    ⚠ Equal to ``from_archive_basis(None, n_bars=n)`` at every ``n``, including the
    raise at ``n < 0`` — pinned by
    ``tests/test_2840_price_basis_carrier.py`` so the two cannot drift into
    disagreeing about the withheld case.
    """
    if n_bars < 0:
        raise ValueError(f"n_bars must be non-negative, got {n_bars}")
    return PriceBasisSeries(values=(None,) * n_bars, not_evaluable_indices=tuple(range(n_bars)))


__all__ = [
    "AsTradedPriceBasis",
    "CERTIFIED_PRICE_BASES",
    "CERTIFYING_ARCHIVE_BASES",
    "PRICE_BASIS_RULE_VERSION",
    "PriceBasisSeries",
    "from_archive_basis",
    "from_undeclared_source",
]
