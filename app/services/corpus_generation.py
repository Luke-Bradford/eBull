"""#2414 — the corpus-provenance stamp a scan pass carries onto every row.

Spec: ``docs/proposals/ta/2026-09-14-2414-corpus-generation-construction.md``.
Parent: ``docs/proposals/ta/2026-09-14-2414-signal-ledger-corpus-stamp.md``.
Table: ``sql/382_strategy_signal_corpus_generation.sql``.

``strategy_version`` hashes the PRODUCER — module, params, universe, cost model,
and since #2333/#3031 the input rule sets. It records nothing about the corpus
VALUES the decision read, and ``price_daily`` is retroactively split-adjusted, so
a bar can change under a stored decision at an age no embargo would cover.

⚠⚠ THIS IS THE PREREQUISITE HALF OF #2414, NOT THE WHOLE TICKET.

The scan spec's §12 is where #2414 comes from, verbatim: *"A corrected historical
bar cannot be reflected in an already-written signal … It needs its own ticket,
and the shape of the fix (**a corpus version in the key, or an explicit
supersede-and-record path**) is a decision about the ledger, not about the scan."*

Both of those shapes need a **computable corpus identity** and neither can be
built without one. This module is that identity. It is deliberately stored as a
non-key column so it prejudges neither shape, and the supersession half stays
open on #2414 — two checkpoint-1 passes (44 findings, then 37) established that
it reaches eight tables, five foreign-key dependents and a retraction case that
has no encoding yet.

⚠ §11 ("It does not backfill") is a DIFFERENT rule and does not forbid the
supersession half. It governs the cold start — deriving a decision for a bar that
was never decided. An earlier draft of this module read §11 as settling §12; it
does not, and conflating them would have closed the ticket on a misreading.

What the stamp answers today, and nothing else:

    Were these two stored decisions computed against the same corpus state?

It is NOT key material, it is WRITE-ONCE on every ledger row, and
``strategy_signals_unique`` is unchanged.

⚠ It is not a database snapshot. ``run_signal_scan`` requires
``autocommit=True``, so the spans, the breaks, the benchmark and each
instrument's bars are separate reads and a concurrent ``daily_candle_refresh``
can produce a combination that never existed together. This records what the
pass READ, mixture included — which is the honest thing for provenance to
record, and the reason it is described as provenance rather than as a snapshot
id.

⚠ It is not a publication unit. One pass commits one transaction per strategy,
so two rows sharing a generation were computed against one corpus but were not
necessarily published together.

⚠ EQUALITY IS THE STRONG DIRECTION; INEQUALITY IS NOT. Two rows sharing a stamp
were computed against one corpus (to the collision bound below). Two rows with
DIFFERENT stamps were not necessarily computed against different corpora — the
digest covers a superset of any one decision's inputs, so an unrelated
instrument's revision rotates the pass stamp for every row in it. That asymmetry
is deliberate and is the conservative direction; it is stated because "different
stamp ⇒ different corpus" is the reading a later reader will reach for.

⚠ A NULL is UNKNOWN, not a value. Every pre-#2414 row carries NULL, and an audit
that grouped NULLs together — or compared them with ``IS NOT DISTINCT FROM`` —
would assert common provenance for rows whose provenance is precisely what is
unrecoverable.

⚠ COLLISION EXPOSURE IS TWO-LEVEL. Each component digest is independently
truncated to 64 bits before the outer digest, so an inner collision hides that
component's difference permanently rather than being re-randomised by the outer
hash. Both levels carry the same ~2**32 birthday bound. The exposure is
acceptable only because this is not key material: a collision degrades an audit
annotation and cannot admit a wrong row.

⚠ THIS SUPERSEDES THE PARENT SPEC'S CONTRACT 2 ("O(1) per pass, never per
signal"), and says so rather than quietly differing. The construction is
O(bars-read) ONCE per pass, shared by every strategy in it — strictly cheaper
than the per-signal O(history) that contract was written to exclude. It is also
the only construction available that meets contract 1, because ``price_daily``
carries no ``updated_at``: there is no metadata shortcut to "a bar was rewritten
in place", so the values must be read, and the scan already reads them.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from decimal import Decimal
from typing import Final

from app.services.indicator_series import BarSeries

#: ⚠ IN THE PAYLOAD, not merely beside it. The parent spec's checkpoint 1 caught
#: exactly this gap in its first draft: a ``..._RULE_VERSION`` constant that
#: appeared in neither the payload nor the key cannot rotate anything. Bump this
#: whenever the encoding below changes in any way that could make two runs over
#: one corpus disagree — or make two runs over different corpora agree.
CORPUS_GENERATION_RULE_VERSION: Final = "corpus-generation-v1"

#: 16 hex = 64 bits, deliberately wider than ``strategy_version``'s 12. That 12
#: is a legibility choice for a value an operator reads and types; a generation
#: is only ever compared. A 64-bit collision is expected at ~2**32 distinct
#: corpus states and would make two different corpora LOOK identical in an
#: audit — it cannot admit a wrong row, because this is not key material.
_DIGEST_BYTES: Final = 8
GENERATION_PATTERN: Final = r"^[0-9a-f]{16}$"

#: The fields of a bar that a strategy can read. ⚠ Fixed and ordered here rather
#: than taken from the row's keys: ``dict`` iteration order is insertion order,
#: which is psycopg's column order, which is a property of the SELECT rather
#: than of the corpus.
_BAR_FIELDS: Final = ("open", "high", "low", "close", "volume")

_FIELD_SEP: Final = "\x1f"
_RECORD_SEP: Final = "\x1e"
_NULL: Final = "~"


def encode_value(value: Decimal | None) -> str:
    """One bar field, encoded so that two DIFFERENT stored values never collide.

    ⚠⚠ THE PROPERTY IS INJECTIVITY, NOT CANONICALITY, AND THE DIFFERENCE IS WHY
    THIS IS ``str`` AND NOT THE PARENT SPEC'S ``as_tuple()`` FORM.

    The parent requires a context-free canonical form because
    ``Decimal.normalize()`` is context-dependent and lossy. That requirement is
    for a digest whose two sides must compare EQUAL across runs. This digest
    needs the weaker half: two different stored values must never produce one
    string. One value producing two strings is a *spurious* rotation, and a
    spurious rotation costs an audit annotation and nothing else — the stamp
    authorises no replacement, because it is not key material.

    ⚠ ``str(Decimal)`` IS context-dependent in general and the claim here is
    bounded by the COLUMN TYPE rather than asserted generally. Reproduced:
    ``str(Decimal("1e30"))`` is ``1E+30`` at the default ``capitals = 1`` and
    ``1e+30`` at ``capitals = 0``. Exponent form needs a non-negative exponent;
    ``price_daily.open/high/low/close`` is ``numeric(18,6)`` and ``volume`` is
    ``numeric(20,4)``, and psycopg's ``NumericLoader`` builds the value with
    ``Decimal(data.decode())`` from Postgres' own text output, which always
    carries the column's scale. So the exponent is ``-6`` / ``-4`` on every
    stored value and plain form is the only reachable form.
    ``tests/test_corpus_generation.py`` pins that bound against a mutated
    context and both columns' extremes, so it is a test rather than a comment.

    ⚠⚠ NON-FINITE VALUES REACH HERE AND ARE ENCODED, NOT REFUSED. An earlier
    draft claimed ``load_masked_bars`` excludes them; it does not, and the
    correction is worth keeping. That loader compares **``open`` only**
    (``open_ if (open_ is not None and open_ > 0) else None``,
    ``price_masked_bars.py:219``) — ``high``, ``low``, ``close`` and ``volume``
    are carried through on the quarantine flags alone, with no comparison. So a
    PostgreSQL ``numeric`` ``NaN`` in any of those four arrives intact. ``str``
    renders it ``NaN``, which no finite value can produce, so it is
    distinguishable rather than silently equal to something.

    ⚠ A ``NaN`` in ``open`` is the one field that does not reach here, and for a
    reason that is context-dependent rather than structural: ``Decimal("NaN") >
    0`` raises ``InvalidOperation`` under the DEFAULT traps, so the loader fails
    before the digest. With that trap disabled the comparison returns false and
    the value masks to ``None`` instead. Neither path reaches this function with
    a ``NaN`` open; both are recorded because "the loader excludes it" is true
    for only one of them.

    A digest that refused a value the corpus can hold would be a digest that
    cannot describe the corpus, which is why this encodes rather than validates.

    ⚠ ``None`` (a masked field) encodes to a sentinel no ``Decimal`` can
    produce. Masking is an INPUT to the decision — the mask is why the strategy
    saw an absence — so a bar that becomes masked must rotate the generation.
    """
    if value is None:
        return _NULL
    return str(value)


def _digest(parts: Iterable[str]) -> str:
    hasher = hashlib.blake2b(digest_size=_DIGEST_BYTES)
    hasher.update(_RECORD_SEP.join(parts).encode("utf-8"))
    return hasher.hexdigest()


class CorpusGenerationBuilder:
    """Accumulates the pass's corpus digest while the scan loop reads it.

    ⚠ THE PLACEMENT IS THE DESIGN. The parent spec's checkpoint 1 killed a
    per-signal bar-prefix digest on two findings, and both are properties of
    where the digest is taken rather than of what it hashes:

    1. ``_PendingMember.series`` is *"A TRIMMED SLICE — the window bars plus the
       ONE bar after them"*, so a digest taken at the WRITER covers the scan
       window rather than the corpus. This one is taken at the LOADER, so the
       trimming cannot reach it.
    2. The regime series, the unresolved breaks and the cross-sectional peer
       panel are inputs a single instrument's bars do not cover. The peer panel
       is derived from the same bars and is covered by construction; the other
       two are folded in explicitly by :meth:`add_regime` and
       :meth:`add_unresolved_breaks`.

    ⚠ ``add_series`` must be called in ASCENDING instrument order and that is
    enforced, not documented. The digest streams rather than buffering — holding
    3.36M bars to sort them at the end would be the full-corpus materialisation
    the scan spec calls unsafe — so caller order IS the digest, and a caller that
    reordered its loop would silently produce a different stamp for one corpus.
    ``run_signal_scan`` iterates ``eligible``, which is ``sorted(...)``.
    """

    def __init__(
        self,
        *,
        frontier_date: date,
        quarantine_rule_set_version: str,
        rule_version: str = CORPUS_GENERATION_RULE_VERSION,
    ) -> None:
        # ⚠ The payload is delimiter-framed, not length-framed, so a version
        # string carrying a separator or an `=` could make two different payloads
        # serialise identically. Dates and digests cannot contain them; opaque
        # version strings can, so they are the only fields that need the check —
        # and they get it here rather than in a comment saying they "should not".
        for name, value in (
            ("rule_version", rule_version),
            ("quarantine_rule_set_version", quarantine_rule_set_version),
        ):
            if not value or any(character in value for character in (_FIELD_SEP, _RECORD_SEP, "=")):
                raise ValueError(
                    f"{name} {value!r} must be non-empty and free of the payload's delimiters — "
                    "the serialisation is delimiter-framed, so an embedded separator makes two "
                    "different payloads indistinguishable"
                )
        self._rule_version = rule_version
        self._frontier_date = frontier_date
        self._quarantine_rule_set_version = quarantine_rule_set_version
        self._components: dict[str, str] = {}
        self._bars = hashlib.blake2b(digest_size=_DIGEST_BYTES)
        self._last_instrument_id: int | None = None
        self._bar_count = 0
        self._finished = False

    @property
    def bar_count(self) -> int:
        """Bars folded in so far — reported by the scan, never stored."""
        return self._bar_count

    def _set(self, name: str, value: str) -> None:
        if self._finished:
            raise RuntimeError(f"corpus generation already finished; {name!r} arrived too late")
        if name in self._components:
            raise RuntimeError(f"corpus generation component {name!r} was already recorded")
        self._components[name] = value

    def add_spans(self, spans: Mapping[int, object]) -> None:
        """Every **loadable** instrument's span, not only the eligible ones.

        ⚠ ALL LOADABLE, because a stale name excluded from ``eligible`` still
        reaches the cross-sectional union calendar. Hashing only the eligible
        subset would leave a real input outside the stamp.

        ⚠⚠ A SPAN IS A SUMMARY AND IS NOT SUFFICIENT ON ITS OWN — which is why
        :meth:`add_panel_calendars` exists beside it. ``InstrumentBarSpan``
        carries ``last_bar`` and ``bars``, so a stale instrument whose INTERIOR
        dates move while its count and last bar hold is invisible here, and its
        bars are never folded in because it is not eligible. Caught at
        checkpoint 2 with a reproduction: two different stale-date sets gave
        rebalance dates ``2026-02-02`` and ``2026-02-03`` under one identical
        stamp. The span digest is kept because it is the cheap detector for the
        common case (a name appearing, vanishing, or growing); the calendar
        digest is what makes the pair complete.
        """
        self._set(
            "spans",
            _digest(
                f"{instrument_id}{_FIELD_SEP}{getattr(span, 'last_bar')}{_FIELD_SEP}{getattr(span, 'bars')}"
                for instrument_id, span in sorted(spans.items())
            ),
        )

    def add_panel_calendars(self, panel_dates: Mapping[str, frozenset[date]]) -> None:
        """The union calendars this pass actually published, per strategy.

        ⚠ THE DATES THEMSELVES, not a summary of them. ``_publish_decision_calendars``
        reads every LOADABLE instrument's dates — a superset of ``eligible`` — and the
        result decides which bars a cross-sectional strategy rebalances on. A
        revision to a stale name's interior history therefore changes a verdict
        while moving neither its bar count nor its last bar, which is exactly the
        gap :meth:`add_spans` cannot close (checkpoint 2, P2).

        Keyed by strategy because the calendars are: S-2 and S-10 rank different
        panels. A pass with no cross-sectional plan records an EMPTY component
        rather than omitting one — "no calendar" and "calendar not recorded" must
        stay distinguishable, and :meth:`finish` raises on the second.
        """
        self._set(
            "panel_calendars",
            _digest(
                f"{strategy_id}{_FIELD_SEP}" + ",".join(day.isoformat() for day in sorted(dates))
                for strategy_id, dates in sorted(panel_dates.items())
            ),
        )

    def add_unresolved_breaks(self, breaks: Mapping[int, Sequence[date]]) -> None:
        """``segmented_signals`` evaluates each segment with fresh state.

        Resolving a break changes a verdict without changing a single bar, so
        the break set is an input in its own right.
        """
        self._set(
            "breaks",
            _digest(
                f"{instrument_id}{_FIELD_SEP}" + ",".join(day.isoformat() for day in dates)
                for instrument_id, dates in sorted(breaks.items())
            ),
        )

    def add_regime(self, classification: Sequence[tuple[date, object]]) -> None:
        """The benchmark classification, WITH its membership.

        ⚠⚠ MEMBERSHIP IS HASHED, NOT JUST THE VALUES. ``for_dates`` separates
        two kinds of ``None`` — a date the benchmark traded but could not yet be
        classified (warm-up, present in the map with a ``None`` value) and one it
        did not trade at all (absent from the map, and therefore
        ``not_evaluable``). They produce different reason codes. Hashing the
        values alone would lose exactly that distinction, so the pairs come from
        the map's own items and an absent date is simply not in this sequence.
        """
        self._set(
            "regime",
            _digest(
                f"{day.isoformat()}{_FIELD_SEP}{_NULL if label is None else label}" for day, label in classification
            ),
        )

    def add_series(self, instrument_id: int, series: BarSeries) -> None:
        """Fold in one instrument's masked bars, exactly as the strategies read them."""
        if self._finished:
            raise RuntimeError(f"corpus generation already finished; instrument {instrument_id} arrived too late")
        if self._last_instrument_id is not None and instrument_id <= self._last_instrument_id:
            raise ValueError(
                f"instrument {instrument_id} was folded in after {self._last_instrument_id}: the bar digest "
                "streams, so the caller's order IS the digest and it must be strictly ascending"
            )
        self._last_instrument_id = instrument_id
        parts = [str(instrument_id)]
        for day, row in zip(series.dates, series.rows, strict=True):
            parts.append(day.isoformat())
            parts.append(_FIELD_SEP.join(encode_value(row.get(field)) for field in _BAR_FIELDS))
        self._bars.update(_RECORD_SEP.join(parts).encode("utf-8"))
        self._bars.update(_RECORD_SEP.encode("utf-8"))
        self._bar_count += len(series)

    def finish(self) -> str:
        """The 16-hex stamp. Every component must have been recorded.

        ⚠ Missing components RAISE rather than defaulting. A generation computed
        without the regime would be indistinguishable from one computed with it,
        which is the one failure this value must not have.
        """
        if self._finished:
            raise RuntimeError("corpus generation was already finished")
        missing = {"spans", "panel_calendars", "breaks", "regime"} - self._components.keys()
        if missing:
            raise RuntimeError(f"corpus generation is missing component(s) {sorted(missing)}")
        self._finished = True
        payload = {
            **self._components,
            "bars": self._bars.hexdigest(),
            "frontier_date": self._frontier_date.isoformat(),
            "quarantine_rule_set_version": self._quarantine_rule_set_version,
            "rule_version": self._rule_version,
        }
        # ⚠ The inner digests are opaque, so the OUTER form is specified rather
        # than delegated to `json.dumps(sort_keys=True)`: sorting an object's
        # keys canonicalises nothing about values it cannot see into.
        return _digest(f"{key}={payload[key]}" for key in sorted(payload))
