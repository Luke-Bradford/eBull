"""The promoted capture certificate (#2840).

``tests/test_2840_forward_daily_provenance.py`` already pins the RULE's
behaviour and now exercises it through this module unchanged — that suite is the
regression evidence for the move and is not duplicated here.

What this file adds is the two things the move itself introduced: the
equivalence that licenses dropping the ``expected_bars`` hop, and the direction
of the error in the calendar-date proxy that was nearly shipped instead.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

from app.services import bar_capture_certificate as module_under_test
from app.services.bar_capture_certificate import (
    CERTIFIED_BUCKET,
    next_session_open_utc,
    nominality_bucket,
)
from app.services.market_calendar import us_market_status
from scripts.census_2840_forward_daily_provenance import expected_bars

_NY = ZoneInfo("America/New_York")


class TestTheDroppedHopIsEquivalent:
    """``next_session_open_utc`` used ``expected_bars(day)`` as "is this a session".

    Asserting the two agree on one date would prove nothing; the point of the
    simplification is that they agree EVERYWHERE, so it is checked over a year
    that contains every closure shape the calendar knows about.
    """

    def test_a_positive_bar_expectation_is_exactly_an_open_session(self) -> None:
        day = date(2026, 1, 1)
        checked = 0
        while day < date(2027, 1, 1):
            assert (expected_bars(day) > 0) == (us_market_status(day) != "closed"), day
            checked += 1
            day += timedelta(days=1)
        assert checked == 365

    def test_the_year_actually_contains_all_three_session_kinds(self) -> None:
        """Without this the equivalence above could hold over a vacuous year."""
        statuses = {us_market_status(date(2026, 1, 1) + timedelta(days=offset)) for offset in range(365)}
        assert statuses == {"open", "half_day", "closed"}


class TestTheCalendarDateProxyIsWrongAndInWhichDirection:
    """A "captured on the same New York date" proxy was drafted and withdrawn.

    It is not merely less precise: it refuses bars for which no session open
    intervened, which is the whole content of the certificate. The Friday/Saturday
    case below is the shape every discarded bar has; the CURRENT count is printed
    by the census arm (see ``bar_capture_certificate``'s module docstring) and is
    deliberately not frozen here, because it moves with every harvest.
    """

    def _same_ny_date(self, bar_time: datetime, captured_at: datetime) -> bool:
        return captured_at.astimezone(_NY).date() == bar_time.astimezone(_NY).date()

    def test_a_weekend_capture_is_certified_but_the_proxy_refuses_it(self) -> None:
        # Friday 2026-09-18, last RTH 30m bar starts 15:30 ET / 19:30 UTC.
        bar_time = datetime(2026, 9, 18, 19, 30, tzinfo=UTC)
        # Captured 02:00 ET Saturday. Next open is Monday 09:30 ET.
        captured_at = datetime(2026, 9, 19, 6, 0, tzinfo=UTC)

        assert nominality_bucket(bar_time, captured_at) == CERTIFIED_BUCKET
        assert self._same_ny_date(bar_time, captured_at) is False

    def test_the_proxy_never_certifies_what_the_rule_refuses(self) -> None:
        """The error is one-directional, which is why it reads as conservative."""
        bar_time = datetime(2026, 9, 18, 19, 30, tzinfo=UTC)
        for hours in range(1, 24 * 14):
            captured_at = bar_time + timedelta(hours=hours)
            if self._same_ny_date(bar_time, captured_at):
                assert nominality_bucket(bar_time, captured_at) == CERTIFIED_BUCKET, hours


class TestTheHorizonIsNotDeterminableRatherThanAbsent:
    def test_no_session_inside_the_horizon_returns_none(self) -> None:
        assert next_session_open_utc(date(2026, 9, 18), horizon_days=1) is None

    def test_an_undeterminable_next_open_REFUSES(self) -> None:
        """⚠ An earlier draft of this test asserted the opposite, and was wrong.

        It claimed exhausting the 30-day search was "safe HERE" because the
        horizon exceeds any real closure — an argument, and an unverified
        historical one, standing in for a guarantee on the single branch where
        the rule has no information. Fail-open on absence is precisely what this
        module exists to refuse.

        Refusing also demotes ``horizon_days`` to a plain compute bound: the
        constant can be wrong without any verdict being wrong.
        """
        from app.services.bar_capture_certificate import UNDETERMINABLE_BUCKET, nominality_bucket

        bar_time = datetime(2026, 9, 18, 19, 30, tzinfo=UTC)
        with patch.object(module_under_test, "next_session_open_utc", lambda *a, **k: None):
            assert nominality_bucket(bar_time, bar_time + timedelta(minutes=30)) == UNDETERMINABLE_BUCKET

    def test_a_normal_capture_is_unaffected_by_that_refusal(self) -> None:
        """The control: without it the test above could pass on a broken rule."""
        bar_time = datetime(2026, 9, 18, 19, 30, tzinfo=UTC)
        assert nominality_bucket(bar_time, bar_time + timedelta(minutes=30)) == CERTIFIED_BUCKET


class TestTheWriterStampsAnUpperBoundOnTheObservation:
    """``sql/402``'s reason, pinned where it can drift (#2840, Codex checkpoint 2).

    ⚠ THIS IS DRIFT DETECTION, NOT PROTECTION. It reads the statement text, so
    it cannot catch a caller that writes the column some other way — only a
    later edit that quietly puts ``now()`` back. The protection is the column
    default in ``sql/402`` plus the literal in the statement; this test is what
    tells the next person the literal is load-bearing rather than incidental.

    Why it matters: ``now()`` is ``transaction_timestamp()``, so under a caller
    that already holds a transaction the stamp can PRECEDE the observation, and
    ``nominality_bucket`` would certify a post-open observation as pre-open. A
    later stamp can only refuse, which is the safe direction.
    """

    def test_the_insert_names_captured_at_as_a_literal_clock_timestamp(self) -> None:
        from app.services.strategy_observation_storage import _INSERT_INTRADAY_BAR

        assert "captured_at" in _INSERT_INTRADAY_BAR
        assert "clock_timestamp()" in _INSERT_INTRADAY_BAR

    def test_the_stamp_is_not_caller_suppliable_and_is_not_a_transaction_timestamp(self) -> None:
        from app.services.strategy_observation_storage import _INSERT_INTRADAY_BAR

        assert "%(captured_at)s" not in _INSERT_INTRADAY_BAR
        assert "now()" not in _INSERT_INTRADAY_BAR


class TestTheVersionCoversWhatTheVerdictDependsOn:
    """A bare rule id would make two different admission decisions look identical.

    The verdict is a function of the session calendar (an added extraordinary
    closure moves the next open) and of the tier bar lengths (they decide when a
    bar completed). Neither is owned here, so both are composed into the
    exported version — the ``INPUT_RULE_SETS`` argument one layer down.
    """

    def test_the_version_carries_the_calendar_and_the_tier_lengths(self) -> None:
        from app.services.bar_capture_certificate import (
            CAPTURE_CERTIFICATE_RULE_ID,
            CAPTURE_CERTIFICATE_VERSION,
            _tier_minutes_hash,
        )
        from app.services.market_calendar import RULE_SET_VERSION as CALENDAR_RULE_SET_VERSION

        assert CAPTURE_CERTIFICATE_VERSION.startswith(f"{CAPTURE_CERTIFICATE_RULE_ID}+")
        assert f"+cal-{CALENDAR_RULE_SET_VERSION}" in CAPTURE_CERTIFICATE_VERSION
        assert f"+tiers-{_tier_minutes_hash()}" in CAPTURE_CERTIFICATE_VERSION

    def test_a_changed_tier_length_moves_the_hash(self) -> None:
        """⚠ The dependency is exercised, not asserted — a component that is IN
        the string but never moves would pass the test above and fail the job."""
        from unittest.mock import patch

        from app.services import bar_capture_certificate as module
        from app.services.strategy_observation_storage import INTRADAY_TIERS, IntradayTier

        before = module._tier_minutes_hash()
        widened = dict(INTRADAY_TIERS)
        widened["30m"] = IntradayTier(
            "30m",
            minutes_per_bar=60,
            max_instruments=1_000,
            retention_days=None,
            retention_months=24,
            partition_granularity="month",
        )
        with patch.object(module, "INTRADAY_TIERS", widened):
            assert module._tier_minutes_hash() != before


class TestAdmissionRefusesWhatTheArithmeticCannotSpeakFor:
    """``capture_certificate`` is the admission gate; ``nominality_bucket`` is its arithmetic.

    The split exists because the calendar arithmetic is IDENTICAL for a bar whose
    stamp is trustworthy and one whose stamp may precede its observation. Before
    ``sql/402`` the column defaulted to ``now()``, which under a caller-held
    transaction can be earlier than the fetch — so a pre-migration row can be
    arithmetically ``before_next_open`` and still not admissible.
    """

    BAR = datetime(2026, 9, 18, 19, 30, tzinfo=UTC)
    CAPTURE = datetime(2026, 9, 18, 20, 5, tzinfo=UTC)

    def test_a_row_predating_the_cutover_is_unverifiable_not_certified(self) -> None:
        from app.services.bar_capture_certificate import (
            UNVERIFIABLE_BUCKET,
            capture_certificate,
            nominality_bucket,
        )

        cutover = datetime(2026, 9, 20, tzinfo=UTC)
        # The arithmetic says the bar is clean...
        assert nominality_bucket(self.BAR, self.CAPTURE) == CERTIFIED_BUCKET
        # ...and admission still refuses it, because the stamp predates sql/402.
        assert capture_certificate(self.BAR, self.CAPTURE, capture_semantics_from=cutover) == UNVERIFIABLE_BUCKET

    def test_a_row_after_the_cutover_passes_THIS_gate_and_meets_the_next(self) -> None:
        """⚠ It does NOT become ``before_next_open``: the provider-rewrite gate
        is downstream and currently refuses. What this asserts is that the
        CUTOVER stopped being the reason — the verdict moved off
        ``unverifiable_capture_semantics``, which is the only thing this layer
        owns. See ``TestTheOpenBoundsEconomicEffectNotTheProvidersRewrite``."""
        from app.services.bar_capture_certificate import (
            UNVERIFIABLE_BUCKET,
            UNVERIFIED_PROVIDER_BUCKET,
            capture_certificate,
        )

        cutover = datetime(2026, 9, 18, tzinfo=UTC)
        verdict = capture_certificate(self.BAR, self.CAPTURE, capture_semantics_from=cutover)
        assert verdict != UNVERIFIABLE_BUCKET
        assert verdict == UNVERIFIED_PROVIDER_BUCKET

    def test_an_unapplied_migration_refuses_everything(self) -> None:
        """``None`` is the explicit "not applied here" answer, and it fails closed."""
        from app.services.bar_capture_certificate import UNVERIFIABLE_BUCKET, capture_certificate

        assert capture_certificate(self.BAR, self.CAPTURE, capture_semantics_from=None) == UNVERIFIABLE_BUCKET

    def test_admission_still_refuses_a_late_capture_after_the_cutover(self) -> None:
        """The cutover admits a row to JUDGEMENT; it does not certify it."""
        from app.services.bar_capture_certificate import capture_certificate

        late = datetime(2026, 9, 22, 14, 0, tzinfo=UTC)
        cutover = datetime(2026, 9, 18, tzinfo=UTC)
        assert capture_certificate(self.BAR, late, capture_semantics_from=cutover).startswith("after_")


class TestTheOpenBoundsEconomicEffectNotTheProvidersRewrite:
    """The P1 from Codex checkpoint 2, and the reason nothing certifies today.

    A session open bounds when a corporate action becomes ECONOMICALLY
    effective. It does not bound when the PROVIDER rewrites its own history — if
    eToro pre-adjusts a Friday candle ahead of a Monday split, ``captured_at`` is
    still before the next open and the level is already re-based. So the open
    test is NECESSARY, not SUFFICIENT, and ``91267518`` left the provider's
    behaviour deliberately unverified.
    """

    def test_a_clean_post_cutover_bar_is_refused_on_provider_grounds(self) -> None:
        from app.services.bar_capture_certificate import (
            PROVIDER_REWRITE_TIMING_VERIFIED,
            UNVERIFIED_PROVIDER_BUCKET,
            capture_certificate,
            nominality_bucket,
        )

        assert PROVIDER_REWRITE_TIMING_VERIFIED is False
        bar = datetime(2026, 9, 18, 19, 30, tzinfo=UTC)
        capture = datetime(2026, 9, 18, 20, 5, tzinfo=UTC)
        cutover = datetime(2026, 9, 18, tzinfo=UTC)

        assert nominality_bucket(bar, capture) == CERTIFIED_BUCKET
        assert capture_certificate(bar, capture, capture_semantics_from=cutover) == UNVERIFIED_PROVIDER_BUCKET

    def test_only_the_certifying_verdict_is_downgraded(self) -> None:
        """``after_n_opens`` keeps its count — it is how many opportunities a
        later split check must rule out, and relabelling would destroy it."""
        from app.services.bar_capture_certificate import capture_certificate

        bar = datetime(2026, 9, 18, 19, 30, tzinfo=UTC)
        late = datetime(2026, 9, 22, 14, 0, tzinfo=UTC)
        cutover = datetime(2026, 9, 18, tzinfo=UTC)
        assert capture_certificate(bar, late, capture_semantics_from=cutover) == "after_2_opens"

    def test_the_two_refusals_are_distinguishable(self) -> None:
        """Conflating them would hide which evidence is still missing."""
        from app.services.bar_capture_certificate import UNVERIFIABLE_BUCKET, UNVERIFIED_PROVIDER_BUCKET

        assert UNVERIFIABLE_BUCKET != UNVERIFIED_PROVIDER_BUCKET


class TestImpossibleIsNamedBecauseItMustBeExcluded:
    def test_a_capture_before_completion_is_impossible(self) -> None:
        from app.services.bar_capture_certificate import IMPOSSIBLE_BUCKET, nominality_bucket

        bar = datetime(2026, 9, 18, 19, 30, tzinfo=UTC)
        assert nominality_bucket(bar, bar + timedelta(minutes=10)) == IMPOSSIBLE_BUCKET

    def test_an_impossible_row_is_same_session_by_construction(self) -> None:
        """Which is why the census excludes it from the proxy comparison rather
        than counting it as 'refused by the rule' — it would make the
        must-be-zero cell non-zero for an unrelated reason."""
        from app.services.bar_capture_certificate import IMPOSSIBLE_BUCKET, nominality_bucket

        bar = datetime(2026, 9, 18, 19, 30, tzinfo=UTC)
        capture = bar + timedelta(minutes=10)
        assert nominality_bucket(bar, capture) == IMPOSSIBLE_BUCKET
        assert capture.astimezone(_NY).date() == bar.astimezone(_NY).date()
