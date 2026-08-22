"""Phase 5e-3 — criterion 6's declared trial count, and what it is allowed to omit.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §8 (stage 5e-3),
acceptance C6. Parent ``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md``
criterion 6. Consumer: ``app/services/deflated_sharpe.py``. Refs #2240.

⚠⚠ THIS FILE IS A DECLARATION, NOT A DERIVATION.

Criterion 6: *"Its trial count must include every variant evaluated — abandoned
branches, manual eyeballing, and parameter values tried and discarded. An honest
trial count is the whole mechanism; an undercounted one makes the correction
decorative."* And the phase spec's C6: an undeclared count *"fails; it does not
default to the number of shipped strategies"*.

No query can produce this number. A trial that was eyeballed in a session and
dropped left no row anywhere, and the four shipped strategies are exactly the
number criterion 6 names as the wrong answer. So the register is a hand-declared
artefact reviewed in git, every entry carrying the evidence it was drawn from,
and appended by whichever PR runs the next variant.

⚠⚠ NO ENTRY CARRIES A MEASURED SHARPE, AND THAT IS DELIBERATE.

``V[{SR_n}]`` needs the trials' estimated Sharpes, and writing them here as
literals would be a derived statistic hardcoded into source — the thing
``.claude/CLAUDE.md`` forbids outright, because it goes stale silently the moment
the derivation changes and it goes stale where a reader trusts it most. So the
register declares WHICH TRIALS EXIST and the caller supplies what it measured
THIS RUN, keyed by ``trial_id``. ``sharpe_variance`` refuses a key it does not
recognise, which is what stops the two drifting apart.

⚠⚠ WHAT COUNTS AS A TRIAL: A SEARCH OF THE DATA, NOT A DESIGN.

The multiple-testing correction exists because searching data repeatedly
produces a winner by chance. A rule that was designed, reviewed and never run
against price data cannot have contributed a chance winner, so it is NOT a
trial. A rule that was run and then discarded IS one, however bad it looked.

By that test, and stated so a later reader can contest it rather than guess:

- **counted** — the four shipped strategies, and every #2260 RSI arm, including
  the ones whose result was rejected and the original non-causal measurement
  that turned out to be an artefact. An artefact is still a search.
- **NOT counted** — S-5 (support/resistance retest) and S-6 (Fibonacci
  retracement). Both are specified in the parent catalogue §4 and both are
  blocked on #2279 with no evaluation against price data. ⚠ S-6's *"last swing
  was a look-ahead trap"* correction is likewise absent: it was a review finding
  against the SPEC (parent §"What the review changed", item 3), not a
  measurement, so nothing was searched.

⚠⚠ THE UNIT IS A SEARCH, NOT A CANDIDATE STRATEGY, AND THAT IS SETTLED.

Bailey/López de Prado's ``M`` is nominally the number of candidate strategy
Sharpes a maximum could have been taken over, which would exclude a conditioning
diagnostic. This register already rejected that narrower reading: the merged
``short-horizon-search-session-2026-08-09`` entry charges *"25 breadth cells, 12
confluence buckets, 13 individual conditions"* — diagnostics, not candidates —
because criterion 6 names *"manual eyeballing"* explicitly. #2600's
reconstruction stayed consistent with that unit rather than re-litigating it: a
mixed population is safe in the direction that matters (a larger ``M`` raises
``SR_0`` and LOWERS the DSR), and switching units mid-register would make the 101
floor incommensurable with everything added after it. ⚠ The cost is that a DSR
computed here is conservative by construction; anyone reading a **pass** off it
must know that. A **fail** is unaffected.

⚠⚠ THE ROBUSTNESS FAN IS ONE SEARCH, NOT FOUR.

``ambiguity_arm`` × ``quarantine_arm`` fans every stored evaluation into four
result rows. They are not four trials, because ``check_promotable`` requires them
to pass jointly (``ambiguity_material``, ``quarantine_arms_not_compared``) — the
flattering arm cannot be selected, so no maximum was taken over them. The same
rule is applied to every family here, including where it costs count: the
autocorrelation grid's pooled and year-clustered tables are two inference
treatments of the same 28 effects, so that family is 28 and not 56.

⚠⚠ THE COUNT IS A FLOOR, AND THE BIAS DIRECTION IS THE REASON TO SAY SO.

Sessions before this register existed did not record their variants, so trials
are missing from it. Under-counting ``M`` lowers ``N_hat``, which lowers
``SR_0``, which RAISES the Deflated Sharpe — the anti-conservative direction. A
DSR computed here is therefore an UPPER BOUND on the honest one, and a strategy
that fails criterion 6 against this register would fail it harder against a
complete one. ⚠ The converse does not hold and must not be read into a pass.

⚠ #2600's reconstruction narrowed that gap; it did not close it. What it still
does not reach is listed in
``docs/proposals/ta/2026-08-12-trial-register-reconstruction.md`` §"What this
reconstruction still does not reach" — chiefly the pre-ledger parameter
development that SELECTED S-1..S-4's windows and thresholds. The counts below are
evaluations of already-chosen rules.

⚠⚠ FAMILY CORRELATION IS NOT THE REGISTER'S JOB, AND MUST NOT BECOME IT.

Eight per-name-cap arms of one event stream are eight searches, and the register
says eight. Whether they carry eight arms' worth of independent evidence is
equation (9)'s question: ``deflated_sharpe.implied_independent_trials(rho, M)``
shrinks ``M`` to an effective ``N`` using a ``rho`` MEASURED off the trials'
realised return series (``scripts/verify_2240_statistics.py`` P11 asserts exactly
that it is measured, not declared). Discounting correlated arms inside ``M`` as
well would apply the same correction twice, in the anti-conservative direction.

The same asymmetry applies to ``V[{SR_n}]``: it is estimated from the trials
that carry a measured Sharpe, and those are the ones that survived far enough to
be measured. If the unmeasured trials were dropped because they looked bad, the
measured subset understates the spread of trial Sharpes, which again understates
``SR_0`` and raises the DSR. Both biases point the same way, and it is the
flattering one.
"""

from __future__ import annotations

import statistics
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Final

#: Bumped whenever a trial is added or an entry's meaning changes. ⚠ Stored on
#: the result row beside the DSR: a deflated Sharpe means nothing without the
#: trial population it was deflated against, and that population grows.
TRIAL_REGISTER_VERSION: Final = "trial-register-2026-08-15-r7"

#: #2600 Gate D-0.1. Every search this register counts happened at or before this
#: instant; the two durable clocks (``strategy_results_store.created_at`` and
#: ``strategy_holdout_accesses.accessed_at``) both top out at 2026-08-12
#: 06:39:47Z. A search opened AFTER it charges itself under #2599's declaration
#: contract rather than waiting for the next reconstruction.
#:
#: ⚠ THIS IS A DECLARATION, NOT AN ENFORCEMENT. Nothing here intercepts a
#: read-only script or an ad-hoc SQL session, so an undeclared post-cutoff search
#: is as invisible as a pre-ledger one. Closing that is #2599's scope; the
#: constant exists so #2599 has a boundary to enforce from.
TRIAL_REGISTER_CUTOFF: Final = datetime(2026, 8, 12, 7, 0, tzinfo=UTC)

#: ``sql/333``'s identity CHECK bounds every declaration identity field at 200
#: characters. Mirrored rather than imported because SQL cannot export it, in the
#: same spirit as ``PreregDeclaration``'s deliberate duplication of those CHECKs.
_IDENTITY_LIMIT: Final = 200


class TrialExactness(StrEnum):
    """Whether a declaration's ``searches`` is the count or a lower bound on it.

    ⚠ REQUIRED on every entry — no default. The flag is a claim about evidence,
    and a default would let an author skip the one judgement it records. Guessing
    it wrong in either direction is worse than being made to state it: ``EXACT``
    over-claims precision the register exists to avoid, ``FLOOR`` on a fully
    enumerated family invites a later reader to pad it.
    """

    #: Every arm is individually enumerated by a durable artefact — a database
    #: census, a code-level grid at the commit that ran it, or a result page's own
    #: table. The number IS the search count, not an estimate of it.
    EXACT = "exact"

    #: The true number of searches is AT LEAST this, and the excess is not
    #: recoverable. ⚠ A floored count admits only arms the evidence shows actually
    #: ran; where the evidence bounds a range it takes the SMALLEST count
    #: consistent with it. Gate D-0.1 asks for "a visible overcount over false
    #: precision", and this is narrower on purpose: an unevidenced padding number
    #: is not conservative, it is indistinguishable from an entry invented to make
    #: a DSR look harder-won — which is what ``evidence`` exists to prevent. The
    #: honest conservative move is a defensible floor plus a flag saying it is one.
    FLOOR = "floor"


@dataclass(frozen=True)
class DeclaredTrial:
    """One traceable declaration of variants evaluated against price data.

    ``searches`` is normally one.  It may be greater than one only when a
    historical research session recorded the size and construction of a search
    family but did not retain a durable id for every arm.  Collapsing that
    family to one would under-count ``M`` in the flattering direction; inventing
    one id per lost arm would imply provenance the repository does not have.

    ⚠ ``evidence`` is REQUIRED and non-empty. A trial count is only honest if
    each declaration's count and construction can be checked. A grouped legacy
    declaration does NOT imply individual-arm provenance; an entry nobody can
    trace is indistinguishable from one invented to pad the count in the safe
    direction.
    """

    trial_id: str
    description: str
    #: Where the evaluation is recorded — an issue, a commit, a spec section.
    evidence: str
    #: Whether ``searches`` is enumerated or a lower bound. ⚠ No default: see
    #: ``TrialExactness``.
    exactness: TrialExactness
    #: Number of price-data searches represented by this traceable declaration.
    searches: int = 1
    #: #2829 — the ``(strategy_id, strategy_version)`` whose preregistration
    #: declaration this trial's searches account for, or ``None``.
    #:
    #: ⚠⚠ AT MOST ONE, AND THAT IS THE INVARIANT, NOT A SIMPLIFICATION. The first
    #: draft allowed a tuple of pairs and Codex checkpoint 2 killed it: a trial
    #: may then claim a second declaration while ``searches`` stays where it was,
    #: so the freeze gate would admit a new search that never moved
    #: ``declared_count`` — the exact under-count the gate exists to prevent,
    #: passing the gate. One trial to one declaration makes "this declaration has
    #: its own counted trial" structurally true instead of checked.
    #:
    #: ⚠ EMPTY IS LEGITIMATE AND COMMON, not an omission to fill in. Many entries
    #: here are research SESSIONS (``short-horizon-search-session-2026-08-09``,
    #: ``autocorrelation-term-structure-2026-08-09``) that no declaration
    #: corresponds to; ``strategy_preregistration_declarations`` holds 5 rows
    #: against 30 trials.
    #:
    #: ⚠ Identity is ``(strategy_id, strategy_version)`` because that is
    #: ``sql/333``'s own ``strategy_preregistration_declaration_unique``.
    #: ``contract_version`` is deliberately not part of it, for the same reason.
    #:
    #: ⚠ There is NO naming convention to infer this from, which is why it is
    #: declared. Measured 2026-08-22 across the five stored declarations, the
    #: matching ``trial_id`` was ``strategy_version`` twice, ``strategy_id``
    #: twice, and ``strategy_id + "-v1"`` once.
    declared_for: tuple[str, str] | None = None

    def __post_init__(self) -> None:
        for field_name in ("trial_id", "description", "evidence"):
            if not getattr(self, field_name):
                raise ValueError(f"{field_name} is blank — a present-but-empty declaration declares nothing (#2286)")
        if self.declared_for is not None:
            if len(self.declared_for) != 2:
                raise ValueError(
                    f"{self.trial_id}: declared_for is (strategy_id, strategy_version), got {self.declared_for!r}"
                )
            for part in self.declared_for:
                # Mirrors sql/333's `strategy_preregistration_declaration_identity`
                # CHECK. A pair the table could never hold can never match a row,
                # so it is a typo rather than a mapping — and it would fail SILENT,
                # as a declaration this register does not claim.
                if not isinstance(part, str) or not part.strip() or len(part) > _IDENTITY_LIMIT:
                    raise ValueError(
                        f"{self.trial_id}: declared_for identity {part!r} must be a non-blank string of at most "
                        f"{_IDENTITY_LIMIT} characters (sql/333's identity CHECK)"
                    )
        if type(self.searches) is not int or self.searches < 1:
            raise ValueError(f"searches must be a positive integer, got {self.searches!r}")
        # ⚠ Rejected rather than coerced. A raw string here would pass every
        # `== "floor"` comparison a reader writes and silently fail every
        # `is TrialExactness.FLOOR` one, so `floored_searches` would under-report
        # on an entry that looked correct in the source.
        if not isinstance(self.exactness, TrialExactness):
            raise ValueError(f"exactness must be a TrialExactness, got {self.exactness!r}")


@dataclass(frozen=True)
class TrialRegister:
    """Criterion 6's ``M``, and the ``V[{SR_n}]`` estimator over it."""

    version: str
    trials: tuple[DeclaredTrial, ...]

    def __post_init__(self) -> None:
        if not self.version:
            raise ValueError("register version is blank")
        ids = [trial.trial_id for trial in self.trials]
        if len(ids) != len(set(ids)):
            raise ValueError("trial ids are not distinct — one variant counted twice inflates M silently")
        # #2829. Same failure shape as the id check above, one level down: a
        # declaration claimed by two trials has its searches counted twice in M.
        claimed: dict[tuple[str, str], str] = {}
        for trial in self.trials:
            pair = trial.declared_for
            if pair is None:
                continue
            if pair in claimed:
                raise ValueError(
                    f"declaration {pair[0]}@{pair[1]} is claimed by both {claimed[pair]!r} and "
                    f"{trial.trial_id!r} — one declaration counted twice inflates M silently"
                )
            claimed[pair] = trial.trial_id

    @property
    def declared_count(self) -> int:
        return sum(trial.searches for trial in self.trials)

    @property
    def floored_searches(self) -> int:
        """How much of ``declared_count`` is a lower bound rather than a count.

        ⚠ Reported, never subtracted. A floored family's searches are searches
        that happened; the flag says only that MORE of them happened than the
        register can name. Removing them would move ``M`` in the flattering
        direction, which is the failure this whole module is built against.
        """
        return sum(trial.searches for trial in self.trials if trial.exactness is TrialExactness.FLOOR)

    @property
    def trial_ids(self) -> frozenset[str]:
        return frozenset(trial.trial_id for trial in self.trials)

    def trial_for_declaration(self, strategy_id: str, strategy_version: str) -> DeclaredTrial | None:
        """The trial whose searches account for this declaration, or ``None``.

        ⚠ ``None`` means "``M`` does not count the search this declaration
        represents", which is the under-count criterion 6 calls decorative. It is
        NOT "no declaration exists" — this register knows nothing about which
        declarations have been frozen.

        ⚠ WHAT A MATCH PROVES, precisely: that the register and the declaration
        agree TODAY. It cannot establish that the register counted this search at
        the time a look occurred, because a mapping added after exposure is
        indistinguishable from one that was always there. The prospective
        guarantee comes from ``freeze_preregistration`` refusing an unclaimed
        declaration, not from a lookup here.
        """
        pair = (strategy_id, strategy_version)
        for trial in self.trials:
            if trial.declared_for == pair:
                return trial
        return None

    def sharpe_variance(self, measured: Mapping[str, float]) -> float | None:
        """``V[{SR_n}]`` over the trials measured this run. ``None`` below two.

        ⚠ ``ddof=1``. This is a SAMPLE variance of the trial Sharpes — the
        trials we ran are a sample of the trials that could have been run, which
        is the population equation (1) takes a maximum over. A population
        variance would understate it and so understate ``SR_0``.

        ⚠ RAISES on an unrecognised ``trial_id`` rather than skipping it. A
        measured trial absent from the register is a trial missing from ``M``,
        which is exactly the under-count criterion 6 calls decorative — and
        silently ignoring the key would hide it.

        ⚠⚠ ``searches`` AND ``exactness`` DO NOT ENTER HERE, AND THE OMISSION IS
        A KNOWN BIAS, NOT AN OVERSIGHT. The variance is over the trials MEASURED
        this run, keyed by ``trial_id``, so a family declaring 101 searches
        contributes at most ONE Sharpe. The measured subset is also the subset
        that survived far enough to be measured, so it understates the spread of
        trial Sharpes, which understates ``SR_0`` and RAISES the DSR. Weighting
        by ``searches`` would not repair it — there is no second Sharpe to weight
        — it would only fabricate spread the register never observed.
        """
        unknown = set(measured) - self.trial_ids
        if unknown:
            raise ValueError(
                f"measured Sharpes for undeclared trials {sorted(unknown)} — declare them here or M under-counts the "
                "search that produced them"
            )
        values = list(measured.values())
        if len(values) < 2:
            return None
        return statistics.variance(values)


#: ⚠ THE REPO'S DECLARATION. Every entry was checked against its evidence at
#: implementation time; none is recalled. Append here — do not re-derive.
#:
#: ⚠⚠ #2600 RECONSTRUCTED THIS, IT DID NOT MERELY APPEND TO IT. Six families that
#: were charged as one search each are now charged at their enumerated arm count,
#: and six families that were never charged at all were added. The per-family
#: derivation, the queries that produced each number and the families deliberately
#: NOT counted are in
#: ``docs/proposals/ta/2026-08-12-trial-register-reconstruction.md``. Read it
#: before changing a count here — several of these numbers are the answer to a
#: question a reader will otherwise re-litigate from first principles.
TRIAL_REGISTER: Final = TrialRegister(
    version=TRIAL_REGISTER_VERSION,
    trials=(
        # ⚠⚠ S-1..S-4 ARE COUNTED FROM `strategy_holdout_accesses`, NOT FROM THE
        # RESULT STORE. The access ledger records every LOOK at the hold-out,
        # including `read` eyeballs that wrote no result row, and
        # `check_promotable` already treats it as the complete record
        # (`holdout_accesses_unrecorded`). Per strategy the arithmetic is
        # `evaluate accesses / 4` (the robustness fan) + `in_sample rows / 4` + 1
        # `read`. Both queries are in the reconstruction page.
        #
        # ⚠ FLOOR, for two named populations that left no row at all: the
        # read-only harness runs (`verify_2240_statistics.py`, `probe_2240_*.py`),
        # and — the larger gap — the pre-ledger parameter development that CHOSE
        # these four rules' windows, thresholds and exits. What is counted here is
        # evaluation of an already-selected rule.
        DeclaredTrial(
            trial_id="s1-time-series-momentum",
            description="S-1 time-series momentum: 16 hold-out evaluations, 2 in-sample, 1 audit read.",
            evidence="strategy_holdout_accesses (64 evaluate + 1 read) and strategy_results_store "
            "(8 in_sample rows); docs/proposals/ta/2026-08-12-trial-register-reconstruction.md",
            exactness=TrialExactness.FLOOR,
            searches=19,
        ),
        DeclaredTrial(
            trial_id="s2-cross-sectional-momentum",
            description="S-2 cross-sectional momentum: 16 hold-out evaluations, 2 in-sample, 1 audit read.",
            evidence="strategy_holdout_accesses (64 evaluate + 1 read) and strategy_results_store "
            "(8 in_sample rows); docs/proposals/ta/2026-08-12-trial-register-reconstruction.md",
            exactness=TrialExactness.FLOOR,
            searches=19,
        ),
        DeclaredTrial(
            trial_id="s3-mean-reversion-in-trend",
            description="S-3 mean reversion in an uptrend: 16 hold-out evaluations, 2 in-sample, 1 audit read.",
            evidence="strategy_holdout_accesses (64 evaluate + 1 read) and strategy_results_store "
            "(8 in_sample rows); docs/proposals/ta/2026-08-12-trial-register-reconstruction.md",
            exactness=TrialExactness.FLOOR,
            searches=19,
        ),
        DeclaredTrial(
            trial_id="s4-volatility-compression-breakout",
            description="S-4 volatility compression breakout: 7 hold-out evaluations, 1 audit read.",
            evidence="strategy_holdout_accesses (28 evaluate + 1 read); no in_sample rows; "
            "docs/proposals/ta/2026-08-12-trial-register-reconstruction.md",
            exactness=TrialExactness.FLOOR,
            searches=8,
        ),
        # ⚠ THE #2260 ARMS. Six causal recomputes (three rule variants across two
        # corpora) plus the original non-causal measurement they were run to
        # explain. Each is counted separately because each is a separate search
        # of price data — and they are NEAR-DUPLICATES of one another, which is
        # not a reason to drop them but the reason equation (9)'s correlation
        # term exists. Figures on the issue; none is reproduced here.
        DeclaredTrial(
            trial_id="rsi30-20d-noncausal-s7",
            description="RSI<30 → 20-day forward hit rate, the original non-causal measurement (the 76.8%).",
            evidence="issue #2260 (opening report, spike S7); withdrawn 2026-08-05",
            exactness=TrialExactness.EXACT,
        ),
        DeclaredTrial(
            trial_id="rsi30-20d-overlapping-price-daily",
            description="RSI<30 → 20-day hit, causal Wilder, overlapping triggers, price_daily.",
            evidence="issue #2260 comment 2026-08-05 (full-population recompute)",
            exactness=TrialExactness.EXACT,
        ),
        DeclaredTrial(
            trial_id="rsi30-20d-nonoverlapping-price-daily",
            description="RSI<30 → 20-day hit, causal Wilder, non-overlapping triggers (candidate 3), price_daily.",
            evidence="issue #2260 comment 2026-08-05 (full-population recompute)",
            exactness=TrialExactness.EXACT,
        ),
        DeclaredTrial(
            trial_id="rsi30-20d-quarantined-price-daily",
            description="RSI<30 → 20-day hit, causal Wilder, quarantined bars excluded (candidate 4), price_daily.",
            evidence="issue #2260 comment 2026-08-05 (full-population recompute)",
            exactness=TrialExactness.EXACT,
        ),
        DeclaredTrial(
            trial_id="rsi30-20d-overlapping-research-corpus",
            description="RSI<30 → 20-day hit, causal Wilder, overlapping triggers, research corpus.",
            evidence="issue #2260 comment 2026-08-05 (full-population recompute)",
            exactness=TrialExactness.EXACT,
        ),
        DeclaredTrial(
            trial_id="rsi30-20d-nonoverlapping-research-corpus",
            description="RSI<30 → 20-day hit, causal Wilder, non-overlapping triggers (candidate 3), research corpus.",
            evidence="issue #2260 comment 2026-08-05 (full-population recompute)",
            exactness=TrialExactness.EXACT,
        ),
        DeclaredTrial(
            trial_id="rsi30-20d-quarantined-research-corpus",
            description="RSI<30 → 20-day hit, causal Wilder, quarantined bars excluded (candidate 4), research corpus.",
            evidence="issue #2260 comment 2026-08-05 (full-population recompute)",
            exactness=TrialExactness.EXACT,
        ),
        # ⚠ #2600 raised this from 1 to 8. The result page tabulates its own arms
        # and the original entry charged the family as one: the preregistered
        # 62-session equal-gross primary, its separately-tabulated Long and Short
        # legs, trailing-24 and trailing-36 pooled slices, and the declared
        # 5/20/40-session horizon diagnostics. The matched middle-SUE row is a
        # CONTROL and is excluded.
        DeclaredTrial(
            trial_id="pead-historical-sue-net-income-v1",
            description=(
                "Issuer-deduplicated historical-SUE SEC filing drift: 62-session equal-gross long/short primary, "
                "its long and short legs, trailing-24/36-month slices and 5/20/40-session horizon diagnostics."
            ),
            evidence="docs/proposals/ta/2026-08-10-pead-result.md (§'Preregistered primary result' arm table and "
            "§'Recency and horizon diagnostics'); issue #2476 comment 2026-08-10 (sealed outcome)",
            exactness=TrialExactness.EXACT,
            searches=8,
            declared_for=("pead-historical-sue-net-income", "pead-historical-sue-net-income-v1"),
        ),
        DeclaredTrial(
            trial_id="short-horizon-search-session-2026-08-09",
            description=(
                "Conservative historical-search floor: 25 gap-fade band/era arms, 15 reversal arms, "
                "25 breadth cells, 12 confluence buckets, 13 individual conditions, 6 short arms and 5 stop arms."
            ),
            evidence="docs/proposals/ta/2026-08-09-plan-of-attack.md §2b",
            exactness=TrialExactness.FLOOR,
            searches=101,
        ),
        # ⚠ #2600 raised this from 8 to 15. The page charges eight itself ("those
        # eight evaluations are now charged to the trial [register]") and then
        # reports SEVEN calendar-year returns for the 1%/25% diagnostic arm, which
        # were never charged. An era cut is exactly what the page warns against
        # selecting on — "do not rescue it by selecting a cap, threshold, hold,
        # stop, era, sector" — so it is a search.
        DeclaredTrial(
            trial_id="extreme-shock-portfolio-sizing-stress-v1",
            description=(
                "Frozen extreme-shock event stream under four per-name caps, each with and without the "
                "declared 25% sector cap (8 rejected capital-weighted arms), plus 7 calendar-year cuts of "
                "the 1%/25% diagnostic arm (2020 through the 2026 corpus frontier)."
            ),
            evidence="docs/proposals/ta/2026-08-11-extreme-shock-portfolio-result.md (arm table and the "
            "calendar-return paragraph); issue #2481",
            exactness=TrialExactness.EXACT,
            searches=15,
        ),
        # ⚠ #2600 raised this from 1 to 7: the primary spread, the five reported
        # windows (trailing 36, trailing 24, 2024, 2025, 2026 YTD) and the
        # equal-weight spread. The timing-matched placebo is a CONTROL, excluded.
        DeclaredTrial(
            trial_id="form4-code-p-opportunistic-purchase-v1",
            description=(
                "Purchase-value-weighted long opportunistic Form-4 code-P buys / short routine buys, monthly: "
                "the primary spread, five reported windows and the equal-weight spread."
            ),
            evidence="docs/proposals/ta/2026-08-10-insider-purchase-result.md (§'Sealed result' window table); "
            "https://github.com/Luke-Bradford/eBull/issues/2480#issuecomment-5238836691",
            exactness=TrialExactness.EXACT,
            searches=7,
            declared_for=("form4-code-p-opportunistic-purchase", "form4-code-p-opportunistic-purchase-v1"),
        ),
        # ⚠⚠ THE 2026-08-09 02:28 SCRIPTS (commit 61fb17da), ADDED BY #2600.
        # All three predate the plan-of-attack's §2b floor (03:13:41, dbe5107b),
        # whose seven named families sum to exactly 101 and name none of them.
        # ⚠ A double-count against that floor is possible: §2b is itemised only to
        # family names and cannot be reconciled arm-by-arm. Declaring these
        # separately may count some arms twice; folding them in would count them
        # zero times if §2b never covered them. Under-counting M raises the DSR,
        # so the overcount is the safe error, and this is the one place the
        # reconstruction knowingly takes it.
        DeclaredTrial(
            trial_id="autocorrelation-term-structure-2026-08-09",
            description=(
                "Return-autocorrelation term structure on the research corpus: 7 horizons "
                "(1/5/21/63/126/252/756d) x 4 price bands. The pooled and year-clustered tables are two "
                "inference treatments of the same 28 cells, not 56 searches."
            ),
            evidence="scripts/verify_2437_autocorrelation_term_structure.py at 61fb17da (HORIZONS has 7 entries, "
            "_band returns 4 labels); docs/proposals/ta/2026-08-12-trial-register-reconstruction.md",
            exactness=TrialExactness.FLOOR,
            searches=28,
        ),
        DeclaredTrial(
            trial_id="roll-bounce-spread-recovery-2026-08-09",
            description=(
                "Roll (1984) implied effective spread recovered from return autocovariance and compared with "
                "the calibrated band spread, once per cost_model price band."
            ),
            evidence="scripts/verify_2437_roll_bounce.py at 61fb17da; len(app.services.cost_model.BANDS) == 4",
            exactness=TrialExactness.EXACT,
            searches=4,
        ),
        DeclaredTrial(
            trial_id="insider-purchase-forward-returns-first-look-2026-08-09",
            description=(
                "First look at Form-4 code-P forward excess returns, year-clustered against a matched "
                "random-date control, at 21/63/126/252 sessions. Distinct construction from the later sealed "
                "form4-code-p-opportunistic-purchase-v1 portfolio run."
            ),
            evidence="scripts/verify_2437_insider_forward_returns.py at 61fb17da (HORIZONS has 4 entries)",
            exactness=TrialExactness.EXACT,
            searches=4,
        ),
        # ⚠ FLOOR at 7, not higher. Six arms are evidenced by the result page; the
        # page also records the intended 2026 hold-out as "contaminated by
        # discarded diagnostic runs", which evidences AT LEAST ONE such run and
        # bounds nothing above it. A larger number would be invented. The page's
        # no-model comparator is a CONTROL; its raw-shock, market-only and
        # matched-random challenger arms were preregistered and NOT executed, so
        # by the admission test they are not trials.
        DeclaredTrial(
            trial_id="residual-confluence-v1-development-arms",
            description=(
                "residual-confluence-v1+946d549861cc development arms: calendar-2024 and calendar-2025 primaries, "
                "their broad top predicted-EV decile cuts, their predicted-EV-crosses-zero action boundaries, "
                "and at least one discarded 2026 diagnostic run."
            ),
            evidence="docs/proposals/ta/2026-08-10-residual-confluence-development-result.md; issue #2499",
            exactness=TrialExactness.FLOOR,
            searches=7,
        ),
        # ⚠ FLOOR at the 4 EXECUTED arms, not the 6 preregistered ones. The
        # preregistration declares `signed` and `long_only` across SPY/QQQ/IWM,
        # but an unexecuted design is not a trial, and the census evidences
        # long_only for all three plus a signed SPY diagnostic only. The
        # always-long comparators are CONTROLS; the first census attempt selected
        # no rows and loaded no outcome.
        DeclaredTrial(
            trial_id="etf-intraday-momentum-v1-retained-census",
            description=(
                "etf-intraday-momentum-v1+0b3804ab4111 gross feasibility on retained 30-minute bars: "
                "long_only for SPY, QQQ and IWM plus the signed SPY diagnostic."
            ),
            evidence="docs/proposals/ta/2026-08-10-etf-intraday-momentum-retained-census.md; issue #2502",
            exactness=TrialExactness.FLOOR,
            searches=4,
        ),
        # ⚠ The production `equal_weight_concurrent_v1` column of that page is NOT
        # counted here — those are the S-1..S-4 evaluations already charged above,
        # and charging them again would double-count the same searches.
        DeclaredTrial(
            trial_id="sizing-rule-attribution-2026-08-12",
            description=(
                "Causal sizing-rule attribution: entry_weight_drift_v1 and calendar_month_end_equal_weight_v1 "
                "across the four controls (8 arms), plus the first monthly pass stopped on the month-end "
                "boundary defect. S-4's best/worst rows are the ambiguity fan, not separate arms."
            ),
            evidence="docs/proposals/ta/2026-08-12-sizing-rule-attribution-result.md — §'Entry-weight drift' and "
            "§'Calendar-month-end equal weight' each tabulate 5 rows for 4 strategies because S-4 is split into "
            "best/worst, which is why 8 arms and not 10; §'Boundary correction' records the stopped first monthly "
            "pass. Issue #2430; scripts/verify_2430_sizing_rule_ab.py --window primary-2022-plus",
            exactness=TrialExactness.FLOOR,
            searches=9,
        ),
        # ⚠⚠ THE FIRST ENTRY DECLARED **BEFORE** ITS RUN, AND THE ONLY WAY THAT
        # ORDER CAN HOLD. `evaluate_2582_schedule13d_outcomes.require_outcome_gate`
        # refuses to open C-4's outcomes while its trial id is absent from this
        # register, so the entry has to precede the search — which is what
        # `TRIAL_REGISTER_CUTOFF` means by "a search opened after it charges
        # itself under #2599's declaration contract". Every entry above it was
        # reconstructed after the fact.
        #
        # ⚠ NOT "each arm reads its own bars" — that was the first draft of this
        # entry and it is false. `load_initial_13g_price_windows` loads the 13G
        # challenger population ONCE and arms 4-7 partition it. The count is
        # three searches that load new bars, plus four separately-reported cells
        # of one loaded population, which the merged
        # `short-horizon-search-session-2026-08-09` entry above already charges
        # per cell ("12 confluence buckets, 13 individual conditions").
        #
        # ⚠ The fan-collapse rule ("one search, not four") does NOT rescue arms
        # 1/3/4/5 into one despite their being jointly required: the contract
        # itself Holm-adjusts across random_time, 13g_1b and 13g_1c, and a study
        # that corrects for three tests internally cannot declare one here.
        #
        # ⚠ NOT counted, deliberately: the eight non-paired gates in
        # `_decision_gates` and the three 6-month stability windows. All are
        # computed from the single primary `OutcomeStatistics` — no new bars —
        # and all are conjunctive, so no maximum is taken over them. Same rule
        # that makes the autocorrelation grid 28 and not 56.
        DeclaredTrial(
            trial_id="c4-schedule13d-public-catalyst-v1",
            description=(
                "Sealed Schedule 13D public-catalyst falsification, 7 arms: the clean 13D primary population, "
                "the unfiltered-eligible 13D robustness population, the matched random-time challenger, and the "
                "four initial-13G rule cells (1b, 1c, both, unknown). Declared before the run, which the outcome "
                "gate requires."
            ),
            evidence="docs/proposals/ta/2026-08-12-c4-declaration-gate-binding.md §'Trial register entry'; "
            "scripts/schedule13d_report.py::build_historical_falsification_report enumerates all seven "
            "unconditionally; contract docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json "
            "sha256 8f4424bea0581ba501d9779b93ff9268c65c6f0c899f1a66962bcb260cce895f. Issues #2614, #2582",
            exactness=TrialExactness.EXACT,
            searches=7,
            # ⚠ The one mapping no naming rule would have produced: the trial id
            # is the CONTRACT version, and the declaration's strategy_id drops
            # the `-v1`. Measured against the stored row, not inferred.
            declared_for=("c4-schedule13d-public-catalyst", "schedule13d-public-catalyst-v1"),
        ),
        # ⚠ DECLARED BEFORE THE FIRST BACKTEST. The four robustness rows
        # (ambiguity best/worst x quarantine admitted/masked), the eight pinned
        # recent windows, and the declared regime cohorts are conjunctive
        # reports of one frozen rule. No winner may be selected from them, so
        # each strategy is one search under the register's fan-collapse rule.
        # A later parameter or domain variant is a new entry, even if it keeps
        # the same human-readable strategy name.
        *(
            DeclaredTrial(
                trial_id=strategy_id,
                description=f"{label}: first survivorship-free, cost-aware walk-forward and recent-window run.",
                evidence=(
                    "docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md §0 and §3; "
                    "issue #2437 comment 2026-08-14 (queue item 6 staked before evaluation)"
                ),
                exactness=TrialExactness.EXACT,
            )
            for strategy_id, label in (
                ("s5-support-bounce", "S-5 support bounce"),
                ("s6-resistance-breakout", "S-6 resistance breakout"),
                ("s7-trend-pullback", "S-7 trend pullback"),
                ("s8-range-mean-reversion", "S-8 range mean reversion"),
                ("s9-squeeze-expansion", "S-9 squeeze expansion"),
                ("s10-relative-strength-leader", "S-10 relative-strength leader"),
            )
        ),
        # ⚠ DECLARED BEFORE EITHER CONTROLLED OUTCOME IS OPENED. These are two
        # searches, not one: the S-8 negative control has its own scaled versus
        # unscaled estimand and can falsify an overlay effect that appears on
        # MT-1. The four monthly arms within the pair are jointly required by
        # one difference-in-differences evaluator; no favourable arm can be
        # selected, so the fan-collapse rule makes each pair one search.
        DeclaredTrial(
            trial_id="mt1-capped-volatility-managed-relative-strength-v1",
            description=(
                "MT-1 capped volatility-managed long-only relative strength: one preregistered scaled/unscaled "
                "controlled pair, evaluated only inside the frozen four-arm difference-in-differences design."
            ),
            evidence=(
                "docs/proposals/ta/2026-08-15-mt1-volatility-managed-relative-strength-preregistration.md "
                "§'Arms and trial accounting' and §'Frozen primary estimand and inference'; issue #2437"
            ),
            exactness=TrialExactness.EXACT,
            # ⚠ Here the declaration's strategy_id IS the trial id and the
            # strategy_version is a registry hash — the opposite key from the
            # three above. Measured against the stored row.
            declared_for=("mt1-capped-volatility-managed-relative-strength-v1", "strategy-registry-v1+32970feefa00"),
        ),
        DeclaredTrial(
            trial_id="mt1-s8-capped-volatility-negative-control-v1",
            description=(
                "S-8 capped-volatility negative control: one preregistered scaled/unscaled controlled pair, "
                "jointly required with MT-1 and never independently selectable."
            ),
            evidence=(
                "docs/proposals/ta/2026-08-15-mt1-volatility-managed-relative-strength-preregistration.md "
                "§'Arms and trial accounting' and §'Frozen primary estimand and inference'; issue #2437"
            ),
            exactness=TrialExactness.EXACT,
            declared_for=("mt1-s8-capped-volatility-negative-control-v1", "strategy-registry-v1+b83c3e4fc997"),
        ),
    ),
)


__all__ = [
    "TRIAL_REGISTER",
    "TRIAL_REGISTER_CUTOFF",
    "TRIAL_REGISTER_VERSION",
    "DeclaredTrial",
    "TrialExactness",
    "TrialRegister",
]
