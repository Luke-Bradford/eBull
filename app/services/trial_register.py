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

import hashlib
import re
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Final

#: Bumped whenever a trial is added or an entry's meaning changes. ⚠ Stored on
#: the result row beside the DSR: a deflated Sharpe means nothing without the
#: trial population it was deflated against, and that population grows.
#:
#: ⚠⚠ ADDING A TRIAL *IS* THE SUPERSESSION — bumping this string only labels it.
#: `deflation_promotion_refusals` fires `trial_register_superseded` on
#: `deflated.declared_trials != TRIAL_REGISTER.declared_count`, so a new entry
#: strands every stored deflation whether or not this constant moves. There is
#: no way to count a new search without it, and `freeze_preregistration` refuses
#: a declaration no trial claims — so "add the entry but don't bump" is not a
#: state, and declining to add one to protect old rows is the flattering
#: direction this module is built against.
#:
#: r8 (2026-08-22, #2837) is the first bump measured rather than argued. What it
#: stranded, reproduce with:
#:
#:   PYTHONPATH=. uv run python -c "
#:   import psycopg; from app.config import settings
#:   with psycopg.connect(settings.database_url) as c:
#:       c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
#:       print(c.execute('select trial_register_version, purpose, count(*) from strategy_results_store "
#:   "where deflated_sharpe is not null group by 1,2 order by 3 desc').fetchall()); c.rollback()"
#:
#: On dev at the bump every row with a DSR carried `purpose = harness_validation`
#: — so `purpose_promotion_refusals` already returned a terminal
#: `harness_validation_only` on all of them, and the bump added a second refusal
#: code to rows that could not promote anyway. Four earlier versions appear in
#: the same output, which is what a register that is doing its job looks like.
#:
#: r9 (2026-08-22, #2840) adds S-H arm 1 and was measured the same way. The
#: command above returned the SAME five (version, purpose) groups — 488 rows,
#: every one `harness_validation`, and r8 itself carrying none, because nothing
#: has been backtested since it landed. So r9 strands nothing that could have
#: promoted either.
#:
#: r10 (2026-09-22, #2834) adds ARM B stage (i), measured the same way before the
#: bump: the SAME five (version, purpose) groups, 488 rows, every one
#: `harness_validation`. It strands nothing that could have promoted.
#:
#: r11 (2026-09-25, #2901) adds #2908's nine exposed searches, which ran on
#: 2026-08-24 after the cutoff below without charging this register, and #2901's
#: quality arm. Measured the same way before the bump: the SAME five groups, 488
#: rows, every one `harness_validation`. It strands nothing that could have promoted.
#:
#: r12 (2026-09-26, #3385 slice 1) reconciles #2832 and #2840 and adds one
#: search, the s8 in-sample fan S-H arm 1's job wrote uncharged. Measured the
#: same way before the bump: the SAME five groups, 488 rows, every one
#: `harness_validation`. It strands nothing that could have promoted.
#:
#: r13 (2026-09-26, #3385 slice 3c-iv) reconciles #2827's post-cutoff batches and
#: #3238's A/B, adding 88 searches. Measured the same way before the bump: the
#: SAME five groups, 488 rows, every one `harness_validation`. It strands nothing
#: that could have promoted.
TRIAL_REGISTER_VERSION: Final = "trial-register-2026-09-26-r13"

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

    ``searches`` is normally one.  It may be greater than one in exactly three
    cases:

    - **legacy grouped** — a historical research session recorded the size and
      construction of a search family but did not retain a durable id for every
      arm.  Collapsing that family to one would under-count ``M`` in the
      flattering direction; inventing one id per lost arm would imply provenance
      the repository does not have.
    - **log-backed** (#3385, a hunt's discovery split) — every search has a
      durable id in ``hunt_trials``.  ``evidence`` is built by
      ``log_backed_evidence``: the query that selects the ids, a closing
      timestamp read from the DB clock, and the sha256 of the ordered id list,
      whose length IS ``searches``.  The freeze tripwire re-runs the query and
      refuses ``register_disagrees_with_log`` on any mismatch.
    - **declaration-backed** (#3385, a hunt's validation or holdout split) — the
      searches are the specs pinned by one frozen declaration.  ``evidence`` is
      built by ``declaration_backed_evidence``: the declaration doc's sha256 and
      its pinned count, which IS ``searches``; ``declared_for`` names it.

    ⚠ A ``hunt-`` trial id is reserved for the last two, and only in the form
    ``hunt-<n>-<split>``; ``__post_init__`` refuses anything else.  The
    inherited floor ``M_inh`` is ``declared_count`` excluding every ``hunt-``
    entry, so a non-hunt entry that borrowed the prefix would silently drop out
    of it (#3385 contract decision 121).  A hunt with no searches in a split has
    no entry — never ``searches=0``.

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
        if self.trial_id.startswith(HUNT_TRIAL_PREFIX):
            self._check_hunt_entry()

    def _check_hunt_entry(self) -> None:
        """#3385: a ``hunt-`` id is a hunt entry, and its evidence carries its count."""
        id_match = _HUNT_TRIAL_ID.fullmatch(self.trial_id)
        if id_match is None:
            raise ValueError(
                f"{self.trial_id}: the {HUNT_TRIAL_PREFIX!r} prefix is reserved for hunt entries "
                "'hunt-<n>-<discovery|validation|holdout>' — M_inh excludes every such id"
            )
        if id_match["split"] == "discovery":
            evidence_match = _LOG_BACKED_EVIDENCE.fullmatch(self.evidence)
            if evidence_match is None or self.declared_for is not None:
                raise ValueError(f"{self.trial_id}: a discovery entry is log-backed (log_backed_evidence), unclaimed")
        else:
            evidence_match = _DECLARATION_BACKED_EVIDENCE.fullmatch(self.evidence)
            expected_claim = (self.trial_id, _HUNT_DECLARATION_VERSION)
            if evidence_match is None or self.declared_for != expected_claim:
                raise ValueError(
                    f"{self.trial_id}: a {id_match['split']} entry is declaration-backed "
                    f"(declaration_backed_evidence) and claims {expected_claim!r}"
                )
            if self.exactness is not TrialExactness.EXACT:
                raise ValueError(f"{self.trial_id}: pinned specs are enumerated, so the entry is EXACT")
        if int(evidence_match["n"]) != self.searches:
            raise ValueError(
                f"{self.trial_id}: evidence records {evidence_match['n']} searches but the entry declares "
                f"{self.searches} — the register would disagree with the log it cites"
            )


#: #3385. Reserved for hunt entries; see ``DeclaredTrial``.
HUNT_TRIAL_PREFIX: Final = "hunt-"
#: The hunt spec pins each hunt declaration at ``v1`` (a validation batch is
#: one declaration; a holdout is ``v1`` only).
_HUNT_DECLARATION_VERSION: Final = "v1"
_HUNT_TRIAL_ID: Final = re.compile(r"hunt-[1-9][0-9]*-(?P<split>discovery|validation|holdout)")
_SHA256: Final = r"[0-9a-f]{64}"
_LOG_BACKED_EVIDENCE: Final = re.compile(
    rf"hunt_trials log; query=(?P<query>.+); closed_at=(?P<closed_at>\S+); "
    rf"ids_sha256=(?P<sha>{_SHA256}); n=(?P<n>[1-9][0-9]*)",
    re.DOTALL,
)
_DECLARATION_BACKED_EVIDENCE: Final = re.compile(
    rf"declaration (?P<path>\S+) sha256=(?P<sha>{_SHA256}); pinned_specs=(?P<n>[1-9][0-9]*)"
)


def ordered_ids_sha256(trial_ids: Sequence[str]) -> str:
    """sha256 of the ordered id list, one id per line — the tripwire recomputes this from the log."""
    return hashlib.sha256("\n".join(trial_ids).encode()).hexdigest()


def log_backed_evidence(*, query: str, closed_at: datetime, trial_ids: Sequence[str]) -> str:
    """Evidence for a discovery entry: ``searches`` must be ``len(trial_ids)``.

    ⚠ ``closed_at`` comes from the DB clock (``now()`` in the closing query), not
    the host's: the tripwire compares it against ``hunt_trials.registered_at``.
    """
    if not query.strip():
        raise ValueError("log-backed evidence needs the query that selects the ids")
    if closed_at.utcoffset() != timedelta(0):
        raise ValueError(f"closed_at must be aware with a zero UTC offset, got {closed_at!r}")
    if not trial_ids:
        raise ValueError("a hunt with no searches in a split has no register entry")
    if len(set(trial_ids)) != len(trial_ids):
        raise ValueError("duplicate hunt trial ids — one search counted twice")
    return (
        f"hunt_trials log; query={query}; closed_at={closed_at.isoformat()}; "
        f"ids_sha256={ordered_ids_sha256(trial_ids)}; n={len(trial_ids)}"
    )


def declaration_backed_evidence(*, declaration_path: str, declaration_sha256: str, pinned_specs: int) -> str:
    """Evidence for a validation or holdout entry: ``searches`` must be ``pinned_specs``."""
    if not re.fullmatch(_SHA256, declaration_sha256):
        raise ValueError(f"declaration_sha256 must be 64 lowercase hex, got {declaration_sha256!r}")
    if not declaration_path or any(ch.isspace() for ch in declaration_path):
        raise ValueError(f"declaration_path must be a non-blank path without whitespace, got {declaration_path!r}")
    if type(pinned_specs) is not int or pinned_specs < 1:
        raise ValueError("a declaration pinning no specs has no register entry")
    return f"declaration {declaration_path} sha256={declaration_sha256}; pinned_specs={pinned_specs}"


@dataclass(frozen=True)
class InheritedFloor:
    """#3385's ``M_inh``: the searches a hunt inherits from before it began.

    ⚠ Read at each declaration's freeze and stored with it, never re-read at
    readout: the register grows, and a verdict deflated against a count that
    moved after its freeze would not be the verdict that was frozen.
    """

    register_version: str
    #: ``declared_count`` excluding every ``hunt-`` entry.
    searches: int
    #: True when any inherited entry is ``FLOOR``: ``M`` is then only a lower
    #: bound, so a DSR computed against it is an upper bound on significance.
    #: This is the spec's ``m_is_floor``.
    is_floor: bool


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

    def inherited_floor(self) -> InheritedFloor:
        """``M_inh`` (#3385 spec §"Inherited floor M_inh"): every non-``hunt-`` entry.

        ⚠ Hunt entries are excluded because the hunt counts its own searches
        from ``hunt_trials`` and its pinned specs; counting their register
        entries too would charge each hunt search twice.
        """
        inherited = [trial for trial in self.trials if not trial.trial_id.startswith(HUNT_TRIAL_PREFIX)]
        return InheritedFloor(
            register_version=self.version,
            searches=sum(trial.searches for trial in inherited),
            is_floor=any(trial.exactness is TrialExactness.FLOOR for trial in inherited),
        )

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
        DeclaredTrial(
            trial_id="se-ma-overlay-2026-08-22",
            description=(
                "S-E 10-month-SMA overlay on the passive core: ONE frozen rule (10-month lookback, no band, no "
                "confirmation delay) measured at three evaluation offsets. ⚠ searches=1 and not 3 — the three "
                "offsets are a fragility screen over one rule, not three variants selected between. Choosing the "
                "best offset is forbidden by the pass bar, which requires all three."
            ),
            evidence=(
                "docs/proposals/ta/2026-08-22-se-ma-overlay-preregistration.md §4 (frozen rule), §5 (fragility) "
                "and §8 (pass bar); issue #2837"
            ),
            exactness=TrialExactness.EXACT,
            declared_for=("se-ma-overlay-drawdown-insurance", "se-ma-overlay-drawdown-insurance-v1"),
        ),
        DeclaredTrial(
            trial_id="sh-volatile-regime-gate-2026-08-22",
            description=(
                "S-H arm 1: S-4's compression breakout with entry gated to regime in {bear_volatile, "
                "bull_volatile}. ONE frozen rule read across four cells ({bear,bull} x {masked,admitted}). "
                "⚠ searches=1 and not 4 — the cells are a fragility screen the pass bar requires jointly "
                "(bear_volatile positive in BOTH quarantine arms), so no favourable cell is selectable. "
                "⚠ Narrowing permitted_regimes to bear_volatile after the look would be a DIFFERENT rule: "
                "the set is hashed into S11_PARAMS, so it mints a new strategy_version and charges this "
                "register a second time rather than re-reading this entry."
            ),
            evidence=(
                "docs/proposals/ta/2026-08-22-sh-volatile-regime-gated-breakout.md §'The rule', "
                "§'Readout and abort bar' and §'Sequencing'; issue #2840"
            ),
            exactness=TrialExactness.EXACT,
            # ⚠ The `survivorship_free` identity, NOT the `survivor_only` one
            # (`strategy-registry-v1+65274a70a40b`). They are two different
            # trials: `BACKTEST_UNIVERSE` is `survivorship_free` and that is what
            # the exploration measures; `survivor_only` is `SCAN_UNIVERSE`.
            declared_for=("s11-volatile-regime-gated-breakout", "strategy-registry-v1+d5f25fd08376"),
        ),
        DeclaredTrial(
            trial_id="armb-12-2-dv-weighted-stage-i-2026-09-22",
            description=(
                "#2834 ARM B stage (i): canonical 12-2 momentum top decile, dollar-volume weighted, against a "
                "DV-weighted market, on the survivorship_free admission, 2000-01 to before HOLDOUT_BOUNDARY. "
                "ONE decision arm; the equal-weight arm, the EW market and the regime cohorts are frozen "
                "readouts, not searches. Both termination-ambiguity arms must pass jointly, so neither is "
                "selectable. Charged BEFORE the first run (spec §3)."
            ),
            evidence=(
                "docs/proposals/ta/2026-09-22-armb-dv-weighting-prototype.md §1 and §3 (merged 936da1a3); "
                "scripts/measure_2834_armb_dv_prototype.py; issue #2834"
            ),
            exactness=TrialExactness.EXACT,
        ),
        DeclaredTrial(
            trial_id="r6-2908-exclusion-arms-2026-08-24",
            description=(
                "R6 #2908: dilution exclusion (primary), filing-risk exclusion and their union, 2022-07 to "
                "2024-09; pass rule against literal buy-and-hold, scientific control the identical annual 1/N. "
                "THREE arms x THREE configurations (original, halt-bound correction 1, resolver correction 3). "
                "⚠ searches=9: a recompute counts when the earlier number was exposed (reconstruction policy, "
                "2026-08-12-trial-register-reconstruction.md), and these are the same 9 rows #2901's declaration "
                "counts as H. It ran after the cutoff without a #2599 declaration, so it is charged here, late."
            ),
            evidence=(
                "docs/proposals/ta/2026-08-24-r6-exclusion-preregistration.md, corrections 1-4 and "
                "2026-08-24-r6-exclusion-result.md; strategy_holdout_accesses 640, 641; issue #2908"
            ),
            exactness=TrialExactness.EXACT,
            searches=9,
        ),
        DeclaredTrial(
            trial_id="r6-2901-quality-gpa-2026-09-25",
            description=(
                "#2901: the top GP/A decile of the complete-case eligible set E(D), 12 June formations "
                "2013-2024, against the identical annual 1/N control C(D) (headline) and the complete-case "
                "diagnostic control C'(D). ONE arm, two comparator rows, both gating; searches=2 follows the "
                "declaration's first-run rows, the conservative direction. The D0 gate leg, the buy-and-hold "
                "and SPY are frozen readouts, not searches. Charged BEFORE the first run."
            ),
            evidence=(
                "docs/proposals/ta/2026-09-25-2901-quality-declaration.md (frozen declaration), "
                "2026-09-25-2901-quality-declaration-spec.md and 2026-09-24-2901-quality-arm.md; issue #2901"
            ),
            exactness=TrialExactness.EXACT,
            searches=2,
            declared_for=("r6-quality-gpa", "r6-2901-quality-v1"),
        ),
        # ⚠⚠ #3385 SLICE 1 — #2832 AND #2840 RECONCILED, ONE ENTRY ADDED. Per
        # candidate, under "a search of the data, not a design":
        #
        # - #2832's 32 kills: desk kills. The sweep was web/primary-source
        #   research plus skeptics applying bars ALREADY measured (turnover, the
        #   cost bands, #2827's deflation miss); no kill reports a new return
        #   computation. Not trials. ⚠ The per-verdict journal
        #   `wf_f24998c8-93a` is not retained, so this rests on #2832's body and
        #   the kill table in `.claude/skills/quant/strategy-evidence.md` §3.2.
        # - #2832's six survivors: S-A (#2833) and #2834 ARM A were selected on
        #   quote SPREADS, not returns; ARM A's one return figure (12.1%/yr
        #   tracking error of a fixed, unselected sleeve) is a risk readout no
        #   choice was taken over. S-C (#2835) and S-D (#2836) were cut unrun.
        #   S-F (#2838) stage 1 read eligibility only; stage 2 is unrun. S-E is
        #   `se-ma-overlay-2026-08-22`; ARM B stage (i) is
        #   `armb-12-2-dv-weighted-stage-i-2026-09-22`. ARM B's re-reads of s2's
        #   stored hold-out rows computed nothing new; its decile-displacement
        #   censuses read no forward return.
        # - #2840 arm 1: `sh-volatile-regime-gate-2026-08-22`; S-4 in the same
        #   job is its declared turnover control. Arm 2
        #   (`s12-cheapest-band-price-gated-breakout`): outcome-free censuses
        #   only — never declared or backtested. Arm 3: never built. ⚠ Arm 2's
        #   $100 band was chosen after s4/s8's gross populations were re-costed
        #   at the cheaper bands. Slice 1 called that a constant shift and not a
        #   search; slice 3c-iv's Codex ckpt-2 falsified it (profit factor moves
        #   non-uniformly, see `gross-vs-net-2827-2026-08-21` below, which now
        #   charges those band readouts). Arm 2's declaration must still
        #   disclose them as prior exposure.
        # - Added below: the s8 fan arm 1's job also computed and stored.
        #
        # #2827's post-cutoff batches and #3238's A/B were outside slice 1's
        # scope; slice 3c-iv reconciles them in the block after this entry.
        DeclaredTrial(
            trial_id="s8-in-sample-survivorship-free-2026-08-23",
            description=(
                "S-8 range mean reversion, in-sample survivorship_free fan (1962-01-02 to 2024-09-27), computed and "
                "stored by #2840 arm 1's run but charged by no entry: s8 is neither arm 1 nor its declared control. "
                "One search under the fan-collapse rule."
            ),
            evidence=(
                "strategy_results_store result_id 734-737 (s8-range-mean-reversion @ "
                "strategy-registry-v1+9052ecd5fb62, namespace in_sample, created 2026-08-23 01:09Z, same run as "
                "s11's); issue #2840 comment 5383525513 (run 116064)"
            ),
            exactness=TrialExactness.EXACT,
        ),
        # ⚠⚠ #3385 SLICE 3c-iv — EVERY POST-CUTOFF SEARCH NO ENTRY CHARGED. The
        # population is `strategy_results_store` and `strategy_holdout_accesses`
        # after `TRIAL_REGISTER_CUTOFF`; everything there that is not below is
        # already charged: s5-s10's first evaluation (their entries, §4.1's fan),
        # reads 637/638/640/641/642 (the opening look of the S-E, S-H arm 1,
        # #2908 and #2901 entries' own searches), read 639 (S-H arm 1's declared
        # control) and the 2026-08-23 in-sample fans (S-H arm 1, its control and
        # the s8 entry above). Access id 425 is a sequence gap, not a row.
        #
        # ⚠ ONE RULE FOR RECENT WINDOWS, stated because Codex found the first
        # draft applied two: windows collapse to one search ONLY where a
        # declaration froze them as jointly required BEFORE the look (s5-s10's
        # §4.1). Otherwise each stored window evaluation is a search, which is
        # S-1..S-4's settled arithmetic (`evaluate accesses / 4`). Neither s1-s4
        # batch below had such a declaration, so both count per window.
        #
        # ⚠ RE-PRICING A STORED POPULATION AT ANOTHER COST IS A SEARCH. The first
        # draft followed slice 1 in calling it "a constant shift" and Codex
        # ckpt-2 falsified that: the MEAN shifts by a constant, but profit factor
        # does not, because a cost moves trades across zero
        # (`measure_2827_gross_vs_net.py`'s own header), and PF is a decision
        # metric. The cheaper-band readouts were then used to pick s4/s8. So
        # every re-priced cost level that was read is a changed estimand
        # (reconstruction clause 2). A CONTROL that reproduces an already-stored
        # number is not.
        DeclaredTrial(
            trial_id="s1-s4-survivor-only-v2-cost-calendar-windows-2026-08-12",
            description=(
                "S-1..S-4 at their survivor_only versions re-evaluated on the hold-out under cost model "
                "static-p75-insession-v2+split-adjusted-max for calendar 2022, 2023, 2024, 2025 and 2026-YTD: 5 "
                "windows x 4 strategies. Pre-cutoff those windows existed only under cost v1, so each re-run "
                "changed the estimand of a stored number; no declaration made the windows jointly required."
            ),
            evidence=(
                "strategy_holdout_accesses 305-384 (80 evaluate, purpose 'complete declared recent-regime evidence "
                "denominator', 2026-08-12 18:56-22:25Z; 80 / 4 = 20); strategy_results_store 385-464"
            ),
            exactness=TrialExactness.EXACT,
            searches=20,
        ),
        DeclaredTrial(
            trial_id="s1-s4-survivorship-free-pinned-windows-2026-08-21",
            description=(
                "S-1..S-4 at their survivorship_free / cost-v3 versions (a domain variant, so new trials), on the "
                "hold-out windows primary-2022-plus, rolling-36m, rolling-24m, year-2022, year-2023 and year-2024: "
                "6 windows x 4 strategies. No entry declared them before the look, so §4.1's window fan, which is "
                "s5-s10's, does not extend to them."
            ),
            evidence=(
                "strategy_holdout_accesses 385-636, the 96 evaluate rows for s1-s4 (purpose 'pinned recent-evidence "
                "windows for the S-1..S-10 walk-forward (build-queue item 6)', 2026-08-21 13:56Z to 08-22 06:55Z; "
                "96 / 4 = 24); strategy_results_store 485-725; scripts/run_2825_decisive_holdout_evidence.py; "
                "issue #2827"
            ),
            exactness=TrialExactness.EXACT,
            searches=24,
        ),
        DeclaredTrial(
            trial_id="s1-in-sample-survivorship-free-2026-08-21",
            description=(
                "S-1 at its survivorship_free version, in-sample fan 1962-01-02 to 2024-09-27, stored before the "
                "hold-out batches began. One search under the fan-collapse rule (the shape of the s8 entry above)."
            ),
            evidence=(
                "strategy_results_store result_id 481-484 (s1-time-series-momentum @ "
                "strategy-registry-v1+cd8a60d57047, namespace in_sample, created 2026-08-21 09:43Z)"
            ),
            exactness=TrialExactness.EXACT,
        ),
        DeclaredTrial(
            trial_id="gross-vs-net-2827-2026-08-21",
            description=(
                "#2831's gross-vs-net measurement: all ten strategies' primary-2022-plus arms re-evaluated at zero "
                "cost, a new estimand of published numbers, plus each gross population re-priced at the three "
                "bands cheaper than the <$5 band every trade was then charged, with profit factor recomputed: "
                "11 reads + 10 x 3 bands. The four robustness arms collapse per the fan rule. ⚠ 11 reads and not "
                "10: access 466 (s9, 25s before the other ten) looks like an aborted start, but the ledger "
                "records a look and nothing records that it saw nothing."
            ),
            evidence=(
                "strategy_holdout_accesses 466-476 (11 read, 2026-08-21 21:34Z, purpose 'measure gross vs net "
                "per-trade return...'); scripts/measure_2827_gross_vs_net.py; PR #2831; issue #2827 comment "
                "2026-08-22T00:56Z (band table); _print_band_table over cost_model.BANDS (4 bands)"
            ),
            exactness=TrialExactness.EXACT,
            searches=41,
        ),
        DeclaredTrial(
            trial_id="cost-basis-ab-3238-s1-in-sample-2026-09-20",
            description=(
                "#3238's cost-basis A/B treatment arm: S-1 survivorship_free in-sample re-run with each trade "
                "charged its own nominal band, on a 60-series slice and on all 17,285 series. Two populations, "
                "two searches; the control reproduces the stored max-band number and is not counted. FLOOR: the "
                "PR also names bounded cohort slices it does not enumerate."
            ),
            evidence=(
                "scripts/ab_3238_cost_basis.py --strategy s1-time-series-momentum; PR #3240 body (60-series slice "
                "table) and its full-population comment (--no-cohort, full_population: true); issue #2840 comment "
                "5747000124"
            ),
            exactness=TrialExactness.FLOOR,
            searches=2,
        ),
    ),
)


__all__ = [
    "HUNT_TRIAL_PREFIX",
    "TRIAL_REGISTER",
    "TRIAL_REGISTER_CUTOFF",
    "TRIAL_REGISTER_VERSION",
    "DeclaredTrial",
    "InheritedFloor",
    "TrialExactness",
    "TrialRegister",
    "declaration_backed_evidence",
    "log_backed_evidence",
    "ordered_ids_sha256",
]
