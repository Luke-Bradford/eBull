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

⚠⚠ THE COUNT IS A FLOOR, AND THE BIAS DIRECTION IS THE REASON TO SAY SO.

Sessions before this register existed did not record their variants, so trials
are missing from it. Under-counting ``M`` lowers ``N_hat``, which lowers
``SR_0``, which RAISES the Deflated Sharpe — the anti-conservative direction. A
DSR computed here is therefore an UPPER BOUND on the honest one, and a strategy
that fails criterion 6 against this register would fail it harder against a
complete one. ⚠ The converse does not hold and must not be read into a pass.

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
from typing import Final

#: Bumped whenever a trial is added or an entry's meaning changes. ⚠ Stored on
#: the result row beside the DSR: a deflated Sharpe means nothing without the
#: trial population it was deflated against, and that population grows.
TRIAL_REGISTER_VERSION: Final = "trial-register-2026-08-07"


@dataclass(frozen=True)
class DeclaredTrial:
    """One variant that was evaluated against price data.

    ⚠ ``evidence`` is REQUIRED and non-empty. A trial count is only honest if
    each entry can be checked, and an entry nobody can trace is indistinguishable
    from one invented to pad the count in the safe direction.
    """

    trial_id: str
    description: str
    #: Where the evaluation is recorded — an issue, a commit, a spec section.
    evidence: str

    def __post_init__(self) -> None:
        for field_name in ("trial_id", "description", "evidence"):
            if not getattr(self, field_name):
                raise ValueError(f"{field_name} is blank — a present-but-empty declaration declares nothing (#2286)")


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

    @property
    def declared_count(self) -> int:
        return len(self.trials)

    @property
    def trial_ids(self) -> frozenset[str]:
        return frozenset(trial.trial_id for trial in self.trials)

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
TRIAL_REGISTER: Final = TrialRegister(
    version=TRIAL_REGISTER_VERSION,
    trials=(
        DeclaredTrial(
            trial_id="s1-time-series-momentum",
            description="S-1 time-series momentum, evaluated over the validated universe.",
            evidence="app/services/strategies/s1_time_series_momentum.py; full-population run in "
            "scripts/verify_2240_statistics.py --curve",
        ),
        DeclaredTrial(
            trial_id="s2-cross-sectional-momentum",
            description="S-2 cross-sectional momentum, monthly rebalance on the panel.",
            evidence="app/services/strategies/s2_cross_sectional_momentum.py; "
            "docs/proposals/ta/2026-08-06-cross-sectional-contract-and-s2.md",
        ),
        DeclaredTrial(
            trial_id="s3-mean-reversion-in-trend",
            description="S-3 mean reversion in an established uptrend.",
            evidence="app/services/strategies/s3_mean_reversion_in_trend.py; full-population run in "
            "scripts/verify_2240_statistics.py --curve",
        ),
        DeclaredTrial(
            trial_id="s4-volatility-compression-breakout",
            description="S-4 volatility compression breakout.",
            evidence="app/services/strategies/s4_volatility_compression_breakout.py",
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
        ),
        DeclaredTrial(
            trial_id="rsi30-20d-overlapping-price-daily",
            description="RSI<30 → 20-day hit, causal Wilder, overlapping triggers, price_daily.",
            evidence="issue #2260 comment 2026-08-05 (full-population recompute)",
        ),
        DeclaredTrial(
            trial_id="rsi30-20d-nonoverlapping-price-daily",
            description="RSI<30 → 20-day hit, causal Wilder, non-overlapping triggers (candidate 3), price_daily.",
            evidence="issue #2260 comment 2026-08-05 (full-population recompute)",
        ),
        DeclaredTrial(
            trial_id="rsi30-20d-quarantined-price-daily",
            description="RSI<30 → 20-day hit, causal Wilder, quarantined bars excluded (candidate 4), price_daily.",
            evidence="issue #2260 comment 2026-08-05 (full-population recompute)",
        ),
        DeclaredTrial(
            trial_id="rsi30-20d-overlapping-research-corpus",
            description="RSI<30 → 20-day hit, causal Wilder, overlapping triggers, research corpus.",
            evidence="issue #2260 comment 2026-08-05 (full-population recompute)",
        ),
        DeclaredTrial(
            trial_id="rsi30-20d-nonoverlapping-research-corpus",
            description="RSI<30 → 20-day hit, causal Wilder, non-overlapping triggers (candidate 3), research corpus.",
            evidence="issue #2260 comment 2026-08-05 (full-population recompute)",
        ),
        DeclaredTrial(
            trial_id="rsi30-20d-quarantined-research-corpus",
            description="RSI<30 → 20-day hit, causal Wilder, quarantined bars excluded (candidate 4), research corpus.",
            evidence="issue #2260 comment 2026-08-05 (full-population recompute)",
        ),
    ),
)


__all__ = [
    "TRIAL_REGISTER",
    "TRIAL_REGISTER_VERSION",
    "DeclaredTrial",
    "TrialRegister",
]
