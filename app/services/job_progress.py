"""#2218 — the progress verdict: did this job actually do anything?

WHY THIS EXISTS
---------------
Job health was derived from COMPLETION. `cusip_resolver_post_bulk_sweep` binds
a whole-batch OpenFIGI failure to an `api_errors` counter rather than raising,
so a pass where every CUSIP errored recorded `status='success', row_count=0`;
OpenFIGI resolution was dark for seven weeks behind that signal (#2213). The
same shape starved ETF filer typing for two months (#2214). Neither was caught
by any automated check, because every check reads the same "did it finish"
signal.

ONE RULE, DELIBERATELY, AND WHAT IT IS NOT
------------------------------------------
⚠ **"Zero rows written" is NOT a degradation.** Plenty of jobs legitimately
have nothing to do on a given run, and a blanket zero-rows alarm is noise that
trains the operator to ignore the signal — which is strictly worse than no
signal. The distinction is `candidates_seen`: a job that saw no work and did
none is healthy; a job that saw work and produced no terminal outcome is
stalled. That is why the counters are persisted rather than inferred.

The verdict is pure and lives here rather than in each invoker so that all
jobs answer the same question the same way — the recurring lesson from the
closed-vocabulary defects is that a rule expressed once in code cannot drift
from a rule expressed twice.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class JobProgress:
    """What a job saw, what it produced, and what went wrong.

    ``candidates_seen`` — units of work the job identified. ``None`` means the
    job does not count them, which is NOT zero: a job that cannot say how much
    work it saw cannot be judged stalled, and claiming otherwise would invent
    an alarm out of an absence.

    ``outcomes`` — terminal buckets that each represent work genuinely
    completed. Every one of them counts as progress, so a job must NOT put a
    bucket here that it also reports as an error.

    ⚠ Buckets may legitimately OVERLAP within ``outcomes`` (the CUSIP sweep
    reports both ``resolved`` and ``promoted``, and a promoted CUSIP was also
    resolved). That is fine for the "any outcome" test and would be WRONG for
    anyone who later sums them as units of work — do not add that reading
    without changing the contract first.

    ``errors`` — buckets that represent work the job could not do. Any non-zero
    value degrades the run.
    """

    candidates_seen: int | None = None
    outcomes: Mapping[str, int] = field(default_factory=dict)
    errors: Mapping[str, int] = field(default_factory=dict)
    # Optional bounded provenance for aggregate population/session counters.
    # It is omitted from the serialised shape when empty, preserving existing
    # readers of the three axes above. The degradation verdict ignores it.
    context: Mapping[str, object] = field(default_factory=dict)

    def as_json(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "candidates_seen": self.candidates_seen,
            "outcomes": dict(self.outcomes),
            "errors": dict(self.errors),
        }
        if self.context:
            payload["context"] = dict(self.context)
        return payload


def degradation_reason(progress: JobProgress | None) -> str | None:
    """Operator-visible reason this run made no progress, or ``None``.

    Two conditions, both from the #2218 acceptance criteria and neither
    inferred from row counts:

    1. **Any error bucket is non-zero.** A run that errored on some of its work
       and completed the rest is still degraded — partial progress hides the
       same stall at a smaller scale, and #2213's job was at 100% errors for
       seven weeks without one of them reaching the health verdict.
    2. **Saw candidates, produced no terminal outcome.** The silent-stall
       shape: work identified, nothing resolved, nothing errored either.

    ``None`` progress returns ``None`` — a job that does not report progress is
    judged exactly as it was before, which keeps this change inert for every
    job that has not opted in.
    """
    if progress is None:
        return None

    # ``n > 0`` rather than truthiness (Codex ckpt-3): a negative count is
    # nonsense either way, but truthiness makes ``{"api_errors": -1}`` degrade
    # while ``{"done": -1}`` reads as progress — the two nonsense values would
    # be treated as opposites.
    failed = {name: n for name, n in progress.errors.items() if n > 0}
    if failed:
        detail = ", ".join(f"{name}={n}" for name, n in sorted(failed.items()))
        return f"errors reported: {detail}"

    seen = progress.candidates_seen
    if seen is not None and seen > 0 and not any(n > 0 for n in progress.outcomes.values()):
        buckets = ", ".join(sorted(progress.outcomes)) or "none reported"
        return f"saw {seen} candidates and produced no terminal outcome (buckets: {buckets})"

    return None
