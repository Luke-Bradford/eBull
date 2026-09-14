"""Shared per-file health sentinels for the two FINRA ingest jobs (#2337, #2795).

⚠ LIFTED, NOT COPIED. `finra_short_interest_refresh` and
`finra_regsho_daily_refresh` both carried a byte-equivalent match-rate WARNING
over the same resolver; #2337 fixed one and #2795 the other. A second copy of the
replacement is how #1955's sibling-drift class recurs, so the arms live here once
and both jobs call them.

WHY THE OLD ARM WENT. Both jobs warned when ``rows_resolved / rows_parsed <
0.50``. The ratio's two sides are governed by **different populations**: the
numerator is bounded by OUR universe (``build_preloaded_symbol_resolver`` selects
``instruments WHERE is_tradable``), the denominator by FINRA's — every
US-reported symbol, including OTC, preferreds, ETFs and share-class siblings.
Nothing links them, so the ratio has no healthy value anyone could write down.
Measured: the bimonthly ran 25.47%-26.58%, below the floor on 34 of 34 files;
RegSHO's pooled ratio straddled the floor and fired on ~38% of stored days. See
``docs/review-prevention-log.md`` ("A health sentinel whose two sides come from
different populations detects nothing").

⚠⚠ THE RETENTION ARM IS OPTIONAL AND IS **OFF** FOR REGSHO, DELIBERATELY.
``retention_floor=None`` means the arm is ABSENT, not set to a permissive number,
so nobody later reads a loose constant as a calibrated one. #2795 measured three
candidate constructions for RegSHO and all three failed — a fitted per-tape floor
is the miscalibration being fixed one level down; a ``sqrt(n)`` counting-noise
band is falsified by FNQC, whose day-to-day variation is not counting noise; and
a "half the previous day" floor is a pick with no stated detection requirement.
Full reasoning: ``docs/proposals/etl/2026-09-14-2795-regsho-daily-sentinels.md``
§3. Building one needs a stated detection requirement and a replayed
``rows_resolved`` series, both recorded on #2795.

WHAT THESE ARMS DO NOT COVER, stated because an alarm's silence is read as
health:

* A row that is syntactically valid but semantically wrong — a wrong symbol,
  swapped numeric columns, a non-empty but invalid market code, a negative
  volume. None of these reaches ``skipped_invalid_row``.
* Header / footer / body-date corruption, which raises a FILE-level failure and
  is surfaced by each job's partial-failure ``RuntimeError`` contract instead.
* A file that never arrived. A provider 403/404 produces no stats entry at all,
  so neither arm can see it, and a tape disappearing permanently is a successful
  run. That is an existing coverage limit of both jobs, not one these arms
  introduce.
* An empty body (header plus a ``0`` footer) succeeds for EVERY prefix, not only
  the legacy ADF one, and yields no findings by design — ``rows_parsed == 0``
  returns early.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["SentinelFinding", "evaluate_ingest_sentinels"]


@dataclass(frozen=True)
class SentinelFinding:
    """One health finding about one ingested file.

    ``key`` identifies the file in whatever terms its job uses — a settlement
    date for the bimonthly, ``"{trade_date}/{prefix}"`` for RegSHO, where the
    prefix is load-bearing because RegSHO's tapes are separate populations.
    """

    key: str
    kind: str
    detail: str


def evaluate_ingest_sentinels(
    *,
    key: str,
    failed: bool,
    rows_parsed: int,
    rows_resolved: int,
    skipped_invalid_row: int,
    previous: tuple[str, int] | None,
    retention_floor: float | None,
) -> list[SentinelFinding]:
    """Health arms for ONE ingested file. Pure — no I/O, no clock, no DB.

    ``previous`` is ``(label, rows_stored)`` for the newest stored period
    strictly before this file's, or ``None`` when there is no earlier one.
    ``label`` is only ever interpolated into the finding text, so the caller owns
    its formatting.

    ``retention_floor=None`` omits the retention arm entirely; ``previous`` is
    then not read at all.

    A file that FAILED, or that parsed no rows, yields nothing — those are
    already surfaced as per-file failures and by the ``RuntimeError``
    partial-failure contract, and re-reporting them here would be noise.
    """
    if failed or rows_parsed == 0:
        return []

    findings: list[SentinelFinding] = []

    # Arm 1 — row shape. The healthy value is 0, and it is knowable BY
    # CONSTRUCTION rather than fitted, which is the whole difference from the
    # ratio this replaced.
    #
    # ⚠ "Six columns" is NOT the whole predicate and describing it that way
    # understates the arm. Each job's parser increments this counter on several
    # branches — for RegSHO (`finra_regsho_ingest.py:168-198`): a bare
    # `split("|")` not yielding 6 parts, a blank symbol, ANY of the three volume
    # columns failing `Decimal` conversion, or a blank market. All four are
    # shape faults with a healthy value of zero; none is a threshold.
    if skipped_invalid_row > 0:
        findings.append(
            SentinelFinding(
                key,
                "row_shape",
                f"{skipped_invalid_row} of {rows_parsed} rows failed the "
                "required-field check (healthy value is 0) — FINRA column-shape "
                "regression suspected",
            )
        )

    if rows_resolved == 0:
        # Total resolution failure. Retention would fire too (0 / anything is
        # below any floor), so return here rather than reporting one fault twice.
        #
        # ⚠ The wording names the likeliest cause, not the only one: a body that
        # is entirely invalid, or entirely ambiguous, also resolves nothing. Arm
        # 1 fires alongside in the first case, which is what distinguishes them.
        findings.append(
            SentinelFinding(
                key,
                "no_resolution",
                f"0 of {rows_parsed} rows resolved to an instrument — the "
                "symbol resolver or the tradable universe is broken",
            )
        )
        return findings

    # Arm 3 — retention against the previous stored period. Caller-supplied
    # floor; see the module docstring for why RegSHO passes None.
    if retention_floor is not None and previous is not None and previous[1] > 0:
        retention = rows_resolved / previous[1]
        if retention < retention_floor:
            findings.append(
                SentinelFinding(
                    key,
                    "universe_drift",
                    f"resolved {rows_resolved} against {previous[1]} stored at "
                    f"{previous[0]} = {retention:.4f} retention, below the "
                    f"{retention_floor:.2f} floor",
                )
            )
    return findings
