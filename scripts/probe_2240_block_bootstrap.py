"""Revert-probe the phase-5e-2 block-bootstrap invariant tests (#2240).

    uv run python scripts/probe_2240_block_bootstrap.py

Sister to ``scripts/probe_2240_statistics.py``; the five guards in its header
apply unchanged and the strict runner is IMPORTED rather than copied again:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.**
3. ⚠ **The SELECTOR is not guarded by either.** Triage ``NOT CAUGHT`` in the
   order selector → fixture → code.
4. ⚠⚠ **Gate on exit code 1, never on "non-zero".** A syntax break exits 4 and a
   pytest USAGE error exits 4 as well; both would read as a catch under a
   "non-zero means failed" rule (the #2214 false 4/4).
5. ⚠ **Run a BASELINE first**, so "the mutation broke it" and "it was already
   broken" are distinguishable.

⚠ TWO SOURCE FILES: ``block_bootstrap`` is the construction and
``strategy_statistics`` the metric set that carries its output.

⚠ NOT A TEST. It mutates tracked source files on disk; CI does not run it.
Everything here is pure-tier, so no database is needed.

⚠ MUST NOT RUN CONCURRENTLY with ``scripts/verify_2240_statistics.py``, which
imports both files — a concurrent run would be stamped with injected source and
the restore would hide it.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **``BOOTSTRAP_MODEL_ID``'s value.** Nothing branches on it; it is stamped and
reported, and ``sql/265``'s non-empty CHECK plus the literal in
``tests/test_block_bootstrap.py`` are what keep it honest.

⚠ **``_BATCH``.** It bounds peak memory and nothing else — the batches are
concatenated and the RNG stream is continuous, so any value produces identical
output. There is no defect to inject.

⚠⚠ **``MIN_CLUSTERS``, and this one is a FINDING rather than a judgement call.**
A probe replacing the ``cluster_count < MIN_CLUSTERS`` early return with ``if
False`` was written, ran, and came back ``NOT CAUGHT``. Triage says the code is
right and the branch is dead: on a one-cluster axis every resample draws that
one cluster, so the statistic is CONSTANT, the bootstrap variance is exactly 0.0
(measured), and the zero-variance guard further down returns ``None`` anyway.
The early return is a fast path that names its reason, not the deciding check —
so a test named after it would pass with it deleted. Same shape as phase 5d's
``0.0 ** x == 0.0`` dead branch. It is kept for the message and the 2,000
pointless resamples it avoids, and it is NOT probed, because a probe that cannot
fail proves nothing.

⚠ **The ``b_max`` cap inside ``optimal_block_length``.** Removing it changes no
reachable answer on the fixtures here (none of them are clipped by it), so a
probe would report ``NOT CAUGHT`` for a guard that is nonetheless correct. The
cap is Politis & White's own and is covered by the reference transcription in
``test_it_matches_an_independent_transcription_of_the_published_formula``.
"""

from __future__ import annotations

import sys
from pathlib import Path

from scripts.probe_2240_cost_model import PYTEST_PASSED, PYTEST_TEST_FAILED, run, selected

BOOT = Path("app/services/block_bootstrap.py")
STATS = Path("app/services/strategy_statistics.py")
SOURCES = (BOOT, STATS)

BOOT_TESTS = "tests/test_block_bootstrap.py"
STATS_TESTS = "tests/test_strategy_statistics.py"

#: (what the injected defect IS, source file, test file, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, Path, str, list[tuple[str, str]], str]] = [
    (
        # ⚠ The cluster carries the LAST return instead of the sum, so the
        # pooled ratio silently becomes an unweighted per-date mean.
        "a date cluster storing the last return instead of the sum",
        BOOT,
        BOOT_TESTS,
        [("            sums[-1] += float(returns[index])", "            sums[-1] = float(returns[index])")],
        "test_a_date_becomes_one_cluster_carrying_the_sum_not_the_mean",
    ),
    (
        # ⚠ Kish's denominator computed as a population variance. Inflates the
        # design effect by n/(n-1) on ONE side of the ratio only.
        "the trade variance taken with ddof=0",
        BOOT,
        BOOT_TESTS,
        [("np.var(returns, ddof=1)) if len(returns) > 1", "np.var(returns, ddof=0)) if len(returns) > 1")],
        "test_the_trade_variance_is_the_sample_variance_of_the_raw_trades",
    ),
    (
        # ⚠⚠ THE CROSSED CONSTANT. 2 is the STATIONARY bootstrap's; using it with
        # the circular scheme mis-sizes every block, and nothing downstream can
        # see it — the block length still looks entirely plausible.
        "the stationary bootstrap's constant used for the circular scheme",
        BOOT,
        BOOT_TESTS,
        [("    d_cb = 4.0 / 3.0 * lr_acv**2", "    d_cb = 2.0 * lr_acv**2")],
        "test_it_matches_an_independent_transcription_of_the_published_formula",
    ),
    (
        # ⚠ The flat-top lag window degraded to a rectangular one.
        "the lag window flattened to a rectangle",
        BOOT,
        BOOT_TESTS,
        [("lam = 1.0 if k / m <= 0.5 else 2.0 * (1.0 - k / m)", "lam = 1.0")],
        "test_it_matches_an_independent_transcription_of_the_published_formula",
    ),
    (
        # ⚠⚠ The two nominal counts allowed to diverge: Kish's denominator would
        # then be a different population from the point estimate's, inside one
        # result, with nothing downstream able to tell. Review WARNING on #2370.
        "a declared trade_count disagreeing with the cluster axis admitted",
        BOOT,
        BOOT_TESTS,
        [("        if pooled != self.trade_count:", "        if False:")],
        "test_a_declared_trade_count_disagreeing_with_the_axis_is_refused",
    ),
    (
        # ⚠ The wrap-around removed. Blocks starting near the end of the axis run
        # off it — the moving-block scheme, which the 4/3 constant does not match.
        "the circular wrap removed from the block index",
        BOOT,
        BOOT_TESTS,
        [
            (
                "        index = (starts[:, :, None] + offsets) % n",
                "        index = np.minimum(starts[:, :, None] + offsets, n - 1)",
            )
        ],
        "test_every_cluster_is_drawn_with_equal_frequency",
    ),
    (
        # ⚠⚠ The truncation removed, so each resample gathers ceil(n/b)*b > n
        # clusters. Biases the bootstrap variance DOWNWARD — a narrower interval
        # and a larger effective sample size, both in the flattering direction.
        "the resample not truncated back to the axis length",
        BOOT,
        BOOT_TESTS,
        [
            (
                "        index = index.reshape(batch, blocks_per_resample * block_length)[:, :n]",
                "        index = index.reshape(batch, blocks_per_resample * block_length)",
            )
        ],
        "test_every_resample_gathers_exactly_the_axis_length",
    ),
    (
        # ⚠⚠ KISH INVERTED. Multiplying by the design effect turns the overlap
        # correction into an overlap REWARD — the more correlated the trades, the
        # more evidence the row claims.
        "the effective sample size MULTIPLIED by the design effect",
        BOOT,
        BOOT_TESTS,
        [
            (
                "    effective_sample_size = clusters.trade_count / design_effect",
                "    effective_sample_size = clusters.trade_count * design_effect",
            )
        ],
        "test_clustering_a_population_onto_fewer_dates_costs_sample_size",
    ),
    (
        # ⚠ The design effect inverted. Same direction of harm as above, one
        # layer up, and it also flips the reported deff below 1.
        "the design effect computed upside down",
        BOOT,
        BOOT_TESTS,
        [
            (
                "    design_effect = bootstrap_variance / iid_variance",
                "    design_effect = iid_variance / bootstrap_variance",
            )
        ],
        "test_clustering_a_population_onto_fewer_dates_costs_sample_size",
    ),
    (
        # ⚠ The percentile tail halved — a one-sided 95% read as a two-sided one,
        # which reports an interval narrower than its own label.
        "the interval tail not halved between the two sides",
        BOOT,
        BOOT_TESTS,
        [("    tail = (1.0 - confidence) / 2.0 * 100.0", "    tail = (1.0 - confidence) * 100.0")],
        "test_the_interval_covers_the_declared_share_of_the_distribution",
    ),
    (
        # ⚠⚠ The declared seed ignored, so every result silently carries no
        # criterion-3 correction while the caller believes it asked for one.
        "compute_metrics ignoring the declared bootstrap seed",
        STATS,
        STATS_TESTS,
        [("    if bootstrap_seed is not None:", "    if bootstrap_seed is None and bootstrap_seed is not None:")],
        "test_a_declared_seed_fills_the_sample_size_and_the_interval",
    ),
    (
        # ⚠ The all-or-nothing invariant removed: a row could carry a corrected
        # sample size with no interval to judge the correction by.
        "a partial block-bootstrap field set admitted",
        STATS,
        STATS_TESTS,
        [("        if present not in (0, len(bootstrap_fields)):", "        if False:")],
        "test_dropping_any_single_field_is_refused",
    ),
    (
        # ⚠ Returns and their cluster dates allowed to fall out of step, which
        # clusters returns under the wrong dates and raises nowhere downstream.
        "the trade/date parallelism check removed",
        STATS,
        STATS_TESTS,
        [
            (
                "        if len(self.net_return_pct) != len(self.entry_fill_date):",
                "        if False:",
            )
        ],
        "test_returns_and_entry_dates_must_be_parallel",
    ),
]


def main() -> int:
    originals = {source: source.read_text() for source in SOURCES}
    failures: list[str] = []
    try:
        for name, source, tests, edits, selector in PROBES:
            count = selected(tests, selector)
            if count == 0:
                failures.append(f"{name}: selector {selector!r} names no test — probe proves nothing")
                print(f"  {'*** NO SUCH TEST ***':<24} {name}", flush=True)
                continue
            mutated = originals[source]
            bad_anchor = False
            for old, new in edits:
                occurrences = mutated.count(old)
                if occurrences != 1:
                    failures.append(
                        f"{name}: anchor occurs {occurrences} times, expected exactly 1 — probe proves nothing"
                    )
                    bad_anchor = True
                    break
                mutated = mutated.replace(old, new)
            if bad_anchor:
                print(f"  {'*** BAD ANCHOR ***':<24} {name}", flush=True)
                continue
            rc_baseline = run([tests], selector)
            if rc_baseline != PYTEST_PASSED:
                failures.append(f"{name}: baseline exit {rc_baseline} on unmutated source — probe proves nothing")
                print(f"  {'*** BAD BASELINE ***':<24} {name}  (exit {rc_baseline})", flush=True)
                continue
            source.write_text(mutated)
            rc = run([tests], selector)
            source.write_text(originals[source])
            if rc == PYTEST_TEST_FAILED:
                verdict = "CAUGHT"
            elif rc == PYTEST_PASSED:
                verdict = "*** NOT CAUGHT ***"
                failures.append(name)
            else:
                verdict = f"*** HARNESS FAULT {rc} ***"
                failures.append(f"{name}: pytest exit {rc} is not a test result — the mutation was never evaluated")
            print(f"  {verdict:<24} {name}  ({count} test{'' if count == 1 else 's'})", flush=True)
    finally:
        # ⚠ Restored even on KeyboardInterrupt. A harness that can leave a
        # tracked source file mutated is one Ctrl-C away from a defect committed
        # by accident.
        for source, text in originals.items():
            source.write_text(text)

    rc_suite = run([BOOT_TESTS, STATS_TESTS], "test_")
    suite = "PASS" if rc_suite == PYTEST_PASSED else f"*** FAIL (exit {rc_suite}) ***"
    print(f"\n  restored suite: {suite}", flush=True)
    if rc_suite != PYTEST_PASSED:
        failures.append(f"restored suite exits {rc_suite}")
    if failures:
        print("\nUNCAUGHT:\n  " + "\n  ".join(failures), flush=True)
        return 1
    print(f"\nall {len(PROBES)} probes caught", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
