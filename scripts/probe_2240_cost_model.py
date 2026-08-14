"""Revert-probe the phase-5b cost-model invariant tests (#2240).

    uv run python scripts/probe_2240_cost_model.py

Sister to ``scripts/probe_2240_position_builder.py``, whose three guards apply
unchanged:

1. ⚠ **Every anchor must occur EXACTLY ONCE**, asserted before the replace. A
   probe that silently matches nothing mutates nothing, the test passes, and the
   harness reports ``CAUGHT`` for a defect it never injected.
2. ⚠ **A probe must DELETE or INVERT behaviour, never wrap it.** Guard 1 says
   nothing about whether the replacement changes anything.
3. ⚠ **The SELECTOR is not guarded by either.** ``NOT CAUGHT`` has three causes
   and the triage order is selector → fixture → code (prevention log, #2240
   S-2).

Plus two this harness adds, both from #2214's entry and neither present in the
5a sister:

4. ⚠⚠ **Gate on exit code 1, never on "non-zero"** — see ``PYTEST_TEST_FAILED``.
5. ⚠ **Run a BASELINE first.** The selected test must PASS on unmutated source
   before anything is injected; otherwise "the mutation broke it" and "it was
   already broken" are the same observation, and the second reads as ``CAUGHT``.

⚠ TWO SOURCE FILES, so each probe names its own — ``cost_model`` is a leaf and
``position_costing`` is its consumer, and a defect in either is a different
class. The harness restores both regardless of which one a probe touched.

⚠ NOT A TEST, and it must never become one: it mutates tracked source files on
disk. CI does not run it. Everything here is pure-tier, so no database is needed.

⚠ MUST NOT RUN CONCURRENTLY with ``scripts/verify_2240_cost_model.py`` or
``verify_2240_position_builder.py``. Phase 4b's lesson: a concurrent run stamps
its output with the INJECTED source hash, and a start-vs-end check misses it
because the probe restores the file.

WHAT IS NOT PROBED, AND WHY
---------------------------
⚠ **``band_for``'s unreachable "no band covers this price" raise.** It is
unreachable while ``_check_bands_are_total`` holds, so no fixture can construct
it through the public path. The check that makes it unreachable IS probed.

⚠ **``CALIBRATION_LIMITS`` and the other provenance constants.** They are
reported, not enforced; nothing branches on them, so there is no behaviour to
revert. ``--calibrate`` prints the live figures beside them, which is the
mechanism that keeps them honest.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

MODEL = Path("app/services/cost_model.py")
COSTING = Path("app/services/position_costing.py")
SOURCES = (MODEL, COSTING)

MODEL_TESTS = "tests/test_cost_model.py"
COSTING_TESTS = "tests/test_position_costing.py"

#: ⚠⚠ GATE ON EXIT CODE 1, NEVER ON "NON-ZERO" (prevention log, #2214). pytest
#: exits 1 for a test failure and 2/3/4/5 for interrupted / internal error /
#: USAGE error / no tests collected. A harness reading "non-zero" as CAUGHT
#: reports a clean sweep for mutations that were never evaluated — a syntax
#: break in the injected source exits 4 and reads as a catch. That direction is
#: the dangerous one: NOT CAUGHT is loud and gets triaged, a false CAUGHT is
#: silent and its conclusion is exactly what nobody re-checks.
PYTEST_PASSED = 0
PYTEST_TEST_FAILED = 1

#: (what the injected defect IS, source file, test file, [(anchor, replacement), ...], -k selector)
PROBES: list[tuple[str, Path, str, list[tuple[str, str]], str]] = [
    (
        # ⚠⚠ THE PIN ITSELF. Acceptance C2(c) wants the band table pinned so a
        # recalibration cannot land without somebody deciding whether the
        # cost_model_id should have moved with it. This reverts one band to
        # §5.1's printed figure — the exact edit a careless "sync with the spec"
        # would make.
        "a frozen band silently recalibrated back to the §5.1 figure",
        MODEL,
        MODEL_TESTS,
        [('p75_spread_pct=Decimal("0.322")', 'p75_spread_pct=Decimal("0.316")')],
        "test_every_band_matches_the_frozen_calibration",
    ),
    (
        # ⚠ THIS ANCHOR IS THE FROZEN VALUE, so a LEGITIMATE re-freeze breaks it
        # by construction — there is no smaller span, because the rule being
        # reverted IS the literal. That is the intended cost: `audit_probe_anchors`
        # now fails at push time on a re-freeze that did not update this probe,
        # where before the probe simply stopped proving anything in silence
        # (#2695 — it had been stranded on `-v1` since the v2 split).
        "the cost model id renamed without the table moving",
        MODEL,
        MODEL_TESTS,
        [
            (
                'COST_MODEL_ID = "static-p75-insession-v2+split-adjusted-max"',
                'COST_MODEL_ID = "static-p75-v3"',
            )
        ],
        "test_the_cost_model_id_is_the_frozen_one",
    ),
    (
        # ⚠ /200 is p75-as-percent → one side as a FRACTION. /100 charges the
        # whole round trip on each side, i.e. double the cost, which no
        # arithmetic test on adjusted prices would notice on its own.
        "the half-spread charging the full round trip per side",
        MODEL,
        MODEL_TESTS,
        [("return self.p75_spread_pct / 200", "return self.p75_spread_pct / 100")],
        "test_the_half_spread_is_half_the_round_trip",
    ),
    (
        "the buy side paying DOWN instead of up",
        MODEL,
        MODEL_TESTS,
        [
            (
                "    return price * (Decimal(1) + half_spread)",
                "    return price * (Decimal(1) - half_spread)",
            )
        ],
        "test_a_buy_pays_the_half_spread",
    ),
    (
        "the sell side paying UP instead of down",
        MODEL,
        MODEL_TESTS,
        [
            (
                "    return price * (Decimal(1) - half_spread)",
                "    return price * (Decimal(1) + half_spread)",
            )
        ],
        "test_a_sell_pays_the_half_spread",
    ),
    (
        # ⚠ Without the raise, a zero or negative price falls through to the
        # lowest band (``lower is None``), so a position built from something
        # that is not a price gets costed as a penny stock.
        "the non-positive-price refusal removed (a zero price bands as <$5)",
        MODEL,
        MODEL_TESTS,
        [
            (
                "    if price <= 0:\n"
                '        raise ValueError(f"price must be > 0 to carry a cost band, got {price}")\n',
                "",
            )
        ],
        "test_a_non_positive_price_has_no_band",
    ),
    (
        # ⚠ The lower bound is INCLUSIVE. Made strict, a price exactly on a
        # boundary belongs to no band and `band_for` raises the gap error — a
        # crash on an ordinary $5.00 fill.
        "the band's lower bound made exclusive (a boundary price falls in no band)",
        MODEL,
        MODEL_TESTS,
        [
            (
                "        return (self.lower is None or price >= self.lower) and "
                "(self.upper is None or price < self.upper)",
                "        return (self.lower is None or price > self.lower) and "
                "(self.upper is None or price < self.upper)",
            )
        ],
        "test_a_price_lands_in_the_band_that_claims_it",
    ),
    (
        "the contiguity check removed (a gap between two bands imports fine)",
        MODEL,
        MODEL_TESTS,
        [
            (
                "    for lower_band, upper_band in zip(bands, bands[1:], strict=False):\n"
                "        if lower_band.upper != upper_band.lower:\n"
                "            raise ValueError(\n"
                '                f"bands {lower_band.label} and {upper_band.label} are not contiguous: '
                '{lower_band.label} ends at "\n'
                '                f"{lower_band.upper} and {upper_band.label} starts at {upper_band.lower}"\n'
                "            )\n",
                "",
            )
        ],
        "test_a_gap_between_two_bands_is_rejected",
    ),
    (
        # ⚠ At h >= 1 the sell side is zero or negative. The return still
        # divides cleanly, which is why an upper bound exists at all.
        "the half-spread upper bound removed (a sell side at or below zero)",
        MODEL,
        MODEL_TESTS,
        [
            (
                "    if half_spread >= 1:\n"
                '        raise ValueError(f"half_spread must be < 1, got {half_spread} — a sell side at or below '
                'zero is not a price")\n',
                "",
            )
        ],
        "test_a_half_spread_at_or_above_one_is_rejected",
    ),
    (
        # ⚠ #2286's shape, injected: a value that is PRESENT and wrong. Setting
        # carry to zero also flips CARRY_UNMODELLED, which is what the promotion
        # gate refuses on — so this one defect quietly promotes every result.
        "carry set to zero instead of NULL (which also clears the unmodelled marker)",
        MODEL,
        MODEL_TESTS,
        [("CARRY_BPS: Decimal | None = None", 'CARRY_BPS: Decimal | None = Decimal("0")')],
        "test_carry_is_none or test_the_unmodelled_marker_is_set",
    ),
    (
        # ⚠⚠ THE RE-KEY. §5.1: *"re-keying mid-hold would make the cost depend
        # on the outcome"* — a winner that ran from $4 to $500 would be charged
        # the cheap band on the way out.
        "the exit side re-keyed to the EXIT band instead of the entry band",
        COSTING,
        COSTING_TESTS,
        [
            (
                "        exit_net = sell_price(exit_gross, half_spread=half_spread)",
                "        exit_net = sell_price(exit_gross, half_spread=half_spread_for(exit_gross))",
            )
        ],
        "test_the_exit_side_uses_the_entry_half_spread",
    ),
    (
        # ⚠ §5.1's explicit prohibition: *"never by subtracting a cost from
        # gross_return_pct"*. The two differ because the half-spread is
        # multiplicative in the denominator as well as the numerator.
        "the net return computed by SUBTRACTING the cost from the gross return",
        COSTING,
        COSTING_TESTS,
        [
            (
                "        net_return = (exit_net - entry_net) / entry_net * hundred",
                "        net_return = gross_return - 2 * half_spread * hundred",
            )
        ],
        "test_the_net_return_is_computed_from_the_adjusted_prices",
    ),
    (
        # ⚠⚠ THE MIRROR DEFECT, injected — the 3c prevention entry. Under the
        # ANDed form a row that says "no exit" while carrying a stray return
        # passes, and is then silently excluded from every statistic instead of
        # raising. The counted form is the only one that sees it.
        "the partial-costing guard rewritten from COUNTED to ANDed",
        COSTING,
        COSTING_TESTS,
        [
            (
                "        priced = (\n"
                "            (self.exit_price_gross is not None)\n"
                "            + (self.exit_price_net is not None)\n"
                "            + (self.gross_return_pct is not None)\n"
                "            + (self.net_return_pct is not None)\n"
                "        )\n"
                "        if priced != (4 if self.exit_basis is not None else 0):",
                "        priced = (\n"
                "            self.exit_price_gross is not None\n"
                "            and self.exit_price_net is not None\n"
                "            and self.gross_return_pct is not None\n"
                "            and self.net_return_pct is not None\n"
                "        )\n"
                "        if priced != (self.exit_basis is not None):",
            )
        ],
        "test_an_unpriced_row_carrying_a_stray_return_is_rejected",
    ),
    (
        "the uncharged-entry guard removed (a free buy)",
        COSTING,
        COSTING_TESTS,
        [
            (
                "        if self.entry_price_net <= self.position.entry_fill_price:\n"
                "            raise ValueError(\n"
                '                f"position on signal {self.position.entry_signal_id}: net entry '
                '{self.entry_price_net} does not "\n'
                '                f"exceed the gross fill {self.position.entry_fill_price} — a buy pays the '
                'spread (criterion 2 "\n'
                '                "forbids a zero-cost trade)"\n'
                "            )\n",
                "",
            )
        ],
        "test_an_uncharged_entry_is_rejected",
    ),
    (
        "the costs-cannot-improve-a-trade guard removed",
        COSTING,
        COSTING_TESTS,
        [
            (
                "            if self.net_return_pct >= self.gross_return_pct:\n"
                "                raise ValueError(\n"
                '                    f"position on signal {self.position.entry_signal_id}: net return '
                '{self.net_return_pct} is not "\n'
                '                    f"below gross {self.gross_return_pct} — costs cannot improve a trade"\n'
                "                )\n",
                "",
            )
        ],
        "test_a_net_return_at_or_above_gross_is_rejected",
    ),
    (
        # ⚠ §3.2 rule 5's one side on the mark. Without the branch every open
        # position is `no_mark`, so an unrealised hold contributes nothing to the
        # equity curve — which is the bias toward positions that CLOSED that
        # rule 5 exists to prevent.
        "the open-position mark dropped (every open position becomes no_mark)",
        COSTING,
        COSTING_TESTS,
        [('    return position.mark_price, "mark", None', '    return None, None, "no_mark"')],
        "test_the_mark_is_charged_the_exit_side",
    ),
    (
        "band crossings no longer counted",
        COSTING,
        COSTING_TESTS,
        [
            (
                "        if band_for(row.exit_price_gross).label != row.band_label:\n            crossings += 1\n",
                "",
            )
        ],
        "test_crossings_are_counted_not_prevented",
    ),
]


def run(tests: list[str], selector: str) -> int:
    """The named tests, in a subprocess so the mutated module is re-imported."""
    return subprocess.run(
        ["uv", "run", "pytest", *tests, "-q", "-k", selector, "-p", "no:randomly", "-n", "0"],
        capture_output=True,
    ).returncode


def selected(tests: str, selector: str) -> int:
    """How many tests the selector actually names.

    ⚠ A selector matching ZERO tests makes ``pytest`` exit non-zero, which the
    harness would read as ``CAUGHT``. ⚠ ``-q --collect-only`` prints one
    ``path: <count>`` line per FILE, not one line per test id — counting ``::``
    returns 0 for every selector (prevention log, #2240 5a).
    """
    result = subprocess.run(
        ["uv", "run", "pytest", tests, "-q", "-k", selector, "-p", "no:randomly", "-n", "0", "--collect-only"],
        capture_output=True,
        text=True,
    )
    total = 0
    for line in result.stdout.splitlines():
        head, _, tail = line.partition(": ")
        if head == tests and tail.strip().isdigit():
            total += int(tail.strip())
    return total


def main() -> int:
    originals = {source: source.read_text() for source in SOURCES}
    failures: list[str] = []
    try:
        for name, source, tests, edits, selector in PROBES:
            count = selected(tests, selector)
            if count == 0:
                failures.append(f"{name}: selector {selector!r} names no test — probe proves nothing")
                print(f"  {'*** NO SUCH TEST ***':<20} {name}", flush=True)
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
                print(f"  {'*** BAD ANCHOR ***':<20} {name}", flush=True)
                continue
            # ⚠⚠ BASELINE FIRST — assert the selected test PASSES on unmutated
            # source before mutating anything. Without it a probe cannot tell
            # "the mutation broke the test" from "the test was already broken",
            # and the second reads as CAUGHT (prevention log, #2214).
            rc_baseline = run([tests], selector)
            if rc_baseline != PYTEST_PASSED:
                failures.append(f"{name}: baseline exit {rc_baseline} on unmutated source — probe proves nothing")
                print(f"  {'*** BAD BASELINE ***':<20} {name}  (exit {rc_baseline})", flush=True)
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
            print(f"  {verdict:<20} {name}  ({count} test{'' if count == 1 else 's'})", flush=True)
    finally:
        # ⚠ Restored even on KeyboardInterrupt. A harness that can leave a
        # tracked source file mutated is one Ctrl-C away from a defect committed
        # by accident.
        for source, text in originals.items():
            source.write_text(text)

    rc_suite = run([MODEL_TESTS, COSTING_TESTS], "test_")
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
