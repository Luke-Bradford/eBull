"""Every revert-probe anchor still matches its source exactly once (#2695).

    uv run python -m scripts.audit_probe_anchors
    bash scripts/check_probe_anchors.sh      # the pre-push / CI wrapper

WHAT THIS CATCHES, AND WHY NOTHING ELSE DOES
--------------------------------------------
A revert probe injects a defect by replacing a VERBATIM copy of a source line.
Each harness asserts its anchors occur exactly once before mutating, so a stale
anchor cannot fake a ``CAUGHT`` — it reports ``*** BAD ANCHOR ***`` instead.

⚠⚠ But that report only exists on a day somebody RUNS the harness, and nothing
runs them: a probe script is not a test, CI does not collect it (by design — it
mutates tracked source on disk), and the pre-push hook does not either. Meanwhile
the CLAIM derived from a run — "28 probes, all CAUGHT" — is written into a PR
description or a spec's acceptance section and read forever after as a property
of the code.

Precedent (#2357, 2026-08-14): ``probe_2240_position_builder``'s anchor for "an
unbookable hold left open forever" was a copy of
``open_until = ceiling if open_reason == "close_bar_unfillable" else None``. A
later ``series_break`` arm turned that into a three-way conditional and the
formatter split it across six lines. The anchor matched 0 times, and the phase-5a
claim of 28/28 had silently been 27 for however long. It was found by a re-run
that happened for an unrelated reason.

⚠ The decay is silent in the safe direction, which is what makes it survive: a
stale anchor removes a probe from the count without removing the sentence citing
the count. A full sweep costs ~35 s/probe (two pytest subprocesses each) — near
an hour for the 280 probes below. This check is the part of that sweep which
needs no pytest at all, so it can run on every push.

WHAT THIS DOES NOT CATCH
------------------------
⚠ **A branch that no probe names.** The harnesses' two standing guards are
properties of probes that EXIST — anchor uniqueness, and delete-don't-wrap — and
both are vacuously satisfied by an arm nobody wrote a probe for. Precedent
(#2689): the ``series_break`` arm of the very same expression had no probe at
all, because it post-dated the harness. That class needs a per-harness reading of
the source against the probe list; it is #2695's other half and it is not
mechanizable.

⚠ **Whether the mutation still fails the test.** A selector can rot (the test is
renamed, or its fixture drifts until it no longer observes the defect) with the
anchor perfectly intact. Only a real run settles that.

⚠ Scope is ``scripts/probe_2240_*.py`` deliberately. Several other ``probe_*.py``
files execute work at import — ``probe_def14a_high_identity_escape.py`` reads
``sys.argv[1]`` at module level — so importing the whole glob would run them.

A NOTE ON WHAT A FAILURE MEANS
------------------------------
⚠ A failure here has two causes and they need different fixes. Either an anchor
went stale (re-anchor it, preferably on the smallest span that still identifies
the rule, so it survives edits to its siblings), or **a probe run was killed and
left a tracked source file mutated** — the harnesses restore in a ``finally``,
which survives Ctrl-C but not a hard kill. Check ``git status`` before
re-anchoring anything.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

# ⚠ Run as `-m scripts.audit_probe_anchors`, but `python scripts/audit_probe_anchors.py`
# puts `scripts/` on sys.path rather than the repo root and the import below then
# raises ModuleNotFoundError. Prepending the root makes both forms work (#2357).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

HARNESS_GLOB = "probe_2240_*.py"


def _edits_of(probe: tuple[Any, ...]) -> list[tuple[Any, ...]] | None:
    """The (anchor, replacement) list inside one probe tuple.

    ⚠ Found BY SHAPE, not by position. The 16 harnesses carry five different
    tuple arities (3, 4 and 5) and three ways of naming the source, because each
    grew whatever its own invariants needed. Indexing by ordinal would make this
    audit the seventeenth thing that has to be updated when a harness changes —
    and it would fail silently, reading some other field as the edit list.
    """
    for element in probe:
        if isinstance(element, list) and element and all(isinstance(item, tuple) for item in element):
            return element
    return None


def _anchor_and_source(edit: tuple[Any, ...], probe: tuple[Any, ...], default: Path | None) -> tuple[str, Path]:
    """The anchor string and the file it must occur in, for one edit.

    Three shapes in the tree, in narrowing order of specificity: the source can
    be named per EDIT (``probe_2240_s2_cross_sectional``, whose edits are
    ``(Path, old, new)``), per PROBE (the ``SOURCES`` harnesses, which carry a
    ``Path`` element in the tuple), or once for the module (``SRC``).
    """
    if len(edit) == 3 and isinstance(edit[0], Path):
        return str(edit[1]), edit[0]
    for element in probe:
        if isinstance(element, Path):
            return str(edit[0]), element
    if default is None:
        raise ValueError("probe names no source file and its module declares no SRC")
    return str(edit[0]), default


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    failures: list[str] = []
    total = 0

    for path in sorted((root / "scripts").glob(HARNESS_GLOB)):
        module = importlib.import_module(f"scripts.{path.stem}")
        probes: list[tuple[Any, ...]] = getattr(module, "PROBES", [])
        default_src: Path | None = getattr(module, "SRC", None)
        sources: dict[Path, str] = {}
        checked = bad = 0

        for probe in probes:
            name = probe[0]
            edits = _edits_of(probe)
            if edits is None:
                failures.append(f"{path.name}: {name}: no (anchor, replacement) list found in the probe tuple")
                bad += 1
                continue
            for edit in edits:
                try:
                    anchor, src = _anchor_and_source(edit, probe, default_src)
                except ValueError as exc:
                    failures.append(f"{path.name}: {name}: {exc}")
                    bad += 1
                    continue
                if src not in sources:
                    sources[src] = (root / src).read_text()
                occurrences = sources[src].count(anchor)
                checked += 1
                total += 1
                if occurrences != 1:
                    bad += 1
                    failures.append(
                        f"{path.name}: {name}: anchor occurs {occurrences} times in {src} "
                        f"(expected exactly 1) — this probe currently proves nothing"
                    )

        marker = "OK " if bad == 0 else "***"
        print(f"  {marker} {path.name:<45} {len(probes):>3} probes, {checked:>3} anchors", flush=True)

    print(f"\n{total} anchors checked across {len(list((root / 'scripts').glob(HARNESS_GLOB)))} harnesses", flush=True)
    if failures:
        print("\nSTALE:\n  " + "\n  ".join(failures), file=sys.stderr, flush=True)
        print(
            "\n⚠ Two causes, two different fixes: an anchor went stale (re-anchor it), or a killed "
            "probe run left a tracked source file mutated (check `git status` FIRST).",
            file=sys.stderr,
            flush=True,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
