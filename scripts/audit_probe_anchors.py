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

⚠⚠ IT ALSO CHECKS THE HARNESS CAN BE LAUNCHED AT ALL, which is a THIRD decay
class and one this file's own access path hides. The anchor sweep reaches a
harness through ``import_module``, which puts the repo root on ``sys.path`` for
free — so it happily validates 27 anchors in a file whose ``__main__`` entry
raises ``ModuleNotFoundError`` under the command its own docstring prints.
Measured 2026-08-14 (#2695): four of the sixteen were in exactly that state
(``block_bootstrap``, ``deflated_sharpe``, ``result_model``, ``statistics``),
because #2357 fixed the seven harnesses it happened to touch and the rest were
never swept. A harness nobody can start proves as little as one with a dead
anchor, and it fails LOUDER — which is why it survived: nobody had started it.

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
import os
import subprocess
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


#: Executed by the child in ``_launch_failure``. ``run_name`` is deliberately NOT
#: ``"__main__"``, so module top level runs (the sibling import, the ``PROBES``
#: table) while ``main()`` does not — these harnesses mutate tracked source on
#: disk, and an audit that started one would be a far worse defect than any it
#: checks for. See the function's docstring for what that leaves unproved.
_LAUNCH_PROBE = "import runpy, sys; runpy.run_path(sys.argv[1], run_name='__harness_launch_check__')"


def _launch_failure(path: Path, root: Path) -> str | None:
    """The error a harness raises when its top level is executed BY PATH, or ``None``.

    ⚠ A FRESH INTERPRETER, not this one. ``python -c`` with ``cwd=scripts/``
    puts ``scripts/`` on ``sys.path[0]`` and the repo root nowhere, which is
    exactly the condition under which ``from scripts.probe_2240_cost_model
    import …`` raises. ``import_module`` cannot reproduce it — it needs the root
    on the path to find the harness at all — so every anchor this file checks is
    validated through an access path the harness's own entry point does not have.

    ⚠⚠ AN IN-PROCESS EMULATION IS NOT ENOUGH, and this is the half that is easy
    to miss. The first version manipulated ``sys.path`` in this interpreter and
    passed on a harness with its fix deliberately removed: the sweep had already
    ``import_module``-ed one, caching ``scripts.probe_2240_cost_model``, so the
    child import was answered from ``sys.modules`` and never consulted
    ``sys.path``. Purging ``sys.modules`` fixed that case and still left the
    warm interpreter's ``app.*`` and third-party caches, its import hooks, and
    a repo root removable only by exact string match. A subprocess has none of
    those (#2695; both defects found by revert-probing this function, not by
    reading it). ``PYTHONPATH`` is stripped from the child for the same reason —
    a caller exporting ``PYTHONPATH=.`` would otherwise mask the failure.

    ⚠ WHAT THIS STILL DOES NOT PROVE, because ``main()`` never runs: a harness
    whose ``main()`` is broken, or whose failure lives under its
    ``if __name__ == "__main__"`` guard, passes here and cannot be started. Only
    a real sweep settles that. The claim is bounded to "its module top level
    executes under a path launch", which is where the #2695 defect lived — the
    import is what raised — and is as far as a check that must not mutate source
    can go.
    """
    env = {name: value for name, value in os.environ.items() if name != "PYTHONPATH"}
    proc = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _LAUNCH_PROBE, str(root / path)],
        cwd=str(root / "scripts"),
        capture_output=True,
        text=True,
        env=env,
    )
    if proc.returncode == 0:
        return None
    stderr = proc.stderr.strip().splitlines()
    return stderr[-1] if stderr else f"exit {proc.returncode} with no stderr"


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    failures: list[str] = []
    total = 0
    harnesses = sorted((root / "scripts").glob(HARNESS_GLOB))

    for path in harnesses:
        module = importlib.import_module(f"scripts.{path.stem}")
        probes: list[tuple[Any, ...]] = getattr(module, "PROBES", [])
        default_src: Path | None = getattr(module, "SRC", None)
        sources: dict[Path, str] = {}
        checked = bad = 0

        launch = _launch_failure(path.relative_to(root), root)
        if launch is not None:
            bad += 1
            failures.append(
                f"{path.name}: its top level does not execute under a path launch — {launch}. Every anchor "
                f"below is checked through an import that adds the repo root, so they pass while the harness "
                f"itself will not start."
            )

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
                    try:
                        sources[src] = (root / src).read_text()
                    except OSError as exc:
                        # ⚠ REPORTED, never raised. A source file that has been
                        # renamed or deleted is the MOST complete form of the
                        # decay this audit exists to find — letting it abort the
                        # run would surface the worst case as a traceback and
                        # hide every other finding behind it. Not cached, so the
                        # next probe naming the same file reports it too rather
                        # than KeyError-ing on a half-populated cache.
                        bad += 1
                        failures.append(f"{path.name}: {name}: cannot read {src} — {type(exc).__name__}")
                        continue
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

    print(f"\n{total} anchors checked across {len(harnesses)} harnesses", flush=True)
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
