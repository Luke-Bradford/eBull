"""The probe-anchor audit resolves every harness shape in the tree (#2695).

⚠ The audit's own failure mode is going INERT. It finds the edit list and the
source file BY SHAPE, across five tuple arities and three ways of naming the
source, so a harness whose shape it cannot read would otherwise contribute zero
anchors and still print ``OK``. These tests pin the resolution for each shape and
pin that an unreadable shape is REPORTED rather than skipped — which is the same
defect class the audit exists to catch, one level up.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.audit_probe_anchors import _anchor_and_source, _edits_of, main

SRC_A = Path("app/services/alpha.py")
SRC_B = Path("app/services/beta.py")
EDITS = [("old", "new")]


class TestEditsOf:
    def test_finds_the_edit_list_in_the_three_tuple_shape(self) -> None:
        # (name, edits, selector) — probe_2240_position_builder and siblings.
        assert _edits_of(("a name", EDITS, "test_selector")) == EDITS

    def test_finds_the_edit_list_in_the_five_tuple_shape(self) -> None:
        # (name, source, tests, edits, selector) — the SOURCES harnesses.
        assert _edits_of(("a name", SRC_A, "tests/test_x.py", EDITS, "test_selector")) == EDITS

    def test_finds_a_per_edit_source_triple(self) -> None:
        # probe_2240_s2_cross_sectional: the edits are (Path, old, new).
        edits = [(SRC_A, "old", "new")]
        assert _edits_of(("a name", edits, "tests/test_x.py", "test_selector")) == edits

    def test_an_unreadable_shape_is_reported_not_silently_skipped(self) -> None:
        # No list-of-tuples anywhere. `main` turns None into a failure line; the
        # dangerous alternative is returning [] and printing OK for 0 anchors.
        assert _edits_of(("a name", "not a list", "test_selector")) is None

    def test_an_empty_edit_list_is_not_mistaken_for_the_edit_list(self) -> None:
        # An empty list matches "list of tuples" vacuously. Taking it would make
        # the probe contribute nothing while looking resolved.
        assert _edits_of(("a name", [], EDITS, "test_selector")) == EDITS


class TestAnchorAndSource:
    def test_a_per_edit_path_wins_over_everything(self) -> None:
        probe = ("a name", [(SRC_A, "old", "new")], "tests/test_x.py", "sel")
        assert _anchor_and_source((SRC_A, "old", "new"), probe, SRC_B) == ("old", SRC_A)

    def test_a_per_probe_path_beats_the_module_default(self) -> None:
        probe = ("a name", SRC_A, "tests/test_x.py", EDITS, "sel")
        assert _anchor_and_source(("old", "new"), probe, SRC_B) == ("old", SRC_A)

    def test_the_module_src_is_used_when_the_probe_names_none(self) -> None:
        assert _anchor_and_source(("old", "new"), ("a name", EDITS, "sel"), SRC_B) == ("old", SRC_B)

    def test_no_source_anywhere_raises_rather_than_guessing_one(self) -> None:
        with pytest.raises(ValueError, match="names no source file"):
            _anchor_and_source(("old", "new"), ("a name", EDITS, "sel"), None)


def test_every_harness_in_the_tree_contributes_at_least_one_anchor() -> None:
    """⚠ The inert-audit guard, run against the REAL harnesses.

    A shape the resolver cannot read produces a harness with 0 anchors, and the
    per-harness line still prints. This asserts the resolver actually reaches
    every probe in every `probe_2240_*.py`, so adding a harness in a new shape
    fails here rather than quietly shrinking the audit's coverage.
    """
    import importlib

    root = Path(__file__).resolve().parents[1]
    harnesses = sorted((root / "scripts").glob("probe_2240_*.py"))
    assert harnesses, "no probe_2240_* harnesses found — the audit's glob has drifted"
    for path in harnesses:
        module = importlib.import_module(f"scripts.{path.stem}")
        probes = getattr(module, "PROBES", [])
        assert probes, f"{path.name} declares no PROBES"
        for probe in probes:
            edits = _edits_of(probe)
            assert edits, f"{path.name}: {probe[0]}: the audit cannot find this probe's edit list"
            for edit in edits:
                anchor, source = _anchor_and_source(edit, probe, getattr(module, "SRC", None))
                assert anchor, f"{path.name}: {probe[0]}: empty anchor"
                assert (root / source).exists(), f"{path.name}: {probe[0]}: {source} does not exist"


def test_an_unreadable_source_is_reported_not_raised(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """⚠ A deleted or renamed source is the MOST complete form of this decay.

    Letting `read_text` propagate would surface the worst case as a traceback and
    hide every other finding behind it — the audit would fail loudest exactly
    where it has the most to say. Every read is made to fail here, so the run
    must still complete, return 1, and attribute a failure per probe.
    """
    original = Path.read_text

    def _refuse(self: Path, *args: object, **kwargs: object) -> str:
        if self.suffix == ".py" and "app/services" in self.as_posix():
            raise OSError("no such file (synthetic)")
        return original(self, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, "read_text", _refuse)
    assert main() == 1
    # ⚠ The exit code alone does not discriminate: the audit returns 1 today for
    # genuinely stale anchors too, so a test asserting only that would pass with
    # the guard removed (it would ERROR on the raise, but for the wrong reason,
    # and would go green the day the anchors are all fixed). Assert the reason.
    stderr = capsys.readouterr().err
    assert "cannot read" in stderr
    assert "OSError" in stderr
