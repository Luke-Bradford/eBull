"""#2793 — the insider bulk ingest cannot be asked to widen the operator layer.

`ingest_insider_dataset_archive` carried a `retention_cutoff_override` from #2701 until
2026-08-22. Its comment argued the injection was safe because "both consumers keep the
boundary they need". That was false and structurally so: the function has exactly ONE
INSERT target, so there is one table and one boundary. One run using it put 4,189,940
`source='form4'` rows beyond the cap into `ownership_insiders_observations` — the sole
source of the operator's insiders wedge — and delivered nothing to the research corpus it
was for, because #2701's consumer reads tables this function never writes.

Pure: these read a signature and a module's SQL. The cap's runtime behaviour is covered by
`tests/test_insider_transactions_retention_cap.py`, whose eight call sites all pass the
default and keep passing unchanged.
"""

from __future__ import annotations

import inspect
from pathlib import Path

from app.services import sec_insider_dataset_ingest
from app.services.sec_insider_dataset_ingest import (
    form4_retention_cutoff,
    form5_retention_cutoff,
    ingest_insider_dataset_archive,
)


def test_the_ingest_accepts_no_parameter_that_can_move_the_cutoff() -> None:
    """⚠ This forbids the removed affordance coming back, NOT "widening under any
    name" — Codex ckpt-1 correctly called that broader claim unenforceable from a
    signature. Widening could still return through a setting, a global, or a second
    writer; what this pins is the specific regression that already happened once.
    """
    parameters = set(inspect.signature(ingest_insider_dataset_archive).parameters)
    assert parameters == {"conn", "archive_path", "cik_to_instrument", "ingest_run_id"}
    assert not any("cutoff" in name or "retention" in name for name in parameters)


def test_the_cutoffs_are_computed_from_the_module_and_not_from_a_caller() -> None:
    source = inspect.getsource(ingest_insider_dataset_archive)
    assert "retention_cutoff = form4_retention_cutoff()" in source
    assert "retention_cutoff_form5 = form5_retention_cutoff()" in source
    # No `or <something>` fallback, which is exactly how the override was expressed.
    assert "form4_retention_cutoff() or" not in source
    assert "or form4_retention_cutoff()" not in source
    assert "or form5_retention_cutoff()" not in source


def test_the_three_retention_rules_are_distinct_and_ordered() -> None:
    """⚠ Pinned TOGETHER, because testing Form 4 alone leaves the others free to
    drift (ckpt-1). Form 5's cap is 18 months and Form 4's is 3 years, so Form 5's
    cutoff must be the LATER date; Form 3 has no cutoff function at all, and its
    exemption is as much an invariant as the caps are — forgetting it is what made a
    draft of this ticket's own measurement count 142,495 legitimate rows as a
    violation.
    """
    assert form5_retention_cutoff() > form4_retention_cutoff()
    assert not hasattr(sec_insider_dataset_ingest, "form3_retention_cutoff")


def test_the_only_write_target_is_the_operator_layer() -> None:
    """The fact that made the override unfixable-by-widening, asserted rather than
    recalled: one INSERT, and it is the table the ownership rollup reads. A second
    INSERT appearing here means a research destination was added — at which point
    this test should be updated deliberately, not deleted.
    """
    source = Path(sec_insider_dataset_ingest.__file__).read_text()
    inserts = [line.strip() for line in source.splitlines() if "INSERT INTO" in line]
    assert inserts == ["INSERT INTO ownership_insiders_observations ("]


def test_the_research_backfill_script_is_gone() -> None:
    """It was the only caller that passed the override, it achieved the inverse of
    its stated intent, and a single `uv run` would repeat the write. It stays in git
    history, where a future research route can read what it tried to do.
    """
    repo_root = Path(sec_insider_dataset_ingest.__file__).parents[2]
    assert not (repo_root / "scripts" / "backfill_2701_insider_research_ingest.py").exists()
