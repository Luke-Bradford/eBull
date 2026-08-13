"""#1486 — guard the four parallel bootstrap-stage lane vocabularies.

Four independent definitions describe ``bootstrap_stages.lane`` and can
silently drift:

* ``app/jobs/sources.py::Lane`` — the full lane vocabulary (superset).
* ``app/api/bootstrap.py::LaneApi`` — API row-shape Literal.
* ``app/services/bootstrap_state.py::Lane`` — read row-shape Literal.
* the ``bootstrap_stages_lane_check`` CHECK constraint (sql/NNN).

``LaneApi`` and ``bootstrap_state.Lane`` are deliberately NOT equal to
``Lane``: they carry a legacy ``"sec"`` catch-all and omit
non-bootstrap scheduled lanes (``finra``, ``bootstrap``, ``sec_manifest``,
the per-CIK / insider lanes). So a naive ``set(LaneApi.__args__) ==
set(Lane.__args__)`` test is WRONG (false-fails today).

The *correct* invariant (issue #1486): every lane that can be WRITTEN to
``bootstrap_stages.lane`` — i.e. every ``_effective_lane`` of a stage in
``_BOOTSTRAP_STAGE_SPECS`` (consulting ``_STAGE_LANE_OVERRIDES``) — MUST
be a member of all three downstream definitions. A new bootstrap-stage
lane added to ``sources.py::Lane`` + overrides but forgotten in a
row-shape Literal or the CHECK allow-list would surface as a runtime
CHECK violation (not caught at boot). ``"openfigi"`` (#1233 PR-1b S13)
was exactly such a miss in ``bootstrap_state.Lane`` until this guard.

DB-free: the CHECK allow-list is parsed from the migration files on
disk (last-defining migration wins, mirroring apply order), so no
Postgres connection is required.
"""

from __future__ import annotations

import re
from typing import get_args

from app.api.bootstrap import LaneApi
from app.db.migrations import MIGRATIONS_DIR
from app.jobs.sources import Lane as SourcesLane
from app.services.bootstrap_state import Lane as StateLane

_LANE_CHECK_CONSTRAINT = "bootstrap_stages_lane_check"


def _effective_bootstrap_lanes() -> frozenset[str]:
    """The lanes actually writable to ``bootstrap_stages.lane`` — the
    source of truth every downstream definition must cover.

    Imports are lazy: ``app.services.bootstrap_orchestrator`` sits behind a
    ``insider_transactions`` <-> ``manifest_parsers`` import cycle that only
    resolves once ``manifest_parsers`` is imported first (repo idiom — see
    ``tests/test_manifest_parser_eight_k.py``). A module-level import would
    fail collection in isolation."""
    import app.services.manifest_parsers  # noqa: F401  # break the import cycle first
    from app.services.bootstrap_orchestrator import (
        _BOOTSTRAP_STAGE_SPECS,
        _effective_lane,
    )

    return frozenset(_effective_lane(s.stage_key, s.lane) for s in _BOOTSTRAP_STAGE_SPECS)


def _sql_lane_check_allow_list() -> frozenset[str]:
    """Parse the ``bootstrap_stages_lane_check`` allow-list from the
    last migration that (re)defines it. Migrations apply in lexicographic
    filename order, so the highest-numbered defining file is the live
    constraint — same resolution the runtime DB ends up with.

    Guards against a stale parse: if any migration AFTER the last
    ``ADD CONSTRAINT`` drops the constraint without re-adding it, the
    runtime DB has no allow-list to check against, so this helper must
    fail loudly rather than return the obsolete set (Codex #1486)."""
    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    add_re = re.compile(r"ADD\s+CONSTRAINT\s+" + _LANE_CHECK_CONSTRAINT)
    drop_re = re.compile(r"DROP\s+CONSTRAINT\s+(?:IF\s+EXISTS\s+)?" + _LANE_CHECK_CONSTRAINT)

    last_add_idx = next(
        (i for i in range(len(files) - 1, -1, -1) if add_re.search(files[i].read_text(encoding="utf-8"))),
        None,
    )
    assert last_add_idx is not None, (
        f"no migration defines CONSTRAINT {_LANE_CHECK_CONSTRAINT} in {MIGRATIONS_DIR} — "
        "the parser would otherwise vacuously pass; check the constraint name / migration layout"
    )
    last = files[last_add_idx]
    orphaning_drops = [
        files[i].name
        for i in range(last_add_idx + 1, len(files))
        if drop_re.search(files[i].read_text(encoding="utf-8"))
    ]
    assert not orphaning_drops, (
        f"CONSTRAINT {_LANE_CHECK_CONSTRAINT} is DROPped after its last ADD "
        f"(in {last.name}) by {orphaning_drops} with no re-add — the live DB has no "
        "lane allow-list, so this guard cannot verify the SQL side; re-add the "
        "constraint or update the guard."
    )
    block = re.search(
        r"ADD\s+CONSTRAINT\s+" + _LANE_CHECK_CONSTRAINT + r"\s+CHECK\s*\(\s*lane\s+IN\s*\((.*?)\)\s*\)",
        last.read_text(encoding="utf-8"),
        re.S,
    )
    assert block is not None, f"could not parse the lane IN (...) allow-list from {last.name}"
    tokens = frozenset(re.findall(r"'([^']+)'", block.group(1)))
    assert tokens, f"lane CHECK allow-list parsed empty from {last.name}"
    return tokens


def test_effective_lane_set_is_nonempty() -> None:
    """Non-vacuity guard: the subset assertions below are meaningless if
    no stage resolves a lane."""
    assert _effective_bootstrap_lanes(), "_BOOTSTRAP_STAGE_SPECS resolved zero effective lanes"


def test_every_writable_lane_is_in_sources_lane_literal() -> None:
    """``Lane`` is the full vocabulary — it must contain every effective
    bootstrap-stage lane (else ``JobLock`` source resolution KeyErrors)."""
    missing = _effective_bootstrap_lanes() - set(get_args(SourcesLane))
    assert not missing, f"effective bootstrap-stage lanes missing from sources.py::Lane: {sorted(missing)}"


def test_every_writable_lane_is_in_lane_api_literal() -> None:
    missing = _effective_bootstrap_lanes() - set(get_args(LaneApi))
    assert not missing, f"effective bootstrap-stage lanes missing from bootstrap.py::LaneApi: {sorted(missing)}"


def test_every_writable_lane_is_in_bootstrap_state_lane_literal() -> None:
    missing = _effective_bootstrap_lanes() - set(get_args(StateLane))
    assert not missing, (
        f"effective bootstrap-stage lanes missing from bootstrap_state.py::Lane: {sorted(missing)} "
        "(this was the #1486 'openfigi' gap)"
    )


def test_every_writable_lane_is_in_sql_check_allow_list() -> None:
    missing = _effective_bootstrap_lanes() - _sql_lane_check_allow_list()
    assert not missing, (
        f"effective bootstrap-stage lanes missing from the bootstrap_stages_lane_check "
        f"CHECK allow-list: {sorted(missing)} — a persist would raise a runtime CHECK violation"
    )


def test_openfigi_is_an_effective_lane_and_covered_everywhere() -> None:
    """Regression pin for the #1486 gap: ``openfigi`` (S13 CUSIP sweep)
    is a real writable lane and must be present in all four definitions.
    Guards against a future edit silently dropping it from any one."""
    assert "openfigi" in _effective_bootstrap_lanes()
    assert "openfigi" in set(get_args(SourcesLane))
    assert "openfigi" in set(get_args(LaneApi))
    assert "openfigi" in set(get_args(StateLane))
    assert "openfigi" in _sql_lane_check_allow_list()
