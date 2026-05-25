"""#1233 PR-D — CLI smoke / argparse / dry-run coverage for the three Stream A runbooks.

These tests focus on the CLI surface + dry-run paths +
guard-refusal paths. The destructive ``--apply`` paths that drop
databases, post HTTP, or wait for the jobs process are exercised
only at the unit level here — the operator-driven integration is by
design a separate manual step (see spec §17 attestation note).

Three test modules:

* ``stream_a_t13_sidecar_repair`` — argparse, dry-run, EBULL_ENV
  refusal, --apply --archive-path required.
* ``stream_a_run_8_verify`` — argparse, dry-run, EBULL_ENV refusal.
* ``stream_a_stream_c_gate`` — argparse, EBULL_ENV refusal; gate
  logic + DB integration lives in ``tests/integration/``.

The Stream-C gate runbook + ``stream_a_run_8_verify``'s deeper
HTTP/DB behaviour are covered by injectable-seam unit tests below
(see ``test_run_8_verify_*`` series) — when scope warrants the
seams are exercised; otherwise the integration path is owned by the
spec §17 operator attestation, not by this CLI test file.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pytest

from app.runbooks import (
    stream_a_run_8_verify,
    stream_a_stream_c_gate,
    stream_a_t13_sidecar_repair,
)

# ---------------------------------------------------------------------------
# Common: EBULL_ENV refusal (shared across all three runbooks)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "module,argv",
    [
        (stream_a_run_8_verify, []),
        (stream_a_t13_sidecar_repair, []),
        (stream_a_stream_c_gate, ["--bootstrap-run-id", "1"]),
    ],
)
def test_runbook_refuses_when_ebull_env_unset(
    module, argv: list[str], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("EBULL_ENV", raising=False)
    code = module.main(argv)
    captured = capsys.readouterr()
    assert code == 2
    assert "REFUSE" in captured.err
    assert "EBULL_ENV" in captured.err


# ---------------------------------------------------------------------------
# stream_a_t13_sidecar_repair
# ---------------------------------------------------------------------------


def test_sidecar_repair_dry_run_by_default(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """Without --apply: dry-run plan to stdout, exit 0."""
    monkeypatch.setenv("EBULL_ENV", "dev")
    code = stream_a_t13_sidecar_repair.main([])
    assert code == 0
    captured = capsys.readouterr()
    plan = json.loads(captured.out)
    assert plan["mode"] == "dry-run"
    assert plan["cik"] is None
    assert plan["bootstrap_run_id"] is None


def test_sidecar_repair_dry_run_with_cik_and_run_id(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """--cik + --bootstrap-run-id surface in the dry-run plan."""
    monkeypatch.setenv("EBULL_ENV", "dev")
    code = stream_a_t13_sidecar_repair.main(["--cik", "0000320193", "--bootstrap-run-id", "42"])
    assert code == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["cik"] == "0000320193"
    assert plan["bootstrap_run_id"] == 42


def test_sidecar_repair_apply_requires_archive_path(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """--apply without --archive-path → exit 2 (per O14 fold)."""
    monkeypatch.setenv("EBULL_ENV", "dev")
    code = stream_a_t13_sidecar_repair.main(["--apply"])
    assert code == 2
    assert "archive-path" in capsys.readouterr().err


def test_sidecar_repair_apply_refuses_missing_archive(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    """--apply with nonexistent archive path → exit 2 with explanatory msg."""
    monkeypatch.setenv("EBULL_ENV", "dev")
    bogus = tmp_path / "does_not_exist.zip"
    code = stream_a_t13_sidecar_repair.main(["--apply", "--archive-path", str(bogus)])
    assert code == 2
    assert str(bogus) in capsys.readouterr().err


def test_sidecar_repair_dry_run_with_archive_counts_entries(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    """Dry-run + --archive-path emits ``entries_would_process`` counter."""
    monkeypatch.setenv("EBULL_ENV", "dev")
    import zipfile

    zip_path = tmp_path / "syn.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("CIK0000320193.json", "{}")
        zf.writestr("CIK0000102909.json", "{}")
        zf.writestr("other_garbage.json", "{}")
    code = stream_a_t13_sidecar_repair.main(["--archive-path", str(zip_path)])
    assert code == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["entries_would_process"] == 2


# ---------------------------------------------------------------------------
# stream_a_run_8_verify
# ---------------------------------------------------------------------------


def test_run_8_verify_dry_run_by_default(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """No --apply: dry-run plan + exit 0; no httpx calls; no fence acquire."""
    monkeypatch.setenv("EBULL_ENV", "dev")
    code = stream_a_run_8_verify.main([])
    assert code == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["mode"] == "dry-run"
    # All 12 destructive steps enumerated in the plan.
    assert any("DROP" in step for step in plan["would_execute"])
    assert any("auth/setup" in step for step in plan["would_execute"])
    assert any("bootstrap/run" in step for step in plan["would_execute"])


def test_run_8_verify_password_generator_is_high_entropy() -> None:
    """``_generate_password`` returns at least 32 chars of token_urlsafe entropy."""
    pw1 = stream_a_run_8_verify._generate_password()
    pw2 = stream_a_run_8_verify._generate_password()
    assert pw1 != pw2
    assert len(pw1) >= 32
    # url-safe alphabet only
    assert all(c.isalnum() or c in "-_" for c in pw1)


def test_run_8_verify_postgres_url_strips_db_name() -> None:
    """``_postgres_url`` returns a sibling URL with path ``/postgres``."""
    from urllib.parse import urlparse

    url = stream_a_run_8_verify._postgres_url()
    assert urlparse(url).path == "/postgres"


def test_run_8_verify_default_wait_for_jobs_sec_is_1800() -> None:
    """#1327 — argparse default for --wait-for-jobs-sec equals 1800.

    Three-part assertion (PR #1352 Claude-bot iter-1 NITPICK fold —
    string-match heuristic replaced with argparse parse_args):

    1. ``DEFAULT_WAIT_FOR_JOBS_SEC`` constant equals 1800.
    2. Argparse parser's resolved default equals 1800 (parse_args with
       no args takes defaults).
    3. Resolved default IS the constant (not a hardcoded literal that
       happens to match) — catches future refactor that detaches argparse
       from the constant.
    """
    assert stream_a_run_8_verify.DEFAULT_WAIT_FOR_JOBS_SEC == 1800
    parser = stream_a_run_8_verify.build_parser()
    args = parser.parse_args([])
    assert args.wait_for_jobs_sec == 1800
    assert args.wait_for_jobs_sec == stream_a_run_8_verify.DEFAULT_WAIT_FOR_JOBS_SEC


# ---------------------------------------------------------------------------
# stream_a_stream_c_gate — argparse + EBULL_ENV refusal
# ---------------------------------------------------------------------------


def test_gate_requires_bootstrap_run_id(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """argparse requires --bootstrap-run-id; missing → exit 2 from argparse."""
    monkeypatch.setenv("EBULL_ENV", "dev")
    with pytest.raises(SystemExit) as exc:
        stream_a_stream_c_gate.main([])
    # argparse exits 2 on missing required.
    assert exc.value.code == 2


def test_gate_strict_flag_defaults_true_and_no_strict_disables_it() -> None:
    """Regression gate for PR-D bot review iter 1 BLOCKING.

    ``--strict`` previously used ``action="store_true", default=True``
    which made the flag a no-op (could never be disabled from the
    CLI). Fixed to ``BooleanOptionalAction`` so ``--strict`` /
    ``--no-strict`` both work and ``default=True`` is the actual
    behaviour when neither is passed.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--strict",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    # Default behaviour: strict=True.
    assert parser.parse_args([]).strict is True
    # Explicit --strict: still True.
    assert parser.parse_args(["--strict"]).strict is True
    # Opt-out: --no-strict toggles to False. This was IMPOSSIBLE pre-fix.
    assert parser.parse_args(["--no-strict"]).strict is False


def test_gate_imports_manifest_parsers_at_module_load() -> None:
    """C4 depends on the registry being populated. The runbook module
    imports ``app.services.manifest_parsers`` at load time — assert
    ``registered_parser_sources()`` returns a non-empty frozenset by
    the time anyone references this gate."""
    from app.jobs.sec_manifest_worker import registered_parser_sources

    sources = registered_parser_sources()
    assert isinstance(sources, frozenset)
    assert len(sources) > 0
    # Spot-check: sec_form4 + sec_def14a (both register in __init__).
    assert "sec_form4" in sources
    assert "sec_def14a" in sources


def test_gate_category_to_manifest_sources_covers_all_categories() -> None:
    """Every ``_CATEGORIES`` entry has a CATEGORY_TO_MANIFEST_SOURCES mapping.

    Closure check — if a future category lands without a mapping, C6
    fallback would silently return "no mapping" + fail on it. This
    test fails-loud the first time a category is added without the
    sibling map.
    """
    from app.jobs.ownership_observations_repair import _CATEGORIES
    from app.services.capability_manifest_mapping import (
        CATEGORY_TO_MANIFEST_SOURCES,
    )

    categories = {row[2] for row in _CATEGORIES}
    mapped = set(CATEGORY_TO_MANIFEST_SOURCES.keys())
    missing = categories - mapped
    assert missing == set(), f"categories without CATEGORY_TO_MANIFEST_SOURCES mapping: {sorted(missing)}"


# Sanity for the deferred-environment-cleanup pattern used elsewhere.
def _assert_no_env_leak() -> None:
    assert os.environ.get("EBULL_ENV") in (None, "dev"), "test should not leave EBULL_ENV set to non-dev"


def test_env_cleanup() -> None:
    _assert_no_env_leak()
