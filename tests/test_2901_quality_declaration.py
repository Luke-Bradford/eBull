"""#2901's register entry and preregistration declaration.

Declaration: ``docs/proposals/ta/2026-09-25-2901-quality-declaration.md``.

Pure tier, no DB. Everything here is a property of the row the freeze script WOULD write, checked before it
writes it: ``sql/333`` bars UPDATE and DELETE, so a wrong row costs a new strategy version.
"""

from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path

import pytest

from app.services.prereg_contract import declaration_refusals
from app.services.r6_monthly_trial import family_size
from app.services.trial_register import TRIAL_REGISTER, TrialExactness
from scripts import freeze_2901_quality_declaration as freeze
from scripts import run_2901_quality_trial as runner
from scripts.freeze_2901_quality_declaration import (
    EXPECTED_DECLARATION_SHA256,
    MIN_FORWARD_CALENDAR_WEEKS,
    MIN_FORWARD_DECISION_DATES,
    build_declaration,
)

DECLARATION = Path("docs/proposals/ta/2026-09-25-2901-quality-declaration.md")
TRIAL_ID = "r6-2901-quality-gpa-2026-09-25"


def _trial(trial_id: str):
    return next(trial for trial in TRIAL_REGISTER.trials if trial.trial_id == trial_id)


class TestTheRegisterEntries:
    def test_the_trial_claims_the_identity_the_runner_writes_its_holdout_row_under(self) -> None:
        """``freeze_preregistration`` refuses a declaration no trial claims, and #2599's gate keys on this pair."""
        trial = _trial(TRIAL_ID)
        assert trial.declared_for == (runner.STRATEGY_ID, runner.STRATEGY_VERSION)
        declaration = build_declaration()
        assert (declaration.strategy_id, declaration.strategy_version) == trial.declared_for

    def test_the_quality_trial_counts_its_two_comparator_rows(self) -> None:
        trial = _trial(TRIAL_ID)
        assert trial.exactness is TrialExactness.EXACT
        assert trial.searches == 2
        assert DECLARATION.name in trial.evidence

    def test_2908_is_charged_its_nine_exposed_rows_without_a_declaration(self) -> None:
        """The register's reconstruction policy counts an exposed recompute, so it agrees with H."""
        trial = _trial("r6-2908-exclusion-arms-2026-08-24")
        assert trial.searches == 9
        assert trial.declared_for is None
        assert "640, 641" in trial.evidence


class TestTheDeclaration:
    def test_it_would_pass_the_freeze_time_gate(self) -> None:
        assert declaration_refusals(build_declaration()) == ()

    def test_it_is_a_capital_candidate_with_no_structural_refusal(self) -> None:
        declaration = build_declaration()
        assert declaration.prereg_purpose == "capital_candidate"
        assert declaration.declared_universe_basis == "survivorship_free"
        assert declaration.expected_structural_refusals == ()

    def test_the_derivation_fits_the_sql_333_column(self) -> None:
        assert len(build_declaration().forward_shadow.derivation) <= 1000

    def test_the_floor_is_the_frozen_schedule_s_complete_cohorts(self) -> None:
        """11 complete cohorts, X(2013) to X(2024), each date asserted by the runner's census stage."""
        assert MIN_FORWARD_DECISION_DATES == 11
        assert MIN_FORWARD_CALENDAR_WEEKS == -(-(date(2024, 7, 1) - date(2013, 7, 1)).days // 7)

    def test_the_digest_is_stable_across_builds(self) -> None:
        assert build_declaration().sha256 == build_declaration().sha256


class TestTheDocument:
    def test_it_records_the_digest_the_script_will_freeze(self) -> None:
        """The script refuses any other digest, so the document, the constant and the build must agree."""
        text = DECLARATION.read_text(encoding="utf-8")
        assert build_declaration().sha256 == EXPECTED_DECLARATION_SHA256
        assert EXPECTED_DECLARATION_SHA256 in text

    def test_its_family_matches_the_runner(self) -> None:
        """|H| = 9 is passed as ``--history-rows``; M must be what the document states."""
        text = DECLARATION.read_text(encoding="utf-8")
        assert "--history-rows 9" in text
        assert family_size(9) == 17
        assert "**17**" in text
        assert "**|H| = 9.**" in text


class TestTheFreezeEntryPoint:
    def test_it_refuses_a_digest_the_document_does_not_publish_before_touching_the_db(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(freeze, "EXPECTED_DECLARATION_SHA256", "0" * 64)

        def _no_db(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("the digest check must run before any connection")

        monkeypatch.setattr(freeze.psycopg, "connect", _no_db)
        monkeypatch.setattr(freeze, "assert_policy_version_merged", _no_db)
        assert freeze.main([]) == 1
        assert "digest_not_published" in capsys.readouterr().err

    def test_it_has_no_policy_divergence_override(self) -> None:
        with pytest.raises(SystemExit):
            freeze.main(["--allow-policy-divergence", "--dry-run"])

    def test_the_dry_run_reports_whether_the_digest_is_published(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(freeze, "policy_version_report", lambda: {})
        assert freeze.main(["--dry-run"]) == 0
        assert '"digest_matches_published": true' in capsys.readouterr().out

    def test_its_implementation_pins_are_the_files_in_this_tree(self) -> None:
        """A review fix to any pinned file must move the document's pin with it."""
        text = DECLARATION.read_text(encoding="utf-8")
        for path in (
            "scripts/run_2901_quality_trial.py",
            "app/services/r6_monthly_trial.py",
            "app/services/r6_exclusion_trial.py",
            "scripts/freeze_2901_quality_declaration.py",
            "scripts/measure_2901_offcalendar.py",
            "scripts/measure_2901_power.py",
        ):
            assert hashlib.sha256(Path(path).read_bytes()).hexdigest() in text, path
