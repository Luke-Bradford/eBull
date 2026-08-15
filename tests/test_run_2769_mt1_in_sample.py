"""CLI safety boundary for the one MT-1 in-sample invocation (#2769)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import scripts.run_2769_mt1_in_sample as command


class _Connection:
    def __enter__(self) -> object:
        return object()

    def __exit__(self, *_args: object) -> None:
        return None


def test_wrong_execution_acknowledgement_refuses_before_policy_or_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        command,
        "assert_policy_version_merged",
        lambda: (_ for _ in ()).throw(AssertionError("policy guard must not run")),
    )
    monkeypatch.setattr(
        command,
        "assert_exact_clean_main_source",
        lambda: (_ for _ in ()).throw(AssertionError("source guard must not run")),
    )
    with pytest.raises(SystemExit, match="2"):
        command.main(["--execute", "wrong"])


def test_default_command_checks_authority_without_running_outcomes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    authority = SimpleNamespace(
        declaration_id=8,
        declaration_sha256="a" * 64,
        strategy_id="mt1",
        strategy_version="v1",
    )
    monkeypatch.setattr(command, "assert_exact_clean_main_source", lambda: {"source": "merged"})
    monkeypatch.setattr(command, "assert_policy_version_merged", lambda: {"policy": "merged"})
    monkeypatch.setattr(command.psycopg, "connect", lambda _url: _Connection())
    monkeypatch.setattr(command, "validate_mt1_preregistrations", lambda _conn: (authority, authority))
    monkeypatch.setattr(
        command,
        "run_and_store_mt1_in_sample_evaluation",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("outcomes must remain sealed")),
    )

    assert command.main([]) == 0

    assert "authority_ready_no_outcomes_evaluated" in capsys.readouterr().out


def test_execution_requires_literal_and_reports_structural_refusal(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(command, "assert_exact_clean_main_source", lambda: {"runner_source_head": "a" * 40})
    monkeypatch.setattr(command, "assert_policy_version_merged", lambda: {"policy": "merged"})
    monkeypatch.setattr(command.psycopg, "connect", lambda _url: _Connection())
    monkeypatch.setattr(
        command,
        "run_and_store_mt1_in_sample_evaluation",
        lambda *_args, **_kwargs: command.MT1StoredRefusal(42, "turnover refused"),
    )

    assert command.main(["--execute", command.ACKNOWLEDGEMENT]) == 2

    output = capsys.readouterr().out
    assert "structural_gate_refused_no_performance_stored" in output
    assert '"structural_attempt_id": 42' in output


def test_exact_source_guard_refuses_an_unmerged_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(command, "refresh_main_ref", lambda: True)
    outputs = {
        ("status", "--porcelain"): "",
        ("rev-parse", "HEAD"): "a" * 40 + "\n",
        ("rev-parse", "origin/main"): "b" * 40 + "\n",
    }
    monkeypatch.setattr(command, "_git_output", lambda *args: outputs[args])

    with pytest.raises(SystemExit, match="is not exact origin/main"):
        command.assert_exact_clean_main_source()


def test_exact_source_guard_accepts_only_clean_exact_main(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(command, "refresh_main_ref", lambda: True)
    outputs = {
        ("status", "--porcelain"): "",
        ("rev-parse", "HEAD"): "a" * 40 + "\n",
        ("rev-parse", "origin/main"): "a" * 40 + "\n",
    }
    monkeypatch.setattr(command, "_git_output", lambda *args: outputs[args])

    assert command.assert_exact_clean_main_source() == {
        "runner_source_clean": True,
        "runner_source_head": "a" * 40,
        "runner_source_matches_main": True,
    }


def test_source_refusal_happens_before_database_access(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        command,
        "assert_exact_clean_main_source",
        lambda: (_ for _ in ()).throw(SystemExit("unmerged runner")),
    )
    monkeypatch.setattr(
        command.psycopg,
        "connect",
        lambda _url: (_ for _ in ()).throw(AssertionError("database must remain unopened")),
    )
    with pytest.raises(SystemExit, match="unmerged runner"):
        command.main([])
