"""#2611 — a refused outcome-access attempt leaves a trace. Pure-logic half.

Spec: ``docs/proposals/ta/2026-08-13-refused-outcome-access-audit.md``.
Storage: ``sql/340``. Writer: ``app/services/result_ledger.py``.

The properties that need a real relation — that the row survives the caller's
rollback, and that it is invisible to criterion 5 and to #2634's exposure check
— live in ``tests/test_2611_refused_access_audit_db.py``. What is here needs no
Postgres: the never-masks-the-refusal contract, and the AST guard that keeps the
refusal exit a chokepoint.
"""

from __future__ import annotations

import ast
import logging
from pathlib import Path

import pytest
from psycopg.conninfo import conninfo_to_dict

from app.services import result_ledger
from app.services.result_ledger import HoldoutAccess, PreregDeclarationRefused

_MODULE_PATH = Path(result_ledger.__file__)

#: Every function that can refuse an OUTCOME-ACCESS attempt. ⚠ Not every
#: function that raises ``PreregDeclarationRefused``, and each exclusion is a
#: decision rather than an oversight:
#:
#: - ``freeze_preregistration`` / ``supersede_preregistration`` refuse an attempt
#:   to WRITE A DECLARATION, which opens nothing;
#: - ``_refuse_declared_stamp_substitution`` refuses a result WRITE;
#: - ``verify_outcome_access_provenance`` is #2614's RE-CHECK. It requires an
#:   ``access_id`` whose committed row already accounts for the attempt, so
#:   auditing it would file a second refusal for one look.
#:
#: ⚠ ``_declaration_refusal_codes`` is absent because it RETURNS codes and never
#: raises — which is precisely what lets the audit door and the re-check door
#: share the rule while refusing differently.
_ACCESS_PATH_FUNCTIONS = frozenset(
    {
        "_refuse_incoherent_declaration",
        "record_holdout_access",
        "require_outcome_access",
    }
)


def _access(**overrides: object) -> HoldoutAccess:
    base: dict[str, object] = {
        "strategy_id": "S-2611",
        "strategy_version": "refusal-audit-v1",
        "access_kind": "read",
        "accessed_by": "tests/test_2611_refused_access_audit.py",
        "purpose": "pin the refusal-audit contract",
    }
    base.update(overrides)
    return HoldoutAccess(**base)  # type: ignore[arg-type]


class _ExplodingConnection:
    """A connection whose ``info`` cannot be read, so the audit write cannot start.

    ⚠ FAILS AT THE FIRST THING ``_record_access_refusal`` TOUCHES. Patching
    ``psycopg.connect`` would test one failure mode; this tests that the whole
    audit block is inside the guard, which is the property that matters — a
    caller must never learn about an audit problem instead of about its refusal.
    """

    @property
    def info(self) -> object:
        raise RuntimeError("audit connection unavailable")


class TestTheAuditNeverMasksTheRefusal:
    def test_a_failed_audit_write_still_raises_the_refusal(self) -> None:
        with pytest.raises(PreregDeclarationRefused) as excinfo:
            result_ledger._refuse_access(
                _ExplodingConnection(),  # type: ignore[arg-type]
                _access(),
                ("preregistration_not_frozen",),
            )
        assert excinfo.value.refusals == ("preregistration_not_frozen",)

    def test_a_failed_audit_write_logs_the_refusal_codes(self, caplog: pytest.LogCaptureFixture) -> None:
        """⚠ THE CODES GO IN THE LOG LINE, not just "audit write failed".

        A failed audit that logged only its own failure would be the shape this
        repo has been bitten by: a path that writes nothing and reports it in a
        way no reader can act on. The codes are the only content the audit row
        would have carried.
        """
        with caplog.at_level(logging.ERROR, logger=result_ledger.__name__):
            with pytest.raises(PreregDeclarationRefused):
                result_ledger._refuse_access(
                    _ExplodingConnection(),  # type: ignore[arg-type]
                    _access(),
                    ("structural_refusal_policy_superseded", "declaration_digest_mismatch"),
                )
        logged = caplog.text
        assert "structural_refusal_policy_superseded" in logged
        assert "declaration_digest_mismatch" in logged
        assert "S-2611" in logged
        assert "refusal-audit-v1" in logged


class TestTheRefusalExitIsAChokepoint:
    """⚠⚠ THE M9 SHAPE, PRE-EMPTED. Nine times this milestone the defect has been
    *"the control exists, is tested, and sits on a path the decision does not
    take"*. A fourth outcome-access refusal added later that raised directly
    would be audit-silent and no existing test would fail — so the guard is on
    the module's AST, not on behaviour.

    ⚠ AST AND NOT A SUBSTRING GREP: a grep for ``PreregDeclarationRefused`` in
    this module is satisfied by the class definition and by the import line in
    any file that names it (#2631's precedent).
    """

    @staticmethod
    def _direct_raises(function_names: frozenset[str]) -> dict[str, int]:
        tree = ast.parse(_MODULE_PATH.read_text())
        found: dict[str, int] = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef) or node.name not in function_names:
                continue
            for inner in ast.walk(node):
                if not isinstance(inner, ast.Raise) or inner.exc is None:
                    continue
                raised = inner.exc.func if isinstance(inner.exc, ast.Call) else inner.exc
                if isinstance(raised, ast.Name) and raised.id == "PreregDeclarationRefused":
                    found[node.name] = inner.lineno
        return found

    def test_no_access_path_function_raises_the_refusal_directly(self) -> None:
        offenders = self._direct_raises(_ACCESS_PATH_FUNCTIONS)
        assert offenders == {}, (
            f"{offenders} raise PreregDeclarationRefused directly instead of through _refuse_access, so the "
            "attempt is refused with no sql/340 audit row (#2611)"
        )

    def test_the_guard_can_see_a_direct_raise(self) -> None:
        """⚠ THE PROBE FOR THE GUARD ITSELF. A test that only ever asserts "no
        offenders" passes just as happily when the AST walk matches nothing at
        all — which is the failure mode a revert-probe exists to catch, and the
        one this repo has shipped before. ``freeze_preregistration`` genuinely
        does raise directly (it is out of scope, deliberately), so it is a live
        positive control rather than a synthetic one.
        """
        assert self._direct_raises(frozenset({"freeze_preregistration"})) != {}

    def test_every_named_function_exists(self) -> None:
        """⚠ A name that no longer exists silently drops out of the guard above."""
        tree = ast.parse(_MODULE_PATH.read_text())
        defined = {node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}
        assert _ACCESS_PATH_FUNCTIONS <= defined


class TestTheAuditConninfoComesFromTheCaller:
    """⚠⚠ NOT FROM THE PROCESS SETTINGS' DATABASE URL. A settings-derived DSN
    would write a DB test's refusal into the operator's dev database — the wrong
    database, and one ``tests/conftest.py`` has a tripwire for.

    ⚠⚠ THE SETTINGS ATTRIBUTE IS DELIBERATELY NOT SPELLED OUT ANYWHERE IN THIS
    FILE, and that is not squeamishness. ``tests/conftest.py``'s
    ``_DB_SOURCE_MARKERS`` auto-applies the ``db`` marker on a per-MODULE
    SUBSTRING scan of the source, so writing the attribute name even inside a
    docstring moves this whole pure-logic file OUT of the fast pre-push tier.
    Measured on the first draft of this file: ``pytest -m "not db"`` collected
    nothing and exited 5. Detect that by the exit code, never by reading.
    """

    @staticmethod
    def _audit_conninfo_body() -> ast.FunctionDef:
        tree = ast.parse(_MODULE_PATH.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_audit_conninfo":
                return node
        raise AssertionError("_audit_conninfo is gone — #2611's audit connection has no derivation to check")

    def test_the_dsn_is_derived_from_the_connection_not_from_settings(self) -> None:
        """⚠ AST, NOT A SUBSTRING. The first draft of this test read the
        function's SOURCE TEXT and asserted ``"settings" not in it`` — and failed
        on ``_audit_conninfo``'s own docstring, which names the settings URL to
        say why it is the wrong source. That is the #2631 shape exactly: a
        bare-substring convention check is answered by prose and by import lines
        rather than by code. Names referenced, not characters present.
        """
        fn = self._audit_conninfo_body()
        names = {node.id for node in ast.walk(fn) if isinstance(node, ast.Name)}
        assert "settings" not in names, "the audit DSN must come from the caller's connection, never from settings"
        attributes = {
            node.attr
            for node in ast.walk(fn)
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "conn"
        }
        assert "info" in attributes

    def test_a_missing_password_is_omitted_rather_than_sent_as_none(self) -> None:
        """A trust / passfile / service-file deployment has no password on the
        connection, and ``make_conninfo(password=None)`` is not the same thing as
        not naming it.
        """

        class _NoPassword:
            dsn = "dbname=ebull host=localhost user=postgres"
            password = None

        class _Conn:
            info = _NoPassword()

        rendered = result_ledger._audit_conninfo(_Conn())  # type: ignore[arg-type]
        assert "password" not in rendered
        assert "dbname=ebull" in rendered
        assert "lock_timeout" in rendered

    def test_a_present_password_is_reattached(self) -> None:
        """``conn.info.dsn`` strips it (measured, psycopg 3.3.3), so the audit
        connection would fail to authenticate without this.
        """

        class _WithPassword:
            dsn = "dbname=ebull host=localhost user=postgres"
            password = "secret-value"

        class _Conn:
            info = _WithPassword()

        assert "password=secret-value" in result_ledger._audit_conninfo(_Conn())  # type: ignore[arg-type]

    def test_the_callers_own_options_survive_and_the_audit_timeouts_still_win(self) -> None:
        """``make_conninfo`` merges per KEYWORD, not inside a keyword's value, so
        naming ``options=`` replaces the caller's whole string rather than adding
        to it — measured on psycopg 3.3.3, which is how a caller's
        ``-c application_name=…`` was silently dropped on the audit connection.

        ⚠ ORDER IS THE ASSERTION, not merely presence. libpq forwards ``options``
        as server command-line arguments and a repeated ``-c`` is LAST-WINS
        (measured against the dev cluster: caller ``lock_timeout=99s`` then ours
        ``2s`` → ``SHOW lock_timeout`` = ``2s``). Appending therefore keeps both
        the caller's settings and this write's timeout guard; prepending would
        hand a caller the ability to disable the guard, so a test that only
        checked both substrings were present would pass on the broken order.
        """

        class _WithOptions:
            dsn = "dbname=ebull host=localhost user=postgres options='-c application_name=caller -c lock_timeout=99s'"
            password = None

        class _Conn:
            info = _WithOptions()

        rendered = result_ledger._audit_conninfo(_Conn())  # type: ignore[arg-type]
        options = conninfo_to_dict(rendered)["options"]
        assert isinstance(options, str)
        assert "-c application_name=caller" in options
        assert options.index("-c lock_timeout=99s") < options.index("-c lock_timeout=2s")
        assert options.endswith("-c lock_timeout=2s -c statement_timeout=5s")

    def test_a_caller_with_no_options_gets_only_the_audit_timeouts(self) -> None:
        """The common case: nothing to merge, and no stray leading whitespace."""

        class _NoOptions:
            dsn = "dbname=ebull host=localhost user=postgres"
            password = None

        class _Conn:
            info = _NoOptions()

        rendered = result_ledger._audit_conninfo(_Conn())  # type: ignore[arg-type]
        assert conninfo_to_dict(rendered)["options"] == "-c lock_timeout=2s -c statement_timeout=5s"
