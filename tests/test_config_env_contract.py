"""Derived contract between ``Settings``, ``.env.example`` and the alias rule.

⚠ THE CHECK READS THE ALIAS SET, NOT THE BARE FIELD NAME. That is the whole
point and it is not a detail: a check keyed on bare field names would have
passed happily throughout the #2286 defect, because `.env.example` documented
``EBULL_SERVICE_TOKEN`` while the field read only ``SERVICE_TOKEN``. Both
"documented" and "a field exists" were true; the one thing that mattered — that
the documented spelling is one the field actually accepts — was checked nowhere.

Same shape as the closed-vocabulary contract test from #2229, and the same
recurring lesson: a three-place invariant with no derived check always drifts.
Here the three places are `.env.example`, ``Settings``, and the alias rule.

Field names are read by AST rather than by importing ``Settings``, so the test
does not depend on a working environment and cannot be fooled by whatever
happens to be set when it runs.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
from pydantic import AliasChoices

from app.config import Settings, _ebull_alias

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PY = REPO_ROOT / "app" / "config.py"
ENV_EXAMPLE = REPO_ROOT / ".env.example"

# Fields deliberately absent from `.env.example`, each with the reason. An
# entry here is a DECISION, not a snooze: adding one is how you say "an operator
# should never set this", and the reason is what a reviewer checks.
#
# ⚠ Empty on purpose right now. #2286 found that every previously-undocumented
# field turned out to be one an operator legitimately might set — including two
# credentials and a cookie-security flag. If you are about to add a name here,
# the bar is "setting this cannot help anyone", not "documenting it is tedious".
DELIBERATELY_UNDOCUMENTED: dict[str, str] = {}


def _settings_field_names() -> list[str]:
    """Annotated field names on ``Settings``, by AST."""
    tree = ast.parse(CONFIG_PY.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "Settings":
            return [
                stmt.target.id
                for stmt in node.body
                if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name)
            ]
    raise AssertionError("class Settings not found in app/config.py")


def _env_example_keys() -> set[str]:
    """Assignment keys in `.env.example`, commented-out ones included.

    A commented `# FOO=` line still documents the variable — it tells the
    operator it exists and is optional — so it counts. What does NOT count is
    prose merely mentioning the name, which is why this matches an assignment.

    ⚠ UPPERCASE-only, deliberately. Every env var here is UPPER_SNAKE, and the
    looser `[A-Za-z_]` form would match ordinary prose inside a comment (`# set
    foo=bar to ...`). That failure is PERMISSIVE — a false match makes a field
    look documented when it is not — so the check would fail in the direction
    that reports success. Measured on the current file the two forms return an
    identical 38 keys, so this narrows the future, not the present.
    """
    pattern = re.compile(r"^\s*(?:#\s*)?([A-Z][A-Z0-9_]*)\s*=", re.MULTILINE)
    return set(pattern.findall(ENV_EXAMPLE.read_text()))


def _accepted_names(field: str) -> set[str]:
    """Every env-var spelling that actually reaches ``field``."""
    info = Settings.model_fields[field]
    alias = info.validation_alias
    if alias is None:
        return {field.upper()}
    if isinstance(alias, AliasChoices):
        return {str(c) for c in alias.choices}
    return {str(alias)}


def test_every_settings_field_is_documented_under_a_name_it_accepts() -> None:
    documented = _env_example_keys()
    missing = [
        field
        for field in _settings_field_names()
        if field not in DELIBERATELY_UNDOCUMENTED and not (_accepted_names(field) & documented)
    ]
    assert not missing, (
        "Settings fields with no .env.example entry under any name they accept: "
        f"{sorted(missing)}. .env.example is the only onboarding contract a fresh "
        "clone has (README: `cp .env.example .env`), so an absent field is one the "
        "operator can only discover from a runtime failure. Add an entry, or add "
        "the field to DELIBERATELY_UNDOCUMENTED with a reason."
    )


def test_env_example_documents_no_variable_the_app_cannot_read() -> None:
    """The #2286 defect, stated as a test.

    Six `EBULL_*` variables were documented, set in the working `.env`, and read
    by nothing — including two credentials and the session-cookie security flag.
    Nothing failed, because a variable the app ignores produces silence.
    """
    accepted: set[str] = set()
    for field in _settings_field_names():
        accepted |= _accepted_names(field)

    # Names `.env.example` documents that no Settings field accepts. Some are
    # legitimately not Settings fields at all (docker-compose reads POSTGRES_*,
    # the runtime reads EBULL_ENV via os.environ), so only EBULL_-prefixed names
    # whose bare form IS a Settings field can be judged here — those are exactly
    # the ones that look like they configure a setting and do not.
    fields_upper = {f.upper() for f in _settings_field_names()}
    unreadable = sorted(
        key
        for key in _env_example_keys()
        if key.startswith("EBULL_") and key.removeprefix("EBULL_") in fields_upper and key not in accepted
    )
    assert not unreadable, (
        f".env.example documents EBULL_-prefixed variables the app does not read: {unreadable}. "
        "Each names a real Settings field, so an operator setting it gets silence rather than "
        "an error. This is what #2286 found live in the working .env."
    )


@pytest.mark.parametrize("field", ["service_token", "bootstrap_token", "session_cookie_secure", "secrets_key"])
def test_security_relevant_fields_accept_the_documented_ebull_spelling(field: str) -> None:
    """Named individually because these are the ones whose silence costs something.

    ``secrets_key`` is included even though #1406 already fixed it: it is the
    regression that proves the generator subsumes the per-field alias, so the
    explicit one could be removed without re-breaking it.
    """
    assert f"EBULL_{field.upper()}" in _accepted_names(field)
    assert field.upper() in _accepted_names(field), "bare name must stay working for back-compat"


def test_alias_helper_produces_both_spellings() -> None:
    assert {str(c) for c in _ebull_alias("some_field").choices} == {"EBULL_SOME_FIELD", "SOME_FIELD"}


def test_field_name_kwargs_still_construct_settings() -> None:
    """`alias_generator` without `populate_by_name` drops kwargs SILENTLY.

    A `validation_alias` replaces the field name as an input key, and with
    `extra="ignore"` the discarded kwarg raises nothing — the value just falls
    back to `.env` or the default. That is the very defect this module's alias
    work exists to fix, reintroduced one layer up, so it gets a test rather
    than a comment.
    """
    s = Settings(database_url="postgresql://sentinel/x", app_env="sentinel_env")
    assert s.database_url == "postgresql://sentinel/x"
    assert s.app_env == "sentinel_env"
