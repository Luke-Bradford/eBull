"""Static guard: tests must not connect to ``settings.database_url``
to perform destructive writes.

Why this exists
---------------
On 2026-04-08 the user discovered that every pytest run was wiping
their dev database -- ``tests/test_operator_setup_race.py`` ran a
TRUNCATE against ``settings.database_url`` (i.e. ``ebull``, the dev
DB) and the FK CASCADE took out their saved broker credentials too.
The fix on PR #129 isolated that one test to ``ebull_test``, but
without a structural guard a future test author can re-introduce
the same bug just by typing ``psycopg.connect(settings.database_url)``
inside a fixture and adding a TRUNCATE next to it.

This test is the structural guard. It walks ``tests/`` once and
fails if any test file references ``settings.database_url``
*except* on an explicit allowlist. The allowlist is intentionally
short and every entry is justified inline -- read-only smoke
probes that drive the FastAPI lifespan are allowed; anything that
mutates persistent state is not.

If you are a future test author hitting this guard:
  * Do NOT add yourself to ``_ALLOWED`` to make the test pass.
  * Use the isolated ``ebull_test`` pattern from
    ``tests/test_operator_setup_race.py`` instead -- copy
    ``_swap_database`` / ``_ensure_test_db_exists`` /
    ``_apply_migrations_to_test_db`` / ``_assert_test_db``.
  * The PREVENTION note on PR #129 round 1 explicitly asked for
    this guard. Removing or weakening it requires a written
    rebuttal in a follow-up PR.
"""

from __future__ import annotations

from pathlib import Path

# Files allowed to reference ``settings.database_url``. Each entry
# must be justified -- read-only paths only. Any future addition
# requires a comment explaining why a destructive path is not in
# play.
_ALLOWED: dict[str, str] = {
    # Read-only: drives the FastAPI lifespan and probes /health.
    # The lifespan opens a connection pool against settings.database_url,
    # but never TRUNCATEs / DELETEs / DROPs anything. The smoke test
    # itself does not write.
    "smoke/test_app_boots.py": "read-only lifespan probe",
    # Read-only at the helper-function level: the destructive
    # fixture in this file uses ``_test_database_url()``, which is
    # *derived* from ``settings.database_url`` by swapping the
    # database name. The literal token still appears in the source
    # because the helpers reference it; the guard below greps for
    # the token, so we allowlist this file with a justification.
    "test_operator_setup_race.py": "derives ebull_test URL from settings via _swap_database",
    # This file (the guard itself) references the token in its own
    # source for the grep -- exclude it to avoid a self-match.
    "smoke/test_no_settings_url_in_destructive_paths.py": "the guard itself",
}

_TESTS_DIR = Path(__file__).resolve().parents[1]
_TOKEN = "settings.database_url"


def test_no_test_writes_to_dev_database_url() -> None:
    """Fail if any test file references ``settings.database_url``
    outside the explicit allowlist.

    The check is a grep, not an AST walk, on purpose: a string-level
    match catches the token regardless of how it is spelled
    (attribute access, f-string, comment, alias) and there is no
    legitimate non-allowlisted reason for the literal token to
    appear in a test file.
    """
    offenders: list[str] = []
    for path in sorted(_TESTS_DIR.rglob("*.py")):
        rel = path.relative_to(_TESTS_DIR).as_posix()
        if rel in _ALLOWED:
            continue
        text = path.read_text(encoding="utf-8")
        if _TOKEN in text:
            offenders.append(rel)

    assert not offenders, (
        f"The following test files reference {_TOKEN!r} but are not on "
        f"the allowlist: {offenders}. Destructive tests must connect to "
        f"the isolated ebull_test database, not the dev DB. See "
        f"tests/test_operator_setup_race.py for the pattern, and the "
        f"docstring of this file for guidance."
    )
