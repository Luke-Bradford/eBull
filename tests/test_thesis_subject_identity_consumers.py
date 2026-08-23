"""The subject-identity guard census (#2306).

`theses.subject_identity_ok` is the verdict #2431/#2436 put on a thesis whose
memo never names its own instrument. Its value is that EVERY path which reads a
thesis as a decision input refuses a row the verdict rejects — and that property
is asserted in prose, in several docstrings, and (until #2306) in operator-facing
UI copy that enumerated the consumers by name.

Prose goes stale silently. This pins the census instead, so adding a consumer
that forgets the guard, or dropping one the copy still claims, fails a test
rather than leaving a sentence quietly wrong somewhere a reader trusts.

⚠ Pure-logic: reads source text, touches no DB, stays in the fast push tier.
"""

from __future__ import annotations

import re
from pathlib import Path

APP = Path(__file__).resolve().parent.parent / "app"

#: Modules that read the verdict directly rather than through the shared
#: helper, with WHY. Each is a place the rule is duplicated, so each is a place
#: it can drift — the note is the justification for accepting that.
MIRRORS: dict[str, str] = {
    "services/entry_timing.py": "mirrors the predicate in SQL on a joined row (see its comment at the read site)",
    "services/thesis.py": "the WRITER — computes the verdict, does not consume it",
    "services/thesis_subject_identity.py": "defines the rule and the helper",
    "api/theses.py": "projects the raw verdict to the client so the UI can render it (#2306)",
    "api/alerts.py": "compares the verdict on a thesis and its predecessor inline",
}

#: Modules expected to consume the verdict through ``is_thesis_usable``.
GUARDED = {
    "services/portfolio.py",
    "services/scoring.py",
    "services/reporting.py",
}


def _rel(path: Path) -> str:
    return path.relative_to(APP).as_posix()


def _modules_mentioning(token: str) -> set[str]:
    found = set()
    for path in APP.rglob("*.py"):
        if re.search(rf"\b{re.escape(token)}\b", path.read_text(encoding="utf-8")):
            found.add(_rel(path))
    return found


def test_every_reader_of_the_verdict_is_accounted_for() -> None:
    """No module touches `subject_identity_ok` without being on one list or the other.

    A new name here means someone added a path that reads the verdict. Decide
    which it is and say so: if it makes a DECISION, it belongs in ``GUARDED``
    and must call ``is_thesis_usable``; if it re-implements the check, add it to
    ``MIRRORS`` with the reason.
    """
    readers = _modules_mentioning("subject_identity_ok")
    unaccounted = readers - GUARDED - MIRRORS.keys()
    assert not unaccounted, (
        f"module(s) read theses.subject_identity_ok but are on neither list: {sorted(unaccounted)}. "
        "Add to GUARDED (and use is_thesis_usable) or to MIRRORS with a reason."
    )


def test_guarded_consumers_use_the_shared_helper() -> None:
    """A decision path must go through the helper, not re-derive `is True`.

    ⚠ The failure this prevents is specific: `subject_identity_ok is not False`
    LOOKS equivalent and is not — NULL means *nobody decided*, which the helper
    refuses and the naive form admits.
    """
    users = _modules_mentioning("is_thesis_usable")
    missing = GUARDED - users
    assert not missing, (
        f"declared guard consumer(s) no longer call is_thesis_usable: {sorted(missing)}. "
        "Either restore the call or move them to MIRRORS with a reason."
    )


def test_guard_consumers_have_not_silently_disappeared() -> None:
    """The census is non-empty and still contains the paths that hold money.

    `portfolio.py` turns `base_value` into an EXIT trigger on a real position —
    `sql/332` records 14 such exits that fired on wrong-company bands before the
    guard existed. If that module ever drops off this list, the regression is
    the one the whole verdict was built for.
    """
    assert "services/portfolio.py" in GUARDED
    assert GUARDED <= _modules_mentioning("subject_identity_ok") | _modules_mentioning("is_thesis_usable")
