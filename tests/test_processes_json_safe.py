"""Unit tests for ``app.services.processes.json_safe``.

Issue #1064 PR2 — pins the JSON-safe coercion that ``app/api/jobs.py``
calls before publishing the queue payload. The same helper is used by
``app/jobs/runtime.py`` for ``params_snapshot`` writes; the back-compat
re-export in ``ops_monitor`` keeps legacy callers wired.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from app.services.processes.json_safe import to_jsonsafe_params


def test_date_coerces_to_iso_string() -> None:
    assert to_jsonsafe_params({"d": date(2024, 1, 1)}) == {"d": "2024-01-01"}


def test_datetime_coerces_to_iso_string() -> None:
    dt = datetime(2024, 1, 1, 12, 30, 45, tzinfo=UTC)
    out = to_jsonsafe_params({"ts": dt})
    assert out["ts"] == dt.isoformat()


def test_list_and_tuple_normalise_to_list() -> None:
    out = to_jsonsafe_params({"l": [1, 2], "t": (3, 4)})
    assert out["l"] == [1, 2]
    assert out["t"] == [3, 4]


def test_scalars_pass_through_unchanged() -> None:
    out = to_jsonsafe_params({"s": "x", "i": 7, "f": 1.5, "b": True, "n": None})
    assert out == {"s": "x", "i": 7, "f": 1.5, "b": True, "n": None}


def test_back_compat_re_export_in_ops_monitor() -> None:
    """``ops_monitor._jsonable_params`` is the legacy alias.

    PR1c landed it inline; PR2 lifted the canonical helper to
    ``app.services.processes.json_safe`` and the ops_monitor name is
    now a thin re-export. Existing internal callers (runtime.py
    fallback path, ops_monitor's two write sites) keep working
    without a global rewire.
    """
    from app.services.ops_monitor import _jsonable_params

    assert _jsonable_params is to_jsonsafe_params
    assert _jsonable_params({"d": date(2024, 1, 1)}) == {"d": "2024-01-01"}


def test_jsonb_roundtrip_does_not_raise_on_date() -> None:
    """psycopg.Jsonb adapts at SQL-execution time; verify the helper's
    output passes through ``json.dumps`` cleanly so the adapt step
    never crashes. Pre-PR2 a raw ``date`` in the queue payload would
    raise ``TypeError`` here.
    """
    import json

    safe = to_jsonsafe_params({"d": date(2024, 1, 1)})
    # No exception raised — round-trip the same JSON psycopg would emit.
    assert json.dumps(safe) == '{"d": "2024-01-01"}'
