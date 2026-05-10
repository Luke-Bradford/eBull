"""JSON-safe coercion for params dicts before ``Jsonb`` publish.

Issue #1064 PR2 — admin control hub Advanced disclosure renderer.

``validate_job_params`` returns native Python types: ``date`` for
``field_type='date'``, ``datetime`` for ``field_type='datetime'``,
``int``/``float``/``str`` for the scalar types, ``list``/``tuple``
for ``multi_enum``. ``psycopg.types.json.Jsonb`` then serialises with
the stdlib ``json.dumps`` which has no default coercer for ``date``
and raises ``TypeError`` at adapt time.

Two call sites need the coercion:

* ``app/api/jobs.py::run_job`` — ``pending_job_requests.payload`` write.
  Pre-PR2 this path latently crashed when an operator submitted a
  ``date``-typed param; no test exercised it because no FE rendered
  the field.
* ``app/jobs/runtime.py::_run_prelude`` — ``job_runs.params_snapshot``
  write. Already uses ``_jsonable_params`` from ``ops_monitor.py``
  (PR1c). PR2 lifts it here for shared ownership.

Listener re-validates after dequeue (``app/jobs/listener.py:157``),
so ISO-string-on-publish round-trips back to native ``date`` for the
invoker.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from typing import Any


def to_jsonsafe_params(params: Mapping[str, Any]) -> dict[str, Any]:
    """Coerce native Python types in a params dict to JSON-safe scalars.

    ``date`` / ``datetime`` → ISO-8601 string. ``list``/``tuple`` →
    list (preserves multi_enum semantics). All other values pass
    through unchanged — anything that survived ``validate_job_params``
    is already JSON-native (``str``, ``int``, ``float``, ``bool``,
    ``list[str]``).
    """
    result: dict[str, Any] = {}
    for key, value in params.items():
        if isinstance(value, datetime):
            result[key] = value.isoformat()
        elif isinstance(value, date):
            result[key] = value.isoformat()
        elif isinstance(value, (list, tuple)):
            result[key] = list(value)
        else:
            result[key] = value
    return result


__all__ = ["to_jsonsafe_params"]
