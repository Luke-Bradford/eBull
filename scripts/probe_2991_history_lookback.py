"""Observe what eToro does with a trade-history lookback beyond the documented maximum.

Refs #2991. Informational demo reads only — never places, modifies or closes an order.

THE QUESTION
------------
``getTradingInfoTradeDemoHistory`` documents, on the OPERATION (not on the ``minDate``
parameter): *"Keep each request's lookback to less than 1 year (maximum 1 year minus 1
day). For longer history, split the range into successive windows of at most that length
(advance ``minDate`` per batch)"*. ``compute_history_min_date`` returns ``HISTORY_EPOCH``
(2017-01-01) for an empty ledger, so the deep backfill asks for ~9.7 years in one request.

⚠⚠ THE SUGGESTED FIX IS NOT IMPLEMENTABLE AS WRITTEN, and that is a spec fact rather than
an opinion: the operation takes ``x-request-id``, ``minDate`` (required), ``page`` and
``pageSize`` — there is **no ``maxDate``**. Every request therefore ends at the present,
so "lookback" is ``now - minDate`` by construction and advancing ``minDate`` forward only
ever SHRINKS the returned set. A tiling scheme needs an upper bound the API does not have.

WHAT THE ARMS DECIDE
--------------------
The one reading under which the documented advice does tile a range is that the server
caps the response at ``[minDate, minDate + 1y]``. ``tests/fixtures/etoro/
trade_history_demo.json`` (captured 2026-06-13, ``minDate=2020-01-01``) already falsifies
it — it returned a trade closing 2025-11-14, 5.9 years after ``minDate``. Arm A re-runs
that against the current gateway at the real epoch, and arm B is its control.

⚠ SILENT TRUNCATION OF OLD ROWS IS UNDECIDABLE ON THIS ACCOUNT and no arm here claims
otherwise: the demo account's only close is 2025-11-14, which is inside a 364-day window
from today, so arms A and B cannot disagree about a row older than a year because no such
row exists. That is exactly why arm C is here — ``getClosedPositionEventsHistory`` counts
closed position events per ``(closeYear, assetType)`` from a different service, so it is
an INDEPENDENT total that stays decidable on an account whose history does span years.

Run (dev, demo credentials, 3 requests: 2 on lane G's history client, 1 on the read lane):

    PYTHONPATH=. uv run python -m scripts.probe_2991_history_lookback \
        --out tests/fixtures/etoro/history_lookback_probe_2026-09-13.json
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import psycopg

from app.config import settings
from app.providers.implementations.etoro_broker import EtoroBrokerProvider
from app.security.master_key import ensure_broker_key_loaded
from app.services.broker_credentials import load_credential_for_provider_use
from app.services.operators import sole_operator_id
from app.services.trade_events import HISTORY_EPOCH

_CALLER = "probe_2991_history_lookback"

# The documented maximum, in the form the operation states it: "1 year minus 1 day".
_MAX_LOOKBACK = timedelta(days=364)

# What the CONTROL arm asks for. ⚠ Deliberately a day inside the maximum, not on it: the
# arm's `minDate` is fixed by our clock and read by eToro's some seconds later (credential
# load, the deep-backfill arm, pagination, the lane-G floor), and `get_trade_history`
# floors `minDate` to whole seconds on top. An arm sitting exactly on 364 days is over the
# limit by the time it is served, so a gateway that enforced the maximum would reject the
# control as well as the test arm — and a control that can fail for the same reason as the
# arm it controls is not a control.
_CONTROL_LOOKBACK = _MAX_LOOKBACK - timedelta(days=1)

_CLOSED_EVENTS_PATH = "/api/v1/data/positions/closed-events/history"


def _load_demo_credentials(conn: psycopg.Connection[Any]) -> tuple[str, str]:
    ensure_broker_key_loaded(conn)
    operator_id = sole_operator_id(conn)
    keys: list[str] = []
    for label in ("api_key", "user_key"):
        keys.append(
            load_credential_for_provider_use(
                conn,
                operator_id=operator_id,
                provider="etoro",
                label=label,
                environment="demo",
                caller=_CALLER,
            )
        )
        conn.commit()
    return keys[0], keys[1]


def _history_arm(
    broker: EtoroBrokerProvider,
    label: str,
    *,
    min_date: datetime | None = None,
    lookback: timedelta | None = None,
) -> dict[str, Any]:
    """Run one history arm. Exactly one of ``min_date`` / ``lookback`` is given.

    A relative arm resolves its ``minDate`` HERE rather than at run start, so the elapsed
    wall-clock of the preceding arms cannot silently push it past the bound it claims.
    """
    if (min_date is None) == (lookback is None):
        raise ValueError("pass exactly one of min_date / lookback")
    requested_at = datetime.now(UTC)
    if min_date is None:
        assert lookback is not None  # noqa: S101 - narrowed by the guard above
        min_date = requested_at - lookback
    arm: dict[str, Any] = {
        "arm": label,
        "requested_at": requested_at.isoformat(),
        "min_date": min_date.isoformat(),
        "lookback_days": (requested_at - min_date).days,
        "exceeds_documented_max": (requested_at - min_date) > _MAX_LOOKBACK,
    }
    try:
        trades = broker.get_trade_history(min_date)
    except httpx.HTTPStatusError as exc:
        arm["outcome"] = "http_error"
        arm["status_code"] = exc.response.status_code
        arm["body_prefix"] = exc.response.text[:500]
        return arm
    except httpx.HTTPError as exc:
        arm["outcome"] = "transport_error"
        arm["error"] = f"{type(exc).__name__}: {exc}"
        return arm
    closes = sorted(t.close_timestamp for t in trades)
    arm["outcome"] = "ok"
    arm["row_count"] = len(trades)
    arm["position_ids"] = sorted(t.position_id for t in trades)
    arm["min_close_timestamp"] = closes[0].isoformat() if closes else None
    arm["max_close_timestamp"] = closes[-1].isoformat() if closes else None
    # The falsifier for "the server caps the response at minDate + 1 year".
    arm["max_close_minus_min_date_days"] = (closes[-1] - min_date).days if closes else None
    return arm


def _closed_events_arm(broker: EtoroBrokerProvider) -> dict[str, Any]:
    """Independent per-close-year counts — the completeness oracle (#2991)."""
    arm: dict[str, Any] = {"arm": "C_closed_event_counts", "path": _CLOSED_EVENTS_PATH}
    try:
        response = broker._http_read.get(  # noqa: SLF001 - one-off probe, no public method yet
            _CLOSED_EVENTS_PATH,
            headers=broker._request_headers(),  # noqa: SLF001
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        arm["outcome"] = "http_error"
        arm["status_code"] = exc.response.status_code
        arm["body_prefix"] = exc.response.text[:500]
        return arm
    except httpx.HTTPError as exc:
        arm["outcome"] = "transport_error"
        arm["error"] = f"{type(exc).__name__}: {exc}"
        return arm
    arm["outcome"] = "ok"
    arm["status_code"] = response.status_code
    arm["rate_limit_headers"] = {k: v for k, v in response.headers.items() if k.lower().startswith("ratelimit")}
    arm["body"] = response.json()
    return arm


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, help="write the verbatim observation set here")
    args = parser.parse_args(argv)

    now = datetime.now(UTC)
    with psycopg.connect(settings.database_url) as conn:
        api_key, user_key = _load_demo_credentials(conn)

    with EtoroBrokerProvider(api_key, user_key, env="demo") as broker:
        arms = [
            _history_arm(broker, "A_epoch_deep_backfill", min_date=HISTORY_EPOCH),
            _history_arm(broker, "B_within_documented_max", lookback=_CONTROL_LOOKBACK),
            _closed_events_arm(broker),
        ]

    report = {
        "_meta": {
            "refs": "#2991",
            "observed_at": now.isoformat(),
            "environment": "demo",
            "documented_max_lookback_days": _MAX_LOOKBACK.days,
            "control_lookback_days": _CONTROL_LOOKBACK.days,
            "openapi_version": "v1.375.0",
        },
        "arms": arms,
    }
    print(json.dumps(report, indent=2, default=str))
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2, default=str) + "\n")
        print(f"\nwrote {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
