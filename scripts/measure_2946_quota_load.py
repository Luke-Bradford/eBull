"""#2946 step 2: what do we actually SPEND against the eToro quota lanes? Refs #2946.

Step 1 (``bb1c2806``, PR #2969) produced the lane MAP -- what the portal documents --
and closed with the boundary this script exists to cross::

    Documentation proves a number is DOCUMENTED, never that it is ENFORCED.
    #2946 step 2 is the measurement.

Spec + verdict: ``docs/proposals/execution/2026-09-13-etoro-quota-load-census.md``.

READ-ONLY.  One REPEATABLE READ read-only snapshot, plus one log file.  No broker
request, no row written, no portal page re-fetched.  ⚠ It deliberately does NOT call
``_load_etoro_credentials``: that path writes a credential-access audit row and bumps
``last_used_at`` (``app/api/broker_credentials.py:469``), which would make a read-only
census a writer.  Credential identity is read from ``broker_credentials`` directly.

⚠⚠ WHAT THIS SCRIPT CANNOT DO, stated first because the first draft got it wrong
--------------------------------------------------------------------------------

**There is NO per-request artefact for any eToro lane.**  The first draft treated
``strategy_core_eligibility_proofs`` as a request log for lane B.  It is not, for four
independent reasons, each checkable in the schema or the call site:

1. ``observed_at TIMESTAMPTZ NOT NULL DEFAULT now()`` (``sql/346_core_eligibility_proofs.sql:47``)
   and PostgreSQL's ``now()`` is TRANSACTION-START time.  The insert runs inside
   ``with core_submission_lock(conn), conn.transaction():`` (``scheduler.py:6316``), a
   transaction that begins AFTER the HTTP round-trip and after a lock wait.  The column
   times the recording, not the request.
2. A request that fails in transport writes NO row -- eligibility is explicitly
   non-persisting (``etoro_broker.py:867``) and transport failures are excluded
   (``strategy_core_eligibility.py:273``).  Failures still spend quota.
3. Other eligibility callers write no proof at all
   (``strategy_paper_executor.py:1241``, ``strategy_position_manager.py:408``).
4. The table's own comment: *"Evidence of an observation, not an attestation of one ...
   nothing can stop a caller asserting an account it never contacted."*

So this census reports three things and never a fourth:

* **CONFIGURED** -- a pacing constant read from the code by import.  Exact, and on the
  lane-B path it is the ONLY thing standing between us and the quota (see A1).
* **DERIVED** -- an arithmetic upper bound from a code reading and a ``job_runs`` fire.
  A bound, printed with its algorithm, never called a measurement.
* **UNCOUNTED** -- traffic with no artefact and no fire record.  Named every run, never
  zero-filled.

It measures SERVICED DEMAND in the jobs process.  It is not an account-wide rate, and
the word "offered" does not appear in its output.

Source rule
-----------

Budgets are step 1's, read off ``app.providers.implementations.etoro_quota_lanes``
rather than restated -- a second copy of a number is a second thing to go stale.
``CallSite.conservative_per_minute`` is the LOWER of the per-endpoint figure and the
general page's operation tier, which is step 1's recorded policy because the portal
states no precedence between its two readings.

⚠ Step 1's portal evidence is a DATED TRANSCRIPT, not a pinned artefact
(``etoro_quota_lanes.py:38-40``).  Nothing here upgrades that provenance.

⚠ Lane G's two call sites BOTH read ``conservative_per_minute == 20``; what differs
between them is ``membership_ambiguous``, not the number.  (An earlier draft claimed
they carried different conservative readings -- they do not.)

Reconciliation latency has its own governing rule and this script uses it rather than
inventing one: ``enforce_reconciliation_slo`` measures unresolved order identity from
``strategy_order_reconciliation_state.first_unresolved_at`` against a deployment-supplied
``max_unresolved_seconds`` (``app/services/strategy_order_reconciliation.py:1024``).
A whole-job duration is not reconciliation latency and is not reported as one.
"""

from __future__ import annotations

import argparse
import importlib
import re
import subprocess
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, TextIO

import psycopg

from app.config import settings
from app.providers.implementations import etoro_quota_lanes as lanes
from app.providers.implementations import etoro_request_log

# ---------------------------------------------------------------------------
# A1 -- how each caller is actually paced
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PacedCaller:
    """One code path that reaches eToro, and what actually spaces its requests.

    ``construction`` is the load-bearing field and it is read from the call site:

    * ``per_request`` -- the provider is built INSIDE the request loop, so every
      request starts on a clock at 0.0 and the ``ResilientClient`` floor CANNOT pace the
      batch.  Whatever pacing exists is application-level.
    * ``reused`` -- one provider spans the loop, so the floor applies.
    """

    label: str
    site: str
    lane: str
    construction: str  # "per_request" | "reused"
    #: (module, attr) of the constant that actually paces this caller, resolved by
    #: import at run time.  Never copied here: a copied floor makes the census compare
    #: this file with itself.  Step 1 makes the same argument for FLOOR_CONSTANT_NAMES.
    pacing_const: tuple[str, str]
    note: str


PACED_CALLERS: tuple[PacedCaller, ...] = (
    PacedCaller(
        "core_eligibility_refresh (job)",
        "app/workers/scheduler.py:6290",
        "B_eligibility",
        "per_request",
        ("app.services.strategy_core_eligibility", "CORE_ELIGIBILITY_REQUEST_INTERVAL_S"),
        "fresh EtoroBrokerProvider inside the per-instrument loop; paced by an explicit "
        "time.sleep in the loop's finally (scheduler.py:6383)",
    ),
    PacedCaller(
        "prove_2603_core_eligibility prove",
        "scripts/prove_2603_core_eligibility.py:119",
        "B_eligibility",
        "per_request",
        ("app.services.strategy_core_eligibility", "CORE_ELIGIBILITY_REQUEST_INTERVAL_S"),
        "one instrument per request by design (the proof digests the whole response); sleeps at :152",
    ),
    PacedCaller(
        "prove_2603_core_eligibility census",
        "scripts/prove_2603_core_eligibility.py:191",
        "B_eligibility",
        "per_request",
        ("app.services.strategy_core_eligibility", "CORE_ELIGIBILITY_REQUEST_INTERVAL_S"),
        "batched 100 ids per request; sleeps at :209",
    ),
    PacedCaller(
        "daily_portfolio_sync (job)",
        "app/workers/scheduler.py:4479",
        "E_account_read",
        "reused",
        ("app.providers.implementations.etoro_broker", "_ETORO_READ_INTERVAL_S"),
        "one provider for the fire; the read floor applies",
    ),
    PacedCaller(
        "strategy_paper_cycle (job)",
        "app/workers/scheduler.py:5930",
        "D_order_info",
        "reused",
        ("app.providers.implementations.etoro_broker", "_ETORO_READ_INTERVAL_S"),
        "one provider for the fire; reconciliation lookups ride the read clock",
    ),
    PacedCaller(
        "WS reconcile runner (API process)",
        "app/services/etoro_websocket.py:999",
        "E_account_read",
        "reused",
        ("app.providers.implementations.etoro_broker", "_ETORO_READ_INTERVAL_S"),
        "own provider, own clock, DIFFERENT OS PROCESS -- no in-process lock can reach it",
    ),
    PacedCaller(
        "quotes_refresh / daily_candle_refresh",
        "app/providers/implementations/etoro.py:126",
        "F_market_data",
        "reused",
        ("app.providers.implementations.etoro", "_ETORO_READ_INTERVAL_S"),
        "MODULE-level clock (etoro.py:77-78), so every instance in one process shares "
        "one floor -- the only lane with process-wide coordination",
    ),
)

#: The reaper's exact message. ``ops_monitor.py:648`` rewrites an orphaned ``running``
#: row at boot with ``finished_at = now()``, so a reaped row's "duration" spans the
#: OUTAGE, not the work -- the difference between a 90-minute candle sweep and a 17-day
#: one. ⚠ Counting stranded ``running`` rows is the WRONG detector: the reaper has
#: already converted them. Match the message.
REAPER_MSG_PREFIX = "orphaned: reaped at boot"

_SCHED = "app/workers/scheduler.py"


@dataclass(frozen=True)
class JobClock:
    """A scheduled job that can reach eToro, and the scope of its provider's clock.

    ``clock_scope`` is read from the CONSTRUCTOR, never guessed from the job name:
    ``EtoroBrokerProvider.__init__`` builds a fresh ``shared_ts`` + lock per instance
    (``etoro_broker.py:225-236``); ``EtoroMarketDataProvider.__init__`` passes the
    module-level clock (``etoro.py:126-130``).

    ⚠ ``process`` scope is process-LOCAL. The API process holds its own module clock, so
    two processes still do not coordinate.
    """

    job_name: str
    construction_site: str
    clock_scope: str  # "instance" | "process" | "per_request"
    floor_attrs: tuple[tuple[str, str, str], ...]
    lanes_reachable: tuple[str, ...]


_BROKER_MOD = "app.providers.implementations.etoro_broker"
_MARKET_MOD = "app.providers.implementations.etoro"

JOB_CLOCKS: tuple[JobClock, ...] = (
    JobClock(
        "daily_portfolio_sync",
        f"{_SCHED}:4479",
        "instance",
        # Two floors since #2946 step 3 item 1: the portfolio read is lane E on
        # `_http_read`, and `get_trade_history` is lane G on its own `_http_history`
        # client. They share one CLOCK, so the pair still caps their sum — but the
        # paginator no longer pages at the 1.1s read floor.
        (
            (_BROKER_MOD, "_ETORO_READ_INTERVAL_S", "_http_read"),
            (_BROKER_MOD, "_ETORO_HISTORY_INTERVAL_S", "_http_history"),
        ),
        ("E_account_read", "G_default_shared"),
    ),
    JobClock(
        "execute_approved_orders",
        f"{_SCHED}:4988",
        "instance",
        (
            (_BROKER_MOD, "_ETORO_READ_INTERVAL_S", "_http_read"),
            (_BROKER_MOD, "_ETORO_WRITE_INTERVAL_S", "_http_write"),
        ),
        ("A_order_write", "B_eligibility", "C_what_if_costs", "E_account_read"),
    ),
    JobClock(
        "strategy_paper_cycle",
        f"{_SCHED}:5930",
        "instance",
        (
            (_BROKER_MOD, "_ETORO_READ_INTERVAL_S", "_http_read"),
            (_BROKER_MOD, "_ETORO_WRITE_INTERVAL_S", "_http_write"),
        ),
        ("A_order_write", "B_eligibility", "C_what_if_costs", "D_order_info", "E_account_read"),
    ),
    JobClock(
        "core_rebalance_observation",
        f"{_SCHED}:6038",
        "instance",
        ((_BROKER_MOD, "_ETORO_READ_INTERVAL_S", "_http_read"),),
        ("E_account_read",),
    ),
    JobClock(
        "core_eligibility_refresh",
        f"{_SCHED}:6290",
        "per_request",
        ((_BROKER_MOD, "_ETORO_WRITE_INTERVAL_S", "_http_write"),),
        ("B_eligibility",),
    ),
    JobClock(
        "quotes_refresh",
        f"{_MARKET_MOD} (module clock)",
        "process",
        ((_MARKET_MOD, "_ETORO_READ_INTERVAL_S", "_http"),),
        ("F_market_data",),
    ),
    JobClock(
        "daily_candle_refresh",
        f"{_MARKET_MOD} (module clock)",
        "process",
        ((_MARKET_MOD, "_ETORO_READ_INTERVAL_S", "_http"),),
        ("F_market_data",),
    ),
)

JOB_NAMES = tuple(j.job_name for j in JOB_CLOCKS)

#: eToro traffic with neither a per-request artefact nor a ``job_runs`` row. Printed in
#: full every run: an UNCOUNTED source that stops being named stops being remembered.
#: ⚠ Suppressed fires are deliberately NOT in this list -- an earlier draft called them
#: invisible and that is wrong: ``app/jobs/runtime.py:1636`` records a
#: ``max_instances_active`` skip row and ``:1665`` records misfires.
UNCOUNTED_PATHS: tuple[tuple[str, str], ...] = (
    (
        "app/services/etoro_websocket.py:999",
        "WS reconcile -- own provider, get_portfolio + get_trade_history, event-driven, "
        "API process, writes no job_runs row",
    ),
    ("app/services/etoro_websocket.py:1622", "subscriber REST quote polling -- API process"),
    ("app/api/instruments.py:1390", "API candle requests served to the operator UI"),
    ("app/api/strategies.py:3357", "close_strategy_owned_position -- operator-triggered"),
    ("app/api/strategies.py:3852", "rebalance_core_sleeve -- operator-triggered"),
    ("scripts/ (6 broker constructions in 5 files)", "research/operator scripts, plus market-data scripts"),
    ("etoro_quota_lanes.KNOWN_UNTHROTTLED (4 sites)", "raw httpx, bypasses every throttle -- frequency UNMEASURED"),
)

# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------

#: ``ResilientClient`` logs ``"Retryable %d from %s %s"`` with (status, method, url) --
#: ``resilient_client.py:213-216``.
#:
#: ⚠⚠ The third field is THE URL ARGUMENT AS PASSED, not a normalised absolute URL. SEC
#: providers pass absolute URLs; the eToro providers pass RELATIVE PATHS against an
#: httpx base_url. So an eToro 429 carries NO HOSTNAME, and classifying by host silently
#: drops exactly the rows this ticket is about.
RETRYABLE_RE = re.compile(r"Retryable (?P<status>\d{3}) from (?P<method>[A-Z]+) (?P<url>\S+)")
ABSOLUTE_RE = re.compile(r"^https?://(?P<host>[^/]+)")
TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

#: eToro path prefixes, derived from the lane map's own templates rather than typed out.
#: ⚠ Both the demo and real path shapes are covered: step 1 records that our real-env
#: paths DIFFER from the demo ones (``KNOWN_PATH_DRIFT``), so matching only ``demo_path``
#: would miss a real-env retryable entirely.
ETORO_PATH_PREFIXES: tuple[str, ...] = tuple(
    sorted({"/" + "/".join(c.demo_path.lstrip("/").split("/")[:3]) for c in lanes.CALL_SITES})
)

DEFAULT_LOG = Path.home() / "Dev/eBull/var/autonomy-logs/launchd.jobs-daemon.err.log"


def _git_sha() -> str:
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True, check=True).stdout
        return f"{sha}{'-dirty' if dirty.strip() else ''}"
    except Exception:  # pragma: no cover - provenance is best-effort, never fatal
        return "unknown"


def _const(module_path: str, attr: str) -> float:
    return float(getattr(importlib.import_module(module_path), attr))


def stamps_in_window(interval_s: float, window_s: float = 60.0) -> int:
    """Requests one caller at ``interval_s`` can place in any ``window_s``.

    A caller spaced at ``i`` fires at 0, i, 2i, ... so a 60 s window holds
    ``floor(60 / i) + 1``.  ⚠ The ``+ 1`` is not pedantry: at 3.2 s that is 19, not 18,
    and at 3.5 s it is 18, not 17.  Sustained throughput (``60 / i``) is a DIFFERENT
    number and is the wrong one to compare against a rolling-minute quota.
    """
    return int(window_s // interval_s) + 1


def _rolling_bound(duration_s: float, floor_s: float, window_s: float = 60.0) -> int:
    """Most requests one floored clock can place in any window during a fire.

    ⚠ An upper bound and nothing more. It assumes every request fell inside the recorded
    ``[started_at, finished_at]`` and that ONE clock was retained for the whole fire --
    which is false for any ``per_request`` caller. A fire that spent its whole duration
    in Postgres bounds identically to one that spent it on HTTP.
    """
    return int(min(duration_s, window_s) // floor_s) + 1


def _fetch_one(cur: psycopg.Cursor[Any]) -> tuple[Any, ...]:
    """``fetchone`` with the ``None`` case made explicit.

    Every call site here runs an aggregate, which always returns exactly one row. If one
    ever does not, failing on this line beats unpacking ``None`` three frames later.
    """
    row = cur.fetchone()
    if row is None:  # pragma: no cover - an aggregate with no row is a broken query
        raise RuntimeError("aggregate query returned no row")
    return row


def _fmt_dt(value: datetime | None) -> str:
    return "-" if value is None else value.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------


def a1_pacing(out: TextIO) -> None:
    """CONFIGURED pacing -- what actually spaces each caller, and at what rate."""
    print("\n" + "=" * 78, file=out)
    print("A1  CONFIGURED pacing -- read from code by import, not copied", file=out)
    print("=" * 78, file=out)

    budget = {c.lane: c.conservative_per_minute for c in lanes.CALL_SITES}

    print(
        f"\n{'caller':40s} {'lane':16s} {'construction':12s} {'pace s':>7s} {'max/60s':>8s} {'budget':>7s}",
        file=out,
    )
    per_lane: dict[str, list[tuple[str, int]]] = {}
    for c in PACED_CALLERS:
        pace = _const(*c.pacing_const)
        rate = stamps_in_window(pace)
        b = budget.get(c.lane, 0)
        print(f"{c.label:40s} {c.lane:16s} {c.construction:12s} {pace:7.2f} {rate:8d} {b:7d}", file=out)
        per_lane.setdefault(c.lane, []).append((c.label, rate))

    print("\nnotes:", file=out)
    for c in PACED_CALLERS:
        print(f"  {c.label:40s} {c.site}\n      {c.note}", file=out)

    # The mechanism, stated as arithmetic rather than prose.
    write_floor = _const(_BROKER_MOD, "_ETORO_WRITE_INTERVAL_S")
    elig_pace = _const("app.services.strategy_core_eligibility", "CORE_ELIGIBILITY_REQUEST_INTERVAL_S")
    print("\n" + "-" * 78, file=out)
    print("FINDING 1 -- on the lane-B path the ResilientClient floor is unreachable", file=out)
    print("-" * 78, file=out)
    print(
        f"All three lane-B callers build a provider INSIDE the request loop, so every\n"
        f"request is the first on its own clock and _ETORO_WRITE_INTERVAL_S={write_floor}s\n"
        f"never paces the batch. Pacing is CORE_ELIGIBILITY_REQUEST_INTERVAL_S={elig_pace}s.\n"
        f"  one caller:  {stamps_in_window(elig_pace)}/60s vs budget {budget.get('B_eligibility')}\n"
        f"  had the floor applied: {stamps_in_window(write_floor)}/60s\n"
        f"\n"
        f"VERDICT (step 3, 2026-09-13): unreachable here is CORRECT, not a defect.\n"
        f"_ETORO_WRITE_INTERVAL_S paces _http_write, which also carries lane A (order\n"
        f"submission) and lane C; lane B is documented DEDICATED, so pacing it from that\n"
        f"floor would make a research sweep delay order writes for no quota reason. What\n"
        f"step 3 fixed is PROVENANCE: the lane-B constant is now derived from lane B's own\n"
        f"budget via etoro_quota_lanes.min_interval_for_stamps, where it used to be chosen.\n"
        f"See docs/proposals/execution/2026-09-13-lane-b-dead-throttle-fix.md.",
        file=out,
    )

    print("\n" + "-" * 78, file=out)
    print("FINDING 2 -- two lane-B callers on one user key exceed the lane budget", file=out)
    print("-" * 78, file=out)
    b_callers = per_lane.get("B_eligibility", [])
    combined = sum(rate for _, rate in b_callers[:2])
    b_budget = budget.get("B_eligibility", 0)
    # ⚠ A missing or zero budget means the lane map no longer carries an eligibility call
    # site -- a renamed lane, a removed endpoint. That is a finding about the MAP, and it
    # must surface as one rather than as a ZeroDivisionError that takes the whole census
    # down with it (review round 1).
    share = (
        f"{combined / b_budget * 100:.0f}% of the conservative budget"
        if b_budget
        else (
            "share UNCOMPUTABLE -- no eligibility call site in the lane map carries a "
            "non-zero conservative budget. Re-read etoro_quota_lanes.CALL_SITES."
        )
    )
    print(
        f"Nothing serialises the hourly job against an operator/research run of the same\n"
        f"script: the job takes the `etoro` job lane, a script takes no lane at all, and\n"
        f"their clocks are independent by construction. Two concurrent lane-B callers:\n"
        f"  {combined}/60s vs budget {b_budget}  -> {share}\n"
        f"⚠ This is a CAPABILITY, not an observed event. Whether it has happened is M3's\n"
        f"question and M3 can only answer it for callers that write a job_runs row.",
        file=out,
    )


def m2_derived(conn: psycopg.Connection, out: TextIO, *, since: str | None) -> None:
    print("\n" + "=" * 78, file=out)
    print("M2  DERIVED serviced demand per job -- a BOUND, not a count", file=out)
    print("=" * 78, file=out)
    print(
        "bound(fire) = floor(min(duration, 60) / floor_s) + 1, per CLOCK.\n"
        "⚠ Meaningless for a per_request caller: its floor never applies. Those rows are\n"
        "  marked and their bound is taken from the pacing constant in A1 instead.",
        file=out,
    )

    cur = conn.cursor()
    where_since = "and started_at >= %(since)s::timestamptz" if since else ""
    params = {"jobs": list(JOB_NAMES), "reaper": REAPER_MSG_PREFIX + "%", "since": since}

    cur.execute(
        f"""
        select job_name,
               count(*) filter (where not reaped and dur is not null) as fires,
               count(*) filter (where reaped) as reaped,
               count(*) filter (where dur is null) as unfinished,
               count(*) filter (where not reaped and status = 'success') as ok,
               round(avg(dur) filter (where not reaped)::numeric, 1) as avg_s,
               round(max(dur) filter (where not reaped)::numeric, 1) as max_s,
               min(started_at), max(started_at)
          from (
            select job_name, status, started_at,
                   coalesce(error_msg, '') like %(reaper)s as reaped,
                   extract(epoch from (finished_at - started_at)) as dur
              from job_runs
             where job_name = any(%(jobs)s) {where_since}
          ) s
         group by job_name
        """,
        params,
    )
    by_job = {r[0]: r for r in cur.fetchall()}

    print(
        f"\n{'job':28s} {'fires':>6s} {'reap':>5s} {'unfin':>6s} {'ok':>5s} {'avg_s':>8s} {'max_s':>9s} {'clock':>12s}",
        file=out,
    )
    total_reaped = total_unfinished = 0
    for jc in JOB_CLOCKS:
        r = by_job.get(jc.job_name)
        if r is None:
            dashes = f"{'-':>6s} {'-':>5s} {'-':>6s} {'-':>5s} {'-':>8s} {'-':>9s}"
            print(f"{jc.job_name:28s} {dashes} {jc.clock_scope:>12s}", file=out)
            continue
        total_reaped += r[2]
        total_unfinished += r[3]
        print(
            f"{jc.job_name:28s} {r[1]:6d} {r[2]:5d} {r[3]:6d} {r[4]:5d} "
            f"{str(r[5]):>8s} {str(r[6]):>9s} {jc.clock_scope:>12s}",
            file=out,
        )
    print(
        f"\n{total_reaped} reaped and {total_unfinished} unfinished row(s) EXCLUDED from duration\n"
        f"arithmetic and counted above. ⚠ Excluding them removes REAL requests too -- the\n"
        f"affected windows are incomplete, not clean. ops_monitor.py:648 sets a reaped row's\n"
        f"finished_at to boot time, so its 'duration' is the outage.",
        file=out,
    )

    print("\nper-fire bound on the worst non-reaped fire, by clock:", file=out)
    print(f"{'job':28s} {'client':>12s} {'floor s':>8s} {'bound':>7s}  lanes reachable", file=out)
    for jc in JOB_CLOCKS:
        r = by_job.get(jc.job_name)
        if r is None or r[6] is None:
            continue
        if jc.clock_scope == "per_request":
            print(f"{jc.job_name:28s} {'n/a':>12s} {'n/a':>8s} {'n/a':>7s}  per_request -- see A1", file=out)
            continue
        for module_path, attr, client_attr in jc.floor_attrs:
            floor_s = _const(module_path, attr)
            print(
                f"{jc.job_name:28s} {client_attr:>12s} {floor_s:8.2f} "
                f"{_rolling_bound(float(r[6]), floor_s):7d}  {','.join(jc.lanes_reachable)}",
                file=out,
            )

    print(
        "\n⚠ The bound is per CLOCK and one clock serves several lanes: a write-clock bound\n"
        "  of N means at most N across lanes A+B+C+G-write TOGETHER, not N in any one lane.\n"
        "⚠ Lane G is never aggregated -- membership is UNENUMERATED and one of its two\n"
        "  sites is membership_ambiguous, so no lane-G comparison is printed.",
        file=out,
    )


def m3_concurrency(conn: psycopg.Connection, out: TextIO, *, since: str | None) -> None:
    print("\n" + "=" * 78, file=out)
    print("M3  concurrency -- POSSIBLE request overlap, NOT demonstrated contention", file=out)
    print("=" * 78, file=out)
    print(
        "Two runs overlapping in time COULD have placed requests in one rolling minute.\n"
        "They need not have; and two runs that do NOT overlap can still share one. This is\n"
        "an upper bound on co-occurrence between JOBS -- not between provider instances,\n"
        "not between quota families, and blind to every UNCOUNTED caller below.",
        file=out,
    )

    cur = conn.cursor()
    where_since = "and started_at >= %(since)s::timestamptz" if since else ""
    params = {"jobs": list(JOB_NAMES), "reaper": REAPER_MSG_PREFIX + "%", "since": since}
    scope = {j.job_name: j.clock_scope for j in JOB_CLOCKS}
    lane_of = {j.job_name: set(j.lanes_reachable) for j in JOB_CLOCKS}

    cur.execute(
        f"""
        with live as (
          select job_name, started_at, finished_at from job_runs
           where job_name = any(%(jobs)s) {where_since}
             and finished_at is not null
             and coalesce(error_msg, '') not like %(reaper)s
             and finished_at > started_at
        )
        select a.job_name, b.job_name, count(*),
               round(max(extract(epoch from (least(a.finished_at, b.finished_at)
                                           - greatest(a.started_at, b.started_at))))::numeric, 1)
          from live a join live b
            on a.job_name < b.job_name
           and a.started_at < b.finished_at and b.started_at < a.finished_at
         group by 1, 2 order by 3 desc
        """,
        params,
    )
    pairs = cur.fetchall()
    print(f"\n{'job A':26s} {'job B':26s} {'n':>5s} {'max s':>7s}  clocks / shared lanes", file=out)
    for a, b, n, secs in pairs:
        sa, sb = scope.get(a, "?"), scope.get(b, "?")
        shared = sorted(lane_of.get(a, set()) & lane_of.get(b, set()))
        verdict = "SHARED floor" if sa == sb == "process" else "INDEPENDENT (rates ADD)"
        print(f"{a:26s} {b:26s} {n:5d} {str(secs):>7s}  {verdict}; lanes={shared or '-'}", file=out)
    if not pairs:
        print("(none)", file=out)

    cur.execute(
        f"""
        with live as (
          select started_at, finished_at from job_runs
           where job_name = any(%(jobs)s) {where_since}
             and finished_at is not null
             and coalesce(error_msg, '') not like %(reaper)s
             and finished_at > started_at
        ), edges as (
          select started_at as t, 1 as d from live
          union all select finished_at as t, -1 as d from live
        )
        select max(running) from (
          select sum(d) over (order by t, d desc rows unbounded preceding) as running from edges
        ) s
        """,
        params,
    )
    (peak_n,) = _fetch_one(cur)
    print(f"\nMax simultaneous eToro-touching JOB runs: {peak_n}", file=out)
    print(
        "⚠ Job concurrency is not instance concurrency and not quota concurrency: one job\n"
        "  can fan out to several providers (every per_request caller does), two jobs can\n"
        "  share one clock, and two overlapping jobs can touch disjoint lanes.",
        file=out,
    )

    cur.execute(
        """
        select environment, count(*), count(distinct label)
          from broker_credentials where provider = 'etoro' and revoked_at is null
         group by environment order by 1
        """
    )
    rows = cur.fetchall()
    print("\nlive eToro credentials (read directly; the loader would WRITE an audit row):", file=out)
    for env, n, labels in rows:
        print(f"  environment={env:8s} rows={n} distinct labels={labels}", file=out)
    print(
        "⚠ The quota is per USER KEY. Independent clocks contend only on a shared key, and\n"
        "  credential-row inequality does not prove key inequality (two rows can carry the\n"
        "  same key). Historical calls cannot be attributed to today's rows at all.",
        file=out,
    )


def m4_retryables(conn: psycopg.Connection, out: TextIO, *, log_path: Path) -> None:
    print("\n" + "=" * 78, file=out)
    print("M4  retryable-response census -- a LOWER bound, jobs process only", file=out)
    print("=" * 78, file=out)

    if not log_path.exists():
        print(f"log not found: {log_path} -- M4 cannot run, and that is NOT a zero", file=out)
    else:
        size = log_path.stat().st_size
        lines_read = retry_lines = 0
        by_host: dict[tuple[str, str], int] = {}
        by_path: dict[tuple[str, str], int] = {}
        by_other_relative: dict[tuple[str, str], int] = {}
        unparsed: list[str] = []
        first_ts = last_ts = None

        with log_path.open(errors="replace") as fh:
            for line in fh:
                lines_read += 1
                if TS_RE.match(line):
                    ts = line[:19]
                    first_ts = first_ts or ts
                    last_ts = ts
                if "Retryable " not in line:
                    continue  # an ordinary log line is NOT an unmatched request
                retry_lines += 1
                m = RETRYABLE_RE.search(line)
                if not m:
                    unparsed.append(line.rstrip())
                    continue
                status, url = m.group("status"), m.group("url")
                abs_m = ABSOLUTE_RE.match(url)
                if abs_m:
                    key = (abs_m.group("host"), status)
                    by_host[key] = by_host.get(key, 0) + 1
                elif url.startswith("/"):
                    # ⚠ A relative URL is NOT automatically an eToro one. Companies
                    # House also logs relative paths (/company/{n}/filing-history), and
                    # bucketing every relative URL as eToro reports another provider's
                    # 429 as ours. Match the lane map's own path prefixes.
                    prefix = next((pre for pre in ETORO_PATH_PREFIXES if url.startswith(pre)), None)
                    if prefix is not None:
                        key = (prefix + "...", status)
                        by_path[key] = by_path.get(key, 0) + 1
                    else:
                        key = ("/" + url.lstrip("/").split("/")[0] + "/... (NOT eToro)", status)
                        by_other_relative[key] = by_other_relative.get(key, 0) + 1
                else:
                    unparsed.append(line.rstrip())

        print(f"log: {log_path}", file=out)
        print(f"bytes={size}  lines_read={lines_read}  lines_containing_Retryable={retry_lines}", file=out)
        print(f"timestamp span (naive local, as the daemon wrote it): {first_ts} .. {last_ts}", file=out)
        print("\nABSOLUTE-url retryables -- SEC and other absolute-URL providers:", file=out)
        for (host, status), n in sorted(by_host.items(), key=lambda kv: -kv[1]):
            print(f"  {host:34s} {status}  {n}", file=out)
        print("\nRELATIVE-path retryables matching an eToro CALL_SITES prefix:", file=out)
        for (prefix, status), n in sorted(by_path.items(), key=lambda kv: -kv[1]):
            print(f"  {prefix:34s} {status}  {n}", file=out)
        if not by_path:
            print("  (none)", file=out)
        print("\nRELATIVE-path retryables from OTHER providers (not counted as eToro):", file=out)
        for (prefix, status), n in sorted(by_other_relative.items(), key=lambda kv: -kv[1]):
            print(f"  {prefix:34s} {status}  {n}", file=out)
        if not by_other_relative:
            print("  (none)", file=out)
        print(
            f"\nPARSED-EVENT accounting: {retry_lines} = {sum(by_host.values())} absolute + "
            f"{sum(by_path.values())} eToro-relative + {sum(by_other_relative.values())} "
            f"other-relative + {len(unparsed)} malformed",
            file=out,
        )
        for line in unparsed[:5]:
            print(f"  malformed: {line[:150]}", file=out)
        etoro_429 = sum(n for (_, s), n in by_path.items() if s == "429")
        print(f"\neToro 429s in this log: {etoro_429}", file=out)

    cur = conn.cursor()
    cur.execute(
        """
        select job_name, count(*), min(started_at), max(started_at), left(min(coalesce(error_msg,'')), 80)
          from job_runs where error_category = 'rate_limited' group by 1 order by 2 desc
        """
    )
    rows = cur.fetchall()
    print("\nSECOND, INDEPENDENT source -- job_runs.error_category='rate_limited'", file=out)
    print("(exception_classifier.py:55; a final-attempt 429 raises past the warning line):", file=out)
    for job, n, lo, hi, msg in rows or []:
        print(f"  {job:30s} {n:4d}  {_fmt_dt(lo)} .. {_fmt_dt(hi)}  {msg}", file=out)
    if not rows:
        print("  (none)", file=out)

    print(
        "\n⚠ The two sources are reported SIDE BY SIDE, never joined: no deterministic key\n"
        "  links a log line to a job_runs row, and three warnings plus a final 429 are four\n"
        "  RESPONSES that may be one INCIDENT. Attempts and incidents are different units.\n"
        "⚠ LOWER bound: the final attempt raises via raise_for_status() with no warning\n"
        "  line (resilient_client.py:201-203), and callers swallow terminal failures\n"
        "  (scheduler.py:4492 catches risk failures; strategy_paper_executor.py:1242 turns\n"
        "  an eligibility exception into a rejection), so those 429s reach neither source.\n"
        "⚠ Scope: the JOBS process. The API process logs elsewhere; its retained log ends\n"
        "  2026-08-26. Nothing here is account-wide.\n"
        "⚠ Coverage: first/last timestamps bound retention, they do not prove it is\n"
        "  continuous. A rotated-away file or an internal logging gap is undetectable here.",
        file=out,
    )


def m5_reconciliation(conn: psycopg.Connection, out: TextIO) -> None:
    """Reconciliation latency, measured against its OWN governing rule."""
    print("\n" + "=" * 78, file=out)
    print("M5  reconciliation latency -- per enforce_reconciliation_slo, not job duration", file=out)
    print("=" * 78, file=out)
    print(
        "Source rule: app/services/strategy_order_reconciliation.py:1024 measures unresolved\n"
        "order identity from strategy_order_reconciliation_state.first_unresolved_at against\n"
        "a deployment-supplied max_unresolved_seconds. A whole-job duration is NOT this.\n"
        "⚠ Its predicate is copied, not paraphrased: state NOT IN ('resolved','rejected').\n"
        "  first_unresolved_at STAYS populated after a terminal transition, so counting it\n"
        "  alone would report every completed order as unresolved forever (Codex ckpt-2).",
        file=out,
    )
    cur = conn.cursor()
    cur.execute(
        """
        select count(*) filter (
                 where first_unresolved_at is not null
                   and state not in ('resolved', 'rejected')
               ) as unresolved,
               min(first_unresolved_at) filter (where state not in ('resolved', 'rejected')),
               max(first_unresolved_at) filter (where state not in ('resolved', 'rejected')),
               count(*)
          from strategy_order_reconciliation_state
        """
    )
    unresolved, lo, hi, total = _fetch_one(cur)
    print(f"\nrows={total}  with first_unresolved_at set={unresolved}", file=out)
    print(f"oldest unresolved={_fmt_dt(lo)}  newest={_fmt_dt(hi)}", file=out)
    if not unresolved:
        print(
            "\n⚠ Zero unresolved rows means the SLO has no population to measure, NOT that\n"
            "  reconciliation is fast. With the demo account holding one manual filled order\n"
            "  and no recommendation-origin orders, the quantity acceptance item 2 asks for\n"
            "  has no observations at all. That is an evidence gap, not a green light.",
            file=out,
        )


def print_uncounted(out: TextIO) -> None:
    print("\n" + "=" * 78, file=out)
    print("UNCOUNTED -- eToro traffic this census cannot see. Never zero-filled.", file=out)
    print("=" * 78, file=out)
    for site, why in UNCOUNTED_PATHS:
        print(f"  {site:46s} {why}", file=out)
    print(
        "\n⚠ Every rate above is therefore a LOWER bound on account-wide load, while every\n"
        "  per-fire figure is an UPPER bound on that fire. The two bounds point opposite\n"
        "  ways and neither can be turned into a headroom claim on its own.",
        file=out,
    )


# ---------------------------------------------------------------------------
# M6 -- OBSERVED per-lane load, from the #2946 step 3 item 3 request artefact
# ---------------------------------------------------------------------------
#
# Everything above this line is CONFIGURED, DERIVED or UNCOUNTED: no eToro lane had a
# per-request artefact when step 2 ran.  ``app.providers.implementations.
# etoro_request_log`` now emits one line per issued attempt, and this arm reads them.
#
# ⚠ It takes ``--request-log`` MORE THAN ONCE and merges by the line's own ``ts``.  The
# API process and the jobs daemon log to different files, and step 2's open clause 2
# ("two concurrent lane-B callers on one user key") is a cross-PROCESS question that a
# single file cannot answer.


@dataclass(frozen=True)
class ObservedAttempt:
    ts: datetime
    lane: str
    src: str
    env: str
    status: str
    err: str
    wait_s: float


_REQUEST_MARKER = f"{etoro_request_log.LINE_PREFIX} "


def parse_request_log(lines: Iterable[str]) -> tuple[list[ObservedAttempt], int]:
    """Parse emitted request lines out of a log file.

    Returns ``(records, malformed_count)``.  Malformed lines are COUNTED and reported,
    never silently dropped: a reader that drops evidence turns missing data into "no
    traffic", which is the one error that would falsely close this ticket.

    ⚠ A line is claimed by its MARKER first, and only then validated.  Matching on the
    whole well-formed shape instead would make a truncated or corrupted record
    indistinguishable from an unrelated log line -- damaged evidence would vanish into
    the "not ours" bucket, which is the exact failure this counter exists to prevent.

    The handler prefix is skipped rather than parsed -- the API process and the jobs
    daemon format it differently and the API's carries no timestamp, which is why the
    timestamp is inside the line.
    """
    records: list[ObservedAttempt] = []
    malformed = 0
    for line in lines:
        index = line.find(_REQUEST_MARKER)
        if index < 0:
            continue
        fields: dict[str, str] = {}
        for token in line[index + len(_REQUEST_MARKER) :].split(" "):
            key, sep, value = token.partition("=")
            if sep:
                fields[key] = value
        if fields.get("v") != str(etoro_request_log.SCHEMA_VERSION):
            malformed += 1
            continue
        try:
            records.append(
                ObservedAttempt(
                    ts=datetime.fromisoformat(fields["ts"]),
                    lane=fields["lane"],
                    src=fields["src"],
                    env=fields["env"],
                    status=fields["status"],
                    err=fields["err"],
                    wait_s=float(fields["wait_s"]),
                )
            )
        except KeyError, ValueError:
            malformed += 1
    return records, malformed


def max_rolling_window(timestamps: Sequence[datetime], window_s: float = 60.0) -> int:
    """Largest number of attempts in any window, counted the way a quota counts them.

    The window is half-open ``(t - window_s, t]`` and anchored on each attempt, so the
    anchor is always counted.  This is the OBSERVED counterpart of the census's DERIVED
    ``stamps_in_window`` bound, and it is deliberately the same counting rule: a stamp
    exactly ``window_s`` old has left the window.
    """
    ordered = sorted(timestamps)
    left = 0
    best = 0
    for right, now in enumerate(ordered):
        while (now - ordered[left]).total_seconds() >= window_s:
            left += 1
        best = max(best, right - left + 1)
    return best


def m6_observed(out: TextIO, paths: Sequence[Path]) -> None:
    print("\n" + "=" * 78, file=out)
    print("M6 -- OBSERVED per-lane load (#2946 step 3 item 3 request artefact)", file=out)
    print("=" * 78, file=out)

    records: list[ObservedAttempt] = []
    malformed = 0
    for path in paths:
        if not path.exists():
            print(f"  ⚠ MISSING: {path} -- absent evidence, NOT zero traffic", file=out)
            continue
        found, bad = parse_request_log(path.read_text(errors="replace").splitlines())
        print(f"  read {len(found):7d} attempts ({bad} malformed) from {path}", file=out)
        records.extend(found)
        malformed += bad

    if not records:
        print(
            "\n  No request lines found. Either the instrumented build is not deployed yet,\n"
            "  or these files do not carry the eToro logger. This is NOT evidence of zero\n"
            "  traffic and must not be reported as headroom.",
            file=out,
        )
        return

    first, last = min(r.ts for r in records), max(r.ts for r in records)
    print(f"\n  merged window: {_fmt_dt(first)} .. {_fmt_dt(last)}", file=out)
    print(f"  malformed lines across all inputs: {malformed}", file=out)
    print(
        f"\n  {'lane':18s} {'src':22s} {'env':6s} {'attempts':>9s} {'429':>5s} {'err':>5s} "
        f"{'max/60s':>8s} {'budget':>7s} {'wait_s':>9s}",
        file=out,
    )
    groups: dict[tuple[str, str, str], list[ObservedAttempt]] = {}
    for record in records:
        groups.setdefault((record.lane, record.src, record.env), []).append(record)
    for (lane, src, env), rows in sorted(groups.items()):
        budget = lanes.LANES[lane].documented_per_minute if lane in lanes.LANES else None
        print(
            f"  {lane:18s} {src:22s} {env:6s} {len(rows):9d} "
            f"{sum(1 for r in rows if r.status == '429'):5d} "
            f"{sum(1 for r in rows if r.err != '-'):5d} "
            f"{max_rolling_window([r.ts for r in rows]):8d} "
            f"{(str(budget) if budget is not None else '-'):>7s} "
            f"{sum(r.wait_s for r in rows):9.1f}",
            file=out,
        )
    print(
        "\n  ⚠ 'max/60s' is per (lane, src, env) and is therefore a LOWER bound on what the\n"
        "    lane saw: a lane reached from two processes is only bounded by summing them,\n"
        "    which is what step 2's open clause 2 asks. 'wait_s' is SECONDS and is not the\n"
        "    counterpart of the requests bound -- 'max/60s' is.\n"
        "  ⚠ 'budget' is DOCUMENTED per-minute, never enforced-and-measured.",
        file=out,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--since", help="ISO timestamp; restrict M2/M3 to fires at or after it")
    ap.add_argument("--log", type=Path, default=DEFAULT_LOG, help="jobs-daemon log to parse for M4")
    ap.add_argument(
        "--request-log",
        type=Path,
        action="append",
        help="eToro request artefact to tabulate (M6). Repeatable; pass one per process.",
    )
    ap.add_argument("--out", type=Path, help="also write the census to this path")
    args = ap.parse_args()

    buffer: list[str] = []

    class _Tee:
        def write(self, s: str) -> int:
            buffer.append(s)
            return sys.stdout.write(s)

        def flush(self) -> None:
            sys.stdout.flush()

    out: TextIO = _Tee()  # type: ignore[assignment]

    def _write_out() -> None:
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text("".join(buffer))
            print(f"\nwritten: {args.out}", file=sys.stdout)

    # M6 is pure log parsing and deliberately needs no database: the artefact can be
    # tabulated anywhere the log files are, including off this box.  It still goes
    # through the tee, so ``--out`` cannot silently leave a stale report on disk.
    if args.request_log:
        m6_observed(out, args.request_log)
        _write_out()
        return 0

    with psycopg.connect(settings.database_url) as conn:
        # One snapshot for every arm so the arms cannot disagree about the corpus, and
        # read-only so nothing here can write even by accident.
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        cur = conn.cursor()
        cur.execute("select now()")
        (cutoff,) = _fetch_one(cur)

        print("#2946 step 2 -- eToro quota load census", file=out)
        print(f"git={_git_sha()}  db_snapshot_at={_fmt_dt(cutoff)} (REPEATABLE READ, read-only)", file=out)
        print(
            f"lane map VERIFIED_ON={lanes.VERIFIED_ON} — per-endpoint numbers PINNED against the\n"
            "  portal's OpenAPI document (#2946 item 4, tests/fixtures/etoro/); the GENERAL\n"
            "  rate-limit page behind `general_tier_per_minute` is still a dated transcript.",
            file=out,
        )
        if args.since:
            print(f"M2/M3 restricted to fires at or after {args.since}", file=out)
        print(
            "\n⚠⚠ NO eToro lane has a per-request artefact. strategy_core_eligibility_proofs\n"
            "   is NOT a request log: observed_at defaults to now() = TRANSACTION START and\n"
            "   the insert runs after the HTTP call inside a lock (sql/346:47,\n"
            "   scheduler.py:6316); failed requests write no row (etoro_broker.py:867,\n"
            "   strategy_core_eligibility.py:273); other callers write none at all\n"
            "   (strategy_paper_executor.py:1241, strategy_position_manager.py:408).",
            file=out,
        )

        a1_pacing(out)
        m2_derived(conn, out, since=args.since)
        m3_concurrency(conn, out, since=args.since)
        m4_retryables(conn, out, log_path=args.log)
        m5_reconciliation(conn, out)
        print_uncounted(out)

    _write_out()
    return 0


if __name__ == "__main__":  # pragma: no cover - script entry point
    raise SystemExit(main())
