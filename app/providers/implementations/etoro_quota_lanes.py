# app/providers/implementations/etoro_quota_lanes.py
"""eToro documented quota families — the lane map (#2946 items 1 + 3).

Census of the live portal on ``VERIFIED_ON``.  This module is DATA ONLY: it
holds no client, throttles nothing and is imported by tests rather than by the
request path.  It exists so the lane map is a table a test can check instead of
prose that goes stale.

Read ``docs/proposals/execution/2026-09-13-etoro-quota-lane-map.md`` before
changing anything here — it carries the evidence, the four findings and the
list of claims Codex killed at checkpoint 1.

Three things about this table that are easy to get wrong:

* **Two readings per limit, deliberately.**  The portal's general rate-limit
  page and its per-endpoint pages disagree, and the portal states NO precedence
  between them.  So a lane carries the per-endpoint number
  (``documented_per_minute``) and each call site carries the general page's
  tier (``general_tier_per_minute``) — the general page classifies by OPERATION
  ("executes trades, modifies state, or runs heavier queries"), not by family,
  so the two readings do not live at the same level.  We throttle against the
  LOWER of the two and never spend the difference.
* **A call EXPRESSION is not a call SITE.**  ``_submit`` reaches three distinct
  paths and ``place_order`` alone reaches two, so ``CALL_SITES`` is longer than
  ``EXPRESSION_COUNTS``.  The drift test checks the expression counts; the lane
  assignments are what a human has to re-read when it fails.
* **Documentation proves a number is DOCUMENTED, never that it is ENFORCED.**
  Nothing here is a measurement.  #2946 step 2 is the measurement.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal

# Date the portal was read.  Everything below is "documented as read on this
# date" — WebFetch returns a rendered reading, not raw bytes, and no response
# hash was captured, so this is a dated transcript and not a pinned artefact.
#
# ⚠ Since #2946 item 4 that is true of the GENERAL page only.  The per-endpoint
# numbers — every lane's `documented_per_minute`, its window, its scope and its
# pool membership — are now cross-checked offline against the portal's OpenAPI
# document, which carries a structured `x-ratelimit` on all 177 operations and is
# committed with a content hash (`tests/fixtures/etoro/`,
# `tests/test_2946_openapi_ratelimit_census.py`).
#
# ⚠⚠ That artefact is a CROSS-CHECK: it is eToro's document and this is our table.
# Never regenerate this file from it — doing so turns the check into an identity
# and leaves the census unguarded (see the prevention log, "a derivation can
# silently turn a comparison test into an identity test").
VERIFIED_ON = date(2026, 9, 13)

PORTAL_GENERAL_RATE_LIMIT_PAGE = "https://api-portal.etoro.com/core/getting-started/rate-limits"

LaneScope = Literal["dedicated", "shared", "default"]
# "witnessed" -> demo and real memberships were enumerated and are disjoint.
# "neutral"   -> the family has no environment segment at all (market data).
# "spans"     -> one family covers both environments (the default quota).
EnvSeparation = Literal["witnessed", "neutral", "spans"]


def min_interval_for_stamps(window_s: float, max_stamps: int) -> float:
    """Smallest spacing whose ROLLING-window request count is ``<= max_stamps``.

    ⚠ NOT ``window_s / max_stamps``, and the difference is one request.  A caller
    spaced at ``i`` fires at 0, i, 2i … so a window-long ROLLING window holds
    ``floor(window_s / i) + 1`` stamps -- one MORE than sustained throughput, because a
    rolling quota counts stamps and the first one is free.  Compliance therefore needs
    ``floor(window_s / i) <= max_stamps - 1``, which holds for every
    ``i > window_s / max_stamps``.

    That bound is OPEN -- ``window_s / max_stamps`` itself places ``max_stamps + 1`` --
    so the smallest value expressible from the budget that satisfies it is
    ``window_s / (max_stamps - 1)``.

    ⚠ The returned interval places ``max_stamps`` requests, or ``max_stamps - 1`` when
    binary rounding makes ``window_s / result`` land a hair under ``max_stamps - 1``.
    Never MORE: exactly, ``window_s / result == max_stamps - 1``, and the float result is
    within an ULP of that, while the threshold it must stay under is a whole request
    away.  The guarantee is the ``<=``; the exact count is not pinned and callers must
    not assume it (an earlier draft of this docstring claimed the count exactly and was
    wrong at lane B, where ``60 / 18`` rounds up and yields 18 requests, not 19).

    #2946 step 2 measured the consequence: lane B at 3.2 s places 19 requests, not the
    18.75 sustained throughput suggests.  Before this helper existed,
    ``CallSite.conservative_min_interval_s`` returned ``60 / 20 = 3.0`` for that lane and
    a floor of exactly 3.0 would have passed the floor guard at 21 requests/min.
    """
    if max_stamps < 2:
        raise ValueError("max_stamps must be at least 2 to express an interval")
    return window_s / (max_stamps - 1)


@dataclass(frozen=True)
class QuotaLane:
    """One documented eToro quota family."""

    key: str
    title: str
    scope: LaneScope
    documented_per_minute: int
    env_separation: EnvSeparation
    # Number of portal pages that independently enumerated this family.  1 means
    # single-witness.  Even 2 only corroborates transcription: both pages are
    # generated from one upstream annotation, and each list omits its own
    # endpoint, so "agreement" means agreement on ``{self} u peers``.
    witnesses: int
    source_url: str
    window_s: int = 60

    @property
    def min_interval_s(self) -> float:
        """Fastest sustained CADENCE for a caller owning the whole pool.

        ⚠ Sustained throughput, which is NOT the same question as "is a caller paced at
        this interval inside the budget".  It is not: a caller spaced at exactly this
        value places ``documented_per_minute + 1`` stamps in a rolling window.  Use
        ``min_interval_for_stamps`` for the pacing question -- this property is kept for
        what it says, the cadence, and is deliberately not routed through it.
        """
        return self.window_s / self.documented_per_minute


LANES: dict[str, QuotaLane] = {
    "A_order_write": QuotaLane(
        key="A_order_write",
        title="order-write pool (11 members incl. itself)",
        scope="shared",
        documented_per_minute=20,
        env_separation="witnessed",
        witnesses=2,
        source_url="https://api-portal.etoro.com/api-reference/trading--demo/submit-an-order-for-asynchronous-processing",
    ),
    "B_eligibility": QuotaLane(
        key="B_eligibility",
        title="trading eligibility (dedicated, pooled with nothing)",
        scope="dedicated",
        documented_per_minute=20,
        env_separation="witnessed",
        witnesses=1,
        source_url="https://api-portal.etoro.com/api-reference/trading--demo/check-instrument-trading-eligibility",
    ),
    "C_what_if_costs": QuotaLane(
        key="C_what_if_costs",
        title="what-if cost breakdown (dedicated, pooled with nothing)",
        scope="dedicated",
        documented_per_minute=20,
        env_separation="witnessed",
        witnesses=1,
        source_url="https://api-portal.etoro.com/api-reference/trading--demo/get-what-if-trading-cost-breakdown",
    ),
    "D_order_info": QuotaLane(
        key="D_order_info",
        title="order-info pool (3 members)",
        scope="shared",
        documented_per_minute=60,
        env_separation="witnessed",
        witnesses=2,
        source_url="https://api-portal.etoro.com/api-reference/trading--demo/get-order-information-and-position-details",
    ),
    "E_account_read": QuotaLane(
        key="E_account_read",
        title="account-read pool (pnl / portfolio / instrument-breakdown)",
        scope="shared",
        documented_per_minute=60,
        env_separation="witnessed",
        witnesses=1,
        source_url="https://api-portal.etoro.com/api-reference/trading--demo/get-account-pnl-and-portfolio-details",
    ),
    "F_market_data": QuotaLane(
        key="F_market_data",
        title="market-data pool (11 members)",
        scope="shared",
        documented_per_minute=120,
        # Market-data paths carry no environment segment at all.  Partitioning
        # this lane by env would split one budget into two phantom budgets.
        env_separation="neutral",
        witnesses=2,
        source_url="https://api-portal.etoro.com/api-reference/market-data/get-instrument-candle-history",
    ),
    "G_default_shared": QuotaLane(
        key="G_default_shared",
        title="default shared quota (membership UNENUMERATED)",
        scope="default",
        documented_per_minute=60,
        # One family across both environments, plus /api/v1/me, plus an open set
        # outside trading entirely.
        env_separation="spans",
        witnesses=2,
        source_url="https://api-portal.etoro.com/api-reference/trading--demo/list-trading-history",
    ),
}


@dataclass(frozen=True)
class CallSite:
    """One endpoint template this repo reaches, and the lane it draws on."""

    module: str
    method: str
    client_attr: str
    verb: str
    demo_path: str
    lane: str
    # The general rate-limit page's tier for this OPERATION.  It classifies by
    # what the call does, not by family, which is why it lives here and not on
    # the lane.  20 = "executes trades, modifies state, or runs heavier
    # queries"; 60 = "most standard GET requests".
    general_tier_per_minute: int
    # Set when the general page's prose category arguably covers this endpoint
    # but the per-endpoint page disagrees.  Taking the smaller NUMBER does not
    # resolve which CATEGORY an endpoint is in, so the ambiguity is recorded
    # rather than decided.
    membership_ambiguous: bool = False

    @property
    def conservative_per_minute(self) -> int:
        return min(LANES[self.lane].documented_per_minute, self.general_tier_per_minute)

    @property
    def conservative_min_interval_s(self) -> float:
        """Slowest-reading floor this call site must respect, by the ROLLING-window rule.

        ⚠ Was ``window_s / conservative_per_minute`` until #2946 step 3, which is off by
        one request: at lane B that returned 3.0 s, and a floor of exactly 3.0 s places
        21 requests against a documented 20.  The floor guard
        (``tests/test_etoro_quota_lanes.py::test_configured_floor_is_at_least_the_
        conservative_minimum``) is the only live consumer, so the off-by-one was an
        admission gap in the one test that checks real configuration.
        """
        return min_interval_for_stamps(LANES[self.lane].window_s, self.conservative_per_minute)


_BROKER = "app/providers/implementations/etoro_broker.py"
_MARKET = "app/providers/implementations/etoro.py"

CALL_SITES: tuple[CallSite, ...] = (
    # --- lane A: order-write pool -------------------------------------------
    CallSite(
        _BROKER,
        "place_order",
        "_http_write",
        "POST",
        "/api/v1/trading/execution/demo/market-open-orders/by-amount",
        "A_order_write",
        20,
    ),
    CallSite(
        _BROKER,
        "place_order",
        "_http_write",
        "POST",
        "/api/v1/trading/execution/demo/market-open-orders/by-units",
        "A_order_write",
        20,
    ),
    CallSite(
        _BROKER,
        "close_position",
        "_http_write",
        "POST",
        "/api/v1/trading/execution/demo/market-close-orders/positions/{positionId}",
        "A_order_write",
        20,
    ),
    CallSite(
        _BROKER,
        "place_demo_strategy_order",
        "_http_write",
        "POST",
        "/api/v2/trading/execution/demo/orders",
        "A_order_write",
        20,
    ),
    CallSite(
        _BROKER,
        "place_demo_core_order",
        "_http_write",
        "POST",
        "/api/v2/trading/execution/demo/orders",
        "A_order_write",
        20,
    ),
    CallSite(
        _BROKER,
        "close_demo_strategy_position",
        "_http_write",
        "POST",
        "/api/v1/trading/execution/demo/market-close-orders/positions/{positionId}",
        "A_order_write",
        20,
    ),
    # --- lanes B and C: dedicated budgets, queued behind lane A's clock ------
    CallSite(
        _BROKER,
        "check_instrument_eligibility",
        "_http_write",
        "POST",
        "/api/v2/trading/info/demo/eligibility",
        "B_eligibility",
        20,
    ),
    CallSite(
        _BROKER, "get_what_if_costs", "_http_write", "POST", "/api/v2/trading/info/demo/costs", "C_what_if_costs", 20
    ),
    # --- lane G on the WRITE clock: TP/SL edit -------------------------------
    CallSite(
        _BROKER,
        "edit_demo_strategy_position",
        "_http_write",
        "PATCH",
        "/api/v2/trading/demo/positions/{positionId}",
        "G_default_shared",
        20,
    ),
    # --- lane D: order-info pool --------------------------------------------
    CallSite(
        _BROKER,
        "get_demo_close_order",
        "_http_read",
        "GET",
        "/api/v1/trading/info/demo/close-orders/{orderId}",
        "D_order_info",
        60,
    ),
    CallSite(
        _BROKER,
        "get_order_status",
        "_http_read",
        "GET",
        "/api/v1/trading/info/demo/orders/{orderId}",
        "D_order_info",
        60,
    ),
    CallSite(
        _BROKER, "lookup_order", "_http_read", "GET", "/api/v2/trading/info/demo/orders:lookup", "D_order_info", 60
    ),
    # --- lane E: account-read pool ------------------------------------------
    CallSite(
        _BROKER, "get_portfolio", "_http_read", "GET", "/api/v1/trading/info/demo/portfolio", "E_account_read", 60
    ),
    CallSite(
        _BROKER, "get_account_risk_snapshot", "_http_read", "GET", "/api/v1/trading/info/demo/pnl", "E_account_read", 60
    ),
    # --- lane G on its OWN clock: trading history ----------------------------
    # The per-endpoint page says 60/min default shared; the general page's
    # 20/min tier explicitly names "detailed user trade history queries".  The
    # method paginates in a `while True` loop, so one logical call can issue
    # back-to-back requests -- which is why it does not ride `_http_read` at
    # 1.1s.  #2946 step 3 item 1 gave it `_http_history`, whose floor is DERIVED
    # from this very entry (`etoro_broker._ETORO_HISTORY_INTERVAL_S`).
    #
    # ⚠ That derivation makes the floor test tautological for this site.  The
    # live guard on the number is
    # `tests/test_etoro_quota_lanes.py::test_history_floor_reserves_one_request_
    # of_lane_g_headroom`, which asserts the rolling-window COUNT, not the
    # expression.  Do not delete it as redundant.
    CallSite(
        _BROKER,
        "get_trade_history",
        "_http_history",
        "GET",
        "/api/v1/trading/info/trade/demo/history",
        "G_default_shared",
        20,
        membership_ambiguous=True,
    ),
    # --- lane F: market data (7 templates, 8 call expressions) --------------
    CallSite(
        _MARKET, "get_tradable_instruments", "_http", "GET", "/api/v1/market-data/instruments", "F_market_data", 60
    ),
    CallSite(_MARKET, "get_broad_market_snapshot", "_http", "GET", "/api/v1/market-data/search", "F_market_data", 60),
    CallSite(
        _MARKET, "get_instrument_types", "_http", "GET", "/api/v1/market-data/instrument-types", "F_market_data", 60
    ),
    CallSite(
        _MARKET, "get_stocks_industries", "_http", "GET", "/api/v1/market-data/stocks-industries", "F_market_data", 60
    ),
    CallSite(_MARKET, "get_exchanges", "_http", "GET", "/api/v1/market-data/exchanges", "F_market_data", 60),
    # get_daily_candles and get_intraday_candles are two call expressions on ONE
    # template — this is why EXPRESSION_COUNTS is 8 while lane F has 7 entries.
    CallSite(
        _MARKET,
        "get_daily_candles / get_intraday_candles",
        "_http",
        "GET",
        "/api/v1/market-data/instruments/{instrumentId}/history/candles/{direction}/{interval}/{candlesCount}",
        "F_market_data",
        60,
    ),
    CallSite(_MARKET, "get_quotes", "_http", "GET", "/api/v1/market-data/instruments/rates", "F_market_data", 60),
)

# Throttled call EXPRESSIONS per (module, client attribute), measured by AST on
# 2026-09-13.  Deliberately separate from CALL_SITES because one expression can
# serve several templates.  A new endpoint moves these numbers and fails the
# drift test until its lane is recorded above.
EXPRESSION_COUNTS: dict[tuple[str, str], int] = {
    (_BROKER, "_http_read"): 5,
    (_BROKER, "_http_write"): 7,
    # One expression, inside a `while True` — so this count says nothing about
    # how many REQUESTS one logical call issues.  That is the whole reason the
    # endpoint has its own client.
    (_BROKER, "_http_history"): 1,
    (_MARKET, "_http"): 8,
}

# Which module-level constant configures each client's floor.  The floor VALUES
# are deliberately NOT copied here: the test resolves them by ``getattr`` on the
# real provider module, so it compares live configuration against the documented
# limit.  Copying the numbers would make the floor test compare this table with
# itself and pass no matter what the providers do.
FLOOR_CONSTANT_NAMES: dict[tuple[str, str], tuple[str, str]] = {
    (_BROKER, "_http_read"): ("app.providers.implementations.etoro_broker", "_ETORO_READ_INTERVAL_S"),
    (_BROKER, "_http_write"): ("app.providers.implementations.etoro_broker", "_ETORO_WRITE_INTERVAL_S"),
    # ⚠ This one is DERIVED from the table above rather than hand-chosen, so the
    # floor test compares the table with itself here and cannot fail.  See the
    # note on the `get_trade_history` call site for the test that can.
    (_BROKER, "_http_history"): ("app.providers.implementations.etoro_broker", "_ETORO_HISTORY_INTERVAL_S"),
    (_MARKET, "_http"): ("app.providers.implementations.etoro", "_ETORO_READ_INTERVAL_S"),
}


@dataclass(frozen=True)
class UnthrottledCall:
    """A raw ``httpx`` call that bypasses every ResilientClient throttle."""

    module: str
    line_hint: int
    verb: str
    path: str
    lane: str
    note: str


# Four of these, in two modules and two lanes.  A coordinator that misses them
# cannot claim to bound the lanes they draw on.  Frequency is UNMEASURED — do
# not restate "low frequency" as if it were observed.
KNOWN_UNTHROTTLED: tuple[UnthrottledCall, ...] = (
    UnthrottledCall(
        "app/api/broker_credentials.py", 598, "GET", "/api/v1/me", "G_default_shared", "credential validation level 1"
    ),
    UnthrottledCall(
        "app/api/broker_credentials.py",
        641,
        "GET",
        "/api/v1/trading/info/{env}/pnl",
        "E_account_read",
        "credential validation level 2 — runs ONLY after level 1 succeeds, so 'up to two per invocation'",
    ),
    UnthrottledCall(
        "app/api/_debug_ws.py",
        96,
        "GET",
        "/api/v1/market-data/instruments/{instrumentId}/history/candles/...",
        "F_market_data",
        "debug probe; router enabled in development, which is where the jobs daemon runs",
    ),
    UnthrottledCall(
        "app/api/_debug_ws.py", 155, "GET", "/api/v1/market-data/instruments", "F_market_data", "debug probe; see above"
    ),
)

# Raw httpx request expressions per module, measured by AST on 2026-09-13.
RAW_HTTPX_EXPRESSION_COUNTS: dict[str, int] = {
    "app/api/broker_credentials.py": 2,
    "app/api/_debug_ws.py": 2,
}


@dataclass(frozen=True)
class PathDrift:
    """A real-environment path our code builds that the portal does not document.

    Classified ``unknown``, never ``broken``: undocumented is not nonexistent,
    aliases may exist, and the portal CONTRADICTS ITSELF on the v1 real shape
    (the real pnl page's own path carries ``/real/`` while the sibling it
    enumerates, ``portfolio``, does not).  No path shape can be DERIVED from the
    documentation; settling it needs one informational call per shape on real
    credentials, which is a broker-touching acceptance the autonomy loop may not
    run.  Belongs to real-env enablement under #2843 / #2844.
    """

    method: str
    documented_real_path: str
    we_build_for_real: str
    kind: Literal["missing_env_segment", "extra_env_segment"]


KNOWN_PATH_DRIFT: tuple[PathDrift, ...] = (
    PathDrift(
        "get_account_risk_snapshot", "/api/v1/trading/info/real/pnl", "/api/v1/trading/info/pnl", "missing_env_segment"
    ),
    PathDrift(
        "get_order_status",
        "/api/v1/trading/info/real/orders/{orderId}",
        "/api/v1/trading/info/orders/{orderId}",
        "missing_env_segment",
    ),
    PathDrift(
        "check_instrument_eligibility",
        "/api/v2/trading/info/eligibility",
        "/api/v2/trading/info/real/eligibility",
        "extra_env_segment",
    ),
    PathDrift(
        "get_what_if_costs", "/api/v2/trading/info/costs", "/api/v2/trading/info/real/costs", "extra_env_segment"
    ),
    PathDrift(
        "lookup_order",
        "/api/v2/trading/info/orders:lookup",
        "/api/v2/trading/info/real/orders:lookup",
        "extra_env_segment",
    ),
)

# Call sites whose configured floor is BELOW the conservative reading, accepted
# with a stated reason rather than silently passing the floor test.  Keep this
# dict as short as it is; every entry is a known gap.
#
# EMPTY since #2946 step 3 item 1.  Its only entry was `get_trade_history`,
# which rode `_http_read` at 1.1s against a 3.158s conservative reading; it now
# has its own client and complies.  `test_accepted_floor_exceptions_are_real_
# exceptions_with_a_reason` is what forced the deletion — an entry naming a site
# that no longer violates its floor fails that test.
ACCEPTED_FLOOR_EXCEPTIONS: dict[tuple[str, str], str] = {}
