"""#2946 step 3 item 3 — per-request eToro accounting.

Two halves:

* the CLASSIFIER, which must resolve every path in the step-1 lane tables (and only
  those) to the lane the portal documented for it; and
* the OBSERVER, which must see every HTTP attempt ``ResilientClient`` issues --
  including the final retryable one, which raises rather than returning.

⚠ The classifier tests feed SUBSTITUTED urls, never raw templates.  A template fed to
a template-matcher is a test that passes for the wrong reason: ``{env}`` in the input
is a literal brace, and the raw pnl entry's own template does not match its own
pattern.  What production passes is ``/api/v1/trading/info/demo/pnl``.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import httpx
import pytest

from app.providers.implementations.etoro_quota_lanes import (
    CALL_SITES,
    KNOWN_PATH_DRIFT,
    KNOWN_UNTHROTTLED,
)
from app.providers.implementations.etoro_request_log import (
    LINE_PREFIX,
    ROUTES,
    UNCLASSIFIED,
    attempt_observer,
    etoro_lane_counters,
    issue_raw_request,
    lane_for_request,
    normalise_path,
    record_etoro_request,
    reset_etoro_lane_counters,
)
from app.providers.resilient_client import ResilientClient

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture(autouse=True)
def _clean_counters() -> Generator[None]:
    reset_etoro_lane_counters()
    yield
    reset_etoro_lane_counters()


def _sample_url(template: str) -> str:
    """Substitute a documented template into a url a provider would actually build."""
    if template.endswith("..."):
        template = template[: -len("...")] + "asc/OneMinute/90"
    template = template.replace("{env}", "demo")
    return re.sub(r"\{[^{}]*\}", "424242", template)


def _env_variants(url: str) -> tuple[str, str, str]:
    """The three shapes our providers build: demo, real, and no env segment at all."""
    return (url, url.replace("/demo", "/real", 1), url.replace("/demo", "", 1))


# ---------------------------------------------------------------------------
# Classifier — acceptance 1, 2, 3
# ---------------------------------------------------------------------------


def test_every_documented_call_site_classifies_to_its_own_lane() -> None:
    """Acceptance 1, iterated off the table so a new endpoint cannot be forgotten."""
    mismatches = [
        (site.method, url, site.lane, lane_for_request(site.verb, url).lane)
        for site in CALL_SITES
        for url in _env_variants(_sample_url(site.demo_path))
        if lane_for_request(site.verb, url).lane != site.lane
    ]
    assert mismatches == []


def test_every_known_unthrottled_site_classifies_to_its_own_lane() -> None:
    """The raw-``httpx`` half.

    ⚠ ``KNOWN_UNTHROTTLED`` stores its path on ``.path`` while ``CALL_SITES`` uses
    ``.demo_path``.  Reading ``.demo_path`` off both raises ``AttributeError`` rather
    than silently emptying the table, but the accessor is the thing to keep an eye on:
    this half is exactly the traffic step 2 reported as UNCOUNTED.
    """
    mismatches = [
        (raw.path, raw.lane, lane_for_request(raw.verb, _sample_url(raw.path)).lane)
        for raw in KNOWN_UNTHROTTLED
        if lane_for_request(raw.verb, _sample_url(raw.path)).lane != raw.lane
    ]
    assert mismatches == []


def test_real_environment_path_drift_classifies_to_the_documented_lane() -> None:
    """Both drift kinds — the path we build for real, and the one the portal documents.

    This is the test that would fail if the optional env segment were written
    ``/(?:demo|real)?/``: the ``missing_env_segment`` rows would need a literal ``//``
    and every real-environment request would fall out as ``unclassified``.
    """
    by_method = {site.method: site for site in CALL_SITES}
    for drift in KNOWN_PATH_DRIFT:
        site = by_method[drift.method]
        for path in (drift.we_build_for_real, drift.documented_real_path):
            assert lane_for_request(site.verb, path).lane == site.lane, (drift.method, path)


def test_no_documented_path_resolves_to_two_different_lanes() -> None:
    """Acceptance 2.

    Several paths match more than one TEMPLATE by design (the raw pnl probe and the
    broker's own pnl call site; the two submitters on the v2 orders path; the specific
    candle template and the debug prefix template).  That is harmless -- resolution is
    first-match and a request is recorded once -- but two different LANES would mean
    the recorded lane depends on table order, which is not a measurement.
    """
    samples = [(s.verb.upper(), _sample_url(s.demo_path)) for s in CALL_SITES]
    samples += [(r.verb.upper(), _sample_url(r.path)) for r in KNOWN_UNTHROTTLED]
    for verb, url in samples:
        for variant in _env_variants(url):
            lanes = {rt.lane for rt in ROUTES if rt.verb == verb and rt.pattern.fullmatch(variant)}
            assert len(lanes) <= 1, (verb, variant, lanes)


def test_env_placeholder_is_not_an_open_wildcard() -> None:
    """``{env}`` is ``(?:demo|real)``, never ``[^/]+``.

    The raw pnl probe is written ``/api/v1/trading/info/{env}/pnl``.  With an open
    wildcard there, any third path segment would be read as an account-read request.
    """
    assert lane_for_request("GET", "/api/v1/trading/info/staging/pnl").lane == UNCLASSIFIED
    assert lane_for_request("GET", "/api/v1/trading/info/demo/pnl").lane == "E_account_read"


def test_prefix_template_accepts_the_segments_it_stands_in_for() -> None:
    """``KNOWN_UNTHROTTLED``'s debug candle entry ends in ``...``.

    Escaping the marker would make that entry match nothing; leaving it unescaped
    would make it match any three characters.  It is a declared prefix template.
    """
    assert (
        lane_for_request("GET", "/api/v1/market-data/instruments/1699/history/candles/asc/OneMinute/90").lane
        == "F_market_data"
    )
    assert lane_for_request("GET", "/api/v1/market-data/instruments/1699/history").lane == UNCLASSIFIED


def test_verb_is_part_of_the_match() -> None:
    assert lane_for_request("GET", "/api/v1/me").lane == "G_default_shared"
    assert lane_for_request("POST", "/api/v1/me").lane == UNCLASSIFIED


def test_unknown_path_is_unclassified_and_never_guessed() -> None:
    match = lane_for_request("GET", "/api/v1/trading/info/demo/something-new")
    assert match.lane == UNCLASSIFIED
    assert match.template is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("/api/v1/trading/info/demo/portfolio", "/api/v1/trading/info/demo/portfolio"),
        ("/api/v1/trading/info/demo/portfolio/", "/api/v1/trading/info/demo/portfolio"),
        ("/api/v1//trading///info/demo/portfolio", "/api/v1/trading/info/demo/portfolio"),
        ("/api/v1/trading/info/demo/portfolio?page=2", "/api/v1/trading/info/demo/portfolio"),
        ("/api/v1/trading/info/demo/portfolio#frag", "/api/v1/trading/info/demo/portfolio"),
        ("https://public-api.etoro.com/api/v1/me", "/api/v1/me"),
        # A colon inside a segment must not be read as a URL scheme.
        ("/api/v2/trading/info/demo/orders:lookup", "/api/v2/trading/info/demo/orders:lookup"),
    ],
)
def test_normalise_path(raw: str, expected: str) -> None:
    assert normalise_path(raw) == expected


# ---------------------------------------------------------------------------
# Counters + the emitted line
# ---------------------------------------------------------------------------


def test_counters_split_by_lane_src_and_env() -> None:
    """An eToro quota is per user key and per environment.

    A single lane total that merges demo and real traffic is not a number any budget
    can be compared against, so the key carries both labels.
    """
    record_etoro_request(verb="GET", url="/api/v1/trading/info/demo/pnl", status=200, src="broker_read", env="demo")
    record_etoro_request(verb="GET", url="/api/v1/trading/info/real/pnl", status=200, src="broker_read", env="real")
    record_etoro_request(verb="GET", url="/api/v1/trading/info/demo/pnl", status=429, src="broker_read", env="demo")
    record_etoro_request(verb="GET", url="/api/v1/trading/info/demo/pnl", status=None, src="broker_read", env="demo")

    counters = etoro_lane_counters()
    demo = counters[("E_account_read", "broker_read", "demo")]
    assert (demo.attempts, demo.http_429, demo.transport_errors) == (3, 1, 1)
    assert counters[("E_account_read", "broker_read", "real")].attempts == 1


def test_line_carries_its_own_timestamp_and_schema_version(caplog: pytest.LogCaptureFixture) -> None:
    """The reader parses ``ts=`` out of the LINE, not out of the handler's prefix.

    The API process and the jobs daemon format logs differently and the API's format
    carries no timestamp at all, so a handler-parsing reader cannot merge them -- and
    merging them is the whole of step 2's open clause 2.
    """
    with caplog.at_level(logging.INFO, logger="app.etoro.requests"):
        record_etoro_request(
            verb="POST",
            url="/api/v2/trading/info/demo/eligibility",
            status=200,
            src="broker_write",
            env="demo",
            pre_request_wait_s=3.333,
        )
    line = caplog.messages[-1]
    assert line.startswith(f"{LINE_PREFIX} v=1 ts=")
    assert " lane=B_eligibility " in line
    assert " src=broker_write " in line
    assert " wait_s=3.333 " in line
    assert line.endswith(" path=/api/v2/trading/info/demo/eligibility")


def test_unclassified_line_carries_the_url_verbatim(caplog: pytest.LogCaptureFixture) -> None:
    """Acceptance 3 — the one case where the input shape is unknown keeps it in full."""
    with caplog.at_level(logging.INFO, logger="app.etoro.requests"):
        record_etoro_request(verb="GET", url="/api/v1/brand-new?x=1", status=200, src="market", env="demo")
    line = caplog.messages[-1]
    assert f" lane={UNCLASSIFIED} " in line
    assert line.endswith(" raw=/api/v1/brand-new?x=1")


# ---------------------------------------------------------------------------
# Observer wiring in ResilientClient — acceptance 4-10
# ---------------------------------------------------------------------------


def _response(status: int) -> httpx.Response:
    return httpx.Response(status_code=status, request=httpx.Request("GET", "https://example.com/x"))


def _client(
    responses: list[httpx.Response | Exception],
    *,
    max_retries: int = 3,
    src: str = "broker_read",
) -> tuple[ResilientClient, MagicMock]:
    mock_httpx = MagicMock(spec=httpx.Client)
    mock_httpx.build_request.return_value = httpx.Request("GET", "https://example.com/x")
    mock_httpx.send.side_effect = responses
    client = ResilientClient(
        mock_httpx,
        max_retries=max_retries,
        backoff_schedule=(0.0, 0.0, 0.0),
        on_attempt=attempt_observer(src, "demo"),
    )
    return client, mock_httpx


def test_a_retried_429_records_two_attempts_on_the_right_lane() -> None:
    """Acceptance 4.  A retry places its own stamp in a rolling window, so it is its
    own attempt -- counting logical calls would undercount exactly the traffic a
    quota question is about."""
    client, _ = _client([_response(429), _response(200)])
    client.get("/api/v1/trading/info/demo/portfolio")

    counter = etoro_lane_counters()[("E_account_read", "broker_read", "demo")]
    assert (counter.attempts, counter.http_429) == (2, 1)


def test_exhausted_retries_record_the_final_raising_attempt() -> None:
    """Acceptance 5 — the attempt that raises is the one a loop-tail hook would miss."""
    client, _ = _client([_response(429)] * 4, max_retries=3)
    with pytest.raises(httpx.HTTPStatusError):
        client.get("/api/v1/trading/info/demo/portfolio")

    counter = etoro_lane_counters()[("E_account_read", "broker_read", "demo")]
    assert (counter.attempts, counter.http_429) == (4, 4)


def test_max_retries_zero_records_exactly_one_attempt() -> None:
    """Acceptance 6."""
    client, _ = _client([_response(429)], max_retries=0)
    with pytest.raises(httpx.HTTPStatusError):
        client.get("/api/v1/trading/info/demo/portfolio")

    assert etoro_lane_counters()[("E_account_read", "broker_read", "demo")].attempts == 1


def test_transport_failure_records_an_attempt_and_re_raises() -> None:
    """Acceptance 7.

    Step 2's FINDING that "a transport failure writes no row and still spends quota"
    is exactly this case: nothing downstream sees it, so the record has to be here.
    """
    boom = httpx.ConnectTimeout("no route")
    client, _ = _client([boom], max_retries=0)
    with pytest.raises(httpx.ConnectTimeout):
        client.get("/api/v1/trading/info/demo/portfolio")

    counter = etoro_lane_counters()[("E_account_read", "broker_read", "demo")]
    assert (counter.attempts, counter.transport_errors, counter.http_429) == (1, 1, 0)


def test_a_build_request_failure_records_nothing() -> None:
    """Acceptance 8 — no request went out, so no attempt exists to record."""
    mock_httpx = MagicMock(spec=httpx.Client)
    mock_httpx.build_request.side_effect = ValueError("bad url")
    client = ResilientClient(mock_httpx, on_attempt=attempt_observer("broker_read", "demo"))
    with pytest.raises(ValueError, match="bad url"):
        client.get("/api/v1/trading/info/demo/portfolio")

    assert etoro_lane_counters() == {}


def test_no_observer_records_nothing_and_leaves_the_request_path_alone() -> None:
    """Acceptance 9 — every non-eToro provider keeps today's behaviour exactly."""
    mock_httpx = MagicMock(spec=httpx.Client)
    mock_httpx.build_request.return_value = httpx.Request("GET", "https://example.com/x")
    mock_httpx.send.side_effect = [_response(200)]
    client = ResilientClient(mock_httpx)

    assert client.get("/api/v1/trading/info/demo/portfolio").status_code == 200
    assert etoro_lane_counters() == {}


def test_an_observer_that_raises_cannot_fail_the_request() -> None:
    """Acceptance 10 — instrumentation is never load-bearing."""
    mock_httpx = MagicMock(spec=httpx.Client)
    mock_httpx.build_request.return_value = httpx.Request("GET", "https://example.com/x")
    mock_httpx.send.side_effect = [_response(200)]

    def _explode(_attempt: object) -> None:
        raise RuntimeError("observer is broken")

    client = ResilientClient(mock_httpx, on_attempt=_explode)
    assert client.get("/api/v1/trading/info/demo/portfolio").status_code == 200


def test_sec_on_429_is_untouched_by_the_new_hook() -> None:
    """eToro deliberately does NOT wire ``on_429``: ``on_attempt`` already sees every
    429, and wiring both would double-count.  SEC's counter keeps its own path."""
    from app.providers import sec_throttle_metrics as metrics

    mock_httpx = MagicMock(spec=httpx.Client)
    mock_httpx.build_request.return_value = httpx.Request("GET", "https://example.com/x")
    mock_httpx.send.side_effect = [_response(429), _response(200)]
    client = ResilientClient(
        mock_httpx, max_retries=1, backoff_schedule=(0.0,), on_429=metrics.incr_sec_429, on_attempt=None
    )

    before = metrics.sec_throttle_429_total()
    client.get("/cgi-bin/browse-edgar")
    assert metrics.sec_throttle_429_total() == before + 1
    assert etoro_lane_counters() == {}


def test_the_recorded_timestamp_is_issuance_not_completion() -> None:
    """⚠ A quota counts DISPATCHES.

    Stamping the record when ``send()`` returns would make the rolling-window count a
    function of response latency: two requests dispatched at 0s and 59s but completing
    at 0s and 61s would report a maximum of one per minute when the quota saw two.
    """
    seen: list[object] = []
    inside_send: list[datetime] = []

    def _send(request: httpx.Request) -> httpx.Response:
        inside_send.append(datetime.now(UTC))
        return httpx.Response(200, request=request)

    mock_httpx = MagicMock(spec=httpx.Client)
    mock_httpx.build_request.return_value = httpx.Request("GET", "https://example.com/x")
    mock_httpx.send.side_effect = _send

    client = ResilientClient(mock_httpx, on_attempt=seen.append)
    client.get("/api/v1/me")

    recorded_ts = seen[0].ts  # type: ignore[attr-defined]
    assert recorded_ts <= inside_send[0], "record timestamped after send() entered — that is completion time"


# ---------------------------------------------------------------------------
# Raw (unthrottled) call sites — acceptance 11
# ---------------------------------------------------------------------------


def test_issue_raw_request_records_a_non_200_before_the_caller_sees_it() -> None:
    mock_client = MagicMock(spec=httpx.Client)
    mock_client.get.return_value = _response(429)

    response = issue_raw_request(mock_client, "GET", "/api/v1/me", src="credential_validation", env="demo")

    assert response.status_code == 429
    counter = etoro_lane_counters()[("G_default_shared", "credential_validation", "demo")]
    assert (counter.attempts, counter.http_429) == (1, 1)


def test_issue_raw_request_records_a_transport_failure_and_re_raises() -> None:
    """A read timeout can arrive AFTER eToro received the request.

    The throttled path records these; the raw path has to as well, or its attempt total
    undercounts exactly the failure mode step 2 named.
    """
    mock_client = MagicMock(spec=httpx.Client)
    mock_client.get.side_effect = httpx.ReadTimeout("body never arrived")

    with pytest.raises(httpx.ReadTimeout):
        issue_raw_request(mock_client, "GET", "/api/v1/me", src="credential_validation", env="demo")

    counter = etoro_lane_counters()[("G_default_shared", "credential_validation", "demo")]
    assert (counter.attempts, counter.transport_errors) == (1, 1)


# ⚠ Acceptance 11's COVERAGE half — "every raw site goes through the helper, and no
# direct verb call has reappeared" — lives in
# ``tests/test_etoro_quota_lanes.py::test_raw_httpx_call_counts_match_the_known_bypass_list``.
# That guard already owned ``RAW_HTTPX_EXPRESSION_COUNTS`` and already resolved which
# locals are httpx clients; a second AST scan here would be the same assertion with a
# second copy of the scanner to keep in step.


def _emitted_line(caplog: pytest.LogCaptureFixture, **kwargs: object) -> str:
    with caplog.at_level(logging.INFO, logger="app.etoro.requests"):
        record_etoro_request(**kwargs)  # type: ignore[arg-type]
    return caplog.messages[-1]


def test_the_tabulator_reads_the_lines_this_module_emits(caplog: pytest.LogCaptureFixture) -> None:
    """Round-trip: the emitter and the reader must agree, or the artefact is decorative.

    Handler prefixes differ between the API process and the jobs daemon, so the parser
    is given a prefixed line here deliberately.
    """
    from scripts.measure_2946_quota_load import parse_request_log

    line = _emitted_line(
        caplog,
        verb="POST",
        url="/api/v2/trading/info/demo/eligibility",
        status=200,
        src="broker_write",
        env="demo",
        pre_request_wait_s=3.333,
    )
    records, malformed = parse_request_log([f"2026-09-13 19:00:00 INFO app.etoro.requests {line}"])

    assert malformed == 0
    assert len(records) == 1
    assert (records[0].lane, records[0].src, records[0].env) == ("B_eligibility", "broker_write", "demo")
    assert records[0].wait_s == pytest.approx(3.333)


def test_malformed_lines_are_counted_not_dropped() -> None:
    """Missing evidence must never be reported as no traffic.

    A line is claimed by its MARKER and only then validated.  Matching on the whole
    well-formed shape would let a truncated record fall into the "unrelated log line"
    bucket, so damaged evidence would read as zero traffic — the one error that would
    falsely close this ticket.
    """
    from scripts.measure_2946_quota_load import parse_request_log

    records, malformed = parse_request_log(
        [
            "unrelated log line",
            "etoro_request v=1 ts=not-a-timestamp lane=B_eligibility src=x env=demo "
            "status=200 err=- wait_s=0.000 attempt=0 path=/x",
            "etoro_request v=99 ts=2026-09-13T19:00:00+00:00 lane=B_eligibility src=x env=demo "
            "status=200 err=- wait_s=0.000 attempt=0 path=/x",
            # Truncated mid-line: has the marker, has no ts.
            "etoro_request v=1 lane=B_eligibility",
            # Truncated before the version.
            "etoro_request ",
        ]
    )
    assert records == []
    # Everything carrying the marker counts; the unrelated line does not, because it
    # never claimed to be one of ours.
    assert malformed == 4


def test_request_log_mode_honours_the_out_file(tmp_path: Path) -> None:
    """``--request-log`` with ``--out`` must not leave a stale report on disk."""
    import sys
    from unittest.mock import patch

    from scripts.measure_2946_quota_load import main

    log = tmp_path / "jobs.log"
    log.write_text(
        "etoro_request v=1 ts=2026-09-13T19:00:00+00:00 lane=B_eligibility src=broker_write "
        "env=demo verb=POST status=200 err=- wait_s=0.000 attempt=0 path=/api/v2/trading/info/demo/eligibility\n"
    )
    out = tmp_path / "nested" / "census.txt"

    argv = ["measure_2946_quota_load", "--request-log", str(log), "--out", str(out)]
    with patch.object(sys, "argv", argv):
        assert main() == 0

    written = out.read_text()
    assert "M6 -- OBSERVED per-lane load" in written
    assert "B_eligibility" in written


def test_rolling_window_counts_stamps_the_way_a_quota_does() -> None:
    """Half-open ``(t - 60s, t]``, anchored on each attempt.

    Same counting rule as the census's DERIVED ``stamps_in_window``: a caller spaced at
    ``i`` places ``floor(60 / i) + 1`` in a rolling minute because the anchor is free.
    A stamp exactly 60 s old has left the window.
    """
    from scripts.measure_2946_quota_load import max_rolling_window

    base = datetime(2026, 9, 13, 19, 0, tzinfo=UTC)
    # 3.333s spacing, 19 stamps: the last is 60.0s after the first, so it evicts it.
    at_floor = [base + timedelta(seconds=3.333 * n) for n in range(19)]
    assert max_rolling_window(at_floor) == 19

    exactly_60 = [base, base + timedelta(seconds=60)]
    assert max_rolling_window(exactly_60) == 1

    just_inside = [base, base + timedelta(seconds=59.999)]
    assert max_rolling_window(just_inside) == 2

    assert max_rolling_window([]) == 0
