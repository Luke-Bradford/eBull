"""Empirical probe for the OpenFIGI v3 mapping API.

PR-0 of bootstrap-etl-optimisation v3 (issue #1233). Records real
request/response payloads under ``tests/fixtures/openfigi/`` so PR-1b
can build the production ``OpenFigiResolver`` against a verified
contract instead of doc-derived guesses.

Scenarios:
    (a) Single CUSIP   — AAPL ``037833100`` → expect ticker ``AAPL``.
    (b) Batch of 5     — AAPL / MSFT / JPM / GME / HD.
    (c) Batch with bad — 10 CUSIPs including one deliberately invalid
        ``000000000`` to capture the per-row error shape.
    (d) Saturation     — 30 back-to-back batches to trip the rate
        limit and record the 429 response + headers.

Reads optional ``OPENFIGI_API_KEY`` env var; default = unkeyed tier
(25 req/min × 10 jobs per POST).

Fixtures overwrite atomically. If OpenFIGI is unreachable, the script
fails fast with a non-zero exit code BEFORE any fixture is touched.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"

# Five known-good CUSIPs (the eBull smoke panel).
KNOWN_CUSIPS: dict[str, str] = {
    "AAPL": "037833100",
    "MSFT": "594918104",
    "JPM": "46625H100",
    "GME": "36467W109",
    "HD": "437076102",
}

# Five additional well-known CUSIPs to round (c) out to 9 valid + 1 invalid.
EXTRA_VALID_CUSIPS: dict[str, str] = {
    "NVDA": "67066G104",
    "TSLA": "88160R101",
    "AMZN": "023135106",
    "GOOGL": "02079K305",
    "META": "30303M102",
}

INVALID_CUSIP = "000000000"

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "openfigi"


@dataclass(frozen=True)
class ProbeResult:
    """Captured HTTP exchange for one scenario."""

    scenario: str
    request_body: list[dict[str, str]]
    status_code: int
    response_headers: dict[str, str]
    response_body: Any
    elapsed_ms: int

    def to_fixture(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "captured_at": datetime.now(UTC).isoformat(),
            "request": {
                "method": "POST",
                "url": OPENFIGI_URL,
                "body": self.request_body,
            },
            "response": {
                "status_code": self.status_code,
                # NB: dict[str, str] — multi-value headers are joined by httpx.
                "headers": self.response_headers,
                "body": self.response_body,
            },
            "elapsed_ms": self.elapsed_ms,
        }


def _post(
    client: httpx.Client,
    cusips: list[str],
    api_key: str | None,
) -> ProbeResult | None:
    """One synchronous POST. Returns None on transport error (caller decides)."""
    body = [{"idType": "ID_CUSIP", "idValue": c} for c in cusips]
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    start = time.perf_counter()
    response = client.post(OPENFIGI_URL, json=body, headers=headers, timeout=30.0)
    elapsed_ms = int((time.perf_counter() - start) * 1000)

    # Header keys are normalised to lower-case by httpx; preserve them as-is.
    captured_headers = dict(response.headers)
    try:
        parsed_body: Any = response.json()
    except ValueError:
        parsed_body = response.text

    return ProbeResult(
        scenario="(placeholder)",
        request_body=body,
        status_code=response.status_code,
        response_headers=captured_headers,
        response_body=parsed_body,
        elapsed_ms=elapsed_ms,
    )


def _write_fixture(path: Path, result: ProbeResult) -> None:
    """Atomic write: stage to .tmp then rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(result.to_fixture(), fh, indent=2, sort_keys=True, ensure_ascii=False)
        fh.write("\n")
    tmp.replace(path)


def _response_shape(body: Any) -> str:
    """One-line signature of the response body for the summary table."""
    if not isinstance(body, list):
        return f"non-array ({type(body).__name__})"
    out: list[str] = []
    for entry in body[:3]:
        if isinstance(entry, dict):
            if "data" in entry and isinstance(entry["data"], list) and entry["data"]:
                first = entry["data"][0] if isinstance(entry["data"][0], dict) else None
                ticker = first.get("ticker") if isinstance(first, dict) else None
                out.append(f"data[{len(entry['data'])}]→{ticker or '?'}")
            elif "warning" in entry:
                out.append(f"warning={entry['warning']!r}")
            elif "error" in entry:
                out.append(f"error={entry['error']!r}")
            else:
                out.append(f"keys={sorted(entry.keys())!r}")
        else:
            out.append(type(entry).__name__)
    suffix = "..." if len(body) > 3 else ""
    return f"array[{len(body)}]: {', '.join(out)}{suffix}"


def _key_headers(headers: dict[str, str]) -> str:
    """Pick the headers that matter for rate-limit reasoning."""
    interesting = (
        "retry-after",
        "x-ratelimit-remaining",
        "x-ratelimit-limit",
        "x-ratelimit-reset",
        "ratelimit-remaining",
        "ratelimit-limit",
        "ratelimit-reset",
    )
    found = {k: v for k, v in headers.items() if k.lower() in interesting}
    return ", ".join(f"{k}={v}" for k, v in sorted(found.items())) or "(none)"


def _run_scenario(
    client: httpx.Client,
    scenario_name: str,
    cusips: list[str],
    api_key: str | None,
) -> ProbeResult:
    """Run a success-path scenario. Refuses to return non-200 responses.

    If the operator's rate-limit bucket is already burned at the time
    the probe runs, the 2xx success-path fixtures could otherwise be
    overwritten with 429 plain-text bodies — silently breaking the
    contract PR-1b's resolver relies on. Raise instead so the on-disk
    fixture is preserved.
    """
    result = _post(client, cusips, api_key)
    if result is None:  # pragma: no cover - _post does not currently return None
        raise RuntimeError(f"scenario {scenario_name} produced no result")
    if result.status_code != 200:
        raise RuntimeError(
            f"scenario {scenario_name}: expected status 200, got {result.status_code}. "
            f"Refusing to overwrite the success-path fixture with a non-200 capture. "
            f"Wait for the rate-limit window to reset and re-run (response body: "
            f"{result.response_body!r:.200})."
        )
    if not isinstance(result.response_body, list):
        raise RuntimeError(
            f"scenario {scenario_name}: expected JSON array body, got "
            f"{type(result.response_body).__name__}. Refusing to write."
        )
    if len(result.response_body) != len(cusips):
        raise RuntimeError(
            f"scenario {scenario_name}: response array length "
            f"{len(result.response_body)} != request length {len(cusips)}. "
            f"OpenFIGI's parallel-array contract was violated; refusing to write."
        )
    return ProbeResult(
        scenario=scenario_name,
        request_body=result.request_body,
        status_code=result.status_code,
        response_headers=result.response_headers,
        response_body=result.response_body,
        elapsed_ms=result.elapsed_ms,
    )


def _saturate(client: httpx.Client, api_key: str | None) -> ProbeResult:
    """Issue back-to-back batches until we observe a 429.

    Returns the FIRST 429 response captured. If we exhaust the
    iteration cap without ever seeing 429, raises — the caller MUST
    NOT overwrite the on-disk ``rate_limit_429.json`` fixture with a
    non-429 capture (that would silently break PR-1b's contract).
    """
    cusips = list(KNOWN_CUSIPS.values())  # 5 lookups per batch
    for i in range(30):
        result = _post(client, cusips, api_key)
        assert result is not None
        if result.status_code == 429:
            print(f"  saturate: tripped 429 on iteration {i + 1}/30", file=sys.stderr)
            return ProbeResult(
                scenario="rate_limit_429",
                request_body=result.request_body,
                status_code=result.status_code,
                response_headers=result.response_headers,
                response_body=result.response_body,
                elapsed_ms=result.elapsed_ms,
            )
        # Tight loop on purpose — we want to exhaust the bucket.
    raise RuntimeError(
        "saturate completed 30/30 iterations WITHOUT a 429. Refusing to "
        "overwrite rate_limit_429.json with a non-429 capture. Wait for "
        "the rate-limit window to reset and re-run, or investigate why "
        "OpenFIGI is no longer enforcing the 25-req/min unkeyed limit."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixture-dir",
        type=Path,
        default=FIXTURE_DIR,
        help="Override fixture output directory (default: %(default)s).",
    )
    parser.add_argument(
        "--skip-saturation",
        action="store_true",
        help="Skip scenario (d). Useful for debugging when you do NOT want to burn the rate-limit budget.",
    )
    args = parser.parse_args()

    api_key = os.environ.get("OPENFIGI_API_KEY") or None
    tier = "keyed" if api_key else "unkeyed"
    print(f"Probe tier: {tier}", file=sys.stderr)
    print(f"Fixture dir: {args.fixture_dir}", file=sys.stderr)

    summary: list[tuple[str, int, str, str]] = []

    # Reachability check — fail fast BEFORE we touch any fixture so a
    # transient outage doesn't blank a committed fixture.
    try:
        with httpx.Client() as preflight:
            preflight.get("https://api.openfigi.com/", timeout=10.0)
    except httpx.RequestError as exc:
        print(f"OpenFIGI unreachable: {exc}", file=sys.stderr)
        return 2

    with httpx.Client() as client:
        # (a) Single AAPL
        result_a = _run_scenario(client, "single_aapl", [KNOWN_CUSIPS["AAPL"]], api_key)
        _write_fixture(args.fixture_dir / "single_aapl.json", result_a)
        summary.append(
            (
                "single_aapl",
                result_a.status_code,
                _key_headers(result_a.response_headers),
                _response_shape(result_a.response_body),
            )
        )

        # (b) Batch of 5 known
        result_b = _run_scenario(
            client,
            "batch_known_5",
            list(KNOWN_CUSIPS.values()),
            api_key,
        )
        _write_fixture(args.fixture_dir / "batch_known_5.json", result_b)
        summary.append(
            (
                "batch_known_5",
                result_b.status_code,
                _key_headers(result_b.response_headers),
                _response_shape(result_b.response_body),
            )
        )

        # (c) Batch of 10 with 1 invalid (9 valid + 1 invalid)
        mixed = list(KNOWN_CUSIPS.values()) + list(EXTRA_VALID_CUSIPS.values())[:4] + [INVALID_CUSIP]
        assert len(mixed) == 10, f"scenario (c) expected 10 CUSIPs, got {len(mixed)}"
        result_c = _run_scenario(client, "batch_with_invalid", mixed, api_key)
        _write_fixture(args.fixture_dir / "batch_with_invalid.json", result_c)
        summary.append(
            (
                "batch_with_invalid",
                result_c.status_code,
                _key_headers(result_c.response_headers),
                _response_shape(result_c.response_body),
            )
        )

        # (d) Saturation — only if not skipped.
        if not args.skip_saturation:
            result_d = _saturate(client, api_key)
            _write_fixture(args.fixture_dir / "rate_limit_429.json", result_d)
            summary.append(
                (
                    "rate_limit_429",
                    result_d.status_code,
                    _key_headers(result_d.response_headers),
                    _response_shape(result_d.response_body),
                )
            )

    # Summary table to stdout.
    print()
    print(f"{'scenario':<22} {'status':<8} {'key headers':<60} response shape")
    print("-" * 22, "-" * 8, "-" * 60, "-" * 40)
    for scenario, status, headers, shape in summary:
        print(f"{scenario:<22} {status:<8} {headers:<60} {shape}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
