"""#3616 slice 3b: live probes of the Bank of England database CSV endpoint (spec
``docs/specs/metrics/2026-10-13-3616-slice3b-tax-fx-boe.md``, fact 6).

Measurement only. Each probe is one GET with ``Accept-Encoding: identity`` and no redirect following, sent with the
User-Agent the build will use (``USER_AGENT``), except the probes that show the default Python client strings are
refused. The manifest records status, content type, ``Content-Length``,
``Content-Encoding``, body length, gzipped length and sha256. Each exact body is saved under ``--bodies`` (keep that
directory out of the repository: the Bank's exchange-rate data is not licensed for redistribution, spec "Source rule").
Only a body that holds no rate (a header-only or error body) is quoted in the manifest.

    uv run python scripts/probe_3616_boe_endpoint.py --today 2026-10-13 --bodies var/research/3616/inputs \\
      --out <file.json>
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any

MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
BASE = "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp?csv.x=yes"
USER_AGENT = "eBull/1.0 (+https://github.com/Luke-Bradford/eBull)"


def boe_date(day: date) -> str:
    return f"{day.day:02d}/{MONTHS[day.month - 1]}/{day.year}"


def url(series: str, start: date, end: date, vpd: str | None) -> str:
    tail = "&VFD=N" if vpd is None else f"&VPD={vpd}&VFD=N"
    return f"{BASE}&Datefrom={boe_date(start)}&Dateto={boe_date(end)}&SeriesCodes={series}&CSVF=TN&UsingCodes=Y{tail}"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


def probe(name: str, target: str, user_agent: str, bodies: Path) -> dict[str, Any]:
    opener = urllib.request.build_opener(_NoRedirect)
    request = urllib.request.Request(target, headers={"Accept-Encoding": "identity", "User-Agent": user_agent})
    try:
        with opener.open(request, timeout=30) as response:
            status, headers, body = response.status, response.headers, response.read()
    except urllib.error.HTTPError as exc:
        status, headers, body = exc.code, exc.headers, exc.read()
    digest = hashlib.sha256(body).hexdigest()
    (bodies / f"{name}-{digest[:12]}.body").write_bytes(body)
    holds_rates = status == 200 and body.count(b"\r\n") > 1
    return {
        "url": target,
        "user_agent": user_agent,
        "status": status,
        "content_type": headers.get("Content-Type"),
        "content_length_header": headers.get("Content-Length"),
        "content_encoding": headers.get("Content-Encoding"),
        "bytes": len(body),
        "gzip_bytes": len(gzip.compress(body, mtime=0)),
        "sha256": digest,
        "body": None if holds_rates else body.decode("latin-1"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--today", type=date.fromisoformat, required=True)
    parser.add_argument("--bodies", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    today = args.today
    args.bodies.mkdir(parents=True, exist_ok=True)
    supported = url("XUDLUSS", date(2019, 1, 1), today, "N")
    probes = {
        "full_vpd_n": (url("XUDLUSS", date(1975, 1, 1), today, "N"), USER_AGENT),
        "full_vpd_y": (url("XUDLUSS", date(1975, 1, 1), today, "Y"), USER_AGENT),
        "full_no_vpd": (url("XUDLUSS", date(1975, 1, 1), today, None), USER_AGENT),
        "supported_range_vpd_n": (supported, USER_AGENT),
        "weekend_only": (url("XUDLUSS", date(2026, 10, 3), date(2026, 10, 4), "N"), USER_AGENT),
        "unknown_series": (url("XUDLZZZ", date(2026, 10, 1), today, "N"), USER_AGENT),
        "default_urllib_agent": (supported, f"Python-urllib/{sys.version_info.major}.{sys.version_info.minor}"),
        "default_httpx_agent": (supported, "python-httpx/0.28.1"),
    }
    results = {name: probe(name, target, agent, args.bodies) for name, (target, agent) in probes.items()}
    result = {"probed_on": date.today().isoformat(), "probes": results}
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
