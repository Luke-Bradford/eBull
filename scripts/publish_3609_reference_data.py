"""Publish the #3609 step 1 reference-input artefact (slice 1).

Spec: ``docs/research/2026-10-04-3609-step1-factor-panel.md`` §"Slices" item 1. The monthly and daily
series (JKP returns, JKP ``nyse_cutoffs``, French daily RF) live in the #2912 reference store and are
refreshed by ``scripts/refresh_2912_reference_data.py``; this script pins the accepted snapshots it
validated, and freezes the two inputs that are not series:

* JKP Documentation.pdf, whose sha256 must equal ``JKP_DOCUMENTATION_SHA256``, and the committed
  Table 9 sign CSV, checked against the loaded returns file's ``direction`` column;
* SEC DERA FSDS ``sub.txt`` for every quarter 2012Q1 .. 2021Q2, each parsed and validated. The quarter
  ZIP's sha256 is recorded and the ZIP discarded; only ``sub.txt`` is kept.

Publish protocol (as #3360): exclusive ``mkdir``, a dirty checkout refused, the manifest written last.
A failed run deletes its directory. Usage::

    PYTHONPATH=. uv run python scripts/publish_3609_reference_data.py \\
        --out ~/Library/Application\\ Support/eBull/research/factor_panel_3609_reference/<date>-<sha8>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.services.factor_panel_reference import (
    FSDS_SUB_URL,
    JKP_DOCUMENTATION_SHA256,
    JKP_DOCUMENTATION_URL,
    KNOWN_TABLE9_DIRECTION_CONFLICTS,
    TABLE9_SIGNS_PATH,
    fsds_sub_quarters,
    jkp_directions,
    load_table9_signs,
    parse_fsds_sub,
    read_sub_member,
    table9_direction_conflicts,
)

MANIFEST_SCHEMA = "factor-panel-3609-reference-v1"
SPEC_PATH = Path(__file__).resolve().parents[1] / "docs" / "research" / "2026-10-04-3609-step1-factor-panel.md"
#: Stage A's daily reads: the 21-session ``rvol_21d`` window before the first formation (2014-09-30)
#: through the last holding month (spec §"Dates and stages").
RF_WINDOW = (date(2014, 8, 1), date(2021, 5, 31))
CUTOFF_WINDOW = (date(2014, 9, 30), date(2021, 4, 30))
SPY_INTRADER_VENDOR = "icyDenev/Intrader"
_DATASETS = ("jkp_usa_monthly_vw_cap", "jkp_nyse_cutoffs", "french_three_factor_daily")
_10K_FAMILY = ("10-K", "10-K/A", "10-KT", "10-KT/A")
_10Q_FAMILY = ("10-Q", "10-Q/A", "10-QT", "10-QT/A")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_new(path: Path, data: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    return _sha256(data)


def _clean_head() -> str:
    status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True, check=True).stdout
    if status.strip():
        raise RuntimeError("refusing to publish from a dirty checkout")
    return subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()


def _snapshots(conn: psycopg.Connection[Any]) -> dict[str, dict[str, Any]]:
    pinned: dict[str, dict[str, Any]] = {}
    with conn.cursor(row_factory=dict_row) as cursor:
        for key in _DATASETS:
            cursor.execute(
                """
                SELECT snapshot_id, source_url, parser_version, response_sha256, fetched_at,
                       row_count, missing_count, first_observation, last_observation
                FROM reference_data_snapshots
                WHERE dataset_key = %(key)s AND parse_status = 'accepted'
                ORDER BY fetched_at DESC, snapshot_id DESC
                LIMIT 1
                """,
                {"key": key},
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(f"no accepted snapshot for {key}; run scripts/refresh_2912_reference_data.py")
            pinned[key] = row
    returns_day = pinned["jkp_usa_monthly_vw_cap"]["fetched_at"].astimezone(UTC).date()
    cutoffs_day = pinned["jkp_nyse_cutoffs"]["fetched_at"].astimezone(UTC).date()
    if returns_day != cutoffs_day:
        raise RuntimeError(f"JKP returns fetched {returns_day} but cutoffs {cutoffs_day}: not one download date")
    return pinned


def _coverage(conn: psycopg.Connection[Any], pinned: dict[str, dict[str, Any]]) -> dict[str, Any]:
    rf = conn.execute(
        """
        SELECT count(*) AS sessions, count(o.value) AS with_rf
        FROM research_price_daily d
        JOIN research_price_series s ON s.series_id = d.series_id
        LEFT JOIN reference_data_observations o
               ON o.snapshot_id = %(snapshot)s AND o.series_key = 'RF' AND o.observation_date = d.bar_date
        WHERE s.vendor = %(vendor)s AND s.vendor_symbol = 'SPY' AND d.bar_date BETWEEN %(lo)s AND %(hi)s
        """,
        {
            "snapshot": pinned["french_three_factor_daily"]["snapshot_id"],
            "vendor": SPY_INTRADER_VENDOR,
            "lo": RF_WINDOW[0],
            "hi": RF_WINDOW[1],
        },
    ).fetchone()
    cutoffs = conn.execute(
        """
        SELECT count(DISTINCT observation_date) FROM reference_data_observations
        WHERE snapshot_id = %(snapshot)s AND series_key IN ('nyse_p20', 'nyse_p80')
          AND observation_date BETWEEN %(lo)s AND %(hi)s
        """,
        {"snapshot": pinned["jkp_nyse_cutoffs"]["snapshot_id"], "lo": CUTOFF_WINDOW[0], "hi": CUTOFF_WINDOW[1]},
    ).fetchone()
    if rf is None or cutoffs is None:
        raise RuntimeError("coverage query returned no row")
    sessions, with_rf = int(rf[0]), int(rf[1])
    months = (CUTOFF_WINDOW[1].year - CUTOFF_WINDOW[0].year) * 12 + CUTOFF_WINDOW[1].month - CUTOFF_WINDOW[0].month + 1
    if sessions == 0 or with_rf != sessions:
        raise RuntimeError(f"French daily RF covers {with_rf} of {sessions} SPY sessions in {RF_WINDOW}")
    if int(cutoffs[0]) != months:
        raise RuntimeError(f"nyse_cutoffs cover {cutoffs[0]} of {months} formation months in {CUTOFF_WINDOW}")
    return {
        "rf_window": [d.isoformat() for d in RF_WINDOW],
        "spy_sessions": sessions,
        "spy_sessions_with_rf": with_rf,
        "cutoff_window": [d.isoformat() for d in CUTOFF_WINDOW],
        "cutoff_months": months,
    }


def _download(client: httpx.Client, url: str, destination: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with client.stream("GET", url) as response, destination.open("xb") as handle:
        response.raise_for_status()
        for chunk in response.iter_bytes(1 << 20):
            digest.update(chunk)
            handle.write(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _fsds(client: httpx.Client, out: Path) -> list[dict[str, Any]]:
    staging = out / ".download"
    staging.mkdir()
    entries: list[dict[str, Any]] = []
    for quarter in fsds_sub_quarters():
        url = FSDS_SUB_URL.format(quarter=quarter)
        archive = staging / f"{quarter}.zip"
        zip_sha256, zip_bytes = _download(client, url, archive)
        payload = read_sub_member(archive)
        parsed = parse_fsds_sub(payload, quarter=quarter)
        relative = f"inputs/fsds_sub/{quarter}.txt"
        sub_sha256 = _write_new(out / relative, payload)
        archive.unlink()
        entries.append(
            {
                "quarter": quarter,
                "url": url,
                "zip_sha256": zip_sha256,
                "zip_bytes": zip_bytes,
                "path": relative,
                "sha256": sub_sha256,
                "rows": len(parsed.records),
                "sic_null": parsed.sic_null,
                "accepted_outside_quarter": parsed.accepted_outside_quarter,
                "rows_10k_family": sum(parsed.forms.get(form, 0) for form in _10K_FAMILY),
                "rows_10q_family": sum(parsed.forms.get(form, 0) for form in _10Q_FAMILY),
            }
        )
        print(json.dumps(entries[-1]), file=sys.stderr, flush=True)
    staging.rmdir()
    return entries


def publish(out: Path) -> dict[str, Any]:
    git_sha = _clean_head()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.mkdir()  # exclusive: an existing directory is refused, never resumed
    try:
        with psycopg.connect(settings.database_url) as conn:
            pinned = _snapshots(conn)
            coverage = _coverage(conn, pinned)
            returns_payload = conn.execute(
                "SELECT payload FROM reference_data_snapshots WHERE snapshot_id = %(id)s",
                {"id": pinned["jkp_usa_monthly_vw_cap"]["snapshot_id"]},
            ).fetchone()
        if returns_payload is None:
            raise RuntimeError("pinned JKP returns snapshot vanished")
        conflicts = table9_direction_conflicts(load_table9_signs(), jkp_directions(bytes(returns_payload[0])))
        if conflicts != KNOWN_TABLE9_DIRECTION_CONFLICTS:
            raise RuntimeError(
                f"Table 9 sign conflicts {sorted(conflicts)} != known {sorted(KNOWN_TABLE9_DIRECTION_CONFLICTS)}"
            )

        with httpx.Client(timeout=120, follow_redirects=True) as client:
            documentation = client.get(JKP_DOCUMENTATION_URL)
            documentation.raise_for_status()
            if _sha256(documentation.content) != JKP_DOCUMENTATION_SHA256:
                raise RuntimeError("JKP Documentation.pdf no longer matches the pinned sha256")
            _write_new(out / "inputs" / "jkp_documentation.pdf", documentation.content)
            table9_sha256 = _write_new(out / "inputs" / TABLE9_SIGNS_PATH.name, TABLE9_SIGNS_PATH.read_bytes())

        with httpx.Client(timeout=600, headers={"User-Agent": settings.sec_user_agent}) as sec_client:
            fsds = _fsds(sec_client, out)

        manifest = {
            "schema": MANIFEST_SCHEMA,
            "git_sha": git_sha,
            "spec_sha256": _sha256(SPEC_PATH.read_bytes()),
            "published_at": datetime.now(UTC).isoformat(),
            "reference_snapshots": {
                key: {
                    **{field: row[field] for field in ("snapshot_id", "source_url", "parser_version")},
                    "response_sha256": row["response_sha256"],
                    "fetched_at": row["fetched_at"].astimezone(UTC).isoformat(),
                    "row_count": row["row_count"],
                    "missing_count": row["missing_count"],
                    "first_observation": row["first_observation"].isoformat(),
                    "last_observation": row["last_observation"].isoformat(),
                }
                for key, row in pinned.items()
            },
            "coverage": coverage,
            "jkp_documentation": {
                "url": JKP_DOCUMENTATION_URL,
                "sha256": JKP_DOCUMENTATION_SHA256,
                "last_modified": documentation.headers.get("Last-Modified"),
                "path": "inputs/jkp_documentation.pdf",
            },
            "table9_signs": {
                "path": f"inputs/{TABLE9_SIGNS_PATH.name}",
                "sha256": table9_sha256,
                "direction_conflicts": sorted(conflicts),
            },
            "fsds_sub": fsds,
        }
        manifest_path = out / "manifest.json"
        _write_new(manifest_path, (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode())
        descriptor = os.open(out, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return manifest
    except BaseException:
        shutil.rmtree(out, ignore_errors=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    manifest = publish(args.out)
    with (args.out / "manifest.json").open("rb") as handle:
        manifest_sha256 = hashlib.file_digest(handle, "sha256").hexdigest()
    fsds = manifest["fsds_sub"]
    summary = {
        "manifest_sha256": manifest_sha256,
        "reference_snapshots": {key: row["snapshot_id"] for key, row in manifest["reference_snapshots"].items()},
        "coverage": manifest["coverage"],
        "table9_direction_conflicts": manifest["table9_signs"]["direction_conflicts"],
        "fsds_quarters": len(fsds),
        "fsds_rows": sum(entry["rows"] for entry in fsds),
        "fsds_sic_null": sum(entry["sic_null"] for entry in fsds),
        "fsds_accepted_outside_quarter": sum(entry["accepted_outside_quarter"] for entry in fsds),
    }
    json.dump(summary, sys.stdout, indent=1, sort_keys=True)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
