"""Publish the #3609 step 2 extended FSDS SUB reference artefact (spec slice 1; run in slice 5).

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"Slices" items 1 and 5 and §"Registration" (PR
#3666). SEC DERA FSDS ``sub.txt`` for every quarter 2021q3 .. 2024q3, each parsed and validated by step 1's
``parse_fsds_sub``. Each quarter ZIP's sha256 is recorded and the ZIP discarded; only ``sub.txt`` is kept.

These are stage-B data. Before any download the script requires the run's ``started`` and ``access_recorded``
ledger rows and the committed access-log row they name, and it writes the run's ``sub_published`` row last (or
``failed``, which ends the run, if anything after the gate fails). The
manifest names the run; the spec and construction hashes are on that run's ``started`` row.

Publish protocol (as step 1's ``publish_3609_reference_data.py``): exclusive ``mkdir``, a dirty checkout refused,
the manifest written last. A failed run deletes its directory. Usage::

    PYTHONPATH=. uv run python scripts/publish_3609_step2_sub.py --run-id <run id> \\
        --out ~/Library/Application\\ Support/eBull/research/factor_book_3609_step2_sub/<run id>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import httpx
import psycopg

from app.config import settings
from app.services.factor_book_ledger import (
    COMMITTED_LEDGER_PATH,
    LEDGER_PATH,
    recorded_access_id,
    require_committed_access,
)
from app.services.factor_book_reference import step2_sub_quarters
from app.services.factor_panel_fidelity import append_ledger, read_ledger
from app.services.factor_panel_reference import FSDS_SUB_URL, parse_fsds_sub, read_sub_member
from app.system.git_identity import head_commit, is_dirty

MANIFEST_SCHEMA: Final = "factor-book-3609-step2-sub-v1"
MANIFEST_FILE: Final = "manifest.json"
EVENT: Final = "sub_published"
_10K_FAMILY: Final = ("10-K", "10-K/A", "10-KT", "10-KT/A")
_10Q_FAMILY: Final = ("10-Q", "10-Q/A", "10-QT", "10-QT/A")


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
    commit = head_commit()
    if is_dirty() is not False or commit is None:
        raise RuntimeError("refusing to publish from a dirty or unreadable checkout")
    return commit


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
    for quarter in step2_sub_quarters():
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


def publish(
    out: Path,
    run_id: str,
    *,
    ledger: Path,
    client: httpx.Client,
    confirm_access: Callable[[str, int], None],
    committed_ledger: Path = COMMITTED_LEDGER_PATH,
) -> str:
    """Gate, fetch, write the artefact and its ``sub_published`` row; returns the manifest's sha256.

    Nothing before the ``try`` reads stage-B data (the gate, the access check, the exclusive ``mkdir``), so a
    refusal there leaves the run open; any failure inside it ends the run.
    """
    git_sha = _clean_head()
    access_id = recorded_access_id(read_ledger(committed_ledger, ledger), run_id, before=EVENT)
    confirm_access(run_id, access_id)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.mkdir()  # exclusive: an existing directory is refused, never resumed
    try:
        manifest = {
            "schema": MANIFEST_SCHEMA,
            "run_id": run_id,
            "access_id": access_id,
            "git_sha": git_sha,
            "fsds_sub": _fsds(client, out),
            "published_at": datetime.now(UTC).isoformat(),
        }
        manifest_sha256 = _write_new(
            out / MANIFEST_FILE, (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
        )
        descriptor = os.open(out, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        append_ledger(
            ledger,
            {
                "run_id": run_id,
                "event": EVENT,
                "at": datetime.now(UTC).isoformat(),
                "artefact": str(out),
                "manifest_sha256": manifest_sha256,
            },
        )
        return manifest_sha256
    except BaseException as exc:
        try:
            try:
                shutil.rmtree(out)
            except OSError as cleanup_error:
                exc.add_note(f"{out} was not fully removed ({cleanup_error!r}); delete it, it holds stage-B files")
        finally:
            # Stage-B files may already have been read, so the run ends here, whatever the cleanup raised: a retry
            # needs a fresh run id and its own access row, never a second look under this one. A failure to write
            # that row must not replace the error.
            try:
                append_ledger(
                    ledger,
                    {
                        "run_id": run_id,
                        "event": "failed",
                        "at": datetime.now(UTC).isoformat(),
                        "step": EVENT,
                        "error": repr(exc),
                    },
                )
            except Exception as ledger_error:
                exc.add_note(f"the 'failed' ledger row was not written ({ledger_error!r}); end run {run_id} by hand")
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, default=LEDGER_PATH)
    args = parser.parse_args()

    def confirm(run_id: str, access_id: int) -> None:
        # A fresh connection: it sees the access row only if the run committed it.
        with psycopg.connect(settings.database_url) as conn:
            require_committed_access(conn, run_id, access_id)

    with httpx.Client(timeout=600, headers={"User-Agent": settings.sec_user_agent}) as client:
        digest = publish(args.out, args.run_id, ledger=args.ledger, client=client, confirm_access=confirm)
    print(json.dumps({"artefact": str(args.out), "manifest_sha256": digest}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
