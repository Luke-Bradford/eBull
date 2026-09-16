"""#2701 — one-time download extending the insider data-set span to 2006q1.

⚠ HISTORY, because it explains the shape of the call below. Until #3113,
`archives=` carried two contracts at once: `download_bulk_archives` purged every
`.zip` in the target directory absent from the list it was handed, so a filtered
list meant "this is the complete inventory, delete anything else". A filtered
list of insider archives deleted companyfacts.zip, submissions.zip and 14 fsnds
archives on the first attempt (#2701, 2026-08-14), and the workaround was to
pass the FULL inventory — re-requesting every unrelated archive to avoid losing
it.

#3113 split the contracts: the directory-owning behaviour now lives on an
explicit `prune_strays` flag, default False, opted into only by the bootstrap
stage. So this script passes the filtered insider list it always computed, and
nothing else on disk is touched. Do NOT set `prune_strays=True` here.

Steady-state `n_quarters_insider` stays at 8 deliberately: once the history is
loaded, routine runs need only recent quarters, and a default of 82 would
re-request ~1.2 GB on every fire.
"""

from __future__ import annotations

import asyncio
import logging

from app.config import settings
from app.security.master_key import resolve_data_dir
from app.services.sec_bulk_download import build_bulk_archive_inventory, download_bulk_archives

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

archives = build_bulk_archive_inventory(n_quarters_insider=200)
insider = [a for a in archives if a.name.startswith("insider_")]
target = resolve_data_dir() / "sec" / "bulk"
target.mkdir(parents=True, exist_ok=True)
print(
    f"fetching {len(insider)} insider archives (of {len(archives)} in the full inventory); "
    f"span {insider[-1].name} .. {insider[0].name}",
    flush=True,
)

result = asyncio.run(download_bulk_archives(target_dir=target, user_agent=settings.sec_user_agent, archives=insider))
ok = [r for r in result.archives if r.error is None]
bad = [r for r in result.archives if r.error is not None]
print(f"mode={result.mode} ok={len(ok)} failed={len(bad)}", flush=True)
for r in bad:
    print(f"  FAILED {r.name}: {r.error} optional={r.optional}", flush=True)
