"""#2701 — one-time download extending the insider data-set span to 2006q1.

⚠⚠ PASSES THE **FULL** INVENTORY, NOT A FILTERED ONE. `download_bulk_archives`
purges every `.zip` in the target directory that is absent from the archive list
it is handed ("stray archives not in the current inventory should still be
cleaned", sec_bulk_download.py:755-766). So `archives=` does not mean "fetch
these" — it means "this is the complete inventory, delete anything else". A
filtered list of insider archives deleted companyfacts.zip, submissions.zip and
14 fsnds archives on the first attempt (#2701, 2026-08-14).

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
print(f"full inventory: {len(archives)} archives; insider span {insider[-1].name} .. {insider[0].name}", flush=True)

result = asyncio.run(download_bulk_archives(target_dir=target, user_agent=settings.sec_user_agent, archives=archives))
ok = [r for r in result.archives if r.error is None]
bad = [r for r in result.archives if r.error is not None]
print(f"mode={result.mode} ok={len(ok)} failed={len(bad)}", flush=True)
for r in bad:
    print(f"  FAILED {r.name}: {r.error} optional={r.optional}", flush=True)
