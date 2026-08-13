# RCA — dev Postgres stuck 14h in an OOM crash-loop during WAL recovery (2026-06-03)

**Status:** RCA proven from logs + `pg_controldata`, then **multi-agent reviewed (5 lenses, 2026-06-03)** which CORRECTED the memory model. C1+D2 shipped in #1447. See verdict below.

---

## VERDICT (multi-agent review, 2026-06-03) — corrections to the analysis below

1. **OOM driver is NOT `shared_buffers`** (unanimous). It is **per-relation memory** (relcache/smgr/pending-sync + kernel dentry/inode slab) over ~705k leaked test-DB relation files. `shared_buffers` only ever faults ~720 MB in recovery; lowering it frees ~0. → **Fix B (shared_buffers→1GB) REJECTED; keep 2 GB.** "Recover in place at 512 MB" would NOT have worked.
2. **Decisive call: WIPE + re-bootstrap** (unanimous, high confidence). No knob under 6 g replays a 705k-relation WAL.
3. **C1 (separate test cluster) is the ONLY structural fix** — and must land before declaring the loop closed. C2/C3 (relation ceiling, reaper) do NOT close it (ceiling skipped on kill-9; reaper can't run during recovery). **Shipped in #1447** (`postgres-test` disk-backed service on port 5433 + fixture URL repoint + guard + pre-push auto-start). NB: disk volume, not tmpfs — tmpfs charges DB data to the container cgroup and OOMs under parallel worker-DB clones on the 7.75 GB VM (verified during build).
4. **D2 shipped** (#1447): `restart: on-failure:5` + healthcheck — kills the silent loop.
5. **A (`max_wal_size=512MB`+`checkpoint_timeout=2min`) must follow C1**, not precede it — frequent checkpoints over a 705k-relation bloated catalog amplify the "checkpoint never completes" wedge. Deferred until C1 is in.
6. Stale figures corrected: RestartCount **19** (not 15); loop **~18-19 h**; leaked DBs **~4 GB / ~705k relfiles**; there was exactly **one** early `CHECKPOINT_ONLINE` record (`1A/E4BA8378`, ~280 MB in) then none for the remaining ~19.7 GB (consistency itself was reached immediately — `minRecoveryPoint=0/0`); `wal_compression=on` + `max_locks_per_transaction=1024` also live.

The analysis below is the original (pre-review) hypothesis, retained for history; where it leans on `shared_buffers` as the driver it is **superseded by point 1**.

**Constraint from operator:** fix MUST hold within existing resources — Docker VM = 7.75 GB total, container `mem_limit = 6g`. No raising VM RAM. Constrain DB processes to be optimal/bounded so recovery never wedges again.

---

## Proven chronology (from container logs + control file)

| Time (UTC) | Event |
|---|---|
| 06-02 19:43 | Prior unclean recovery **completed** (redo 15/F1 → done). DB healthy. |
| 06-02 19:47–20:37 | Normal checkpoints. The 20:36–20:37 ones are `immediate force` with **227,062 / 245,679 sync files each** — mass-relation churn (leaked test DBs / full pytest suite on the shared dev cluster). |
| **06-02 20:37:01** | **Last completed checkpoint.** redo lsn `1A/D3A7E988` (== `pg_controldata` REDO location). |
| 20:37 → 21:34 | A workload writes **~20 GB WAL**. Next checkpoint *starts* (`checkpoint starting: wal`) but **never completes**. DB dies unclean at 21:34. |
| 06-02 21:34 | Recovery starts, `redo starts at 1A/D3A7E988`. Must replay ~20 GB to reach consistency. |
| 06-02 22:50 | `startup process … terminated by signal 9: Killed` (~17 GB in, ~76 min). cgroup OOM. |
| 22:50 → 03:01 → … | Loops every ~60–75 min. **Every restart replays from the same `1A/D3A7E988`.** Never advances. `RestartCount = 15`. Stuck ~14 h. |

`pg_controldata`: state = `in crash recovery`; latest checkpoint 20:37; REDO `1A/D3A7E988`. End of WAL ≈ `1F/BB`. Replay distance ≈ **20 GB**.

Disk: `pg_wal` 20 GB, `base/16384` (= `ebull`, oldest user-DB OID) **19 GB legit data**; leaked test DBs (`base/9097xxxx`) ~3 GB.

Resources: container pinned at **5.77 / 6 GiB (96%)**; startup proc RSS ~3.6 GB and climbing before each kill.

---

## Root cause (mechanism)

1. **Generator:** mass-relation churn on the **shared dev cluster** (full pytest suite and/or a bootstrap run creating+dropping millions of relations across `ebull_test_*` DBs — the 227k-sync-file checkpoints) wrote ~20 GB WAL after the 20:37 checkpoint, then died unclean.
2. **No checkpoint completed** in that 20 GB span → there is **no checkpoint record** for recovery to anchor a **restartpoint** on → recovery cannot flush+bound memory mid-replay → memory grows **monotonically** across all 20 GB.
3. That growth exceeds the **6 g cgroup** at ~17 GB replayed → kernel **SIGKILL** (signal 9) on the startup process.
4. `restart: unless-stopped` restarts the container → recovery begins again from the **frozen** redo pointer `1A/D3A7E988` (no new checkpoint was ever written) → **identical replay → identical OOM**. Infinite loop, invisible for 14 h.

### Why prior hardening didn't catch it
- **#1444 (syncfs)** removed the recovery-start **fsync file-walk** (the multi-*hour* stall over millions of files). The kill here is **memory during WAL replay** — a different phase and resource. syncfs is orthogonal. Claiming "recovery solved" post-#1444 was over-scoped.
- **#1426 / #1395 / work_mem caps** bound the **application/ingest** memory regime (batched queries, COPY, executor). Crash recovery is the Postgres **startup process** — it bypasses our concurrency cap, batching, and work_mem. Recovery memory was never bounded.
- **#1444 reaper** cleans leaked test DBs only at jobs-boot/daily and **cannot run while PG is in recovery** (chicken-and-egg the #1444 memo listed as STILL OPEN). So the generator was never cleaned and its WAL is now un-replayable under 6 g.

---

## Durable fix — defense in depth (all within 6 g / 7.75 g)

**A. Bound un-checkpointed WAL so replay is always small + always restartpoint-anchored.**
- Lower `max_wal_size` 1GB→**512MB** and set `checkpoint_timeout`→**2min**. After ANY crash the redo→end distance is ≤ ~max_wal_size → seconds to replay, never OOMs; and frequent checkpoint records mean restartpoints fire even on a larger replay. *(Pairs with C — without stopping the 227k-file churn, frequent checkpoints become sync storms.)*

**B. Cap recovery non-reclaimable memory.**
- Lower `shared_buffers` 2GB→**1GB** permanently. With heavy-ingest concurrency capped at 2 (#1426), 2 GB cache is not load-bearing for throughput; 1 GB frees ~1 GB of recovery headroom under the 6 g cap. (Static GUC — same value both regimes; 1 GB is the both-regimes compromise.)

**C. Remove the generator from the shared cluster.**
- The full pytest suite must NOT create millions of relations on the cluster that holds `ebull`. Options: dedicated throwaway test cluster / tmpfs PG for the suite; OR enforce the per-session relation ceiling at CREATE time (the #1444 setup tripwire) so a runaway test fails fast instead of leaking millions + 20 GB WAL.

**D. Break the silent loop + degrade gracefully.**
- `restart: unless-stopped` → `restart: on-failure` with a cap, or a crash-loop detector on `RestartCount` / repeated signal-9-startup, so an OOM-recovery loop SURFACES in minutes, not 14 h.
- App lifespan: **bind the port + serve 503** when PG is unreachable instead of blocking pre-bind (today the app vanishes entirely + login mis-reports "credentials invalid"). Related to existing #1325.
- FE: distinguish backend-unreachable from HTTP 401 (don't render "username or password invalid" on a network/5xx error).

**E. Immediate unblock for THIS stuck recovery.**
- The existing 20 GB WAL has no checkpoint records → A won't help replay it (nothing to anchor restartpoints). The only path through 20 GB under 6 g without more RAM is to shrink non-reclaimable memory: `shared_buffers` 2GB→**512MB** + `--force-recreate postgres`, let the full replay peak fit, reach consistency once, then `cleanup_test_dbs`, then restore `shared_buffers=1GB` (B).
- **Fallback if 512MB still OOMs** (replay memory dominated by relcache over the leaked relations, not shared_buffers): wipe volume + re-bootstrap (loses the 19 GB ebull data; a clean bootstrap is needed anyway for #1435 / #1431 DoD backfill).

---

## Open questions for the review panel
1. Will B (`shared_buffers=1GB`) + A (`max_wal_size=512MB`, `checkpoint_timeout=2min`) **provably** keep any future recovery under 6 g, given the dev cluster's partition count + potential leaked-DB relcache pressure? Quantify the recovery memory model.
2. Is E (`shared_buffers=512MB`) **sufficient** to replay the current 20 GB, or is the OOM driver something other than shared_buffers (relcache/smgr for millions of relations) that 512MB won't fix → wipe is the only real option?
3. Does lowering `shared_buffers` to 1 GB materially hurt steady-state ingest under concurrency-cap=2, or is it free?
4. Frequent checkpoints over a leaked-DB-bloated catalog = 12–28 s sync storms (seen at 20:36). Does A make steady-state worse unless C lands first? Sequencing.
5. Anything that still lets un-checkpointed WAL grow unbounded (long single transactions, `CREATE/DROP DATABASE` forcing immediate checkpoints that themselves can't complete)?
6. Is there a safe way to drop the leaked test DBs WITHOUT completing recovery (so we never gamble on replaying 20 GB)?
