"""Measure the Nasdaq halt-feed pubDate regression magnitude WITHOUT a deploy (#3049).

    PYTHONPATH=. uv run python scripts/probe_3049_halt_pubdate_skew.py --poll --clients 2
    PYTHONPATH=. uv run python scripts/probe_3049_halt_pubdate_skew.py --report /tmp/probe_3049.jsonl

#3049 scope item 1 says "put stored, fetched and the delta into the error message,
let it run one session, and read the distribution" — i.e. deploy logging, wait a day.
That is unnecessary: the guard at ``strategy_halts.py::store_halt_snapshot`` compares
two operands that are BOTH observable from outside the database.

* ``fetched`` is the ``<pubDate>`` of the payload the poll just retrieved.
* ``stored`` is nothing but the ``fetched`` of the last poll the guard ACCEPTED.

So the whole guard is a pure function of a poll series, and replaying it over a dense
client-side series measures the same distribution in minutes instead of a session.

⚠ This probe is READ-ONLY against the upstream RSS. It opens no database connection,
imports no writer, and deliberately calls ``parse_halt_rss`` rather than
``fetch_halt_snapshot`` so that the stored payload bytes are the thing under test.

WHAT IT MEASURED, 2026-09-14 (both runs in-window, US RTH open)
---------------------------------------------------------------

⚠⚠ **``--clients 1`` CANNOT REPRODUCE #3049 AND THAT IS THE FIRST FINDING.** Thirty-two
sequential polls produced **0** refusals: one client keeps hitting one CDN cache entry
and sees a monotonic stamp. Two CONCURRENT clients reproduced it on the FIRST round —
stamps ``16:31:30`` and ``16:31:09``, 21 seconds apart, identical item count. That is
the production shape, because ``strategy_halt_feed_refresh`` and
``strategy_paper_cycle`` both call the same refresher and their 5-minute ticks coincide.

**The mechanism is two publication lineages, not clock skew.** ``last-modified`` lands on
a 60-second cadence at a fixed offset, and the two clients sit on offsets ``:09`` and
``:30``. ``cache-control: max-age=N`` is a COUNTDOWN to that boundary (32 distinct values
over 32 polls), served by ``x-cdn: Imperva``, so each lineage is held for up to 60s.

**``payload_sha256`` cannot answer scope item 2.** One 10-minute window: **12** distinct
pubDate values, **12** distinct ``payload_sha256`` — exactly one per stamp, because
``<pubDate>`` is inside the hashed bytes — and **2** distinct halt-content fingerprints,
one of which was served under 8 stamps spanning 421 seconds. The fingerprint over parsed
halt identities is the separable one, which is why ``store_halt_snapshot`` now stores it.

**The decisive run — 60 concurrent polls, 16:44-16:55 UTC.** 21 refused (35.0%);
magnitude min 5s / median 6s / max 54s, i.e. bounded inside a minute, so "the feed is
serving a 6-hour stale cache" is excluded by measurement rather than by argument. Raw
``payload_sha256`` matched the stored one on **0 of 21**; the parsed halt content
matched on **21 of 21**. ⚠ An earlier concurrent window reported 17/19 and is
DISCARDED, not reconciled: it was replayed in request-START order (the bug Codex
checkpoint 2 caught below), and it predates ``completed_at``, so it cannot be replayed
correctly.

⚠ The report resamples to the production cadence as a separate arm. The probe polls far
denser than the job's 5 minutes, so its own refusal rate is NOT comparable to the ~20%
measured in ``job_runs``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic, sleep
from typing import Any

import httpx

# ⚠ ``content_sha256`` is NOT imported: ``parse_halt_rss`` already computes it onto the
# snapshot, and recomputing it here would be a second copy of the rule that could drift.
from app.services.strategy_halts import NASDAQ_HALT_RSS_URL, HaltFeedError, parse_halt_rss

# Headers that evidence a caching layer or a multi-node origin. Captured because
# "which node served this" is the reading #3049 offers for the alternating stamp, and
# the ticket states it as unproven.
#
# ⚠ ``cache-control`` and ``x-iinfo`` are the load-bearing two and were added after a
# first run without them. The origin sits behind Imperva and declares its own staleness
# bound in ``max-age``; ``x-iinfo`` carries the edge identifiers. A tolerance read off
# the source's own header is a source rule, whereas the same number picked by eye is the
# uncalibratable constant #2795 refused to ship.
_HEADERS_OF_INTEREST = (
    "date",
    "age",
    "cache-control",
    "expires",
    "last-modified",
    "etag",
    "content-length",
    "x-cache",
    "x-served-by",
    "x-cdn",
    "x-iinfo",
    "server",
)


@dataclass(frozen=True)
class Poll:
    """One observation of the upstream feed.

    ⚠⚠ ``completed_at`` is the ordering key, NOT ``fetched_at``. ``fetched_at`` is when
    the request was ISSUED, and two concurrent requests can finish in the opposite
    order from which they started. The production guard sees each poll at its STORE,
    i.e. after its response has landed, so replaying a concurrent series in start
    order can invent regressions that never happened and hide ones that did.
    """

    fetched_at: datetime
    completed_at: datetime
    source_pub_at: datetime | None
    payload_sha256: str | None
    content_sha256: str | None
    item_count: int | None
    error: str | None
    headers: dict[str, str]


def poll_once(client: httpx.Client) -> Poll:
    """Fetch and parse one payload, recording the failure rather than raising."""
    fetched_at = datetime.now(UTC)
    try:
        response = client.get(NASDAQ_HALT_RSS_URL, timeout=20.0)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        return Poll(fetched_at, datetime.now(UTC), None, None, None, None, f"request failed: {exc!r}", {})
    headers = {k: response.headers[k] for k in _HEADERS_OF_INTEREST if k in response.headers}
    sha = hashlib.sha256(response.content).hexdigest()
    try:
        snapshot = parse_halt_rss(response.content)
    except HaltFeedError as exc:
        # A parse refusal is a DIFFERENT failure class from the regression guard and
        # must not be folded into it — #3049 measured 54 of 56 lifetime failures as
        # the regression, and the other two were exactly this class.
        return Poll(fetched_at, datetime.now(UTC), None, sha, None, None, f"parse refused: {exc}", headers)
    return Poll(
        fetched_at,
        datetime.now(UTC),
        snapshot.source_pub_at,
        snapshot.payload_sha256,
        snapshot.content_sha256,
        len(snapshot.halts),
        None,
        headers,
    )


def _serialise(poll: Poll) -> str:
    return json.dumps(
        {
            "fetched_at": poll.fetched_at.isoformat(),
            "completed_at": poll.completed_at.isoformat(),
            "source_pub_at": poll.source_pub_at.isoformat() if poll.source_pub_at else None,
            "payload_sha256": poll.payload_sha256,
            "content_sha256": poll.content_sha256,
            "item_count": poll.item_count,
            "error": poll.error,
            "headers": poll.headers,
        }
    )


def _deserialise(line: str) -> Poll:
    raw: dict[str, Any] = json.loads(line)
    pub = raw["source_pub_at"]
    return Poll(
        datetime.fromisoformat(raw["fetched_at"]),
        datetime.fromisoformat(raw["completed_at"]),
        datetime.fromisoformat(pub) if pub else None,
        raw["payload_sha256"],
        raw.get("content_sha256"),
        raw["item_count"],
        raw["error"],
        raw["headers"],
    )


def run_poll(out_path: Path, *, seconds: int, interval: int, clients: int) -> int:
    """Poll for a bounded wall-clock span, flushing each observation as it lands.

    ⚠ Flushed per line on purpose: a probe whose output only appears at exit is
    indistinguishable from a hung one, and this repo has burned a session on exactly
    that (the buffered-background-measurement entry in the prevention log).

    ⚠⚠ ``clients`` is the arm that matters and ``clients=1`` CANNOT reproduce #3049.
    A single sequential client keeps hitting one CDN cache entry and sees a monotonic
    stamp; the production regression needs two pollers firing at the same instant on
    independent connections, which is exactly what ``strategy_halt_feed_refresh`` and
    ``strategy_paper_cycle`` do when their 5-minute ticks coincide. Each round fires
    ``clients`` requests concurrently and records all of them in completion order.
    """
    deadline = monotonic() + seconds
    count = 0
    pool = [httpx.Client(follow_redirects=True) for _ in range(clients)]
    try:
        with out_path.open("w", encoding="utf-8") as handle:
            while True:
                with ThreadPoolExecutor(max_workers=clients) as executor:
                    round_polls = list(executor.map(poll_once, pool))
                # ⚠ Completion order, never submission order. ``executor.map`` yields in
                # submission order and ``fetched_at`` is the request START, so either
                # would replay two concurrent polls in an order the guard never saw.
                round_polls.sort(key=lambda p: p.completed_at)
                for poll in round_polls:
                    handle.write(_serialise(poll) + "\n")
                    count += 1
                handle.flush()
                stamps = " ".join(
                    (p.source_pub_at.strftime("%H:%M:%S") if p.source_pub_at else "-") for p in round_polls
                )
                errors = " ".join(p.error for p in round_polls if p.error)
                print(
                    f"{round_polls[0].fetched_at:%H:%M:%S} pub=[{stamps}] "
                    f"items={[p.item_count for p in round_polls]} {errors}",
                    flush=True,
                )
                if monotonic() >= deadline:
                    break
                sleep(interval)
    finally:
        for client in pool:
            client.close()
    print(f"\n{count} polls written to {out_path}")
    return count


def resample(polls: Iterable[Poll], *, every: timedelta) -> Iterator[Poll]:
    """Thin a dense series down to one poll per window, mimicking the job's cadence."""
    next_at: datetime | None = None
    for poll in polls:
        if next_at is None or poll.completed_at >= next_at:
            yield poll
            next_at = poll.completed_at + every


@dataclass(frozen=True)
class Refusal:
    """One replay of the production guard refusing a poll."""

    # When the guard would have seen this poll, i.e. its response COMPLETION.
    completed_at: datetime
    stored: datetime
    fetched: datetime
    same_payload: bool
    same_content: bool

    @property
    def delta(self) -> timedelta:
        return self.stored - self.fetched


def replay_guard(polls: Iterable[Poll]) -> tuple[list[Refusal], int]:
    """Replay ``store_halt_snapshot``'s monotonic refusal over a poll series.

    Returns the refusals and the number of polls the guard ACCEPTED. ``stored`` only
    advances on an accepted poll, which is the behaviour that makes the guard stateful
    — a refused poll writes nothing, so the next comparison is against the same value.
    """
    stored: datetime | None = None
    stored_sha: str | None = None
    stored_content: str | None = None
    accepted = 0
    refusals: list[Refusal] = []
    for poll in polls:
        if poll.source_pub_at is None:
            continue
        if stored is not None and poll.source_pub_at < stored:
            refusals.append(
                Refusal(
                    poll.completed_at,
                    stored,
                    poll.source_pub_at,
                    poll.payload_sha256 == stored_sha,
                    poll.content_sha256 == stored_content,
                )
            )
            continue
        stored = poll.source_pub_at
        stored_sha = poll.payload_sha256
        stored_content = poll.content_sha256
        accepted += 1
    return refusals, accepted


def _describe(refusals: list[Refusal], accepted: int, label: str) -> None:
    total = len(refusals) + accepted
    if total == 0:
        print(f"{label}: no parseable polls")
        return
    rate = len(refusals) / total
    print(f"\n{label}: {len(refusals)} refused / {total} polls ({rate:.1%})")
    if not refusals:
        return
    deltas = sorted(r.delta.total_seconds() for r in refusals)
    same_payload = sum(1 for r in refusals if r.same_payload)
    same_content = sum(1 for r in refusals if r.same_content)
    print(
        f"  regression magnitude (stored - fetched), seconds: "
        f"min={deltas[0]:.1f} median={deltas[len(deltas) // 2]:.1f} max={deltas[-1]:.1f}"
    )
    print(f"  raw payload_sha256 identical to the stored one: {same_payload}/{len(refusals)}")
    print(f"  parsed halt CONTENT identical to the stored one: {same_content}/{len(refusals)}")
    for refusal in refusals[:10]:
        print(
            f"    {refusal.completed_at:%H:%M:%S} stored={refusal.stored:%H:%M:%S} "
            f"fetched={refusal.fetched:%H:%M:%S} delta={refusal.delta.total_seconds():.0f}s "
            f"same_payload={refusal.same_payload} same_content={refusal.same_content}"
        )


def run_report(path: Path) -> None:
    polls = [_deserialise(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not polls:
        print(f"{path} is empty")
        return
    errors = [p for p in polls if p.error]
    # ⚠ Re-sorted here rather than trusting the file's write order, so a series
    # collected by an older build of this probe still replays in the order the guard
    # would have seen (Codex checkpoint 2 on #3049 caught the start-order bug).
    polls.sort(key=lambda p: p.completed_at)
    parseable = [p for p in polls if p.source_pub_at is not None]
    span = polls[-1].completed_at - polls[0].completed_at
    print(f"{len(polls)} polls over {span}, {len(errors)} transport/parse errors")
    for poll in errors[:5]:
        print(f"  {poll.fetched_at:%H:%M:%S} {poll.error}")

    distinct_pub = sorted({p.source_pub_at for p in parseable if p.source_pub_at})
    print(
        f"distinct pubDate values: {len(distinct_pub)}; "
        f"distinct payload sha256: {len({p.payload_sha256 for p in parseable})}; "
        f"distinct halt CONTENT sha256: {len({p.content_sha256 for p in parseable})}"
    )

    # ⚠ The cross-tab that decides the fix: a stamp that moves while the CONTENT does
    # not is a re-serve, and that is separable without inventing a time tolerance.
    # Grouped on the CONTENT hash, never the raw one — see content_fingerprint().
    by_content: dict[str | None, set[datetime]] = {}
    for poll in parseable:
        assert poll.source_pub_at is not None
        by_content.setdefault(poll.content_sha256, set()).add(poll.source_pub_at)
    multi = {sha: stamps for sha, stamps in by_content.items() if len(stamps) > 1}
    print(f"halt content served under MORE THAN ONE pubDate: {len(multi)}/{len(by_content)}")
    for sha, stamps in list(multi.items())[:5]:
        ordered = sorted(stamps)
        print(
            f"  content={(sha or '-')[:8]} stamps={len(ordered)} "
            f"first={ordered[0]:%H:%M:%S} last={ordered[-1]:%H:%M:%S} "
            f"spread={(ordered[-1] - ordered[0]).total_seconds():.0f}s"
        )

    header_values: dict[str, set[str]] = {}
    for poll in polls:
        for key, value in poll.headers.items():
            header_values.setdefault(key, set()).add(value)
    for key in sorted(header_values):
        values = header_values[key]
        preview = sorted(values)[:3]
        print(f"header {key}: {len(values)} distinct value(s) e.g. {preview}")

    refusals, accepted = replay_guard(parseable)
    _describe(refusals, accepted, "probe cadence")
    thinned = list(resample(parseable, every=timedelta(minutes=5)))
    refusals_5m, accepted_5m = replay_guard(thinned)
    _describe(refusals_5m, accepted_5m, "resampled to the job's 5-minute cadence")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--poll", action="store_true", help="collect a fresh series")
    parser.add_argument("--report", type=Path, help="replay the guard over a collected series")
    parser.add_argument("--out", type=Path, default=Path("/tmp/probe_3049.jsonl"))
    parser.add_argument("--seconds", type=int, default=720, help="wall-clock span to poll for")
    parser.add_argument("--interval", type=int, default=20, help="seconds between rounds")
    parser.add_argument("--clients", type=int, default=2, help="concurrent pollers per round; 1 cannot reproduce #3049")
    args = parser.parse_args(argv)
    if args.poll:
        run_poll(args.out, seconds=args.seconds, interval=args.interval, clients=args.clients)
        run_report(args.out)
        return 0
    if args.report:
        run_report(args.report)
        return 0
    parser.error("pass --poll or --report")
    return 2


if __name__ == "__main__":
    sys.exit(main())
