"""One-time manual sweep of data/raw/ keeping the latest file per identity.

Non-destructive by default: run with --dry-run (default) to see what would
be deleted. Re-run with --apply to actually delete.

Identity is derived from the filename:
  - sec_fundamentals/sec_facts_{CIK}_{timestamp}.json         -> key = ("sec_fundamentals", CIK)
  - sec/sec_submissions_{CIK}_{timestamp}.json                -> key = ("sec", CIK)
  - fmp/fmp_profile_{SYMBOL}_{timestamp}.json                 -> key = ("fmp", endpoint, SYMBOL)
  - fmp/fmp_income_{SYMBOL}_{timestamp}.json                    etc.
  - etoro/candles_{INSTRUMENT}_{timestamp}.json                -> key = ("etoro", "candles", INSTRUMENT)
  - etoro/quotes_{INSTRUMENT}_{timestamp}.json                   etc.
  - etoro_broker/*_{timestamp}.json                            -> key = ("etoro_broker", prefix)

Any file whose name does not match an expected shape is left alone.

The newest file per key (by the sortable ISO-compact timestamp embedded
in the filename) is always kept.

Does NOT touch any SQL-tracked state. Does NOT modify DB. Safe to re-run.
"""

from __future__ import annotations

import argparse
import os
import re
from collections import defaultdict
from pathlib import Path

RAW_ROOT = Path("data/raw")

# Each entry: (subdir, filename regex with `ident` group, key tuple builder)
PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("sec_fundamentals", re.compile(r"^sec_facts_(?P<ident>\d+)_(?P<ts>\d{8}T\d{6}Z)\.json$")),
    ("sec", re.compile(r"^sec_submissions_(?P<ident>\d+)_(?P<ts>\d{8}T\d{6}Z)\.json$")),
    ("fmp", re.compile(r"^fmp_(?P<endpoint>[a-z_]+)_(?P<ident>[A-Z0-9.\-]+)_(?P<ts>\d{8}T\d{6}Z)\.json$")),
    ("etoro", re.compile(r"^(?P<endpoint>candles|quotes|positions|instruments)_(?P<ident>[\w\-]+)_(?P<ts>\d{8}T\d{6}Z)\.json$")),
    ("etoro_broker", re.compile(r"^(?P<endpoint>[a-z_]+)_(?P<ident>[\w\-]+)_(?P<ts>\d{8}T\d{6}Z)\.json$")),
]


def scan() -> dict[tuple[str, ...], list[tuple[str, Path, int]]]:
    """Group files by identity key. Returns {key: [(ts, path, size), ...]}."""
    groups: dict[tuple[str, ...], list[tuple[str, Path, int]]] = defaultdict(list)
    for subdir, pat in PATTERNS:
        folder = RAW_ROOT / subdir
        if not folder.is_dir():
            continue
        for entry in os.scandir(folder):
            if not entry.is_file():
                continue
            m = pat.match(entry.name)
            if not m:
                continue
            gd = m.groupdict()
            if "endpoint" in gd:
                key = (subdir, gd["endpoint"], gd["ident"])
            else:
                key = (subdir, gd["ident"])
            groups[key].append((gd["ts"], Path(entry.path), entry.stat().st_size))
    return groups


def plan(groups: dict[tuple[str, ...], list[tuple[str, Path, int]]]):
    """Return (to_keep, to_delete) lists of Path, and byte totals."""
    to_keep: list[Path] = []
    to_delete: list[tuple[Path, int]] = []
    per_subdir_keep = defaultdict(int)
    per_subdir_del = defaultdict(int)
    per_subdir_bytes = defaultdict(int)
    for key, entries in groups.items():
        entries.sort(key=lambda e: e[0])  # ts ascending; newest last
        newest = entries[-1][1]
        to_keep.append(newest)
        per_subdir_keep[key[0]] += 1
        for ts, path, size in entries[:-1]:
            to_delete.append((path, size))
            per_subdir_del[key[0]] += 1
            per_subdir_bytes[key[0]] += size
    return to_keep, to_delete, per_subdir_keep, per_subdir_del, per_subdir_bytes


def fmt_bytes(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.1f}{unit}"
        n = n // 1024 if unit == "B" else n / 1024
    return f"{n:.1f}TB"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="actually delete (default is dry-run)")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    groups = scan()
    to_keep, to_delete, per_keep, per_del, per_bytes = plan(groups)

    print(f"Scanned groups: {len(groups)}")
    print(f"Files to keep (newest per identity): {len(to_keep)}")
    print(f"Files to delete (older duplicates):  {len(to_delete)}")
    total_bytes = sum(sz for _, sz in to_delete)
    print(f"Bytes to reclaim:                    {fmt_bytes(total_bytes)}")
    print()
    print(f"{'subdir':<20} {'keep':>8} {'delete':>8} {'reclaim':>12}")
    for subdir in sorted(set(list(per_keep) + list(per_del))):
        print(
            f"{subdir:<20} {per_keep.get(subdir, 0):>8} {per_del.get(subdir, 0):>8} "
            f"{fmt_bytes(per_bytes.get(subdir, 0)):>12}"
        )

    if args.verbose:
        print()
        print("Sample of files that would be deleted:")
        for path, size in to_delete[:20]:
            print(f"  {path}  ({fmt_bytes(size)})")

    if not args.apply:
        print()
        print("Dry run. Re-run with --apply to delete.")
        return 0

    print()
    print("Deleting...")
    errors = 0
    reclaimed = 0
    for path, size in to_delete:
        try:
            path.unlink()
            reclaimed += size
        except OSError as exc:
            errors += 1
            print(f"  WARN could not delete {path}: {exc}")
    print(f"Deleted {len(to_delete) - errors} files, {fmt_bytes(reclaimed)} reclaimed, {errors} errors.")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
