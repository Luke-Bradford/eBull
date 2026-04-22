"""Migration-prefix hygiene (#250).

Every ``sql/NNN_*.sql`` migration must have a unique ``NNN_`` prefix so
human-authored ordering references stay unambiguous. One historical
collision (``024_broker_positions.sql`` + ``024_fundamentals_enrichment.sql``)
pre-dates the rule; it is pinned in the allow-list below so renaming an
already-applied migration doesn't break deployed ``schema_migrations``
tracking. Any NEW duplicate must be caught at test time.
"""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

# Historical collisions — intentionally preserved because renaming would
# desync ``schema_migrations`` rows on deployed DBs. Every entry is the
# three-digit prefix (as str) followed by the set of filenames that share
# it. ADD to this list ONLY after an explicit discussion; the default
# contract is "new migrations MUST be unique".
_GRANDFATHERED_DUPLICATES: dict[str, set[str]] = {
    "024": {"024_broker_positions.sql", "024_fundamentals_enrichment.sql"},
}

_SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
_PREFIX_RE = re.compile(r"^(\d{3})_[a-zA-Z0-9_]+\.sql$")


def test_migration_numeric_prefixes_are_unique_or_grandfathered() -> None:
    """Every sql/NNN_*.sql prefix is either unique or on the grandfathered list."""
    files = sorted(p.name for p in _SQL_DIR.glob("*.sql"))
    assert files, "sql/ directory is empty — test precondition violated"

    prefix_to_files: dict[str, list[str]] = {}
    for name in files:
        match = _PREFIX_RE.match(name)
        assert match is not None, (
            f"Migration {name!r} does not match the NNN_<snake>.sql naming contract. "
            f"Fix the filename before merging."
        )
        prefix = match.group(1)
        prefix_to_files.setdefault(prefix, []).append(name)

    # Collisions → either grandfathered exactly, or a new bug.
    for prefix, names in prefix_to_files.items():
        if len(names) == 1:
            continue
        expected = _GRANDFATHERED_DUPLICATES.get(prefix)
        assert expected is not None, (
            f"New duplicate migration prefix {prefix!r}: {names}. "
            f"Rename one file before merging OR — if you have an explicit "
            f"reason + discussion log — add {prefix!r} to "
            f"_GRANDFATHERED_DUPLICATES with the exact set."
        )
        assert set(names) == expected, (
            f"Grandfathered prefix {prefix!r} expected files {sorted(expected)} "
            f"but found {sorted(names)}. If a grandfathered migration was "
            f"renamed or a new file added under the same prefix, update "
            f"_GRANDFATHERED_DUPLICATES explicitly."
        )


def test_no_accidental_prefix_regression() -> None:
    """Smoke: all prefixes are strictly 3-digit. A 2- or 4-digit prefix
    would silently sort wrong under lexicographic ordering."""
    files = sorted(p.name for p in _SQL_DIR.glob("*.sql"))
    prefixes = [f.split("_", 1)[0] for f in files]
    widths = Counter(len(p) for p in prefixes)
    # Allow exactly width=3; anything else is a regression.
    assert list(widths.keys()) == [3], (
        f"Migration prefix widths drifted from 3-digit: {widths}. "
        f"Lexicographic sort would order '10_foo.sql' before '2_foo.sql' "
        f"under mixed widths."
    )
