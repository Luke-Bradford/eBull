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


def next_free_prefix(prefixes: set[str]) -> str:
    """The lowest three-digit prefix above every one in use (#2362 item 1).

    The collision this reports on is structural, not careless: two loops branch
    from ``main`` at the same moment, each picks the next free number, and both
    are right when they pick it. The gate can only ever catch that at push time,
    so the least it can do is hand back the number to rename to instead of
    making the author re-derive it.

    ⚠ ``max + 1``, never "the lowest gap". A gap in the sequence is a migration
    that was renumbered away or never merged, and reusing its number would
    collide with a ``schema_migrations`` row that may still exist on a deployed
    database (see ``app.db.migrations.orphaned_ledger_filenames``).
    """
    numbers = {int(prefix) for prefix in prefixes}
    return f"{(max(numbers) + 1) if numbers else 1:03d}"


def test_migration_numeric_prefixes_are_unique_or_grandfathered() -> None:
    """Every sql/NNN_*.sql prefix is either unique or on the grandfathered list."""
    files = sorted(p.name for p in _SQL_DIR.glob("*.sql"))
    assert files, "sql/ directory is empty — test precondition violated"

    prefix_to_files: dict[str, list[str]] = {}
    for name in files:
        match = _PREFIX_RE.match(name)
        assert match is not None, (
            f"Migration {name!r} does not match the NNN_<snake>.sql naming contract. Fix the filename before merging."
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
            f"Rename one file to {next_free_prefix(set(prefix_to_files))}_<name>.sql before merging OR "
            f"— if you have an explicit reason + discussion log — add {prefix!r} to "
            f"_GRANDFATHERED_DUPLICATES with the exact set. "
            f"⚠ If the renamed file was already applied on a dev DB, its schema_migrations row keeps "
            f"the OLD name and becomes an orphan; that is reported by the migration runner, not repaired."
        )
        assert set(names) == expected, (
            f"Grandfathered prefix {prefix!r} expected files {sorted(expected)} "
            f"but found {sorted(names)}. If a grandfathered migration was "
            f"renamed or a new file added under the same prefix, update "
            f"_GRANDFATHERED_DUPLICATES explicitly."
        )


def test_no_accidental_prefix_regression() -> None:
    """Smoke: all prefixes are strictly 3-digit. A 2- or 4-digit prefix
    would silently sort wrong under lexicographic ordering.

    Files that don't match ``_PREFIX_RE`` (e.g. a hypothetical
    ``init.sql``) are handled by
    ``test_migration_numeric_prefixes_are_unique_or_grandfathered`` —
    this test only polices the width of files that ARE prefixed, so
    non-prefix failures surface with the correct error message there.
    """
    files = sorted(p.name for p in _SQL_DIR.glob("*.sql"))
    prefixes = [m.group(1) for f in files if (m := _PREFIX_RE.match(f))]
    widths = Counter(len(p) for p in prefixes)
    # Allow exactly width=3; anything else is a regression.
    assert list(widths.keys()) == [3], (
        f"Migration prefix widths drifted from 3-digit: {widths}. "
        f"Lexicographic sort would order '10_foo.sql' before '2_foo.sql' "
        f"under mixed widths."
    )


class TestNextFreePrefix:
    """#2362 item 1 — the gate now names the number to rename to."""

    def test_it_is_max_plus_one_not_the_lowest_gap(self) -> None:
        # 002 is free, and reusing it would collide with whatever
        # schema_migrations row still carries that name on a deployed DB.
        assert next_free_prefix({"001", "003"}) == "004"

    def test_it_stays_three_digits(self) -> None:
        assert next_free_prefix({"008"}) == "009"

    def test_an_empty_sql_dir_starts_at_001(self) -> None:
        assert next_free_prefix(set()) == "001"

    def test_it_names_a_number_above_every_real_migration(self) -> None:
        files = sorted(p.name for p in _SQL_DIR.glob("*.sql"))
        prefixes = {m.group(1) for f in files if (m := _PREFIX_RE.match(f))}
        assert int(next_free_prefix(prefixes)) > max(int(p) for p in prefixes)
