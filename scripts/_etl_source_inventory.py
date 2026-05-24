"""Canonical inventory of ETL sources + required spec sections.

Single source of truth consumed by:
- ``tests/smoke/test_etl_source_to_sink.py`` (via import).
- ``scripts/check_etl_source_docs.sh`` (via ``python -m scripts._etl_source_inventory <list>``).

Eliminates the 3-way drift class (``ManifestSource`` Literal ↔ shell
``REQUIRED_SECTIONS`` ↔ pytest ``required`` tuple) that the simplify
review flagged as the biggest maintenance risk in PR #1314's successor.

Per-list semantics:

- ``MANIFEST_SOURCES`` — derived at import from
  ``app.services.sec_manifest.ManifestSource`` Literal. Drift
  impossible by construction (no hand-maintained mirror).
- ``AD_HOC_SOURCES`` — sources that bypass the manifest framework but
  still need a per-source spec. Currently only ``sec_n_cen``; spec
  must include ``## 0. Architectural exception`` section. See #1313
  for the live decision.
- ``BULK_REFERENCE_SOURCES`` — non-manifest reference data (CIK ↔
  ticker bridge, 13F Official List, broker REST etc.). Each needs
  the same 13 sections.
- ``REQUIRED_SECTIONS`` — exact text of the section headers per
  ``docs/etl/sources/README.md § Template``. Order matters (operator
  reads top-down).
"""

from __future__ import annotations

import sys
from typing import get_args

from app.services.sec_manifest import ManifestSource

MANIFEST_SOURCES: tuple[str, ...] = tuple(sorted(get_args(ManifestSource)))

AD_HOC_SOURCES: tuple[str, ...] = ("sec_n_cen",)

BULK_REFERENCE_SOURCES: tuple[str, ...] = (
    "company_tickers",
    "company_tickers_mf",
    "company_tickers_exchange",
    "sec_13f_securities_list",
    "etoro_candles",
)

ALL_SOURCES: tuple[str, ...] = tuple(sorted(set(MANIFEST_SOURCES + AD_HOC_SOURCES + BULK_REFERENCE_SOURCES)))

REQUIRED_SECTIONS: tuple[str, ...] = (
    "## 1. Origin",
    "## 2. Watermarking model",
    "## 3. Retry posture",
    "## 4. Bootstrap path",
    "## 5. Steady-state path",
    "## 6. Manifest insert",
    "## 7. Parser",
    "## 8. Observation insert",
    "## 9. Current table refresh",
    "## 10. Operator-visible endpoint",
    "## 11. Verification queries",
    "## 12. Smoke test",
    "## 13. Known gotchas",
)

# Hard-fail if any source appears in more than one category. Catches the
# silent-dedup class the simplify agent flagged.
_seen: dict[str, str] = {}
for _name, _category in (
    [(s, "MANIFEST_SOURCES") for s in MANIFEST_SOURCES]
    + [(s, "AD_HOC_SOURCES") for s in AD_HOC_SOURCES]
    + [(s, "BULK_REFERENCE_SOURCES") for s in BULK_REFERENCE_SOURCES]
):
    if _name in _seen:
        raise RuntimeError(
            f"duplicate source {_name!r}: in both {_seen[_name]} and {_category}. "
            f"Each source must belong to exactly one category."
        )
    _seen[_name] = _category


def _print_list(name: str) -> None:
    """Print a named list to stdout, one item per line. Consumed by
    the shell lint script.
    """
    listing = {
        "manifest": MANIFEST_SOURCES,
        "ad_hoc": AD_HOC_SOURCES,
        "bulk_reference": BULK_REFERENCE_SOURCES,
        "all": ALL_SOURCES,
        "required_sections": REQUIRED_SECTIONS,
    }
    if name not in listing:
        raise SystemExit(f"unknown list: {name!r}. Known: {sorted(listing)}")
    for item in listing[name]:
        print(item)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(
            "usage: python -m scripts._etl_source_inventory <manifest|ad_hoc|bulk_reference|all|required_sections>"
        )
    _print_list(sys.argv[1])
