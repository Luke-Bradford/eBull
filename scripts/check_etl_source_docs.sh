#!/usr/bin/env bash
# scripts/check_etl_source_docs.sh
#
# Lint guard: every ETL source MUST have a per-source spec file at
# docs/etl/sources/<source>.md containing the 13 required sections.
# Mirrors the pytest gate at tests/smoke/test_etl_source_to_sink.py
# but runs fast at pre-push (no pytest dependency).
#
# Single source of truth: scripts/_etl_source_inventory.py owns the
# source lists + required-section headers. Both this script and the
# pytest gate read from there — drift impossible by construction.
#
# Wired into .githooks/pre-push after check_caller_owned_tx.sh +
# .github/workflows/ci.yml lint job.
#
# Exit codes:
#   0 — every source has a complete spec file with all required sections.
#   1 — missing spec file OR missing required section header OR
#       ad-hoc source missing the §0 Architectural exception.

set -euo pipefail

DOCS_DIR="docs/etl/sources"

if [[ ! -d "$DOCS_DIR" ]]; then
    echo "::error::missing $DOCS_DIR — required by docs/etl/sources/README.md"
    exit 1
fi

# Read canonical lists from the inventory module. uv run for env
# parity with pytest. Each call emits one item per line.
ALL_SOURCES=$(uv run python -m scripts._etl_source_inventory all)
AD_HOC_SOURCES=$(uv run python -m scripts._etl_source_inventory ad_hoc)
REQUIRED_SECTIONS_FILE=$(mktemp)
trap 'rm -f "$REQUIRED_SECTIONS_FILE"' EXIT
uv run python -m scripts._etl_source_inventory required_sections > "$REQUIRED_SECTIONS_FILE"
required_count=$(wc -l < "$REQUIRED_SECTIONS_FILE" | tr -d ' ')

failures=0

for source in $ALL_SOURCES; do
    spec_file="$DOCS_DIR/${source}.md"
    if [[ ! -f "$spec_file" ]]; then
        echo "::error file=$spec_file::missing per-source spec for '$source'"
        ((failures++)) || true
        continue
    fi

    # Single-pass section check: one grep -Fxf invocation per file
    # instead of the per-section loop (cuts ~273 forks → ~21).
    # Match-count == required_count means every header present.
    match_count=$(grep -Fxf "$REQUIRED_SECTIONS_FILE" "$spec_file" | sort -u | wc -l | tr -d ' ')
    if [[ "$match_count" -ne "$required_count" ]]; then
        # Fall back to per-section loop ONLY for the failing file to
        # report which header is missing.
        while IFS= read -r section; do
            if ! grep -qxF "$section" "$spec_file"; then
                echo "::error file=$spec_file::missing required section '$section' for '$source'"
                ((failures++)) || true
            fi
        done < "$REQUIRED_SECTIONS_FILE"
    fi

    # Ad-hoc sources need an additional §0 Architectural exception.
    # Prefix-match so authors can append a qualifier like "— READ FIRST".
    case " $AD_HOC_SOURCES " in
        *" $source "*)
            if ! grep -qE "^## 0\. Architectural exception" "$spec_file"; then
                echo "::error file=$spec_file::ad-hoc source '$source' missing '## 0. Architectural exception' section"
                ((failures++)) || true
            fi
            ;;
    esac
done

if (( failures > 0 )); then
    echo "::error::$failures ETL source spec violation(s). See docs/etl/sources/README.md § Template."
    exit 1
fi

source_count=$(echo "$ALL_SOURCES" | wc -l | tr -d ' ')
echo "ETL source docs lint: clean ($source_count sources)"
exit 0
