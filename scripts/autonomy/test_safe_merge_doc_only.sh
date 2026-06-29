#!/usr/bin/env bash
# Unit test for safe_merge.sh::is_doc_only() — the #1863 doc-only fast path.
# Sources the REAL safe_merge.sh (its executable body is guarded by a
# BASH_SOURCE==$0 check, so sourcing only defines is_doc_only) and table-tests
# the predicate against representative `gh pr view --json files` outputs. No gh,
# no network — pure string logic, so this is the right place to pin it down.
#
# Run:  bash scripts/autonomy/test_safe_merge_doc_only.sh   (exit 0 = all pass)

set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$HERE/safe_merge.sh"

fails=0
check() { # check <desc> <expected: doc|strict> <files-list>
  local want="$1" desc="$2" files="$3" got
  if is_doc_only "$files"; then got=doc; else got=strict; fi
  if [ "$got" = "$want" ]; then echo "ok   - $desc"; else
    echo "FAIL - $desc (expected '$want', got '$got')"; fails=$((fails + 1)); fi
}

check doc    "single .md"                      "docs/a.md"
check doc    "multiple .md"                     $'docs/a.md\ndocs/b.md'
check doc    "nested .md paths"                 $'README.md\ndocs/specs/ui/x.md'
check strict "one code file among md disqualifies" $'docs/a.md\napp/x.py'
check strict "code file alone"                  "app/services/scoring.py"
check strict "favicon PR (svg + html)"          $'frontend/index.html\nfrontend/public/favicon.svg'
check strict "empty diff"                       ""
check strict ".md as a directory, not extension" "docs/readme.md/thing.py"
check strict "non-md extension that contains md" "docs/x.mdx"

if [ "$fails" -eq 0 ]; then echo "ALL PASS"; exit 0; else echo "$fails FAILED"; exit 1; fi
