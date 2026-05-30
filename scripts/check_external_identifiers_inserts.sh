#!/usr/bin/env bash
#
# Lint guard: every ``INSERT INTO external_identifiers`` MUST list
# ``is_primary`` in its column list, OR live in a file on the
# ALLOW_LIST (an intentional-DEFAULT write site, justified below).
# #1173 + prevention-log §"Writer-vs-resolver ``is_primary`` mismatch
# on ``external_identifiers``".
#
# Why: readers of ``external_identifiers`` filter on ``is_primary`` to
# pick the canonical (provider, identifier) row for an instrument. A
# writer that omits the column relies on the table DEFAULT — which
# couples the writer's correctness to a column-level decision that may
# be revisited, and silently mislabels which mapping is primary at the
# row's birth (the exact PR #1172 BLOCKING that prompted this gate).
#
# Mirrors ``scripts/check_instruments_inserts.sh`` (the ``is_tradable``
# guard) — same case-insensitive + schema-qualified opener match, same
# balanced-paren column-list slice, same 5-line docstring-mention
# tolerance. Behavioural deltas (all #1173 review hardening):
#   - ALLOW_LIST: a per-file escape hatch for write sites that
#     intentionally rely on the DEFAULT, each annotated inline below.
#   - Positional INSERTs (no column list) are flagged as violations
#     rather than silently skipped — they cannot set is_primary.
#   - ``--`` line comments are stripped from the sliced column list
#     before the flag check, so an is_primary that appears only in a
#     comment does not satisfy the guard.
# (The sibling instruments guard predates these two hardenings; a
# backport is a reasonable follow-up but out of scope here.)
#
# Exits non-zero on the first violation. Wired into ``.githooks/pre-push``
# AND ``.github/workflows/ci.yml`` (the parity guard
# ``check_ci_mirrors_prepush.sh`` enforces the mirror).

set -euo pipefail

# Search the source tree. Skip Python caches, virtualenvs, and ``.git``.
SEARCH_DIRS=(app tests sql scripts)
# Files allowed to mention the pattern outside a real INSERT — the
# guard script + its self-test. Without this, the script flags its own
# comments + its test's synthetic violations.
SKIP_BASENAMES=("check_external_identifiers_inserts.sh" "test_check_external_identifiers_inserts_lint.py")
# ALLOW_LIST: write sites that INTENTIONALLY omit ``is_primary`` and
# rely on the column DEFAULT. Each entry MUST carry a justification.
# Empty today — every production + test writer sets is_primary
# explicitly. Add a basename here only with a reviewed reason.
ALLOW_LIST=()
# Case-insensitive pattern accepting optional ``schema.`` prefix.
PATTERN_GREP='[Ii][Nn][Ss][Ee][Rr][Tt][[:space:]]+[Ii][Nn][Tt][Oo][[:space:]]+([a-zA-Z_][a-zA-Z0-9_]*\.)?[Ee][Xx][Tt][Ee][Rr][Nn][Aa][Ll]_[Ii][Dd][Ee][Nn][Tt][Ii][Ff][Ii][Ee][Rr][Ss]\b'
# Awk regex for the same opener (used to start the column-list slice).
PATTERN_AWK='[Ii][Nn][Ss][Ee][Rr][Tt][[:space:]]+[Ii][Nn][Tt][Oo][[:space:]]+([a-zA-Z_][a-zA-Z0-9_]*\.)?[Ee][Xx][Tt][Ee][Rr][Nn][Aa][Ll]_[Ii][Dd][Ee][Nn][Tt][Ii][Ff][Ii][Ee][Rr][Ss]'

violations=0
violation_files=()

# Extract the (column, list) between the first ``(`` and the matching
# ``)`` after an ``INSERT INTO external_identifiers`` opener. The
# matching accounts for newlines: the ``(`` may appear on a later line.
# Returns the column-list text on stdout, or empty if no balanced
# parens found within the candidate window.
extract_column_list() {
  local file="$1"
  local start_line="$2"
  # Read up to 100 lines from the opener — long enough for the most
  # verbose INSERT in the tree, short enough that pathological files
  # don't slow the gate.
  local window_end=$((start_line + 100))
  # ``LANG=C`` so awk uses byte semantics rather than the locale's
  # multibyte semantics, which choke on non-UTF8 fixture bytes.
  LANG=C awk -v start="$start_line" -v end="$window_end" -v pat="$PATTERN_AWK" '
    BEGIN { found_open = 0; depth = 0; buffer = ""; lines_scanned = 0 }
    NR < start { next }
    NR > end { exit }
    {
      lines_scanned++
      # If we have not seen the opening ``(`` within 5 lines of the
      # opener match, the pattern is almost certainly inside a
      # docstring / prose / comment — silently bail (no output).
      if (!found_open && lines_scanned > 5) {
        exit
      }
      line = $0
      # On the opener line, drop everything up to and including the
      # ``external_identifiers`` token so we start matching parens at
      # the column list, not at an earlier syntactically-irrelevant ``(``.
      if (NR == start) {
        sub(pat, "", line)
      }
      for (i = 1; i <= length(line); i++) {
        ch = substr(line, i, 1)
        if (!found_open) {
          if (ch == "(") {
            found_open = 1
            depth = 1
            continue
          }
          # Hitting "VALUES" before any "(" means a positional INSERT
          # with NO column list — which definitionally cannot set
          # is_primary explicitly. Emit a sentinel so the caller flags
          # it as a violation (not a silent skip). #1173 Codex finding.
          rest = substr(line, i, 6)
          if (toupper(rest) == "VALUES") {
            print "__POSITIONAL_INSERT__"
            exit
          }
          continue
        }
        if (ch == "(") {
          depth++
          buffer = buffer ch
        } else if (ch == ")") {
          depth--
          if (depth == 0) {
            print buffer
            exit
          }
          buffer = buffer ch
        } else {
          buffer = buffer ch
        }
      }
      if (found_open) {
        buffer = buffer "\n"
      }
    }
  ' "$file"
}

for dir in "${SEARCH_DIRS[@]}"; do
  if [[ ! -d "${dir}" ]]; then
    continue
  fi
  while IFS= read -r file; do
    [[ -z "${file}" ]] && continue
    basename="${file##*/}"
    # Skip files explicitly allowed to mention the pattern.
    skip=0
    for sb in "${SKIP_BASENAMES[@]}"; do
      if [[ "${basename}" == "${sb}" ]]; then
        skip=1
        break
      fi
    done
    if (( skip )); then continue; fi
    # Skip intentional-DEFAULT write sites on the ALLOW_LIST. Guard the
    # expansion with a length check: macOS ships bash 3.2 where
    # ``"${empty_array[@]}"`` trips ``set -u`` (nounset).
    if (( ${#ALLOW_LIST[@]} )); then
      for al in "${ALLOW_LIST[@]}"; do
        if [[ "${basename}" == "${al}" ]]; then
          skip=1
          break
        fi
      done
    fi
    if (( skip )); then continue; fi
    # Also skip Markdown — docstrings/specs may quote the pattern.
    if [[ "${file}" == *.md ]]; then continue; fi
    # For each opener line, slice out the actual column list and check
    # ``is_primary`` appears there.
    while IFS=: read -r lineno _; do
      [[ -z "${lineno}" ]] && continue
      column_list="$(extract_column_list "${file}" "${lineno}")"
      if [[ "${column_list}" == "__POSITIONAL_INSERT__" ]]; then
        # Positional INSERT (no column list) — cannot set is_primary.
        echo "::error file=${file},line=${lineno}::INSERT INTO external_identifiers with no column list (positional) — list columns incl. is_primary explicitly"
        violations=$((violations + 1))
        violation_files+=("${file}:${lineno}")
        continue
      fi
      if [[ -z "${column_list}" ]]; then
        # No balanced column list within 5 lines of the opener —
        # almost certainly a docstring / prose mention. Skip silently.
        continue
      fi
      # Strip ``--`` line comments before the flag check so an
      # ``is_primary`` mentioned only in an inline comment inside the
      # column list does not satisfy the guard. #1173 Codex finding.
      if ! echo "${column_list}" | sed 's/--.*//' | grep -qi 'is_primary'; then
        echo "::error file=${file},line=${lineno}::INSERT INTO external_identifiers without is_primary in column list"
        violations=$((violations + 1))
        violation_files+=("${file}:${lineno}")
      fi
    done < <(grep -nE "${PATTERN_GREP}" "${file}" || true)
  done < <(grep -rlE "${PATTERN_GREP}" "${dir}" 2>/dev/null \
             --exclude-dir=__pycache__ \
             --exclude-dir=.venv \
             --exclude-dir=.git \
             --exclude='*.pyc' || true)
done

if (( violations > 0 )); then
  echo ""
  echo "FAIL: ${violations} INSERT INTO external_identifiers site(s) missing is_primary in the column list."
  echo "Every write to external_identifiers must set is_primary explicitly — see #1173 +"
  echo "docs/review-prevention-log.md §'Writer-vs-resolver is_primary mismatch on external_identifiers'."
  echo "If a site intentionally relies on the DEFAULT, add its basename to ALLOW_LIST with a reason."
  echo ""
  echo "Violating sites:"
  for v in "${violation_files[@]}"; do
    echo "  - ${v}"
  done
  exit 1
fi

echo "OK: every INSERT INTO external_identifiers includes is_primary in the column list."
