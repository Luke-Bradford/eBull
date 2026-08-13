#!/usr/bin/env bash
#
# Lint guard: every ``INSERT INTO instruments`` MUST list ``is_tradable``
# in its column list. #1233 §6.2 + prevention-log §"INSERT INTO
# instruments fixtures must supply is_tradable".
#
# Why: ``is_tradable`` is the universe filter for SEC bootstrap entry
# points (#1233 §6.2). A row inserted without it relies on the
# ``DEFAULT TRUE`` in sql/001 — which silently corrupts the fixture's
# contract on any future schema migration that removes the default,
# AND obscures the operator-visible "delisted vs tradable" signal at
# the row's birth.
#
# The prevention-log entry called for the gate on ``tests/fixtures/``
# only; #1233 extends it to the whole tree (``app/`` + ``tests/``)
# because the same risk applies to any production write site.
#
# Codex 1a hardening pass (PR1 #1233 review):
#
# - **Case-insensitive** (``insert into instruments`` matches as well
#   as the canonical ``INSERT INTO instruments``).
# - **Schema-qualified** (``INSERT INTO public.instruments`` matches).
# - **is_tradable must appear inside the column list** — between the
#   opening ``(`` after ``instruments`` and the matching ``)`` — not
#   just somewhere in the 30-line window. Catches the false-negative
#   where ``is_tradable`` lives in a comment or in an ``ON CONFLICT``
#   clause without being part of the actual column list.
#
# Exits non-zero on the first violation. Designed to be wired into
# ``.githooks/pre-push`` so a violation blocks the push.

set -euo pipefail

# Search the source tree. Skip Python caches, virtualenvs, and
# ``.git`` (git's index, packs, and refs would otherwise eat the
# subprocess with binary noise).
SEARCH_DIRS=(app tests sql scripts)
# Files allowed to mention the pattern outside a real INSERT — the
# guard script + its self-test + Markdown docs. Without this, the
# script flags its own comments + its test's synthetic violations.
SKIP_BASENAMES=("check_instruments_inserts.sh" "test_check_instruments_inserts_lint.py")
# Case-insensitive pattern accepting optional ``schema.`` prefix.
PATTERN_GREP='[Ii][Nn][Ss][Ee][Rr][Tt][[:space:]]+[Ii][Nn][Tt][Oo][[:space:]]+([a-zA-Z_][a-zA-Z0-9_]*\.)?[Ii][Nn][Ss][Tt][Rr][Uu][Mm][Ee][Nn][Tt][Ss]\b'
# Awk regex for the same opener (used to start the column-list slice).
PATTERN_AWK='[Ii][Nn][Ss][Ee][Rr][Tt][[:space:]]+[Ii][Nn][Tt][Oo][[:space:]]+([a-zA-Z_][a-zA-Z0-9_]*\.)?[Ii][Nn][Ss][Tt][Rr][Uu][Mm][Ee][Nn][Tt][Ss]'

violations=0
violation_files=()

# Extract the (column, list) between the first ``(`` and the matching
# ``)`` after an ``INSERT INTO instruments`` opener. The matching
# accounts for newlines: the ``(`` may appear on a later line.
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
  # multibyte semantics, which choke on non-UTF8 fixture bytes
  # ("illegal byte sequence" abort under the default C.UTF-8 locale).
  LANG=C awk -v start="$start_line" -v end="$window_end" -v pat="$PATTERN_AWK" '
    BEGIN { found_open = 0; depth = 0; buffer = ""; lines_scanned = 0 }
    NR < start { next }
    NR > end { exit }
    {
      lines_scanned++
      # If we have not seen the opening ``(`` within 5 lines of the
      # opener match, the pattern is almost certainly inside a
      # docstring / prose / comment — silently bail (no output) so
      # the caller treats it as a non-match, not a violation.
      if (!found_open && lines_scanned > 5) {
        exit
      }
      line = $0
      # On the opener line, drop everything up to and including the
      # ``instruments`` token so we start matching parens at the
      # column list, not at a syntactically-irrelevant earlier ``(``.
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
          # Bail if we hit "VALUES" before any "(" — that means the
          # INSERT used positional column list elsewhere or is
          # malformed for our purposes.
          rest = substr(line, i, 6)
          if (toupper(rest) == "VALUES") {
            exit
          }
          # Skip non-( non-VALUES chars without bailing on the line.
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
    # Skip files explicitly allowed to mention the pattern.
    basename="${file##*/}"
    skip=0
    for sb in "${SKIP_BASENAMES[@]}"; do
      if [[ "${basename}" == "${sb}" ]]; then
        skip=1
        break
      fi
    done
    if (( skip )); then continue; fi
    # Also skip Markdown — docstrings/specs may quote the pattern.
    if [[ "${file}" == *.md ]]; then continue; fi
    # For each opener line, slice out the actual column list and
    # check ``is_tradable`` appears there.
    while IFS=: read -r lineno _; do
      [[ -z "${lineno}" ]] && continue
      column_list="$(extract_column_list "${file}" "${lineno}")"
      if [[ -z "${column_list}" ]]; then
        # No balanced column list found within 5 lines of the
        # opener — almost certainly a docstring / prose mention,
        # not a real INSERT. Skip silently. The trade-off: a
        # genuinely malformed INSERT (e.g. a stray ``INSERT INTO
        # instruments`` followed by nothing) slips past. We accept
        # that — such a statement would fail at parse time anyway,
        # so the cost of a missed lint is bounded.
        continue
      fi
      # Case-insensitive ``is_tradable`` match inside the column list.
      if ! echo "${column_list}" | grep -qi 'is_tradable'; then
        echo "::error file=${file},line=${lineno}::INSERT INTO instruments without is_tradable in column list"
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
  echo "FAIL: ${violations} INSERT INTO instruments site(s) missing is_tradable in the column list."
  echo "Every write to the instruments table must set is_tradable explicitly — see #1233 §6.2 +"
  echo "docs/review-prevention-log.md §'INSERT INTO instruments fixtures must supply is_tradable'."
  echo ""
  echo "Violating sites:"
  for v in "${violation_files[@]}"; do
    echo "  - ${v}"
  done
  exit 1
fi

echo "OK: every INSERT INTO instruments includes is_tradable in the column list."
