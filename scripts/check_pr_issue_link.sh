#!/usr/bin/env bash
# Validate PR issue references and refuse negated GitHub closing keywords.

set -euo pipefail

pr_title=${PR_TITLE:-}
pr_body=${PR_BODY:-}

# Strip HTML comments + fenced code blocks + inline code so examples cannot
# satisfy the link gate or trigger the negation incident guard.
body_clean=$(printf '%s' "$pr_body" | perl -0pe 's/<!--.*?-->//gs; s/```.*?```//gs; s/`[^`]*`//g')

# GitHub does not understand prose negation. For example, the sentence
# "does not close #2493" closed #2493 when PR #2740 merged. Refuse a negator
# followed on the same line by a closing keyword reference; require an explicit
# Refs/Part of/Umbrella form instead.
if printf '%s\n' "$body_clean" | perl -ne '
  $found = 1 if /(?:^|[^\w])(?:not|never|without|doesn.t|don.t|didn.t|won.t|wouldn.t|shouldn.t|mustn.t|can.t|cannot)(?!\w)[^#\r\n]{0,80}(?:^|[^\w])(?:close[sd]?|closing|fix(?:e[sd]|ing)?|resolve[sd]?|resolving)\s+#\d+/i;
  END { exit(!$found) }
'; then
  echo "::error::PR body contains a negated closing-keyword reference. GitHub ignores negation and may close the issue on merge."
  echo "Use 'Refs #N', 'Part of #N', or 'Umbrella #N' for work that must leave the issue open."
  exit 1
fi

# Pull every #N out of the title. Empty title means no required issue link.
title_nums=()
while IFS= read -r issue_number; do
  if [ -n "$issue_number" ]; then
    title_nums+=("$issue_number")
  fi
done < <(printf '%s\n' "$pr_title" \
  | grep -oE '#[0-9]+' \
  | sed 's/#//' \
  | sort -u)

if [ ${#title_nums[@]} -eq 0 ]; then
  echo "Title has no issue reference; nothing to verify."
  exit 0
fi

# Left boundary blocks substrings such as "unfixes #N". The issue-number
# boundary prevents #86 from matching #869.
verb_re='(^|[^[:alnum:]_])(close[sd]?|closing|fix(e[sd]|ing)?|resolve[sd]?|resolving|refs?|references?|track[sd]?|tracking|part of|umbrella)[[:space:]:]+#'
missing=()
for n in "${title_nums[@]}"; do
  if printf '%s\n' "$body_clean" | grep -qiE "${verb_re}${n}([^0-9]|$)"; then
    continue
  fi
  missing+=("$n")
done

if [ ${#missing[@]} -gt 0 ]; then
  echo "::error::PR title references issue(s) but body has no Closes/Fixes/Resolves/Refs/Tracks/Part of/Umbrella line for: ${missing[*]}"
  echo "Use 'Closes #N' only for complete work; use 'Refs #N' or 'Part of #N' for partial work."
  exit 1
fi

echo "PR body links every title-referenced issue."
